"""Maps stage fingerprints to the artifacts they produced.

A fingerprint with stored artifacts means the stage can be skipped. That single
lookup is what delivers crash-resume, cheap iteration, and cross-project dedup.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence

from ytauto.core.errors import ValidationError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.content_hash import ContentHash, validate_digest
from ytauto.infra.cas.store import CasStore
from ytauto.infra.clock import utc_now_iso
from ytauto.infra.db.engine import transaction


class ArtifactStore:
    """Fingerprint-keyed index over the content-addressed store."""

    def __init__(self, cas: CasStore, conn: sqlite3.Connection) -> None:
        self._cas = cas
        self._conn = conn

    @staticmethod
    def _validate_fingerprint(fingerprint: str) -> str:
        """Raises:
        ValidationError: if the fingerprint is not a sha256 hex digest.
        """
        try:
            validate_digest(fingerprint)
        except ValidationError as exc:
            raise ValidationError(f"not a valid fingerprint: {fingerprint!r}") from exc
        return fingerprint

    def lookup(self, fingerprint: str) -> tuple[ArtifactRef, ...] | None:
        """Return the artifacts recorded for this fingerprint, or None on a miss.

        A recorded fingerprint whose blobs are no longer in the store counts as
        a miss. The cache pins nothing it records, so a cached blob is an
        ordinary eviction candidate and being aged out is an expected miss, not
        corruption. Reporting a hit for artifacts that no longer exist would
        make the scheduler skip a stage whose output is gone.

        Detecting those stale rows is all this does with them; reclaiming them
        is ``heal``'s job. The split is deliberate and load-bearing: this stays
        a pure read, so the scheduler can probe the cache from inside the same
        transaction that claims a job. A self-healing DELETE here would take a
        write lock on the caller's connection and roll that claim back.

        Raises:
            ValidationError: if ``fingerprint`` is malformed.
        """
        self._validate_fingerprint(fingerprint)
        rows = self._conn.execute(
            "SELECT name, kind, digest FROM artifacts WHERE fingerprint = ? ORDER BY name ASC",
            (fingerprint,),
        ).fetchall()
        if not rows:
            return None

        found = tuple(
            ArtifactRef(name=row["name"], kind=row["kind"], digest=ContentHash(row["digest"]))
            for row in rows
        )
        if all(self._cas.exists(artifact.digest) for artifact in found):
            return found

        return None

    def heal(self) -> int:
        """Drop rows whose blobs are gone. Returns the number of fingerprints cleared.

        ``lookup`` deliberately does not do this: it must stay a pure read so the
        scheduler can probe the cache while holding a job claim. Detection lives
        there, reclamation lives here, and the split is why a cache probe cannot
        take a write lock.

        Raises:
            sqlite3.OperationalError: if the delete cannot acquire the write lock
                within ``busy_timeout`` (legitimate contention).
            TransactionError: if ``immediate=True`` is requested inside an open
                transaction - do not call this from inside a claim.
        """
        rows = self._conn.execute("SELECT DISTINCT fingerprint FROM artifacts").fetchall()
        stale = [
            row["fingerprint"]
            for row in rows
            if not all(
                self._cas.exists(ContentHash(a["digest"]))
                for a in self._conn.execute(
                    "SELECT digest FROM artifacts WHERE fingerprint = ?", (row["fingerprint"],)
                )
            )
        ]
        for fingerprint in stale:
            self._drop_rows(fingerprint)
        return len(stale)

    def _drop_rows(self, fingerprint: str) -> None:
        """Delete a fingerprint's rows without touching blob refcounts.

        There is no pin to release: the cache does not retain what it records,
        so every refcount on one of these blobs belongs to somebody else - a
        project asset, or an in-flight job holding what it consumes. Releasing
        here would drive those below what their holders expect.
        """
        with transaction(self._conn, immediate=True):
            self._conn.execute("DELETE FROM artifacts WHERE fingerprint = ?", (fingerprint,))

    def _has_rows(self, fingerprint: str) -> bool:
        """Whether any row exists for this fingerprint, regardless of its blobs.

        ``lookup`` cannot answer this question: it treats a row whose blob is
        gone as a miss. This is the raw table state, which is what distinguishes
        a concurrent writer's committed rows from an integrity violation that has
        nothing to do with the primary key, and a stale entry ``forget`` must
        still clear from a fingerprint that was never recorded at all.
        """
        row = self._conn.execute(
            "SELECT 1 FROM artifacts WHERE fingerprint = ? LIMIT 1", (fingerprint,)
        ).fetchone()
        return row is not None

    def record(self, fingerprint: str, stage_id: str, artifacts: Sequence[ArtifactRef]) -> bool:
        """Index the artifacts a fingerprint produced. The blobs are not pinned.

        Returns True on a first write, False if this fingerprint was already
        recorded.

        Recording deliberately does not ``retain``. A cache entry is not a
        holder: it says "these bytes were produced by this stage", not "somebody
        needs these bytes now". Pinning here put every cached blob permanently
        beyond ``iter_evictable``'s ``WHERE refcount = 0``, which stopped the
        disk ceiling being enforceable and left the evictor with nothing to
        choose from but the outputs of running stages. In-flight protection
        belongs to the job, which retains what it consumes and releases when it
        finishes or is reaped; an entry whose blob was aged out in the meantime
        is a miss, which ``lookup`` already reports.

        The already-recorded check buys idempotence: a crash-resume
        re-executing a completed stage would otherwise take an
        ``IntegrityError`` from ``PRIMARY KEY (fingerprint, name)`` and kill the
        worker on every single resume. The guard turns that would-be primary-key
        violation into the documented False.

        The same violation is also reachable legitimately. The check reads in
        one transaction and the INSERT runs in another with no lock held across
        the gap, so a concurrent writer - normal here, since cross-project dedup
        means shared fingerprints are expected - can commit in between. Both
        callers see a miss, both INSERT, and the loser collides. That collision
        is proof somebody else recorded it, which is precisely the False
        contract, so it is caught and reported as False - but only after
        confirming rows for this fingerprint actually exist, so that an
        integrity violation from some future constraint is re-raised rather than
        silently reported as a cache hit.

        Must be called outside an open transaction: the INSERTs run in an
        ``immediate=True`` transaction, which is still refused when nested.

        Raises:
            ValidationError: if ``fingerprint`` is malformed, ``stage_id`` is
                blank, ``artifacts`` is empty, two artifacts in the batch share
                a name, or a referenced blob is absent from the CAS. Every one
                of those is checked pre-flight, before anything is written.
            sqlite3.OperationalError: if the write lock cannot be acquired
                within ``busy_timeout`` (legitimate contention).
            TransactionError: if a transaction is already open on the
                connection.
            sqlite3.IntegrityError: if an INSERT violates a constraint other
                than the primary-key collision described above - today
                unreachable, since the primary key is this table's only
                constraint, but re-raised rather than assumed away.
        """
        self._validate_fingerprint(fingerprint)
        if not stage_id.strip():
            raise ValidationError(f"stage_id must not be blank for {fingerprint}")
        if not artifacts:
            raise ValidationError(f"no artifacts to record for {fingerprint}")

        # Two artifacts sharing a name is malformed input, not a race: the
        # primary key would reject it, and (unlike the concurrent-writer
        # collision below) reporting False for it would silently swallow a
        # caller bug. It has to be caught here, before any write, so the
        # IntegrityError handler never sees it.
        seen: set[str] = set()
        for artifact in artifacts:
            if artifact.name in seen:
                raise ValidationError(
                    f"duplicate artifact name in one batch for {fingerprint}: {artifact.name!r}"
                )
            seen.add(artifact.name)

        for artifact in artifacts:
            if not self._cas.exists(artifact.digest):
                raise ValidationError(
                    f"artifact {artifact.name!r} references a blob absent from the "
                    f"store: {artifact.digest}"
                )

        if self.lookup(fingerprint) is not None:
            return False

        now = utc_now_iso()
        try:
            with transaction(self._conn, immediate=True):
                for artifact in artifacts:
                    self._conn.execute(
                        "INSERT INTO artifacts "
                        "(fingerprint, name, stage_id, kind, digest, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            fingerprint,
                            artifact.name,
                            stage_id,
                            artifact.kind,
                            artifact.digest,
                            now,
                        ),
                    )
        except sqlite3.IntegrityError:
            # Today the primary key is the only constraint on this table, so a
            # collision can only mean a concurrent writer committed after our
            # lookup missed - the documented False. Rather than trust that as a
            # standing assumption, verify it: a later migration adding a CHECK or
            # FOREIGN KEY would otherwise turn this handler into a silent
            # swallower of real errors. Ask the table directly instead of calling
            # lookup(), which would report a miss - and so re-raise as if nothing
            # had been written - if the winner's blobs had since been evicted.
            if not self._has_rows(fingerprint):
                raise
            # Somebody else recorded it: the documented False.
            return False
        return True

    def forget(self, fingerprint: str) -> None:
        """Drop a fingerprint's index rows. Idempotent. Blobs are not released.

        Symmetry with ``record``: nothing was retained, so nothing may be
        released. Any refcount these blobs carry belongs to another holder - a
        project asset, or a job holding what it consumes - and releasing it here
        would drive that below what the holder expects. Dropping the rows simply
        returns the blobs to the evictor's ordinary candidate pool.

        The row check runs alongside ``lookup`` because they answer different
        questions: a fingerprint whose blobs were evicted is a *miss* but still
        has rows, and forgetting it must clear them rather than silently no-op.

        Must be called outside an open transaction: the row delete runs in an
        ``immediate=True`` transaction, which is still refused when nested.

        Raises:
            ValidationError: if ``fingerprint`` is malformed. Raised before
                anything is written, so a caller seeing ValidationError from
                ``forget`` can rely on "bad input, nothing happened".
            sqlite3.OperationalError: if the row delete cannot acquire the write
                lock within ``busy_timeout`` (legitimate contention).
            TransactionError: if a transaction is already open on the
                connection.
        """
        self._validate_fingerprint(fingerprint)
        if self.lookup(fingerprint) is None and not self._has_rows(fingerprint):
            return
        self._drop_rows(fingerprint)
