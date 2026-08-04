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
        a miss, and its stale rows are dropped. This is what makes the cache
        safe: ``record`` commits rows before it retains blobs (see the class
        docstring), so a crash in that window can leave rows pointing at
        evictable blobs. Reporting a hit for artifacts that no longer exist
        would make the scheduler skip a stage whose output is gone.

        Must be called outside an open transaction: the self-healing path opens
        its own, and ``transaction`` is not re-entrant.

        Raises:
            ValidationError: if ``fingerprint`` is malformed.
            sqlite3.OperationalError: if the self-healing delete cannot acquire
                the write lock within ``busy_timeout`` (legitimate contention).
            TransactionError: if a transaction is already open on the
                connection. Note this is reachable from a method that reads as a
                pure query - the self-healing path takes a write lock.
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

        self._drop_rows(fingerprint)
        return None

    def _drop_rows(self, fingerprint: str) -> None:
        """Delete a fingerprint's rows without releasing blobs.

        Used when the blobs are already gone, so releasing would drive
        refcounts below what the remaining holders expect.

        Not releasing is right for one of the two callers and lossy for the
        other, and the rows alone cannot tell them apart:

        - Crash window (``record`` committed rows, then died before retaining):
          no retain ever happened, so releasing would push the refcount below
          what other holders expect. Dropping the rows is exactly correct.
        - Retained-then-vanished (``record`` completed, the blob file was later
          lost): the +1 from ``retain`` is still on the ``cas_objects`` row and
          this path strands it - a refcount above zero with no holder, which
          makes the blob permanently unevictable.

        The stranded count is normally reclaimed by
        ``CasStore.forget_rows_without_files()`` (driven by
        ``Evictor.sweep_orphans``), which deletes the whole row, refcount
        included. That reclamation is lost if identical content is re-stored
        before the sweep runs: ``put_bytes``'s ``ON CONFLICT DO UPDATE`` touches
        only ``last_accessed_at``, so the phantom +1 survives forever.

        Telling the two branches apart needs a ``retained`` marker column, an
        append-only migration that belongs with the Phase 1b savepoint work
        (carry-forward 1.2) that makes ``record`` atomic and dissolves this
        whole class of window.
        """
        with transaction(self._conn, immediate=True):
            self._conn.execute("DELETE FROM artifacts WHERE fingerprint = ?", (fingerprint,))

    def _has_rows(self, fingerprint: str) -> bool:
        """Whether any row exists for this fingerprint, without self-healing.

        ``lookup`` cannot answer this question: it treats a row whose blob is
        gone as a miss and deletes it. This is the raw table state, which is what
        distinguishes a concurrent writer's committed rows from an integrity
        violation that has nothing to do with the primary key.
        """
        row = self._conn.execute(
            "SELECT 1 FROM artifacts WHERE fingerprint = ? LIMIT 1", (fingerprint,)
        ).fetchone()
        return row is not None

    def record(self, fingerprint: str, stage_id: str, artifacts: Sequence[ArtifactRef]) -> bool:
        """Store the artifacts for a fingerprint and retain their blobs.

        Returns True on a first write, False if this fingerprint was already
        recorded.

        The already-recorded check buys idempotence, not refcount protection.
        ``PRIMARY KEY (fingerprint, name)`` rejects the duplicate INSERT before
        the retain loop below can run a second time, so refcounts stay symmetric
        with or without the check. What it converts is the failure mode: a
        crash-resume re-executing a completed stage would otherwise take an
        ``IntegrityError`` and kill the worker on every single resume. The guard
        turns that would-be primary-key violation into the documented False.

        The same violation is also reachable legitimately. The check reads in
        one transaction and the INSERT runs in another with no lock held across
        the gap, so a concurrent writer - normal here, since cross-project dedup
        means shared fingerprints are expected - can commit in between. Both
        callers see a miss, both INSERT, and the loser collides. That collision
        is proof somebody else recorded it, which is precisely the False
        contract, so it is caught and reported as False - but only after
        confirming rows for this fingerprint actually exist, so that an
        integrity violation from some future constraint is re-raised rather than
        silently reported as a cache hit. The loser must not retain: the winner
        already did, and a second retain would be the inflation this docstring
        used to wrongly claim the check prevents.

        Must be called outside an open transaction: it opens its own, and
        ``transaction`` is not re-entrant.

        Raises:
            ValidationError: if ``fingerprint`` is malformed, ``stage_id`` is
                blank, ``artifacts`` is empty, two artifacts in the batch share
                a name, or a referenced blob is absent from the CAS. The last is
                also raised late, from ``retain`` after the rows are committed,
                for a blob whose file is present but whose ``cas_objects`` row
                is not - the orphan state the eviction sweep exists to reclaim.
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
            # lookup(), whose self-healing path would report a miss (and delete
            # the winner's rows) if the blobs had since vanished.
            if not self._has_rows(fingerprint):
                raise
            # Somebody else recorded it. No retain: the winner holds the pin.
            return False
        for artifact in artifacts:
            self._cas.retain(artifact.digest)
        return True

    def forget(self, fingerprint: str) -> None:
        """Drop a fingerprint's artifacts and release their blobs. Idempotent.

        Must be called outside an open transaction: it opens its own, and
        ``transaction`` is not re-entrant.

        Raises:
            ValidationError: if ``fingerprint`` is malformed. Note this can only
                be raised before anything is written; a release that fails
                because its ``cas_objects`` row is already gone is tolerated
                rather than reported, so a caller seeing ValidationError from
                ``forget`` can rely on "bad input, nothing happened".
            sqlite3.OperationalError: if the row delete, or a ``release``'s own
                transaction, cannot acquire the write lock within
                ``busy_timeout`` (legitimate contention). Unlike ValidationError above,
                this one can arrive from the release loop *after* the rows are
                committed, leaving the blobs partly released.
            TransactionError: if a transaction is already open on the
                connection.
        """
        self._validate_fingerprint(fingerprint)
        existing = self.lookup(fingerprint)
        if existing is None:
            return
        self._drop_rows(fingerprint)
        for artifact in existing:
            try:
                self._cas.release(artifact.digest)
            except ValidationError:
                # release() raises ValidationError for exactly two reasons: a
                # malformed digest, or no cas_objects row for that digest. Every
                # ArtifactRef validates its digest in __post_init__, and these
                # refs were constructed by lookup(), so the digest is provably
                # well-formed - the only reachable cause here is the missing
                # row. Without that argument this catch would be swallowing two
                # different failures.
                #
                # A missing row means the pin is already gone, which is the end
                # state forget() is driving towards. Propagating instead would
                # be actively worse: _drop_rows has already committed, so the
                # caller would get "bad input" semantics for a half-finished
                # release with the rows irrecoverably deleted.
                continue
