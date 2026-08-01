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

        self._drop_rows(fingerprint)
        return None

    def _drop_rows(self, fingerprint: str) -> None:
        """Delete a fingerprint's rows without releasing blobs.

        Used when the blobs are already gone, so releasing would drive
        refcounts below what the remaining holders expect.
        """
        with transaction(self._conn, immediate=True):
            self._conn.execute("DELETE FROM artifacts WHERE fingerprint = ?", (fingerprint,))

    def record(self, fingerprint: str, stage_id: str, artifacts: Sequence[ArtifactRef]) -> bool:
        """Store the artifacts for a fingerprint and retain their blobs.

        Returns True on a first write, False if this fingerprint was already
        recorded. On False nothing is retained again - double-retaining on every
        resume would inflate refcounts and make the artifact permanently
        unevictable.

        Raises:
            ValidationError: if ``fingerprint`` is malformed, ``artifacts`` is
                empty, or a referenced blob is absent from the CAS.
        """
        self._validate_fingerprint(fingerprint)
        if not artifacts:
            raise ValidationError(f"no artifacts to record for {fingerprint}")

        for artifact in artifacts:
            if not self._cas.exists(artifact.digest):
                raise ValidationError(
                    f"artifact {artifact.name!r} references a blob absent from the "
                    f"store: {artifact.digest}"
                )

        if self.lookup(fingerprint) is not None:
            return False

        now = utc_now_iso()
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
        for artifact in artifacts:
            self._cas.retain(artifact.digest)
        return True

    def forget(self, fingerprint: str) -> None:
        """Drop a fingerprint's artifacts and release their blobs. Idempotent.

        Raises:
            ValidationError: if ``fingerprint`` is malformed.
        """
        self._validate_fingerprint(fingerprint)
        existing = self.lookup(fingerprint)
        if existing is None:
            return
        self._drop_rows(fingerprint)
        for artifact in existing:
            self._cas.release(artifact.digest)
