"""``ingest_story``: the pipeline's first stage, built on the ``StorySource`` port.

Depends on ``core.ports.providers.StorySource`` - the Protocol purpose-built
for this seam - never on a concrete provider class. ``ytauto.app`` may not
import ``ytauto.providers`` (an import-linter ``forbidden`` contract), so
this module cannot construct a ``PastedStorySource`` itself even if it
wanted to. Instead the concrete source is constructed and injected by
``providers/story/pasted.py``'s ``make_stage`` - the one thing standing on
both sides of that boundary, since the forbidden contract only runs one way
(``app`` may not import ``providers``; nothing stops ``providers`` importing
``app``).

That injection is not decoration: it is what makes ``IngestStory`` testable
against a fake ``StorySource`` (see ``tests/unit/app/stages/test_ingest_story.py``)
and what will let a future provider swap in - a Google Doc import, say -
with a one-line change to ``make_stage`` and no change here at all.

Fingerprints over ``settings["story_digest"]``, never ``settings["story_path"]``.
The digest is computed by the CLI at enqueue time, from the same bytes this
stage will read at run time. Fingerprinting the path instead would put
a machine-specific filesystem path into the hash - ``core.pipeline.fingerprint``'s
``_encode`` rejects a bare ``Path`` for exactly this reason, but a
``str(path)`` would sail through undetected, hashing happily and uselessly.
Fingerprinting by reading the file inside ``fingerprint()`` would make it
impure instead: two different files that happened to occupy the same path
at different times would fingerprint identically, and ``fingerprint`` must
be a pure function of the ``JobContext`` alone.

**That split is only safe while something keeps the two in step, so ``run``
checks.** ``fingerprint`` hashes the digest; ``run`` reads the path. The
enqueue-time refresh in ``app.services.enqueue.refresh_run_settings`` is
what keeps them agreeing on every path this CLI drives, and the guard at the
end of ``run`` below is what makes any *other* path fail loudly instead of
silently. Without it, a divergence is a silent wrong answer twice over: the
job succeeds serving stale content, and the fresh content gets recorded
under the stale digest's fingerprint - which, since this stage's fingerprint
carries no ``project_id`` (deliberately, so identical stories dedupe across
projects), is then served to any other project whose story genuinely hashes
to that digest.
"""

from __future__ import annotations

from ytauto.app.stage_support import stage_fingerprint
from ytauto.core.errors import ErrorKind, ProviderError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.models.content_hash import hash_bytes
from ytauto.core.pipeline.stage import JobContext, ProgressFn, StageResult
from ytauto.core.ports.providers import StorySource
from ytauto.infra.cas.store import CasStore

PROVIDER_ID = "pasted"
"""Literal, fed to ``stage_fingerprint``, matching
``providers/story/pasted.py``'s ``PastedStorySource.capabilities.provider_id``
- pinned equal by a test in that provider's own test module.

This used to be read off ``self._source.capabilities`` instead, which made
``ingest_story`` the one stage in the pipeline whose fingerprint depended on
an object its factory constructed. Every other stage carries literals, for
the reason ``synthesize_speech.py``'s module docstring sets out in full: the
dispatcher builds a stage once per process and a worker rebuilds it per job,
so the moment a factory picks its provider *from settings* the two processes
can inject different providers from different settings snapshots and compute
different fingerprints for what the dispatcher believes is one cached stage.
The whole-branch review flagged the inconsistency; the literal convention is
the one kept."""

PROVIDER_VERSION = "1"
"""Bump when ``PastedStorySource.fetch``'s behaviour changes - and bump
``providers/story/pasted.py``'s ``PROVIDER_VERSION`` in the same commit. The
two are asserted equal by that provider's own test, so bumping either alone
fails the gate. That assertion is the whole point: only *this* constant
reaches ``stage_fingerprint``, so a provider-side bump made on its own would
change ``capabilities`` and invalidate nothing at all."""


class IngestStory:
    """Turns a story fetched through an injected ``StorySource`` into ``story.txt``."""

    id = "ingest_story"
    version = 1
    depends_on: tuple[str, ...] = ()
    settings_keys: tuple[str, ...] = ("story_digest",)
    gpu_pool = "gpu_compute"
    """No GPU work at all; the plain default pool - see
    ``core.pipeline.stage.Stage.gpu_pool``'s own docstring for why this is a
    required, explicit literal rather than an implicit fallback."""

    def __init__(self, *, cas: CasStore, source: StorySource) -> None:
        self._cas = cas
        self._source = source

    def fingerprint(self, ctx: JobContext) -> str:
        """See the module docstring for why this reads ``story_digest``, not
        ``story_path``, and why ``provider_id``/``provider_version`` are the
        literals above rather than anything read off ``self._source``."""
        return stage_fingerprint(
            self, ctx, provider_id=PROVIDER_ID, provider_version=PROVIDER_VERSION
        )

    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
        """Read the story named by ``ctx.settings["story_path"]`` and stage it,
        refusing to stage content that is not what this job was fingerprinted
        against.

        The digest check is the loud half of the fix the whole-branch review
        asked for (see the module docstring); the enqueue-time refresh in
        ``app.services.enqueue.refresh_run_settings`` is the half that makes
        the ordinary edit-and-rerun path simply work. This one exists for
        every *other* path: a hand-written job, a settings mapping edited
        between enqueue and execution, a future caller that forgets to
        refresh. FATAL rather than RETRYABLE because a retry reads the same
        file and computes the same digest - the mismatch is a state
        divergence, not a transient fault.

        The comparison happens *before* staging rather than after, so a
        rejected story never leaves a blob behind: Task 11's review found the
        same shape in ``compose.py`` (an ``.ass`` staged before ffmpeg ran
        left an orphan on every FATAL) and fixed it the same way.

        Raises:
            ProviderError: FATAL, propagated from the injected ``StorySource``'s
                ``fetch`` if the story cannot be read; or FATAL, raised here,
                if the story on disk does not hash to
                ``ctx.settings["story_digest"]``.
            KeyError: if ``ctx.settings`` carries no ``"story_path"`` or no
                ``"story_digest"`` - a job enqueued with no story attached.
                ``run_stage`` (the worker's caller) translates any exception
                raised here into a FATAL worker-protocol error, so this is
                not caught specially.
        """
        story_path = ctx.settings["story_path"]
        text = self._source.fetch(str(story_path))
        encoded = text.encode("utf-8")
        digest = hash_bytes(encoded)
        expected = str(ctx.settings["story_digest"])
        if str(digest) != expected:
            raise ProviderError(
                f"the story at {story_path} hashes to {digest}, but this job was "
                f"fingerprinted against story_digest {expected}. The two must agree "
                "or this job's artifacts would be recorded under a digest that "
                "describes different text - and ingest_story's fingerprint carries "
                "no project_id, so that entry is shared with every other project. "
                "Re-run `ytauto run`, which recomputes story_digest from the file "
                "before enqueueing",
                provider_id=PROVIDER_ID,
                kind=ErrorKind.FATAL,
            )
        staged = self._cas.stage_file(encoded, kind="text")
        return StageResult(artifacts=(ArtifactRef(name="story.txt", kind="text", digest=staged),))
