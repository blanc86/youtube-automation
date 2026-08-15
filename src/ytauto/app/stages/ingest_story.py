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
The digest is computed once by the CLI at enqueue time, from the same bytes
this stage will read at run time. Fingerprinting the path instead would put
a machine-specific filesystem path into the hash - ``core.pipeline.fingerprint``'s
``_encode`` rejects a bare ``Path`` for exactly this reason, but a
``str(path)`` would sail through undetected, hashing happily and uselessly.
Fingerprinting by reading the file inside ``fingerprint()`` would make it
impure instead: two different files that happened to occupy the same path
at different times would fingerprint identically, and ``fingerprint`` must
be a pure function of the ``JobContext`` alone.
"""

from __future__ import annotations

from ytauto.app.stage_support import stage_fingerprint
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.pipeline.stage import JobContext, ProgressFn, StageResult
from ytauto.core.ports.providers import StorySource
from ytauto.infra.cas.store import CasStore


class IngestStory:
    """Turns a story fetched through an injected ``StorySource`` into ``story.txt``."""

    id = "ingest_story"
    version = 1
    depends_on: tuple[str, ...] = ()
    settings_keys: tuple[str, ...] = ("story_digest",)

    def __init__(self, *, cas: CasStore, source: StorySource) -> None:
        self._cas = cas
        self._source = source

    def fingerprint(self, ctx: JobContext) -> str:
        """See the module docstring for why this reads ``story_digest``, not
        ``story_path``.

        ``provider_id``/``provider_version`` are read off the injected
        source's own ``capabilities`` rather than hardcoded here - this
        stage does not know, and must not need to know, which concrete
        ``StorySource`` it was handed.
        """
        return stage_fingerprint(
            self,
            ctx,
            provider_id=self._source.capabilities.provider_id,
            provider_version=self._source.capabilities.version,
        )

    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
        """Read the story named by ``ctx.settings["story_path"]`` and stage it.

        Raises:
            ProviderError: FATAL, propagated from the injected ``StorySource``'s
                ``fetch`` if the story cannot be read.
            KeyError: if ``ctx.settings`` carries no ``"story_path"`` - a job
                enqueued with no story attached. ``run_stage`` (the worker's
                caller) translates any exception raised here into a FATAL
                worker-protocol error, so this is not caught specially.
        """
        story_path = ctx.settings["story_path"]
        text = self._source.fetch(str(story_path))
        digest = self._cas.stage_file(text.encode("utf-8"), kind="text")
        return StageResult(artifacts=(ArtifactRef(name="story.txt", kind="text", digest=digest),))
