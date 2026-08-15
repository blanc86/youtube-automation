"""The pasted-story source, and the ``ingest_story`` stage built on it.

Both the port implementation (``PastedStorySource``) and the stage
(``IngestStory``) live in this one module, under ``providers/`` rather than
split across ``providers/`` and ``app/stages/``. ``ytauto.app`` may not
import ``ytauto.providers`` - an import-linter ``forbidden`` contract - so a
stage that needs a provider at construction time cannot itself live in
``app/`` while the provider it wraps lives in ``providers/``. The two are
wired together the way every stage is: ``app/registry.py`` resolves the
entry point ``"story_video:ingest_story"`` to ``make_stage`` below without
ever importing this module by name, so ``app`` never depends on
``providers`` and the contract stays intact.

``IngestStory`` fingerprints over ``settings["story_digest"]``, never
``settings["story_path"]``. The digest is computed once by the CLI at
enqueue time, from the same bytes this stage will read at run time.
Fingerprinting the path instead would put a machine-specific filesystem path
into the hash - ``core.pipeline.fingerprint``'s ``_encode`` rejects a bare
``Path`` for exactly this reason, but a ``str(path)`` would sail through
undetected, hashing happily and uselessly. Fingerprinting by reading the
file inside ``fingerprint()`` would make it impure instead: two different
files that happened to occupy the same path at different times would
fingerprint identically, and ``fingerprint`` must be a pure function of the
``JobContext`` alone.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from ytauto.app.stage_support import stage_fingerprint
from ytauto.core.errors import ErrorKind, ProviderError
from ytauto.core.models.artifact import ArtifactRef
from ytauto.core.pipeline.stage import JobContext, ProgressFn, StageResult
from ytauto.core.ports.capability import CapabilityDescriptor, CostModel, LatencyClass
from ytauto.infra.cas.store import CasStore

PROVIDER_VERSION = "1"
"""Bump when ``PastedStorySource.fetch``'s behaviour changes.

Fed to ``stage_fingerprint`` as ``provider_version``. A behaviour change that
did not bump this would let artifacts staged under the old behaviour
masquerade as this version's output - the same hazard ``Stage.version``
guards against for the stage itself, one layer down at the provider.
"""


class PastedStorySource:
    """Reads a story that already exists as a local UTF-8 text file.

    The only ``StorySource`` this project ships for Phase 2a - importing a
    story from a URL or a note-taking app is out of scope, and ``reference``
    is always a filesystem path.
    """

    capabilities = CapabilityDescriptor(
        provider_id="pasted",
        version=PROVIDER_VERSION,
        cost_model=CostModel.FREE,
        latency_class=LatencyClass.INSTANT,
        offline=True,
        requires_gpu=False,
        vram_mb=None,
        quality_tier=5,
        # A byte-for-byte passthrough has no language-specific behaviour to
        # restrict - "und" (ISO 639-2 "undetermined") signals that rather
        # than naming a language this provider does not actually care about.
        languages=frozenset({"und"}),
    )

    def fetch(self, reference: str) -> str:
        """Return ``reference``'s contents verbatim, decoded as UTF-8.

        Goes through ``Path.read_text`` rather than reading and decoding raw
        bytes by hand. That looks backwards - ``read_text``'s universal-
        newlines translation normalises ``\\r\\n``/``\\r`` to ``\\n`` on the
        way in - until you notice ``write_text`` performs the *matching*
        translation on the way out (encoding a lone ``\\n`` as the
        platform's ``os.linesep``, ``\\r\\n`` on Windows). The two are
        inverses of each other: on any given platform, whatever string a
        caller wrote is exactly the string this returns, which is the
        verbatim contract the brief's own pinned test enforces. Decoding raw
        bytes instead would return the platform's literal on-disk encoding
        of that string - ``\\r\\n``-for-``\\n`` on Windows - which is a
        *different* string and fails that pinned test outright. (This was
        the implementer's own first attempt; it was wrong. See the task
        report.)

        Raises:
            ProviderError: FATAL, if ``reference`` cannot be opened as a file
                (missing, a directory, permission denied - anything raising
                ``OSError``) or is not valid UTF-8. Both failures are
                deterministic: the same bytes on disk fail the same way on
                every retry, so neither is retryable.
        """
        try:
            return Path(reference).read_text(encoding="utf-8")
        except OSError as exc:
            raise ProviderError(
                f"could not read story file {reference!r}: {exc}",
                provider_id="pasted",
                kind=ErrorKind.FATAL,
            ) from exc
        except UnicodeDecodeError as exc:
            raise ProviderError(
                f"story file {reference!r} is not valid UTF-8: {exc}",
                provider_id="pasted",
                kind=ErrorKind.FATAL,
            ) from exc


class IngestStory:
    """The pipeline's first stage: turns a pasted story into ``story.txt``.

    See the module docstring for why this fingerprints over
    ``settings["story_digest"]`` rather than ``settings["story_path"]``.
    """

    id = "ingest_story"
    version = 1
    depends_on: tuple[str, ...] = ()
    settings_keys: tuple[str, ...] = ("story_digest",)

    def __init__(self, *, cas: CasStore, settings: Mapping[str, object]) -> None:
        # ``settings`` is accepted, not stored: every entry-point factory has
        # the same ``(*, cas, settings)`` shape, but this stage has exactly
        # one provider and makes no construction-time decision from project
        # settings - the fingerprint-divergence hazard registry.build_stage
        # warns about only exists for a factory that does. What this stage
        # actually reads at run time comes from ``ctx.settings`` instead (see
        # ``run``), which is the job's own settings rather than whatever this
        # process happened to be constructed with.
        self._cas = cas
        self._source = PastedStorySource()

    def fingerprint(self, ctx: JobContext) -> str:
        return stage_fingerprint(self, ctx, provider_id="pasted", provider_version=PROVIDER_VERSION)

    def run(self, ctx: JobContext, emit: ProgressFn) -> StageResult:
        """Read the story named by ``ctx.settings["story_path"]`` and stage it.

        Raises:
            ProviderError: FATAL, propagated from ``PastedStorySource.fetch``
                if the story file is missing or not valid UTF-8.
            KeyError: if ``ctx.settings`` carries no ``"story_path"`` - a job
                enqueued with no story attached. ``run_stage`` (the worker's
                caller) translates any exception raised here into a FATAL
                worker-protocol error, so this is not caught specially.
        """
        story_path = ctx.settings["story_path"]
        text = self._source.fetch(str(story_path))
        digest = self._cas.stage_file(text.encode("utf-8"), kind="text")
        return StageResult(artifacts=(ArtifactRef(name="story.txt", kind="text", digest=digest),))


def make_stage(*, cas: CasStore, settings: Mapping[str, object]) -> IngestStory:
    """Entry point ``story_video:ingest_story``."""
    return IngestStory(cas=cas, settings=settings)
