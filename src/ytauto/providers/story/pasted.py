"""The pasted-story source, and the entry-point factory that wires it into ``ingest_story``.

``PastedStorySource`` (the ``StorySource`` port implementation) lives here,
under ``providers/``, where every concrete provider belongs. The stage that
consumes it, ``IngestStory``, lives in ``ytauto.app.stages.ingest_story``
instead - typed against the ``StorySource`` Protocol, never against this
concrete class - because ``ytauto.app`` may not import ``ytauto.providers``
(an import-linter ``forbidden`` contract). ``make_stage`` below is the one
thing that stands on both sides of that boundary: it is free to import both
``app.stages.ingest_story`` and this module's own ``PastedStorySource``,
since the forbidden contract runs only one way (``app`` may not import
``providers``; nothing stops ``providers`` importing ``app``), and it is
resolved by ``app/registry.py`` dynamically through ``importlib.metadata``
entry points - invisible to import-linter's static analysis - so ``app``
never names this module either.

An earlier version of this module defined ``IngestStory`` itself, reasoning
that a stage needing a provider at construction time could not live in
``app/`` while the provider lived in ``providers/``. Review caught that this
conflated needing a *concrete provider class* with depending on the *port
Protocol* purpose-built for this seam: ``StorySource`` already lets
``IngestStory`` live in ``app/`` and receive its provider by injection here.
The version that skipped the Protocol could not be tested against a fake
source and had no check that ``PastedStorySource`` actually satisfied the
Protocol it claimed to implement - both fixed below.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from ytauto.app.stages.ingest_story import IngestStory
from ytauto.core.errors import ErrorKind, ProviderError
from ytauto.core.ports.capability import CapabilityDescriptor, CostModel, LatencyClass
from ytauto.infra.cas.store import CasStore

PROVIDER_VERSION = "1"
"""Bump when ``PastedStorySource.fetch``'s behaviour changes.

Fed to ``stage_fingerprint`` (via ``IngestStory.fingerprint``, which reads it
off ``capabilities.version``) as ``provider_version``. A behaviour change
that did not bump this would let artifacts staged under the old behaviour
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
        # Highest tier: the story's *content* reaches the pipeline unchanged.
        # Content, not bytes - fetch()'s read_text call normalises newlines
        # (see its own docstring), so this is fidelity to what a human
        # wrote, not a literal byte-for-byte copy of the file on disk.
        quality_tier=5,
        # A pass-through provider has no language-specific behaviour to
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


def make_stage(*, cas: CasStore, settings: Mapping[str, object]) -> IngestStory:
    """Entry point ``story_video:ingest_story``.

    ``settings`` is accepted, not forwarded: every entry-point factory has
    the same ``(*, cas, settings)`` shape (``app.registry.build_stage``'s
    contract), but ``IngestStory`` needs nothing from project settings at
    construction time - it reads ``ctx.settings["story_path"]`` at run time
    instead (see ``IngestStory.run``). This factory picks exactly one
    provider unconditionally, so there is no settings-dependent decision
    here that could disagree between the dispatcher's copy of this stage and
    the worker's - see ``registry.build_stage``'s fingerprint-divergence
    warning.
    """
    return IngestStory(cas=cas, source=PastedStorySource())
