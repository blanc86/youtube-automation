"""What every concrete stage needs and none of them should re-derive.

A stage's fingerprint is the whole caching mechanism (see
``core.pipeline.fingerprint``), and the two ways to get it wrong are equally
silent. Feeding it the *whole* project settings makes every stage depend on
every setting, so changing a caption colour re-runs edge-tts and the cache
looks broken for no visible reason. Feeding it too little serves a cached
narration in a voice nobody asked for. ``project_settings`` is the one place
that projection is written down, and ``stage_fingerprint`` is the only
sanctioned way to build a fingerprint from it.

Lives in ``app`` rather than ``core`` because it reaches
``app.scheduler.runner.build_spec`` for the input-ordering rule that module
already owns - ``core`` may not import ``app``, and duplicating that ordering
here is exactly the kind of second source of truth that disables caching
across processes without failing anything.
"""

from __future__ import annotations

from collections.abc import Mapping

from ytauto.app.scheduler.runner import build_spec
from ytauto.core.pipeline.fingerprint import compute_fingerprint
from ytauto.core.pipeline.stage import JobContext, Stage


def project_settings(settings: Mapping[str, object], keys: tuple[str, ...]) -> dict[str, object]:
    """Narrow settings to the keys a stage declared.

    Load-bearing: ``FingerprintSpec.settings`` is hashed whole, so passing the
    full project settings would make every stage's fingerprint depend on
    every setting - changing a caption colour would re-run edge-tts. A key
    that is absent is simply omitted, so adding an unrelated setting to a
    project never invalidates a stage that does not read it.

    Omitted rather than defaulted to ``None`` on purpose: ``{"rate": None}``
    and ``{}`` are different JSON documents and therefore different hashes, so
    defaulting would make a stage's fingerprint depend on whether an
    unrelated key had ever been written to the project.

    Raises:
        Nothing. An unknown key is not an error - a stage may declare a
        setting the project has never set.
    """
    return {key: settings[key] for key in keys if key in settings}


def stage_fingerprint(
    stage: Stage, ctx: JobContext, *, provider_id: str, provider_version: str
) -> str:
    """The content hash of one stage execution: the sanctioned implementation
    of ``Stage.fingerprint``.

    ``provider_id``/``provider_version`` are passed rather than read off the
    stage because one stage can run through several providers (edge-tts or
    Piper for the same ``synthesize_speech``), and swapping engines must
    invalidate the cache.

    ``ctx.workdir`` never reaches the hash: it is job- and machine-specific,
    and a fingerprint that moved with it would never hit across jobs at all.
    That is enforced by what this passes to ``build_spec``, and independently
    by ``canonical_json``, which rejects a path outright.

    Raises:
        ValidationError: a declared setting is not fingerprintable - a path, a
            non-finite float, or an unsupported type (from
            ``compute_fingerprint``).
    """
    spec = build_spec(
        stage,
        provider_id,
        provider_version,
        ctx.inputs,
        project_settings(ctx.settings, stage.settings_keys),
    )
    return compute_fingerprint(spec)
