from pathlib import Path

import pytest

from ytauto.core.errors import ConfigurationError
from ytauto.infra.paths import AppPaths, ensure_writable_dir, resolve_output_dir


def test_explicit_override_wins(tmp_path: Path) -> None:
    paths = AppPaths.resolve(override=tmp_path)
    assert paths.root == tmp_path


def test_env_var_is_used_when_no_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("YTAUTO_DATA_DIR", str(tmp_path))
    paths = AppPaths.resolve()
    assert paths.root == tmp_path


def test_falls_back_to_platform_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("YTAUTO_DATA_DIR", raising=False)
    paths = AppPaths.resolve()
    assert paths.root.is_absolute()
    assert "ytauto" in str(paths.root).lower()


def test_all_subpaths_live_under_root(tmp_path: Path) -> None:
    paths = AppPaths.resolve(override=tmp_path)
    for child in (paths.projects, paths.cas, paths.logs, paths.cache, paths.exports):
        assert child.is_relative_to(tmp_path)
    assert paths.db_file.is_relative_to(tmp_path)


def test_ensure_creates_every_directory(tmp_path: Path) -> None:
    paths = AppPaths.resolve(override=tmp_path / "fresh")
    paths.ensure()
    for child in (paths.projects, paths.cas, paths.logs, paths.cache, paths.exports):
        assert child.is_dir()


def test_ensure_is_idempotent(tmp_path: Path) -> None:
    paths = AppPaths.resolve(override=tmp_path)
    paths.ensure()
    paths.ensure()
    assert paths.projects.is_dir()


def test_paths_are_frozen(tmp_path: Path) -> None:
    paths = AppPaths.resolve(override=tmp_path)
    with pytest.raises(AttributeError):
        paths.root = tmp_path  # type: ignore[misc]


def test_ensure_translates_oserror_to_configuration_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = AppPaths.resolve(override=tmp_path / "denied")

    def _deny(*_args: object, **_kwargs: object) -> None:
        raise PermissionError(13, "Access is denied")

    monkeypatch.setattr(Path, "mkdir", _deny)

    with pytest.raises(ConfigurationError) as excinfo:
        paths.ensure()
    assert isinstance(excinfo.value.__cause__, PermissionError)


# -- resolve_output_dir: where rendered videos land for the operator --------


def _blocked_root(tmp_path: Path, name: str) -> Path:
    """A path whose own creation is guaranteed to fail: ``name`` is written
    as an ordinary *file*, so ``mkdir(parents=True)`` on anything underneath
    it raises ``OSError`` (``NotADirectoryError`` on POSIX, a Windows
    ``OSError`` with a "not a directory"-flavoured winerror on Windows) -
    a portable way to simulate "cannot be created" without relying on
    platform-specific permission bits.
    """
    blocker = tmp_path / name
    blocker.write_text("not a directory", encoding="utf-8")
    return blocker / "root"


def test_resolve_output_dir_uses_videos_when_writable(tmp_path: Path) -> None:
    videos = tmp_path / "Videos"
    downloads = tmp_path / "Downloads"

    result = resolve_output_dir(videos_dir=videos, downloads_dir=downloads)

    assert result == videos / "ytauto"
    assert result.is_dir()


def test_resolve_output_dir_falls_back_to_downloads_when_videos_is_unusable(
    tmp_path: Path,
) -> None:
    videos = _blocked_root(tmp_path, "videos-blocker")
    downloads = tmp_path / "Downloads"

    result = resolve_output_dir(videos_dir=videos, downloads_dir=downloads)

    assert result == downloads / "ytauto"
    assert result.is_dir()


def test_resolve_output_dir_raises_naming_both_paths_when_both_are_unusable(
    tmp_path: Path,
) -> None:
    videos = _blocked_root(tmp_path, "videos-blocker")
    downloads = _blocked_root(tmp_path, "downloads-blocker")

    with pytest.raises(ConfigurationError) as excinfo:
        resolve_output_dir(videos_dir=videos, downloads_dir=downloads)

    message = str(excinfo.value)
    assert str(videos / "ytauto") in message
    assert str(downloads / "ytauto") in message


def test_ensure_writable_dir_proves_a_real_write_not_just_permission_bits(
    tmp_path: Path,
) -> None:
    target = tmp_path / "probe-me"

    assert ensure_writable_dir(target) is True
    assert target.is_dir()
    # The probe file itself must be cleaned up, not left behind.
    assert list(target.iterdir()) == []


def test_ensure_writable_dir_returns_false_rather_than_raising(tmp_path: Path) -> None:
    blocked = _blocked_root(tmp_path, "blocker")

    assert ensure_writable_dir(blocked) is False
