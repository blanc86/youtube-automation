from pathlib import Path

import pytest

from ytauto.infra.paths import AppPaths


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
