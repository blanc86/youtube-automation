import sqlite3

import pytest

from ytauto.app.services.projects import ProjectService
from ytauto.core.errors import ValidationError


@pytest.fixture()
def svc(db_conn: sqlite3.Connection) -> ProjectService:
    return ProjectService(db_conn)


def test_settings_round_trip_through_json(svc: ProjectService) -> None:
    pid = svc.create(
        slug="ghost-train",
        title="Ghost Train",
        story_digest=None,
        settings={"voice": "en-US-GuyNeural", "seed": 7},
    )
    assert svc.settings_for(pid) == {"voice": "en-US-GuyNeural", "seed": 7}


def test_a_duplicate_slug_is_refused(svc: ProjectService) -> None:
    svc.create(slug="dup", title="A", story_digest=None, settings={})
    with pytest.raises(ValidationError, match="slug"):
        svc.create(slug="dup", title="B", story_digest=None, settings={})


def test_set_setting_merges_without_clobbering_other_keys(svc: ProjectService) -> None:
    pid = svc.create(
        slug="merge-test",
        title="Merge Test",
        story_digest=None,
        settings={"voice": "en-US-GuyNeural", "seed": 7},
    )

    svc.set_setting(pid, "resolution", "1080p")
    assert svc.settings_for(pid) == {
        "voice": "en-US-GuyNeural",
        "seed": 7,
        "resolution": "1080p",
    }

    svc.set_setting(pid, "seed", 99)
    assert svc.settings_for(pid) == {
        "voice": "en-US-GuyNeural",
        "seed": 99,
        "resolution": "1080p",
    }


def test_get_raises_on_unknown_project(svc: ProjectService) -> None:
    with pytest.raises(ValidationError, match="no such project"):
        svc.get("does-not-exist")


def test_settings_for_raises_on_unknown_project(svc: ProjectService) -> None:
    with pytest.raises(ValidationError, match="no such project"):
        svc.settings_for("does-not-exist")


def test_set_setting_raises_on_unknown_project(svc: ProjectService) -> None:
    with pytest.raises(ValidationError, match="no such project"):
        svc.set_setting("does-not-exist", "key", "value")
