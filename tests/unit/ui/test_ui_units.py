"""The three pure pieces of the UI that are easier to pin directly than through HTTP.

Slug derivation, the ASS colour conversion, and the ``ytauto ui`` command's
own surface. Everything else about the UI is tested where it is used, through
Flask's test client, in ``test_web_ui.py``.
"""

from __future__ import annotations

import sqlite3

import pytest

from ytauto.cli.__main__ import main
from ytauto.ui import HOST
from ytauto.ui.colours import alpha_of, ass_to_hex, hex_to_ass
from ytauto.ui.script_prompt import extract
from ytauto.ui.slugs import MAX_SLUG_LENGTH, slugify, unique_slug

# -- slugs ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("The Ghost Train", "the-ghost-train"),
        ("  Leading and trailing  ", "leading-and-trailing"),
        ("Punctuation!!! Everywhere???", "punctuation-everywhere"),
        ("Café Noir", "cafe-noir"),
        ("2:17 A.M.", "2-17-a-m"),
        ("under_scores", "under-scores"),
        ("🙂🙂🙂", "project"),
        ("", "project"),
    ],
)
def test_slugify(title: str, expected: str) -> None:
    assert slugify(title) == expected


def test_a_very_long_title_is_truncated_at_a_hyphen_boundary() -> None:
    slug = slugify("word " * 40)

    assert len(slug) <= MAX_SLUG_LENGTH
    assert not slug.endswith("-"), "a truncated slug must not end on a separator"


def test_unique_slug_suffixes_from_two(db_conn: sqlite3.Connection) -> None:
    """Guard-pin. The unsuffixed slug IS the first one, so the second is -2."""
    _insert(db_conn, "night-shift")
    assert unique_slug(db_conn, "Night Shift") == "night-shift-2"

    _insert(db_conn, "night-shift-2")
    assert unique_slug(db_conn, "Night Shift") == "night-shift-3"


def test_a_windows_device_name_is_treated_as_taken(db_conn: sqlite3.Connection) -> None:
    """``projects/nul/story.txt`` is the null device, not a file. A project
    titled "NUL" would write its story nowhere and read back nothing.
    """
    assert unique_slug(db_conn, "NUL") == "nul-2"
    assert unique_slug(db_conn, "com1") == "com1-2"


def test_a_suffixed_slug_still_respects_the_length_cap(db_conn: sqlite3.Connection) -> None:
    title = "x" * 200
    _insert(db_conn, slugify(title))

    suffixed = unique_slug(db_conn, title)

    assert len(suffixed) <= MAX_SLUG_LENGTH
    assert suffixed.endswith("-2")


def _insert(conn: sqlite3.Connection, slug: str) -> None:
    conn.execute(
        """
        INSERT INTO projects (id, slug, title, story_digest, settings_json, created_at, updated_at)
        VALUES (?, ?, ?, NULL, '{}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
        """,
        (slug, slug, slug),
    )
    conn.commit()


# -- colours --------------------------------------------------------------


@pytest.mark.parametrize(
    ("html", "ass"),
    [
        ("#ffffff", "&H00FFFFFF"),  # white - the only one that cannot expose the bug
        ("#ff0000", "&H000000FF"),  # pure red  -> BB=00 GG=00 RR=FF
        ("#0000ff", "&H00FF0000"),  # pure blue -> BB=FF GG=00 RR=00
        ("#ffff00", "&H0000FFFF"),  # yellow, the default accent
        ("#123456", "&H00563412"),
    ],
)
def test_html_and_ass_colours_round_trip_in_bgr_order(html: str, ass: str) -> None:
    """Guard-pin. ASS is ``&HAABBGGRR`` - blue before red. An RGB-ordered
    implementation passes the white case and swaps every other colour.
    """
    assert hex_to_ass(html) == ass
    assert ass_to_hex(ass) == html


def test_alpha_is_read_back_rather_than_forced_opaque() -> None:
    assert alpha_of("&H8000FFFF") == "80"
    assert hex_to_ass("#ffff00", alpha=alpha_of("&H8000FFFF")) == "&H8000FFFF"


def test_an_unparseable_stored_colour_falls_back_instead_of_raising() -> None:
    """A hand-edited ``caption_style`` must still render a usable form."""
    assert ass_to_hex("purple-ish", default="#abcdef") == "#abcdef"
    assert ass_to_hex(None, default="#abcdef") == "#abcdef"


def test_a_colour_that_is_not_six_hex_digits_is_refused() -> None:
    with pytest.raises(ValueError, match="not a #rrggbb colour"):
        hex_to_ass("rgb(1,2,3)")


# -- the script prompt ----------------------------------------------------


def test_extract_takes_only_the_fenced_block_under_the_prompt_heading() -> None:
    markdown = "# Title\n\nprose\n\n## The prompt\n\n```text\nTHE PROMPT\n```\n\n## Notes\n\nmore\n"

    assert extract(markdown) == "THE PROMPT"


def test_extract_falls_back_to_the_whole_document_if_the_shape_changed() -> None:
    markdown = "# Title\n\nJust prose, no heading anyone recognises.\n"

    assert extract(markdown) == markdown.strip()


def test_the_real_docs_file_still_has_the_shape_the_panel_expects() -> None:
    """If SCRIPT-PROMPT.md is restructured, the panel silently starts showing
    the whole document instead of the prompt. This is the thing that notices.
    """
    from ytauto.ui import script_prompt

    text = script_prompt.load()

    assert text != script_prompt.MISSING_MESSAGE, "docs/SCRIPT-PROMPT.md was not found"
    assert "TARGET LENGTH:" in text
    assert "## Notes" not in text, "the surrounding prose must not be in the panel"


# -- the command ----------------------------------------------------------


def test_the_ui_binds_to_loopback_only() -> None:
    assert HOST == "127.0.0.1"


def test_ytauto_ui_has_no_host_flag(capsys: pytest.CaptureFixture[str]) -> None:
    """Guard-pin on the one security property this UI has.

    There is no authentication behind these routes. A ``--host`` flag would
    exist only to be pointed at 0.0.0.0, so its absence is the feature.
    """
    with pytest.raises(SystemExit):
        main(["ui", "--help"])

    help_text = capsys.readouterr().out
    assert "--port" in help_text
    assert "--host" not in help_text
