"""Workspace naming, no network.

One workspace is created and retained per run, so these names are what an operator navigates by
later: they must be unique enough that a rerun does not collide, and readable enough that a pile of
them can be told apart by shape (single PFB vs manifest fan-out) at a glance.
"""

from __future__ import annotations

from datetime import datetime

from terra_import_prototype.workspace import (
    AVRO_NAME_INFIX,
    MANIFEST_NAME_INFIX,
    infix_for,
    sanitize,
    workspace_name,
)

WHEN = datetime(2026, 9, 21, 16, 51)


def test_the_name_carries_user_shape_label_and_minute():
    name = workspace_name("koon@broadinstitute.org", "export_1.avro", WHEN, infix=AVRO_NAME_INFIX)
    assert name == "koon_qc_bdc_avro_export_1_avro_202609211651"


def test_the_infix_says_which_import_shape_produced_the_workspace():
    """The whole point of this tool is comparing the two shapes; the workspaces must be tellable
    apart without opening them."""
    assert infix_for("avro") == AVRO_NAME_INFIX
    assert infix_for("manifest") == MANIFEST_NAME_INFIX

    avro = workspace_name("k@b.org", "x.avro", WHEN, infix=infix_for("avro"))
    manifest = workspace_name("k@b.org", "x.json", WHEN, infix=infix_for("manifest"))
    assert "bdc_avro" in avro and "bdc_manifest" in manifest


def test_runs_a_minute_apart_do_not_collide():
    """Minute granularity is for the rerun case: a run that fails transiently must be repeatable
    without hitting 409 name-exists."""
    first = workspace_name("k@b.org", "x.avro", datetime(2026, 9, 21, 16, 51))
    second = workspace_name("k@b.org", "x.avro", datetime(2026, 9, 21, 16, 52))
    assert first != second


def test_disallowed_characters_collapse_to_single_underscores():
    """Gen3 filenames carry colons and percent-escapes; Terra names accept [A-Za-z0-9_-]."""
    name = workspace_name("k@b.org", "export_2026-09-21T16%3A50%3A59.avro", WHEN)
    assert set(name) <= set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
    assert "__" not in name


def test_a_long_label_is_truncated_but_the_timestamp_never_is():
    """Truncating the timestamp would reintroduce collisions; truncating the label only costs
    context."""
    name = workspace_name("k@b.org", "x" * 300, WHEN, max_length=60)

    assert len(name) <= 60
    assert name.endswith("202609211651")
    assert name.startswith("k_qc_bdc_avro_")


def test_only_the_local_part_of_the_email_is_used():
    assert workspace_name("first.last@broadinstitute.org", "x.avro", WHEN).startswith("first_last_")


def test_sanitize_collapses_and_trims_the_separators_it_introduces():
    # Only the underscores sanitize itself inserts are trimmed. A hyphen is a legal Terra name
    # character, so it is left where the source put it rather than being quietly rewritten.
    assert sanitize(":::abc:::") == "abc"
    assert sanitize("a///b") == "a_b"
    assert sanitize("--abc--") == "--abc--"
    assert sanitize("") == ""
