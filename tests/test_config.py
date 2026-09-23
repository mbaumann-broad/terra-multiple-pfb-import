"""Config loading and tier resolution, no network.

The point of a tier is that base URLs, credentials and target endpoints switch **together**, so
moving from dev to prod is a configuration change and never a code change. The tests that matter are
therefore about a tier being all-or-nothing: no partial resolution, no mixing.
"""

from __future__ import annotations

from pathlib import Path

import pydantic
import pytest
import yaml

from terra_import_prototype.config import SERVICE_URLS, TIERS, load_config, resolve_tier

MINIMAL = {
    "default_tier": "dev",
    "tiers": {
        "dev": {"email": "you@test.firecloud.org", "terra_billing_project": "dev-proj"},
        "prod": {"email": "you@example.org", "terra_billing_project": "prod-proj"},
    },
}


def write(tmp_path: Path, data) -> Path:
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(data))
    return path


def test_the_default_tier_is_used_when_none_is_named(tmp_path):
    tier = resolve_tier(load_config(write(tmp_path, MINIMAL)))
    assert tier.name == "dev"


def test_every_url_switches_together_with_the_tier(tmp_path):
    """The whole reason tier is a first-class parameter: a run must never mix dev and prod."""
    config = load_config(write(tmp_path, MINIMAL))
    dev = resolve_tier(config, "dev")
    prod = resolve_tier(config, "prod")

    assert dev.url("firecloud") == "https://firecloud-orchestration.dsde-dev.broadinstitute.org/"
    assert prod.url("firecloud") == "https://api.firecloud.org/"
    assert "dsde-dev" in dev.url("rawls") and "dsde-prod" in prod.url("rawls")
    assert "dsde-dev" in dev.url("sam") and "dsde-prod" in prod.url("sam")


def test_the_service_url_table_matches_the_documented_tiers():
    """docs/services.md is the single source of truth; this is the table derived from it."""
    assert set(TIERS) == {"dev", "prod"}
    for tier in TIERS:
        assert set(SERVICE_URLS[tier]) == {"firecloud", "rawls", "sam"}
        assert all(url.startswith("https://") for url in SERVICE_URLS[tier].values())


def test_gen3_is_not_in_the_service_table():
    """It is the input's origin, not a service this tool calls. Where a signed URL may come from is a
    safety question (SIGNED_URL_ALLOWED_PREFIXES), not a routing one."""
    assert not any("gen3" in url for urls in SERVICE_URLS.values() for url in urls.values())


def test_an_unknown_tier_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="Unknown tier"):
        resolve_tier(load_config(write(tmp_path, MINIMAL)), "staging")


def test_a_known_but_unconfigured_tier_is_rejected(tmp_path):
    """Knowing prod's URLs is not the same as being set up to run against it."""
    data = {"default_tier": "dev", "tiers": {"dev": MINIMAL["tiers"]["dev"]}}
    with pytest.raises(ValueError, match="not configured"):
        resolve_tier(load_config(write(tmp_path, data)), "prod")


def test_a_missing_config_says_how_to_create_one(tmp_path):
    with pytest.raises(FileNotFoundError, match="config.example.yaml"):
        load_config(tmp_path / "nope.yaml")


def test_authorized_emails_include_the_primary_and_are_lowercased(tmp_path):
    """Multi-operator: the identity guard accepts any of them, and case must not decide access."""
    data = {
        "default_tier": "dev",
        "tiers": {
            "dev": {
                "email": "You@Test.firecloud.org",
                "terra_billing_project": "p",
                "authorized_users": ["Teammate@broadinstitute.org", "  "],
            }
        },
    }
    tier = resolve_tier(load_config(write(tmp_path, data)), "dev")
    assert tier.authorized_emails == {"you@test.firecloud.org", "teammate@broadinstitute.org"}


def test_an_absent_adc_file_means_the_ambient_identity(tmp_path):
    """So the same config works locally and wherever the runtime identity is provided for you."""
    assert resolve_tier(load_config(write(tmp_path, MINIMAL)), "dev").adc_credentials_file is None


def _dev_tier(tmp_path, **overrides):
    """Resolve a dev tier whose config carries ``overrides`` on top of the minimal one."""
    data = {
        "default_tier": "dev",
        "tiers": {"dev": {**MINIMAL["tiers"]["dev"], **overrides}},
    }
    return resolve_tier(load_config(write(tmp_path, data)), "dev")


def test_the_configured_workspace_name_reaches_the_resolved_tier(tmp_path):
    """It names the single workspace every run reuses, so it has to survive tier resolution."""
    tier = _dev_tier(tmp_path, default_workspace_name="ek_import_target")
    assert tier.default_workspace_name == "ek_import_target"


def test_an_omitted_workspace_name_means_generate_one_per_run(tmp_path):
    """The pre-existing behaviour stays reachable, so old configs keep working unchanged."""
    assert _dev_tier(tmp_path).default_workspace_name is None


@pytest.mark.parametrize(
    "bad",
    [
        "has spaces",
        "has/slash",  # would silently re-aim the URL path at a different workspace
        "has.dot",
        "x" * 101,
    ],
)
def test_an_illegal_workspace_name_is_rejected_at_load(tmp_path, bad):
    """This name aims a DELETE. A name Terra cannot hold must fail while reading config, not after
    the run has started resolving a destination."""
    with pytest.raises(pydantic.ValidationError, match="default_workspace_name"):
        _dev_tier(tmp_path, default_workspace_name=bad)


def test_an_empty_workspace_name_is_rejected_rather_than_treated_as_omitted(tmp_path):
    """A half-filled template must not quietly fall back to the generated name."""
    with pytest.raises(pydantic.ValidationError, match="Omit the key"):
        _dev_tier(tmp_path, default_workspace_name="   ")


def test_the_committed_example_config_actually_loads():
    """A template that does not parse is worse than none: the first thing a new operator does is
    copy it."""
    example = Path(__file__).parent.parent / "config" / "config.example.yaml"
    config = load_config(example)

    assert set(config.tiers) == {"dev", "prod"}
    for name in config.tiers:
        tier = resolve_tier(config, name)
        assert tier.url("firecloud")
        # The template demonstrates the key rather than omitting it -- an operator who copies it and
        # fills in the placeholders gets the reuse-one-workspace behaviour, not the legacy one.
        assert tier.default_workspace_name
