"""Signed-URL containment and hand-off verification, no network.

The pre-signed URL is the only credential in this flow and it travels through the whole pipeline, so
the tests that matter most are the ones asserting it *cannot* leak: not that the code currently
happens not to log it, but that the type makes logging it impossible by the obvious routes.
"""

from __future__ import annotations

import json

import pytest

from terra_import_prototype.safety import (
    ManifestFetchError,
    SignedUrl,
    SignedUrlDestinationError,
    SignedUrlProvenanceError,
    assert_uniform_provenance,
    fetch_signed_json,
    verify_pfb_handoff,
)

BUCKET = "https://gen3-biodatacatalyst-nhlbi-nih-gov-pfb-export.s3.amazonaws.com"
PREFIXES = (f"{BUCKET}/",)
SECRET = "X-Amz-Signature=4af013ea333253f1e3f766d6ac36e0702cd07155"
URL = SignedUrl(f"{BUCKET}/export_2026-09-21T16%3A50%3A59.avro?{SECRET}")


# --- containment -------------------------------------------------------------


@pytest.mark.parametrize(
    "render",
    [repr, str, lambda u: f"{u}", lambda u: "{}".format(u), lambda u: f"job failed for {u}"],
    ids=["repr", "str", "fstring", "format", "interpolated"],
)
def test_the_signature_survives_no_rendering_path(render):
    """Every way a URL usually reaches a log line, an exception message, or a traceback."""
    assert SECRET not in render(URL)
    assert "redacted" in render(URL)


def test_the_safe_location_is_present_so_the_redaction_stays_useful():
    """Redaction that hid everything would just move the debugging cost elsewhere: the host and path
    are what an operator needs to tell which export failed."""
    assert URL.location == f"{BUCKET}/export_2026-09-21T16%3A50%3A59.avro"
    assert "?" not in URL.location
    assert URL.host == "gen3-biodatacatalyst-nhlbi-nih-gov-pfb-export.s3.amazonaws.com"
    assert URL.filename == "export_2026-09-21T16%3A50%3A59.avro"


def test_it_is_not_json_serializable():
    """So it cannot be returned in a result dict or posted in a body by accident."""
    with pytest.raises(TypeError):
        json.dumps({"url": URL})


def test_reveal_is_the_only_way_out():
    assert URL.reveal().endswith(SECRET)


# --- hand-off verification ---------------------------------------------------


def handoff(destination="api.firecloud.org", expected="api.firecloud.org", prefixes=PREFIXES):
    verify_pfb_handoff(
        URL,
        tier_name="prod",
        expected_firecloud_host=expected,
        destination_host=destination,
        allowed_prefixes=prefixes,
    )


def test_a_matching_destination_and_source_passes():
    handoff()


def test_a_cross_tier_destination_is_refused():
    """A prod export URL must not be deliverable to dev Orchestration, or vice versa."""
    with pytest.raises(SignedUrlDestinationError):
        handoff(destination="firecloud-orchestration.dsde-dev.broadinstitute.org")


def test_an_off_list_source_is_refused():
    with pytest.raises(SignedUrlProvenanceError):
        handoff(prefixes=("https://some-other-bucket.s3.amazonaws.com/",))


def test_a_sibling_bucket_cannot_satisfy_the_prefix():
    """The trailing slash anchors the check at the bucket boundary."""
    attacker = SignedUrl(
        "https://gen3-biodatacatalyst-nhlbi-nih-gov-pfb-export-attacker.s3.amazonaws.com/x.avro?s=1"
    )
    with pytest.raises(SignedUrlProvenanceError):
        verify_pfb_handoff(
            attacker,
            tier_name="prod",
            expected_firecloud_host="api.firecloud.org",
            destination_host="api.firecloud.org",
            allowed_prefixes=PREFIXES,
        )


def test_an_unset_allow_list_warns_but_does_not_block(caplog):
    """A tier with no prefixes configured still runs -- the destination check is the one that must
    never be optional -- but it says so, loudly."""
    with caplog.at_level("WARNING"):
        handoff(prefixes=())
    assert "not verified" in caplog.text


# --- fan-out containment -----------------------------------------------------


def test_uniform_provenance_accepts_a_wholly_on_list_manifest():
    assert_uniform_provenance(
        [URL, SignedUrl(f"{BUCKET}/b.avro?s=2")],
        tier_name="prod",
        allowed_prefixes=PREFIXES,
        source="manifest.json",
    )


def test_uniform_provenance_names_the_offenders_and_the_totals():
    """An operator needs to know *which* of 18 entries is wrong, and how many."""
    urls = [URL, SignedUrl("https://attacker.example.com/x.avro?s=1")]
    with pytest.raises(SignedUrlProvenanceError) as excinfo:
        assert_uniform_provenance(
            urls, tier_name="prod", allowed_prefixes=PREFIXES, source="manifest.json"
        )
    message = str(excinfo.value)
    assert "1 of 2" in message
    assert "attacker.example.com" in message
    assert SECRET not in message, "the offender's signature must not be echoed"


def test_uniform_provenance_truncates_a_long_offender_list():
    urls = [SignedUrl(f"https://attacker.example.com/{i}.avro?s=1") for i in range(9)]
    with pytest.raises(SignedUrlProvenanceError, match=r"\.\.\."):
        assert_uniform_provenance(
            urls, tier_name="prod", allowed_prefixes=PREFIXES, source="manifest.json"
        )


# --- the manifest dereference ------------------------------------------------


class FakeSession:
    def __init__(self, status=200, content=b'{"urls": []}'):
        self.status = status
        self.content = content
        self.calls: list[dict] = []

    def get(self, url, timeout=None, allow_redirects=None):
        self.calls.append(
            {"url": url, "timeout": timeout, "allow_redirects": allow_redirects}
        )
        return type("R", (), {"status_code": self.status, "content": self.content})()


def test_the_fetch_reveals_the_url_but_sends_no_headers_of_its_own():
    session = FakeSession()
    fetch_signed_json(URL, tier_name="prod", allowed_prefixes=PREFIXES, session=session)

    call = session.calls[0]
    assert call["url"] == URL.reveal(), "the signature IS the authorization"
    assert call["allow_redirects"] is False


def test_an_off_list_url_is_refused_before_any_request_is_made():
    session = FakeSession()
    off_list = SignedUrl("https://attacker.example.com/manifest.json?s=1")

    with pytest.raises(SignedUrlProvenanceError):
        fetch_signed_json(off_list, tier_name="prod", allowed_prefixes=PREFIXES, session=session)

    assert session.calls == []


def test_a_403_is_reported_as_a_probable_expiry():
    """The single most common real failure, and the one whose error message saves the most time."""
    with pytest.raises(ManifestFetchError, match="expired"):
        fetch_signed_json(
            URL,
            tier_name="prod",
            allowed_prefixes=PREFIXES,
            session=FakeSession(status=403, content=b""),
        )


def test_a_redirect_is_refused_rather_than_followed():
    with pytest.raises(ManifestFetchError, match="redirect"):
        fetch_signed_json(
            URL,
            tier_name="prod",
            allowed_prefixes=PREFIXES,
            session=FakeSession(status=302, content=b""),
        )


def test_the_error_never_carries_the_signature():
    with pytest.raises(ManifestFetchError) as excinfo:
        fetch_signed_json(
            URL,
            tier_name="prod",
            allowed_prefixes=PREFIXES,
            session=FakeSession(status=500, content=b""),
        )
    assert SECRET not in str(excinfo.value)
    assert URL.location in str(excinfo.value)
