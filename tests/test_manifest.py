"""Turning one signed URL into the list of PFB URLs to import, no network.

The rule these tests hold to: **a request that is not fully importable creates zero jobs.** Under
fan-out the alternative is worse than a clean refusal -- a manifest whose fourth entry is bad would
otherwise import three PFBs into a new workspace and then fail, leaving a half-populated workspace
that looks like a partial service failure rather than bad input.
"""

from __future__ import annotations

import json

import pytest

from terra_import_prototype.manifest import (
    DEFAULT_ALLOWED_HOST_PATTERNS,
    ImportRequestError,
    build_request,
    infer_kind,
)
from terra_import_prototype.safety import SignedUrl, SignedUrlProvenanceError

BUCKET = "https://gen3-biodatacatalyst-nhlbi-nih-gov-pfb-export.s3.amazonaws.com"
PREFIXES = (f"{BUCKET}/",)

AVRO = SignedUrl(f"{BUCKET}/export_2026-09-21T16%3A50%3A59.avro?X-Amz-Signature=deadbeef")
MANIFEST = SignedUrl(f"{BUCKET}/manifest.json?X-Amz-Signature=deadbeef")


def pfb(n: int) -> str:
    return f"{BUCKET}/export_{n}.avro?X-Amz-Signature=cafe{n}"


class FakeSession:
    """Serves one JSON body for the manifest GET, and records that no token was sent."""

    def __init__(self, payload, status=200):
        self._payload = payload
        self._status = status
        self.calls: list[dict] = []

    def get(self, url, timeout=None, allow_redirects=None):
        self.calls.append({"url": url, "allow_redirects": allow_redirects})
        body = self._payload if isinstance(self._payload, bytes) else json.dumps(self._payload).encode()
        return _Response(self._status, body)


class _Response:
    def __init__(self, status_code, content):
        self.status_code = status_code
        self.content = content


def build(payload, source=MANIFEST, **kwargs):
    kwargs.setdefault("tier_name", "dev")
    kwargs.setdefault("allowed_prefixes", PREFIXES)
    return build_request(source, session=FakeSession(payload), **kwargs)


# --- which kind of URL is this? ----------------------------------------------


def test_avro_and_manifest_are_inferred_from_the_path():
    assert infer_kind(AVRO) == "avro"
    assert infer_kind(MANIFEST) == "manifest"


def test_an_unrecognised_extension_asks_rather_than_guesses():
    """Importing a manifest as a PFB fails minutes later inside cWDS with a confusing parse error.
    'Tell me which one this is' is a better answer than a guess."""
    with pytest.raises(ImportRequestError, match="--kind"):
        infer_kind(SignedUrl(f"{BUCKET}/export?X-Amz-Signature=deadbeef"))


def test_an_explicit_kind_overrides_the_guess():
    """The extension is a heuristic; the operator's word wins."""
    request = build_request(
        AVRO, kind="avro", tier_name="dev", allowed_prefixes=PREFIXES, session=FakeSession(None)
    )
    assert request.kind == "avro" and len(request.urls) == 1


# --- the avro shape ----------------------------------------------------------


def test_an_avro_url_needs_no_fetch_at_all():
    session = FakeSession(None)
    request = build_request(
        AVRO, kind="avro", tier_name="dev", allowed_prefixes=PREFIXES, session=session
    )

    assert session.calls == [], "the avro flow must not dereference anything"
    assert request.urls == (AVRO,)
    assert request.raw is None
    assert not request.is_fan_out


# --- the manifest shape ------------------------------------------------------


@pytest.mark.parametrize(
    "payload",
    [
        {"urls": [pfb(1)]},
        {"files": [{"url": pfb(1)}]},
        {"url": pfb(1)},
        [pfb(1)],
        [{"url": pfb(1)}],
    ],
    ids=["urls", "files", "single", "bare-list", "bare-list-of-objects"],
)
def test_every_manifest_shape_the_ui_accepts_normalises_the_same_way(payload):
    """Ported from terra-ui rather than narrowed to the shape we happen to have seen: a manifest the
    real UI would import must import here."""
    request = build(payload)
    assert [u.reveal() for u in request.urls] == [pfb(1)]


def test_a_multi_entry_manifest_fans_out():
    request = build({"urls": [pfb(1), pfb(2), pfb(3)]})

    assert len(request) == 3
    assert request.is_fan_out
    assert request.kind == "manifest"
    assert request.description == "3 PFB(s) from manifest manifest.json"


def test_the_manifest_fetch_sends_no_token_and_follows_no_redirect():
    """The signature in the query string *is* the authorization. Adding a Terra bearer would hand a
    Terra credential to an S3 host; following a redirect would move the still-signed URL to a host
    nobody allow-listed."""
    session = FakeSession({"urls": [pfb(1)]})
    build_request(MANIFEST, tier_name="dev", allowed_prefixes=PREFIXES, session=session)

    assert session.calls[0]["allow_redirects"] is False
    # The fetch goes through requests directly, not the token-injecting BaseClient.


def test_a_manifest_that_is_not_json_says_so_and_suggests_the_fix():
    with pytest.raises(Exception, match="--kind avro"):
        build(b"this is not json {")


def test_an_entry_without_a_url_is_rejected_without_echoing_it():
    """A malformed manifest can still carry signed URLs; the error must not quote the entry back."""
    with pytest.raises(ImportRequestError) as excinfo:
        build({"urls": [pfb(1), {"not_a_url": f"{BUCKET}/secret.avro?X-Amz-Signature=leak"}]})
    assert "X-Amz-Signature" not in str(excinfo.value)
    assert "entry 1" in str(excinfo.value)


# --- validation --------------------------------------------------------------


def test_an_empty_manifest_creates_nothing():
    with pytest.raises(ImportRequestError, match="no URLs"):
        build({"urls": []})


def test_a_disallowed_host_is_rejected_client_side():
    """cWDS would reject it too, but the UI must not get that far -- and under fan-out the sibling
    jobs must not be created either.

    Exercised with the provenance allow-list unset, which is the configuration where the host
    allow-list is the remaining gate. With a tier's bucket prefixes configured the (stricter)
    provenance check fires first; that path is ``test_a_manifest_cannot_name_urls_its_own_bucket_
    could_not``.
    """
    with pytest.raises(ImportRequestError, match="allow-list"):
        build({"urls": ["https://evil.example.com/export.avro"]}, allowed_prefixes=())


def test_a_disallowed_host_is_rejected_on_the_avro_path_too():
    """No manifest, so the host allow-list is the only build-time gate."""
    with pytest.raises(ImportRequestError, match="allow-list"):
        build_request(
            SignedUrl("https://evil.example.com/export.avro?sig=x"),
            kind="avro",
            tier_name="dev",
            allowed_prefixes=(),
            session=FakeSession(None),
        )


def test_duplicate_urls_are_rejected():
    with pytest.raises(ImportRequestError, match="duplicate"):
        build({"urls": [pfb(1), pfb(1)]})


def test_the_fan_out_cap_is_enforced():
    """Not a Terra limit -- a self-imposed one. N importJobs is N concurrent translations inside a
    service shared with everyone else."""
    with pytest.raises(ImportRequestError, match="over the self-imposed max_worker"):
        build({"urls": [pfb(i) for i in range(5)]}, max_urls=4)


def test_all_problems_are_reported_at_once_not_just_the_first():
    """One message listing four bad URLs is one round-trip for the operator; four runs that each
    fail on the next URL is four."""
    with pytest.raises(ImportRequestError) as excinfo:
        build(
            {"urls": ["https://evil.example.com/a.avro", "ftp://nope/b.avro", pfb(1), pfb(1)]},
            allowed_prefixes=(),  # so _validate is the gate, not the earlier provenance check
        )
    message = str(excinfo.value)
    assert "3 problem(s)" in message
    assert "allow-list" in message and "not an http(s) URL" in message and "duplicate" in message


def test_the_recorded_gen3_export_host_is_on_the_allow_list():
    """Virtual-hosted-style S3, which a path-style-only allow-list does not match. Without this
    pattern every real BDC export is rejected before a job is created."""
    host_patterns = DEFAULT_ALLOWED_HOST_PATTERNS
    request = build_request(
        AVRO,
        kind="avro",
        tier_name="dev",
        allowed_prefixes=PREFIXES,
        allowed_host_patterns=host_patterns,
        session=FakeSession(None),
    )
    assert len(request.urls) == 1


# --- fan-out containment -----------------------------------------------------


def test_a_manifest_cannot_name_urls_its_own_bucket_could_not():
    """The allow-list must gate the manifest's *contents*, not just the index.

    Without this, a manifest fetched from the allow-listed bucket could name N URLs pointing
    anywhere and the fan-out would dutifully hand each one to Terra.
    """
    with pytest.raises(SignedUrlProvenanceError, match="not from an allow-listed source"):
        build({"urls": [pfb(1), "https://attacker.example.com/export.avro"]})


def test_provenance_is_checked_before_anything_is_dispatched():
    """The check runs inside build_request, so a bad manifest never reaches the fan-out at all."""
    with pytest.raises(SignedUrlProvenanceError):
        build({"urls": ["https://attacker.example.com/export.avro"]})


def test_an_off_list_manifest_url_is_never_even_fetched():
    """Provenance is checked before the GET, so this cannot be used to make an outbound request to
    an arbitrary host."""
    session = FakeSession({"urls": [pfb(1)]})
    off_list = SignedUrl("https://attacker.example.com/manifest.json?sig=x")

    with pytest.raises(SignedUrlProvenanceError):
        build_request(off_list, tier_name="dev", allowed_prefixes=PREFIXES, session=session)

    assert session.calls == [], "an off-list URL must not be dereferenced"
