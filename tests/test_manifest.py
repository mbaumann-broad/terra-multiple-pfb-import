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
    MAX_URLS,
    ImportRequestError,
    _extract_urls,
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


# --- manifest normalisation (_extract_urls, directly) ------------------------
#
# build_request covers these shapes end-to-end above, but only for the ones that survive validation.
# _extract_urls is tested directly as well because it is the port of terra-ui's normalisation and its
# job is to be *permissive in the same ways the UI is* -- key precedence, the bare-list forms, an
# empty result rather than a raise -- none of which is observable through build_request, which
# rejects the empty and mixed cases before they are returned.


@pytest.mark.parametrize(
    "raw",
    [
        {"urls": [pfb(1), pfb(2)]},
        {"files": [pfb(1), pfb(2)]},
        {"sources": [pfb(1), pfb(2)]},
        {"pfbs": [pfb(1), pfb(2)]},
        [pfb(1), pfb(2)],
        [{"url": pfb(1)}, {"url": pfb(2)}],
        {"urls": [{"url": pfb(1)}, pfb(2)]},
    ],
    ids=["urls", "files", "sources", "pfbs", "bare-list", "list-of-objects", "mixed-entries"],
)
def test_extract_urls_flattens_every_accepted_shape(raw):
    """Strings and ``{"url": ...}`` objects are interchangeable, under any of the four list keys or
    at the root. The result is always a flat list of raw strings, in manifest order."""
    assert _extract_urls(raw, MANIFEST) == [pfb(1), pfb(2)]


def test_extract_urls_accepts_the_single_file_shape_gen3_emits():
    """``{"url": ...}`` with no list key at all: the object *is* the one entry."""
    assert _extract_urls({"url": pfb(1)}, MANIFEST) == [pfb(1)]


def test_extract_urls_prefers_the_first_key_it_recognises():
    """Key precedence is urls > files > sources > pfbs, and a recognised list key wins over a
    sibling top-level 'url'. Pinned because a manifest carrying two of them must normalise the same
    way here as in the UI, not the way dict ordering happens to fall."""
    raw = {"urls": [pfb(1)], "files": [pfb(2)], "sources": [pfb(3)], "url": pfb(4)}
    assert _extract_urls(raw, MANIFEST) == [pfb(1)]

    assert _extract_urls({"files": [pfb(2)], "pfbs": [pfb(3)]}, MANIFEST) == [pfb(2)]


def test_extract_urls_preserves_order_and_duplicates():
    """Normalisation does not dedupe or sort -- _validate is what rejects duplicates, and it can
    only report them if they survive to here."""
    raw = {"urls": [pfb(2), pfb(1), pfb(2)]}
    assert _extract_urls(raw, MANIFEST) == [pfb(2), pfb(1), pfb(2)]


@pytest.mark.parametrize(
    "raw",
    [{}, [], {"urls": []}, {"other": "field"}],
    ids=["empty-object", "empty-list", "empty-url-list", "no-recognised-key"],
)
def test_extract_urls_returns_empty_rather_than_raising(raw):
    """An empty result is a *validation* failure, not a parse failure: _validate turns it into the
    'yielded no URLs' message alongside any other problems, so the operator gets one round-trip."""
    assert _extract_urls(raw, MANIFEST) == []


@pytest.mark.parametrize(
    "raw, expected_type",
    [("a string", "str"), (7, "int"), (None, "NoneType"), (True, "bool")],
    ids=["str", "int", "null", "bool"],
)
def test_extract_urls_rejects_a_root_that_is_neither_object_nor_list(raw, expected_type):
    with pytest.raises(ImportRequestError) as excinfo:
        _extract_urls(raw, MANIFEST)
    message = str(excinfo.value)
    assert "root must be an object or a list" in message
    assert expected_type in message


@pytest.mark.parametrize(
    "raw, expected_type",
    [({"urls": "not-a-list"}, "str"), ({"files": {"url": pfb(1)}}, "dict"), ({"pfbs": 3}, "int")],
    ids=["string", "object", "int"],
)
def test_extract_urls_rejects_a_recognised_key_that_is_not_a_list(raw, expected_type):
    """``{"urls": "<one url>"}`` is a shape the UI does not accept either. Naming the type it got is
    what lets the operator fix the manifest without a second run."""
    with pytest.raises(ImportRequestError) as excinfo:
        _extract_urls(raw, MANIFEST)
    message = str(excinfo.value)
    assert "expected a list of URLs" in message
    assert expected_type in message


@pytest.mark.parametrize(
    "entry",
    [
        {"not_a_url": f"{BUCKET}/secret.avro?X-Amz-Signature=leak"},
        {"url": None},
        {"url": 7},
        {"url": [f"{BUCKET}/secret.avro?X-Amz-Signature=leak"]},
        [f"{BUCKET}/secret.avro?X-Amz-Signature=leak"],
        None,
        7,
    ],
    ids=["wrong-key", "null-url", "int-url", "list-url", "list-entry", "null-entry", "int-entry"],
)
def test_extract_urls_rejects_an_entry_without_a_url_string_without_echoing_it(entry):
    """A 'url' that is not a string is as unusable as a missing one -- and a malformed manifest can
    still carry signed URLs, so the error names the index and never quotes the entry back."""
    with pytest.raises(ImportRequestError) as excinfo:
        _extract_urls({"urls": [pfb(1), entry]}, MANIFEST)
    message = str(excinfo.value)
    assert "entry 1 has no 'url' string" in message
    assert "X-Amz-Signature" not in message


def test_extract_urls_errors_name_the_manifest_by_location_not_by_signed_url():
    """Every message in this function interpolates ``source``; ``.location`` is the redacted form,
    and getting that wrong would put a signed URL into an exception that the CLI prints."""
    with pytest.raises(ImportRequestError) as excinfo:
        _extract_urls("nope", MANIFEST)
    message = str(excinfo.value)
    assert MANIFEST.location in message
    assert "X-Amz-Signature" not in message


def test_extract_urls_does_not_validate_hosts_or_count():
    """Normalisation is only normalisation. The host allow-list, the fan-out cap and provenance are
    _validate's and safety's jobs; doing any of them here would split the gate across two places and
    let one drift."""
    off_list = ["https://attacker.example.com/export.avro", "ftp://nope/b.avro"]
    assert _extract_urls({"urls": off_list}, MANIFEST) == off_list
    assert len(_extract_urls([pfb(i) for i in range(MAX_URLS + 5)], MANIFEST)) == MAX_URLS + 5
