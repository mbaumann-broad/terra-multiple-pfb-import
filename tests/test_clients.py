"""HTTP clients, no network.

The base client's job is to make every call uniform: bearer injected, secrets redacted, token pinned
to its own host. The Firecloud client's job is to speak exactly the wire format the real Terra UI
speaks -- which is why the assertions here are about the *shape* of the request and the tolerance of
the response parsing, not about behaviour the pipeline owns.
"""

from __future__ import annotations

import json

import pytest
import requests

from terra_import_prototype.clients.base import BaseClient
from terra_import_prototype.clients.firecloud import (
    FirecloudClient,
    JobNotFound,
    OrchError,
    import_job_id,
    to_status,
)
from terra_import_prototype.clients.rawls import RawlsClient, workspace_auth_domains
from terra_import_prototype.safety import BearerDestinationError, SignedUrl

BUCKET = "https://gen3-biodatacatalyst-nhlbi-nih-gov-pfb-export.s3.amazonaws.com"
SECRET = "X-Amz-Signature=deadbeefcafe"
URL = SignedUrl(f"{BUCKET}/export.avro?{SECRET}")


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self.reason = "OK"
        self.headers: dict[str, str] = {}
        self._payload = payload
        self.text = text or (json.dumps(payload) if payload is not None else "")

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            err = requests.exceptions.HTTPError(f"HTTP {self.status_code}")
            err.response = self
            raise err


class FakeSession:
    """Records every request and replays a queue of responses."""

    def __init__(self, responses=None):
        self._responses = list(responses or [FakeResponse(200, {})])
        self.requests: list[dict] = []

    def request(self, method, url, params=None, json=None, headers=None, timeout=None):
        self.requests.append(
            {"method": method, "url": url, "params": params, "json": json, "headers": headers}
        )
        return self._responses.pop(0) if len(self._responses) > 1 else self._responses[0]


def client(cls, base="https://api.firecloud.org", responses=None, token="tok"):
    c = cls(base, lambda: token)
    c._session = FakeSession(responses)
    return c


# --- base client -------------------------------------------------------------


def test_the_bearer_is_injected_on_every_call():
    c = client(BaseClient)
    c.request("GET", "/api/thing")
    assert c._session.requests[0]["headers"]["Authorization"] == "Bearer tok"


def test_the_token_is_fetched_per_request_not_once():
    """A fan-out run polls for hours and outlives the ~1 h token expiry, so the provider must be
    consulted every time rather than a token captured at construction."""
    tokens = iter(["first", "second", "third"])
    c = BaseClient("https://api.firecloud.org", lambda: next(tokens))
    c._session = FakeSession()
    c.request("GET", "/a")
    c.request("GET", "/b")
    sent = [r["headers"]["Authorization"] for r in c._session.requests]
    assert sent == ["Bearer first", "Bearer second"]


def test_a_token_is_never_sent_to_another_host():
    """Reusable, broad access -- so it fails closed rather than warning."""
    c = client(BaseClient)
    with pytest.raises(BearerDestinationError):
        c.request("GET", "https://attacker.example.com/steal")
    assert c._session.requests == [], "the request must not be made at all"


def test_a_token_is_never_sent_over_plain_http():
    c = client(BaseClient, base="http://localhost:8080")
    with pytest.raises(BearerDestinationError):
        c.request("GET", "http://localhost:8080/api/thing")


def test_a_non_2xx_raises_by_default():
    c = client(BaseClient, responses=[FakeResponse(500, text="boom")])
    with pytest.raises(requests.exceptions.HTTPError):
        c.request("GET", "/api/thing")


def test_raise_for_status_can_be_turned_off_so_a_404_is_a_result():
    """The importJob status endpoint's 404 means 'not known yet', which must not end a run."""
    c = client(BaseClient, responses=[FakeResponse(404)])
    assert c.request("GET", "/api/thing", raise_for_status=False).status_code == 404


def test_the_logged_body_does_not_carry_the_signature(caplog):
    c = client(BaseClient)
    with caplog.at_level("DEBUG", logger="terra_import_prototype"):
        c.request("POST", "/api/thing", json_body={"url": URL.reveal(), "filetype": "pfb"})
    assert SECRET not in caplog.text
    assert "REDACTED" in caplog.text
    assert "export.avro" in caplog.text, "redaction must keep the path, or logs stop being useful"


# --- firecloud: the submit body ----------------------------------------------


def test_the_submit_body_matches_the_recorded_shape():
    """url + filetype + options, with options present even when null. See recorded.har."""
    fc = client(FirecloudClient, responses=[FakeResponse(202, {"jobId": "j1", "url": URL.reveal()})])
    fc.submit_import_job("ns", "ws", URL)

    sent = fc._session.requests[0]
    assert sent["method"] == "POST"
    assert sent["url"] == "https://api.firecloud.org/api/workspaces/ns/ws/importJob"
    assert sent["json"] == {"url": URL.reveal(), "filetype": "pfb", "options": None}
    assert "options" in sent["json"], "the real UI sends the key; do not omit it"


def test_workspace_names_are_url_encoded():
    fc = client(FirecloudClient, responses=[FakeResponse(202, {"jobId": "j1"})])
    fc.submit_import_job("my ns", "my/ws", URL)
    assert "my%20ns" in fc._session.requests[0]["url"]
    assert "my%2Fws" in fc._session.requests[0]["url"]


# --- firecloud: reading statuses ---------------------------------------------


def test_a_404_from_the_status_endpoint_is_job_not_found_not_an_error():
    fc = client(FirecloudClient, responses=[FakeResponse(404)])
    with pytest.raises(JobNotFound):
        fc.get_import_job("ns", "ws", "j1")


def test_a_500_from_the_status_endpoint_is_an_orch_error_carrying_the_code():
    fc = client(FirecloudClient, responses=[FakeResponse(503, text="unavailable")])
    with pytest.raises(OrchError) as excinfo:
        fc.get_import_job("ns", "ws", "j1")
    assert excinfo.value.status_code == 503


def test_the_list_endpoint_passes_running_only_as_a_string():
    fc = client(FirecloudClient, responses=[FakeResponse(200, [])])
    fc.list_import_jobs("ns", "ws", running_only=True)
    assert fc._session.requests[0]["params"] == {"running_only": "true"}


@pytest.mark.parametrize(
    "payload,expected",
    [
        ({"jobId": "j1", "status": "Done"}, ("j1", "Done")),
        ({"id": "j1", "state": "Done"}, ("j1", "Done")),
        ({"job_id": "j1", "status": "Error"}, ("j1", "Error")),
        ({"jobId": "j1", "url": "..."}, ("j1", "")),  # the 202: no status at all
    ],
    ids=["get", "alt-fields", "snake", "the-202"],
)
def test_status_parsing_tolerates_every_recorded_field_spelling(payload, expected):
    status = to_status(payload)
    assert (status.job_id, status.status) == expected


def test_a_missing_status_is_empty_not_invented():
    """The 202 has no status. Defaulting to 'Pending' *here* would hide that from the caller, which
    is the one place that knows whether an unknown state is normal."""
    assert to_status({"jobId": "j1"}).status == ""


def test_an_error_message_is_found_wherever_orchestration_put_it():
    assert to_status({"jobId": "j", "message": "bad"}).message == "bad"
    assert to_status({"jobId": "j", "result": {"errorMessage": "bad"}}).message == "bad"


def test_import_job_id_raises_rather_than_returning_empty():
    assert import_job_id({"jobId": "j1"}) == "j1"
    with pytest.raises(ValueError):
        import_job_id({"url": "..."})


# --- rawls -------------------------------------------------------------------


def test_create_workspace_requests_an_empty_auth_domain():
    """The import applies the real one -- a privileged operation we verify, never perform."""
    rawls = client(RawlsClient, base="https://rawls.dsde-prod.broadinstitute.org",
                   responses=[FakeResponse(201, {})])
    rawls.create_workspace("ns", "ws", description="d")

    body = rawls._session.requests[0]["json"]
    assert body["authorizationDomain"] == []
    assert body["enhancedBucketLogging"] is True
    assert body["attributes"] == {"description": "d"}


def test_entity_metadata_is_one_call_regardless_of_fan_out_width():
    rawls = client(
        RawlsClient,
        base="https://rawls.dsde-prod.broadinstitute.org",
        responses=[FakeResponse(200, {"subject": {"count": 5}})],
    )
    assert rawls.entity_type_metadata("ns", "ws") == {"subject": {"count": 5}}
    assert len(rawls._session.requests) == 1


def test_auth_domains_are_extracted_from_either_response_shape():
    assert workspace_auth_domains({"workspace": {"authorizationDomain": [{"membersGroupName": "g"}]}}) == ["g"]
    assert workspace_auth_domains({"authorizationDomain": []}) == []
    assert workspace_auth_domains({"workspace": {}}) == []
