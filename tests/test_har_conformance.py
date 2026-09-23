"""The recorded import flow, replayed against this tool's own code. No network.

``recorded.har`` is a real browser capture of Terra importing one pre-signed Gen3 BDC export
(prod, 2026-09-21): the UI is handed a signed Avro URL, POSTs a single ``importJob``, and polls the
returned jobId to ``Done``.

It is the only evidence in this repository of what the production wire format actually is, so these
tests **read it** rather than restating it. A restated contract drifts from the recording and nobody
notices; a test that parses the capture fails the moment the two disagree. This is the same
principle the reference project states as "mirror the interactive UI's API calls as closely as is
reasonably possible -- captured real traffic is the reference of record, more reliable than
inferring intent from the OpenAPI specs alone".

Nothing here writes to the HAR. It is a private capture and is not required to be present: without
it these tests skip, and the same behaviours are covered from constants in ``test_clients.py``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from terra_import_prototype.clients.firecloud import FirecloudClient, JobStatus, to_status
from terra_import_prototype.manifest import build_request
from terra_import_prototype.models import KNOWN_STATUSES, DispatchPolicy, is_terminal
from terra_import_prototype.pipeline import ImportFanOut
from terra_import_prototype.safety import SIGNED_URL_ALLOWED_PREFIXES, SignedUrl, verify_pfb_handoff

HAR_PATH = Path(__file__).parent.parent / "recorded.har"
NS, WS = "biodata-catalyst", "ek_sept_17_1000_gen"


# --- reading the recording ---------------------------------------------------


@pytest.fixture(scope="module")
def har() -> list[dict[str, Any]]:
    if not HAR_PATH.exists():
        pytest.skip(f"{HAR_PATH.name} not present; the recorded flow cannot be replayed")
    return json.loads(HAR_PATH.read_text())["log"]["entries"]


def _body(entry: dict[str, Any]) -> Any:
    text = entry["response"]["content"].get("text")
    return json.loads(text) if text else None


@pytest.fixture(scope="module")
def submit(har) -> dict[str, Any]:
    """The single recorded importJob POST."""
    posts = [
        e
        for e in har
        if e["request"]["method"] == "POST" and e["request"]["url"].endswith("/importJob")
    ]
    assert len(posts) == 1, f"expected one recorded importJob POST, found {len(posts)}"
    return posts[0]


@pytest.fixture(scope="module")
def polls(har, submit) -> list[dict[str, Any]]:
    """The recorded per-job status GETs, in order."""
    job_id = _body(submit)["jobId"]
    return [
        e
        for e in har
        if e["request"]["method"] == "GET" and e["request"]["url"].endswith(f"/importJob/{job_id}")
    ]


@pytest.fixture(scope="module")
def recorded_url(submit) -> SignedUrl:
    return SignedUrl(json.loads(submit["request"]["postData"]["text"])["url"])


# --- what the capture establishes about the flow -----------------------------


def test_the_recorded_flow_fetches_no_manifest(har, recorded_url):
    """The premise of the ``avro`` shape: there is nothing to dereference.

    If the recording contained a manifest fetch, ``build_request`` skipping it for an Avro URL would
    be wrong. It does not: the signed URL appears in exactly one place -- the body of the POST that
    consumes it -- and no request in the trace is for a manifest.
    """
    carriers = [e for e in har if recorded_url.reveal() in e["request"]["url"]]
    assert carriers == [], "the signed URL was fetched by the browser, not only posted"
    assert not [e for e in har if "manifest" in e["request"]["url"].lower()]


def test_the_recorded_export_comes_from_the_allow_listed_bucket(recorded_url):
    """Pins ``SIGNED_URL_ALLOWED_PREFIXES`` to a real BDC export.

    This is the test that catches an allow-list written for path-style S3: the recorded host is
    virtual-hosted-style (bucket in the host), and an allow-list that misses it refuses every real
    BDC export before a job is created.
    """
    verify_pfb_handoff(
        recorded_url,
        tier_name="prod",
        expected_firecloud_host="api.firecloud.org",
        destination_host="api.firecloud.org",
        allowed_prefixes=SIGNED_URL_ALLOWED_PREFIXES["prod"],
    )


def test_the_recorded_url_builds_an_importable_request(recorded_url):
    """End to end through the host allow-list and the kind heuristic, on the real URL."""
    request = build_request(
        recorded_url,
        tier_name="prod",
        allowed_prefixes=SIGNED_URL_ALLOWED_PREFIXES["prod"],
    )
    assert request.kind == "avro", "the recorded path ends in .avro"
    assert request.urls == (recorded_url,)
    assert not request.is_fan_out


def test_our_submit_body_matches_the_recorded_one_exactly(submit, recorded_url):
    """Built by FirecloudClient, compared to what the browser actually sent.

    ``options: null`` is the part worth pinning: it is easy to treat as omittable and it is not what
    any recorded client sends.
    """
    recorded = json.loads(submit["request"]["postData"]["text"])
    sent: dict[str, Any] = {}

    class CapturingSession:
        def request(self, method, url, params=None, json=None, headers=None, timeout=None):
            sent.update({"method": method, "url": url, "body": json})
            return _FakeResponse(202, _body(submit))

    client = FirecloudClient("https://api.firecloud.org", lambda: "tok")
    client._session = CapturingSession()
    client.submit_import_job(NS, WS, recorded_url, filetype=recorded["filetype"])

    assert sent["body"] == recorded
    assert sent["url"] == submit["request"]["url"]
    assert sent["method"] == "POST"


# --- the responses the tool has to parse -------------------------------------


def test_the_202_carries_a_job_id_but_no_status(submit):
    """Which is why the fan-out seeds ``Pending`` itself rather than trusting the 202.

    A client that read ``status`` off the submit response would record an empty string as the job's
    first observed state, and every history assertion would start from a lie.
    """
    status = to_status(_body(submit))

    assert status.job_id
    assert status.status == ""


def test_every_recorded_status_is_one_this_tool_knows(polls):
    """An unrecognised status is a reported failure, not a skip -- including here."""
    observed = {to_status(_body(e)).status for e in polls}

    assert observed <= KNOWN_STATUSES, f"unknown statuses in the recording: {observed - KNOWN_STATUSES}"
    assert "Done" in observed, "the recorded job never finished; the trace is unusable"


def test_the_recorded_run_reaches_a_terminal_state_exactly_once(polls):
    statuses = [to_status(_body(e)).status for e in polls]
    assert [s for s in statuses if is_terminal(s)] == ["Done"], statuses


def test_the_recording_polls_the_list_endpoint_too(har):
    """Both poll strategies are on the single-import path in production, which is why both are kept
    and why a test compares them."""
    assert [e for e in har if e["request"]["url"].endswith("/importJob?running_only=true")]


# --- replaying it through the fan-out ----------------------------------------


class RecordedOrch:
    """Orchestration, played back from the recording.

    The submit returns the recorded 202; each poll returns the next recorded status and then holds
    at the last one, so a poller that asks more often than the browser did still sees the same
    sequence.
    """

    base_url = "https://api.firecloud.org"

    def __init__(self, submit_body: dict[str, Any], statuses: list[str]):
        self._submit = submit_body
        self._statuses = statuses
        self._cursor = -1
        self.submitted: list[tuple[str, str, str, str, Any]] = []

    def submit_import_job(self, namespace, name, signed_url, *, filetype="pfb", options=None):
        self.submitted.append((namespace, name, signed_url.reveal(), filetype, options))
        return self._submit

    def get_import_job(self, namespace, name, job_id):
        self._cursor = min(self._cursor + 1, len(self._statuses) - 1)
        return JobStatus(job_id=job_id, status=self._statuses[self._cursor])

    def list_import_jobs(self, namespace, name, *, running_only=False):
        status = self.get_import_job(namespace, name, self._submit["jobId"])
        return [] if running_only and is_terminal(status.status) else [status]


def replay(submit, polls, **kwargs):
    orch = RecordedOrch(_body(submit), [to_status(_body(e)).status for e in polls])
    fanout = ImportFanOut(
        orch,
        NS,
        WS,
        policy=DispatchPolicy(mode="parallel", max_worker=3),
        poll_interval_s=0,
        sleep=lambda _s: None,
        **kwargs,
    )
    url = SignedUrl(json.loads(submit["request"]["postData"]["text"])["url"])
    return orch, fanout.wait(fanout.submit([url]))


def test_the_recorded_run_replays_to_the_same_outcome(submit, polls, recorded_url):
    """One submit, poll to Done, one job in the result.

    This is the test that fails if the fan-out grows a step the real UI does not take, or drops one
    it does.
    """
    orch, result = replay(submit, polls)

    assert orch.submitted == [(NS, WS, recorded_url.reveal(), "pfb", None)]
    assert len(result.jobs) == 1
    assert result.all_succeeded
    assert not result.unknown_statuses and not result.forbidden_statuses

    observed = [s.status for s in result.jobs[0].history]
    assert observed[0] == "Pending", "Pending is ours; the 202 carried no status"
    assert observed[-1] == "Done"


def test_the_recorded_run_replays_the_same_way_under_list_polling(submit, polls):
    _orch, result = replay(submit, polls, poll_strategy="list")
    assert result.all_succeeded


def test_the_replayed_report_never_carries_the_signature(submit, polls, recorded_url):
    """The realest possible check that a run's output is safe to paste into a ticket."""
    _orch, result = replay(submit, polls)
    signature = recorded_url.reveal().split("X-Amz-Signature=")[-1]

    assert signature not in result.render()
    assert recorded_url.filename in result.render()


class _FakeResponse:
    def __init__(self, status_code: int, payload: Any):
        self.status_code = status_code
        self.reason = "Accepted"
        self.headers: dict[str, str] = {}
        self._payload = payload
        self.text = json.dumps(payload)

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        pass
