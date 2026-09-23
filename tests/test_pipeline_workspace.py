"""Tests for the pipeline's workspace-scoped behaviour, no network.

Two halves, both about the one workspace a run creates and then imports into:

1. **createWorkspace resilience** (``_create_workspace_resilient``) -- Rawls can time out
   client-side, or return a 5xx from a proxy in front of it, even though the workspace *was* created
   server-side. The helper turns that into a successful run by GET-ing the workspace and adopting
   it, while still failing fast when the create genuinely did not happen or the existing workspace
   is not the one we created. It matters more here than in a single-import tool: the workspace is
   created once and then N jobs fan out into it, so abandoning one that actually exists costs the
   whole run.

2. **The fan-out** (``ImportFanOut`` / ``submit_import_jobs``) -- one
   ``firecloud.submit_import_job`` per PFB URL, into that same workspace. This is the behaviour the
   project exists to study, so it is tested for the properties that are easy to get wrong and
   invisible in a happy-path run: N URLs produce N distinct jobIds, a rejected submit does not take
   its siblings down, the concurrency max_worker really bounds jobs in flight rather than just POSTs, a
   hung job does not hang the run, and status history records transitions rather than only the
   final state.
"""

from __future__ import annotations

import itertools
import threading
import types

import pytest
import requests

from terra_import_prototype import pipeline
from terra_import_prototype.clients.firecloud import JobNotFound, JobStatus, OrchError
from terra_import_prototype.config import SERVICE_URLS, ResolvedTier
from terra_import_prototype.models import (
    TIMEOUT,
    DispatchPolicy,
    ImportRequest,
    is_terminal,
)
from terra_import_prototype.pipeline import (
    ImportFanOut,
    _create_workspace_resilient,
    submit_import_jobs,
)
from terra_import_prototype.safety import SignedUrl, SignedUrlProvenanceError

NS, WS = "ns", "ws"

#: Real BDC export prefix, so the provenance allow-list is exercised rather than bypassed.
BUCKET = "https://gen3-biodatacatalyst-nhlbi-nih-gov-pfb-export.s3.amazonaws.com"


def signed(name: str) -> SignedUrl:
    return SignedUrl(f"{BUCKET}/{name}?X-Amz-Signature=deadbeef")


URLS = [signed(f"export_{i}.avro") for i in range(5)]


# =============================================================================
# 1. createWorkspace resilience
# =============================================================================


def _http_error(status: int) -> requests.exceptions.HTTPError:
    err = requests.exceptions.HTTPError(f"HTTP {status}")
    err.response = types.SimpleNamespace(status_code=status)
    return err


def _ws(domains):
    """A get_workspace response with the given auth-domain group names."""
    return {"workspace": {"authorizationDomain": [{"membersGroupName": d} for d in domains]}}


class FakeRawls:
    """Stand-in for RawlsClient: create_workspace raises a configured error; get_workspace replays
    a list of outcomes (each an exception to raise or a dict to return)."""

    def __init__(self, *, create_exc=None, get_results=None, entities=None):
        self._create_exc = create_exc
        self._get_results = list(get_results or [])
        self._entities = entities or {}
        self.create_calls = 0
        self.get_calls = 0

    def create_workspace(self, namespace, name, *, description=""):
        self.create_calls += 1
        if self._create_exc is not None:
            raise self._create_exc
        return {"name": name}

    def get_workspace(self, namespace, name, fields=None, timeout=None):
        self.get_calls += 1
        result = self._get_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def entity_type_metadata(self, namespace, name):
        return self._entities


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(pipeline.time, "sleep", lambda _s: None)


def test_normal_path_no_adopt():
    rawls = FakeRawls()
    _create_workspace_resilient(rawls, NS, WS, description="d")
    assert rawls.create_calls == 1 and rawls.get_calls == 0  # created cleanly; no GET/adopt


def test_adopts_when_timeout_but_workspace_exists():
    rawls = FakeRawls(
        create_exc=requests.exceptions.ReadTimeout("read timed out"),
        get_results=[_ws([])],  # exists, empty auth domain (the import applies the real one)
    )
    _create_workspace_resilient(rawls, NS, WS, description="d")  # returns == adopted
    assert rawls.get_calls == 1


def test_adopts_after_transient_404():
    rawls = FakeRawls(
        create_exc=requests.exceptions.ConnectionError("connection reset"),
        get_results=[_http_error(404), _ws([])],  # not visible yet, then appears
    )
    _create_workspace_resilient(rawls, NS, WS, description="d")
    assert rawls.get_calls == 2


def test_raises_when_workspace_never_created():
    rawls = FakeRawls(
        create_exc=requests.exceptions.ReadTimeout("read timed out"),
        get_results=[_http_error(404)] * pipeline.WORKSPACE_ADOPT_ATTEMPTS,
    )
    with pytest.raises(RuntimeError, match="could not be confirmed created"):
        _create_workspace_resilient(rawls, NS, WS, description="d")
    assert rawls.get_calls == pipeline.WORKSPACE_ADOPT_ATTEMPTS


def test_refuses_to_adopt_workspace_with_auth_domain():
    # A workspace we just created must have an empty auth domain; a non-empty one isn't ours, and
    # fanning N imports into someone else's controlled-access workspace is the worst outcome here.
    rawls = FakeRawls(
        create_exc=requests.exceptions.ReadTimeout("read timed out"),
        get_results=[_ws(["AUTH_some_controlled_study"])],
    )
    with pytest.raises(pipeline.SafetyError, match="non-empty auth domain"):
        _create_workspace_resilient(rawls, NS, WS, description="d")


def test_409_name_exists_fails_fast_not_adopted():
    # A 409 (name already exists) is not a timeout -> fail fast, never GET/adopt.
    rawls = FakeRawls(create_exc=_http_error(409))
    with pytest.raises(requests.exceptions.HTTPError):
        _create_workspace_resilient(rawls, NS, WS, description="d")
    assert rawls.get_calls == 0


def test_definitive_4xx_get_error_propagates():
    # A definitive 4xx (e.g. 403) during the adopt-check propagates -- don't mask it (5xx would retry).
    rawls = FakeRawls(
        create_exc=requests.exceptions.ReadTimeout("read timed out"),
        get_results=[_http_error(403)],
    )
    with pytest.raises(requests.exceptions.HTTPError):
        _create_workspace_resilient(rawls, NS, WS, description="d")


def test_retries_adopt_get_on_transient_error():
    rawls = FakeRawls(
        create_exc=requests.exceptions.ReadTimeout("read timed out"),
        get_results=[requests.exceptions.ConnectionError("connection reset"), _ws([])],
    )
    _create_workspace_resilient(rawls, NS, WS, description="d")
    assert rawls.get_calls == 2


def test_non_timeout_create_error_fails_fast():
    rawls = FakeRawls(create_exc=requests.exceptions.RequestException("unexpected"))
    with pytest.raises(requests.exceptions.RequestException):
        _create_workspace_resilient(rawls, NS, WS, description="d")
    assert rawls.get_calls == 0


def test_adopts_on_gateway_502():
    # A 502 Bad Gateway from createWorkspace (workspace created server-side) -> confirm-or-adopt.
    rawls = FakeRawls(create_exc=_http_error(502), get_results=[_ws([])])
    _create_workspace_resilient(rawls, NS, WS, description="d")
    assert rawls.get_calls == 1


def test_gateway_5xx_but_never_created_raises():
    rawls = FakeRawls(
        create_exc=_http_error(503),
        get_results=[_http_error(404)] * pipeline.WORKSPACE_ADOPT_ATTEMPTS,
    )
    with pytest.raises(RuntimeError, match="could not be confirmed created"):
        _create_workspace_resilient(rawls, NS, WS, description="d")


def test_4xx_create_error_fails_fast_not_adopted():
    rawls = FakeRawls(create_exc=_http_error(403))
    with pytest.raises(requests.exceptions.HTTPError):
        _create_workspace_resilient(rawls, NS, WS, description="d")
    assert rawls.get_calls == 0


def test_retries_adopt_get_on_transient_5xx():
    rawls = FakeRawls(create_exc=_http_error(502), get_results=[_http_error(502), _ws([])])
    _create_workspace_resilient(rawls, NS, WS, description="d")
    assert rawls.get_calls == 2


# =============================================================================
# 2. The fan-out: N x firecloud.submit_import_job into that one workspace
# =============================================================================


class FakeFirecloud:
    """Stand-in for FirecloudClient with scripted status sequences per URL.

    Either status endpoint advances a job's script by one step, so the per-job and list pollers
    drive the same state machine and can be compared. ``peak_in_flight`` is recomputed on every
    interaction, which is what lets a test assert the concurrency max_worker from the outside -- no
    instrumentation inside the code under test.
    """

    def __init__(self, base_url: str, *, scripts=None, default=None, reject=()):
        self.base_url = base_url.rstrip("/")
        self._scripts = scripts or {}
        self._default = default or ["Pending", "Translating", "ReadyForUpsert", "Done"]
        self._reject = set(reject)
        self._ids = itertools.count(1)
        self._script_for: dict[str, list[str]] = {}
        self._cursor: dict[str, int] = {}
        self._lock = threading.Lock()
        #: Every submit call, in order: (namespace, name, raw url, filetype, options).
        self.submitted: list[tuple[str, str, str, str, object]] = []
        self.peak_in_flight = 0

    def submit_import_job(self, namespace, name, signed_url, *, filetype="pfb", options=None):
        raw = signed_url.reveal()
        with self._lock:
            self.submitted.append((namespace, name, raw, filetype, options))
        if raw in self._reject:
            raise OrchError("rejected", 400, "disallowed url")
        with self._lock:
            job_id = f"job-{next(self._ids)}"
            self._script_for[job_id] = list(self._scripts.get(raw, self._default))
            self._cursor[job_id] = 0
            self._note_in_flight()
        # The real 202 carries jobId + url and NO status (recorded.har).
        return {"jobId": job_id, "url": raw}

    def get_import_job(self, namespace, name, job_id):
        with self._lock:
            if job_id not in self._script_for:
                raise JobNotFound(job_id)
            return JobStatus(job_id=job_id, status=self._advance(job_id))

    def list_import_jobs(self, namespace, name, *, running_only=False):
        with self._lock:
            out = [
                JobStatus(job_id=job_id, status=self._advance(job_id))
                for job_id in list(self._script_for)
            ]
        return [s for s in out if not is_terminal(s.status)] if running_only else out

    # callers hold the lock
    def _advance(self, job_id: str) -> str:
        script = self._script_for[job_id]
        cursor = self._cursor[job_id]
        if cursor < len(script) - 1:
            cursor += 1
            self._cursor[job_id] = cursor
        self._note_in_flight()
        return script[cursor]

    def _note_in_flight(self) -> None:
        live = sum(
            1
            for job_id, script in self._script_for.items()
            if not is_terminal(script[self._cursor[job_id]])
        )
        self.peak_in_flight = max(self.peak_in_flight, live)


def make_fanout(firecloud, *, policy=None, **kwargs) -> ImportFanOut:
    kwargs.setdefault("poll_interval_s", 0)
    kwargs.setdefault("sleep", lambda _s: None)
    return ImportFanOut(
        firecloud, NS, WS, policy=policy or DispatchPolicy(mode="parallel", max_worker=3), **kwargs
    )


def run(firecloud, urls, **kwargs):
    fanout = make_fanout(firecloud, **kwargs)
    return fanout.wait(fanout.submit(list(urls)))


def fc(**kwargs) -> FakeFirecloud:
    return FakeFirecloud(SERVICE_URLS["dev"]["firecloud"], **kwargs)


# --- the fan-out itself ------------------------------------------------------


@pytest.mark.parametrize("count", [1, 2, 5])
def test_every_url_gets_its_own_submit_and_its_own_job_id(count):
    """The fan-out check, and its inverse.

    Trivial to assert and exactly what catches a fan-out that silently dropped an entry -- or one
    that reused a jobId, which would make N jobs look like one and hide N-1 failures. Parameterised
    down to 1 because a single-file import must take the same path, not a special case.
    """
    firecloud = fc()
    result = run(firecloud, URLS[:count])

    # Every URL submitted exactly once. Compared as a multiset, not a sequence: under parallel
    # dispatch the POSTs genuinely race, and pinning their order here would assert something the
    # fan-out does not promise. The order that *is* promised -- the reported job order -- is
    # asserted in test_dispatch_order_is_preserved_under_parallel.
    assert sorted(s[2] for s in firecloud.submitted) == sorted(u.reveal() for u in URLS[:count])
    assert len({job.job_id for job in result.jobs}) == count
    assert result.all_succeeded


def test_every_submit_targets_the_same_workspace():
    """N jobs, one workspace. A fan-out that spread across workspaces would 'pass' every per-job
    check while producing exactly the wrong end state."""
    firecloud = fc()
    run(firecloud, URLS[:3])

    assert {(s[0], s[1]) for s in firecloud.submitted} == {(NS, WS)}


def test_every_submit_sends_the_recorded_body_shape():
    """filetype 'pfb' and options present-but-null, on every job of the fan-out.

    recorded.har shows the real UI sending ``options: null`` rather than omitting the key; a fan-out
    that got this right for the first job and wrong for the rest is exactly the kind of bug a
    single-job test cannot see.
    """
    firecloud = fc()
    run(firecloud, URLS[:3])

    assert {(s[3], s[4]) for s in firecloud.submitted} == {("pfb", None)}


def test_a_single_url_is_the_degenerate_fan_out_not_a_special_case():
    one = fc()
    many = fc()
    run(one, URLS[:1])
    run(many, URLS[:3])

    # Same call shape, same workspace, same body -- only the count differs.
    assert one.submitted[0][:2] == many.submitted[0][:2]
    assert one.submitted[0][3:] == many.submitted[0][3:]


# --- partial failure ---------------------------------------------------------


def test_a_rejected_submit_does_not_take_its_siblings_down():
    """If job 2 of 3 is rejected, jobs 1 and 3 are already running and cannot be recalled.

    So the fan-out reports the state it is actually in rather than raising -- which is both what the
    Terra UI has to render and what an operator has to triage.
    """
    firecloud = fc(reject={URLS[1].reveal()})
    result = run(firecloud, URLS[:3])

    assert len(result.jobs) == 3, "a rejected submit must still appear in the result"
    assert len(result.succeeded) == 2
    rejected = [job for job in result.jobs if job.submit_error]
    assert len(rejected) == 1
    assert rejected[0].job_id is None, "a rejected submit has no jobId to poll"
    assert rejected[0].source_url == URLS[1]
    assert not result.all_succeeded


def test_a_rejected_submit_records_the_status_code_not_the_url():
    """The error text an operator sees must not carry the signed URL."""
    firecloud = fc(reject={URLS[0].reveal()})
    result = run(firecloud, URLS[:1])

    error = result.jobs[0].submit_error
    assert "400" in error
    assert "X-Amz-Signature" not in error
    assert "X-Amz-Signature" not in result.render()


def test_one_failing_job_leaves_the_others_succeeded():
    firecloud = fc(scripts={URLS[0].reveal(): ["Pending", "Error"]})
    result = run(firecloud, URLS[:3])

    assert len(result.succeeded) == 2
    assert len(result.failed) == 1
    assert result.failed[0].source_url == URLS[0]


# --- pacing ------------------------------------------------------------------


def test_parallel_dispatch_never_exceeds_the_max_worker_limit():
    """The max_worker bounds jobs *in flight*, not just concurrent POSTs.

    Asserted from the fake's own view of how many jobs are non-terminal at once, so it would catch a
    pool that released its worker as soon as the POST returned -- which would leave cWDS running N
    translations regardless of the max_worker.
    """
    firecloud = fc()
    result = run(firecloud, URLS, policy=DispatchPolicy(mode="parallel", max_worker=2))

    assert firecloud.peak_in_flight <= 2
    assert len(result.jobs) == 5 and result.all_succeeded


def test_sequential_await_terminal_really_serialises():
    firecloud = fc()
    result = run(firecloud, URLS[:3], policy=DispatchPolicy(mode="sequential", await_terminal=True))

    assert firecloud.peak_in_flight == 1
    # Each job was posted only after its predecessor went terminal.
    starts = [result.dispatch_started_at[job.job_id] for job in result.jobs]
    assert starts == sorted(starts)


def test_dispatch_order_is_preserved_under_parallel():
    """Jobs come back in manifest order even though they were posted concurrently.

    Upserts merge, so the order N PFBs land in is not ours to promise -- but the *reported* order
    must match the manifest, or a per-entry diagnosis points at the wrong file.
    """
    firecloud = fc()
    result = run(firecloud, URLS, policy=DispatchPolicy(mode="parallel", max_worker=3))

    assert [job.source_url for job in result.jobs] == URLS


@pytest.mark.parametrize(
    "policy",
    [
        DispatchPolicy(mode="sequential"),
        DispatchPolicy(mode="sequential", await_terminal=True),
        DispatchPolicy(mode="parallel", max_worker=1),
        DispatchPolicy(mode="parallel", max_worker=3),
    ],
    ids=lambda p: p.label,
)
def test_the_end_state_does_not_depend_on_the_dispatch_policy(policy):
    """Pacing changes when jobs run, never whether they succeed. This is the question the policy
    parameter exists to answer, so it is asserted rather than assumed."""
    firecloud = fc()
    result = run(firecloud, URLS[:3], policy=policy)

    assert len(firecloud.submitted) == 3
    assert result.all_succeeded


# --- polling -----------------------------------------------------------------


def test_status_history_records_transitions_not_just_the_terminal_state():
    """A terminal-state-only record would pass even if status translation regressed."""
    firecloud = fc(default=["Pending", "Translating", "ReadyForUpsert", "Done"])
    result = run(firecloud, URLS[:1])

    assert [s.status for s in result.jobs[0].history] == [
        "Pending",
        "Translating",
        "ReadyForUpsert",
        "Done",
    ]


def test_repeated_identical_statuses_are_not_recorded_twice():
    """Polling a two-hour import every 5 s must not produce 1440 identical samples per job."""
    firecloud = fc(default=["ReadyForUpsert", "ReadyForUpsert", "ReadyForUpsert", "Done"])
    result = run(firecloud, URLS[:1])

    assert [s.status for s in result.jobs[0].history] == ["Pending", "ReadyForUpsert", "Done"]


def test_404_is_treated_as_pending_not_as_an_error():
    """Under a wide fan-out this happens routinely on the first tick: Orchestration is asked about a
    jobId before it knows of it."""

    class Missing(FakeFirecloud):
        asked = 0

        def get_import_job(self, namespace, name, job_id):
            self.asked += 1
            if self.asked < 3:
                raise JobNotFound(job_id)
            return JobStatus(job_id=job_id, status="Done")

    result = run(Missing(SERVICE_URLS["dev"]["firecloud"]), URLS[:1])
    assert result.jobs[0].status == "Done"


def test_a_hung_job_times_out_without_hanging_the_run():
    firecloud = fc(default=["Translating"])
    result = run(firecloud, URLS[:1], job_timeout_s=0)

    assert result.jobs[0].status == TIMEOUT
    assert result.jobs[0].history[-1].status == TIMEOUT
    assert not result.all_succeeded


def test_siblings_still_report_when_one_job_hangs():
    """The whole reason each job carries its own deadline: one hung job must not consume the budget
    of the others, or a fan-out reports nothing instead of 'one of five hung'."""
    firecloud = fc(scripts={URLS[0].reveal(): ["Translating"]}, default=["Done"])
    result = run(
        firecloud, URLS[:3], job_timeout_s=0, policy=DispatchPolicy(mode="parallel", max_worker=3)
    )

    assert {job.status for job in result.jobs} == {TIMEOUT, "Done"}
    assert len(result.succeeded) == 2


def test_an_unrecognised_status_is_terminal_and_is_surfaced():
    """Not a skip. If Orchestration starts returning something new, that is the regression."""
    firecloud = fc(default=["Pending", "Frobnicating"])
    result = run(firecloud, URLS[:1])

    assert result.jobs[0].status == "Frobnicating"
    assert result.failed, "an unrecognised status must count as a failure"
    assert result.unknown_statuses == {"Frobnicating"}


def test_upserting_must_never_reach_the_client():
    firecloud = fc(default=["Pending", "Upserting", "Done"])
    result = run(firecloud, URLS[:1])

    assert result.forbidden_statuses == {"Upserting"}


def test_list_polling_reaches_the_same_terminal_state_as_per_job():
    """The only check anywhere that Orchestration's two status endpoints do not disagree."""
    per_job = fc()
    listed = fc()

    a = run(per_job, URLS[:3])
    b = run(listed, URLS[:3], poll_strategy="list")

    assert [j.status for j in a.jobs] == [j.status for j in b.jobs] == ["Done"] * 3


def test_list_polling_still_submits_every_url():
    firecloud = fc()
    run(firecloud, URLS[:4], poll_strategy="list")

    assert len(firecloud.submitted) == 4


# --- the hand-off check, once per job ----------------------------------------


def _request(urls, kind="manifest"):
    source = signed("manifest.json") if kind == "manifest" else urls[0]
    return ImportRequest(kind=kind, source=source, urls=tuple(urls))


def test_handoff_is_verified_for_every_url_not_just_the_first():
    """A manifest's N URLs are N separate deliveries of a credential to Terra. Verifying only the
    first would leave N-1 unverified -- so one bad URL must stop the whole request."""
    firecloud = fc()
    smuggled = SignedUrl("https://evil.example.com/export.avro?X-Amz-Signature=deadbeef")

    with pytest.raises(SignedUrlProvenanceError):
        submit_import_jobs(
            firecloud,
            NS,
            WS,
            _request([URLS[0], URLS[1], smuggled]),
            tier_name="dev",
            policy=DispatchPolicy(mode="parallel", max_worker=3),
        )

    assert firecloud.submitted == [], "a rejected request must create no jobs at all"


def test_handoff_refuses_a_destination_that_is_not_the_tier_firecloud_host():
    """A dev run must not be able to deliver its URLs to prod Orchestration, or vice versa."""
    wrong_host = fc()
    wrong_host.base_url = SERVICE_URLS["prod"]["firecloud"].rstrip("/")

    with pytest.raises(pipeline.SafetyError):
        submit_import_jobs(
            wrong_host,
            NS,
            WS,
            _request(URLS[:2]),
            tier_name="dev",
            policy=DispatchPolicy(mode="parallel", max_worker=3),
        )

    assert wrong_host.submitted == []


def test_submit_import_jobs_fans_out_when_the_handoff_passes():
    firecloud = fc()
    fanout, jobs = submit_import_jobs(
        firecloud,
        NS,
        WS,
        _request(URLS[:3]),
        tier_name="dev",
        policy=DispatchPolicy(mode="parallel", max_worker=3),
        poll_interval_s=0,
    )
    result = fanout.wait(jobs)

    assert len(firecloud.submitted) == 3
    assert result.all_succeeded


# =============================================================================
# 3. The shared tail: workspace + fan-out + QC wired together
# =============================================================================
#
# The seam where the two halves above meet. Tested with the client classes swapped out, because the
# tail constructs them itself from the tier's URLs -- which is the thing worth checking: that a run
# creates exactly one workspace, fans N jobs into *that* workspace, and reports one verdict covering
# both, whichever input shape it started from.


def _setup(tmp_path):
    return pipeline._RunSetup(
        tier=ResolvedTier(
            name="dev",
            email="you@test.firecloud.org",
            adc_credentials_file=None,
            terra_billing_project="proj",
            service_urls=SERVICE_URLS["dev"],
        ),
        creds=None,
        google_token=lambda: "tok",
        log_file=tmp_path / "run.log",
        identity="you@test.firecloud.org",
    )


def run_tail(monkeypatch, tmp_path, request_obj, *, entities, firecloud=None, rawls=None):
    firecloud = firecloud or fc()
    rawls = rawls or FakeRawls(entities=entities)
    monkeypatch.setattr(pipeline, "RawlsClient", lambda *a, **k: rawls)
    monkeypatch.setattr(pipeline, "FirecloudClient", lambda *a, **k: firecloud)
    summary = pipeline._create_import_and_qc(
        setup=_setup(tmp_path),
        request=request_obj,
        policy=DispatchPolicy(mode="parallel", max_worker=3),
        poll_strategy="per_job",
        poll_interval_s=0,
        job_timeout_s=60,
        durations={},
    )
    return summary, firecloud, rawls


def test_the_tail_creates_one_workspace_and_fans_every_url_into_it(monkeypatch, tmp_path):
    summary, firecloud, rawls = run_tail(
        monkeypatch, tmp_path, _request(URLS[:3]), entities={"subject": {"count": 7}}
    )

    assert rawls.create_calls == 1, "one workspace per run, however wide the fan-out"
    assert len(firecloud.submitted) == 3
    assert {(s[0], s[1]) for s in firecloud.submitted} == {
        ("proj", summary["workspace_name"])
    }, "every job went into the workspace this run created"


def test_the_tail_reports_one_verdict_covering_jobs_and_data(monkeypatch, tmp_path):
    summary, _fc, _rawls = run_tail(
        monkeypatch, tmp_path, _request(URLS[:3]), entities={"subject": {"count": 7}}
    )

    assert summary["job_count"] == 3 and summary["jobs_succeeded"] == 3
    assert summary["total_rows"] == 7 and summary["table_count"] == 1
    assert summary["qc_passed"] is True
    assert summary["kind"] == "manifest"
    assert summary["dispatch_policy"] == "parallel-max_worker3"
    assert len(summary["job_ids"]) == 3


def test_the_tail_fails_the_verdict_when_the_workspace_stays_empty(monkeypatch, tmp_path):
    """Every job says Done and nothing arrived -- the failure mode the data check exists for."""
    summary, _fc, _rawls = run_tail(monkeypatch, tmp_path, _request(URLS[:2]), entities={})

    assert summary["jobs_succeeded"] == 2
    assert summary["total_rows"] == 0
    assert summary["qc_passed"] is False


def test_the_tail_still_reports_when_part_of_the_fan_out_failed(monkeypatch, tmp_path):
    """A partial import leaves real data behind; the run is still not the one that was asked for."""
    summary, _fc, _rawls = run_tail(
        monkeypatch,
        tmp_path,
        _request(URLS[:3]),
        entities={"subject": {"count": 4}},
        firecloud=fc(reject={URLS[1].reveal()}),
    )

    assert summary["job_count"] == 3 and summary["jobs_succeeded"] == 2
    assert summary["total_rows"] == 4, "the survivors' data is reported, not discarded"
    assert summary["qc_passed"] is False


def test_the_avro_and_manifest_shapes_take_the_same_tail(monkeypatch, tmp_path):
    """Only the workspace-name infix distinguishes them -- the wiring is identical."""
    avro, _fc, _rawls = run_tail(
        monkeypatch, tmp_path, _request(URLS[:1], kind="avro"), entities={"subject": {"count": 1}}
    )
    manifest, _fc2, _rawls2 = run_tail(
        monkeypatch, tmp_path, _request(URLS[:1]), entities={"subject": {"count": 1}}
    )

    assert avro["job_count"] == manifest["job_count"] == 1
    assert avro["qc_passed"] and manifest["qc_passed"]
    assert "bdc_avro" in avro["workspace_name"]
    assert "bdc_manifest" in manifest["workspace_name"]


def test_the_summary_carries_no_signed_url(monkeypatch, tmp_path):
    """It is returned to the CLI and printed, so it must be safe to paste into a ticket."""
    summary, _fc, _rawls = run_tail(
        monkeypatch, tmp_path, _request(URLS[:2]), entities={"subject": {"count": 2}}
    )

    assert "X-Amz-Signature" not in repr(summary)
    assert summary["source"] == "manifest.json", "the filename is what identifies the run"
