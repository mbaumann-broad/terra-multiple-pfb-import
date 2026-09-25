"""End-to-end import + QC orchestration, with the Terra UI's fan-out.

One flow, two input shapes (``docs/import_flow.md``):

- **avro** — the operator holds one pre-signed Gen3 PFB URL. One ``importJob``. This is the flow
  captured in ``recorded.har``.
- **manifest** — the operator holds one pre-signed manifest URL naming N PFB URLs. **N**
  ``importJob`` calls into the same workspace.

Both run the same tail (:func:`_create_terra_workspace` then :func:`_create_import_submit`): Rawls
createWorkspace (``authorizationDomain: []``) -> fan out ``firecloud.submit_import_job`` -> poll every
job to a terminal status -> QC (the workspace holds data). A single Avro import is the degenerate
one-element fan-out, so there is no single-file code path that could drift from the N-file one.

:func:`run_import_job` takes **one or N** signed URLs and runs that tail **once**: every source is
expanded, the results are concatenated into one list of PFB URLs (:class:`models.ImportRun`), one
workspace is created, and the whole list fans out into it. N sources are not N runs and not N
workspaces -- a manifest naming five PFBs and five Avro URLs given on the command line produce the
same five-job fan-out into the same single workspace, which is the point: what this tool studies is
what happens when many PFBs land in one workspace.

**The fan-out is the thing under test.** Terra's UI does not import a manifest; it expands the
manifest client-side and posts one ``importJob`` per URL, then polls each jobId. That means N
concurrent translations inside cWDS and N upserts into one workspace, and the questions this tool
exists to answer are about that: does every job get its own jobId, does a partial failure leave the
survivors intact, does capped concurrency change the outcome, does the workspace end up with
everything. :func:`submit_import_jobs` is therefore written to be *paced* (``DispatchPolicy``) and to
treat a rejected submit as a per-job result rather than a run-ending exception.

End-to-end execution requires real per-tier ADC credentials and a real pre-signed Gen3 export, and
creates a real (retained) workspace.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal, Optional
from urllib.parse import urlsplit

import requests
from google.auth.credentials import Credentials

from . import __version__
from .auth import assert_tier_identity, bearer_token, credentials_for_tier
from .clients.firecloud import FirecloudClient, JobNotFound, OrchError, import_job_id, to_status
from .clients.rawls import RawlsClient, workspace_auth_domains
from .config import SERVICE_URLS, ResolvedTier, load_config, resolve_tier
from .logging_setup import LOGGER_NAME, setup_logging, write_header
from .manifest import build_run
from .models import (
    TIMEOUT,
    BatchResult,
    DispatchPolicy,
    ImportJob,
    ImportRun,
    RequestKind,
    StatusSample,
    is_terminal,
)
from .qc import QcResult, run_qc
from .safety import (
    SIGNED_URL_ALLOWED_PREFIXES,
    SafetyError,
    SignedUrl,
    verify_pfb_handoff,
)
from .timing import log_stage_summary, stage_timer
from .workspace import infix_for, workspace_name

logger = logging.getLogger(LOGGER_NAME)

#: The importJob ``filetype`` for a Gen3 PFB export. Recorded in ``recorded.har``; the status
#: endpoint echoes ``DATA_IMPORT`` back, which is informational and not asserted on.
IMPORT_FILETYPE = "pfb"

#: ``options`` in the submit body. ``None`` is what the real UI sends -- and it sends the key, rather
#: than omitting it. See ``clients/firecloud.py``.
IMPORT_OPTIONS: Optional[dict] = None

#: Rawls createWorkspace provisions a GCS bucket and is the slow call in the tail. Under load the
#: POST can either read-time-out client-side or draw a 5xx from a proxy in front of Rawls even while
#: the workspace *was* created server-side. Give it headroom and confirm-or-adopt rather than
#: abandoning an orphaned, import-less workspace.
RAWLS_TIMEOUT = 300  # seconds
WORKSPACE_ADOPT_ATTEMPTS = 5  # GETs to confirm the workspace after a failed/uncertain create
WORKSPACE_ADOPT_POLL_SECONDS = 5.0  # between those GETs
WORKSPACE_ADOPT_GET_TIMEOUT = 30  # per-GET timeout, so a slow GET is not governed by RAWLS_TIMEOUT

#: Firecloud client timeout. Generous because a fan-out run makes many calls under concurrency.
FIRECLOUD_TIMEOUT = 180

#: Polling defaults. The interval matches terra-ui's ``ImportStatus.tsx`` (5 s). The per-job budget
#: is generous because a large PFB can translate for hours -- and because Terra's import service has
#: a job TTL of its own, so the interesting failure is a job that never reaches a terminal status at
#: all rather than one that is merely slow.
POLL_INTERVAL_SECONDS = 5.0
JOB_TIMEOUT_SECONDS = 7200.0

#: How the fan-out polls. ``per_job`` is one GET per jobId per interval -- exactly what the UI does
#: today, and O(N) requests per interval. ``list`` is one GET covering every job. Both are kept
#: because running them against the same import and comparing the answers is the only check anywhere
#: that the two Orchestration endpoints do not disagree.
PollStrategy = Literal["per_job", "list"]


#: Prefix on a jobId this tool invented because the 202 carried none. Orchestration's ids are UUIDs
#: with no prefix, so a prefixed id can never collide with a real one -- and anything downstream can
#: tell "a job we cannot track" from "a job we can".
SYNTHETIC_JOB_ID_PREFIX = "local-"


def _http_status(exc: requests.exceptions.HTTPError) -> Optional[int]:
    """The HTTP status code carried by a requests ``HTTPError``, or ``None`` if unavailable."""
    return getattr(getattr(exc, "response", None), "status_code", None)


def _synthetic_job_id() -> str:
    """A locally-generated stand-in jobId. See :func:`_job_id_for`."""
    return f"{SYNTHETIC_JOB_ID_PREFIX}{uuid.uuid4()}"


def is_synthetic_job_id(job_id: Optional[str]) -> bool:
    return bool(job_id) and job_id.startswith(SYNTHETIC_JOB_ID_PREFIX)


def _job_id_for(response: dict, url: SignedUrl) -> str:
    """The submit response's jobId, or a random stand-in when it carries none.

    A 202 without a jobId is a protocol violation -- ``recorded.har`` shows Orchestration always
    returns one -- but it must not take the run down. Every job is keyed by its jobId (dispatch
    offsets, status history, the batch summary), so a job with no id would collide with every other
    idless job and erase them from the report. A random id keeps this job distinct and visible;
    ``is_synthetic_job_id`` marks it as one this tool cannot actually poll.
    """
    try:
        return import_job_id(response)
    except ValueError:
        job_id = _synthetic_job_id()
        logger.warning(
            "Import submit for %s returned no jobId; tracking it locally as %s. It cannot be "
            "polled, so it will be reported as failed.",
            url.filename,
            job_id,
        )
        return job_id


# --- workspace creation ------------------------------------------------------


def _create_workspace_resilient(
    rawls: RawlsClient, namespace: str, name: str, *, description: str
) -> None:
    """Create the destination workspace, tolerating a slow/uncertain createWorkspace.

    Rawls createWorkspace can be slow under concurrent load, so the POST may either time out
    client-side or get a **5xx** from a proxy in front of Rawls -- in both cases the workspace is
    often already created server-side, so aborting leaves an orphaned, import-less workspace. On a
    timeout/connection error OR a 5xx we therefore GET the workspace (retrying a not-yet-visible 404
    or a transient 5xx/GET-timeout): if it exists with the empty ``authorizationDomain`` we requested
    we adopt it and continue into the import; if it cannot be confirmed after
    ``WORKSPACE_ADOPT_ATTEMPTS`` checks the create genuinely failed and we re-raise. A **4xx** (incl.
    ``409`` name-exists, ``403`` no-access) is a definitive error and stays fail-fast.

    This matters more here than it would for a single import: the workspace is created **once** and
    then N jobs fan out into it, so an abandoned-but-actually-created workspace costs the whole run,
    not one job.

    Safety: a workspace our POST just created must carry the empty ``authorizationDomain`` we
    requested (the import applies the real one). If an adopted workspace already has a non-empty auth
    domain it is not the one we created -- refuse it (``SafetyError``) rather than fan N imports into
    an unexpected target.
    """
    try:
        rawls.create_workspace(namespace, name, description=description)
        return
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
        create_exc: Exception = exc
        logger.warning(
            "createWorkspace for %s/%s did not return (%s); checking whether it was created "
            "server-side before giving up...",
            namespace,
            name,
            type(exc).__name__,
        )
    except requests.exceptions.HTTPError as exc:
        status = _http_status(exc)
        if status is None or status < 500:
            raise  # 4xx (incl. 409 name-exists / 403 no-access) or unknown -- definitive, fail-fast
        create_exc = exc
        logger.warning(
            "createWorkspace for %s/%s returned %s (gateway/server error -- likely a slow create "
            "behind a proxy); checking whether it was created server-side before giving up...",
            namespace,
            name,
            status,
        )
    # Confirm-or-adopt. A 404 means "not visible yet"; a transient 5xx or Timeout/ConnectionError on
    # the GET is the same contended condition that failed the create -- all just retry. A definitive
    # 4xx propagates.
    for attempt in range(1, WORKSPACE_ADOPT_ATTEMPTS + 1):
        ws = None
        try:
            ws = rawls.get_workspace(
                namespace,
                name,
                fields="workspace.authorizationDomain",
                timeout=WORKSPACE_ADOPT_GET_TIMEOUT,
            )
        except requests.exceptions.HTTPError as get_exc:
            status = _http_status(get_exc)
            retryable = status == 404 or (status is not None and status >= 500)
            if not retryable:
                raise  # a definitive 4xx (403/...) -- do not mask it
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            pass  # same load that failed the create; treat as not-yet-visible and retry
        if ws is not None:
            domains = workspace_auth_domains(ws)
            if domains:
                raise SafetyError(
                    f"createWorkspace for {namespace}/{name} did not succeed and an existing "
                    f"workspace of that name has a non-empty auth domain {domains}; refusing to "
                    "adopt it (a just-created workspace must have none -- the import applies the "
                    "auth domain)."
                )
            logger.info(
                "createWorkspace did not return cleanly but %s/%s exists server-side with an empty "
                "auth domain; adopting it and continuing to the import.",
                namespace,
                name,
            )
            return
        if attempt < WORKSPACE_ADOPT_ATTEMPTS:
            time.sleep(WORKSPACE_ADOPT_POLL_SECONDS)
    # Never confirmed the workspace -- the create genuinely failed (or Rawls stayed unreachable).
    raise RuntimeError(
        f"createWorkspace for {namespace}/{name} did not succeed and the workspace could not be "
        f"confirmed created after {WORKSPACE_ADOPT_ATTEMPTS} checks. Re-run to retry."
    ) from create_exc


# --- the fan-out -------------------------------------------------------------


class ImportFanOut:
    """Submits and polls N ``importJob`` calls into one workspace.

    A class rather than a function because a fan-out has state a bare function would have to thread
    through every call: the dispatch clock (so timings are comparable across jobs), per-job status
    history, and a lock guarding both from the pool's threads.

    Threads rather than asyncio: the HTTP client is synchronous, the concurrency is tiny (a max_worker of a
    few), and a thread's stack trace is far easier to read when a run fails at 2am.
    """

    def __init__(
        self,
        firecloud: FirecloudClient,
        namespace: str,
        name: str,
        *,
        policy: DispatchPolicy,
        poll_interval_s: float = POLL_INTERVAL_SECONDS,
        job_timeout_s: float = JOB_TIMEOUT_SECONDS,
        poll_strategy: PollStrategy = "per_job",
        filetype: str = IMPORT_FILETYPE,
        options: Optional[dict] = IMPORT_OPTIONS,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._firecloud = firecloud
        self._namespace = namespace
        self._name = name
        self.policy = policy
        self.poll_interval_s = poll_interval_s
        self.job_timeout_s = job_timeout_s
        self.poll_strategy: PollStrategy = poll_strategy
        self._filetype = filetype
        self._options = options
        self._clock = clock
        self._sleep = sleep

        self._t0 = 0.0
        self._lock = threading.Lock()
        self.dispatch_started_at: dict[str, float] = {}

    # --- submit ------------------------------------------------------------

    def submit(self, urls: list[SignedUrl]) -> list[ImportJob]:
        """One ``firecloud.submit_import_job`` per URL, paced by the dispatch policy.

        Under ``parallel`` the pool bounds **jobs in flight**, not just concurrent POSTs: a worker
        holds its slot until its job is terminal. Capping POSTs alone would bound neither cWDS
        concurrency nor the workspace policy updates that follow, which is what the max_worker is for. The
        exception is ``poll_strategy="list"``, a batch operation with nowhere to wait per job; there
        the max_worker degrades to a POST-concurrency max_worker and says so.

        **Partial failure is a first-class outcome, not an exception.** If job 4 of 10 is rejected,
        jobs 1-3 are already running inside cWDS and cannot be recalled, so the caller gets the jobs
        it has plus the error on the one it does not -- exactly the state the Terra UI has to render
        and an operator has to triage. Raising here would discard the jobs that *did* start.
        """
        self._t0 = self._clock()
        self.dispatch_started_at = {}
        logger.info(
            "Fanning out %d import job(s) into %s/%s (%s, poll=%s).",
            len(urls),
            self._namespace,
            self._name,
            self.policy.label,
            self.poll_strategy,
        )

        if self.policy.mode == "sequential":
            # _submit_one waits for terminal state itself when await_terminal is set, so this loop
            # covers both sequential variants.
            return [self._submit_import_job(url, index) for index, url in enumerate(urls)]

        with ThreadPoolExecutor(
            max_workers=self.policy.max_worker, thread_name_prefix="import-fanout"
        ) as pool:
            futures = [pool.submit(self._submit_import_job, url, i) for i, url in enumerate(urls)]
            return [future.result() for future in futures]

    def _submit_import_job(self, url: SignedUrl, index: int) -> ImportJob:
        started = self._clock() - self._t0
        job = ImportJob(source_url=url)
        try:
            response = self._firecloud.submit_import_job(
                self._namespace,
                self._name,
                url,
                filetype=self._filetype,
                options=self._options,
            )
        except (OrchError, requests.exceptions.RequestException) as exc:
            # A rejected submit kills this job only. Record it and let the siblings run.
            job.submit_error = _submit_error_text(exc)
            job.status = "Error"
            job.message = job.submit_error
            logger.warning(
                "Import job %d/%s for %s was rejected: %s",
                index + 1,
                self._name,
                url.filename,
                job.submit_error,
            )
            return job

        status = to_status(response)
        # The jobId is the handle for everything that follows. If the 202 carried none, a random
        # stand-in is used so this job stays distinct in the report rather than vanishing.
        job.job_id = status.job_id or _job_id_for(response, url)
        # The 202 carries no status (recorded.har), so "Pending" is ours, not Orchestration's. A
        # job *can* already be terminal in the 202 -- a rejected URL fails before the first poll --
        # so a status that is present is kept.
        job.status = status.status or "Pending"
        job.message = status.message or job.message
        with self._lock:
            self.dispatch_started_at[job.job_id] = started
        self._record(job, job.status)
        logger.info(
            "Import job submitted: %s (%s) at t+%.2fs", job.job_id, url.filename, started
        )

        if self._holds_slot_until_terminal:
            self._poll_one_to_terminal(job)
        return job

    @property
    def _holds_slot_until_terminal(self) -> bool:
        if self.poll_strategy == "list":
            return False  # nowhere to wait per job; the max_worker becomes a POST max_worker
        if self.policy.mode == "sequential":
            return self.policy.await_terminal
        return True

    # --- poll --------------------------------------------------------------

    def wait(self, jobs: list[ImportJob]) -> BatchResult:
        """Poll every submitted, non-terminal job until it finishes, times out, or errors."""
        pending = [job for job in jobs if job.submitted and not is_terminal(job.status)]
        if pending:
            if self.poll_strategy == "list":
                self._poll_via_list(pending)
            else:
                self._poll_per_job(pending)
        return BatchResult(
            jobs=jobs,
            dispatch_started_at=dict(self.dispatch_started_at),
            wall_clock_s=self._clock() - self._t0,
        )

    def _poll_per_job(self, jobs: list[ImportJob]) -> None:
        """One request per jobId per interval. Mirrors today's UI exactly.

        Each job carries its **own** deadline, so one hung job cannot consume the budget of the
        others -- under fan-out that distinction is the difference between reporting "1 of 8 hung"
        and reporting nothing at all.
        """
        deadlines = {job.job_id: self._clock() + self.job_timeout_s for job in jobs}
        outstanding = list(jobs)
        while outstanding:
            self._sleep(self.poll_interval_s)
            still_running: list[ImportJob] = []
            for job in outstanding:
                if self._refresh(job):
                    continue  # reached a terminal state
                if self._clock() > deadlines[job.job_id]:
                    self._expire(job)
                    continue
                still_running.append(job)
            outstanding = still_running

    def _poll_via_list(self, jobs: list[ImportJob]) -> None:
        """One request covers every job; an empty ``running_only`` list means they are all done.

        Cheaper than per-job polling at O(1) requests per interval instead of O(N), and needs no
        backend change. Kept alongside ``per_job`` so a run can prove the two endpoints agree.
        """
        by_id = {job.job_id: job for job in jobs}
        deadline = self._clock() + self.job_timeout_s
        while True:
            self._sleep(self.poll_interval_s)
            running = {
                s.job_id: s
                for s in self._firecloud.list_import_jobs(
                    self._namespace, self._name, running_only=True
                )
            }
            for job_id, job in by_id.items():
                sample = running.get(job_id)
                if sample is not None:
                    self._apply(job, sample.status, sample.message)
            if not [j for j in by_id.values() if j.job_id in running]:
                # Nothing running: one final full read settles every terminal status, then anything
                # the list did not mention is asked about directly.
                for status in self._firecloud.list_import_jobs(self._namespace, self._name):
                    if (job := by_id.get(status.job_id)) is not None:
                        self._apply(job, status.status, status.message)
                for job in by_id.values():
                    if not is_terminal(job.status):
                        self._refresh(job)
                return
            if self._clock() > deadline:
                for job in by_id.values():
                    if not is_terminal(job.status):
                        self._expire(job)
                return

    def _poll_one_to_terminal(self, job: ImportJob) -> None:
        """Hold this pool slot until the job finishes -- the 'jobs in flight' half of the max_worker."""
        deadline = self._clock() + self.job_timeout_s
        while not is_terminal(job.status):
            self._sleep(self.poll_interval_s)
            if self._refresh(job):
                return
            if self._clock() > deadline:
                self._expire(job)
                return

    def _refresh(self, job: ImportJob) -> bool:
        """Poll once. Returns True once the job is terminal.

        HTTP 404 is **pending, not an error**: the status endpoint can be asked about a jobId before
        Orchestration knows of it, which under a wide fan-out happens routinely on the first tick.
        """
        try:
            status = self._firecloud.get_import_job(self._namespace, self._name, job.job_id or "")
        except JobNotFound:
            if is_synthetic_job_id(job.job_id):
                # We invented this id; Orchestration will never know it, so waiting out the full
                # per-job budget would only hold a pool slot for nothing.
                job.status = "Error"
                job.message = "submit response carried no jobId; the job cannot be tracked"
                self._record(job, job.status)
                return True
            self._record(job, job.status)
            return False
        self._apply(job, status.status, status.message)
        return is_terminal(job.status)

    def _apply(self, job: ImportJob, status: str, message: Optional[str]) -> None:
        job.status = status or job.status
        if message:
            job.message = message
        self._record(job, job.status)

    def _expire(self, job: ImportJob) -> None:
        """A hung job must never hang the run.

        Record the timeout with its full history and let the siblings keep reporting -- under fan-out
        the other jobs' outcomes are exactly what tells you whether the hang was that job's PFB or
        the service as a whole.
        """
        job.status = TIMEOUT
        job.message = f"no terminal status within {self.job_timeout_s:.0f}s"
        self._record(job, TIMEOUT)
        logger.warning("Import job %s timed out after %.0fs", job.label, self.job_timeout_s)

    def _record(self, job: ImportJob, status: str) -> None:
        """Append a status *transition*, not a tick-by-tick log.

        A terminal-state-only record would pass even if Orchestration's status translation regressed,
        so every observed change is kept with its offset from dispatch. Deduplicated because polling
        every 5 s for an hour would otherwise produce 720 identical samples per job.
        """
        with self._lock:  # pool threads share the histories
            if job.history and job.history[-1].status == status:
                return
            job.history.append(
                StatusSample(
                    job_id=job.job_id or "", status=status, t=self._clock() - self._t0
                )
            )


def _submit_error_text(exc: Exception) -> str:
    """A short, safe description of a rejected submit. Never includes the request body."""
    if isinstance(exc, OrchError):
        return f"HTTP {exc.status_code}: {exc.body[:500]}"
    status = _http_status(exc) if isinstance(exc, requests.exceptions.HTTPError) else None
    body = ""
    response = getattr(exc, "response", None)
    if response is not None:
        body = (getattr(response, "text", "") or "")[:500]
    return f"HTTP {status}: {body}" if status else f"{type(exc).__name__}: {exc}"


def submit_import_jobs(
    firecloud: FirecloudClient,
    namespace: str,
    name: str,
    request: ImportRun,
    *,
    tier_name: str,
    policy: DispatchPolicy,
    poll_strategy: PollStrategy = "per_job",
    poll_interval_s: float = POLL_INTERVAL_SECONDS,
    job_timeout_s: float = JOB_TIMEOUT_SECONDS,
) -> tuple[ImportFanOut, list[ImportJob]]:
    """Verify each URL's hand-off, then fan out one ``submit_import_job`` per URL.

    The hand-off check runs **per URL** -- not once per run, and not once per source. Every URL the
    run's sources expanded to is a separate delivery of a credential to Terra, so checking only the
    first (of the run, or of each source) would leave the rest unverified. It runs before any submit,
    so one bad destination anywhere in the run creates zero jobs rather than failing partway.
    """
    expected_host = urlsplit(SERVICE_URLS[tier_name]["firecloud"]).netloc
    destination_host = urlsplit(firecloud.base_url).netloc
    allowed_prefixes = SIGNED_URL_ALLOWED_PREFIXES.get(tier_name, ())
    for url in request.urls:
        verify_pfb_handoff(
            url,
            tier_name=tier_name,
            expected_firecloud_host=expected_host,
            destination_host=destination_host,
            allowed_prefixes=allowed_prefixes,
        )

    fanout = ImportFanOut(
        firecloud,
        namespace,
        name,
        policy=policy,
        poll_strategy=poll_strategy,
        poll_interval_s=poll_interval_s,
        job_timeout_s=job_timeout_s,
    )
    return fanout, fanout.submit(list(request.urls))


# --- run setup ---------------------------------------------------------------


@dataclass(frozen=True)
class _RunSetup:
    """Shared per-run setup: resolved tier, ADC credentials, a bearer-token provider, the log file."""

    tier: ResolvedTier
    creds: Credentials
    google_token: Callable[[], str]
    log_file: Path
    identity: str  # the Sam-resolved Terra user the active credentials operate as (identity guard)


def _setup_run(
    run_label: str, tier_name: Optional[str], config_path: Path, log_dir: Path
) -> _RunSetup:
    """Load config, start logging, load ADC credentials, and run the tier identity guard.

    ``run_label`` names the log file. It must be a **safe** string -- never the signed URL, whose
    query string is the secret and which a filename is not redacted by. Callers pass the export's
    filename.
    """
    config = load_config(config_path)
    tier = resolve_tier(config, tier_name)

    _logger, log_file = setup_logging(log_dir, tier.name, run_label)
    write_header(
        _logger,
        version=__version__,
        tier=tier.name,
        source=run_label,
        terra_billing_project=tier.terra_billing_project,
        log_file=str(log_file),
    )

    creds = credentials_for_tier(tier)

    def google_token() -> str:
        return bearer_token(creds)

    identity = assert_tier_identity(creds, tier)
    return _RunSetup(
        tier=tier, creds=creds, google_token=google_token, log_file=log_file, identity=identity
    )


# --- the shared tail ---------------------------------------------------------


def _create_terra_workspace(
    *,
    request: ImportRun,
    durations: dict[str, float],
    setup: _RunSetup,
    rawls: RawlsClient,
) -> str:
    """Create this import's destination workspace and return its name.

    One workspace per import, however wide the fan-out that follows. The name is logged BEFORE the
    import so the marker is present even if the import later fails -- the workspace is retained, and
    it is where an operator goes to see what actually landed. ``namespace`` and ``name`` are both
    ``[A-Za-z0-9_-]``, so the single ``/`` in the marker splits them cleanly.
    """
    ws_name = workspace_name(
        setup.tier.email, request.source.filename, infix=infix_for(request.kind)
    )
    logger.info("Creating workspace %s/%s", setup.tier.terra_billing_project, ws_name)
    try:
        with stage_timer("workspace_create", durations):
            _create_workspace_resilient(
                rawls,
                setup.tier.terra_billing_project,
                ws_name,
                description=f"terra-import-prototype {__version__}: {request.description}",
            )
        logger.info("Terra workspace: %s/%s", setup.tier.terra_billing_project, ws_name)
    finally:
        log_stage_summary(durations)

    return ws_name


def _create_import_submit(
    *,
    setup: _RunSetup,
    request: ImportRun,
    policy: DispatchPolicy,
    poll_strategy: PollStrategy,
    poll_interval_s: float,
    job_timeout_s: float,
    durations: dict[str, float],
    ws_name: str,
    rawls: RawlsClient,
) -> dict:
    """Fan the import out into ``ws_name``, wait for every job, then QC the workspace."""
    google_token = setup.google_token
    try:
        firecloud = FirecloudClient(
            setup.tier.url("firecloud"), google_token, timeout=FIRECLOUD_TIMEOUT
        )

        with stage_timer("import_submit", durations):
            fanout, jobs = submit_import_jobs(
                firecloud,
                setup.tier.terra_billing_project,
                ws_name,
                request,
                tier_name=setup.tier.name,
                policy=policy,
                poll_strategy=poll_strategy,
                poll_interval_s=poll_interval_s,
                job_timeout_s=job_timeout_s,
            )
        with stage_timer("import_wait", durations):
            batch = fanout.wait(jobs)
        logger.info("Import finished.\n%s", batch.render())

        with stage_timer("qc", durations):
            qc = run_qc(
                rawls=rawls,
                namespace=setup.tier.terra_billing_project,
                workspace_name=ws_name,
                batch=batch,
            )
        logger.info("QC report:\n%s", qc.render())
        if not qc.passed:
            logger.warning(
                "QC FAILED for %s/%s -- see the report above.",
                setup.tier.terra_billing_project,
                ws_name,
            )
        return _summary(setup, qc, request, policy, durations)
    finally:
        # End-of-run summary of the user-experienced stage delays (minutes). In a `finally` so it
        # surfaces even when the import fails or times out -- the partial durations are exactly what
        # an operator needs to triage.
        log_stage_summary(durations)


def _summary(
    setup: _RunSetup,
    qc: QcResult,
    request: ImportRun,
    policy: DispatchPolicy,
    durations: dict[str, float],
) -> dict:
    """The run's machine-readable result. Contains no signed URL -- only filenames and jobIds."""
    return {
        "workspace_namespace": qc.namespace,
        "workspace_name": qc.workspace_name,
        "kind": request.kind,
        "source": request.source.filename,
        "source_count": len(request.requests),
        "job_count": len(qc.batch.jobs),
        "jobs_succeeded": len(qc.batch.succeeded),
        "job_ids": qc.batch.job_ids,
        "dispatch_policy": policy.label,
        "table_count": qc.data.table_count,
        "total_rows": qc.data.total_rows,
        "entity_counts": dict(qc.data.counts),
        "qc_passed": qc.passed,
        "durations": durations,
        "log_file": str(setup.log_file),
    }


# --- commands ----------------------------------------------------------------


def run_import_job(
    urls: list[str],
    tier_name: Optional[str],
    config_path: Path,
    *,
    kind: Optional[RequestKind] = None,
    log_dir: Path = Path("logs"),
    policy: Optional[DispatchPolicy] = None,
    poll_strategy: PollStrategy = "per_job",
    poll_interval_s: float = POLL_INTERVAL_SECONDS,
    job_timeout_s: float = JOB_TIMEOUT_SECONDS,
    dry_run: bool = False,
    verify_auth: bool = False,
) -> dict:
    """Import the operator's pre-signed Gen3 export(s) into **one** fresh Terra workspace, then QC it.

    ``urls`` holds one or more signed URLs -- each a PFB (``.avro``) or a manifest (``.json``);
    ``kind`` overrides the extension-based guess for all of them. Every source is expanded and the
    results concatenated, so the run is always: **one** workspace, **one** fan-out over every PFB URL
    the sources named between them, **one** verdict. One URL is the one-element case of that, and a
    manifest that expands to N is indistinguishable downstream from N URLs given directly -- which is
    what keeps the single-source path from drifting from the multi-source one.

    Nothing is created until **every** source has been fetched, expanded and validated
    (:func:`manifest.build_run`), so a bad URL anywhere in the list produces zero jobs and no
    workspace rather than an import that got halfway.

    ``verify_auth`` is a side-effect-free preflight: run the identity guard and stop, before any
    manifest fetch, workspace or import. ``dry_run`` (weaker) stops after the run is built and
    validated -- so every manifest is fetched and every URL checked, but no workspace is created and
    no job is submitted. Both exist because the expensive, state-creating part of this flow is the
    fan-out, and being able to check everything before it is worth a flag.
    """
    if not urls:
        raise ValueError("run_import_job needs at least one pre-signed URL.")
    policy = policy or DispatchPolicy()
    signed_urls = [SignedUrl(url) for url in urls]

    # The log file is named after the first source; the run's other sources are named in the header
    # line build_run logs. Never the URL itself -- the query string is the secret.
    setup = _setup_run(signed_urls[0].filename or "import", tier_name, config_path, log_dir)
    tier = setup.tier

    if verify_auth:
        logger.info(
            "--verify-auth OK: identity %s authorized for tier '%s'. No manifest fetched, no "
            "workspace created, no import submitted (%d source URL(s) were not read).",
            setup.identity,
            tier.name,
            len(signed_urls),
        )
        return {"verify_auth": True, "identity": setup.identity, "log_file": str(setup.log_file)}

    durations: dict[str, float] = {}
    with stage_timer("build_request", durations):
        request = build_run(
            signed_urls,
            kind=kind,
            tier_name=tier.name,
            allowed_prefixes=SIGNED_URL_ALLOWED_PREFIXES.get(tier.name, ()),
        )
    logger.info("Import request: %s (%s).", request.description, request.kind)

    if dry_run:
        logger.info(
            "--dry-run: %d source(s) validated, expanding to %d URL(s); no workspace created and no "
            "import submitted.",
            len(request.requests),
            len(request.urls),
        )
        log_stage_summary(durations)
        return {
            "dry_run": True,
            "kind": request.kind,
            "source": request.source.filename,
            "source_count": len(request.requests),
            "job_count": len(request.urls),
            "durations": durations,
            "log_file": str(setup.log_file),
        }

    rawls = RawlsClient(tier.url("rawls"), setup.google_token, timeout=RAWLS_TIMEOUT)

    # One workspace for the whole run...
    ws_name = _create_terra_workspace(
        request=request,
        durations=durations,
        setup=setup,
        rawls=rawls,
    )

    # ...and every URL from every source fans out into that one workspace.
    return _create_import_submit(
        setup=setup,
        request=request,
        policy=policy,
        poll_strategy=poll_strategy,
        poll_interval_s=poll_interval_s,
        job_timeout_s=job_timeout_s,
        durations=durations,
        ws_name=ws_name,
        rawls=rawls,
    )


def run_check_workspace(
    namespace: str,
    workspace: str,
    tier_name: Optional[str],
    config_path: Path,
    *,
    log_dir: Path = Path("logs"),
) -> dict:
    """Re-run the data check on an existing workspace, importing nothing.

    For a workspace a previous run left behind (every run retains one), or one imported by hand
    through the Terra UI. There are no jobs to report, so the verdict rests on the workspace's
    contents alone.
    """
    setup = _setup_run(workspace, tier_name, config_path, log_dir)
    tier = setup.tier
    rawls = RawlsClient(tier.url("rawls"), setup.google_token, timeout=RAWLS_TIMEOUT)
    qc = run_qc(rawls=rawls, namespace=namespace, workspace_name=workspace, batch=BatchResult(jobs=[]))
    logger.info("QC report:\n%s", qc.render())
    return {
        "workspace_namespace": namespace,
        "workspace_name": workspace,
        "table_count": qc.data.table_count,
        "total_rows": qc.data.total_rows,
        "entity_counts": dict(qc.data.counts),
        "qc_passed": qc.data_passed,
        "log_file": str(setup.log_file),
    }
