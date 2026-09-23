"""Value types shared across the pipeline: the status vocabulary and the fan-out result.

The status vocabulary is copied from terra-ui's ``ImportStatus.tsx`` rather than re-derived. An
unrecognised status is a **failure**, never a skip: if Orchestration starts returning something this
list does not cover, that is precisely the regression this tool exists to catch, and treating it as
"probably fine, keep polling" would hide it until a job hung forever.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Literal, Optional

from .safety import SignedUrl

# --- status vocabulary -------------------------------------------------------

NON_TERMINAL: frozenset[str] = frozenset(
    {"Pending", "Translating", "ReadyForUpsert", "Upserting", "RUNNING", "CREATED", "QUEUED"}
)
TERMINAL_SUCCESS: frozenset[str] = frozenset({"Done", "SUCCEEDED"})
TERMINAL_FAILURE: frozenset[str] = frozenset({"Error", "ERROR"})

#: Synthesised locally when a job outlives its budget. Never returned by Orchestration.
TIMEOUT = "TIMEOUT"

#: Statuses cWDS-backed jobs must never surface to the client. Reaching a client with one of these
#: means Orchestration's status translation regressed.
FORBIDDEN_CLIENT_STATUSES: frozenset[str] = frozenset({"Upserting"})

KNOWN_STATUSES: frozenset[str] = NON_TERMINAL | TERMINAL_SUCCESS | TERMINAL_FAILURE

#: The prefix-valid path a cWDS-backed PFB import walks. Polling samples this sequence; it does not
#: see every state, so an observed history must be a *subsequence*, never an exact match.
EXPECTED_STATUS_PATH: tuple[str, ...] = ("Pending", "Translating", "ReadyForUpsert", "Done")


def is_known(status: str) -> bool:
    return status in KNOWN_STATUSES


def is_terminal(status: str) -> bool:
    """Unrecognised statuses are terminal failures, matching the UI's behaviour."""
    return status == TIMEOUT or status not in NON_TERMINAL


def is_success(status: str) -> bool:
    return status in TERMINAL_SUCCESS


def is_failure(status: str) -> bool:
    return is_terminal(status) and not is_success(status)


# --- the import request ------------------------------------------------------

#: How the operator's signed URL was interpreted. ``avro`` is one signed PFB URL posted directly --
#: today's production flow, recorded in ``recorded.har``. ``manifest`` is one signed manifest URL
#: that expands to N PFB URLs.
RequestKind = Literal["avro", "manifest"]


@dataclass(frozen=True)
class ImportRequest:
    """The normalised import request: the PFB URLs to fan out over, and where they came from.

    Both input shapes produce this one type on purpose. A single Avro URL is the degenerate
    one-element case of a manifest fan-out, so dispatch, polling and QC are literally the same code
    for both -- there is no single-file branch that could diverge from the N-file one.

    ``source`` is the operator's original signed URL (the manifest, or the Avro itself); ``urls`` is
    what gets imported. ``raw`` retains the parsed manifest document so a caller can inspect
    shape-specific fields the normalisation drops; it is None when there was no document.
    """

    kind: RequestKind
    source: SignedUrl
    urls: tuple[SignedUrl, ...]
    raw: object = None

    def __len__(self) -> int:
        return len(self.urls)

    @property
    def is_fan_out(self) -> bool:
        return len(self.urls) > 1

    @property
    def description(self) -> str:
        """A safe one-line summary for logs and the workspace description (no secrets)."""
        if self.kind == "avro":
            return f"1 PFB from {self.source.filename}"
        return f"{len(self.urls)} PFB(s) from manifest {self.source.filename}"


# --- jobs --------------------------------------------------------------------


@dataclass(frozen=True)
class StatusSample:
    """One observed status transition, timestamped relative to the start of dispatch."""

    job_id: str
    status: str
    t: float  # monotonic seconds since dispatch began
    wall: float = field(default_factory=time.time)


@dataclass
class ImportJob:
    """One ``importJob``: one PFB URL, one jobId, one status history.

    ``source_url`` stays a :class:`SignedUrl` for the job's whole life so that a job appearing in a
    log line, an exception, or a returned summary cannot leak its URL.
    """

    source_url: SignedUrl
    job_id: Optional[str] = None
    status: str = "Pending"
    message: Optional[str] = None
    #: Set when the POST that would have created the job was itself rejected. Distinct from a job
    #: that was created and then failed: there is no jobId to poll and nothing to cancel.
    submit_error: Optional[str] = None
    history: list[StatusSample] = field(default_factory=list)

    @property
    def submitted(self) -> bool:
        return self.job_id is not None

    @property
    def succeeded(self) -> bool:
        return self.submitted and is_success(self.status)

    @property
    def label(self) -> str:
        """Safe identifier for logs: the jobId if there is one, else the source filename."""
        return self.job_id or f"<not submitted: {self.source_url.filename}>"


@dataclass
class BatchResult:
    """The outcome of one fan-out: every job, its history, and the wall clock.

    A batch is reported, never raised. If job 4 of 10 is rejected, jobs 1-3 are already running and
    cannot be recalled, so the caller gets the jobs it has plus the error on the one it does not --
    exactly the state the Terra UI has to render, and the state an operator has to triage.
    """

    jobs: list[ImportJob]
    dispatch_started_at: dict[str, float] = field(default_factory=dict)
    wall_clock_s: float = 0.0

    @property
    def succeeded(self) -> list[ImportJob]:
        return [j for j in self.jobs if j.succeeded]

    @property
    def failed(self) -> list[ImportJob]:
        return [j for j in self.jobs if not j.succeeded]

    @property
    def all_succeeded(self) -> bool:
        return bool(self.jobs) and not self.failed

    @property
    def job_ids(self) -> list[str]:
        return [j.job_id for j in self.jobs if j.job_id is not None]

    @property
    def unknown_statuses(self) -> set[str]:
        """Statuses observed that this tool's vocabulary does not cover (``TIMEOUT`` is ours)."""
        seen = {s.status for job in self.jobs for s in job.history if not is_known(s.status)}
        seen.discard(TIMEOUT)
        return seen

    @property
    def forbidden_statuses(self) -> set[str]:
        """Statuses observed that must never reach a client (see ``FORBIDDEN_CLIENT_STATUSES``)."""
        return {
            s.status
            for job in self.jobs
            for s in job.history
            if s.status in FORBIDDEN_CLIENT_STATUSES
        }

    def render(self) -> str:
        """Human-readable summary. Safe to log: no signed URL appears, only filenames and jobIds."""
        lines = [
            f"Import — {len(self.succeeded)}/{len(self.jobs)} job(s) succeeded "
            f"in {self.wall_clock_s / 60:.1f} min"
        ]
        for job in self.jobs:
            path = " -> ".join(s.status for s in job.history) or "(never polled)"
            lines.append(f"  {job.label}  {job.source_url.filename}: {path}")
            if job.message:
                lines.append(f"      message: {job.message}")
            if job.submit_error:
                lines.append(f"      submit error: {job.submit_error}")
        return "\n".join(lines)


# --- dispatch policy ---------------------------------------------------------


@dataclass(frozen=True)
class DispatchPolicy:
    """How the fan-out is paced.

    The Terra UI fans a manifest out into N concurrent ``importJob`` POSTs. Whether that should stay
    unbounded, be capped, or be serialised is an open product question, so it is a parameter here
    rather than a hardcoded choice -- the same run can be repeated under each policy and the results
    compared.

    ``max_worker`` bounds **jobs in flight**, not just concurrent POSTs: a worker holds its slot until its
    job reaches a terminal status. That is what the max_worker is for -- it exists to bound concurrency
    inside cWDS and the workspace policy updates that follow, and capping POSTs alone would bound
    neither.
    """

    mode: Literal["sequential", "parallel"] = "parallel"
    max_worker: int = 3  # max in flight; ignored when sequential
    await_terminal: bool = False  # sequential only: wait for job i before posting i+1

    def __post_init__(self) -> None:
        if self.mode not in ("sequential", "parallel"):
            raise ValueError(f"Unknown dispatch mode {self.mode!r} (expected sequential|parallel).")
        if self.mode == "parallel" and self.max_worker < 1:
            raise ValueError("Parallel dispatch needs a max_worker of at least 1.")
        if self.await_terminal and self.mode != "sequential":
            raise ValueError("await_terminal is a sequential-only option.")

    @property
    def label(self) -> str:
        if self.mode == "sequential":
            return "sequential-await" if self.await_terminal else "sequential"
        return f"parallel-max_worker{self.max_worker}"
