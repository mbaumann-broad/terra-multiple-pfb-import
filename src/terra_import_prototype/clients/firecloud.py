"""Firecloud Orchestration client: the ``importJob`` endpoints.

The only service the import flow talks to. Orchestration owns status translation and the
workspace-id lookup, so a check that reaches past it into cWDS is checking something the Terra UI
cannot see.

  POST /api/workspaces/<ns>/<name>/importJob   {"url": <signed URL>, "filetype": "pfb", "options": null}  -> 202
  GET  /api/workspaces/<ns>/<name>/importJob/<jobId>          -> {"filetype", "jobId", "status"}
  GET  /api/workspaces/<ns>/<name>/importJob?running_only=true -> [ ... ]

Confirmed against the real capture in ``recorded.har`` (prod, 2026-09-21):

- The submit body carries ``options`` **even when it is null**. It is easy to treat as omittable; no
  recorded client omits it, so neither does this one.
- The 202 body is ``{"jobId", "url", "workspace"}`` -- it carries **no status**. A client that read
  ``status`` off the submit response would record an empty first state, so the caller seeds
  ``Pending`` itself.
- The status body is ``{"filetype", "jobId", "status"}``. ``filetype`` comes back as
  ``"DATA_IMPORT"``, not the ``"pfb"`` that was sent; it is informational and is not asserted on.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import quote

from ..logging_setup import LOGGER_NAME
from ..safety import SignedUrl
from .base import BaseClient

logger = logging.getLogger(LOGGER_NAME)

JOB_ID_FIELDS = ("jobId", "id", "job_id")  # the submit response uses "jobId"


class JobNotFound(LookupError):
    """HTTP 404 from the status endpoint.

    Not an error, and not a missing job: Orchestration can be asked about a jobId before it knows of
    it, and the Terra UI treats that as still-pending. Raised as its own type so the poller can make
    that distinction instead of every 404 aborting a run.
    """


class OrchError(RuntimeError):
    """A non-success response from Orchestration, with the status code and body kept.

    Carried rather than raised-and-lost because a rejected submit is a per-job outcome under fan-out:
    the other N-1 jobs are unaffected and the run continues.
    """

    def __init__(self, message: str, status_code: Optional[int], body: str = ""):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


@dataclass(frozen=True)
class JobStatus:
    """One status reading, normalised across the submit / get / list response shapes."""

    job_id: str
    status: str
    message: Optional[str] = None
    raw: Optional[dict[str, Any]] = None


class FirecloudClient(BaseClient):
    def _ws(self, namespace: str, name: str) -> str:
        # safe="" so a '/' in a name is escaped rather than becoming a path separator. Terra names
        # are [A-Za-z0-9_-] in practice, but a client should not depend on its caller's validation
        # to avoid building a path that points somewhere else.
        return f"/api/workspaces/{quote(namespace, safe='')}/{quote(name, safe='')}"

    def submit_import_job(
        self,
        namespace: str,
        name: str,
        signed_url: SignedUrl,
        *,
        filetype: str = "pfb",
        options: Optional[dict] = None,
    ) -> dict:
        """POST importJob. Returns the 202 body, whose ``jobId`` drives all polling.

        Called **once per PFB URL** -- N times for an N-entry manifest. That fan-out is the behaviour
        under test, and it lives in ``pipeline``; this method stays deliberately single-job so the
        request it builds is identical whether it is the only one or one of eighteen.
        """
        # ONE OF THE TWO SITES that unwraps a signed URL: it goes straight into the importJob body
        # and nowhere else. The destination/provenance checks (safety.verify_pfb_handoff) run before
        # this, and the request body is logged redacted (base client). The other site is
        # safety.fetch_signed_json. Do not add a third reveal() call.
        body = {"url": signed_url.reveal(), "filetype": filetype, "options": options}
        return self.request(
            "POST", f"{self._ws(namespace, name)}/importJob", json_body=body
        ).json()

    def get_import_job(self, namespace: str, name: str, job_id: str) -> JobStatus:
        """Poll one job. Raises :class:`JobNotFound` on 404 (pending, not an error)."""
        response = self.request(
            "GET",
            f"{self._ws(namespace, name)}/importJob/{quote(job_id, safe='')}",
            raise_for_status=False,
        )
        if response.status_code == 404:
            raise JobNotFound(job_id)
        if response.status_code != 200:
            raise OrchError(
                f"importJob GET {job_id} returned {response.status_code}",
                response.status_code,
                response.text[:2000],
            )
        return to_status(response.json())

    def list_import_jobs(self, namespace: str, name: str, *, running_only: bool = False) -> list[JobStatus]:
        """One request covering every job in the workspace.

        The cheaper polling alternative to N per-job GETs, and the call the real UI makes first (it
        appears in ``recorded.har`` immediately after the submit). With ``running_only`` an empty
        list means every job is terminal.
        """
        params = {"running_only": "true"} if running_only else None
        response = self.request("GET", f"{self._ws(namespace, name)}/importJob", params=params)
        return [to_status(item) for item in response.json()]


def to_status(payload: dict[str, Any]) -> JobStatus:
    """Normalise a submit / get / list payload into a :class:`JobStatus`.

    Tolerant of the field-name variants seen across the three endpoints, and of a **missing status**
    -- the 202 has none. An empty status is returned as ``""`` rather than defaulted here, so the
    caller decides what an unknown state means instead of this function quietly inventing "Pending".
    """
    job_id = payload.get("jobId") or payload.get("id") or payload.get("job_id") or ""
    status = payload.get("status") or payload.get("state") or ""
    message = payload.get("message")
    if message is None:
        result = payload.get("result")
        if isinstance(result, dict):
            message = result.get("errorMessage") or result.get("message")
    return JobStatus(job_id=str(job_id), status=str(status), message=message, raw=payload)


def import_job_id(submit_response: dict) -> str:
    """Extract the import job id from a submit response."""
    for field in JOB_ID_FIELDS:
        if submit_response.get(field):
            return str(submit_response[field])
    raise ValueError(f"Could not find an import job id in the submit response: {submit_response}")
