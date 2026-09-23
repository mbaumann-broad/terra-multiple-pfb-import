"""Base HTTP client: shared auth + centralized, redacted request/response logging.

Every service client (Firecloud, Rawls, Sam) builds on this so auth and logging are uniform.

Logging matters more here than in a normal client. The behaviour under test is a fan-out across N
concurrent importJob calls, and when it misbehaves the evidence is the request/response sequence --
which job was posted when, which status came back, which correlation id to hand another team. So
every call is logged, with secrets redacted, and the log file is the run's primary artifact.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from typing import Any, Optional
from urllib.parse import urlsplit

import requests

from ..logging_setup import LOGGER_NAME, correlation_fields, redact_headers
from ..redaction import redact_text
from ..safety import BearerDestinationError

logger = logging.getLogger(LOGGER_NAME)


class BaseClient:
    """A thin wrapper over ``requests`` that injects a bearer token and logs every call.

    ``token_provider`` is a callable returning the bearer token string -- the gcloud ADC token for
    every service here. It is a callable, not a string, because a fan-out run can outlive the ~1 h
    token expiry: a large PFB import polls for hours, so the token is fetched per request and the
    provider refreshes it.
    """

    def __init__(self, base_url: str, token_provider: Callable[[], str], timeout: int = 60) -> None:
        self.base_url = base_url.rstrip("/")
        self._host = urlsplit(self.base_url).netloc
        self._token_provider = token_provider
        self._timeout = timeout
        self._session = requests.Session()

    def _auth_header(self) -> dict[str, str]:
        token = self._token_provider()
        # A falsy token means "send no auth". No Terra endpoint this tool calls is anonymous, so
        # in practice this only ever fires for a deliberately token-less test client.
        return {"Authorization": f"Bearer {token}"} if token else {}

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict] = None,
        json_body: Optional[Any] = None,
        headers: Optional[dict] = None,
        timeout: Optional[float] = None,
        raise_for_status: bool = True,
    ) -> requests.Response:
        """Issue one request. Set ``raise_for_status=False`` when a non-2xx is a *result*.

        The importJob status endpoint is the reason this exists: a 404 there means "Orchestration
        does not know this jobId yet", which the Terra UI treats as still-pending. Raising on it
        would turn a normal early poll into a failed run.
        """
        url = path if path.startswith("http") else f"{self.base_url}/{path.lstrip('/')}"
        all_headers = {"Accept": "application/json", **(headers or {}), **self._auth_header()}

        # Token destination pinning: a bearer token must go ONLY to this client's own HTTPS host --
        # never to a wrong/cross-tier/off-host destination (e.g. an unexpected redirect Location). A
        # bearer is reusable, broad access, so this fails closed. See safety.BearerDestinationError.
        if "Authorization" in all_headers:
            target = urlsplit(url)
            if target.scheme != "https" or target.netloc != self._host:
                logger.warning(
                    "Refused to send a bearer token to %s://%s (expected https://%s).",
                    target.scheme,
                    target.netloc,
                    self._host,
                )
                raise BearerDestinationError(
                    f"Refusing to send a bearer token to {target.scheme}://{target.netloc} "
                    f"(expected https://{self._host}). A token may only go to its own service host."
                )

        # Log the request with secrets redacted: bearer token in headers, plus signed-URL
        # signatures / AWS creds / tokens that can appear in the URL, params, or body (e.g. the
        # importJob body carries the signed Gen3 PFB URL).
        logger.debug("%s %s", method, redact_text(url))
        if params:
            logger.debug("  params: %s", redact_text(str(params)))
        logger.debug("  headers: %s", redact_headers(all_headers))
        if json_body is not None:
            logger.debug("  body: %s", redact_text(json.dumps(json_body)))

        resp = self._session.request(
            method,
            url,
            params=params,
            json=json_body,
            headers=all_headers,
            timeout=self._timeout if timeout is None else timeout,
        )

        # Log the response status and any correlation/trace ids.
        corr = correlation_fields(resp.headers)
        logger.debug(
            "  -> %s %s%s", resp.status_code, resp.reason or "", f"  {corr}" if corr else ""
        )
        if raise_for_status:
            resp.raise_for_status()
        return resp
