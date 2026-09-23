"""Authentication via Google Application Default Credentials (ADC), with a tier identity guard.

One human Terra user account per tier. Locally the credentials come from a per-tier saved ADC file; in
deployment, from the ambient runtime identity (a Terra **pet service account**). The same credentials
yield the REST bearer tokens for every Terra service this tool calls. Before any tier work, the identity
guard resolves the **canonical Terra user** behind the active credentials via Sam
(``/api/users/v2/self``, which maps a pet service account to its owning human user) and asserts it
equals the configured email for the selected tier.
"""

from __future__ import annotations

import logging
from pathlib import Path

import google.auth
import requests
from google.auth.credentials import Credentials
from google.auth.transport.requests import Request

from .config import ResolvedTier
from .logging_setup import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)

# Scopes granted by a plain `gcloud auth application-default login` (openid, email, cloud-platform).
SCOPES = (
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/cloud-platform",
)

USERINFO_URL = "https://openidconnect.googleapis.com/v1/userinfo"

#: Sam endpoint that maps an access token (a human login OR a Terra pet service account) to the
#: registered Terra user, returning their human email -- so the identity guard treats a pet SA as its
#: owning user (Sam Swagger: GET /api/users/v2/self).
SAM_SELF_PATH = "/api/users/v2/self"


class IdentityMismatchError(RuntimeError):
    """Raised when the active credential's identity does not match the configured tier email."""


def credentials_for_tier(tier: ResolvedTier) -> Credentials:
    """Load ADC credentials for the tier: a per-tier file if configured, else the ambient ADC."""
    if tier.adc_credentials_file:
        path = str(Path(tier.adc_credentials_file).expanduser())
        creds, _ = google.auth.load_credentials_from_file(path, scopes=SCOPES)
    else:
        creds, _ = google.auth.default(scopes=SCOPES)
    return creds


def bearer_token(creds: Credentials) -> str:
    """Refresh (if needed) and return the OAuth2 access token for `Authorization: Bearer` headers."""
    if not creds.valid:
        creds.refresh(Request())
    return creds.token


def active_identity_email(creds: Credentials) -> str:
    """Resolve the Google account email behind the active credentials (OIDC userinfo)."""
    token = bearer_token(creds)
    resp = requests.get(USERINFO_URL, headers={"Authorization": f"Bearer {token}"}, timeout=30)
    resp.raise_for_status()
    email = resp.json().get("email")
    if not email:
        raise RuntimeError("Could not resolve the active account email from the userinfo endpoint.")
    return email


def terra_user_email(creds: Credentials, sam_url: str) -> str:
    """Resolve the canonical Terra user email behind the active credentials, via Sam.

    Works for both a human ADC login and a Terra pet service account: Sam's ``/api/users/v2/self``
    maps the access token to the registered Terra user and returns their (human) email. This lets the
    identity guard treat a pet service account as its owning human user.
    """
    token = bearer_token(creds)
    url = f"{sam_url.rstrip('/')}{SAM_SELF_PATH}"
    try:
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(
            f"Could not resolve the Terra user from Sam ({url}): {exc}. Is the active account a "
            "registered Terra user, and is Sam reachable?"
        ) from exc
    email = resp.json().get("email")
    if not email:
        raise RuntimeError("Sam /api/users/v2/self returned no email for the active identity.")
    return email


def assert_tier_identity(creds: Credentials, tier: ResolvedTier) -> str:
    """Identity guard: abort unless the active credentials' Terra user is authorized for the tier.

    Resolves the canonical Terra user via Sam (mapping a pet service account to its owning human user)
    and asserts it is in ``tier.authorized_emails`` (= ``email`` ∪ ``authorized_users``, so multiple
    operators are supported). The raw ADC identity (e.g. the pet SA in a Terra workflow) is logged for
    the audit trail. Returns the resolved Terra-user email.
    """
    # Raw ADC account (e.g. the pet SA) -- audit-only now that Sam is the authority, so best-effort:
    # never let this lookup block an otherwise-valid run.
    try:
        adc_identity = active_identity_email(creds)
    except (requests.RequestException, RuntimeError) as exc:
        logger.debug("Could not resolve the raw ADC identity for the audit log: %s", exc)
        adc_identity = "unknown"
    terra_user = terra_user_email(creds, tier.url("sam"))
    if terra_user.lower() not in tier.authorized_emails:
        raise IdentityMismatchError(
            f"Active Terra user '{terra_user}' (ADC identity '{adc_identity}') is not authorized for "
            f"tier '{tier.name}' (authorized: {sorted(tier.authorized_emails)}). Refusing to proceed "
            "(wrong-tier / unauthorized-user guard)."
        )
    logger.info(
        "Identity guard OK: Terra user %s (ADC identity %s; tier '%s').",
        terra_user,
        adc_identity,
        tier.name,
    )
    return terra_user
