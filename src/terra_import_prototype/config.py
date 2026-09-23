"""Configuration loading and tier resolution.

Per-developer settings (Google identity, ADC credentials file, billing project) live in a YAML file
(gitignored ``config/config.yaml``; see ``config/config.example.yaml``). The shared, non-secret
service URLs are baked in here -- mirroring ``docs/services.md`` -- so they are not duplicated in
every developer's config.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, field_validator

from .workspace import MAX_NAME_LENGTH

# Per-tier service base URLs. Mirrors docs/services.md (the single source of truth for endpoints).
#
# Gen3 BioData Catalyst is deliberately absent: it is not a service this tool calls. Its pre-signed
# export URL is the *input*, and where that URL may legitimately come from is a safety concern, not a
# routing one -- see safety.SIGNED_URL_ALLOWED_PREFIXES.
SERVICE_URLS: dict[str, dict[str, str]] = {
    "dev": {
        "firecloud": "https://firecloud-orchestration.dsde-dev.broadinstitute.org/",
        "rawls": "https://rawls.dsde-dev.broadinstitute.org/",
        "sam": "https://sam.dsde-dev.broadinstitute.org/",
    },
    "prod": {
        "firecloud": "https://api.firecloud.org/",
        "rawls": "https://rawls.dsde-prod.broadinstitute.org/",
        "sam": "https://sam.dsde-prod.broadinstitute.org/",
    },
}

TIERS: tuple[str, ...] = tuple(SERVICE_URLS.keys())


#: Terra workspace names are ``[A-Za-z0-9_-]``. Enforced here rather than left to Rawls because this
#: name aims a DELETE (see docs/import_flow.md step 2): a typo that would 404 harmlessly on a GET is
#: better caught while loading config than after the run has started resolving a destination.
_WORKSPACE_NAME_RE = re.compile(r"\A[A-Za-z0-9_-]+\Z")


def _check_workspace_name(value: Optional[str]) -> Optional[str]:
    """Validate a configured workspace name, or pass through ``None`` (= generate one per run)."""
    if value is None:
        return None
    name = value.strip()
    if not name:
        # An empty string is a half-filled template, not a request for the generated name -- say so
        # rather than silently behaving like an omitted key.
        raise ValueError(
            "default_workspace_name is empty. Omit the key entirely to get a generated "
            "per-run workspace name, or give it a real name."
        )
    if not _WORKSPACE_NAME_RE.match(name):
        raise ValueError(
            f"default_workspace_name {name!r} is not a legal Terra workspace name: "
            "only letters, digits, '_' and '-' are allowed."
        )
    if len(name) > MAX_NAME_LENGTH:
        raise ValueError(
            f"default_workspace_name is {len(name)} characters; Terra names are capped at "
            f"{MAX_NAME_LENGTH}."
        )
    return name


class TierConfig(BaseModel):
    """Per-developer settings for one tier."""

    email: str
    # None -> use the ambient ADC identity (e.g. a Terra workflow's runtime service account).
    adc_credentials_file: Optional[str] = None
    # The Terra billing project (a.k.a. workspace namespace) that import runs create workspaces under.
    terra_billing_project: str
    # Additional human Terra users authorized to run this tier (multi-operator). The identity guard
    # accepts any account in {email} u authorized_users.
    authorized_users: list[str] = []
    # The workspace (within terra_billing_project) that runs import into. Reused across runs: if it
    # exists and is empty it is adopted, if it exists with data it is deleted and re-created, and if
    # it is absent it is created (docs/import_flow.md step 2). None -> fall back to the legacy
    # generated per-run name (workspace.workspace_name), which creates a new workspace every run.
    default_workspace_name: Optional[str] = None

    _validate_default_workspace_name = field_validator("default_workspace_name")(
        _check_workspace_name
    )


class AppConfig(BaseModel):
    default_tier: str = "dev"
    tiers: dict[str, TierConfig]


class ResolvedTier(BaseModel):
    """A tier's per-developer config combined with its (baked-in) service URLs."""

    name: str
    email: str
    adc_credentials_file: Optional[str]
    terra_billing_project: str
    authorized_users: list[str] = []
    #: None -> generate a per-run workspace name instead of reusing a configured one.
    default_workspace_name: Optional[str] = None
    service_urls: dict[str, str]

    def url(self, service: str) -> str:
        return self.service_urls[service]

    @property
    def authorized_emails(self) -> set[str]:
        """Human Terra users allowed to run this tier (lowercased): ``{email}`` u ``authorized_users``."""
        return {e.strip().lower() for e in [self.email, *self.authorized_users] if e and e.strip()}


def load_config(path: Path) -> AppConfig:
    if not path.exists():
        raise FileNotFoundError(
            f"Config not found: {path}. Copy config/config.example.yaml to {path} and fill it in."
        )
    data = yaml.safe_load(path.read_text()) or {}
    return AppConfig.model_validate(data)


def resolve_tier(config: AppConfig, tier_name: Optional[str] = None) -> ResolvedTier:
    name = tier_name or config.default_tier
    if name not in SERVICE_URLS:
        raise ValueError(f"Unknown tier '{name}'. Known tiers: {', '.join(TIERS)}.")
    if name not in config.tiers:
        raise ValueError(f"Tier '{name}' is not configured in your config file.")
    tc = config.tiers[name]
    return ResolvedTier(
        name=name,
        email=tc.email,
        adc_credentials_file=tc.adc_credentials_file,
        terra_billing_project=tc.terra_billing_project,
        authorized_users=tc.authorized_users,
        default_workspace_name=tc.default_workspace_name,
        service_urls=SERVICE_URLS[name],
    )
