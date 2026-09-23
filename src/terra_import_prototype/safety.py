"""Safety controls for handling Gen3 pre-signed export URLs.

A BDC pre-signed URL grants direct read of an exported cohort -- for a controlled-access study, of
NIH controlled data. Mishandling one (importing a prod URL into the insecure dev tier, logging it,
returning it) would be a Federal Data Management Incident (DMI). The URL is the only credential in
this flow and it travels through the whole pipeline, so it is wrapped rather than passed as a string.

Four layers of defense-in-depth (do not weaken any without security review):

1. **Secret containment** -- every signed URL is wrapped in :class:`SignedUrl`, which redacts on
   repr/str/format and is not JSON-serializable. The raw value is reachable only via ``.reveal()``,
   called at exactly **two** sites, both in this package and both documented:
   ``clients/firecloud.submit_import_job`` (the importJob body) and :func:`fetch_signed_json` (the
   manifest dereference below). Do not add a third.
2. **Hand-off verification** -- :func:`verify_pfb_handoff` refuses unless the import destination is
   the run tier's canonical Firecloud host, and the signed URL comes from an allow-listed source --
   so a URL from one tier or an unexpected bucket can never be delivered.
3. **Fan-out containment** -- a manifest expands one signed URL into N, and those N were chosen by
   whoever wrote the manifest, not by us. :func:`assert_uniform_provenance` requires every expanded
   URL to satisfy the same allow-list as the manifest that named them, so a manifest cannot smuggle
   an off-list URL into the import. See :func:`fetch_signed_json`.
4. **Bearer-token destination pinning** -- the HTTP base client (``clients/base.py``) raises
   ``BearerDestinationError`` if a bearer token would be sent to any host but the client's own HTTPS
   host, so a Terra token can only ever reach its intended service.

What this module deliberately does **not** do is gate on consent code. The reference project
(anvil-data-qc) reads a TDR snapshot's ``consentCode`` and fails closed on anything but ``NRES``.
There is no equivalent here: the input is a signed URL, and a signed URL carries no consent
metadata -- by the time this tool sees one, the export has already happened and the operator's own
Gen3 credentials authorized it. The gate that remains is provenance: we verify *where the URL came
from* and *where it is going*, which is the part still in our hands. Treat every run as potentially
controlled-access.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional
from urllib.parse import urlsplit

import requests

from .logging_setup import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


class SafetyError(RuntimeError):
    """Base for safety-gate aborts (the CLI presents these cleanly and exits non-zero)."""


# --- Layer 1: secret containment ---


class SignedUrl:
    """Opaque holder for a sensitive pre-signed export URL.

    The raw value grants direct read of exported study data, so it must not leak. This wrapper
    **redacts on repr/str/format** and is **not JSON-serializable**; the raw value is reachable only
    via :meth:`reveal`, which is called at exactly two sites (see the module docstring).
    :attr:`location` (scheme+host+path, no query) and :attr:`host` are safe to log and are what the
    destination/provenance checks operate on.
    """

    __slots__ = ("_url",)

    def __init__(self, url: str) -> None:
        self._url = url

    def reveal(self) -> str:
        """Return the raw signed URL. Call ONLY at the two documented hand-off sites."""
        return self._url

    @property
    def host(self) -> str:
        return urlsplit(self._url).netloc

    @property
    def location(self) -> str:
        """``scheme://host/path`` -- the URL with the secret query string removed (safe to log)."""
        parts = urlsplit(self._url)
        return f"{parts.scheme}://{parts.netloc}{parts.path}"

    @property
    def filename(self) -> str:
        """The last path segment (e.g. ``export_2026-09-21T16:50:59.avro``). Safe to log."""
        return urlsplit(self._url).path.rsplit("/", 1)[-1]

    def __repr__(self) -> str:
        return f"<SignedUrl {self.location} ?…redacted>"

    __str__ = __repr__

    def __format__(self, _spec: str) -> str:
        return self.__repr__()

    def __eq__(self, other: object) -> bool:
        return isinstance(other, SignedUrl) and other._url == self._url

    def __hash__(self) -> int:
        return hash(self._url)


# --- Layer 2: hand-off verification (right destination, right provenance) ---

#: Allow-listed signed-URL location prefixes: where a PFB export may legitimately come from.
#:
#: Verified against the real capture in ``recorded.har`` (2026-09-21): BDC exports are served from
#: ``gen3-biodatacatalyst-nhlbi-nih-gov-pfb-export.s3.amazonaws.com``, i.e. **virtual-hosted-style**
#: S3 (bucket in the host), not path-style ``s3.amazonaws.com/<bucket>/``. Each prefix carries the
#: full host and a trailing ``/`` so the startswith check is anchored at the bucket boundary -- a
#: sibling bucket (``...-pfb-export-attacker.s3.amazonaws.com``) cannot satisfy it.
#:
#: Keyed by tier because the destination is tier-specific, not the source: Gen3 BDC has no Broad dev
#: tier, so **dev and prod share the same allowed source**. That is deliberate and is the one place
#: this tool's provenance model is weaker than the reference project's -- a prod-origin export URL
#: may legitimately be imported into dev Terra for testing. The destination check still pins where
#: it goes.
SIGNED_URL_ALLOWED_PREFIXES: dict[str, tuple[str, ...]] = {
    "dev": ("https://gen3-biodatacatalyst-nhlbi-nih-gov-pfb-export.s3.amazonaws.com/",),
    "prod": ("https://gen3-biodatacatalyst-nhlbi-nih-gov-pfb-export.s3.amazonaws.com/",),
}


class PfbHandoffError(SafetyError):
    """Raised when a signed URL would be delivered to an unverified destination or source."""


class SignedUrlDestinationError(PfbHandoffError):
    """The import destination host is not the run tier's canonical Firecloud host."""


class SignedUrlProvenanceError(PfbHandoffError):
    """The signed URL did not originate from an allow-listed source."""


def verify_pfb_handoff(
    signed_url: SignedUrl,
    *,
    tier_name: str,
    expected_firecloud_host: str,
    destination_host: str,
    allowed_prefixes: tuple[str, ...],
) -> None:
    """Verify one signed URL is delivered only to the intended, same-tier import destination.

    - **Destination integrity:** refuse unless ``destination_host`` equals the tier's canonical
      Firecloud host (so the URL goes only to this tier's import service).
    - **Provenance:** when ``allowed_prefixes`` is set, the signed URL's location must start with one
      of them; when empty, warn but do not block.
    - **Audit:** log the destination host (no secret) on success.

    Called **once per import job**, not once per run: under fan-out the manifest's N URLs are N
    separate hand-offs, and checking only the first would leave N-1 unverified.
    """
    if destination_host != expected_firecloud_host:
        raise SignedUrlDestinationError(
            f"Import destination host {destination_host!r} does not match the expected Firecloud "
            f"host {expected_firecloud_host!r} for tier {tier_name!r}. Refusing to deliver the PFB "
            "signed URL."
        )
    if allowed_prefixes:
        if not any(signed_url.location.startswith(prefix) for prefix in allowed_prefixes):
            raise SignedUrlProvenanceError(
                f"Signed URL {signed_url.location!r} is not from an allow-listed source for tier "
                f"{tier_name!r}. Refusing to deliver it."
            )
    else:
        logger.warning(
            "No signed-URL provenance allow-list configured for tier %r; source not verified "
            "(destination check still applies). Populate SIGNED_URL_ALLOWED_PREFIXES[%r].",
            tier_name,
            tier_name,
        )
    logger.info(
        "PFB hand-off verified: %s -> Firecloud host %s (tier %s).",
        signed_url.filename or signed_url.location,
        destination_host,
        tier_name,
    )


# --- Layer 3: fan-out containment ---


def assert_uniform_provenance(
    urls: list[SignedUrl], *, tier_name: str, allowed_prefixes: tuple[str, ...], source: str
) -> None:
    """Every URL a manifest expanded to must satisfy the same allow-list as the manifest itself.

    A manifest is a list of URLs written by someone else. Without this check, a manifest fetched from
    an allow-listed bucket could name N URLs pointing anywhere, and the fan-out would dutifully hand
    each one to Terra -- the allow-list would have gated the *index* while leaving the *contents*
    unchecked. Enforced before any job is created, so a bad manifest creates none at all rather than
    partially importing and failing on entry 4.
    """
    if not allowed_prefixes:
        logger.warning(
            "No provenance allow-list for tier %r; the %d URLs from %s are not source-verified.",
            tier_name,
            len(urls),
            source,
        )
        return
    offenders = [
        u.location for u in urls if not any(u.location.startswith(p) for p in allowed_prefixes)
    ]
    if offenders:
        raise SignedUrlProvenanceError(
            f"{len(offenders)} of {len(urls)} URLs named by {source} are not from an allow-listed "
            f"source for tier {tier_name!r}: {offenders[:5]}"
            f"{' ...' if len(offenders) > 5 else ''}. Refusing to create any import job."
        )
    logger.info(
        "Provenance verified for all %d URL(s) from %s (tier %s).", len(urls), source, tier_name
    )


class ManifestFetchError(SafetyError):
    """The manifest URL could not be dereferenced, or did not return JSON."""


def fetch_signed_json(
    signed_url: SignedUrl,
    *,
    tier_name: str,
    allowed_prefixes: tuple[str, ...],
    timeout: float = 60.0,
    session: Optional[requests.Session] = None,
) -> Any:
    """Dereference a signed **manifest** URL and return the parsed JSON.

    The second (and last) ``reveal()`` site. Contained here rather than in a client so that the one
    place a manifest URL is turned back into a string is next to the rules that govern it:

    - Provenance is checked **first**: an off-list URL is never dereferenced at all, so this cannot
      be used to make an outbound request to an arbitrary host.
    - **No bearer token is sent.** The signature in the query string *is* the authorization; adding a
      Terra token would hand a Terra credential to an S3 host, which is exactly what the base
      client's destination pinning exists to prevent. This is why the fetch does not go through
      ``BaseClient``.
    - Redirects are not followed, for the same reason: a redirect would move the (still-signed) URL
      to a host nobody allow-listed.
    - Nothing derived from the URL reaches the exception messages -- only its safe ``location``.
    """
    if allowed_prefixes and not any(
        signed_url.location.startswith(prefix) for prefix in allowed_prefixes
    ):
        raise SignedUrlProvenanceError(
            f"Manifest URL {signed_url.location!r} is not from an allow-listed source for tier "
            f"{tier_name!r}. Refusing to dereference it."
        )
    logger.info("Fetching manifest %s (no bearer token sent).", signed_url.location)
    http = session or requests
    try:
        response = http.get(signed_url.reveal(), timeout=timeout, allow_redirects=False)
    except requests.RequestException as exc:
        raise ManifestFetchError(
            f"Could not fetch the manifest at {signed_url.location}: {type(exc).__name__}. "
            "Has the signed URL expired?"
        ) from exc
    if response.status_code != 200:
        raise ManifestFetchError(
            f"Manifest fetch for {signed_url.location} returned HTTP {response.status_code}. "
            "A 403 usually means the signed URL has expired; a 3xx means it redirected, which is "
            "refused."
        )
    try:
        return json.loads(response.content)
    except json.JSONDecodeError as exc:
        raise ManifestFetchError(
            f"The manifest at {signed_url.location} is not valid JSON: {exc}. If this URL points at "
            "an Avro export rather than a manifest, run with --kind avro."
        ) from exc


# --- Layer 4: bearer-token destination pinning (enforced in clients/base.py) ---


class BearerDestinationError(SafetyError):
    """Raised when a bearer token would be sent to a host other than the client's own HTTPS host."""
