"""Turn the operator's signed URL(s) into the list of PFB URLs to import.

Two input shapes, one result. A signed **Avro** URL is the request; a signed **manifest** URL is
fetched and expands to N of them. Both produce an :class:`ImportRequest`, so everything downstream --
provenance checks, fan-out, polling, QC -- is the same code for both.

An operator may supply several signed URLs at once. :func:`build_run` builds each one independently
and then checks them as a whole, because they all import into **one** workspace: the fan-out's width
and its duplicate URLs are properties of the run, not of any single source.

The validation here is a faithful port of terra-ui's ``useImportRequest.getImportRequest`` and the
client-side host allow-list, not a convenience wrapper: a URL this module accepts must be one the
real Terra UI would accept, and one cWDS's ``twds.data-import.allowed-hosts`` would accept after it.
Rejecting here rather than letting Orchestration reject is what makes a bad manifest create *zero*
jobs instead of failing partway through a fan-out.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional, Sequence
from urllib.parse import urlsplit

import requests

from .logging_setup import LOGGER_NAME
from .models import ImportRequest, ImportRun, RequestKind
from .safety import SignedUrl, assert_uniform_provenance, fetch_signed_json

logger = logging.getLogger(LOGGER_NAME)

#: Hosts a PFB URL may point at. Mirrors cWDS's ``twds.data-import.allowed-hosts``; the UI applies
#: the same check client-side so a bad host is rejected before a job is created.
#:
#: The virtual-hosted-style S3 pattern is the one that matters in practice: BDC serves exports from
#: ``gen3-biodatacatalyst-nhlbi-nih-gov-pfb-export.s3.amazonaws.com`` (bucket in the host), which the
#: path-style ``s3.amazonaws.com`` pattern does not match. An allow-list without it rejects every
#: real BDC export. Verified against ``recorded.har`` (2026-09-21).
DEFAULT_ALLOWED_HOST_PATTERNS: tuple[str, ...] = (
    r"^storage\.googleapis\.com$",
    r"^.*\.core\.windows\.net$",
    r"^s3\.amazonaws\.com$",
    r"^.*\.s3\.amazonaws\.com$",
    r"^.*\.s3[.-][a-z0-9-]+\.amazonaws\.com$",
    r"^gen3\..*\.org$",
    r"^gen3\.biodatacatalyst\.nhlbi\.nih\.gov$",
)

#: Upper bound on a single fan-out. Not a Terra limit -- a self-imposed one, because N importJobs is
#: N concurrent translations inside cWDS and a runaway manifest is a denial of service against a
#: shared service. Raise it deliberately, with the cWDS team, not because a manifest was big.
MAX_URLS = 100


class ImportRequestError(ValueError):
    """The operator's URL could not be turned into a valid set of PFB URLs to import."""


def infer_kind(signed_url: SignedUrl) -> RequestKind:
    """Guess whether a signed URL points at an Avro export or a manifest, from its path.

    A heuristic, and named like one. Gen3 mints ``…/export_<timestamp>.avro`` for a single export and
    ``…/manifest.json`` for a multi-file one, so the extension is reliable in practice -- but it is
    the *path*, not content negotiation, so the CLI always offers ``--kind`` to override it. An
    unrecognised extension raises rather than guessing: importing a manifest as if it were a PFB
    produces a confusing cWDS parse failure several minutes later, which is a worse answer than
    "tell me which one this is".
    """
    path = urlsplit(signed_url.reveal()).path.lower()
    if path.endswith(".avro"):
        return "avro"
    if path.endswith(".json"):
        return "manifest"
    raise ImportRequestError(
        f"Cannot tell whether {signed_url.location} is a PFB export or a manifest (expected a path "
        "ending in .avro or .json). Pass --kind avro|manifest explicitly."
    )


def build_request(
    signed_url: SignedUrl,
    *,
    kind: Optional[RequestKind] = None,
    tier_name: str,
    allowed_prefixes: tuple[str, ...],
    allowed_host_patterns: tuple[str, ...] = DEFAULT_ALLOWED_HOST_PATTERNS,
    max_urls: int = MAX_URLS,
    session: Optional[requests.Session] = None,
) -> ImportRequest:
    """Build the import request from the operator's signed URL, fetching a manifest if needed.

    The single entry point for both shapes. Order matters and is deliberate:

    1. Decide the kind (explicit ``kind`` wins over :func:`infer_kind`).
    2. For a manifest: verify provenance, dereference it, normalise it to a URL list, then require
       **every** expanded URL to pass the same provenance allow-list (``assert_uniform_provenance``)
       -- a manifest must not be able to name URLs its own bucket could not.
    3. Validate every URL against the host allow-list and the count max_worker.

    Nothing is dispatched until all of that passes, so a rejected request creates no jobs at all.
    """
    kind = kind or infer_kind(signed_url)

    if kind == "avro":
        urls = [signed_url]
        raw: Any = None
        source_label = f"the --url argument ({signed_url.filename})"
    else:
        raw = fetch_signed_json(
            signed_url, tier_name=tier_name, allowed_prefixes=allowed_prefixes, session=session
        )
        urls = [SignedUrl(u) for u in _extract_urls(raw, signed_url)]
        source_label = f"manifest {signed_url.filename}"
        logger.info("Manifest %s names %d PFB URL(s).", signed_url.filename, len(urls))
        assert_uniform_provenance(
            urls, tier_name=tier_name, allowed_prefixes=allowed_prefixes, source=source_label
        )

    _validate(urls, source_label, allowed_host_patterns=allowed_host_patterns, max_urls=max_urls)
    return ImportRequest(kind=kind, source=signed_url, urls=tuple(urls), raw=raw)


def build_run(
    signed_urls: Sequence[SignedUrl],
    *,
    kind: Optional[RequestKind] = None,
    tier_name: str,
    allowed_prefixes: tuple[str, ...],
    allowed_host_patterns: tuple[str, ...] = DEFAULT_ALLOWED_HOST_PATTERNS,
    max_urls: int = MAX_URLS,
    session: Optional[requests.Session] = None,
) -> ImportRun:
    """Build the whole run -- one or N operator-supplied signed URLs, all bound for one workspace.

    Each URL goes through :func:`build_request` on its own (so a manifest is still fetched, expanded
    and provenance-checked exactly as before), and then the merged result is checked as a whole by
    :func:`_validate_run`. ``kind`` applies to **every** URL given: it overrides the per-URL extension
    guess, so a run of mixed shapes must let the extensions speak for themselves.

    Nothing is dispatched until every source and the run as a whole pass, so a bad URL anywhere in
    the list creates *zero* jobs rather than importing the first two and failing on the third -- the
    same reason a bad manifest entry rejects the whole manifest.
    """
    if not signed_urls:
        raise ImportRequestError("No signed URL to import.")

    run = ImportRun(
        tuple(
            build_request(
                signed_url,
                kind=kind,
                tier_name=tier_name,
                allowed_prefixes=allowed_prefixes,
                allowed_host_patterns=allowed_host_patterns,
                max_urls=max_urls,
                session=session,
            )
            for signed_url in signed_urls
        )
    )
    _validate_run(run, max_urls=max_urls)
    if len(run.requests) > 1:
        logger.info(
            "Run: %d source(s) expanded to %d PFB URL(s), all importing into one workspace.",
            len(run.requests),
            len(run.urls),
        )
    return run


def _validate_run(run: ImportRun, *, max_urls: int) -> None:
    """Check the merged run, after every source has already been checked on its own.

    Two things only become checkable once the sources are merged, and both matter precisely *because*
    everything now lands in one workspace:

    1. **Total fan-out width.** ``MAX_URLS`` bounds one fan-out, and the run is one fan-out. Checking
       it per source would let four 40-URL manifests become a 160-way fan-out into cWDS.
    2. **Duplicates across sources.** Two manifests naming the same PFB, or the same file passed
       twice, would import it twice into the same entity tables. Within one source that is already a
       rejection (see :func:`_validate`); across sources it has the same effect and the same cause --
       an operator listing something twice.

    Compared on :class:`SignedUrl` identity, which is value equality on the full URL, so no signed
    URL is unwrapped here.
    """
    errors: list[str] = []

    if len(run.urls) > max_urls:
        errors.append(
            f"{len(run.requests)} source(s) yielded {len(run.urls)} URLs in total, over the "
            f"self-imposed limit of {max_urls} for one fan-out (see manifest.MAX_URLS)"
        )

    seen: set[SignedUrl] = set()
    for request in run.requests:
        for url in request.urls:
            if url in seen:
                errors.append(
                    f"duplicate URL across sources: {url.filename} (from {request.source.filename})"
                )
            seen.add(url)

    if errors:
        raise ImportRequestError(
            f"This run is not importable ({len(errors)} problem(s)):\n  - " + "\n  - ".join(errors)
        )


# TODO Eugene Need to check this will extract the Avro URLs correctly
def _extract_urls(raw: Any, source: SignedUrl) -> list[str]:
    """Normalise every manifest shape the Terra UI accepts into a flat URL list.

    Accepted: ``{"urls": [...]}``, ``{"files": [{"url": ...}]}``, ``{"url": ...}`` (the single-file
    shape Gen3 emits today), and a bare list of strings or objects. Ported from terra-ui rather than
    narrowed to the shape we happen to have seen -- a manifest the UI would import must import here.
    """
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        for key in ("urls", "files", "sources", "pfbs"):
            if key in raw:
                items = raw[key]
                break
        else:
            items = [raw] if "url" in raw else []
        if not isinstance(items, list):
            raise ImportRequestError(
                f"Manifest {source.location}: expected a list of URLs, got {type(items).__name__}."
            )
    else:
        raise ImportRequestError(
            f"Manifest {source.location}: root must be an object or a list, got "
            f"{type(raw).__name__}."
        )

    urls: list[str] = []
    for index, item in enumerate(items):
        if isinstance(item, str):
            urls.append(item)
        elif isinstance(item, dict) and isinstance(item.get("url"), str):
            urls.append(item["url"])
        else:
            # Do not echo the entry: a malformed manifest can still carry signed URLs in it.
            raise ImportRequestError(
                f"Manifest {source.location}: entry {index} has no 'url' string."
            )
    return urls


def _host(url: str) -> Optional[str]:
    match = re.match(r"^https?://([^/:?#]+)", url, re.IGNORECASE)
    return match.group(1).lower() if match else None


def _validate(
    urls: list[SignedUrl],
    source_label: str,
    *,
    allowed_host_patterns: tuple[str, ...],
    max_urls: int,
) -> None:
    """Reject bad input before any job is created. Reports *all* problems, not just the first.

    One message listing four bad URLs is one round-trip for the operator; four runs that each fail on
    the next URL is four. The messages name hosts and filenames only -- never a signed URL.
    """
    errors: list[str] = []

    if not urls:
        errors.append(f"{source_label} yielded no URLs to import")
    if len(urls) > max_urls:
        errors.append(
            f"{source_label} yielded {len(urls)} URLs, over the self-imposed max_worker of {max_urls} "
            "(see manifest.MAX_URLS)"
        )

    seen: set[str] = set()
    for url in urls:
        raw = url.reveal()
        host = _host(raw)
        if host is None:
            errors.append(f"not an http(s) URL: {url.location}")
            continue
        if not any(re.match(pattern, host) for pattern in allowed_host_patterns):
            errors.append(f"host {host} is not on the allow-list ({url.filename})")
        # Duplicates are compared on the full URL: the same object signed twice is two different
        # strings, and importing it twice is wasteful but not wrong. Identical URLs are a mistake.
        if raw in seen:
            errors.append(f"duplicate URL in {source_label}: {url.filename}")
        seen.add(raw)

    if errors:
        raise ImportRequestError(
            f"{source_label} is not importable ({len(errors)} problem(s)):\n  - "
            + "\n  - ".join(errors)
        )
