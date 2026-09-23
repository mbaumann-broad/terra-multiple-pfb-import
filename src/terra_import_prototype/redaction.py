"""Secret redaction for logs.

The HTTP client logs every request (method, URL, params, body) for cross-org debugging, so those
strings must never carry secrets. This module redacts bearer tokens, JWTs, AWS access keys, and
signed-URL signatures / credentials (including nested and fragment-routed URLs) while preserving
structure (hosts, paths, parameter names).

NOTE: this mirrors the redaction in ``scripts/scrub_har.py`` (which is standalone/stdlib-only for use
without the package). Keep the two in sync; consolidating them is a known follow-up.
"""

from __future__ import annotations

import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

REDACTED = "REDACTED"

# Query / form / JSON parameter names whose value is a secret (value redacted, name kept).
SENSITIVE_PARAM_NAMES = frozenset(
    {
        # signed-URL signatures
        "signature",
        "x-goog-signature",
        "x-amz-signature",
        "sig",
        # AWS presigned-URL credentials
        "awsaccesskeyid",
        "x-amz-security-token",
        "x-amz-credential",
        # OAuth / API tokens & secrets
        "access_token",
        "accesstoken",
        "token",
        "id_token",
        "idtoken",
        "refresh_token",
        "refreshtoken",
        "code",
        "code_verifier",
        "client_secret",
        "clientsecret",
        "secret",
        "password",
        "passwd",
        "api_key",
        "apikey",
        "key",
    }
)

_BEARER_RE = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/\-]+=*")
_JWT_SEG = r"[A-Za-z0-9+/_-]+={0,2}"
_JWT_RE = re.compile(r"\beyJ[A-Za-z0-9+/_-]+={0,2}\." + _JWT_SEG + r"\." + _JWT_SEG)
_URL_RE = re.compile(r"https?://[^\s\"'<>\\]+")
_AWS_KEY_RE = re.compile(r"\b(?:A3T[A-Z0-9]|AKIA|AGPA|AIDA|AROA|AIPA|ANPA|ANVA|ASIA)[A-Z0-9]{16}")


def _norm(name: object) -> str:
    return name.strip().lower() if isinstance(name, str) else ""


def _redact_query(query: str) -> tuple[str, bool]:
    if not query:
        return query, False
    changed = False
    pairs = []
    for key, value in parse_qsl(query, keep_blank_values=True):
        if _norm(key) in SENSITIVE_PARAM_NAMES and value != REDACTED:
            pairs.append((key, REDACTED))
            changed = True
        elif "://" in value:  # value is itself a URL (e.g. a nested signed URL)
            redacted = redact_url(value)
            pairs.append((key, redacted))
            changed = changed or redacted != value
        else:
            pairs.append((key, value))
    return (urlencode(pairs), True) if changed else (query, False)


def redact_url(url: str) -> str:
    """Redact sensitive query params (incl. fragment-routed and nested) in a URL."""
    if not isinstance(url, str) or "://" not in url:
        return url
    try:
        parts = urlsplit(url)
    except ValueError:
        return url
    new_query, q_changed = _redact_query(parts.query)
    fragment, f_changed = parts.fragment, False
    if "?" in fragment:
        fpath, fquery = fragment.split("?", 1)
        new_fquery, f_changed = _redact_query(fquery)
        if f_changed:
            fragment = fpath + "?" + new_fquery
    if not (q_changed or f_changed):
        return url
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, fragment))


def redact_text(text: str) -> str:
    """Redact bearer tokens, JWTs, AWS access keys, and signed-URL secrets in any free text."""
    if not isinstance(text, str) or not text:
        return text
    text = _BEARER_RE.sub("Bearer " + REDACTED, text)
    text = _JWT_RE.sub(REDACTED, text)
    text = _AWS_KEY_RE.sub(REDACTED, text)
    text = _URL_RE.sub(lambda m: redact_url(m.group(0)), text)
    return text
