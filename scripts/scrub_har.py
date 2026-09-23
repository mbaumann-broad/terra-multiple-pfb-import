#!/usr/bin/env python3
"""Scrub sensitive data from a Chrome DevTools HAR capture.

Reads a .har file, redacts credentials and other secrets while *preserving the
structure* that makes the capture useful (header names, URL hosts/paths, query
parameter names, JSON keys), and writes a new ``*.scrubbed.har`` file. The
original file is never modified.

What it redacts
---------------
- Sensitive request/response **headers** (Authorization, Cookie, Set-Cookie, ...)
  -- value redacted, header name kept.
- **Cookies** (request and response) -- values always redacted.
- Sensitive **query-string / form / JSON** parameters, including signed-URL
  **signatures** (X-Goog-Signature, X-Amz-Signature, Signature, sig). Signed
  URLs are parsed with ``urllib`` for reliable handling of encoding/ordering;
  the host, path, and parameter *names* are preserved.
- **Bearer tokens** and **JWTs** wherever they appear in URLs, header values, or
  request/response bodies (regex).

This is a **best-effort aid, not a guarantee.** Always review the scrubbed
output before sharing, and extend the ``SENSITIVE_*`` / regex sets below if a
particular capture carries service-specific secrets. After scrubbing, the script
runs a residual-secret scan and warns if anything secret-looking remains.

Usage
-----
    python scripts/scrub_har.py capture.har                 # -> capture.scrubbed.har
    python scripts/scrub_har.py capture.har -o cleaned.har  # explicit output path

Standard library only; requires Python 3.x. No third-party dependencies.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl

REDACTED = "REDACTED"

# --------------------------------------------------------------------------
# Configuration -- extend these sets for capture-specific secrets.
# --------------------------------------------------------------------------

# Header names whose VALUE is fully redacted (case-insensitive). Names are kept.
SENSITIVE_HEADER_NAMES = {
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "x-api-key",
    "x-goog-api-key",
    "x-goog-iam-authorization-token",
    "x-auth-token",
    "x-amz-security-token",
    "x-csrf-token",
    "x-xsrf-token",
}

# Query-string / form / JSON key names whose VALUE is fully redacted. Names kept.
SENSITIVE_PARAM_NAMES = {
    # signed-URL signatures
    "signature",
    "x-goog-signature",
    "x-amz-signature",
    "sig",
    # AWS presigned-URL credentials (SigV2: AWSAccessKeyId/Signature; SigV4: X-Amz-*)
    "awsaccesskeyid",
    "x-amz-security-token",
    "x-amz-credential",
    # NOTE: Google's signing identity (X-Goog-Credential / GoogleAccessId, the SA email) is
    # deliberately NOT redacted -- it is an identity, not a secret, and is structurally useful.
    # OAuth / API tokens & secrets
    "access_token",
    "accesstoken",
    "token",
    "id_token",
    "idtoken",
    "refresh_token",
    "refreshtoken",
    # OAuth / OIDC authorization-flow credentials (redacted by name, JWT or opaque)
    "code",            # authorization code -- exchangeable for tokens
    "code_verifier",   # PKCE secret
    "assertion",       # JWT bearer assertion (RFC 7523)
    "subject_token",   # token exchange (RFC 8693)
    "actor_token",     # token exchange (RFC 8693)
    "client_secret",
    "clientsecret",
    "secret",
    "password",
    "passwd",
    "api_key",
    "apikey",
    "key",
    # Semi-sensitive (non-secret) fields: voluminous / PII-ish values redacted on request.
    "groupemail",
    # OAuth token-endpoint identity blob (Azure AD / B2C). NOT a credential -- a single unsigned
    # base64 segment, so JWT_RE (which needs header.payload.signature) does not match it -- but it
    # decodes to the operator's given name, tenant GUID and IdP. Redacted by name, both because it
    # is PII in a capture meant to be shared and because the residual scanner's looser "jwt" pattern
    # flags it on every run, which trains a reader to ignore the warning that matters.
    "profile_info",
}

# Free-text patterns redacted wherever they appear (URLs, header values, bodies).
BEARER_RE = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/\-]+=*")
# JWT = header.payload.signature. Allow base64url AND base64-standard chars (+ and /)
# plus optional '=' padding in EVERY segment, so the whole token is redacted even when a
# segment isn't strict base64url (e.g. partially-sanitized captures). Over-redacting the
# token is strongly preferred to leaving any fragment of it behind.
_JWT_SEG = r"[A-Za-z0-9+/_-]+={0,2}"
JWT_RE = re.compile(r"\beyJ[A-Za-z0-9+/_-]+={0,2}\." + _JWT_SEG + r"\." + _JWT_SEG)
URL_RE = re.compile(r"https?://[^\s\"'<>\\]+")
# AWS access key IDs (long-term AKIA…, temporary ASIA…, and other AWS id prefixes).
AWS_KEY_RE = re.compile(r"\b(?:A3T[A-Z0-9]|AKIA|AGPA|AIDA|AROA|AIPA|ANPA|ANVA|ASIA)[A-Z0-9]{16}")

# Patterns used only for the post-scrub residual-secret WARNING (not redaction).
RESIDUAL_RES = [
    ("bearer-token", re.compile(r"(?i)\bBearer\s+(?!" + REDACTED + r")[A-Za-z0-9._~+/\-]{10,}")),
    ("jwt", re.compile(r"\beyJ[A-Za-z0-9+/_=-]{10,}")),
    (
        "unredacted-signature",
        re.compile(
            r"(?i)(?:X-Goog-Signature|X-Amz-Signature|Signature|sig)=(?!" + REDACTED + r")[A-Za-z0-9%._\-]{12,}"
        ),
    ),
    ("aws-access-key", AWS_KEY_RE),
    (
        "aws-credential",
        re.compile(
            r"(?i)(?:x-amz-security-token|AWSAccessKeyId|x-amz-credential)(?:=|%3D)(?!"
            + REDACTED
            + r")[A-Za-z0-9%/+_-]{12,}"
        ),
    ),
]


def _norm(name) -> str:
    return (name or "").strip().lower() if isinstance(name, str) else ""


# --------------------------------------------------------------------------
# Core redaction primitives
# --------------------------------------------------------------------------

def _redact_query_pairs(query, counts):
    """Redact sensitive params in a query string; recurse into URL-valued params.

    Returns (new_query, changed). Recursion handles a **nested signed URL** carried as a
    parameter value -- e.g. a handoff ``url=<percent-encoded signed url>`` whose own
    ``X-Goog-Signature`` must be redacted.
    """
    if not query:
        return query, False
    changed = False
    new_pairs = []
    for key, value in parse_qsl(query, keep_blank_values=True):
        if _norm(key) in SENSITIVE_PARAM_NAMES and value != REDACTED:
            new_pairs.append((key, REDACTED))
            counts["query_params"] += 1
            changed = True
        elif "://" in value:  # the value is itself a URL -> redact it recursively
            redacted = redact_url(value, counts)
            new_pairs.append((key, redacted))
            changed = changed or redacted != value
        else:
            new_pairs.append((key, value))
    if not changed:
        return query, False
    return urlencode(new_pairs), True


def redact_url(url, counts):
    """Redact sensitive query parameters in a URL using urllib (reliable parsing).

    Handles params in the query string AND in the **fragment** -- hash-routed apps such
    as Terra put params after ``#`` (e.g. ``#import-data?url=...``) -- plus **nested**
    signed URLs carried as a parameter value.
    """
    if not isinstance(url, str) or "://" not in url:
        return url
    try:
        parts = urlsplit(url)
    except ValueError:
        return url
    new_query, q_changed = _redact_query_pairs(parts.query, counts)
    fragment, f_changed = parts.fragment, False
    if "?" in fragment:  # hash-routed query params, e.g. "#import-data?url=..."
        fpath, fquery = fragment.split("?", 1)
        new_fquery, f_changed = _redact_query_pairs(fquery, counts)
        if f_changed:
            fragment = fpath + "?" + new_fquery
    if not (q_changed or f_changed):
        return url
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, fragment))


def redact_text(text, counts):
    """Redact bearer tokens, JWTs, and signed-URL signatures in any free text."""
    if not isinstance(text, str) or not text:
        return text

    def _bearer(_m):
        counts["bearer"] += 1
        return "Bearer " + REDACTED

    def _jwt(_m):
        counts["jwt"] += 1
        return REDACTED

    def _url(m):
        return redact_url(m.group(0), counts)

    def _aws(_m):
        counts["aws_keys"] += 1
        return REDACTED

    text = BEARER_RE.sub(_bearer, text)  # before JWT, so "Bearer <jwt>" -> "Bearer REDACTED"
    text = JWT_RE.sub(_jwt, text)
    text = AWS_KEY_RE.sub(_aws, text)  # AWS access key IDs, wherever they appear
    text = URL_RE.sub(_url, text)
    return text


def walk_json(obj, counts):
    """Recursively redact sensitive keys, and scrub free text in all string leaves."""
    if isinstance(obj, dict):
        result = {}
        for key, value in obj.items():
            if isinstance(key, str) and _norm(key) in SENSITIVE_PARAM_NAMES:
                if value not in (None, "", REDACTED):
                    counts["json_keys"] += 1
                result[key] = REDACTED
            else:
                result[key] = walk_json(value, counts)
        return result
    if isinstance(obj, list):
        return [walk_json(item, counts) for item in obj]
    if isinstance(obj, str):
        return redact_text(obj, counts)
    return obj


def scrub_body_text(text, counts):
    """Scrub a request/response body: parse as JSON when possible, else free text."""
    if not isinstance(text, str) or not text:
        return text
    if text.lstrip()[:1] in "{[":
        try:
            return json.dumps(walk_json(json.loads(text), counts))
        except (ValueError, TypeError):
            pass
    return redact_text(text, counts)


# --------------------------------------------------------------------------
# HAR-structure-aware scrubbing
# --------------------------------------------------------------------------

def scrub_headers(headers, counts):
    for header in headers or []:
        if not isinstance(header, dict):
            continue
        if _norm(header.get("name")) in SENSITIVE_HEADER_NAMES:
            if header.get("value") not in (None, "", REDACTED):
                counts["headers"] += 1
            header["value"] = REDACTED
        else:
            header["value"] = redact_text(header.get("value"), counts)


def scrub_cookies(cookies, counts):
    for cookie in cookies or []:
        if not isinstance(cookie, dict):
            continue
        if cookie.get("value") not in (None, "", REDACTED):
            counts["cookies"] += 1
        cookie["value"] = REDACTED


def scrub_name_value_params(params, counts):
    for param in params or []:
        if not isinstance(param, dict):
            continue
        if _norm(param.get("name")) in SENSITIVE_PARAM_NAMES:
            if param.get("value") not in (None, "", REDACTED):
                counts["query_params"] += 1
            param["value"] = REDACTED
        else:
            param["value"] = redact_text(param.get("value"), counts)


def scrub_request(request, counts):
    if not isinstance(request, dict):
        return
    if isinstance(request.get("url"), str):
        # redact_text catches JWTs / bearer tokens anywhere in the URL and also runs
        # redact_url (signature/token query params) via the embedded URL pattern.
        request["url"] = redact_text(request["url"], counts)
    scrub_headers(request.get("headers"), counts)
    scrub_cookies(request.get("cookies"), counts)
    scrub_name_value_params(request.get("queryString"), counts)
    post = request.get("postData")
    if isinstance(post, dict):
        scrub_name_value_params(post.get("params"), counts)
        if isinstance(post.get("text"), str):
            post["text"] = scrub_body_text(post["text"], counts)


def scrub_response(response, counts):
    if not isinstance(response, dict):
        return
    if isinstance(response.get("redirectURL"), str):
        response["redirectURL"] = redact_text(response["redirectURL"], counts)
    scrub_headers(response.get("headers"), counts)
    scrub_cookies(response.get("cookies"), counts)
    content = response.get("content")
    if isinstance(content, dict) and isinstance(content.get("text"), str):
        # Skip base64-encoded binary bodies (images, etc.) -- not text to scrub.
        if content.get("encoding") != "base64":
            content["text"] = scrub_body_text(content["text"], counts)


def scrub_har(data, counts):
    entries = (data.get("log") or {}).get("entries") if isinstance(data, dict) else None
    if entries is None:
        raise ValueError("Not a recognizable HAR file (missing log.entries).")
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        scrub_request(entry.get("request"), counts)
        scrub_response(entry.get("response"), counts)
    counts["entries"] = len(entries)
    return data


def residual_scan(text):
    findings = {}
    for label, pattern in RESIDUAL_RES:
        hits = len(pattern.findall(text))
        if hits:
            findings[label] = hits
    return findings


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Scrub secrets from a Chrome DevTools HAR capture (writes a new *.scrubbed.har).",
    )
    parser.add_argument("input", type=Path, help="Path to the .har capture to scrub.")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Output path (default: <input>.scrubbed.har). Will not overwrite the input.",
    )
    args = parser.parse_args(argv)

    if not args.input.is_file():
        parser.error(f"Input file not found: {args.input}")

    output = args.output or args.input.with_name(
        f"{args.input.stem}.scrubbed{args.input.suffix or '.har'}"
    )
    if output.resolve() == args.input.resolve():
        parser.error("Output path must differ from the input (the original is never modified).")

    try:
        data = json.loads(args.input.read_text(encoding="utf-8"))
    except (ValueError, OSError) as exc:
        parser.error(f"Could not read/parse HAR: {exc}")

    counts = {"entries": 0, "headers": 0, "cookies": 0, "query_params": 0, "bearer": 0, "jwt": 0, "aws_keys": 0, "json_keys": 0}
    try:
        scrub_har(data, counts)
    except ValueError as exc:
        parser.error(str(exc))

    scrubbed_text = json.dumps(data, indent=2)
    output.write_text(scrubbed_text + "\n", encoding="utf-8")

    print(f"Scrubbed {counts['entries']} HAR entries -> {output}")
    print("  Redactions:")
    print(f"    sensitive headers : {counts['headers']}")
    print(f"    cookies           : {counts['cookies']}")
    print(f"    query/form params : {counts['query_params']}  (incl. signed-URL signatures)")
    print(f"    bearer tokens     : {counts['bearer']}")
    print(f"    JWTs              : {counts['jwt']}")
    print(f"    AWS access keys   : {counts['aws_keys']}")
    print(f"    sensitive JSON keys: {counts['json_keys']}")

    residual = residual_scan(scrubbed_text)
    if residual:
        print("\n  ⚠️  Possible residual secrets detected -- REVIEW before sharing:")
        for label, hits in residual.items():
            print(f"      {label}: {hits}")
        print("      Extend the SENSITIVE_* sets / regexes in this script if needed.")
        return 2

    print("\n  No residual secret patterns detected. Still review before sharing.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
