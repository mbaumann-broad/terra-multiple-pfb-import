"""Logging setup and helpers: detailed, structured, redacted request/response logging.

Mirrors the patterns from drs-test-notebooks (notebooks/common/common_functions.ipynb): a header at
the top of the log, detailed per-call request/response logging with secret redaction, and capture of
correlation/trace headers so developers can find the corresponding server-side logs. The detailed
log file is intended to be a workflow (WDL) output.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

REDACTED = "REDACTED"

# Header values that must never be logged in the clear.
SENSITIVE_HEADERS = frozenset({"authorization", "proxy-authorization", "cookie", "set-cookie"})

# Correlation/trace headers worth logging (paste a trace id into Cloud Logging to find the request).
CORRELATION_HEADERS = ("x-cloud-trace-context", "traceparent", "x-request-id")

LOGGER_NAME = "terra_import_prototype"


def setup_logging(
    log_dir: Path, tier: str, run_label: str, verbose: bool = True
) -> tuple[logging.Logger, Path]:
    """Configure the logger to write to both the console and a timestamped detailed log file.

    ``run_label`` names the run in the filename. It must never be the signed URL: that carries the
    secret query string, and a log *filename* is not covered by the redaction that protects the log
    body. Callers pass the URL's safe location or the dataset label instead (see
    ``pipeline._setup_run``).
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_label = "".join(c if (c.isalnum() or c in "-_") else "_" for c in run_label)[:64]
    log_file = log_dir / f"terra_import_qc_{tier}_{safe_label}_{ts}.log"

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s")

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    return logger, log_file


def write_header(logger: logging.Logger, **fields: object) -> None:
    """Write a boxed header near the top of the log (tool version, tier, snapshot, identity, time)."""
    border = "=" * 80
    now = datetime.now(timezone.utc)
    lines = [border, "terra-import-prototype run", f"Time (UTC): {now.strftime('%Y-%m-%d %H:%M:%S %Z')}"]
    lines.extend(f"{key}: {value}" for key, value in fields.items())
    lines.append(border)
    logger.info("\n".join(lines))


def redact_headers(headers: dict[str, str]) -> dict[str, str]:
    """Return a copy of headers with sensitive values masked (names preserved)."""
    return {k: (REDACTED if k.lower() in SENSITIVE_HEADERS else v) for k, v in headers.items()}


def correlation_fields(response_headers) -> dict[str, str]:
    """Extract correlation/trace headers from a response for logging (empty dict if absent)."""
    lower = {k.lower(): v for k, v in dict(response_headers).items()}
    return {h: lower[h] for h in CORRELATION_HEADERS if h in lower}
