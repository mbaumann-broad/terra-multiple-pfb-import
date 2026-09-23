"""Lightweight stage timing.

A small context manager to measure how long a pipeline stage takes: it logs the duration (in seconds)
as the stage finishes (so early stages report before a long one runs) and records it into a dict.
``log_stage_summary`` then emits a single end-of-run summary line **in minutes** -- the salient,
user-experienced delays (export, workspace creation, import) that grow large for big datasets. That
line is a **stable contract**: it is appended to the QC report and parsed into the per-stage WDL
duration outputs, so keep its ``name=<minutes>`` shape parseable.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager

from .logging_setup import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)

#: Marker prefix for the end-of-run stage-timing summary line (the WDLs grep for this).
STAGE_SUMMARY_PREFIX = "Stage timings (minutes)"


@contextmanager
def stage_timer(name: str, into: dict[str, float]) -> Iterator[None]:
    """Time the wrapped block: record its elapsed seconds in ``into[name]`` and log on completion."""
    start = time.monotonic()
    try:
        yield
    finally:
        elapsed = time.monotonic() - start
        into[name] = elapsed
        logger.info("Stage '%s' took %.1fs", name, elapsed)


def format_stage_summary(durations: dict[str, float]) -> str:
    """One-line end-of-run summary of stage durations, converted to **minutes** (1 decimal).

    ``durations`` holds elapsed **seconds** per stage (insertion order = run order). The line is both
    human-readable and machine-parseable: ``Stage timings (minutes) — total T: name=M, name=M, ...``.
    Minutes are rounded to **one decimal** -- these are long-running stages, and coarse values keep
    cross-run/cross-stage comparison easy (a sub-~3s stage rounds to 0.0, same as "did not run"). The
    ``name=<minutes>`` pairs are parsed into the WDL per-stage duration outputs, so this format is a
    stable contract.
    """
    items = ", ".join(f"{name}={secs / 60:.1f}" for name, secs in durations.items())
    total = sum(durations.values()) / 60
    return f"{STAGE_SUMMARY_PREFIX} — total {total:.1f}: {items}"


def log_stage_summary(durations: dict[str, float]) -> None:
    """Log the end-of-run stage-timing summary (no-op if no stages were recorded)."""
    if durations:
        logger.info("%s", format_stage_summary(durations))
