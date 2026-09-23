"""Command-line entrypoint (Typer).

terra-import-prototype import-qc --url <pre-signed URL> [--kind avro|manifest] [--tier dev|prod]
    [--dispatch parallel|sequential|sequential-await] [--max_worker N] [--poll-strategy per_job|list]
    [--dry-run] [--verify-auth] [--config path]
terra-import-prototype check-workspace --workspace-namespace <ns> --workspace-name <name> [--tier ...]

The signed URL is a secret: it grants direct read of the exported study data. Prefer
``--url-file <path>`` (or ``TERRA_IMPORT_QC_URL`` in the environment) over ``--url`` on an
interactive shell, where the value lands in shell history and in the process list. The tool itself
never logs it -- see ``safety.SignedUrl``.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional

import typer

from .auth import IdentityMismatchError
from .config import TIERS
from .manifest import ImportRequestError
from .models import DispatchPolicy
from .pipeline import (
    JOB_TIMEOUT_SECONDS,
    POLL_INTERVAL_SECONDS,
    run_check_workspace,
    run_import_qc,
)
from .safety import SafetyError

app = typer.Typer(
    help="Import a pre-signed Gen3 BioData Catalyst PFB export into a Terra workspace and check it.",
    no_args_is_help=True,
)

#: Environment variable holding the pre-signed URL, so it need not appear on the command line.
URL_ENV_VAR = "TERRA_IMPORT_QC_URL"

#: Exit codes. 2 is the retryable one (a transient service or an expired URL); 3 means the tool ran
#: to completion and the QC verdict was FAIL, which is a *result*, not a crash -- a batch caller
#: needs to tell those apart.
EXIT_ERROR = 1
EXIT_RETRYABLE = 2
EXIT_QC_FAILED = 3


@app.callback()
def main() -> None:
    """Terra PFB import QC commands."""


def _resolve_url(url: Optional[str], url_file: Optional[Path]) -> str:
    """Take the signed URL from exactly one of --url, --url-file, or the environment."""
    sources = [
        ("--url", url),
        ("--url-file", url_file.read_text().strip() if url_file else None),
        (URL_ENV_VAR, os.environ.get(URL_ENV_VAR)),
    ]
    supplied = [(name, value) for name, value in sources if value]
    if not supplied:
        raise typer.BadParameter(
            f"No pre-signed URL supplied. Pass --url, --url-file, or set {URL_ENV_VAR}."
        )
    if len(supplied) > 1:
        raise typer.BadParameter(
            f"The pre-signed URL was supplied more than once ({', '.join(n for n, _ in supplied)}); "
            "pass it exactly once so there is no doubt which one ran."
        )
    return supplied[0][1]


def _policy(dispatch: str, max_worker: int) -> DispatchPolicy:
    if dispatch == "parallel":
        return DispatchPolicy(mode="parallel", max_worker=max_worker)
    if dispatch == "sequential":
        return DispatchPolicy(mode="sequential")
    if dispatch == "sequential-await":
        return DispatchPolicy(mode="sequential", await_terminal=True)
    raise typer.BadParameter(
        f"Unknown --dispatch {dispatch!r} (expected parallel|sequential|sequential-await)."
    )


@app.command("import-qc")
def import_qc(
    url: Optional[str] = typer.Option(
        None, "--url", "-u", help="The pre-signed Gen3 export URL (PFB .avro or manifest .json)."
    ),
    url_file: Optional[Path] = typer.Option(
        None,
        "--url-file",
        help="Read the pre-signed URL from this file instead (keeps it out of shell history).",
    ),
    kind: Optional[str] = typer.Option(
        None,
        "--kind",
        help="avro|manifest. Default: inferred from the URL's path extension.",
    ),
    tier: Optional[str] = typer.Option(
        None, "--tier", "-t", help=f"Tier to run against ({'|'.join(TIERS)}). Default: config's default_tier."
    ),
    config: Path = typer.Option(
        Path("config/config.yaml"), "--config", "-c", help="Path to the YAML config file."
    ),
    dispatch: str = typer.Option(
        "parallel",
        "--dispatch",
        help="How to pace the fan-out: parallel (capped), sequential, or sequential-await "
        "(wait for each job before posting the next).",
    ),
    max_worker: int = typer.Option(
        3, "--max_worker", help="Max import jobs in flight under --dispatch parallel."
    ),
    poll_strategy: str = typer.Option(
        "per_job",
        "--poll-strategy",
        help="per_job mirrors today's UI (one GET per job per interval); list is the cheaper "
        "one-GET-covers-all alternative.",
    ),
    poll_interval: float = typer.Option(
        POLL_INTERVAL_SECONDS, "--poll-interval", help="Seconds between status polls."
    ),
    job_timeout: float = typer.Option(
        JOB_TIMEOUT_SECONDS, "--job-timeout", help="Per-job budget in seconds before it is recorded as TIMEOUT."
    ),
    log_dir: Path = typer.Option(Path("logs"), "--log-dir", help="Where to write the detailed log."),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Build and validate the request (fetching a manifest if needed); create no workspace "
        "and submit no import job.",
    ),
    verify_auth: bool = typer.Option(
        False,
        "--verify-auth",
        help="Auth preflight: run the identity guard and exit. Fetches nothing, creates nothing.",
    ),
) -> None:
    """Import a pre-signed Gen3 export into a fresh Terra workspace, then check the workspace."""
    if poll_strategy not in ("per_job", "list"):
        raise typer.BadParameter(f"Unknown --poll-strategy {poll_strategy!r} (expected per_job|list).")
    if kind is not None and kind not in ("avro", "manifest"):
        raise typer.BadParameter(f"Unknown --kind {kind!r} (expected avro|manifest).")

    resolved_url = _resolve_url(url, url_file)
    policy = _policy(dispatch, max_worker)

    try:
        result = run_import_qc(
            resolved_url,
            tier,
            config,
            kind=kind,  # type: ignore[arg-type]
            log_dir=log_dir,
            policy=policy,
            poll_strategy=poll_strategy,  # type: ignore[arg-type]
            poll_interval_s=poll_interval,
            job_timeout_s=job_timeout,
            dry_run=dry_run,
            verify_auth=verify_auth,
        )
    except (SafetyError, ImportRequestError, IdentityMismatchError) as exc:
        # A refused hand-off, an unimportable request, or the wrong identity. All are the tool
        # working as intended, so report them cleanly rather than as a traceback.
        typer.secho(f"Refused: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(EXIT_ERROR)
    except FileNotFoundError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(EXIT_ERROR)

    _report(result)
    if result.get("qc_passed") is False:
        raise typer.Exit(EXIT_QC_FAILED)


@app.command("check-workspace")
def check_workspace(
    workspace_namespace: str = typer.Option(
        ..., "--workspace-namespace", help="The workspace's Terra billing project."
    ),
    workspace_name: str = typer.Option(..., "--workspace-name", help="The workspace name."),
    tier: Optional[str] = typer.Option(None, "--tier", "-t", help=f"Tier ({'|'.join(TIERS)})."),
    config: Path = typer.Option(Path("config/config.yaml"), "--config", "-c"),
    log_dir: Path = typer.Option(Path("logs"), "--log-dir"),
) -> None:
    """Re-check an existing workspace's data tables. Imports nothing."""
    try:
        result = run_check_workspace(
            workspace_namespace, workspace_name, tier, config, log_dir=log_dir
        )
    except (SafetyError, IdentityMismatchError, FileNotFoundError) as exc:
        typer.secho(f"Refused: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(EXIT_ERROR)

    _report(result)
    if result.get("qc_passed") is False:
        raise typer.Exit(EXIT_QC_FAILED)


def _report(result: dict) -> None:
    """Print the run's summary. The detailed log file holds the per-request narrative."""
    for key, value in result.items():
        if key == "durations":
            continue
        typer.echo(f"{key}: {value}")


if __name__ == "__main__":  # pragma: no cover
    sys.exit(app())
