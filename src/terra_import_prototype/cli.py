"""Command-line entrypoint (Typer).

terra-import-prototype import-qc --url <pre-signed URL> [--kind avro|manifest] [--tier dev|prod]
    [--dispatch parallel|sequential|sequential-await] [--max_worker N] [--poll-strategy per_job|list]
    [--dry-run] [--verify-auth] [--config path]
terra-import-prototype check-workspace --workspace-namespace <ns> --workspace-name <name> [--tier ...]

``import-qc`` takes one or more pre-signed URLs and imports them all into **one** fresh workspace:
every source is expanded, and the combined list of PFB URLs fans out into that single workspace. Two
Avro URLs and a manifest naming two PFBs are the same thing to this tool -- one workspace, one
fan-out, one verdict. A single URL is the one-element case, so there is no separate single-URL path.

The signed URL is a secret: it grants direct read of the exported study data. Prefer
``--url-files <path>`` (or ``TERRA_IMPORT_QC_URL`` in the environment) over ``--url`` on an
interactive shell, where the value lands in shell history and in the process list. The tool itself
never logs it -- see ``safety.SignedUrl``.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Optional, Sequence

import typer

from .auth import IdentityMismatchError
from .config import TIERS
from .manifest import ImportRequestError
from .models import DispatchPolicy
from .pipeline import (
    JOB_TIMEOUT_SECONDS,
    POLL_INTERVAL_SECONDS,
    run_check_workspace,
    run_import_job,
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


def _read_url_file(path: Path) -> str:
    """Read one pre-signed URL from a file. The file holds the URL and nothing else."""
    try:
        text = path.read_text()
    except OSError as exc:
        raise typer.BadParameter(f"--url-files: could not read {path}: {exc}") from exc
    url = text.strip()
    if not url:
        raise typer.BadParameter(f"--url-files: {path} is empty; it must hold one pre-signed URL.")
    return url


def _url_file_paths(values: Sequence[str]) -> list[Path]:
    """Expand the ``--url-files`` values into file paths.

    Two spellings, both accepted, because both are natural to type:

      --url-files='["./a.txt","./b.txt"]'     one JSON list
      --url-files ./a.txt --url-files ./b.txt  the flag repeated

    The values are **paths**, never the URLs themselves -- that is the whole point of the flag: the
    signed URL stays out of shell history and the process list.
    """
    paths: list[Path] = []
    for value in values:
        entry = value.strip()
        if not entry:
            continue
        if not entry.startswith("["):
            paths.append(Path(entry))
            continue
        try:
            parsed = json.loads(entry)
        except json.JSONDecodeError as exc:
            raise typer.BadParameter(
                f"--url-files: {entry!r} looks like a JSON list but does not parse: {exc}"
            ) from exc
        if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
            raise typer.BadParameter(
                '--url-files: expected a JSON list of file paths, e.g. \'["./a.txt","./b.txt"]\'.'
            )
        paths.extend(Path(item.strip()) for item in parsed if item.strip())
    return paths


def _resolve_urls(url: Optional[str], url_files: Optional[Sequence[str]]) -> list[str]:
    """Collect the run's pre-signed URLs from --url, --url-files, and the environment.

    ``--url`` and ``--url-files`` are both explicit operator intent, so they add up: the run imports
    every URL that was named. ``TERRA_IMPORT_QC_URL`` is a **fallback**, used only when neither flag
    was passed -- an exported URL left over in a shell must never silently join a run that named its
    own, because in this tool a stray extra URL is a stray extra import of controlled-access data.
    """
    urls: list[str] = [url] if url else []
    urls.extend(_read_url_file(path) for path in _url_file_paths(url_files or []))

    if not urls:
        env_url = (os.environ.get(URL_ENV_VAR) or "").strip()
        if env_url:
            urls.append(env_url)

    if not urls:
        raise typer.BadParameter(
            f"No pre-signed URL supplied. Pass --url, --url-files, or set {URL_ENV_VAR}."
        )
    return urls


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
    url_files: Optional[list[str]] = typer.Option(
        None,
        "--url-files",
        help="Read the pre-signed URLs from files instead (keeps them out of shell history). "
        'Either a JSON list -- --url-files=\'["./a.txt","./b.txt"]\' -- or the flag repeated. '
        "Each file holds one URL, and every URL is imported into the same one workspace.",
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
    """Import the pre-signed Gen3 export(s) into one fresh Terra workspace, then check it."""
    if poll_strategy not in ("per_job", "list"):
        raise typer.BadParameter(f"Unknown --poll-strategy {poll_strategy!r} (expected per_job|list).")
    if kind is not None and kind not in ("avro", "manifest"):
        raise typer.BadParameter(f"Unknown --kind {kind!r} (expected avro|manifest).")

    resolved_urls = _resolve_urls(url, url_files)
    policy = _policy(dispatch, max_worker)

    try:
        result = run_import_job(
            resolved_urls,
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
