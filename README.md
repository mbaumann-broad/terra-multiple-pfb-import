# terra-import-prototype

Import a pre-signed **Gen3 BioData Catalyst** PFB export into a fresh Terra workspace and verify the
result — including the **N-way fan-out** Terra's UI performs when the export is a manifest.

- [`CLAUDE.md`](CLAUDE.md) — purpose, architecture and settled conventions.
- [`docs/import_flow.md`](docs/import_flow.md) — the flow, endpoint by endpoint.
- [`docs/services.md`](docs/services.md) — the service registry (the single source of truth for URLs).

## What it does

You have one pre-signed URL from a BDC export. It is either a PFB (`.avro`) or a `manifest.json`
naming N of them. This tool:

1. builds the import request (fetching and validating the manifest if there is one);
2. creates one Terra workspace;
3. posts **one `importJob` per PFB URL** into it — the same client-side fan-out the Terra UI does —
   and polls every job to a terminal status;
4. checks the workspace actually holds data.

It does **not** compare the workspace against TDR, a snapshot, or another workspace, and does not use
Azul/ADE. There is no source of truth to compare a pre-signed export against, so the check is the
honest one: every job succeeded, and entities arrived. Every report says so explicitly.

## Setup

```bash
python -m venv .venv && .venv/bin/python -m pip install -e '.[dev]'
cp config/config.example.yaml config/config.yaml   # then fill in your tiers
gcloud auth application-default login              # as the account in config.yaml
```

`config.yaml` is gitignored and holds only per-developer settings — your Terra user, ADC file, and
billing project per tier. Service URLs are baked into the package from `docs/services.md`.

## Running

The pre-signed URL is a **secret** — it grants direct read of the exported data. Prefer a file or the
environment over an interactive shell, where `--url` lands in shell history and the process list:

```bash
# activate virtual environment
source .venv/bin/activate

# one PFB
terra-import-prototype import-qc --url-file ./export-url.txt --tier prod # dev or prod

# a manifest: N import jobs into one workspace, three in flight at a time
export TERRA_IMPORT_QC_URL='https://…/manifest.json?X-Amz-…'
terra-import-prototype import-qc --kind manifest --dispatch parallel --max_worker 3 --tier dev

# check everything without creating anything
terra-import-prototype import-qc --url-file ./url.txt --verify-auth   # identity only
terra-import-prototype import-qc --url-file ./url.txt --dry-run       # + fetch and validate the manifest

# re-check a workspace a previous run left behind
terra-import-prototype check-workspace --workspace-namespace <ns> --workspace-name <name>
```

Useful flags:

| Flag | Why |
|---|---|
| `--kind avro\|manifest` | override the extension-based guess |
| `--dispatch parallel\|sequential\|sequential-await`, `--max_worker N` | pace the fan-out; the max_worker bounds **jobs in flight**, not POSTs |
| `--poll-strategy per_job\|list` | `per_job` mirrors today's UI; `list` is one request covering every job |
| `--poll-interval`, `--job-timeout` | per-job budget; a hung job is recorded as `TIMEOUT` and its siblings keep reporting |

Exit codes: `0` pass, `1` refused (bad input, failed safety check, wrong identity), `3` the run
completed and QC **failed** — a result, not a crash, so a caller can tell them apart.

Each run **creates and retains one workspace**, named
`<you>_qc_bdc_{avro,manifest}_<label>_<YYYYMMDDHHMM>`. They accumulate; clean up periodically. The
detailed log (every request and response, secrets redacted) is written to `logs/`.

## Tests

```bash
.venv/bin/python -m pytest -q
```

No test touches the network. `tests/test_har_conformance.py` reads `recorded.har` — a real capture of
the production flow — and replays it, so the wire format is verified against the capture rather than
restated in an assertion that can drift. It skips if the capture is absent.

**Never modify `recorded.har`.** It is the reference of record. Re-capture alongside it and scrub with
`scripts/scrub_har.py`, which redacts bearer tokens and signed-URL signatures while preserving the
structure that makes a capture useful.

## Limitations, and where human review matters most

- **No reference comparison.** Row counts, column names, values and entity relationships are not
  checked against anything. A PASS means the jobs finished and entities exist.
- **Overlapping PFBs are not adjudicated.** Two PFBs sharing entity ids merge; which one wins is
  Rawls's batch ordering, not something this tool promises or verifies.
- **No consent gate.** A signed URL carries no consent metadata, so — unlike a snapshot-driven tool —
  this one cannot tell open-access from controlled data. **Treat every run as potentially
  controlled-access.** What is enforced is provenance: where the URL came from and where it goes.
- **Not yet run end-to-end.** v0.1.0 is unit-tested and has not been exercised against a real tier.

Open the retained workspace's Data tab and review what actually landed.
