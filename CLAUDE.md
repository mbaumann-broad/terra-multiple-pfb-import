# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Current state

`terra-import-prototype` is **early** (v0.1.0): the package is built and unit-tested, and has not yet been
run end-to-end against a real Terra tier. The package `src/terra_import_prototype/` provides: `config` (YAML
+ pydantic, tier resolution), `auth` (ADC per tier + Sam-resolved identity guard), `logging_setup` +
`redaction` (structured, secret-redacted request/response logging), `timing` (per-stage durations),
`safety` (signed-URL containment, hand-off and fan-out provenance), `models` (status vocabulary,
`ImportRequest`, `ImportJob`, `BatchResult`, `DispatchPolicy`), `manifest` (signed URL → the list of
PFB URLs to import), `clients/` (base + `firecloud`, `rawls`; Sam is reached directly from `auth`, being one endpoint used once per run), `workspace` (naming), `qc`
(the post-import data check), `pipeline` (the wired flow, including the fan-out) and `cli` (Typer:
`import-qc`, `check-workspace`).

**One operation, two input shapes — one path through:**
- **`import-qc --kind avro`** — the operator holds one pre-signed Gen3 PFB (`.avro`) URL. Rawls
  `createWorkspace` → **one** Firecloud `importJob` → poll → QC. This is the flow captured in
  [`recorded.har`](recorded.har).
- **`import-qc --kind manifest`** — the operator holds one pre-signed `manifest.json` URL naming N
  PFB URLs. The manifest is fetched and expanded → Rawls `createWorkspace` → **N** Firecloud
  `importJob` calls into that one workspace → poll every job → the same QC.
- **`check-workspace`** — re-run the data check on an existing workspace; import nothing.

Both shapes share `pipeline._create_import_and_qc`. A single Avro URL is the **degenerate
one-element fan-out**, so there is no single-file code path that can drift from the N-file one.

**The fan-out is the thing under study.** Terra's UI does not import a manifest — it expands the
manifest client-side and posts one `importJob` per URL, then polls each jobId. That means N
concurrent translations inside cWDS and N upserts into one workspace. The questions this repo exists
to answer are about that: does every URL get its own jobId, does a partial failure leave the
survivors intact, does capped concurrency change the outcome, does the workspace end up with
everything. See `pipeline.ImportFanOut` and `tests/test_pipeline_workspace.py`.

**Cross-cutting capabilities:**
- **Dispatch policy** (`--dispatch parallel|sequential|sequential-await`, `--max_worker N`) — whether the
  fan-out should be unbounded, capped, or serialised is an **open product question**, so it is a
  parameter, not a hardcoded choice. The max_worker bounds *jobs in flight*, not just concurrent POSTs: a
  worker holds its slot until its job is terminal. Capping POSTs alone would bound neither cWDS
  concurrency nor the workspace policy updates that follow.
- **Two poll strategies** (`--poll-strategy per_job|list`) — `per_job` is what the UI does today
  (O(N) requests per interval); `list` is one request covering every job. Both are kept because
  running them against the same import and comparing the answers is the only check anywhere that
  Orchestration's two status endpoints do not diverge.
- **Partial failure is a first-class outcome.** A rejected submit, a failed job and a hung job are
  all *results* carried in `BatchResult`, never exceptions that discard the siblings — because that
  is the state the Terra UI has to render and an operator has to triage.
- **Signed-URL containment** — the pre-signed URL is the only credential in the flow and it travels
  the whole pipeline, so it is wrapped in `safety.SignedUrl` (redacts on repr/str/format, not JSON
  serializable) with exactly **two** `reveal()` sites. See *Safety* below.
- **Per-stage durations** (`timing`) — surfaced even on a failed or timed-out import; the partial
  timings are what triage needs.

Environment: Python **3.11+**, local `.venv`, deps from `pyproject.toml`. See `README.md` for
developer setup and [`docs/import_flow.md`](docs/import_flow.md) for the flow.

## What this project does

Given a pre-signed **Gen3 BioData Catalyst** PFB export URL, import it into a fresh Terra GCP
workspace and verify the result. It exists to exercise and characterise Terra's **multi-PFB import
fan-out** across four independently-owned services, before a UI change depends on it.

## Domain architecture

```
Gen3 BDC Explorer ──PFB export──▶ pre-signed URL ──▶ Terra AW
                                                     (Firecloud → cWDS → Rawls)
```

- **Gen3 BDC** — https://gen3.biodatacatalyst.nhlbi.nih.gov/. A researcher exports a cohort; Gen3
  writes PFB (Avro) to its S3 export bucket and returns a pre-signed URL — one `.avro`, or a
  `manifest.json` naming N of them. **This tool never calls Gen3's API.** The signed URL is its only
  input.
- **Firecloud Orchestration** — owns the `importJob` endpoints and status translation. The only
  service the import flow talks to.
- **cWDS** (Terra Workspace Data Service) — does the PFB translation and the Rawls upsert behind
  `importJob`. Never called directly: reaching past Orchestration into cWDS is checking something
  the Terra UI cannot see.
- **Rawls** — workspace + data-table (entity) backend. Creates the workspace; read back for QC.
- **Sam** — Terra identity/authorization. Used only by the identity guard.

Repos, spec URLs and per-tier deployment URLs are in **[`docs/services.md`](docs/services.md)** —
treat that file as the single source of truth for service endpoints.

### Two tiers: dev and prod

Every Terra service exists in **dev** and **prod**. Develop against dev; promote once solid. Design
implication: **tier is a first-class config parameter** — base URLs, credentials and target
endpoints switch together, so moving from dev to prod is a configuration change, never a code
change. Never mix tiers within a run.

Gen3 BDC has no Broad dev tier, so a dev run legitimately consumes a **prod-origin** export URL.
That is the one place this project's provenance model is weaker than its reference project's, and it
is written down in `safety.SIGNED_URL_ALLOWED_PREFIXES` rather than left implicit.

## Primary functionality (current scope)

For one pre-signed export URL:

1. **Build the import request.** For an Avro URL, that is the URL. For a manifest, fetch it and
   normalise it to a flat URL list, then check the host allow-list (mirroring cWDS's
   `twds.data-import.allowed-hosts`) and provenance. Rejecting here is what makes a bad manifest
   create **zero** jobs instead of importing three PFBs and failing on the fourth.
2. **Create one workspace** (`authorizationDomain: []` — the import applies the real one for
   controlled-access data; we **verify**, never set it).
3. **Fan out** one `firecloud.submit_import_job` per URL, paced by the dispatch policy, and poll
   every jobId to a terminal status.
4. **Check the workspace holds data** (Rawls entity-type counts).

**Out of scope, deliberately:** this repo does **not** compare the workspace against TDR, a snapshot,
or an analysis workspace, and does **not** use Azul/ADE. There is no source of truth to compare
against — the input is a pre-signed export, not a snapshot with a queryable origin — so the check is
the honest one: every job that was supposed to run succeeded, and the workspace now holds entities.
`qc.QcResult.render()` states what it did not verify, so the limitation travels with the verdict.

## Safety

The pre-signed URL grants direct read of the exported cohort; for a controlled-access study, of NIH
controlled data. Mishandling one (importing a prod URL into dev, logging it, returning it) would be
a Federal Data Management Incident. Four layers, in `safety.py` — do not weaken any without security
review:

1. **Containment** — `SignedUrl` redacts on repr/str/format and is not JSON-serializable. Exactly
   **two** `reveal()` sites: `clients/firecloud.submit_import_job` (the importJob body) and
   `safety.fetch_signed_json` (the manifest dereference). **Do not add a third.**
2. **Hand-off verification** — `verify_pfb_handoff` pins the destination to the run tier's canonical
   Firecloud host and the source to an allow-listed bucket. Runs **once per import job**, not once
   per run: a manifest's N URLs are N separate deliveries, and checking only the first leaves N-1
   unverified.
3. **Fan-out containment** — `assert_uniform_provenance` requires every URL a manifest expanded to
   satisfy the same allow-list as the manifest itself, before any job is created. Without it the
   allow-list would gate the *index* while leaving the *contents* unchecked.
4. **Bearer-token destination pinning** — `clients/base.py` refuses to send a Terra token to any host
   but the client's own HTTPS host. This is why `fetch_signed_json` does **not** go through
   `BaseClient`: the signature in the query string is the authorization, and adding a Terra bearer
   would hand a Terra credential to an S3 host.

**No consent gate.** The reference project reads a TDR snapshot's `consentCode` and fails closed on
anything but `NRES`. There is no equivalent here: a signed URL carries no consent metadata, and by
the time this tool sees one the export has already happened under the operator's own Gen3
credentials. What remains in our hands is provenance — where the URL came from and where it is
going. **Treat every run as potentially controlled-access.**

## Settled build decisions / conventions

**Stack & layout** (Python 3.11+):
- **PyYAML + pydantic v2** (config), **Typer** (CLI), **requests** (HTTP), **google-auth** (ADC →
  tokens), **ruff** + **pytest** (dev).
- `src/`-layout package holding all logic; a non-interactive **CLI** entrypoint. Keep core logic in
  the importable package so the identical code path runs from the CLI and from a notebook.

**Auth:** Google **ADC** per tier; one or more authorized human Terra users per tier (`email` plus
optional `authorized_users`). **Identity guard:** before any API call, resolve the active credentials
to their canonical Terra user via Sam (`/api/users/v2/self`, which maps a pet service account to its
owning human user) and assert it is in the tier's authorized set; abort otherwise. The token provider
is a **callable**, not a string: a fan-out run polls for hours and outlives the ~1 h token expiry.

**Workspace creation:** one **retained** workspace per run. Name =
`<user-local-part>_<infix>_<sanitized label>_YYYYMMDDHHMM`. The infix names the **import shape**
(`qc_bdc_avro` / `qc_bdc_manifest`) so a pile of result workspaces can be told apart at a glance.
Minute granularity so a transiently-failed run can be rerun without a 409. Create with
`authorizationDomain: []` and `enhancedBucketLogging: true`. `_create_workspace_resilient` tolerates
a slow/uncertain createWorkspace by confirm-or-adopt — it matters more here than in a single-import
tool, because the workspace is created once and then N jobs fan out into it.

**Logging:** centralized, structured, redacted request/response logging in the HTTP client base, plus
correlation/trace headers. This matters more than in a normal client: when a fan-out misbehaves, the
evidence *is* the request/response sequence — which job was posted when, which status came back,
which correlation id to hand another team.

**No local stack.** An earlier iteration shipped a `docker/compose.yaml` with stand-ins for Sam,
Rawls, cWDS and Orchestration. It was removed. The behaviour under test — fan-out concurrency inside
cWDS, Orchestration's status translation, Rawls's batch-upsert semantics — is exactly what a
stand-in cannot reproduce, so a green local run was evidence about the stand-ins, not about Terra.
Runs go against a real tier.

**Testing:** unit tests with mocked HTTP; no test touches the network. `tests/test_har_conformance.py`
reads `recorded.har` and replays it, so the wire format is **verified against the capture** rather
than restated in an assertion that can drift. **Never modify `recorded.har`** — it is the reference
of record. Re-capture with the procedure in the reference repo and scrub with `scripts/scrub_har.py`
before sharing.

**Process:** discuss design and get **explicit approval before scaffolding** structural code.

## Documentation is a first-class deliverable

Two audiences. **Users:** what the tool checks, how to run it (per-tier ADC login, config, CLI), and
how to read the report — including a prominent statement of what it does **not** verify (that one is
also emitted inside every QC report). **Maintainers:** the flow
([`docs/import_flow.md`](docs/import_flow.md)), the service registry
([`docs/services.md`](docs/services.md)), and how to update clients when an API drifts.

Keep `docs/` updated as functionality lands.

## Working with the maintainer

- The maintainer has **deep domain knowledge** of these services and their APIs and will provide
  context, specifics and Swagger files before planning/design/implementation.
- This is intended to be **long-lived, multi-author** code — prioritize clarity, ease of use and
  maintainability over cleverness.
