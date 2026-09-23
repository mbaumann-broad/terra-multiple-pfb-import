# Service Registry

Canonical reference for the external services this project integrates with. **This is the single
source of truth** for repos, OpenAPI/Swagger spec locations, and per-tier deployment URLs — the
tier-aware config (`src/terra_import_prototype/config.py`) is derived from the table below, not by
hardcoding URLs elsewhere.

## Tiers

Every Terra service exists in two independent tiers — **dev** and **prod**. Develop against **dev**;
promote to **prod** once solid. Never mix tiers within a single run.

The tier→URL mapping is **not** a pure string substitution:
- The Broad DSDE services (Firecloud, Rawls, Sam) mostly follow `dsde-dev` ↔ `dsde-prod`.
- **Firecloud prod** (`api.firecloud.org`) uses a vanity hostname — it is the host in
  [`recorded.har`](../recorded.har), the captured reference for the import flow.
- **Gen3 BioData Catalyst** has no Broad dev tier. Its exports are the *input* to this tool, not a
  service it calls, so a dev run consumes a prod-origin signed URL (see the provenance note below).

So the config needs an explicit per-service, per-tier URL table (below) rather than substitution.

## Services

### Gen3 BioData Catalyst (BDC)
- **Role:** The data source. A researcher exports a cohort from the BDC Explorer; Gen3 writes a PFB
  (Avro) export to its S3 export bucket and hands back a **pre-signed URL** — either one signed
  `.avro` URL, or a signed `manifest.json` listing N of them. That URL is this tool's only input.
- **Portal:** https://gen3.biodatacatalyst.nhlbi.nih.gov/
- **Export bucket (observed in `recorded.har`, 2026-09-21):**
  `gen3-biodatacatalyst-nhlbi-nih-gov-pfb-export.s3.amazonaws.com` — virtual-hosted-style S3, which
  is what pins `safety.SIGNED_URL_ALLOWED_PREFIXES` and the client-side host allow-list. Note this
  is **not** path-style `s3.amazonaws.com`; an allow-list written for path-style rejects every real
  BDC export.
- **Spec:** Gen3 is not called by this tool over an API — only its signed URL is dereferenced (a
  manifest) or handed to Terra (an Avro). There is no client for it.

### Terra Firecloud Orchestration
- **Role:** Terra orchestration layer; owns the `importJob` endpoints (submit, status, list) and the
  status translation the UI renders. **The only service the import flow talks to.**
- **Repo:** https://github.com/broadinstitute/firecloud-orchestration
- **Spec (OpenAPI 3.0.1):** https://raw.githubusercontent.com/broadinstitute/firecloud-orchestration/refs/heads/develop/src/main/resources/swagger/api-docs.yaml
- **Dev:** https://firecloud-orchestration.dsde-dev.broadinstitute.org/
- **Prod:** https://api.firecloud.org/

### Terra Rawls
- **Role:** Terra workspace + data-table (entity) backend. Creates the destination workspace and is
  the source for the post-import "does this workspace have data?" check.
- **Repo:** https://github.com/broadinstitute/rawls
- **Spec (OpenAPI 3.0.1):** https://raw.githubusercontent.com/broadinstitute/rawls/refs/heads/develop/core/src/main/resources/swagger/api-docs.yaml
  - ⚠️ Spec path is `core/src/main/...` — Rawls differs from Sam (see below).
- **Dev:** https://rawls.dsde-dev.broadinstitute.org/
- **Prod:** https://rawls.dsde-prod.broadinstitute.org/

### Terra Sam (auth)
- **Role:** Terra identity / authorization. Used only by the identity guard, which maps the active
  credentials (a human login *or* a Terra pet service account) to their canonical Terra user. Called
  from ``auth.terra_user_email`` rather than through a client class -- it is one endpoint, reached
  once per run before any client exists.
- **Repo:** https://github.com/broadinstitute/sam
- **Spec (OpenAPI 3.0.1):** https://raw.githubusercontent.com/broadinstitute/sam/refs/heads/develop/src/main/resources/swagger/api-docs.yaml
  - ⚠️ Spec path is `src/main/...` (**no** `core/` prefix). Do not copy Rawls's `core/` here.
- **Dev:** https://sam.dsde-dev.broadinstitute.org/
- **Prod:** https://sam.dsde-prod.broadinstitute.org/

### cWDS (Terra Workspace Data Service)
- **Role:** Does the actual PFB translation and the Rawls upsert behind `importJob`. Listed for
  orientation only — **this tool never calls it**. Orchestration owns status translation and the
  workspace-id lookup, so a check that reaches past Orchestration into cWDS is checking something
  the Terra UI cannot see. Its `twds.data-import.allowed-hosts` config is what the client-side host
  allow-list in `manifest.py` mirrors.
- **Repo:** https://github.com/DataBiosphere/terra-workspace-data-service

## Quick reference table

| Service | Tier | Deployment URL |
|---|---|---|
| Gen3 BioData Catalyst | prod | https://gen3.biodatacatalyst.nhlbi.nih.gov/ |
| Firecloud Orchestration | dev | https://firecloud-orchestration.dsde-dev.broadinstitute.org/ |
| Firecloud Orchestration | prod | https://api.firecloud.org/ |
| Rawls | dev | https://rawls.dsde-dev.broadinstitute.org/ |
| Rawls | prod | https://rawls.dsde-prod.broadinstitute.org/ |
| Sam | dev | https://sam.dsde-dev.broadinstitute.org/ |
| Sam | prod | https://sam.dsde-prod.broadinstitute.org/ |

All Broad DSDE services expose a `/status` health endpoint.

## Why there is no local stack

An earlier iteration of this repo shipped a `docker/compose.yaml` with stand-ins for Sam, Rawls,
cWDS and Orchestration. It has been removed. The behaviour under test — fan-out concurrency inside
cWDS, Orchestration's status translation, Rawls's batch-upsert semantics, workspace policy updates
— is exactly what a stand-in cannot reproduce, so a green local run was evidence about the stand-ins
and not about Terra. Runs go against a real tier.
