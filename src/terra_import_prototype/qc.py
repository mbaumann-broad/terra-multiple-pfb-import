"""Post-import QC: did the fan-out actually put data in the Terra workspace?

Deliberately narrow. The reference project (anvil-data-qc) compares an analysis workspace table by
table against a TDR snapshot queried through BigQuery. There is nothing to compare against here: the
input is a pre-signed Gen3 export, not a snapshot with a queryable source of truth, so a row-count
parity check would need a reference this tool does not have and cannot get.

So the question this answers is the one it can answer honestly: **every job that was supposed to run
succeeded, and the workspace now holds entities.** That is a real check -- an import job can report
``Done`` having written nothing (an empty or truncated export produces exactly that), and under
fan-out a single silently-dropped job is invisible in any per-job status. It is also the check that
catches the failure mode this project exists to study: N jobs upserting into one workspace where
fewer than N sets of entities arrive.

What it does **not** verify is stated in the report itself, so the limitation travels with the
result rather than living only in a document nobody reads next to a PASS.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from .clients.rawls import RawlsClient
from .logging_setup import LOGGER_NAME
from .models import BatchResult

logger = logging.getLogger(LOGGER_NAME)

#: Entity types to exclude from the "does this workspace hold data?" answer. **Empty on purpose.**
#:
#: The tempting entries are PFB-generated ``*_set`` membership tables -- they are real rows that a
#: researcher did not author, so excluding them would make the count read more like "how much data
#: arrived". They are not excluded, because this check has no reference to compare against: the only
#: thing it can say honestly is whether Rawls reports anything at all, and a filter would let a
#: successful-but-empty import look identical to a workspace holding only set tables. Add an entry
#: here only for a type Terra creates *without* an import having happened.
IGNORED_ENTITY_TYPES: frozenset[str] = frozenset()


@dataclass(frozen=True)
class WorkspaceData:
    """What Rawls reports the workspace holds, after the import."""

    counts: dict[str, int] = field(default_factory=dict)

    @property
    def total_rows(self) -> int:
        return sum(self.counts.values())

    @property
    def table_count(self) -> int:
        return len(self.counts)

    @property
    def has_data(self) -> bool:
        return self.total_rows > 0

    @property
    def empty_tables(self) -> list[str]:
        """Types Rawls knows about that hold no rows -- a table created but never populated."""
        return sorted(name for name, count in self.counts.items() if count == 0)


@dataclass(frozen=True)
class QcResult:
    """The verdict, plus everything an operator needs to triage it without re-running."""

    batch: BatchResult
    data: WorkspaceData
    namespace: str
    workspace_name: str

    @property
    def jobs_passed(self) -> bool:
        return self.batch.all_succeeded

    @property
    def data_passed(self) -> bool:
        return self.data.has_data

    @property
    def status_vocabulary_passed(self) -> bool:
        """No status was observed that this tool's vocabulary does not cover.

        A failure here is not a failed import -- it means Orchestration returned something new, and
        the tool's ``KNOWN_STATUSES`` (or Orchestration's status translation) needs attention. It is
        reported rather than ignored precisely because the alternative is silently mapping an unknown
        state onto "probably failed".
        """
        return not self.batch.unknown_statuses and not self.batch.forbidden_statuses

    @property
    def passed(self) -> bool:
        return self.jobs_passed and self.data_passed and self.status_vocabulary_passed

    def render(self) -> str:
        lines = [
            self.batch.render(),
            "",
            f"Workspace data — {self.namespace}/{self.workspace_name}",
            f"  {self.data.table_count} table(s), {self.data.total_rows} row(s) total",
        ]
        for name, count in sorted(self.data.counts.items()):
            lines.append(f"    {name}: {count}")
        if not self.data.counts:
            lines.append("    (no entity types — the workspace is empty)")
        if self.data.empty_tables:
            lines.append(
                f"  Tables present but empty: {', '.join(self.data.empty_tables)}"
            )
        if self.batch.unknown_statuses:
            lines.append(
                f"  Unknown job statuses observed: {sorted(self.batch.unknown_statuses)} — either "
                "this tool's status list needs updating or status translation regressed."
            )
        if self.batch.forbidden_statuses:
            lines.append(
                f"  Statuses that must never reach a client were observed: "
                f"{sorted(self.batch.forbidden_statuses)}."
            )
        lines += [
            "",
            f"Jobs:  {'PASS' if self.jobs_passed else 'FAIL'}   "
            f"Data:  {'PASS' if self.data_passed else 'FAIL'}   "
            f"Status vocabulary:  {'PASS' if self.status_vocabulary_passed else 'FAIL'}",
            f"Overall QC — {'PASS' if self.passed else 'FAIL'}",
            "",
            "Not verified by this check: row counts against any source of truth, column or value "
            "correctness, entity relationships, or whether two PFBs that share ids merged as "
            "intended. A PASS means every job finished and data arrived — open the workspace's Data "
            "tab to review what actually landed.",
        ]
        return "\n".join(lines)


def read_workspace_data(rawls: RawlsClient, namespace: str, name: str) -> WorkspaceData:
    """Read the workspace's entity-type counts (one Rawls call, regardless of fan-out width)."""
    metadata = rawls.entity_type_metadata(namespace, name)
    counts = {
        entity_type: int(body.get("count", 0))
        for entity_type, body in (metadata or {}).items()
        if entity_type not in IGNORED_ENTITY_TYPES
    }
    logger.info(
        "Workspace %s/%s holds %d table(s), %d row(s).",
        namespace,
        name,
        len(counts),
        sum(counts.values()),
    )
    return WorkspaceData(counts=counts)


def run_qc(
    *, rawls: RawlsClient, namespace: str, workspace_name: str, batch: BatchResult
) -> QcResult:
    """Read the workspace back and combine it with the fan-out's job outcomes into one verdict.

    Run even when jobs failed. A partial fan-out is the interesting case -- knowing that 7 of 8 jobs
    succeeded *and* that the workspace holds 7 tables tells an operator something that neither fact
    alone does.
    """
    data = read_workspace_data(rawls, namespace, workspace_name)
    return QcResult(batch=batch, data=data, namespace=namespace, workspace_name=workspace_name)
