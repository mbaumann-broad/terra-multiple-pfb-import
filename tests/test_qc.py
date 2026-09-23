"""The post-import check: did the fan-out put data in the workspace? No network.

The check is narrow on purpose (there is no source of truth to compare against), so these tests are
mostly about it being *honest*: a PASS must mean something, a FAIL must say which of the three parts
failed, and the report must state what it did not verify.
"""

from __future__ import annotations

from terra_import_prototype.models import BatchResult, ImportJob, StatusSample
from terra_import_prototype.qc import WorkspaceData, read_workspace_data, run_qc
from terra_import_prototype.safety import SignedUrl

BUCKET = "https://gen3-biodatacatalyst-nhlbi-nih-gov-pfb-export.s3.amazonaws.com"
SECRET = "X-Amz-Signature=deadbeef"


class FakeRawls:
    def __init__(self, entities):
        self._entities = entities
        self.calls = 0

    def entity_type_metadata(self, namespace, name):
        self.calls += 1
        return self._entities


def job(name="export_1.avro", status="Done", job_id="j1", history=None, submit_error=None):
    j = ImportJob(
        source_url=SignedUrl(f"{BUCKET}/{name}?{SECRET}"),
        job_id=job_id,
        status=status,
        submit_error=submit_error,
    )
    j.history = [StatusSample(job_id=job_id or "", status=s, t=0.0) for s in (history or [status])]
    return j


def qc(jobs, entities):
    return run_qc(
        rawls=FakeRawls(entities), namespace="ns", workspace_name="ws",
        batch=BatchResult(jobs=jobs),
    )


# --- reading the workspace ---------------------------------------------------


def test_counts_are_read_in_one_call_whatever_the_fan_out_width():
    rawls = FakeRawls({"subject": {"count": 5}, "sample": {"count": 12}})
    data = read_workspace_data(rawls, "ns", "ws")

    assert rawls.calls == 1
    assert data.counts == {"subject": 5, "sample": 12}
    assert data.table_count == 2 and data.total_rows == 17
    assert data.has_data


def test_an_empty_workspace_has_no_data():
    assert not read_workspace_data(FakeRawls({}), "ns", "ws").has_data


def test_a_table_that_exists_but_is_empty_is_not_data():
    """An import job can report Done having written nothing -- a truncated or empty export produces
    exactly that. Counting the *table* rather than its rows would call that a pass."""
    data = read_workspace_data(FakeRawls({"subject": {"count": 0}}), "ns", "ws")

    assert not data.has_data
    assert data.empty_tables == ["subject"]


def test_a_missing_count_field_is_treated_as_zero_not_as_a_crash():
    assert read_workspace_data(FakeRawls({"subject": {}}), "ns", "ws").total_rows == 0


# --- the verdict -------------------------------------------------------------


def test_all_jobs_done_and_data_present_passes():
    result = qc([job(), job(name="export_2.avro", job_id="j2")], {"subject": {"count": 5}})

    assert result.jobs_passed and result.data_passed and result.status_vocabulary_passed
    assert result.passed


def test_jobs_succeeded_but_no_data_fails():
    """The failure mode this check exists for: every job says Done, the workspace is empty."""
    result = qc([job()], {})

    assert result.jobs_passed
    assert not result.data_passed
    assert not result.passed


def test_one_failed_job_fails_the_run_even_with_data_present():
    """Under fan-out a partial import leaves real data behind; it is still not the import asked for."""
    result = qc([job(), job(name="export_2.avro", job_id="j2", status="Error")],
                {"subject": {"count": 5}})

    assert result.data_passed
    assert not result.jobs_passed
    assert not result.passed


def test_a_rejected_submit_fails_the_run():
    result = qc([job(job_id=None, status="Error", submit_error="HTTP 400")], {"subject": {"count": 1}})
    assert not result.jobs_passed


def test_an_unknown_status_fails_the_vocabulary_check_separately():
    """Not a failed import -- a signal that Orchestration returned something new. Reported as its
    own axis so it is not confused with the import having gone wrong."""
    result = qc([job(status="Done", history=["Pending", "Frobnicating", "Done"])],
                {"subject": {"count": 5}})

    assert result.jobs_passed and result.data_passed
    assert not result.status_vocabulary_passed
    assert not result.passed


def test_a_forbidden_status_reaching_the_client_fails():
    result = qc([job(status="Done", history=["Pending", "Upserting", "Done"])],
                {"subject": {"count": 5}})

    assert not result.status_vocabulary_passed
    assert "Upserting" in result.render()


def test_a_run_with_no_jobs_is_not_a_pass():
    """An empty batch must not vacuously satisfy 'every job succeeded'."""
    assert not qc([], {"subject": {"count": 5}}).jobs_passed


# --- the report --------------------------------------------------------------


def test_the_report_never_carries_a_signed_url():
    result = qc([job(), job(name="export_2.avro", job_id="j2", status="Error")],
                {"subject": {"count": 5}})
    rendered = result.render()

    assert SECRET not in rendered
    assert "export_1.avro" in rendered, "the filename is what tells an operator which export failed"


def test_the_report_states_what_it_did_not_verify():
    """The limitation travels with the verdict rather than living only in a document nobody reads
    next to a PASS."""
    rendered = qc([job()], {"subject": {"count": 5}}).render()

    assert "Not verified" in rendered
    assert "source of truth" in rendered


def test_the_report_names_each_failing_axis():
    rendered = qc([job(status="Error")], {}).render()

    assert "Jobs:  FAIL" in rendered
    assert "Data:  FAIL" in rendered
    assert "Overall QC — FAIL" in rendered


def test_the_report_lists_per_table_counts():
    rendered = qc([job()], {"subject": {"count": 5}, "sample": {"count": 12}}).render()

    assert "subject: 5" in rendered and "sample: 12" in rendered
    assert "2 table(s), 17 row(s)" in rendered


def test_an_empty_workspace_says_so_explicitly():
    assert "the workspace is empty" in qc([job()], {}).render()


def test_workspace_data_helpers_are_consistent():
    data = WorkspaceData(counts={"a": 0, "b": 3})
    assert data.total_rows == 3 and data.table_count == 2
    assert data.has_data and data.empty_tables == ["a"]
