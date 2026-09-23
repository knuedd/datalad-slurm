from datalad.tests.utils_pytest import assert_result_count
import datalad.support.exceptions as dl_exceptions

def test_register():
    import datalad.api as da
    assert hasattr(da, 'slurm_schedule')
    assert hasattr(da, 'slurm_finish')
    assert hasattr(da, 'slurm_reschedule')
    assert_result_count(
        da.slurm_schedule(cmd="echo test", outputs=["res"], dry_run="basic"),
        1,
        status="ok")
    assert_result_count(
        da.slurm_finish(),
        0,
        status="ok")
    try:
        da.slurm_reschedule(since="HEAD~1", report=True)
        assert False
    except dl_exceptions.IncompleteResultsError:
        assert True
