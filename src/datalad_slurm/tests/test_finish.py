"""Regression tests for `datalad slurm-finish` path handling.

`slurm-schedule` records output paths relative to the dataset root (prefixed
with the submission directory when the job was scheduled from a subdirectory).
`slurm-finish` must therefore resolve and glob those paths relative to the
dataset root, independent of the directory it is invoked from. Otherwise,
running `slurm-finish` from any directory other than the submission directory
or the dataset root silently saves nothing, while still removing the job from
the database.
"""

import os
import os.path as op
import sqlite3
import json

import pytest

import datalad.api as da
from datalad.tests.utils_pytest import (
    assert_result_count,
    with_tempfile,
)
from datalad.utils import chpwd

from datalad_slurm import finish as finish_mod


def _seed_open_job(ds, slurm_job_id, outputs, slurm_outputs, pwd, alt_dir=""):
    """Insert a fake open-job row into the slurm database."""
    from datalad_slurm.common import get_database_path

    db_path = get_database_path(ds)
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS open_jobs (
        slurm_job_id INTEGER,
        message TEXT,
        chain TEXT,
        cmd TEXT,
        dsid TEXT,
        inputs TEXT,
        extra_inputs TEXT,
        outputs TEXT,
        slurm_outputs TEXT,
        pwd TEXT,
        alt_dir TEXT)
        """
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS locked_prefixes (slurm_job_id INTEGER, prefix TEXT)"
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS locked_names (slurm_job_id INTEGER, name TEXT)"
    )
    cur.execute("DELETE FROM open_jobs WHERE slurm_job_id = ?", (slurm_job_id,))
    cur.execute(
        "INSERT INTO open_jobs VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (
            slurm_job_id,
            "test message",
            json.dumps([]),
            "sbatch slurm.sh",
            ds.id,
            json.dumps([]),
            json.dumps([]),
            json.dumps(outputs),
            json.dumps(slurm_outputs),
            pwd,
            alt_dir,
        ),
    )
    con.commit()
    con.close()


def _open_job_count(ds, slurm_job_id):
    from datalad_slurm.common import get_database_path

    con = sqlite3.connect(get_database_path(ds))
    cur = con.cursor()
    try:
        cur.execute(
            "SELECT COUNT(*) FROM open_jobs WHERE slurm_job_id = ?", (slurm_job_id,)
        )
        n = cur.fetchone()[0]
    except sqlite3.Error:
        n = 0
    con.close()
    return n


@pytest.fixture(autouse=True)
def completed_job(monkeypatch):
    """Make `get_job_status` report a completed job, without touching slurm."""
    monkeypatch.setattr(
        finish_mod,
        "get_job_status",
        lambda job_id: ({str(job_id): "COMPLETED"}, "COMPLETED"),
    )


@with_tempfile(mkdir=True)
def test_finish_from_any_directory(path=None):
    """The job is committed no matter which directory it is finished from.

    The output is recorded as an absolute path (as produced by `slurm-schedule
    -o $PWD`), but resolved by `finish_cmd` while the current working directory
    varies. Only the dataset root and the submission directory happen to align.
    """
    ds = da.create(path, annex=False)
    # a directory that is neither the dataset root nor the submission dir
    intermediate = op.join(ds.path, "a")
    submit = op.join(intermediate, "b")
    os.makedirs(submit, exist_ok=True)

    output_abs = op.join(submit, "out.txt")
    with open(output_abs, "w") as f:
        f.write("original\n")
    ds.save(path=".", message="track output")

    slurm_job_id = "12345"

    for fin_cwd in (submit, intermediate, ds.path):
        # reset the tracked file and the database row for each attempt
        with open(output_abs, "w") as f:
            f.write("original\n")
        ds.save(path=".", message="reset")

        # the job overwrote its output
        with open(output_abs, "w") as f:
            f.write("job output for %s\n" % op.basename(fin_cwd))

        # output recorded as absolute, exactly as `-o $PWD/out.txt` does
        _seed_open_job(ds, slurm_job_id, [output_abs], [], "a/b")

        before = ds.repo.get_hexsha()
        with chpwd(fin_cwd):
            results = list(finish_mod.finish_cmd(slurm_job_id, dataset=None))
        after = ds.repo.get_hexsha()

        # the change must have been committed ...
        assert before != after, (
            "no commit was made when finishing from %s" % fin_cwd
        )
        # ... and the job must be removed from the database
        assert _open_job_count(ds, slurm_job_id) == 0
        # at least one save result must reference the output
        assert any(
            r.get("action") == "add"
            and op.basename(r.get("path", "")) == op.basename(output_abs)
            for r in results
        )


@with_tempfile(mkdir=True)
def test_finish_relative_output_from_any_directory(path=None):
    """Relative outputs (recorded relative to the dataset root) also work."""
    ds = da.create(path, annex=False)
    intermediate = op.join(ds.path, "a")
    submit = op.join(intermediate, "b")
    os.makedirs(submit, exist_ok=True)

    output_rel = "a/b/out.txt"
    output_abs = op.join(ds.path, output_rel)
    with open(output_abs, "w") as f:
        f.write("original\n")
    ds.save(path=".", message="track output")

    slurm_job_id = "67890"
    for fin_cwd in (submit, intermediate, ds.path):
        with open(output_abs, "w") as f:
            f.write("original\n")
        ds.save(path=".", message="reset")
        with open(output_abs, "w") as f:
            f.write("job output\n")
        _seed_open_job(ds, slurm_job_id, [output_rel], [], "a/b")

        before = ds.repo.get_hexsha()
        with chpwd(fin_cwd):
            list(finish_mod.finish_cmd(slurm_job_id, dataset=None))
        assert before != ds.repo.get_hexsha(), (
            "no commit was made when finishing from %s" % fin_cwd
        )
        assert _open_job_count(ds, slurm_job_id) == 0


@with_tempfile(mkdir=True)
def test_finish_keeps_job_on_save_failure(path=None):
    """A failed save must not remove the job from the database."""
    ds = da.create(path, annex=False)
    submit = op.join(ds.path, "a", "b")
    os.makedirs(submit, exist_ok=True)

    output_rel = "a/b/out.txt"
    with open(op.join(ds.path, output_rel), "w") as f:
        f.write("original\n")
    ds.save(path=".", message="track output")

    slurm_job_id = "54321"
    _seed_open_job(ds, slurm_job_id, [output_rel], [], "a/b")

    # force Save to report an error result
    original_save = finish_mod.Save.__call__

    def failing_save(*args, **kwargs):
        yield {
            "action": "save",
            "status": "error",
            "path": ds.path,
            "message": "forced failure",
        }

    finish_mod.Save.__call__ = failing_save
    try:
        with chpwd(ds.path):
            results = list(finish_mod.finish_cmd(slurm_job_id, dataset=None))
    finally:
        finish_mod.Save.__call__ = original_save

    # the job must still be open so it can be finished again
    assert _open_job_count(ds, slurm_job_id) == 1
    assert_result_count(results, 1, action="slurm-finish", status="error")
