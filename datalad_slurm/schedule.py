# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Schedule a slurm command"""

__docformat__ = "restructuredtext"


import json
import logging
import os
import subprocess
import re
import os.path as op
from argparse import REMAINDER
from pathlib import Path
from tempfile import mkdtemp
import sqlite3

import datalad
from datalad.distribution.dataset import (
    EnsureDataset,
    datasetmethod,
    require_dataset,
)
from datalad.interface.base import (
    Interface,
    build_doc,
    eval_results,
)
from datalad.interface.common_opts import (
    jobs_opt,
    save_message_opt,
)
from datalad.interface.results import get_status_dict
from datalad.interface.utils import generic_result_renderer
from datalad.support.constraints import (
    EnsureBool,
    EnsureChoice,
    EnsureNone,
)
from datalad.support.globbedpaths import GlobbedPaths
from datalad.support.param import Parameter
from datalad.ui import ui
from datalad.utils import ensure_list

from datalad.core.local.run import (
    _format_cmd_shorty,
    get_command_pwds,
    _display_basic,
    _prep_worktree,
    format_command,
    normalize_command,
    _format_iospecs,
    _get_substitutions,
)

from .common import connect_to_database

lgr = logging.getLogger("datalad.slurm.schedule")

assume_ready_opt = Parameter(
    args=("--assume-ready",),
    constraints=EnsureChoice(None, "inputs", "outputs", "both"),
    doc="""Assume that inputs do not need to be retrieved and/or outputs do not
    need to unlocked or removed before running the command. This option allows
    you to avoid the expense of these preparation steps if you know that they
    are unnecessary.""",
)


@build_doc
class Schedule(Interface):
    """
    Summary:
    This class schedules a Slurm script to be run and records it in the git history.

    It is recommended to craft the command such that it can run in the root
    directory of the dataset that the command will be recorded in. However,
    as long as the command is executed somewhere underneath the dataset root,
    the exact location will be recorded relative to the dataset root.

    If the executed command did not alter the dataset in any way, no record of
    the command execution is made.

    If the given command errors, a `CommandError` exception with the same exit
    code will be raised, and no modifications will be saved. A command
    execution will not be attempted, by default, when an error occurred during
    input or output preparation. This default ``stop`` behavior can be
    overridden via [CMD: --on-failure ... CMD][PY: `on_failure=...` PY].

    In the presence of subdatasets, the full dataset hierarchy will be checked
    for unsaved changes prior command execution, and changes in any dataset
    will be saved after execution. Any modification of subdatasets is also
    saved in their respective superdatasets to capture a comprehensive record
    of the entire dataset hierarchy state. The associated provenance record is
    duplicated in each modified (sub)dataset, although only being fully
    interpretable and re-executable in the actual top-level superdataset. For
    this reason the provenance record contains the dataset ID of that
    superdataset.

    *Command format*

    || REFLOW >>
    A few placeholders are supported in the command via Python format
    specification. "{pwd}" will be replaced with the full path of the current
    working directory. "{dspath}" will be replaced with the full path of the
    dataset that run is invoked on. "{tmpdir}" will be replaced with the full
    path of a temporary directory. "{inputs}" and "{outputs}" represent the
    values specified by [CMD: --input and --output CMD][PY: `inputs` and
    `outputs` PY]. If multiple values are specified, the values will be joined
    by a space. The order of the values will match that order from the command
    line, with any globs expanded in alphabetical order (like bash). Individual
    values can be accessed with an integer index (e.g., "{inputs[0]}").
    << REFLOW ||

    || REFLOW >>
    Note that the representation of the inputs or outputs in the formatted
    command string depends on whether the command is given as a list of
    arguments or as a string[CMD:  (quotes surrounding the command) CMD]. The
    concatenated list of inputs or outputs will be surrounded by quotes when
    the command is given as a list but not when it is given as a string. This
    means that the string form is required if you need to pass each input as a
    separate argument to a preceding script (i.e., write the command as
    "./script {inputs}", quotes included). The string form should also be used
    if the input or output paths contain spaces or other characters that need
    to be escaped.
    << REFLOW ||

    To escape a brace character, double it (i.e., "{{" or "}}").

    Custom placeholders can be added as configuration variables under
    "datalad.run.substitutions".  As an example:

      Add a placeholder "name" with the value "joe"::

        % datalad configuration --scope branch set datalad.run.substitutions.name=joe
        % datalad save -m "Configure name placeholder" .datalad/config

      Access the new placeholder in a command::

        % datalad run "echo my name is {name} >me"
    """

    result_renderer = "tailored"
    # make run stop immediately on non-success results.
    # this prevents command execution after failure to obtain inputs of prepare
    # outputs. but it can be overriding via the common 'on_failure' parameter
    # if needed.
    on_failure = "stop"

    _params_ = dict(
        cmd=Parameter(
            args=("cmd",),
            nargs=REMAINDER,
            metavar="COMMAND",
            doc="""command for execution. A leading '--' can be used to
            disambiguate this command from the preceding options to
            DataLad.""",
        ),
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""specify the dataset to record the command results in.
            An attempt is made to identify the dataset based on the current
            working directory. If a dataset is given, the command will be
            executed in the root directory of this dataset.""",
            constraints=EnsureDataset() | EnsureNone(),
        ),
        inputs=Parameter(
            args=("-i", "--input"),
            dest="inputs",
            metavar=("PATH"),
            action="append",
            doc="""A dependency for the run. Before running the command, the
            content for this relative path will be retrieved. A value of "." means "run
            :command:`datalad get .`". The value can also be a glob. [CMD: This
            option can be given more than once. CMD]""",
        ),
        outputs=Parameter(
            args=(
                "-o",
                "--output",
            ),
            dest="outputs",
            metavar=("PATH"),
            action="append",
            doc="""Prepare this relative path to be an output file of the command. A
            value of "." means "run :command:`datalad unlock .`" (and will fail
            if some content isn't present). For any other value, if the content
            of this file is present, unlock the file. Otherwise, remove it.
            [CMD: This option can be given more than once. CMD]""",
        ),
        expand=Parameter(
            args=("--expand",),
            doc="""Expand globs when storing inputs and/or outputs in the
            commit message.""",
            constraints=EnsureChoice(None, "inputs", "outputs", "both"),
        ),
        assume_ready=assume_ready_opt,
        message=save_message_opt,
        check_outputs=Parameter(
            args=("--check-outputs",),
            doc="""Check previous scheduled commits for output conflicts.""",
            constraints=EnsureNone() | EnsureBool(),
        ),
        dry_run=Parameter(
            # Leave out common -n short flag to avoid confusion with
            # `containers-run [-n|--container-name]`.
            args=("--dry-run",),
            doc="""Do not schedule the slurm job; just display details about the
            command execution. A value of "basic" reports a few important
            details about the execution, including the expanded command and
            expanded inputs and outputs. "command" displays the expanded
            command only. Note that input and output globs underneath an
            uninstalled dataset will be left unexpanded because no subdatasets
            will be installed for a dry run.""",
            constraints=EnsureChoice(None, "basic", "command"),
        ),
        alt_dir=Parameter(
            args=(
                "-a",
                "--alt-dir",
            ),
            dest="alt_dir",
            metavar=("PATH"),
            doc="""Provide an alternative directory (alt-dir) prefix where to 
            execute the Slurm job. This needs to be outside of the repository.
            The relative path of all inputs relative to the repository root 
            will be copied to the corresponding relative directory below the alt-dir. 
            Then the Datalad will cd (change dir) to alt_dir/realtive_dir where
            relative dir is the current pwd relative to the repository root.
            Then the job is scheduled there. In the end all output is moved 
            (not copied) back to the relative path inside the repository, 
            to be added and committed.
            This allows to make the Slurm job run on a parallel filesystem
            while the Datalad repository stays on a local filesystem like /tmp/ or
            /scratch/. This reduced the metadata pressure on HPC filesystems.
            """,
        ),
        jobs=jobs_opt,
    )
    _params_[
        "jobs"
    ]._doc += """\
        NOTE: This option can only parallelize input retrieval (get) and output
        recording (save). DataLad does NOT parallelize your scripts for you.
    """

    @staticmethod
    @datasetmethod(name="slurm_schedule")
    @eval_results
    def __call__(
        cmd=None,
        *,
        dataset=None,
        inputs=None,
        outputs=None,
        expand=None,
        assume_ready=None,
        message=None,
        check_outputs=True,
        dry_run=None,
        alt_dir= None,
        jobs=None,
    ):
        for r in schedule_cmd(
            cmd,
            dataset=dataset,
            inputs=inputs,
            outputs=outputs,
            expand=expand,
            assume_ready=assume_ready,
            message=message,
            check_outputs=check_outputs,
            dry_run=dry_run,
            alt_dir=alt_dir,
            jobs=jobs,
        ):
            yield r

    @staticmethod
    def custom_result_renderer(res, **kwargs):
        dry_run = kwargs.get("dry_run")
        if dry_run and "dry_slurm_run_info" in res:
            if dry_run == "basic":
                _display_basic(res)
            elif dry_run == "command":
                ui.message(res["dry_slurm_run_info"]["cmd_expanded"])
            else:
                raise ValueError(f"Unknown dry-run mode: {dry_run!r}")
        else:
            if (
                kwargs.get("on_failure") == "stop"
                and res.get("action") == "run"
                and res.get("status") == "error"
            ):
                msg_path = res.get("msg_path")
                if msg_path:
                    ds_path = res["path"]
                    if datalad.get_apimode() == "python":
                        help = (
                            f"\"Dataset('{ds_path}').save(path='.', "
                            "recursive=True, message_file='%s')\""
                        )
                    else:
                        help = "'datalad save -d . -r -F %s'"
                    lgr.info(
                        "The command had a non-zero exit code. "
                        "If this is expected, you can save the changes with "
                        f"{help}",
                        # shorten to the relative path for a more concise
                        # message
                        Path(msg_path).relative_to(ds_path),
                    )
            generic_result_renderer(res)


def _execute_slurm_command(command, pwd):
    """Execute a Slurm submission command and create a job tracking file.

    Parameters
    ----------
    command : str
        Command to execute (typically an sbatch command)
    pwd : str
        Working directory for command execution

    Returns
    -------
    tuple
        (exit_code, exception)
        exit_code is 0 on success, exception is None on success
    """
    exc = None
    cmd_exitcode = None

    try:
        lgr.info("== Slurm submission start (output follows) =====")
        # Run the command and capture output
        result = subprocess.run(
            command, shell=True, capture_output=True, text=True, cwd=pwd
        )

        # Extract job ID from Slurm output
        # Typical output: "Submitted batch job 123456"
        stdout = result.stdout
        match = re.search(r"Submitted batch job (\d+)", stdout)

        # if match and save_tracking_file:
        #     job_id = match.group(1)
        #     # Create job tracking file
        #     tracking_file = os.path.join(pwd, f"slurm-job-submission-{job_id}")
        #     try:
        #         with open(tracking_file, 'w') as f:
        #             # Could add additional metadata here if needed
        #             pass
        #         lgr.info(f"Created tracking file: {tracking_file}")
        #     except IOError as e:
        #         lgr.warning(f"Failed to create tracking file: {e}")
        #         # Don't fail the command just because tracking file creation failed
        # else:
        #     lgr.warning("Could not extract job ID from Slurm output")

        if match:
            job_id = match.group(1)
            lgr.info("== Slurm submission complete =====")
        else:
            job_id = None
            lgr.warning("Could not extract job ID from Slurm output")

    except subprocess.SubprocessError as e:
        exc = e
        cmd_exitcode = e.returncode if hasattr(e, "returncode") else 1
        lgr.error(f"Command failed with exit code {cmd_exitcode}")

    return cmd_exitcode or 0, exc, job_id


def schedule_cmd(
    cmd,
    dataset=None,
    inputs=None,
    outputs=None,
    expand=None,
    assume_ready=None,
    message=None,
    check_outputs=True,
    dry_run=False,
    alt_dir= None,
    jobs=None,
    explicit=True,
    extra_info=None,
    reslurm_run_info=None,
    extra_inputs=None,
    rerun_outputs=None,
    parametric_record=False,
    remove_outputs=False,
    skip_dirtycheck=False,
    yield_expanded=None,
):
    """Run `cmd` in `dataset` and record the results.

    `Run.__call__` is a simple wrapper over this function. Aside from backward
    compatibility kludges, the only difference is that `Run.__call__` doesn't
    expose all the parameters of this function. The unexposed parameters are
    listed below.

    Parameters
    ----------
    extra_info : dict, optional
        Additional information to dump with the json run record. Any value
        given here will take precedence over the standard run key. Warning: To
        avoid collisions with future keys added by `run`, callers should try to
        use fairly specific key names and are encouraged to nest fields under a
        top-level "namespace" key (e.g., the project or extension name).
    reslurm_run_info : dict, optional
        Record from a previous run. This is used internally by `rerun`.
    extra_inputs : list, optional
        Inputs to use in addition to those specified by `inputs`. Unlike
        `inputs`, these will not be injected into the {inputs} format field.
    rerun_outputs : list, optional
        Outputs, in addition to those in `outputs`, determined automatically
        from a previous run. This is used internally by `rerun`.
    parametric_record : bool, optional
        If enabled, substitution placeholders in the input/output specification
        are retained verbatim in the run record. This enables using a single
        run record for multiple different re-runs via individual
        parametrization.
    remove_outputs : bool, optional
        If enabled, all declared outputs will be removed prior command
        execution, except for paths that are also declared inputs.
    skip_dirtycheck : bool, optional
        If enabled, a check for dataset modifications is unconditionally
        disabled, even if other parameters would indicate otherwise. This
        can be used by callers that already performed analog verififcations
        to avoid duplicate processing.
    yield_expanded : {'inputs', 'outputs', 'both'}, optional
        Include a 'expanded_%s' item into the run result with the expanded list
        of paths matching the inputs and/or outputs specification,
        respectively.


    Yields
    ------
    Result records for the run.
    """
    if not cmd:
        lgr.warning("No command given")
        return
    specs = {
        k: ensure_list(v)
        for k, v in (
            ("inputs", inputs),
            ("extra_inputs", extra_inputs),
            ("outputs", outputs),
        )
    }

    rel_pwd = reslurm_run_info.get("pwd") if reslurm_run_info else None
    if rel_pwd and dataset:
        # recording is relative to the dataset
        pwd = op.normpath(op.join(dataset.path, rel_pwd))
        rel_pwd = op.relpath(pwd, dataset.path)
    else:
        pwd, rel_pwd = get_command_pwds(dataset)

    ds = require_dataset(
        dataset, check_installed=True, purpose="track command outcomes"
    )
    ds_path = ds.path

    lgr.debug("tracking command output underneath %s", ds)

    if not outputs:
        yield get_status_dict(
            "slurm-schedule",
            ds=ds,
            status="impossible",
            message=("At least one output must be specified for datalad schedule."),
        )
        return

    # make all outputs relative to the repository root. 
    # If the job was scheduled from a subdir inside the repo, 
    # this needs to be prefixed
    rel_path= rel=op.relpath(Path.cwd(),ds_path)
    specs["outputs"] = [ op.join(rel_path,output.rstrip("/")) for output in specs["outputs"]]

    # skip for callers that already take care of this
    if not (skip_dirtycheck or reslurm_run_info):
        # For explicit=True, we probably want to check whether any inputs have
        # modifications. However, we can't just do is_dirty(..., path=inputs)
        # because we need to consider subdatasets and untracked files.
        # MIH: is_dirty() is gone, but status() can do all of the above!
        if not explicit and ds.repo.dirty:
            yield get_status_dict(
                "slurm-schedule",
                ds=ds,
                status="impossible",
                message=(
                    "clean dataset required to detect changes from command; "
                    "use `datalad status` to inspect unsaved changes"
                ),
            )
            return

    wildcard_list = ["*", "?", "[", "]", "!", "^", "{", "}"]
    if any(char in output for char in wildcard_list for output in outputs):
        yield get_status_dict(
            "slurm-schedule",
            ds=ds,
            status="impossible",
            message=(
                "Wildcards in output_files are forbidden due to potential conflicts."
            ),
        )
        return

    # everything below expects the string-form of the command
    cmd = normalize_command(cmd)
    # pull substitutions from config
    cmd_fmt_kwargs = _get_substitutions(ds)
    # amend with unexpanded dependency/output specifications, which might
    # themselves contain substitution placeholder
    for n, val in specs.items():
        if val:
            cmd_fmt_kwargs[n] = val

    # apply the substitution to the IO specs
    expanded_specs = {k: _format_iospecs(v, **cmd_fmt_kwargs) for k, v in specs.items()}

    # get all the prefixes of the outputs
    locked_prefixes = get_sub_paths(expanded_specs["outputs"])

    # Check for output conflicts HERE
    # now check history of outputs in un-finished slurm commands
    if check_outputs:
        output_conflict, status_ok = check_output_conflict(
            ds, expanded_specs["outputs"], locked_prefixes
        )
        if not status_ok:
            yield get_status_dict(
                "slurm-schedule",
                ds=ds,
                status="error",
                message=("Database connection cannot be established"),
            )
            return
        if output_conflict:
            yield get_status_dict(
                "slurm-schedule",
                ds=ds,
                status="impossible",
                message=(
                    "There are conflicting outputs with previously scheduled jobs. "
                    "Finish those jobs or adjust output for the current job first."
                ),
            )
            return

    # try-expect to catch expansion issues in _format_iospecs() which
    # expands placeholders in dependency/output specification before
    # globbing
    try:
        globbed = {
            k: GlobbedPaths(
                v,
                pwd=pwd,
                expand=expand
                in (
                    # extra_inputs follow same expansion rules as `inputs`.
                    ["both"]
                    + (["outputs"] if k == "outputs" else ["inputs"])
                ),
            )
            for k, v in expanded_specs.items()
        }
    except KeyError as exc:
        yield get_status_dict(
            "slurm-schedule",
            ds=ds,
            status="impossible",
            message=(
                "input/output specification has an unrecognized " "placeholder: %s",
                exc,
            ),
        )
        return

    if not dry_run:
        yield from _prep_worktree(
            ds_path,
            pwd,
            globbed,
            assume_ready=assume_ready,
            remove_outputs=remove_outputs,
            rerun_outputs=rerun_outputs,
            jobs=None,
        )
    else:
        # If an inject=True caller wants to override the exit code, they can do
        # so in extra_info.
        cmd_exitcode = 0
        exc = None

    # prepare command formatting by extending the set of configurable
    # substitutions with the essential components
    cmd_fmt_kwargs.update(
        pwd=pwd,
        dspath=ds_path,
        # Check if the command contains "{tmpdir}" to avoid creating an
        # unnecessary temporary directory in most but not all cases.
        tmpdir=mkdtemp(prefix="datalad-run-") if "{tmpdir}" in cmd else "",
        # the following override any matching non-glob substitution
        # values
        inputs=globbed["inputs"],
        outputs=globbed["outputs"],
    )
    try:
        cmd_expanded = format_command(ds, cmd, **cmd_fmt_kwargs)
    except KeyError as exc:
        yield get_status_dict(
            "slurm-schedule",
            ds=ds,
            status="impossible",
            message=("command has an unrecognized placeholder: %s", exc),
        )
        return

    # amend commit message with `run` info:
    # - pwd if inside the dataset
    # - the command itself
    # - exit code of the command
    slurm_run_info = {
        "cmd": cmd,
        # rerun does not handle any prop being None, hence all
        # the `or/else []`
        "chain": reslurm_run_info["chain"] if reslurm_run_info else [],
    }

    # for all following we need to make sure that the raw
    # specifications, incl. any placeholders make it into
    # the run-record to enable "parametric" re-runs
    # ...except when expansion was requested
    for k, v in specs.items():
        slurm_run_info[k] = (
            globbed[k].paths
            if expand in ["both"] + (["outputs"] if k == "outputs" else ["inputs"])
            else (v if parametric_record else expanded_specs[k]) or []
        )

    if rel_pwd is not None:
        # only when inside the dataset to not leak information
        slurm_run_info["pwd"] = rel_pwd
    if ds.id:
        slurm_run_info["dsid"] = ds.id
    if extra_info:
        slurm_run_info.update(extra_info)

    if dry_run:
        yield get_status_dict(
            "schedule [dry-run]",
            ds=ds,
            status="ok",
            message="Dry run",
            slurm_run_info=slurm_run_info,
            dry_slurm_run_info=dict(
                cmd_expanded=cmd_expanded,
                pwd_full=pwd,
                **{k: globbed[k].expand() for k in ("inputs", "outputs")},
            ),
        )
        return

    # if alt_dir given, check that it exists
    if alt_dir:
        if not op.isdir(alt_dir):
            yield get_status_dict(
                "slurm-schedule",
                ds=ds,
                status="error",
                message=(f"Alternative job directory '{alt_dir}' doesn't exist"),
            )
            return

    target_pwd= pwd
    if alt_dir:
        target_pwd= op.join(alt_dir,rel_pwd)

        for inp in [inputs, extra_inputs]:
            if inp:
                for i in inp:

                    dirname= op.dirname( i.rstrip("/") ) # remove trailing '/', otherwise dirname() gives the wrong result
                    source_path= op.join(pwd,i)
                    target_dir= op.join(alt_dir,rel_pwd,dirname)
                    os.makedirs( target_dir, exist_ok=True)

                    command=f"cp -r -L -u {source_path} {target_dir}/"
                    print(f"        copy to alternative dir: '{command}' in '{pwd}'")
                    result = subprocess.run(
                        command, shell=True, capture_output=True, text=True, cwd=pwd
                    )
                    # Extract result from copy command
                    #stdout = result.stdout
                    #print("            result: ", result.stdout)


    cmd_exitcode, exc, slurm_job_id = _execute_slurm_command(cmd_expanded, target_pwd) # later target_pwd here
    if not slurm_job_id:
        yield get_status_dict(
            "slurm-schedule",
            ds=ds,
            status="impossible",
            message=("No job was submitted to slurm. "
                     "Check your submission script exists and is valid."),
        )
        return

    slurm_run_info["exit"] = cmd_exitcode
    # TODO: expand these paths
    slurm_outputs, slurm_env_file = get_slurm_output_files(ds_path,slurm_job_id,alt_dir)
    #slurm_run_info["outputs"].extend(slurm_outputs) ## TODO don't have slurm outputs twice, in "outputs" and in "slurm_outputs"
    #slurm_run_info["outputs"].append(slurm_env_file)
    slurm_run_info["slurm_outputs"] = slurm_outputs
    slurm_run_info["slurm_outputs"].append(slurm_env_file)

    # add the slurm job id to the run info
    slurm_run_info["slurm_job_id"] = slurm_job_id

    # Re-glob to capture any new outputs.
    #
    # TODO: If a warning or error is desired when an --output pattern doesn't
    # have a match, this would be the spot to do it.
    if explicit or expand in ["outputs", "both"]:
        # also for explicit mode we have to re-glob to be able to save all
        # matching outputs
        globbed["outputs"].expand(refresh=True)
        if expand in ["outputs", "both"]:
            slurm_run_info["outputs"] = globbed["outputs"].paths
            # add the slurm outputs and environment files
            # these are not captured in the initial globbing
            slurm_run_info["outputs"].extend(slurm_outputs)

    # abbreviate version of the command for illustrative purposes
    cmd_shorty = _format_cmd_shorty(cmd_expanded)

    # add extra info for re-scheduled jobs
    if reslurm_run_info:
        slurm_id_old = reslurm_run_info["slurm_job_id"]
        message += f"\n\nRe-submission of job {slurm_id_old}."

    msg = message if message else None
    msg_path = None

    expected_exit = reslurm_run_info.get("exit", 0) if reslurm_run_info else None
    if cmd_exitcode and expected_exit != cmd_exitcode:
        status = "error"
    else:
        status = "ok"

    status_ok = add_to_database(
        ds, slurm_run_info, msg, expanded_specs["outputs"], locked_prefixes, alt_dir
    )
    if not status_ok:
        yield get_status_dict(
            "slurm-schedule",
            ds=ds,
            status="error",
            message=("Database connection cannot be established"),
        )
        return

    run_result = get_status_dict(
        "slurm-schedule",
        ds=ds,
        status=status,
        # use the abbrev. command as the message to give immediate clarity what
        # completed/errors in the generic result rendering
        message=cmd_shorty,
        slurm_run_info=slurm_run_info,
        # use the same key that `get_status_dict()` would/will use
        # to record the exit code in case of an exception
        exit_code=cmd_exitcode,
        exception=exc,
        # Provide msg_path and explicit outputs so that, under
        # on_failure='stop', callers can react to a failure and then call
        # save().
        msg_path=str(msg_path) if msg_path else None,
    )
    for s in ("inputs", "outputs"):
        # this enables callers to further inspect the outputs without
        # performing globbing again. Together with remove_outputs=True
        # these would be guaranteed to be the outcome of the executed
        # command. in contrast to `outputs_to_save` this does not
        # include aux file, such as the run record sidecar file.
        # calling .expand_strict() again is largely reporting cached
        # information
        # (format: relative paths)
        if yield_expanded in (s, "both"):
            run_result[f"expanded_{s}"] = globbed[s].expand_strict()
    yield run_result


def check_output_conflict(dset, outputs, output_prefixes):
    """
    Check for conflicts between provided outputs and existing outputs in the database.

    Parameters
    ----------
    dset : object
        Dataset object containing repository information.
    outputs : list of str
        List of strings representing output paths to check.

    Returns
    -------
    list
        List of slurm_job_ids that have conflicting outputs.
        Empty list if no conflicts or if database error occurs.
    """
    # Connect to database
    con, cur = connect_to_database(dset, row_factory=True)
    if not con or not cur:
        return None, None

    # Get all existing outputs from database
    try:
        # first check the CURRENT NAMES against PRIOR PREFIXES
        cur.execute("SELECT prefix FROM locked_prefixes")
        existing_prefixes = cur.fetchall()
        has_match = bool(set(existing_prefixes) & set(outputs))
        if has_match:
            return True, True

        # now check CURRENT PREFIXES and against PRIOR NAMES
        cur.execute("SELECT name FROM locked_names")
        existing_names = cur.fetchall()
        has_match = bool(set(existing_names) & set(output_prefixes))
        if has_match:
            return True, True

        # now check CURRENT NAMES and against PRIOR NAMES
        has_match = bool(set(existing_names) & set(outputs))
        if has_match:
            return True, True

    except sqlite3.Error:
        return False, True
    return False, True


def get_sub_paths(paths):
    """
    Extract sub-paths from directories.

    Parameters
    ----------
    paths : list of str
        List of directory paths.

    Returns
    -------
    list of str
        List of sub-paths extracted from the input paths.

    Examples
    --------
    >>> get_sub_paths(['/a/b/c/d/'])
    ['/a', '/a/b', '/a/b/c']
    """
    # Set to store unique sub-paths
    all_sub_paths = set()

    for path in paths:
        # Remove trailing slash if present
        path = path.rstrip("/")

        # Split the path into components
        components = path.split("/")

        # Build sub-paths, excluding the full path
        current_path = ""
        for component in components[:-1]:  # Stop before the last component
            current_path += component + "/"
            all_sub_paths.add(current_path.rstrip("/"))

    # Convert set to sorted list for consistent output
    return sorted(list(all_sub_paths))


def get_slurm_output_files(ds_root, job_id, alt_dir=None):
    """
    Get the relative paths to StdOut and StdErr files for a Slurm job.

    Parameters
    ----------
    job_id : str
        The Slurm job ID.

    Returns
    -------
    list
        List containing relative path(s) to output files. If StdOut and StdErr
        are the same file, returns a single path.

    Raises
    ------
    subprocess.CalledProcessError
        If scontrol command fails.
    ValueError
        If required file paths cannot be found in scontrol output.
    """
    # Run scontrol command and get output
    try:
        result = subprocess.run(
            ["scontrol", "show", "job", str(job_id)],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        raise subprocess.CalledProcessError(
            e.returncode, e.cmd, f"Failed to get job information: {e.output}"
        )

    # Parse output to find StdOut and StdErr
    parsed_data = parse_slurm_output(result.stdout)
    if "ArrayJobId" in parsed_data:
        array_task_id = parsed_data["ArrayTaskId"]
        slurm_job_ids = generate_array_job_names(str(job_id), str(array_task_id))
    else:
        slurm_job_ids = [job_id]

    slurm_out_paths = []
    for i, slurm_job_id in enumerate(slurm_job_ids):
        # Run scontrol command and get output
        try:
            result = subprocess.run(
                ["scontrol", "show", "job", str(slurm_job_id)],
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise subprocess.CalledProcessError(
                e.returncode, e.cmd, f"Failed to get job information: {e.output}"
            )

        # Parse output to find StdOut and StdErr
        parsed_data = parse_slurm_output(result.stdout)

        stdout_path = parsed_data.get("StdOut")
        stderr_path = parsed_data.get("StdErr")

        if not stdout_path or not stderr_path:
            raise ValueError("Could not find StdOut or StdErr paths in scontrol output")

        cwd = alt_dir if alt_dir else Path.cwd()

        stdout_path = Path(stdout_path)
        stderr_path = Path(stderr_path)

        if i == 0:
            # Write parsed data to JSON file
            slurm_env_file = stdout_path.parent / f"slurm-job-{job_id}.env.json"
            with open(slurm_env_file, "w") as f:
                json.dump(parsed_data, f, indent=2)
            rel_slurmenv = os.path.relpath(slurm_env_file, cwd)

        # Get relative paths
        try:
            rel_stdout = os.path.relpath(stdout_path, cwd)
            rel_stderr = os.path.relpath(stderr_path, cwd)
        except ValueError as e:
            raise ValueError(f"Cannot compute relative path: {e}")

        slurm_out_paths.append(rel_stdout)
        if rel_stdout != rel_stderr:
            slurm_out_paths.append(rel_stderr)

    return slurm_out_paths, rel_slurmenv


def parse_slurm_output(output):
    """
    Parse SLURM output into a dictionary, handling space-separated assignments.

    Parameters
    ----------
    output : str
        The SLURM output as a string.

    Returns
    -------
    dict
        A dictionary containing the parsed key-value pairs from the SLURM output,
        excluding keys such as 'UserId' and 'JobId' for privacy purposes.
    """
    result = {}
    # TODO Is this necessary for privacy purposes?
    # What is useful to oneself vs for the community when pushing to git
    excluded_keys = {"UserId", "JobId"}
    for line in output.split("\n"):
        # Split line into space-separated parts
        parts = line.strip().split()
        for part in parts:
            if "=" in part:
                key, value = part.split("=", 1)
                if key not in excluded_keys:
                    result[key] = value
    return result


def generate_array_job_names(job_id, job_task_id):
    """
    Generate individual job names for a Slurm array job.

    Parameters
    ----------
    job_id : str
        The base Slurm job ID.
    job_task_id : str
        The array specification (e.g., "1-5", "1,3,5", "1-10:2").

    Returns
    -------
    list of str
        List of job names in the format "job_id_array_index".

    Examples
    --------
    >>> generate_array_job_names("12345", "1-3")
    ['12345_1', '12345_2', '12345_3']
    """
    job_names = []

    # Remove any % limitations if present
    if "%" in job_task_id:
        job_task_id = job_task_id.split("%")[0]

    # Split by comma to handle multiple ranges
    ranges = job_task_id.split(",")

    for range_spec in ranges:
        # Handle individual numbers
        if "-" not in range_spec:
            job_names.append(f"{job_id}_{range_spec}")
            continue

        # Handle ranges with optional step
        range_parts = range_spec.split(":")
        start, end = map(int, range_parts[0].split("-"))
        step = int(range_parts[1]) if len(range_parts) > 1 else 1

        for i in range(start, end + 1, step):
            job_names.append(f"{job_id}_{i}")

    return job_names


def add_to_database(dset, slurm_run_info, message, outputs, prefixes, alt_dir):
    """
    Add a `datalad schedule` command to an sqlite database.

    Parameters
    ----------
    dset : object
        The dataset object.
    slurm_run_info : dict
        A dictionary containing information about the run. Expected keys are:
        - 'slurm_job_id': int
        - 'inputs': list
        - 'extra_inputs': list
        - 'outputs': list
        - 'slurm_outputs': list
        - 'chain': list
        - 'cmd': str
        - 'dsid': str
        - 'pwd': str
    message : str
        The message to be stored in the database.
    outputs : list
        A list of output names to be stored in the database.
    prefixes : list
        A list of prefix names to be stored in the database.

    Returns
    -------
    bool or None
        Returns True if the command was successfully added to the database,
        None if the connection to the database could not be established.
    """
    con, cur = connect_to_database(dset)
    if not cur or not con:
        return None

    # create an empty table if it doesn't exist
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS open_jobs (
    slurm_job_id INTEGER,
    message TEXT,
    chain TEXT CHECK (json_valid(chain)),
    cmd TEXT,
    dsid TEXT,
    inputs TEXT CHECK (json_valid(inputs)),
    extra_inputs TEXT CHECK (json_valid(extra_inputs)),
    outputs TEXT CHECK (json_valid(outputs)),
    slurm_outputs TEXT CHECK (json_valid(slurm_outputs)),
    pwd TEXT,
    alt_dir TEXT
    )
    """
    )

    # convert the inputs to json
    inputs_json = json.dumps(slurm_run_info["inputs"])

    # convert the extra inputs to json
    extra_inputs_json = json.dumps(slurm_run_info["extra_inputs"])

    # convert the outputs to json
    outputs_json = json.dumps(slurm_run_info["outputs"])

    # convert the slurm outputs to json
    slurm_outputs_json = json.dumps(slurm_run_info["slurm_outputs"])

    # convert chain to json
    chain_json = json.dumps(slurm_run_info["chain"])

    # add the most recent schedule command to the table
    cur.execute(
        """
    INSERT INTO open_jobs (slurm_job_id,
    message,
    chain,
    cmd,
    dsid,
    inputs,
    extra_inputs,
    outputs,
    slurm_outputs,
    pwd,
    alt_dir)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            slurm_run_info["slurm_job_id"],
            message,
            chain_json,
            slurm_run_info["cmd"],
            slurm_run_info["dsid"],
            inputs_json,
            extra_inputs_json,
            outputs_json,
            slurm_outputs_json,
            slurm_run_info["pwd"],
            alt_dir if alt_dir else ""
        ),
    )

    # now create the tables with the locked_prefixes and locked_names
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS locked_prefixes (
    slurm_job_id INTEGER,
    prefix TEXT )
    """
    )

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS locked_names (
    slurm_job_id INTEGER,
    name TEXT )
    """
    )

    for output in outputs:
        cur.execute(
            """
        INSERT INTO locked_names (slurm_job_id,
        name)
        VALUES (?, ?)
        """,
            (slurm_run_info["slurm_job_id"], output.rstrip("/")),
        )

    if prefixes:
        for prefix in prefixes:
            cur.execute(
                """
            INSERT INTO locked_prefixes (slurm_job_id,
            prefix)
            VALUES (?, ?)
            """,
                (slurm_run_info["slurm_job_id"], prefix),
            )

    # save and close
    con.commit()
    con.close()

    return True


def _none_to_empty_list(value):
    return [] if value is None else value
