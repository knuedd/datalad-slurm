# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 et:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Reschedule commands recorded with `datalad schedule`"""

__docformat__ = "restructuredtext"


import json
import logging
import os.path as op
import re
import sys
from copy import copy
from functools import partial
from itertools import dropwhile

from datalad.consts import PRE_INIT_COMMIT_SHA
from datalad.core.local.run import (
    _format_cmd_shorty,
    assume_ready_opt,
    format_command,
)
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
from datalad.interface.common_opts import jobs_opt
from datalad.interface.results import get_status_dict
from datalad.support.constraints import (
    EnsureNone,
    EnsureStr,
)
from datalad.support.exceptions import CapturedException
from datalad.support.json_py import load_stream
from datalad.support.param import Parameter

from datalad.utils import (
    SequenceFormatter,
    chpwd,
    ensure_list,
    ensure_unicode,
    get_dataset_root,
    getpwd,
    join_cmdline,
    quote_cmdlinearg,
)

from datalad.core.local.run import (
    _format_cmd_shorty,
    get_command_pwds,
    _display_basic,
    prepare_inputs,
    _prep_worktree,
    format_command,
    normalize_command,
    _create_record,
    _format_iospecs,
    _get_substitutions,
)

from datalad.support.globbedpaths import GlobbedPaths

# from .schedule import _execute_slurm_command
from .schedule import run_command

from .common import get_finish_info, check_finish_exists, get_schedule_info

lgr = logging.getLogger("datalad.local.reschedule")

reschedule_assume_ready_opt = copy(assume_ready_opt)
reschedule_assume_ready_opt._doc += """
Note that this option also affects any additional outputs that are
automatically inferred based on inspecting changed files in the schedule commit."""


@build_doc
class Reschedule(Interface):
    """Re-execute previous `datalad schedule` commands.

    This will unlock any dataset content that is on record to have
    been modified by the command in the specified revision.  It will
    then re-execute the command in the recorded path (if it was inside
    the dataset). Afterwards, all modifications will be saved.

    *Report mode*

    || REFLOW >>
    When called with [CMD: --report CMD][PY: report=True PY], this command
    reports information about what would be re-executed as a series of records.
    There will be a record for each revision in the specified revision range.
    Each of these will have one of the following "reschedule_action" values:
    << REFLOW ||

      - run: the revision has a recorded command that would be re-executed
      - skip-or-pick: the revision does not have a recorded command and would
        be either skipped or cherry picked
      - merge: the revision is a merge commit and a corresponding merge would
        be made

    The decision to skip rather than cherry pick a revision is based on whether
    the revision would be reachable from HEAD at the time of execution.

    In addition, when a starting point other than HEAD is specified, there is a
    rerun_action value "checkout", in which case the record includes
    information about the revision the would be checked out before rerunning
    any commands.

    .. note::
      Currently the "onto" feature only sets the working tree of the current
      dataset to a previous state. The working trees of any subdatasets remain
      unchanged.
    """

    _params_ = dict(
        revision=Parameter(
            args=("revision",),
            metavar="REVISION",
            nargs="?",
            doc="""reschedule command(s) in `revision`. By default, the command from
            this commit will be executed, but [CMD: --since CMD][PY: `since`
            PY] can be used to construct a revision range. The default value is
            like "HEAD" but resolves to the main branch when on an adjusted
            branch.""",
            default=None,
            constraints=EnsureStr() | EnsureNone(),
        ),
        since=Parameter(
            args=("--since",),
            doc="""If `since` is a commit-ish, the commands from all commits
            that are reachable from `revision` but not `since` will be
            re-executed (in other words, the commands in :command:`git log
            SINCE..REVISION`). If SINCE is an empty string, it is set to the
            parent of the first commit that contains a recorded command (i.e.,
            all commands in :command:`git log REVISION` will be
            re-executed).""",
            constraints=EnsureStr() | EnsureNone(),
        ),
        branch=Parameter(
            metavar="NAME",
            args=(
                "-b",
                "--branch",
            ),
            doc="create and checkout this branch before reschedulening the commands.",
            constraints=EnsureStr() | EnsureNone(),
        ),
        onto=Parameter(
            metavar="base",
            args=("--onto",),
            doc="""start point for rerunning the commands. If not specified,
            commands are executed at HEAD. This option can be used to specify
            an alternative start point, which will be checked out with the
            branch name specified by [CMD: --branch CMD][PY: `branch` PY] or in
            a detached state otherwise. As a special case, an empty value for
            this option means the parent of the first run commit in the
            specified revision list.""",
            constraints=EnsureStr() | EnsureNone(),
        ),
        message=Parameter(
            args=(
                "-m",
                "--message",
            ),
            metavar="MESSAGE",
            doc="""use MESSAGE for the reran commit rather than the
            recorded commit message.  In the case of a multi-commit
            rerun, all the reran commits will have this message.""",
            constraints=EnsureStr() | EnsureNone(),
        ),
        script=Parameter(
            args=("--script",),
            metavar="FILE",
            doc="""extract the commands into [CMD: FILE CMD][PY: this file PY]
            rather than rerunning. Use - to write to stdout instead. [CMD: This
            option implies --report. CMD]""",
            constraints=EnsureStr() | EnsureNone(),
        ),
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""specify the dataset from which to rerun a recorded
            command. If no dataset is given, an attempt is made to
            identify the dataset based on the current working
            directory. If a dataset is given, the command will be
            executed in the root directory of this dataset.""",
            constraints=EnsureDataset() | EnsureNone(),
        ),
        report=Parameter(
            args=("--report",),
            action="store_true",
            doc="""Don't actually re-execute anything, just display what would
            be done. [CMD: Note: If you give this option, you most likely want
            to set --output-format to 'json' or 'json_pp'. CMD]""",
        ),
        assume_ready=reschedule_assume_ready_opt,
        jobs=jobs_opt,
    )

    _examples_ = [
        dict(
            text="Re-execute the command from the previous commit",
            code_py="reschedule()",
            code_cmd="datalad reschedule",
        ),
        dict(
            text="Re-execute any commands in the last five commits",
            code_py="reschedule(since='HEAD~5')",
            code_cmd="datalad reschedule --since=HEAD~5",
        ),
        dict(
            text="Do the same as above, but re-execute the commands on top of "
            "HEAD~5 in a detached state",
            code_py="reschedule(onto='', since='HEAD~5')",
            code_cmd="datalad reschedule --onto= --since=HEAD~5",
        ),
        dict(
            text="Re-execute all previous commands and compare the old and "
            "new results",
            code_cmd="""% # on master branch
                % datalad reschedule --branch=verify --since=
                % # now on verify branch
                % datalad diff --revision=master..
                % git log --oneline --left-right --cherry-pick master...""",
        ),
    ]

    @staticmethod
    @datasetmethod(name="reschedule")
    @eval_results
    def __call__(
        revision=None,
        *,
        since=None,
        dataset=None,
        branch=None,
        message=None,
        onto=None,
        script=None,
        report=False,
        assume_ready=None,
        jobs=None,
    ):

        ds = require_dataset(
            dataset, check_installed=True, purpose="reschedule a command"
        )
        ds_repo = ds.repo

        lgr.debug("rescheduling command output underneath %s", ds)

        if not ds_repo.get_hexsha():
            yield get_status_dict(
                "run",
                ds=ds,
                status="impossible",
                message="cannot reschedule command, nothing recorded",
            )
            return

        # ATTN: Use get_corresponding_branch() rather than is_managed_branch()
        # for compatibility with a plain GitRepo.
        if (
            onto is not None or branch is not None
        ) and ds_repo.get_corresponding_branch():
            yield get_status_dict(
                "run",
                ds=ds,
                status="impossible",
                message=(
                    "--%s is incompatible with adjusted branch",
                    "branch" if onto is None else "onto",
                ),
            )
            return

        if branch and branch in ds_repo.get_branches():
            yield get_status_dict(
                "run",
                ds=ds,
                status="error",
                message="branch '{}' already exists".format(branch),
            )
            return

        # get branch
        rev_branch = (
            ds_repo.get_corresponding_branch() or ds_repo.get_active_branch() or "HEAD"
        )

        if revision is None:
            revision = rev_branch

        if not ds_repo.commit_exists(revision + "^"):
            # Only a single commit is reachable from `revision`.  In
            # this case, --since has no effect on the range construction.
            revrange = revision
        elif since is None:
            revrange = "{rev}^..{rev}".format(rev=revision)
        elif since.strip() == "":
            revrange = revision
        else:
            revrange = "{}..{}".format(since, revision)

        # get the revrange to check for datalad finish corresponding command
        # don't allow reschedule because we only check for the original job
        if not since:
            job_finished = check_finish_exists(
                ds, revision, rev_branch, allow_reschedule=False
            )
            if not job_finished:
                if job_finished == 0:
                    err_msg = (
                        f"Commit {revision[:7]} is not a scheduled job. \n"
                        "N.B., already re-scheduled jobs cannot be re-re-scheduled."
                    )
                else:
                    err_msg = f"No finish found for schedule commit {revision}"
                yield get_status_dict(
                    "run",
                    ds=ds,
                    status="error",
                    message=err_msg,
                )
                return
        results = _rerun_as_results(ds, revrange, since, branch, onto, message, rev_branch)
        if script:
            handler = _get_script_handler(script, since, revision)
        elif report:
            handler = _report
        else:
            handler = partial(
                _rerun, assume_ready=assume_ready, explicit=True, jobs=jobs
            )

        for res in handler(ds, results):
            yield res


def _revrange_as_results(dset, revrange):
    ds_repo = dset.repo
    rev_lines = ds_repo.get_revisions(
        revrange, fmt="%H %P", options=["--reverse", "--topo-order"]
    )
    if not rev_lines:
        return

    for rev_line in rev_lines:
        # The strip() below is necessary because, with the format above, a
        # commit without any parent has a trailing space. (We could also use a
        # custom `rev-list --parents ...` call to avoid this.)
        fields = rev_line.strip().split(" ")
        rev, parents = fields[0], fields[1:]
        res = get_status_dict("run", ds=dset, commit=rev, parents=parents)
        full_msg = ds_repo.format_commit("%B", rev)
        try:
            msg, info = get_schedule_info(dset, full_msg, allow_reschedule=False)
        except ValueError as exc:
            # Recast the error so the message includes the revision.
            raise ValueError("Error on {}'s message".format(rev)) from exc

        if info is not None:
            if len(parents) != 1:
                lgr.warning(
                    "%s has run information but is a %s commit; "
                    "it will not be re-executed",
                    rev,
                    "merge" if len(parents) > 1 else "root",
                )
                continue
            res["run_info"] = info
            res["run_message"] = msg
        yield dict(res, status="ok")


def _rerun_as_results(dset, revrange, since, branch, onto, message, rev_branch):
    """Represent the rerun as result records.

    In the standard case, the information in these results will be used to
    actually re-execute the commands.
    """

    try:
        results = _revrange_as_results(dset, revrange)
    except ValueError as exc:
        ce = CapturedException(exc)
        yield get_status_dict("run", status="error", message=str(ce), exception=ce)
        return

    ds_repo = dset.repo
    # Drop any leading commits that don't have a run command. These would be
    # skipped anyways.
    results = list(dropwhile(lambda r: "run_info" not in r, results))
    if not results:
        yield get_status_dict(
            "run",
            status="impossible",
            ds=dset,
            message=("No schedule commits found in range %s", revrange),
        )
        return


    if onto is not None and onto.strip() == "":
        onto = results[0]["commit"] + "^"

    if onto and not ds_repo.commit_exists(onto):
        yield get_status_dict(
            "run",
            ds=dset,
            status="error",
            message=("Revision specified for --onto (%s) does not exist.", onto),
        )
        return

    start_point = onto or "HEAD"
    if branch or onto:
        yield get_status_dict(
            "run",
            ds=dset,
            # Resolve this to the full hexsha so downstream code gets a
            # predictable form.
            commit=ds_repo.get_hexsha(start_point),
            branch=branch,
            rerun_action="checkout",
            status="ok",
        )

    def skip_or_pick(hexsha, result, msg):
        result["rerun_action"] = "skip-or-pick"
        shortrev = ds_repo.get_hexsha(hexsha, short=True)
        result["message"] = ("%s %s; %s", shortrev, msg, "skipping or cherry picking")

    for res in results:
        hexsha = res["commit"]
        if "run_info" in res:
            rerun_dsid = res["run_info"].get("dsid")
            if rerun_dsid is not None and rerun_dsid != dset.id:
                skip_or_pick(hexsha, res, "was ran from a different dataset")
                res["status"] = "impossible"
            else:
                job_finished = check_finish_exists(
                    dset, hexsha, rev_branch, allow_reschedule=False
                )
                if not job_finished:
                    if job_finished == 0:
                        skip_or_pick(hexsha, res, "not a scheduled job")
                    else:
                        skip_or_pick(hexsha, res, "scheduled job must have a corresponding finish")
                else:
                    res["rerun_action"] = "run"
                    res["diff"] = diff_revision(dset, hexsha)
                    # This is the overriding message, if any, passed to this rerun.
                    res["rerun_message"] = message
        else:
            if len(res["parents"]) > 1:
                res["rerun_action"] = "merge"
            else:
                skip_or_pick(hexsha, res, "does not have a command")
        yield res


def _mark_nonrun_result(result, which):
    msg = dict(skip="skipping", pick="cherry picking")[which]
    result["rerun_action"] = which
    result["message"] = result["message"][:-1] + (msg,)
    return result


def _rerun(dset, results, assume_ready=None, explicit=True, jobs=None):
    ds_repo = dset.repo
    # Keep a map from an original hexsha to a new hexsha created by the rerun
    # (i.e. a reran, cherry-picked, or merged commit).
    new_bases = {}  # original hexsha => reran hexsha
    branch_to_restore = ds_repo.get_active_branch()
    head = onto = ds_repo.get_hexsha()
    for res in results:
        lgr.info(_get_rerun_log_msg(res))
        rerun_action = res.get("rerun_action")
        if not rerun_action:
            yield res
            continue

        res_hexsha = res["commit"]
        if rerun_action == "checkout":
            if res.get("branch"):
                branch = res["branch"]
                checkout_options = ["-b", branch]
                branch_to_restore = branch
            else:
                checkout_options = ["--detach"]
                branch_to_restore = None
            ds_repo.checkout(res_hexsha, options=checkout_options)
            head = onto = res_hexsha
            continue

        # First handle the two cases that don't require additional steps to
        # identify the base, a root commit or a merge commit.

        if not res["parents"]:
            _mark_nonrun_result(res, "skip")
            yield res
            continue

        if rerun_action == "merge":
            old_parents = res["parents"]
            new_parents = [new_bases.get(p, p) for p in old_parents]
            if old_parents == new_parents:
                if not ds_repo.is_ancestor(res_hexsha, head):
                    ds_repo.checkout(res_hexsha)
            elif res_hexsha != head:
                if ds_repo.is_ancestor(res_hexsha, onto):
                    new_parents = [
                        p for p in new_parents if not ds_repo.is_ancestor(p, onto)
                    ]
                if new_parents:
                    if new_parents[0] != head:
                        # Keep the direction of the original merge.
                        ds_repo.checkout(new_parents[0])
                    if len(new_parents) > 1:
                        msg = ds_repo.format_commit("%B", res_hexsha)
                        ds_repo.call_git(
                            [
                                "merge",
                                "-m",
                                msg,
                                "--no-ff",
                                "--allow-unrelated-histories",
                            ]
                            + new_parents[1:]
                        )
                    head = ds_repo.get_hexsha()
                    new_bases[res_hexsha] = head
            yield res
            continue

        # For all the remaining actions, first make sure we're on the
        # appropriate base.

        parent = res["parents"][0]
        new_base = new_bases.get(parent)
        head_to_restore = None  # ... to find our way back if we skip.

        if new_base:
            if new_base != head:
                ds_repo.checkout(new_base)
                head_to_restore, head = head, new_base
        elif parent != head and ds_repo.is_ancestor(onto, parent):
            if rerun_action == "run":
                ds_repo.checkout(parent)
                head = parent
            else:
                _mark_nonrun_result(res, "skip")
                yield res
                continue
        else:
            if parent != head:
                new_bases[parent] = head

        # We've adjusted base. Now skip, pick, or run the commit.

        if rerun_action == "skip-or-pick":
            if ds_repo.is_ancestor(res_hexsha, head):
                _mark_nonrun_result(res, "skip")
                if head_to_restore:
                    ds_repo.checkout(head_to_restore)
                    head, head_to_restore = head_to_restore, None
                yield res
                continue
            else:
                ds_repo.cherry_pick(res_hexsha)
                _mark_nonrun_result(res, "pick")
                yield res
        elif rerun_action == "run":
            run_info = res["run_info"]
            # Keep a "rerun" trail.
            if "chain" in run_info:
                run_info["chain"].append(res_hexsha)
            else:
                run_info["chain"] = [res_hexsha]

            # now we have to find out what was modified during the last run,
            # and enable re-modification ideally, we would bring back the
            # entire state of the tree with #1424, but we limit ourself to file
            # addition/not-in-place-modification for now
            auto_outputs = (ap["path"] for ap in new_or_modified(res["diff"]))
            outputs = run_info.get("outputs", [])
            outputs_dir = op.join(dset.path, run_info["pwd"])
            auto_outputs = [
                p
                for p in auto_outputs
                # run records outputs relative to the "pwd" field.
                if op.relpath(p, outputs_dir) not in outputs
            ]

            # remove the slurm outputs from the previous run from the outputs
            old_slurm_outputs = run_info.get("slurm_run_outputs", [])
            outputs = [output for output in outputs if output not in old_slurm_outputs]

            message = res["rerun_message"] or res["run_message"]
            message = check_job_pattern(message)
            for r in run_command(
                run_info["cmd"],
                dataset=dset,
                inputs=run_info.get("inputs", []),
                extra_inputs=run_info.get("extra_inputs", []),
                outputs=outputs,
                assume_ready=assume_ready,
                explicit=explicit,
                rerun_outputs=auto_outputs,
                message=message,
                jobs=jobs,
                rerun_info=run_info,
            ):
                yield r
        new_head = ds_repo.get_hexsha()
        if new_head not in [head, res_hexsha]:
            new_bases[res_hexsha] = new_head
        head = new_head

    if branch_to_restore:
        # The user asked us to replay the sequence onto a branch, but the
        # history had merges, so we're in a detached state.
        ds_repo.update_ref("refs/heads/" + branch_to_restore, "HEAD")
        ds_repo.checkout(branch_to_restore)


def _get_rerun_log_msg(res):
    "Prepare log message for a rerun to summarize an action about to happen"
    msg = ""
    rerun_action = res.get("rerun_action")
    if rerun_action:
        msg += rerun_action
    if res.get("commit"):
        msg += " commit %s;" % res.get("commit")[:7]
    rerun_run_message = res.get("run_message")
    if rerun_run_message:
        if len(rerun_run_message) > 20:
            rerun_run_message = rerun_run_message[:17] + "..."
        msg += " (%s)" % rerun_run_message
    rerun_message = res.get("message")
    if rerun_message:
        msg += " " + rerun_message[0] % rerun_message[1:]
    msg = msg.lstrip()
    return msg


def _report(dset, results):
    ds_repo = dset.repo
    for res in results:
        if "run_info" in res:
            if res["status"] != "impossible":
                res["diff"] = list(res["diff"])
                # Add extra information that is useful in the report but not
                # needed for the rerun.
                out = ds_repo.format_commit("%an%x00%aI", res["commit"])
                res["author"], res["date"] = out.split("\0")
        yield res


def _get_script_handler(script, since, revision):
    ofh = sys.stdout if script.strip() == "-" else open(script, "w")

    def fn(dset, results):
        ds_repo = dset.repo
        header = """\
#!/bin/sh
#
# This file was generated by running (the equivalent of)
#
#   datalad rerun --script={script}{since} {revision}
#
# in {ds}{path}\n"""
        ofh.write(
            header.format(
                script=script,
                since="" if since is None else " --since=" + since,
                revision=ds_repo.get_hexsha(revision),
                ds="dataset {} at ".format(dset.id) if dset.id else "",
                path=dset.path,
            )
        )

        for res in results:
            if res["status"] != "ok":
                yield res
                return

            if "run_info" not in res:
                continue

            run_info = res["run_info"]
            cmd = run_info["cmd"]

            expanded_cmd = format_command(
                dset,
                cmd,
                **dict(
                    run_info, dspath=dset.path, pwd=op.join(dset.path, run_info["pwd"])
                ),
            )

            msg = res["run_message"]
            if msg == _format_cmd_shorty(expanded_cmd):
                msg = ""

            ofh.write("\n" + "".join("# " + ln for ln in msg.splitlines(True)) + "\n")
            commit_descr = ds_repo.describe(res["commit"])
            ofh.write(
                "# (record: {})\n".format(
                    commit_descr if commit_descr else res["commit"]
                )
            )

            ofh.write(expanded_cmd + "\n")
        if ofh is not sys.stdout:
            ofh.close()

        if ofh is sys.stdout:
            yield None
        else:
            yield get_status_dict(
                "run",
                ds=dset,
                status="ok",
                path=script,
                message=("Script written to %s", script),
            )

    return fn


def diff_revision(dataset, revision="HEAD"):
    """Yield files that have been added or modified in `revision`.

    Parameters
    ----------
    dataset : Dataset
    revision : string, optional
        Commit-ish of interest.

    Returns
    -------
    Generator that yields AnnotatePaths instances
    """
    if dataset.repo.commit_exists(revision + "^"):
        fr = revision + "^"
    else:
        # No other commits are reachable from this revision.  Diff
        # with an empty tree instead.
        fr = PRE_INIT_COMMIT_SHA

    def changed(res):
        return res.get("action") == "diff" and res.get("state") != "clean"

    diff = dataset.diff(
        recursive=True,
        fr=fr,
        to=revision,
        result_filter=changed,
        return_type="generator",
        result_renderer="disabled",
    )
    for r in diff:
        yield r


def new_or_modified(diff_results):
    """Filter diff result records to those for new or modified files."""
    for r in diff_results:
        if r.get("type") in ("file", "symlink") and r.get("state") in [
            "added",
            "modified",
        ]:
            r.pop("status", None)
            yield r


# def run_command(cmd, dataset=None, inputs=None, outputs=None, expand=None,
#                 assume_ready=None, explicit=False, message=None, sidecar=None,
#                 dry_run=False, jobs=None,
#                 extra_info=None,
#                 rerun_info=None,
#                 extra_inputs=None,
#                 rerun_outputs=None,
#                 inject=False,
#                 parametric_record=False,
#                 remove_outputs=False,
#                 skip_dirtycheck=False,
#                 yield_expanded=None,):
#     """Run `cmd` in `dataset` and record the results.

#     `Run.__call__` is a simple wrapper over this function. Aside from backward
#     compatibility kludges, the only difference is that `Run.__call__` doesn't
#     expose all the parameters of this function. The unexposed parameters are
#     listed below.

#     Parameters
#     ----------
#     extra_info : dict, optional
#         Additional information to dump with the json run record. Any value
#         given here will take precedence over the standard run key. Warning: To
#         avoid collisions with future keys added by `run`, callers should try to
#         use fairly specific key names and are encouraged to nest fields under a
#         top-level "namespace" key (e.g., the project or extension name).
#     rerun_info : dict, optional
#         Record from a previous run. This is used internally by `rerun`.
#     extra_inputs : list, optional
#         Inputs to use in addition to those specified by `inputs`. Unlike
#         `inputs`, these will not be injected into the {inputs} format field.
#     rerun_outputs : list, optional
#         Outputs, in addition to those in `outputs`, determined automatically
#         from a previous run. This is used internally by `rerun`.
#     inject : bool, optional
#         Record results as if a command was run, skipping input and output
#         preparation and command execution. In this mode, the caller is
#         responsible for ensuring that the state of the working tree is
#         appropriate for recording the command's results.
#     parametric_record : bool, optional
#         If enabled, substitution placeholders in the input/output specification
#         are retained verbatim in the run record. This enables using a single
#         run record for multiple different re-runs via individual
#         parametrization.
#     remove_outputs : bool, optional
#         If enabled, all declared outputs will be removed prior command
#         execution, except for paths that are also declared inputs.
#     skip_dirtycheck : bool, optional
#         If enabled, a check for dataset modifications is unconditionally
#         disabled, even if other parameters would indicate otherwise. This
#         can be used by callers that already performed analog verififcations
#         to avoid duplicate processing.
#     yield_expanded : {'inputs', 'outputs', 'both'}, optional
#         Include a 'expanded_%s' item into the run result with the expanded list
#         of paths matching the inputs and/or outputs specification,
#         respectively.


#     Yields
#     ------
#     Result records for the run.
#     """
#     if not cmd:
#         lgr.warning("No command given")
#         return
#     specs = {
#         k: ensure_list(v) for k, v in (('inputs', inputs),
#                                        ('extra_inputs', extra_inputs),
#                                        ('outputs', outputs))
#     }

#     rel_pwd = rerun_info.get('pwd') if rerun_info else None
#     if rel_pwd and dataset:
#         # recording is relative to the dataset
#         pwd = op.normpath(op.join(dataset.path, rel_pwd))
#         rel_pwd = op.relpath(pwd, dataset.path)
#     else:
#         pwd, rel_pwd = get_command_pwds(dataset)

#     ds = require_dataset(
#         dataset, check_installed=True,
#         purpose='track command outcomes')
#     ds_path = ds.path

#     lgr.debug('tracking command output underneath %s', ds)

#     # skip for callers that already take care of this
#     if not (skip_dirtycheck or rerun_info or inject):
#         # For explicit=True, we probably want to check whether any inputs have
#         # modifications. However, we can't just do is_dirty(..., path=inputs)
#         # because we need to consider subdatasets and untracked files.
#         # MIH: is_dirty() is gone, but status() can do all of the above!
#         if not explicit and ds.repo.dirty:
#             yield get_status_dict(
#                 'run',
#                 ds=ds,
#                 status='impossible',
#                 message=(
#                     'clean dataset required to detect changes from command; '
#                     'use `datalad status` to inspect unsaved changes'))
#             return

#     # everything below expects the string-form of the command
#     cmd = normalize_command(cmd)
#     # pull substitutions from config
#     cmd_fmt_kwargs = _get_substitutions(ds)
#     # amend with unexpanded dependency/output specifications, which might
#     # themselves contain substitution placeholder
#     for n, val in specs.items():
#         if val:
#             cmd_fmt_kwargs[n] = val

#     # apply the substitution to the IO specs
#     expanded_specs = {
#         k: _format_iospecs(v, **cmd_fmt_kwargs) for k, v in specs.items()
#     }
#     # try-expect to catch expansion issues in _format_iospecs() which
#     # expands placeholders in dependency/output specification before
#     # globbing
#     try:
#         globbed = {
#             k: GlobbedPaths(
#                 v,
#                 pwd=pwd,
#                 expand=expand in (
#                     # extra_inputs follow same expansion rules as `inputs`.
#                     ["both"] + (['outputs'] if k == 'outputs' else ['inputs'])
#                 ))
#             for k, v in expanded_specs.items()
#         }
#     except KeyError as exc:
#         yield get_status_dict(
#             'run',
#             ds=ds,
#             status='impossible',
#             message=(
#                 'input/output specification has an unrecognized '
#                 'placeholder: %s', exc))
#         return

#     if not (inject or dry_run):
#         yield from _prep_worktree(
#             ds_path, pwd, globbed,
#             assume_ready=assume_ready,
#             remove_outputs=remove_outputs,
#             rerun_outputs=rerun_outputs,
#             jobs=None)
#     else:
#         # If an inject=True caller wants to override the exit code, they can do
#         # so in extra_info.
#         cmd_exitcode = 0
#         exc = None

#     # prepare command formatting by extending the set of configurable
#     # substitutions with the essential components
#     cmd_fmt_kwargs.update(
#         pwd=pwd,
#         dspath=ds_path,
#         # Check if the command contains "{tmpdir}" to avoid creating an
#         # unnecessary temporary directory in most but not all cases.
#         tmpdir=mkdtemp(prefix="datalad-run-") if "{tmpdir}" in cmd else "",
#         # the following override any matching non-glob substitution
#         # values
#         inputs=globbed['inputs'],
#         outputs=globbed['outputs'],
#     )
#     try:
#         cmd_expanded = format_command(ds, cmd, **cmd_fmt_kwargs)
#     except KeyError as exc:
#         yield get_status_dict(
#             'run',
#             ds=ds,
#             status='impossible',
#             message=('command has an unrecognized placeholder: %s',
#                      exc))
#         return

#     # amend commit message with `run` info:
#     # - pwd if inside the dataset
#     # - the command itself
#     # - exit code of the command
#     run_info = {
#         'cmd': cmd,
#         # rerun does not handle any prop being None, hence all
#         # the `or/else []`
#         'chain': rerun_info["chain"] if rerun_info else [],
#     }
#     # for all following we need to make sure that the raw
#     # specifications, incl. any placeholders make it into
#     # the run-record to enable "parametric" re-runs
#     # ...except when expansion was requested
#     for k, v in specs.items():
#         run_info[k] = globbed[k].paths \
#             if expand in ["both"] + (
#                 ['outputs'] if k == 'outputs' else ['inputs']) \
#             else (v if parametric_record
#                   else expanded_specs[k]) or []

#     if rel_pwd is not None:
#         # only when inside the dataset to not leak information
#         run_info['pwd'] = rel_pwd
#     if ds.id:
#         run_info["dsid"] = ds.id
#     if extra_info:
#         run_info.update(extra_info)

#     if dry_run:
#         yield get_status_dict(
#             "run [dry-run]", ds=ds, status="ok", message="Dry run",
#             run_info=run_info,
#             dry_run_info=dict(
#                 cmd_expanded=cmd_expanded,
#                 pwd_full=pwd,
#                 **{k: globbed[k].expand() for k in ('inputs', 'outputs')},
#             )
#         )
#         return

#     if not inject:
#         cmd_exitcode, exc, slurm_job_id = _execute_slurm_command(cmd_expanded, pwd, save_tracking_file=False)
#         run_info['exit'] = cmd_exitcode

#     # slurm_job_output = [f"slurm-job-submission-{slurm_job_id}"]

#     # Re-glob to capture any new outputs.
#     #
#     # TODO: If a warning or error is desired when an --output pattern doesn't
#     # have a match, this would be the spot to do it.
#     if explicit or expand in ["outputs", "both"]:
#         # also for explicit mode we have to re-glob to be able to save all
#         # matching outputs
#         globbed['outputs'].expand(refresh=True)
#         if expand in ["outputs", "both"]:
#             run_info["outputs"] = globbed['outputs'].paths

#     # create the run record, either as a string, or written to a file
#     # depending on the config/request
#     record, record_path = _create_record(run_info, sidecar, ds)

#     # abbreviate version of the command for illustrative purposes
#     cmd_shorty = _format_cmd_shorty(cmd_expanded)

#     msg_path = None

#     expected_exit = rerun_info.get("exit", 0) if rerun_info else None
#     if cmd_exitcode and expected_exit != cmd_exitcode:
#         status = "error"
#     else:
#         status = "ok"

#     run_result = get_status_dict(
#         "run", ds=ds,
#         status=status,
#         # use the abbrev. command as the message to give immediate clarity what
#         # completed/errors in the generic result rendering
#         message=cmd_shorty,
#         run_info=run_info,
#         # use the same key that `get_status_dict()` would/will use
#         # to record the exit code in case of an exception
#         exit_code=cmd_exitcode,
#         exception=exc,
#         # Provide msg_path and explicit outputs so that, under
#         # on_failure='stop', callers can react to a failure and then call
#         # save().
#         msg_path=str(msg_path) if msg_path else None,
#     )
#     if record_path:
#         # we the record is in a sidecar file, report its ID
#         run_result['record_id'] = record
#     for s in ('inputs', 'outputs'):
#         # this enables callers to further inspect the outputs without
#         # performing globbing again. Together with remove_outputs=True
#         # these would be guaranteed to be the outcome of the executed
#         # command. in contrast to `outputs_to_save` this does not
#         # include aux file, such as the run record sidecar file.
#         # calling .expand_strict() again is largely reporting cached
#         # information
#         # (format: relative paths)
#         if yield_expanded in (s, 'both'):
#             run_result[f'expanded_{s}'] = globbed[s].expand_strict()
#     yield run_result


def check_job_pattern(text):
    pattern = r"Submitted batch job \d+: Pending"
    match = re.search(pattern, text)

    if not match:
        return text

    if text == match.group(0):
        return None

    return text.replace(match.group(0), "").strip()
