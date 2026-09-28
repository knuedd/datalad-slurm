"""DataLad demo extension"""

__docformat__ = "restructuredtext"

import logging

lgr = logging.getLogger("datalad.slurm")

# Defines a datalad command suite.
# This variable must be bound as a setuptools entrypoint
# to be found by datalad
command_suite = (
    # description of the command suite, displayed in cmdline help
    "Demo DataLad command suite",
    [
        (
            # importable module that contains the schedule command implementation
            "datalad_slurm.schedule",
            # name of the command class implementation in above module
            "Schedule",
            # optional name of the command in the cmdline API
            "slurm-schedule",
            # optional name of the command in the Python API
            "slurm_schedule",
        ),
        (
            # importable module that contains the schedule command implementation
            "datalad_slurm.finish",
            # name of the command class implementation in above module
            "Finish",
            # optional name of the command in the cmdline API
            "slurm-finish",
            # optional name of the command in the Python API
            "slurm_finish",
        ),
        (
            # importable module that contains the schedule command implementation
            "datalad_slurm.reschedule",
            # name of the command class implementation in above module
            "Reschedule",
            # optional name of the command in the cmdline API
            "slurm-reschedule",
            # optional name of the command in the Python API
            "slurm_reschedule",
        ),
    ],
)

try:
    from ._version import version as __version__
except ImportError:  # pragma: no cover - only when running from a source tree
    try:
        from importlib.metadata import version as _pkg_version

        __version__ = _pkg_version("datalad-slurm")
    except Exception:
        __version__ = "0.0.0"
