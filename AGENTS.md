# AGENTS.md

## Project

`datalad-slurm` is a DataLad extension adding `datalad slurm-schedule`,
`slurm-finish`, and `slurm-reschedule` commands for HPC/SLURM workflows.
Source lives under `src/` (src-layout); the extension entry point is declared
in `pyproject.toml` under `datalad.extensions`.

## Setup

Requires DataLad and git-annex. In a virtualenv:

    pip install -e .

DataLad is intentionally NOT a Python dependency, so installing this package
does not force a particular DataLad version.

## Tests

- Unit tests: `python -m pytest src/datalad_slurm/tests/`. CI additionally
  runs with `--doctest-modules --cov=datalad_slurm`.
- `tests/*.sh` are end-to-end tests that require a real SLURM cluster and
  cannot be run locally.
- In tests, change directories with `datalad.utils.chpwd`, not `os.chdir`:
  DataLad's `getpwd()` reads `$PWD`, which `os.chdir` does not update.

## Style

- Format with `black`; lint with `flake8` (`.flake8`, max-line-length 88) and
  `isort`; spell-check with `codespell` (config in `pyproject.toml`). CI runs
  codespell on `develop`.
- Do not add comments unless they explain non-obvious behavior.

## Path handling (important)

`slurm-schedule` records output paths relative to the dataset root, and slurm
stdout/stderr files relative to the submission directory (stored in the `pwd`
database column). Code that resolves or globs these paths must not depend on
the current working directory: resolve and glob them relative to the dataset
root. Ignoring this causes `slurm-finish` to silently save nothing depending
on where it is invoked.

## Branching & releases

- The default branch is `develop`. Create feature branches off `develop` and
  open PRs against `develop` (see `CONTRIBUTING.md`). `main` mirrors the
  latest release.
- Add a changelog fragment in `changelog.d/` (scriv) for user-visible changes.
- A release is triggered only by a PR merged into `main` that carries the
  `release` label (plus a `semver-*` label), or by manually dispatching the
  **Release** workflow on branch `main` (never `develop`). See `RELEASE.md`.
- The PyPI upload runs from `publish.yml` on **Release workflow completion**
  (`workflow_run`), because `GITHUB_TOKEN`-triggered `release` events do not
  start new workflow runs. It requires the PyPI trusted publisher configured
  for workflow `publish.yml` and environment `pypi`.
