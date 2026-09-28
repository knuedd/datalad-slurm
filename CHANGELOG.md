
<a id='changelog-0.2.7'></a>
# 0.2.7 (2026-09-28)

## 🐛 Bug Fixes

- Fix if repository path is != its realpath: `datalad slurm-schedule` and
  `datalad slurm-finish` now detect a mismatch between a dataset's recorded
  root path and its realpath (e.g. when reached through a symlinked path) and
  report it with a hint to run the command from the realpath.

<a id='changelog-0.2.6'></a>
# 0.2.6 (2026-09-28)

## 🐛 Bug Fixes

- Fix the documentation build by restoring the `build_manpage` setup command,
  making the author lookup independent of the removed `setup.cfg`, and
  installing DataLad in the docs environment.
  [#96](https://github.com/knuedd/datalad-slurm/pull/96)
  (by [@dimok8a](https://github.com/dimok8a))

## 🏠 Internal

- Switch version handling from versioneer to setuptools-scm, deriving the
  version from the git tag, and update GitHub Actions to Node 24
  (actions/checkout@v6, actions/setup-python@v6) while pinning
  `ubuntu-24.04`. [#98](https://github.com/knuedd/datalad-slurm/pull/98)
  (by [@knuedd](https://github.com/knuedd))
