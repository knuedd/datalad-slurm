### 🐛 Bug Fixes

- Fix if repository path is != its realpath: `datalad slurm-schedule` and
  `datalad slurm-finish` now detect a mismatch between a dataset's recorded
  root path and its realpath (e.g. when reached through a symlinked path) and
  report it with a hint to run the command from the realpath.
