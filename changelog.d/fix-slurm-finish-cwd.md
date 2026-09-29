### 🐛 Bug Fixes

- Fix `datalad slurm-finish` silently saving nothing when run from a
  directory other than the job's submission directory or the dataset root.
  Output paths are now resolved and globbed relative to the dataset root, and
  slurm stdout/stderr files relative to the submission directory, so results
  are committed regardless of the current working directory.
- Only remove a finished job from the database after its outputs have
  actually been saved, so a failed save no longer drops the job silently.
