# Tests for the datalad Slurm extension

The following tests scripts can be executed manually and should run correctly or produce errors that should be handled as errors.

Since it needs to work on datalad repositories which are also git repositories, and because a working Slurm environment is required, this is not (yet) part of automated CI tests ... let's see later if this would be feasible via git CI anyway.

## Running the tests

Each test should be run as:

`./test_x.sh <dir>`, where `<dir>` is some (temporary) directory to store the test results.

All tests will create their own temporary datalad repo inside `<dir>` and work inside that. They can be removed after with `chmod -R u+w datalad-slurm-test*/; rm -Rf datalad-slurm-test*/`

You must have a `slurm_config.txt` file to run the tests, containing account and partition information. A template `slurm_config_template.txt` is provided.

Descriptions of each test can be found in the top of the test script. 

Note: `test_08_timings.sh` and `test_08_timings_very_many_jobs_dont_finish.sh` are a bit different to the other tests, in that they don't test the `datalad-slurm` functionality, but only the time scaling properties. 
