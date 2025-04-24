#!/usr/bin/env bash

set +e # continue on errors

# Test datalad 'slurm-schedule' and 'slurm-finish --list-open-jobs' and 'slurm-finish' functionality
#   - measure how long they take with growing git log length
#
# Expected results: should run without any errors


if [[ -z $1 ]] ; then

    echo "no temporary directory for tests given, abort"
    echo ""
    echo "... call as $0 <dir>"

    exit -1
fi

D=$1

# optionmal second argument for how many extra output specs should be given per job
NUMOUTEXTRA=$2 
if [[ -z $NUMOUTEXTRA ]] ; then

    NUMOUTEXTRA=0
fi

LIMITJOBS=500 ## max number of jobs to schedule

echo "start"

B=`dirname $0`

echo "from src dir "$B

## create a test repo

TESTDIR=$D/"datalad-slurm-test-09_"`date -Is|tr -d ":"`

datalad create -c text2git $TESTDIR


### generic part for all the tests ending here, specific parts follow ###

if [ ! -f "slurm_config.txt" ]; then
    echo "Error: slurm_config.txt must exist"
    echo "Please see slurm_config_sample.txt for a template"
    exit -1
fi

source slurm_config.txt

# Create the script
cat <<EOF > $TESTDIR/slurm.template.sh
#!/bin/bash
#SBATCH --job-name="DLtest09"         # name of the job
#SBATCH --partition=defq              # partition to be used (defq, gpu or intel)
#SBATCH -A casus
#SBATCH --time=0:02:00                # walltime (up to 96 hours)
#SBATCH --ntasks=1                    # number of nodes
#SBATCH --cpus-per-task=1             # number of tasks per node
#SBATCH --output=log.slurm-%j.out
echo "started"
OUTPUT="output_test_"\$(date -Is|tr -d ":").txt
# simulate some text output
for i in \$(seq 1 20); do
   echo \$i | tee -a \$OUTPUT
   sleep 1s
done
# simulate some binary output which will become an annex file
bzip2 -k \$OUTPUT
echo "ended"
EOF

# Make the script executable
chmod u+x $TESTDIR/slurm.template.sh

cd $TESTDIR
echo "PWD "$PWD

TARGETS=`seq 1 1000`

echo "Create jobs:"
for i in $TARGETS ; do

    M=$(($i%30))

    echo $M/$i

    DIR="$M/test_09_output_dir_$i"
    mkdir -p $DIR.datalad $DIR.slurm

    cp slurm.template.sh $DIR.datalad/slurm.sh
    cp slurm.template.sh $DIR.slurm/slurm.sh

    if [[ 0 == $M ]]; then

        echo "################################################################"
        echo "PWD "$PWD
        echo datalad save -m "add test job dirs and scripts"
        datalad save -m "add test job dirs and scripts"
        echo "################################################################"

    fi
done

echo "################################################################"
echo "PWD "$PWD
echo datalad save -m "add test job dirs and scripts"
datalad save -m "add test job dirs and scripts"
echo "################################################################"

echo "Schedule jobs:"
echo "num_jobs time">timing_schedule.txt
echo "num_jobs time">timing_slurm.txt
#echo "num_jobs time">timing_finish-list.txt
for i in $TARGETS ; do

    M=$(($i%30))
    DIR="$M/test_09_output_dir_$i"
    mkdir -p $DIR.datalad $DIR.slurm

    EXTRAOUT=""
    for e in `seq $NUMOUTEXTRA`; do

        EXTRAOUT=$EXTRAOUT" -o IDONTEXIST$e/test_09_output_dir_$i/test$e.txt"
    done

    echo "    running: datalad slurm-schedule -o $DIR.datalad $EXTRAOUT sbatch --chdir $DIR.datalad slurm.sh"

    echo -n $i" ">>timing_schedule.txt
    /usr/bin/time -f "%e" -o timing_schedule.txt -a datalad slurm-schedule -o $DIR.datalad $EXTRAOUT sbatch --chdir $DIR.datalad slurm.sh

    sleep 0.1s

    echo -n $i" ">>timing_slurm.txt
    /usr/bin/time -f "%e" -o timing_slurm.txt -a sbatch --chdir $DIR.slurm slurm.sh

    sleep 0.1s

    if [[ 0 == $M ]]; then
        while [[ $LIMITJOBS < `squeue -u $USER | grep "DLtest09" | wc -l` ]] ; do

            echo "    ... wait for jobs to finish inbetween"
            sleep 30s
        done
    fi

    ## run this only every 100 rounds
    ## disabled because it gets very slow after 1000 jobs or so
    #if [[ 0 == $M ]]; then
    #    echo -n $i" ">>timing_finish-list.txt
    #    /usr/bin/time -f "%e" -o timing_finish-list.txt -a datalad slurm-finish --list-open-jobs
    #fi
done

scancel -n "DLtest09"

echo "done waiting" 

# for the benchmarking of `datalad slurm-schedule` don't call `datalad slurm-finish` because it takes very long
exit 0

echo "finishing completed jobs:"
/usr/bin/time -f "%e" -o timing_finish.txt -a datalad slurm-finish




