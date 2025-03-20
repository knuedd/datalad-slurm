#!/usr/bin/env bash

set -e # continue on errors

# Test datalad 'slurm-schedule' and 'slurm-finish --list-open-jobs' and 'slurm-finish' functionality
#   - measure how long they take with growing number of currently scheduled jobs
#   - do 3 different experiments at the same time, so that they see the same pressure/background noise
#     of Slurm and of the parallel file system
#
# Expected results: should run without any errors


if [[ -z $1 ]] ; then

    echo "no temporary directory for this test given, abort"
    echo ""
    echo "... call as $0 <dir>"

    exit -1
fi
HERE=$1

if [[ -z $2 ]] ; then

    echo "no temporary directory for alternative execution dirs given, abort"
    echo ""
    echo "... call as $0 <dir>"

    exit -1
fi
DA=$2


# optionmal second argument for how many extra output specs should be given per job
NUMOUTEXTRA=$3
if [[ -z $NUMOUTEXTRA ]] ; then

    NUMOUTEXTRA=0
fi


# adjust the number of jobs per type here
TARGETS=`seq 1 11000`


DT="datalad-slurm-test-09_"`date -Is|tr -d ":"`
echo "start "$DT

HERE=$HERE/$DT
mkdir -p $HERE

DA=$DA/$DT
mkdir -p $DA

## create test repos/dirs

TESTDIR_D="datalad-normal-repo"
datalad create -c text2git $HERE/$TESTDIR_D

TESTDIR_S="slurm-alone-dir"
mkdir -p $HERE/$TESTDIR_S

TESTDIR_L="datalad-alt-dir"
datalad create -c text2git $DA/$TESTDIR_L
mkdir -p $HERE/$TESTDIR_L

### generic part for all the tests ending here, specific parts follow ###

if [ ! -f "slurm_config.txt" ]; then
    echo "Error: slurm_config.txt must exist"
    echo "Please see slurm_config_sample.txt for a template"
    exit -1
fi
source slurm_config.txt

# Create the script
cat <<EOF > $HERE/$TESTDIR_D/slurm.template.sh
#!/bin/bash
#SBATCH --job-name="DLtest09"         # name of the job
#SBATCH --partition=defq              # partition to be used (defq, gpu or intel)
#SBATCH -A casus
#SBATCH --time=0:02:00                # walltime (up to 96 hours)
#SBATCH --ntasks=1                    # number of nodes
#SBATCH --cpus-per-task=1             # number of tasks per node
#SBATCH --output=log.slurm-%j.out
echo "started"
OUTPUT="output_test.txt"
# simulate some text output
for i in \$(seq 1 20); do
   echo \$i | tee -a \$OUTPUT
   sleep 1s
done
# simulate some binary output which will become an annex file
bzip2 -k \$OUTPUT
echo "ended"
EOF

cp $HERE/$TESTDIR_D/slurm.template.sh $HERE/$TESTDIR_S/
cp $HERE/$TESTDIR_D/slurm.template.sh $DA/$TESTDIR_L/

# Make the script executable
chmod u+x $HERE/$TESTDIR_D/slurm.template.sh
chmod u+x $HERE/$TESTDIR_S/slurm.template.sh
chmod u+x $DA/$TESTDIR_L/slurm.template.sh


### from here on we are inside $HERE
cd $HERE

echo "Create jobs:"
for i in $TARGETS ; do

    M=$(($i%30))

    echo $M/$i

    DIR="$M/test_09_output_dir_$i"
    mkdir -p $TESTDIR_D/$DIR/
    mkdir -p $TESTDIR_S/$DIR/
    mkdir -p $DA/$TESTDIR_L/$DIR/

    cp $TESTDIR_D/slurm.template.sh $TESTDIR_D/$DIR/slurm.sh
    cp $TESTDIR_S/slurm.template.sh $TESTDIR_S/$DIR/slurm.sh
    cp $DA/$TESTDIR_L/slurm.template.sh $DA/$TESTDIR_L/$DIR/slurm.sh

done

cd $TESTDIR_D
echo "################################################################"
echo "PWD "$PWD
echo datalad save -m "add test job dirs and scripts"
datalad save -m "add test job dirs and scripts"
echo "################################################################"
cd $HERE

cd $DA/$TESTDIR_L
echo "################################################################"
echo "PWD "$PWD
echo datalad save -m "add test job dirs and scripts"
datalad save -m "add test job dirs and scripts"
echo "################################################################"
cd $HERE

echo "Schedule jobs:"
echo "num_jobs time">timing_schedule.txt
echo "num_jobs time">timing_slurm.txt
echo "num_jobs time">timing_schedule_alt.txt
for i in $TARGETS ; do

    M=$(($i%30))
    DIR="$M/test_09_output_dir_$i"

    EXTRAOUT=""
    for e in `seq $NUMOUTEXTRA`; do

        EXTRAOUT=$EXTRAOUT" -o IDONTEXIST$e/test_09_output_dir_$i/test$e.txt"
    done


    # regular datalad schedule

    cd $TESTDIR_D
    echo "    running: datalad slurm-schedule -i $DIR/slurm.sh -o $DIR $EXTRAOUT sbatch --chdir $DIR slurm.sh"
    echo -n $i" ">>../timing_schedule.txt
    /usr/bin/time -f "%e" -o ../timing_schedule.txt -a datalad slurm-schedule -i $DIR/slurm.sh -o $DIR $EXTRAOUT sbatch --chdir $DIR slurm.sh
    cd $HERE

    sleep 0.5s


    # basic slurm submit

    cd $TESTDIR_S
    echo -n $i" ">>../timing_slurm.txt
    /usr/bin/time -f "%e" -o ../timing_slurm.txt -a sbatch --chdir $DIR slurm.sh
    cd $HERE

    sleep 0.5s


    # datalad schedule with --alt-dir

    cd $DA/$TESTDIR_L
    echo "    running: datalad slurm-schedule -i $DIR/slurm.sh -o $DIR $EXTRAOUT --alt-dir $HERE/$TESTDIR_L sbatch --chdir $DIR slurm.sh"
    echo -n $i" ">>$HERE/timing_schedule_alt.txt
    /usr/bin/time -f "%e" -o $HERE/timing_schedule_alt.txt -a datalad slurm-schedule -i $DIR/slurm.sh -o $DIR $EXTRAOUT --alt-dir $HERE/$TESTDIR_L sbatch --chdir $DIR slurm.sh
    cd $HERE

    sleep 0.5s


    if [[ 0 == $M ]]; then
        while [[ 100 -lt `squeue -u $USER | grep "DLtest09" | wc -l` ]] ; do

            echo "    ... wait for jobs to finish inbetween"
            sleep 30s

            ## remove all with "launch failed requeued held"
            for i in `squeue -u $USER | grep "launch failed requeued held" | awk '{print $1}'`; do
                scancel $i
            done

        done

        echo "    ... continue with "`squeue -u $USER | grep "DLtest09" | wc -l`
    fi
done

echo "finished scheduling, now wait for all the jobs to complete" 

while [[ 0 -lt `squeue -u $USER | grep "DLtest09" | wc -l` ]] ; do

    echo "    ... wait for jobs to finish at the end"
    sleep 30s

    ## remove all with "launch failed requeued held"
    for i in `squeue -u $USER | grep "launch failed requeued held" | awk '{print $1}'`; do
        scancel $i
    done

done

echo "done waiting" 


## finsih all jobs in $TESTDIR_D

cd $TESTDIR_D

echo "get all job ids"
/usr/bin/time -f "%e" -o ../timing_finish_list.txt -a datalad slurm-finish --list-open-jobs | tee $HERE/list_of_jobs_normal.txt


echo "finishing completed jobs:"
echo "num_jobs time">../timing_finish.txt
num=0
for id in `cat $HERE/list_of_jobs_normal.txt | grep COMPLETED  |awk '{print $1}'`; do 

    echo -n $num" ">>../timing_finish.txt
    /usr/bin/time -f "%e" -o ../timing_finish.txt -a datalad slurm-finish --slurm-job-id $id

    let num=num+1
done

cd $HERE


## finsih all jobs in $TESTDIR_L

cd $DA/$TESTDIR_L

echo "get all job ids"
/usr/bin/time -f "%e" -o $HERE/timing_finish_list_alt.txt -a datalad slurm-finish --list-open-jobs | tee $HERE/list_of_jobs_alt.txt


echo "finishing completed jobs:"
echo "num_jobs time">$HERE/timing_finish_alt.txt
num=0
for id in `cat $HERE/list_of_jobs_alt.txt | grep COMPLETED  |awk '{print $1}'`; do 

    echo -n $num" ">>$HERE/timing_finish_alt.txt
    /usr/bin/time -f "%e" -o $HERE/timing_finish_alt.txt -a datalad slurm-finish --slurm-job-id $id

    let num=num+1
done

cd $HERE

echo ""
echo "done"
