#!/usr/bin/env bash

set -e # abort on errors

# Test datalad 'slurm-schedule' and 'slurm-finish' functionality
#   - create some job dirs and job scripts and 'commit' them
#   - then 'datalad slurm-schedule' all jobs from their job dirs
#   - wait until all of them are finished, then run 'datalad slurm-finish'
#
# Expected results: should run without any errors

if [[ -z $1 ]] ; then

    echo "no temporary directory for test repository given, abort"
    echo ""
    echo "... call as $0 <test-dir> <alt-dir>"

    exit -1
fi

D=$1

if [[ -z $2 ]] ; then

    echo "no alternative directory for test repository given, abort"
    echo ""
    echo "... call as $0 <test-dir> <alt-dir>"

    exit -1
fi

A=$2

echo "repository in $D and alternative directory in $A"


## create a test repo

TESTDIR=$D/"datalad-slurm-test-13_"`date -Is|tr -d ":"`

datalad create -c text2git $TESTDIR


### generic part for all the tests ending here, specific parts follow ###

# make the slurm batch script

if [ ! -f "slurm_config.txt" ]; then
    echo "Error: slurm_config.txt must exist"
    echo "Please see slurm_config_sample.txt for a template"
    exit -1
fi

source slurm_config.txt

# Create the script
cat << EOF > $TESTDIR/slurm.template.sh
#!/bin/bash
#SBATCH --job-name="DLtest13"         # name of the job
#SBATCH --partition=$partition        
#SBATCH -A $account
#SBATCH --time=0:05:00                # walltime (up to 96 hours)
#SBATCH --ntasks=1                    # number of nodes
#SBATCH --cpus-per-task=1             # number of tasks per node
#SBATCH --output=log.slurm-%j.out
echo "started"
OUTPUT="output_test.txt"
# simulate some text output
for i in \`seq 1 10\`; do
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

TARGETS=`seq 17 17`

for i in $TARGETS ; do

    DIR="test_13_output_dir_"$i
    mkdir -p $DIR

    cp slurm.template.sh $DIR/slurm.sh
    echo "aaa">$DIR/aaa.txt
    mkdir -p $DIR/bbb
    echo "ccc">$DIR/bbb/ccc.txt
    mkdir -p $DIR/ddd/eee
    echo "fff">$DIR/ddd/eee/fff.txt
    mkdir -p $DIR/ggg/hhh/iii/
    echo "jjj">$DIR/ggg/hhh/iii/jjj.txt
    mkdir -p $DIR/kkk/lll/mmm/
    echo "nnn">$DIR/kkk/lll/mmm/nnn.txt

done

datalad save -m "add test job dirs and scripts"

for i in $TARGETS ; do

    DIR="test_13_output_dir_"$i

    cd $DIR
    datalad slurm-schedule -i slurm.sh -i aaa.txt -i bbb/ccc.txt -i ddd/eee/fff.txt -i ggg/hhh/iii -i kkk/lll/mmm/ -o output_test.txt -o output_test.txt.bz2 --alt-dir $A sbatch slurm.sh
    cd ..

done

while [[ 0 != `squeue -u $USER | grep "DLtest13" | wc -l` ]] ; do

    echo "    ... wait for jobs to finish"
    sleep 1m
done

datalad slurm-finish --list-open-jobs

echo "finishing completed jobs:"
echo "    "$PWD
datalad slurm-finish

#echo " ### git log in this repo ### "
#echo ""
#git log



