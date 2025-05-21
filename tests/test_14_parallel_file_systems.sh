#!/usr/bin/env bash

# ERRORS
set -e # stop on errors

# Test datalad **without** 'slurm-schedule' and 'slurm-finish' functionality. 
# Only a loop of file creation and comitts to confirm the hypothesis that git 
# on parallel file systems is the source of long delays once the repository is 
# large enough in terms of number of files.
#
# Expected results: should run without any errors

# necessary argument for the directory where the experiment repository will be created inside
if [[ -z $1 ]] ; then

    echo "no temporary directory for this test given, abort"
    echo ""
    echo "... call as $0 <dir>"

    exit -1
fi
HERE=$1

# optionmal second argument for how many files per commit should be used, default is 1
NUMFILES=$2
if [[ -z $NUMFILES ]] ; then

    NUMFILES=1
fi

# optionmal second argument for how many files per commit should be used, default is 1
FILESIZE=$3
if [[ -z $FILESIZE ]] ; then

    FILESIZE=100
fi


# adjust the number of loop steps
TARGETS=`seq 1 10000`


DT="datalad-slurm-test-14_"`date -Is|tr -d ":"`
echo "START "$DT

HERE=$HERE/$DT
mkdir -p $HERE


## create test repos/dirs

TESTDIR="repository.datalad"
echo "CREATE REPO: $HERE/$TESTDIR"
datalad create -c text2git $HERE/$TESTDIR


### from here on we are inside the repository
cd $HERE/$TESTDIR

echo ""
echo "start"
echo ""

echo "num_jobs time">../timing.txt
echo $0 $* >../experiment.txt
hostname >../hostname.txt

# one loop:
#       - create a subdir == one dataset
#       - add some files inside
#       - commit via "datalad save" or via "git commit"
#
for i in $TARGETS ; do

    # make a directory hierarchy instead of too many directories
    M=$(($i%100))

    echo $M/$i

    DIR="$M/$i"
    mkdir -p $DIR

    for e in `seq $NUMFILES`; do

        if [[ $((e % 2)) -eq 1 ]]; then

            tr -dc A-Za-z0-9 </dev/urandom | head -c $FILESIZE >$DIR/output.$e.txt
        else
            tr -dc A-Za-z0-9 </dev/urandom | head -c $FILESIZE | gzip >$DIR/output.$e.txt.gz
        fi
    done

    /usr/bin/time -f "%e" -o ../timing.txt -a datalad save $DIR -m "commit dataset $i"
done

echo ""
echo "done"
