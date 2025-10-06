#!/usr/bin/env bash

# ERRORS
set -e # stop on errors

# Test datalad or git **without** 'slurm-schedule' and 'slurm-finish' functionality. 
# 
# Always create a DataLad repository, then do a loop of file creation and 
# comitts to confirm the hypothesis that git on parallel file systems is the 
# source of long delays once the repository is large enough in terms of number of files.
#
# Expected results: should run without any errors

# necessary argument: result directory where timings and metadata will be stored under
if [[ -z $1 ]] ; then

    echo "no result directory given, abort"
    echo ""
    echo "... call as $0 <result-dir> <temp-dir> [<num-files-per-step>] [<size-per-file>] [<num-iterations>] [datalad|git]"
    echo ""

    exit -1
fi
RESULTS=`readlink -f $1`
echo "Results dir "$RESULTS

# necessary argument: the directory where the experiment repository will be created inside
if [[ -z $2 ]] ; then

    echo "no temporary directory for this test given, abort"
    echo ""
    echo "... call as $0 <result-dir> <temp-dir> [<num-files-per-step>] [<size-per-file>] [<num-iterations>] [datalad|git]"
    echo ""

    exit -1
fi
REPOS=`readlink -f $2`
echo "Repos dir "$REPOS

# optional argument for how many files per commit should be used, default is 1
NUMFILES=$3
if [[ -z $NUMFILES ]] ; then

    NUMFILES=1
fi

# optional argument for how many files per commit should be used, default is 1K
FILESIZE=$4
if [[ -z $FILESIZE ]] ; then

    FILESIZE=1K
fi

# optional argument for how many files per commit should be used, default is 1K
LOOP=$5
if [[ -z $LOOP ]] ; then

    LOOP=100
fi

# optional argument for how many files per commit should be used, default is 1K
CMD=$6
if [[ -z $CMD ]] ; then

    CMD="datalad save"
    VERSIONCMD="datalad --version"

elif [[ "git" == $CMD ]] ; then

    CMD="git addcommit"
    VERSIONCMD="git --version"

elif [[ "git commit" == $CMD ]] ; then

    CMD="git addcommit"
    VERSIONCMD="git --version"

elif [[ "datalad" == $CMD ]] ; then

    CMD="datalad save"
    VERSIONCMD="datalad --version"

elif [[ "datalad save" == $CMD ]] ; then

    CMD="datalad save"
    VERSIONCMD="datalad --version"

else

    echo "Wrong command specified, use one of 'git' | ' git commit' | 'datalad' | 'datalad save'"
    exit -2
fi


# adjust the number of loop steps
TARGETS=`seq $LOOP`


## create test repos/dirs

DT="datalad-slurm-test-14_"`date -Is|tr -d ":"`
mkdir -p $RESULTS/$DT $REPOS/$DT
REPO=$REPOS/$DT/"repository.datalad"
datalad create -c text2git $REPO

### from here on we are inside the repository
cd $REPO

# create local alias so that we can add and commit with one command
git config  alias.addcommit '!git add -A && git commit'

HOST=`hostname`
FILESYSTEM=`df -h -T $REPO | tail -n 1 | awk '{print $2}'`
VERSION=`$VERSIONCMD`

# Create JSON string with HOST and CMD
JSON_PAYLOAD=$( jq -n \
    --arg hostname "$HOST" \
    --arg cmd "$CMD" \
    --arg version "$VERSION" \
    --arg cmdline "$*" \
    --arg repo "$REPO" \
    --arg filesystem "$FILESYSTEM" \
    --arg numfiles "$NUMFILES" \
    --arg filesize "$FILESIZE" \
    '{hostname: $hostname, test_command: $cmd, version: $version, commandline: $cmdline, repo: $repo, filesystem: $filesystem, numfiles: $numfiles, filesize: $filesize}' )

# Save JSON to a file
echo "$JSON_PAYLOAD" >$RESULTS/$DT/metadata.json


echo ""
echo "start"
echo ""

echo "num_jobs time">$RESULTS/$DT/timing.txt


echo "num_jobs "\
    "ext4_groupinfo_1k.name ext4_groupinfo_1k.active_objs ext4_groupinfo_1k.num_objs ext4_groupinfo_1k.objsize ext4_groupinfo_1k.objperslab ext4_groupinfo_1k.pagesperslab " \
    "ext4_fc_dentry_update.name ext4_fc_dentry_update.active_objs ext4_fc_dentry_update.num_objs ext4_fc_dentry_update.objsize ext4_fc_dentry_update.objperslab ext4_fc_dentry_update.pagesperslab " \
    "ext4_inode_cache.name ext4_inode_cache.active_objs ext4_inode_cache.num_objs ext4_inode_cache.objsize ext4_inode_cache.objperslab ext4_inode_cache.pagesperslab " \
    "ext4_allocation_context.name ext4_allocation_context.active_objs ext4_allocation_context.num_objs ext4_allocation_context.objsize ext4_allocation_context.objperslab ext4_allocation_context.pagesperslab " \
    "ext4_system_zone.name ext4_system_zone.active_objs ext4_system_zone.num_objs ext4_system_zone.objsize ext4_system_zone.objperslab ext4_system_zone.pagesperslab " \
    "ext4_io_end.name ext4_io_end.active_objs ext4_io_end.num_objs ext4_io_end.objsize ext4_io_end.objperslab ext4_io_end.pagesperslab " \
    "ext4_extent_status.name ext4_extent_status.active_objs ext4_extent_status.num_objs ext4_extent_status.objsize ext4_extent_status.objperslab ext4_extent_status.pagesperslab " >$RESULTS/$DT/proc_slabinfo.txt


# every loop:
#       - create a subdir == one dataset
#       - add some files inside
#       - commit via "datalad save" or via "git commit"
#
for i in $TARGETS ; do

    echo -n $i" " >>$RESULTS/$DT/timing.txt
    echo -n $i" " >>$RESULTS/$DT/proc_slabinfo.txt

    # make a directory hierarchy instead of too many directories
    M=$(($i%100))

    #echo $M $i

    DIR="$M/$i"
    mkdir -p $DIR

    for e in `seq $NUMFILES`; do

        if [[ $((e % 2)) -eq 1 ]]; then

            tr -dc A-Za-z0-9 </dev/urandom | head -c $FILESIZE >$DIR/output.$e.txt
        else
            tr -dc A-Za-z0-9 </dev/urandom | head -c $FILESIZE | gzip >$DIR/output.$e.txt.gz
        fi
    done

    /usr/bin/time -f "%e" -o $RESULTS/$DT/timing.txt -a $CMD -m "commit dataset $i" >/dev/null

    cat /proc/slabinfo|grep ext4|awk '{print $1" "$2" "$3" "$4" "$5" "$6}'| tr '\n' ' ' >>$RESULTS/$DT/proc_slabinfo.txt
    echo "" >>$RESULTS/$DT/proc_slabinfo.txt # add the necessary newline
done

echo ""
cd ../../../

# remove the test repo right away
#echo "remove tmp repo "$REPO

chmod -R u+w $REPO
rm -Rf $REPO
rmdir `dirname $REPO`

echo ""
echo "done"
