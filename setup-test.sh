#!/bin/bash

##
# Uses a git repo to create a p4 history that we can replay over and
# over to test that o4 indeed leaves it all pristine.

TD=/tmp/zsync
mkdir -p $TD
#mkdir -p /tmp/p4d

# start p4d -r /tmp/p4d -p localhost:1667
# Create the zsync depot: p4 depot zsync

export P4PORT=tcp:localhost:1667
cd $TD

# Create a client: p4 client
# make sure /tmp/o4test is the client root
# make sure zsync is in the mappings
# make sure to set rmdir, not normdir

# cd $(dirname $TD)
#git clone git@github.com:philipbergen/zsync.git
#cd zsync
#git log --pretty=format:"%H" --reverse >|../zsync.log

set -eux
cat ../zsync.log | while read v; do
#for v in $(head ../zsync.log); do
    echo "****************************************************************"
    echo "GIT $v"
    # Make all files readable so git works and

    git clean -df .
    p4 edit ... >/dev/null
    chmod -R a+w .git *
    git reset --hard $v
    #git reset --hard
    p4 add ... >/dev/null
    # Revert unchanged
    p4 revert -a ...
    git ls-files --others --exclude-standard | xargs -n1 p4 delete
    p4 submit -d $v
    git status
    # Create dummy changelists to create sparse changelist history
    n=$((RANDOM % 100))
    if [ $n -gt 0 ]; then
        for i in $(seq 1 $n); do
            txt=$(echo -e "Change: new\nDescription: dummy-change $i" |p4 change -i)
            p4 change -d $(echo $txt | cut -d\  -f2)
        done
    fi
    #echo "Press enter to continue."
    #read
done
