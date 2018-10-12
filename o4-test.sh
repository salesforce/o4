#!/bin/bash

export P4PORT=tcp:localhost:1667
#TD=/tmp/zsync
#cd $TD

HEAD=$(p4 changes -m1 -s submitted //zsync/... | cut -d\  -f2)

set -eux
for i in $(seq 1 100); do
    n=$((RANDOM%HEAD))
    if [ $n = 0 ]; then
        o4 sync .
    else
        o4 sync .@$n
    fi
    git status >/dev/null
    while read fname; do
        if [[ $fname != */ ]]; then
            echo "*** ERROR: Untracked files."
            exit 1
        fi
    done < <(git ls-files --others --exclude-standard | grep -v '^\.o4')
done
