#!/bin/bash
set -eu

##
# copy changes over from ~/blt/blt-code/plugins/perforce

for dir in o4 server gatling; do
    cd $dir
    for fname in *.py requirements.txt; do
        blt_name=$fname
        [ $fname = o4.py ] && blt_name=o4_sync.py
        blt_name=~/blt/blt-code/plugins/perforce/$blt_name
        [ -f $fname -a -f $blt_name ] || continue
        diff -du $fname $blt_name && diff=false || diff=true
        if [ $diff = true ]; then
            echo -en "[$dir/$fname] \tDo you want to overwrite with changes from $blt_name? [Yn] "
            read go
            if [[ "$go" = y || "$go" = Y ]]; then
                cp -a $blt_name $fname
                git diff $fname
                echo -n "Press [ENTER] to continue"
            fi
        fi
    done
    cd ..
done
