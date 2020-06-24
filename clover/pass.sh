#!/bin/bash
source ./setup.json
#count=$(ps -aux | grep mit_insmod.sh| wc -l)
#if [ "$count" != "1" ]; then pgrep --exact mit_insmod.sh | xargs kill -9; fi
ps aux | grep $path | awk '{print $2}' | xargs kill -9

for VARIABLE in "${pass_others[@]}"
do
        VARI="$prefix$VARIABLE"
        rsync -u *.cc *.h *.sh Makefile setup.json $VARI:$path &
        rsync -u -r workload $VARI:$path &
        echo finish $VARI
done
wait

for VARIABLE in "${all[@]}"
do
        VARI="$prefix$VARIABLE"
        ssh -t $VARI -t "cd $path ; make clean -s > /dev/null ; make all -j 8 -s > /dev/null; echo $VARI done" &
#        ssh -t $VARI -t "cd $path ; make all -s > /dev/null; echo $VARI done" &
done
wait

