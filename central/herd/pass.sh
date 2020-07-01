#!/bin/bash
source ./setup.json
#count=$(ps -aux | grep mit_insmod.sh| wc -l)
#if [ "$count" != "1" ]; then pgrep --exact mit_insmod.sh | xargs kill -9; fi
ps aux | grep $path | awk '{print $2}' | xargs kill -9
for VARIABLE in "${pass_others[@]}"
do
        VARI="$prefix$VARIABLE"
        scp -q -P 456 plot.py Makefile master.c worker.c main.h client.c main.c run-machine.sh run-servers.sh run-memory.sh pre-run.sh setup.json pkill.sh $VARI:$path &
        scp -q -P 456 ../mica/mica.h ../mica/mica.c $VARI:$path/../mica/ &
        echo finish $VARI
done
wait
for VARIABLE in "${pass_others[@]}"
do
        VARI="$prefix$VARIABLE"
        ssh -t $VARI -t "cd $path ; make clean all -s >/dev/null" &
done
wait
