#!/bin/bash
source ./setup.json
#make clean all
sleep 1
gdb -ex run --args init -b 1 -s 1 -c 1 -m 1 -S 1 -I 0 -d $device -L 2

