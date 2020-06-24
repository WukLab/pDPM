#!/bin/bash
source ./setup.json
#make clean all
gdb -ex run --args init -b 1 -s 1 -c 1 -m 1 -M 1 -I 2 -d $device -L 2
#./init.o -b 1 -s 1 -c 2 -C 1 -I $1 -d 1 -L 2
