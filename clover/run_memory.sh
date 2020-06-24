#!/bin/bash
source ./setup.json
#make clean all
if [ -z "$1" ]
  then
    echo "No machine id supplied"
    exit
fi
#./init -b 1 -s 1 -c 2 -m 1 -M 1 -I $1 -d $device -L 2
LD_PRELOAD=libhugetlbfs.so HUGETLB_MORECORE=yes numactl --physcpubind=0 --localalloc ./init -b 1 -M 1 -I $1 -d $device -L 2
