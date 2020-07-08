#!/bin/bash

Help=$(cat <<-"HELP"

run_ms.sh - Run a Clover Metadata Server

Usage:
    ./run_ms.sh <id>

Options:
    <id>   Unique Clover Node ID.
    -h     Show this message.

Examples:
    ./run_ms.sh 0

HELP
)

help() {
	echo "$Help"
}

if [[ $# == 0 ]] || [[ "$1" == "-h" ]]; then
	help
	exit 1
fi

#LD_PRELOAD=libhugetlbfs.so HUGETLB_MORECORE=yes numactl --physcpubind=0 --localalloc ./init -b 1 -S 1 -I $1 -d $device -L 2
device=1
./init -b 1 -S 1 -I $1 -d $device -L 2
