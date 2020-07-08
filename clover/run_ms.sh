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

#
# HACK: Specify your IB DEVICE ID starting from 0.
#       Follow the sequence of ibv_devinfo.
#
ibdev_id=0

#
# num-clients: number of computing nodes
# num-memory: number of memory nodes
# memcached-server-ip: ip of memcached server instance
#
./init -b 1 -S 1 -I $1			\
       -L 2				\
       -device-id $ibdev_id		\
       --num-clients=1			\
       --num-memory=1			\
       --memcached-server-ip=127.0.0.1
