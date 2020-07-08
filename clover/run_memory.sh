#!/bin/bash

Help=$(cat <<-"HELP"

run_memory.sh - Run a Clover Memory Node

Usage:
    ./run_memory.sh <id>

Options:
    <id>   Unique Clover Node ID.
    -h     Show this message.

Examples:
    ./run_memory.sh 2

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
# Configuration
#
# ibdev_id: Specify your IB DEVICE ID starting from 0.
#           Follow the sequence of ibv_devinfo.
# ibdev_base_port: the port index of @ibdev_id.
# Therefore, @ibdev_id + @ibdev_base_port identify one specific IB dev port.
#
# NR_CN: number of computing/client nodes
# NR_MN: number of memory nodes
# MEMCACHED_SERVER_IP: ip of memcached server instance
#
ibdev_id=0
ibdev_base_port=1
NR_CN=1
NR_MN=1
MEMCACHED_SERVER_IP="137.110.222.243"

LD_PRELOAD=libhugetlbfs.so HUGETLB_MORECORE=yes				\
./init -M 1 -L 2							\
       --machine-id=$1							\
       --base-port-index=$ibdev_base_port -device-id=$ibdev_id		\
       --num-clients=$NR_CN						\
       --num-memory=$NR_MN						\
       --memcached-server-ip=$MEMCACHED_SERVER_IP
