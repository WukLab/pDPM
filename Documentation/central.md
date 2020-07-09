# Tutorial for pDPM-Central

## Overview

In order to run pDPM-Central, we need one server for
Coordinator, at least one server for Computing Node (CN),
and at least one server for Memory Node (MN).
For best performance, we recommend dedicating a physical machine
to each pDPM-Central node.

Similar to Clover runtime, we will also run a memcached server instance acting as a central RDMA-metadata store.

## Run

### Quickstart

Switch folder to `pDPM/central/herd/`.

Suppose we use three servers `[S0, S1, and S2]` to run a `[1 coordinator, 1 CN, and 1 MN]` setting. We will run both coordinator and memcached on S0; a single MN on S1; and a single CN on S2. To start, run the following script at each server one by one:
- S0: `./run-servers.sh 0` to start coordinator and memcached
- S1: `./run-memory.sh 1` to start MN
- S2: `./run-machine.sh 2` to start CN

The parameter passed to each script is a static unique node ID. Usually the coordinator uses 0. CNs and MNs will span a contiguous range.

Once experiment finishes, run `pkill.sh` to terminate all relevant processes.

### Configurations

Most of the runtime configurations are controlled by bash scripts. We will use `run-servers.sh` to demonstrate.

First, specify the memcached server IP. The script is able to start (and kill) memcached instance automatically. We should use the IP address of the coordinator.
```bash
export HRD_REGISTRY_IP="192.168.0.1"
```