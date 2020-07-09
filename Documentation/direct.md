# Tutorial for pDPM-Direct

## Overview

Compared to pDPM-Central and Clover, pDPM-Direct does not need
a centralized server for coordination. However, in order to
keep a consistent running experience and to reduce development cost,
we will run a server script similar to pDPM-Central and Clover.
But this server will not run any pDPM-Direct component except memcached server instance.

## Run

### Quickstart

Switch folder to `pDPM/direct/herd/`.

Suppose we use three servers `[S0, S1, and S2]` to run a `[1 CN and 1 MN]` setting. We will run memcached on S0; a single MN on S1; and a single CN on S2. To start, run the following script at each server one by one:
- S0: `./run-servers.sh 0` to start memcached
- S1: `./run-memory.sh 1` to start MN
- S2: `./run-machine.sh 2` to start CN

Once experiment finishes, run `pkill.sh` to terminate all relevant processes.

### Configurations

Most of the runtime configurations are controlled by bash scripts. We will use `run-servers.sh` to demonstrate.

First, specify the memcached server IP. The script is able to start (and kill) memcached instance automatically. We should use the IP address of the coordinator.
```bash
export HRD_REGISTRY_IP="192.168.0.1"
```