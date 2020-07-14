# Tutorial for Clover

## Overview

In order to run Clover, we need one server for
Metadata Server (MS), at least one server for Computing Node (CN),
and at least one server for Memory Node (MN).
For best performance, we recommend dedicating a physical machine
to each Clover node. In theory, it is possible to consolidate Clover nodes
using VMs and SR-IOV enabled RNIC.

We will also run a memcached server instance acting as a
central RDMA-metadata store.
Clover nodes publish their QP/MR information to memcached,
which are used to build all to all connection by Clover automatically.

## Run

### Quickstart

Switch folder to `pDPM/clover/`.

Suppose we use three servers `[S0, S1, and S2]` to run a `[1 MS, 1 CN, and 1 MN]` setting. We will run both MS and memcached on S0; a single MN on S1; and a single CN on S2. To start, run the following script at each server one by one:
- S0: `memcached -u root -I 128m -m 2048`
- S0: `./run_ms.sh 0`
- S1: `./run_memory.sh 1`
- S2: `./run_clients.sh 2`

The parameter passed to each script is a static unique Clover node ID. Usually the MS uses 0. CNs and MNs will span a contiguous range.

Once experiment finishes, run `local_kill.sh` to terminate all relevant processes.

### Configurations

Most of the runtime configurations are controlled by bash scripts. Since each script file is very similar, we will use `run_ms.sh` to demonstrate.

First, specify the RNIC device via a unique device ID and port index. The device ID starts from 0 and follows the sequence reported by `ibv_devinfo`. The port index is per-device and starts from 1. We can configure them via the following two variables in the script:
```bash
# We use the first port of the first device reported by ibv_devinfo
ibdev_id=0
ibdev_base_port=1
```

Second, specify the number of CNs and MNs used in a paritcular run. We can configure them via the following two variables in the script:
```bash
NR_CN=1
NR_MN=1
```

Third, specify the IP address of the memcached server instance, which is expressed by the following variable:
```bash
MEMCACHED_SERVER_IP="192.168.0.1"
```

Fourth, we could choose RoCE and Infiniband mode by enabling one of the following lines at `ibsetup.h`:
```c
#define RSEC_NETWORK_MODE    RSEC_NETWORK_IB
#define RSEC_NETWORK_MODE    RSEC_NETWORK_ROCE
```

All three scripts: `run_ms.sh`, `run_memory,sh`, and `run_clients.sh` at all machines need to have the same configuration. Otherwise the experiment will not start.

## Code Internal

- All instances' entry point is at `init.cc:main()`. They act differently based on the parameters.
- Client testing code entry point is at `mitsume_benchmark.cc:mitsume_benchmark()`
