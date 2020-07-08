# Clover Tutorial

## Overview

In order to run Clover, we need one server for
Metadata Server (MS), at least one server for Computing Node (CN),
and at least one server for Memory Node (MN).
For best performance, we recommend dedicate a physical machine
to each Clover node. However, it is possible to considate Clover nodes
using VMs.

We will also run a Memcached instance that serves as a
central RDMA metadata store.
Clover nodes publish their QP/MR information to Memcached,
which are used to build all to all connection by Clover automatically.

## Run

## Code

- All instances' entry point is at `init.cc:main()`
