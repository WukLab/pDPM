# Passive Disaggregated Persistent Memory at USENIX ATC 2020

[**[USENIX ATC 2020 Paper]**](https://cseweb.ucsd.edu/~yiying/pDPM-ATC20.pdf)  &nbsp;
[**[Slide]**](./Documentation/ATC20-pDPM-slides.pdf)  &nbsp;
[**[Slide-Short]**](./Documentation/ATC20-pDPM-slides-short.pdf) &nbsp;
[**[Talk]**](https://www.youtube.com/watch?v=Oexu-3Sfbxk&t=163s)

## pDPM

We explore an alternative approach of building Disaggregated Persistent Memory (DPM) by treating storage nodes as _passive_ parties that do not perform any data processing or data management tasks, a model we call Passive Disaggregated Persistent Memory, or __pDPM__.

pDPM lowers owning and energy cost, also avoids storage node being the processing scalability bottleneck. pDPM is an instance of _passive disaggregation_ approach and has largely been overlooked in the past. Our work does a thorough exploration of this area.

<p align="center">
<img src="./Documentation/disaggregation-research-spectrum.png" >
</p>

## pDPM-based Key-Value Stores

Based on where to process and manage data, we build three pDPM-based key-value stores: pDPM-Direct, pDPM-Cental, and Clover. All of them provide GET/PUT interfaces and have been tested against YCSB workload.

<!-- Both pDPM-Direct and pDPM-Central are developed based on [HERD](https://github.com/efficient/rdma_bench/tree/master/herd). -->

<p align="center">
<img src="./Documentation/pDPM-systems.png" >
</p>

## Tutorial

All systems in this repository are _userspace_ programs, and requires no special kernel modifications. They should be able to run on top of any popular Linux distributions with the following software packages installed:
- libibverbs
- memcached
- numactl
- C++ boost coroutine

For hardware, each machine must have a RDMA NIC card (e.g., Mellanox CX5) and connected by a switch. All systems are able to run on both RoCE and Infiniband mode. If you do not have such testbed, you can consider using [CloudLab](https://www.cloudlab.us/).

At a high-level, the setup flows of all systems are very similar: start a server instance, then start a set of simulated passive memory instances, and finally start a set of compute instances. All of them leverage memcached as a centralized RDMA-metdata store.

Clover is a vanilla development effort. Both pDPM-Central and pDPM-Direct build on top of a high-performance two-sided KVS called [HERD](https://github.com/efficient/rdma_bench/tree/master/herd). For detailed setup tutorials, please refer to the following documents:
- [Clover](./Documentation/clover.md)
- [Central](./Documentation/central.md)
- [Direct](./Documentation/direct.md)

Code was developed by [Shin-Yeh Tsai](https://www.cs.purdue.edu/homes/tsai46/) at 2018-2019 during his doctoral research at Purdue University.

## Contact

[Shin-Yeh Tsai](https://www.cs.purdue.edu/homes/tsai46/),
[Yizhou Shan](http://lastweek.io),
[Yiying Zhang](https://cseweb.ucsd.edu/~yiying/).

For more disaggregation-related research, please check out our publication list at [WukLab.io](http://wuklab.io).

## Cite

To cite our pDPM paper, please use the following bibtex:

```
@inproceedings {ATC20-pDPM,
title = {{Disaggregating Persistent Memory and Controlling Them Remotely: An Exploration of Passive Disaggregated Key-Value Stores}},
author = {Shin-Yeh Tsai and Yizhou Shan and Yiying Zhang},
booktitle = {2020 {USENIX} Annual Technical Conference ({USENIX} {ATC} 20)},
year = {2020},
url = {https://www.usenix.org/conference/atc20/presentation/tsai},
publisher = {{USENIX} Association},
month = jul,
}
```
