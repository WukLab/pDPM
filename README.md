# Passive Disaggregated Persistent Memory (pDPM) at USENIX ATC 2020

[[USENIX ATC 2020 Paper]](https://cseweb.ucsd.edu/~yiying/pDPM-ATC20.pdf)
[[Slide]](./Documentation/ATC20-pDPM-slides.pdf)
[[Slide-Short]](./Documentation/ATC20-pDPM-slides-short.pdf)

## pDPM

We explore an alternative approach of building Disaggregated Persistent Memory (DPM) by treating storage nodes as _passive_ parties that do not perform any data processing or data management tasks, a model we call Passive Disaggregated Persistent Memory, or __pDPM__.

pDPM lowers owning and energy cost, also avoids storage node being the processing scalability bottleneck. pDPM is an instance of _passive disaggregation_ approach and has largely been overlooked in the past. Our work does a thorough exploration of this area.

<p align="center">
<img src="./Documentation/disaggregation-research-spectrum.png" >
</p>

## pDPM-based Key-Value Stores

Based on where to process and manage data, we build three pDPM-based key-value stores: pDPM-Direct, pDPM-Cental, and Clover. All three pDPM systems run on top of userspace libibverbs, no special kernel modifications are needed.

<!-- Both pDPM-Direct and pDPM-Central are developed based on [HERD](https://github.com/efficient/rdma_bench/tree/master/herd). -->

<p align="center">
<img src="./Documentation/pDPM-systems.png" >
</p>

## Tutorial

Coming soon.

## Contact

[Shin-Yeh Tsai](https://www.cs.purdue.edu/homes/tsai46/),
[Yizhou Shan](http://lastweek.io),
[Yiying Zhang](https://cseweb.ucsd.edu/~yiying/).

Code was developed by Shin-Yeh Tsai at 2018-2019 during his time at Purdue University.

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
