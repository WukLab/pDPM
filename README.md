# Passive Disaggregated Persistent Memory (pDPM, ATC'20)

[[USENIX ATC 2020 Paper]](https://cseweb.ucsd.edu/~yiying/pDPM-ATC20.pdf)
[[Slide]](./Documentation/atc20-paper177-presentation-slides.pdf)
[[Keynote]](./Documentation/ATC20-pDPM-07-2020-long.key)
[[Keynote-iCloud]](https://www.icloud.com/keynote/0Ox0HGeoa5L1pQ7txzyU_RkUA#ATC20-pDPM-iCloud-Public)

## pDPM

We explore an alternative approach of building disaggregated persistent memory (PM) by treating storage nodes as _passive_ parties that do not perform any data processing or data management tasks, a model we call _pDPM_.

pDPM lowers owning and energy cost, also avoids storage node being the processing scalability bottleneck. pDPM is an instance of _passive disaggregation_ approach and has largely been overlooked in the past. Our work does a thorough exploration of this area.

<p align="center">
<img src="./Documentation/disaggregation-research-spectrum.png" >
</p>

## pDPM Systems

Based on where to process and manage data, we further catagorize three types of pDPM systems: pDPM-Direct, pDPM-Cental, and Clover. All three pDPM systems run on userspace libibverbs, no special kernel modifications are needed. Both pDPM-Direct and pDPM-Central are developed based on [HERD](https://github.com/efficient/rdma_bench/tree/master/herd).

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
