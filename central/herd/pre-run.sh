sudo echo 12288 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages
sudo sysctl -w kernel.shmmax=2147483648
sudo sysctl -w kernel.shmall=2147483648
sudo sysctl -p /etc/sysctl.conf

