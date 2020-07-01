# A function to echo in blue color
function blue() {
	es=`tput setaf 4`
	ee=`tput sgr0`
	echo "${es}$1${ee}"
}

#
# HACK!!!
# Change this to the memcached's machine IP
#
export HRD_REGISTRY_IP="10.0.0.120"

export MLX5_SINGLE_THREADED=1

blue "Removing SHM key 24 (request region hugepages)"
sudo ipcrm -M 24

blue "Removing SHM keys used by MICA"
for i in `seq 0 28`; do
	key=`expr 3185 + $i`
	sudo ipcrm -M $key 2>/dev/null
	key=`expr 4185 + $i`
	sudo ipcrm -M $key 2>/dev/null
done

: ${HRD_REGISTRY_IP:?"Need to set HRD_REGISTRY_IP non-empty"}

blue "Reset server QP registry"
sudo killall memcached
#memcached -u root -l 0.0.0.0 -m 2048 1>/dev/null 2>/dev/null &
#memcached -u root -l 0.0.0.0 -m 2048 1>./log_m1.log 2>./log_m2.log &
sleep 1

blue "Starting master process"
sudo LD_LIBRARY_PATH=/usr/local/lib/ -E \
	numactl --cpunodebind=0 --membind=0 ./main \
        --memory 3 \
	--master 1 \
	--base-port-index 0 \
	--num-server-ports 1 &

# Give the master process time to create and register per-port request regions
sleep 1

blue "Starting worker threads"
sudo LD_LIBRARY_PATH=/usr/local/lib/ -E \
	numactl --cpunodebind=0 --membind=0 ./main \
	--is-client 0 \
	--base-port-index 0 \
	--num-server-ports 1 \
	--postlist 1 &

#numactl --physcpubind=0,2,4,6,8,10,12,14 --membind=0 ./main \
