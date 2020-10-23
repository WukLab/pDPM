
#ifndef MITSUME_BENCHMARK
#define MITSUME_BENCHMARK
#include "mitsume.h"
#include "mitsume_tool.h"

#include <atomic>
#include <mutex>
#include <queue>
#include <unordered_map>
#include <vector>
using namespace std;

int mitsume_benchmark(struct mitsume_ctx_clt *local_ctx_clt);
const static char MITSUME_YCSB_WORKLOAD_A_STRING[] =
    "workload/ycsb/workloada_%d";
const static char MITSUME_YCSB_WORKLOAD_B_STRING[] =
    "workload/ycsb/workloadb_%d";
const static char MITSUME_YCSB_WORKLOAD_C_STRING[] =
    "workload/ycsb/workloadc_%d";

#define MITSUME_BENCHMARK_WORKLOAD_NAME_LEN 256
#define MITSUME_YCSB_SIZE 1000000
#define MITSUME_YCSB_MODE_A 50
#define MITSUME_YCSB_MODE_B 5
#define MITSUME_YCSB_MODE_C 0
#define MITSUME_YCSB_OP_MODE MITSUME_YCSB_MODE_B
#define MITSUME_YCSB_KEY_RANGE 100000
#define MITSUME_YCSB_TEST_TIME 1000000
#define MITSUME_YCSB_VERIFY_LEVEL 0

#define MITSUME_YCSB_COROUTINE 6
#define MITSUME_MASTER_COROUTINE 0

#define MITSUME_TEST_LOAD_WRITE_NUM 4
#define MITSUME_TEST_LOAD_READ_NUM 2

#define MITSUME_BENCHMARK_THREAD_NUM            8
#define MITSUME_BENCHMARK_REPLICATION           1
#define MITSUME_BENCHMARK_SIZE                  1000
#define MITSUME_BENCHMARK_TIME                  100000
#define MITSUME_BENCHMARK_RUN_TIME              10

#endif
