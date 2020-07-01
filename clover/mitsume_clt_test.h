#ifndef MITSUME_CLT_TEST
#define MITSUME_CLT_TEST
#include "mitsume.h"

#include "mitsume_clt_tool.h"
#include "mitsume_tool.h"
#include "mutex"
#include "unordered_map"
#include <unistd.h>

#include "mitsume_benchmark.h"

using namespace std;
int mitsume_clt_test(struct mitsume_ctx_clt *client_ctx);
int mitsume_test_thread(int thread_num, struct mitsume_ctx_clt *local_ctx_clt,
                        void *(*fun_ptr)(void *input_metadata));
void *mitsume_test_open(void *input_metadata);

#define MITSUME_CLT_TEST_THREAD_NUM 4
#define MITSUME_TEST_SIZE 4000

#define MITSUME_TEST_MODE_UNIFORM 1
#define MITSUME_TEST_MODE_FIX 2
#define MITSUME_TEST_MODE_DFIX 3
#define MITSUME_TEST_MODE MITSUME_TEST_MODE_UNIFORM

#define MITSUME_TEST_RAND_KEY_RANGE 10000
#define MITSUME_TEST_RAND_LOG_RANGE 10000
#define MITSUME_TEST_RAND_SET_NUM 32

#define MITSUME_TEST_SLEEP_TIMEOUT 1

#define MITSUME_TEST_WORKLOAD_NAME_LEN 256
const static char MITSUME_TEST_WORKLOAD_UNIFORM_STRING[] =
    "workload/uniform_%d";

#endif
