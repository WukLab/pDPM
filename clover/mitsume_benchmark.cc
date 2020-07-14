#include "mitsume_benchmark.h"

#define MITSUME_TARGET_KEY 10

atomic<long> total_op;
mutex output_lock;

volatile int start_flag = 0;
volatile int end_flag = 0;
volatile int ready_flag = 0;

#define MITSUME_TEST_LOAD_READ_R1 10
#define MITSUME_TEST_LOAD_DUMMY 50
#define MITSUME_TEST_LOAD_READ_R2 11
#define MITSUME_TEST_LOAD_WRITE_START 14

void *mitsume_benchmark_latency(void *input_metadata) {
  struct mitsume_consumer_metadata *thread_metadata =
      (struct mitsume_consumer_metadata *)input_metadata;
  void *write;
  void *read;
  uint32_t i, j;
  int ret;
  uint32_t read_size;
  int client_id = get_client_id(thread_metadata->local_ctx_clt);
  struct thread_local_inf *local_inf = thread_metadata->local_inf;
  vector<int> test_size = {128, 256, 512, 1024, 2048, 4096};

  chrono::nanoseconds before, after;

  mitsume_key key = MITSUME_TARGET_KEY;

  write = local_inf->user_input_space[0];
  read = local_inf->user_output_space[0];
  if (client_id != 0)
    return NULL;
  if (thread_metadata->thread_id != 0)
    return NULL;

  if (stick_this_thread_to_core(2)) {
    printf("set affinity fail\n");
    return NULL;
  }

  MITSUME_PRINT("------\n");
  ret = mitsume_tool_open(thread_metadata, key, write, 36,
                          MITSUME_BENCHMARK_REPLICATION);
  for (j = 0; j < test_size.size(); j++) {
    before = get_current_ns();
    for (i = 0; i < MITSUME_BENCHMARK_TIME; i++) {
      ret = mitsume_tool_write(thread_metadata, key, write,
                               test_size[j] - 8 * MITSUME_BENCHMARK_REPLICATION,
                               MITSUME_TOOL_KVSTORE_WRITE);
      if (ret != MITSUME_SUCCESS)
        MITSUME_INFO("error %lld %d\n", (unsigned long long int)key, ret);
    }
    after = get_current_ns();
    cout << test_size[j] << ": average write time(us):"
         << (after - before).count() / MITSUME_BENCHMARK_TIME << endl;

    before = get_current_ns();
    for (i = 0; i < MITSUME_BENCHMARK_TIME; i++) {
      ret = mitsume_tool_read(thread_metadata, key, read, &read_size,
                              MITSUME_TOOL_KVSTORE_READ);
      if (ret != MITSUME_SUCCESS)
        MITSUME_INFO("error %lld %d\n", (unsigned long long int)key, ret);
    }
    after = get_current_ns();
    cout << test_size[j] << ": average read time(us):"
         << (after - before).count() / MITSUME_BENCHMARK_TIME << endl;
  }

  return 0;
}

int mitsume_benchmark_thread(int thread_num,
                             struct mitsume_ctx_clt *local_ctx_clt,
                             void *(*fun_ptr)(void *input_metadata)) {
  int i = 0;
  total_op = 0;
  mitsume_sync_barrier_init(thread_num, get_client_id(local_ctx_clt),
                            MITSUME_CLT_NUM);
  pthread_t thread_job[MITSUME_CLT_CONSUMER_NUMBER];
  chrono::milliseconds before, after;
  assert(MITSUME_BENCHMARK_SIZE);
  assert(MITSUME_BENCHMARK_TIME);

  if (thread_num > MITSUME_CLT_CONSUMER_NUMBER) {
    die_printf("thread_num is larger than max clt number\n");
    exit(1);
  }
  sleep(1);
  for (i = 0; i < thread_num; i++) {
    pthread_create(&thread_job[i], NULL, fun_ptr,
                   &local_ctx_clt->thread_metadata[i]);
  }
  while (ready_flag == 0)
    ;
  start_flag = 1;
  cout << "start waiting" << endl;
  before = get_current_ms();
  sleep(MITSUME_BENCHMARK_RUN_TIME);
  cout << "after waiting" << endl;
  end_flag = 1;
  after = get_current_ms();
  for (i = 0; i < thread_num; i++) {
    pthread_join(thread_job[i], NULL);
  }
  // MITSUME_PRINT("all %d threads are finished\n", thread_num);
  cout << total_op.load() << endl;
  cout << fixed << "throughput "
       << ((float)total_op.load() / (after - before).count()) * 1000
       << " /seconds" << endl;
  // mitsume_stat_show();
  return MITSUME_SUCCESS;
}

int mitsume_benchmark(struct mitsume_ctx_clt *local_ctx_clt) {
  mitsume_benchmark_thread(1, local_ctx_clt, &mitsume_benchmark_latency);
  return 0;
}
