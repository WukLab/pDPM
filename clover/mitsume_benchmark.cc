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

void mitsume_test_read_ycsb(const char *input_string, int **op_key,
                            uint64_t **target_key) {
  uint64_t *key = new uint64_t[MITSUME_YCSB_SIZE];
  int *op = new int[MITSUME_YCSB_SIZE];
  FILE *fp = fopen(input_string, "r");
  ssize_t read;
  char *line = NULL;
  int i = 0;
  size_t len = 0;
  while ((read = getline(&line, &len, fp)) != -1) {
    sscanf(line, "%d %llu\n", &op[i], (unsigned long long int *)&key[i]);
    i++;
    if (i == MITSUME_YCSB_SIZE)
      break;
  }
  *target_key = key;
  *op_key = op;
}

void mitsume_benchmark_slave_func(coro_yield_t &yield,
                                  tuple<struct mitsume_consumer_metadata *, int,
                                        int *, mitsume_key *, int *, long *>
                                      id_tuple) {
  struct mitsume_consumer_metadata *thread_metadata = get<0>(id_tuple);
  int coro_id = get<1>(id_tuple);
  int *op_key = get<2>(id_tuple);
  int test_size = MITSUME_BENCHMARK_SIZE;
  mitsume_key *target_key = get<3>(id_tuple);
  int *next_index = get<4>(id_tuple);
  long *local_op = get<5>(id_tuple);
  int i;
  int ret;

  uint32_t read_size;
  char *read;

  char *write;

  mitsume_key key;
  struct thread_local_inf *local_inf = thread_metadata->local_inf;
  read = (char *)local_inf->user_output_space[coro_id];
  write = (char *)local_inf->user_input_space[coro_id];
  while (!end_flag) {
    i = *next_index;
    key = (uint64_t)target_key[i];
    *next_index = *next_index + 1;
    // MITSUME_PRINT("process %d\n", coro_id);
    if (MITSUME_YCSB_VERIFY_LEVEL)
      memset(write, 0x31 + (key % 30), MITSUME_BENCHMARK_SIZE);
    if (op_key[i] == 0) {
      ret = mitsume_tool_read(thread_metadata, key, read, &read_size,
                              MITSUME_TOOL_KVSTORE_READ, coro_id, yield);
      if (MITSUME_YCSB_VERIFY_LEVEL) {
        if (read[0] != write[0])
          MITSUME_PRINT("doesn't match %c %c\n", read[0], write[0]);
      }
    } else {
      // ret = mitsume_tool_write(thread_metadata, key, write, test_size,
      // MITSUME_TOOL_KVSTORE_WRITE);
      ret = mitsume_tool_write(thread_metadata, key, write, test_size,
                               MITSUME_TOOL_KVSTORE_WRITE, coro_id, yield);
    }
    // MITSUME_PRINT("after process %d\n", coro_id);
    if (ret != MITSUME_SUCCESS)
      MITSUME_INFO("error %lld %d\n", (unsigned long long int)key, ret);
    if (*next_index == MITSUME_YCSB_SIZE)
      *next_index = 0;
    *local_op = *local_op + 1;
  }
}

void mitsume_benchmark_master_func(
    coro_yield_t &yield, tuple<struct mitsume_consumer_metadata *, int, int *,
                               mitsume_key *, int *, long *>
                             id_tuple) {
  struct mitsume_consumer_metadata *thread_metadata = get<0>(id_tuple);
  int coro_id = get<1>(id_tuple);
  long *local_op = get<5>(id_tuple);
  UNUSED(local_op);
  while (!end_flag) {
    yield_to_another_coro(thread_metadata->local_inf, coro_id, yield);
    // MITSUME_PRINT("%ld \n", *local_op);
  }
  MITSUME_PRINT("master ready to flush all queue\n");
  while (!thread_metadata->local_inf->coro_queue.empty()) {
    yield_to_another_coro(thread_metadata->local_inf, coro_id, yield);
  }
  MITSUME_PRINT("finish all request\n");
}

void *mitsume_benchmark_coroutine(void *input_metadata) {
  using std::placeholders::_1;
  struct mitsume_consumer_metadata *thread_metadata =
      (struct mitsume_consumer_metadata *)input_metadata;
  struct thread_local_inf *local_inf = thread_metadata->local_inf;
  int client_id = get_client_id(thread_metadata->local_ctx_clt);
  char ycsb_string[MITSUME_BENCHMARK_WORKLOAD_NAME_LEN];
  int target_set;
  int *op_key = NULL;
  long local_op = 0;
  int coro_i;
  int share_index;
  char *write;
  int i = 0;
  int ret = 0;
  int key_range = MITSUME_YCSB_KEY_RANGE;
  mitsume_key key;

  write = (char *)local_inf->user_input_space[0];

  mitsume_key *target_key = NULL;
  target_set =
      thread_metadata->thread_id + (client_id)*MITSUME_BENCHMARK_THREAD_NUM;

  stick_this_thread_to_core(2 * thread_metadata->thread_id);

  mitsume_sync_barrier_global();
  switch (MITSUME_YCSB_OP_MODE) {
  case MITSUME_YCSB_MODE_A:
    sprintf(ycsb_string, MITSUME_YCSB_WORKLOAD_A_STRING, target_set);
    break;
  case MITSUME_YCSB_MODE_B:
    sprintf(ycsb_string, MITSUME_YCSB_WORKLOAD_B_STRING, target_set);
    break;
  case MITSUME_YCSB_MODE_C:
    sprintf(ycsb_string, MITSUME_YCSB_WORKLOAD_C_STRING, target_set);
    break;
  default:
    MITSUME_PRINT_ERROR("wrong mode %d\n", MITSUME_YCSB_OP_MODE);
    exit(1);
  }

  mitsume_test_read_ycsb(ycsb_string, &op_key, &target_key);
  MITSUME_INFO("%d read %d\n", thread_metadata->thread_id, target_set);

  if (!op_key || !target_key) {
    MITSUME_INFO("check here %d %d", thread_metadata->thread_id, client_id);
  }
  if (thread_metadata->thread_id == 0) {
    if (client_id == 0) {
      MITSUME_PRINT("------\n");
      for (i = 1; i <= key_range; i++) {
        key = i;
        memset(write, 0x31 + (key % 30), MITSUME_BENCHMARK_SIZE);
        ret = mitsume_tool_open(thread_metadata, key, write,
                                MITSUME_BENCHMARK_SIZE,
                                MITSUME_BENCHMARK_REPLICATION);
        if (ret)
          MITSUME_INFO("fail to open %llu\n", (unsigned long long int)key);
      }
      MITSUME_PRINT("======\n");
      MITSUME_PRINT("finish open\n");
    }
  }
  mitsume_sync_barrier_global();
  if (thread_metadata->thread_id == 0)
    MITSUME_PRINT("finish barrier\n");

  share_index = 0;
  for (coro_i = 0; coro_i < MITSUME_YCSB_COROUTINE; coro_i++) {
    if (coro_i == MITSUME_MASTER_COROUTINE) {
      local_inf->coro_arr[coro_i] =
          coro_call_t(bind(mitsume_benchmark_master_func, _1,
                           make_tuple(thread_metadata, coro_i, op_key,
                                      target_key, &share_index, &local_op)));
    } else {
      local_inf->coro_queue.push(coro_i);
      local_inf->coro_arr[coro_i] =
          coro_call_t(bind(mitsume_benchmark_slave_func, _1,
                           make_tuple(thread_metadata, coro_i, op_key,
                                      target_key, &share_index, &local_op)));
    }
  }

  mitsume_sync_barrier_global();
  if (thread_metadata->thread_id == 0)
    ready_flag = 1;
  while (start_flag == 0)
    ;

  MITSUME_PRINT("======\n");
  MITSUME_PRINT("test %d\n", thread_metadata->thread_id);

  local_inf->coro_arr[MITSUME_MASTER_COROUTINE]();

  output_lock.lock();
  cout << thread_metadata->thread_id << " finish ycsb: " << local_op << endl;
  output_lock.unlock();
  total_op += local_op;
  return NULL;
}

void *mitsume_benchmark_ycsb(void *input_metadata) {
  struct mitsume_consumer_metadata *thread_metadata =
      (struct mitsume_consumer_metadata *)input_metadata;
  char *write;
  char *read;
  int i = 0;
  int ret = 0;
  int key_range = MITSUME_YCSB_KEY_RANGE;
  uint32_t read_size;
  struct thread_local_inf *local_inf = thread_metadata->local_inf;
  int client_id = get_client_id(thread_metadata->local_ctx_clt);
  char ycsb_string[MITSUME_BENCHMARK_WORKLOAD_NAME_LEN];
  int target_set;
  mitsume_key key;
  int test_size = MITSUME_BENCHMARK_SIZE;
  int *op_key = NULL;
  long local_op = 0;
  chrono::nanoseconds before, after;
  mitsume_key *target_key = NULL;
  write = (char *)local_inf->user_input_space[0];
  read = (char *)local_inf->user_output_space[0];
  target_set =
      thread_metadata->thread_id + (client_id)*MITSUME_BENCHMARK_THREAD_NUM;

  stick_this_thread_to_core(2 * thread_metadata->thread_id);

  mitsume_sync_barrier_global();
  memset(read, 0, 4096);
  memset(write, 0, 4096);
  memset(write, 0x41, 120);
  switch (MITSUME_YCSB_OP_MODE) {
  case MITSUME_YCSB_MODE_A:
    sprintf(ycsb_string, MITSUME_YCSB_WORKLOAD_A_STRING, target_set);
    break;
  case MITSUME_YCSB_MODE_B:
    sprintf(ycsb_string, MITSUME_YCSB_WORKLOAD_B_STRING, target_set);
    break;
  case MITSUME_YCSB_MODE_C:
    sprintf(ycsb_string, MITSUME_YCSB_WORKLOAD_C_STRING, target_set);
    break;
  default:
    MITSUME_PRINT_ERROR("wrong mode %d\n", MITSUME_YCSB_OP_MODE);
    exit(1);
  }

  mitsume_test_read_ycsb(ycsb_string, &op_key, &target_key);
  MITSUME_INFO("%d read %d\n", thread_metadata->thread_id, target_set);

  if (!op_key || !target_key) {
    MITSUME_INFO("check here %d %d", thread_metadata->thread_id, client_id);
  }
  if (thread_metadata->thread_id == 0) {
    if (client_id == 0) {
      MITSUME_PRINT("------\n");
      for (i = 1; i <= key_range; i++) {
        key = i;
        memset(write, 0x31 + (key % 30), MITSUME_BENCHMARK_SIZE);
        ret = mitsume_tool_open(thread_metadata, key, write,
                                MITSUME_BENCHMARK_SIZE,
                                MITSUME_BENCHMARK_REPLICATION);
        if (ret)
          MITSUME_INFO("fail to open %llu\n", (unsigned long long int)key);
      }
      MITSUME_PRINT("======\n");
      MITSUME_PRINT("finish open\n");
    }
  }
  mitsume_sync_barrier_global();
  if (thread_metadata->thread_id == 0)
    MITSUME_PRINT("finish barrier\n");
  mitsume_sync_barrier_global();
  if (thread_metadata->thread_id == 0)
    ready_flag = 1;
  while (start_flag == 0)
    ;

  MITSUME_PRINT("======\n");
  MITSUME_PRINT("test size: %d\n", test_size);

  while (!end_flag) {
    key = (uint64_t)target_key[i];
    if (MITSUME_YCSB_VERIFY_LEVEL)
      memset(write, 0x31 + (key % 30), MITSUME_BENCHMARK_SIZE);
    if (op_key[i] == 0) {
      mitsume_tool_read(thread_metadata, key, read, &read_size,
                        MITSUME_TOOL_KVSTORE_READ);
      if (MITSUME_YCSB_VERIFY_LEVEL)
        if (read[0] != write[0])
          MITSUME_PRINT("doesn't match %c %c\n", read[0], write[0]);
    } else
      ret = mitsume_tool_write(thread_metadata, key, write, test_size,
                               MITSUME_TOOL_KVSTORE_WRITE);
    if (ret != MITSUME_SUCCESS)
      MITSUME_INFO("error %lld %d\n", (unsigned long long int)key, ret);
    // if(i>0&&i%200000==0)
    //    MITSUME_PRINT("%d-%d write\n", thread_metadata->thread_id, i);
    i++;
    if (i == MITSUME_YCSB_SIZE)
      i = 0;
    local_op++;
  }
  output_lock.lock();
  cout << thread_metadata->thread_id << " finish ycsb: " << local_op << endl;
  output_lock.unlock();
  total_op += local_op;
  return NULL;
}

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
    cout << test_size[j] << ": average write time(ns):"
         << (after - before).count() / MITSUME_BENCHMARK_TIME << endl;

    before = get_current_ns();
    for (i = 0; i < MITSUME_BENCHMARK_TIME; i++) {
      ret = mitsume_tool_read(thread_metadata, key, read, &read_size,
                              MITSUME_TOOL_KVSTORE_READ);
      if (ret != MITSUME_SUCCESS)
        MITSUME_INFO("error %lld %d\n", (unsigned long long int)key, ret);
    }
    after = get_current_ns();
    cout << test_size[j] << ": average read time(ns):"
         << (after - before).count() / MITSUME_BENCHMARK_TIME << endl;
  }

  return 0;
}

void *mitsume_benchmark_load(void *input_metadata) {
  struct mitsume_consumer_metadata *thread_metadata =
      (struct mitsume_consumer_metadata *)input_metadata;
  char *write;
  char *read;
  int i = 0;
  int ret = 0;
  uint32_t read_size;
  struct thread_local_inf *local_inf = thread_metadata->local_inf;
  int client_id = get_client_id(thread_metadata->local_ctx_clt);
  int thread_id = thread_metadata->thread_id;
  mitsume_key key;
  int test_size = MITSUME_BENCHMARK_SIZE;
  write = (char *)local_inf->user_input_space[0];
  read = (char *)local_inf->user_output_space[0];
  int read_thread_id, write_thread_id;
  long int local_op = 0;
  UNUSED(read_size);
  UNUSED(read_thread_id);
  UNUSED(write_thread_id);
  if (thread_id < MITSUME_TEST_LOAD_READ_NUM) {
    read_thread_id = thread_id;
    write_thread_id = -1;
  } else {
    read_thread_id = -1;
    write_thread_id = thread_id - MITSUME_TEST_LOAD_READ_NUM;
  }

  stick_this_thread_to_core(2 * thread_metadata->thread_id);

  mitsume_sync_barrier_global();
  memset(read, 0, 4096);
  memset(write, 0, 4096);
  memset(write, 0x41, 120);

  if (thread_metadata->thread_id == 0) {
    if (client_id == 0) {
      MITSUME_PRINT("------\n");
      mitsume_tool_open(thread_metadata, MITSUME_TEST_LOAD_DUMMY, write,
                        test_size, 1);
      mitsume_tool_open(thread_metadata, MITSUME_TEST_LOAD_DUMMY + 1, write,
                        test_size, 1);
      mitsume_tool_open(thread_metadata, MITSUME_TEST_LOAD_READ_R1, write,
                        test_size, 1);
      mitsume_tool_open(thread_metadata, MITSUME_TEST_LOAD_DUMMY + 2, write,
                        test_size, 1);
      mitsume_tool_open(thread_metadata, MITSUME_TEST_LOAD_DUMMY + 3, write,
                        test_size, 1);
      mitsume_tool_open(thread_metadata, MITSUME_TEST_LOAD_READ_R2, write,
                        test_size, 3);
      for (i = 0; i < MITSUME_TEST_LOAD_WRITE_NUM; i++) {
        key = MITSUME_TEST_LOAD_WRITE_START + i;
        ret = mitsume_tool_open(thread_metadata, key, write, test_size, 1);
        if (ret)
          MITSUME_INFO("fail to open %llu\n", (unsigned long long int)key);
      }
      MITSUME_PRINT("======\n");
      MITSUME_PRINT("finish open\n");
    }
  }
  mitsume_sync_barrier_global();
  if (thread_metadata->thread_id == 0)
    MITSUME_PRINT("finish barrier\n");
  mitsume_sync_barrier_global();
  if (thread_metadata->thread_id == 0)
    ready_flag = 1;
  while (start_flag == 0)
    ;

  MITSUME_PRINT("======\n");
  MITSUME_PRINT("test size: %d\n", test_size);

  while (!end_flag) {
    if (read_thread_id >= 0 && read_thread_id == 0) {
      mitsume_tool_read(thread_metadata, MITSUME_TEST_LOAD_READ_R1, read,
                        &read_size, MITSUME_TOOL_KVSTORE_READ);
    } else if (read_thread_id >= 0) {
      // mitsume_tool_read(thread_metadata, MITSUME_TEST_LOAD_READ_R1, read,
      // &read_size, MITSUME_TOOL_KVSTORE_READ);
      mitsume_tool_read(thread_metadata, MITSUME_TEST_LOAD_READ_R2, read,
                        &read_size, MITSUME_TOOL_KVSTORE_READ);
    } else {
      ret = mitsume_tool_write(thread_metadata,
                               MITSUME_TEST_LOAD_WRITE_START + write_thread_id,
                               write, test_size, MITSUME_TOOL_KVSTORE_WRITE);
      if (ret != MITSUME_SUCCESS)
        MITSUME_INFO("error %lld %d\n", (unsigned long long int)write_thread_id,
                     ret);
    }
    local_op++;
  }

  output_lock.lock();
  cout << thread_metadata->thread_id << " finish load: " << local_op << endl;
  output_lock.unlock();
  total_op += local_op;
  return NULL;
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
  // mitsume_benchmark_thread(1, local_ctx_clt, &mitsume_benchmark_latency);
  mitsume_benchmark_thread(MITSUME_BENCHMARK_THREAD_NUM, local_ctx_clt,
                           &mitsume_benchmark_ycsb);
  // mitsume_benchmark_thread(MITSUME_BENCHMARK_THREAD_NUM, local_ctx_clt,
  // &mitsume_benchmark_coroutine);
  // mitsume_benchmark_thread(MITSUME_TEST_LOAD_WRITE_NUM+MITSUME_TEST_LOAD_READ_NUM,
  // local_ctx_clt, &mitsume_benchmark_load);
  return 0;
}
