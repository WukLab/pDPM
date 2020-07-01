#include "mitsume_clt_test.h"

/*mutex
MITSUME_CON_NAMER_HASHTABLE_LOCK[1<<MITSUME_CON_NAMER_HASHTABLE_SIZE_BIT];
std::unordered_map<uint64_t, struct mitsume_hash_struct*>
MITSUME_CON_NAMER_HASHTABLE;

mutex
MITSUME_CON_GC_HASHTABLE_CURRENT_LOCK[1<<MITSUME_CON_NAMER_HASHTABLE_SIZE_BIT];
std::unordered_map<uint64_t, struct mitsume_gc_hashed_entry*>
MITSUME_CON_GC_HASHTABLE_CURRENT;

mutex
MITSUME_CON_GC_HASHTABLE_BUFFER_LOCK[1<<MITSUME_CON_NAMER_HASHTABLE_SIZE_BIT];
std::unordered_map<uint64_t, struct mitsume_gc_hashed_entry*>
MITSUME_CON_GC_HASHTABLE_BUFFER;*/

int mitsume_test_thread(int thread_num, struct mitsume_ctx_clt *local_ctx_clt,
                        void *(*fun_ptr)(void *input_metadata)) {
  int i = 0;
  mitsume_sync_barrier_init(thread_num, get_client_id(local_ctx_clt),
                            MITSUME_CLT_NUM);
  pthread_t thread_job[MITSUME_CLT_CONSUMER_NUMBER];
  // liteapi_dist_barrier(MITSUME_TEST_NODE_NUM);

  if (thread_num > MITSUME_CLT_CONSUMER_NUMBER) {
    die_printf("thread_num is larger than max clt number\n");
    exit(1);
  }
  sleep(1);
  for (i = 0; i < thread_num; i++) {
    pthread_create(&thread_job[i], NULL, fun_ptr,
                   &local_ctx_clt->thread_metadata[i]);
  }
  sleep(1);
  for (i = 0; i < thread_num; i++) {
    pthread_join(thread_job[i], NULL);
  }
  MITSUME_PRINT("all %d threads are finished\n", thread_num);
  /*
  mitsume_test_start = 1;
  while(atomic_read(&mitsume_test_finish)<thread_num)
  {
      msleep(500);
      schedule();
  }

  for(i=0;i<MITSUME_NUM_REPLICATION_BUCKET;i++)
  {
      MITSUME_INFO("read bucket-%d %ld\n", i,
  atomic64_read(&local_ctx_clt->read_bucket_counter[i]));
  }
  for(i=0;i<MITSUME_NUM_REPLICATION_BUCKET;i++)
  {
      MITSUME_INFO("write bucket-%d %ld\n", i,
  atomic64_read(&local_ctx_clt->write_bucket_counter[i]));
  }*/
  return MITSUME_SUCCESS;
}

/**
 * mitsume_test_open: generate a single thread to test maxium open entries
 */
void *mitsume_test_open(void *input_metadata) {
  struct mitsume_consumer_metadata *thread_metadata =
      (struct mitsume_consumer_metadata *)input_metadata;
  void *write;
  int i;
  int ret;
  // int key_range = MITSUME_TEST_RAND_KEY_RANGE;
  int key_range = 50;
  int test_size = 96 - sizeof(struct mitsume_ptr);
  int client_id = get_client_id(thread_metadata->local_ctx_clt);
  struct thread_local_inf *local_inf = thread_metadata->local_inf;
  // struct ib_inf *ib_ctx = thread_metadata->local_ctx_clt->ib_ctx;
  mitsume_key key;

  write = local_inf->user_input_space[0];

  if (thread_metadata->thread_id == 0) {
    MITSUME_PRINT("------\n");
    if (client_id == 0) {
      for (i = 1; i <= key_range; i++) {
        /*int recv;
        int ret_bucket;
        int target_list = mitsume_con_alloc_size_to_list_num(test_size);
        recv = mitsume_clt_consumer_ask_entries_from_controller(thread_metadata,
        target_list, 16, 0, &ret_bucket); MITSUME_PRINT("%d-%d\n", i, recv);
        UNUSED(key);
        UNUSED(write);
        UNUSED(ret);*/
        key = i + key_range * thread_metadata->thread_id;
        memset(write, 0x31 + i, test_size);
        ret = mitsume_tool_open(thread_metadata, key, write, test_size,
                                MITSUME_TEST_ONE);
        if (ret)
          MITSUME_INFO("fail to open %llu\n", (unsigned long long int)key);
        MITSUME_PRINT("%d-%s\n", i, (char *)write);
      }
      ret = mitsume_tool_open(thread_metadata, key, write, test_size,
                              MITSUME_TEST_ONE);
      if (ret)
        MITSUME_INFO("fail to open %llu\n", (unsigned long long int)key);
      /*while(!thread_metadata->consumer_node_branch[0][0].empty())
      {
          MITSUME_PRINT("%llx\n", (unsigned long long
      int)thread_metadata->consumer_node_branch[0][0].front());
          thread_metadata->consumer_node_branch[0][0].pop();
      }*/
    }
  }
  MITSUME_PRINT("---finish %d-%d---\n", client_id, thread_metadata->thread_id);
  return NULL;
}

/**
 * mitsume_test_open: generate a single thread to test maxium open entries
 */
void *mitsume_test_read(void *input_metadata) {
  struct mitsume_consumer_metadata *thread_metadata =
      (struct mitsume_consumer_metadata *)input_metadata;
  char *write;
  void *read;
  int i;
  int ret;
  // int key_range = MITSUME_TEST_RAND_KEY_RANGE;
  int key_range = 50;
  uint32_t read_size;
  int test_size = 96 - sizeof(struct mitsume_ptr);
  int client_id = get_client_id(thread_metadata->local_ctx_clt);
  struct thread_local_inf *local_inf = thread_metadata->local_inf;
  // struct ib_inf *ib_ctx = thread_metadata->local_ctx_clt->ib_ctx;
  mitsume_key key;

  write = (char *)local_inf->user_input_space[0];
  read = local_inf->user_output_space[0];

  if (thread_metadata->thread_id == 0) {
    MITSUME_PRINT("------\n");
    if (client_id == 0) {
      MITSUME_PRINT("%llx:%llx:%llx\n",
                    (unsigned long long int)&thread_metadata->local_inf
                        ->chasing_shortcut[0],
                    (unsigned long long int)thread_metadata->local_inf
                        ->chasing_shortcut_mr[0]
                        .addr,
                    (unsigned long long int)thread_metadata->local_inf
                        ->chasing_shortcut_mr[0]
                        .lkey);
      for (i = 1; i <= key_range; i++) {
        /*int recv;
        int ret_bucket;
        int target_list = mitsume_con_alloc_size_to_list_num(test_size);
        recv = mitsume_clt_consumer_ask_entries_from_controller(thread_metadata,
        target_list, 16, 0, &ret_bucket); MITSUME_PRINT("%d-%d\n", i, recv);
        UNUSED(key);
        UNUSED(write);
        UNUSED(ret);*/
        key = i + key_range * thread_metadata->thread_id;
        memset(write, 0x31 + i, test_size);
        ret = mitsume_tool_open(thread_metadata, key, (void *)write, test_size,
                                MITSUME_TEST_ONE);
        if (ret)
          MITSUME_INFO("fail to open %llu\n", (unsigned long long int)key);
        MITSUME_PRINT("%d-%s\n", i, write);
        memset(read, 0, test_size);
        mitsume_tool_read(thread_metadata, key, read, &read_size,
                          MITSUME_TOOL_KVSTORE_READ);
        MITSUME_PRINT("%d:%c %u %.*s\n", i, write[0], read_size, read_size,
                      (char *)read);
      }
    }
  }
  MITSUME_PRINT("---finish %d-%d---\n", client_id, thread_metadata->thread_id);
  return NULL;
}

/**
 * mitsume_test_correctness: test the correctness of basic keyvalue read write
 * operation
 */
void *mitsume_test_correctness(void *input_metadata) {
  struct mitsume_consumer_metadata *thread_metadata =
      (struct mitsume_consumer_metadata *)input_metadata;
  char *write;
  void *read;
  int i;
  int ret;
  int test_times = 100;
  uint32_t read_size;
  struct thread_local_inf *local_inf = thread_metadata->local_inf;
  int client_id = get_client_id(thread_metadata->local_ctx_clt);
  UNUSED(client_id);
  UNUSED(read_size);
  mitsume_key key;

  write = (char *)local_inf->user_input_space[0];
  read = local_inf->user_output_space[0];

  mitsume_sync_barrier_global();
  key = 35;
  memset(read, 0, 4096);
  memset(write, 0, 4096);
  memset(write, 0x41, 120);
  if (thread_metadata->thread_id == 0) {
    if (client_id == 0) {
      MITSUME_PRINT("------\n");
      // ret = mitsume_tool_open(thread_metadata, key, write, 36,
      // MITSUME_TEST_ONE);
      ret = mitsume_tool_open(thread_metadata, key, write, 120,
                              MITSUME_TEST_THREE);
      if (ret)
        MITSUME_INFO("fail to open %llu\n", (unsigned long long int)key);
      else
        MITSUME_INFO("open %llu success\n", (unsigned long long int)key);

      MITSUME_PRINT("======\n");

      for (i = 0; i < test_times; i++) {
        memset(write, 0x21 + (i % 95), 120);
        mitsume_tool_write(thread_metadata, key, write, 104,
                           MITSUME_TOOL_KVSTORE_WRITE);
        // mitsume_tool_write(thread_metadata, key, write, 36,
        // MITSUME_TOOL_KVSTORE_WRITE); mitsume_tool_write(thread_metadata, key,
        // write, 36, MITSUME_TOOL_KVSTORE_WRITE); MITSUME_PRINT("after
        // write\n"); memset(read, 0, 120);
        mitsume_tool_read(thread_metadata, key, read, &read_size,
                          MITSUME_TOOL_KVSTORE_READ);
        MITSUME_PRINT("%d:%c %u %.*s\n", i, write[0], read_size, read_size,
                      (char *)read);
      }
    }
  }
  return NULL;
}

uint64_t *mitsume_test_read_file(const char *input_string) {
  uint64_t *ret = new uint64_t[MITSUME_TEST_RAND_LOG_RANGE];
  FILE *fp = fopen(input_string, "r");
  ssize_t read;
  char *line = NULL;
  int i = 0;
  size_t len = 0;
  while ((read = getline(&line, &len, fp)) != -1) {
    sscanf(line, "%llu\n", (unsigned long long int *)&ret[i]);
    i++;
    if (i == MITSUME_TEST_RAND_LOG_RANGE)
      break;
  }
  return ret;
}

void *mitsume_test_distribution(void *input_metadata) {
  struct mitsume_consumer_metadata *thread_metadata =
      (struct mitsume_consumer_metadata *)input_metadata;
  char *write;
  void *read;
  int i;
  int ret;
  int test_times = 1000000;
  int key_range = MITSUME_TEST_RAND_KEY_RANGE;
  uint32_t read_size;
  struct thread_local_inf *local_inf = thread_metadata->local_inf;
  int client_id = get_client_id(thread_metadata->local_ctx_clt);
  char read_string[MITSUME_TEST_WORKLOAD_NAME_LEN];
  char write_string[MITSUME_TEST_WORKLOAD_NAME_LEN];
  int target_write_set, target_read_set;
  mitsume_key key;
  int test_size =
      MITSUME_TEST_SIZE - sizeof(struct mitsume_ptr) * MITSUME_TEST_THREE;
  mitsume_key *write_key = NULL;
  mitsume_key *read_key = NULL;

  UNUSED(test_times);
  UNUSED(read_size);
  UNUSED(read_string);
  UNUSED(write_string);
  UNUSED(test_size);
  UNUSED(target_write_set);
  UNUSED(target_read_set);

  write = (char *)local_inf->user_input_space[0];
  read = local_inf->user_output_space[0];
  target_write_set =
      thread_metadata->thread_id + (client_id)*MITSUME_CLT_TEST_THREAD_NUM;
  target_read_set =
      thread_metadata->thread_id + (client_id)*MITSUME_CLT_TEST_THREAD_NUM;

  mitsume_sync_barrier_global();
  memset(read, 0, 4096);
  memset(write, 0, 4096);
  memset(write, 0x41, 120);
  if (MITSUME_TEST_MODE == MITSUME_TEST_MODE_UNIFORM) {
    sprintf(write_string, MITSUME_TEST_WORKLOAD_UNIFORM_STRING,
            target_write_set);
    sprintf(read_string, MITSUME_TEST_WORKLOAD_UNIFORM_STRING, target_read_set);
  }
  // else if(MITSUME_TEST_MODE == MITSUME_TEST_MODE_FIX)
  // else if(MITSUME_TEST_MODE == MITSUME_TEST_MODE_DFIX)
  else {
    MITSUME_PRINT_ERROR("error test mode %d\n", MITSUME_TEST_MODE);
    return (void *)MITSUME_ERROR;
  }

  read_key = mitsume_test_read_file(read_string);
  write_key = mitsume_test_read_file(write_string);

  if (!read_key || !write_key) {
    MITSUME_INFO("check here %d %d", thread_metadata->thread_id, client_id);
  }
  if (thread_metadata->thread_id == 0) {
    if (client_id == 0) {
      MITSUME_PRINT("------\n");
      for (i = 1; i <= key_range; i++) {
        key = i;
        ret = mitsume_tool_open(thread_metadata, key, write, 36,
                                MITSUME_TEST_THREE);
        if (ret)
          MITSUME_INFO("fail to open %llu\n", (unsigned long long int)key);
        // else
        //    MITSUME_INFO("open %llu success\n", (unsigned long long int) key);
      }
      MITSUME_PRINT("======\n");
    }
  }
  mitsume_sync_barrier_global();
  if (thread_metadata->thread_id == 0)
    MITSUME_PRINT("finish open\n");
  mitsume_sync_barrier_global();

  MITSUME_PRINT("======\n");
  MITSUME_PRINT("test size: %d\n", test_size);

  for (i = 0; i < test_times; i++) {
    key = (uint64_t)write_key[i % 10000];
    ret = mitsume_tool_write(thread_metadata, key, write, test_size,
                             MITSUME_TOOL_KVSTORE_WRITE);
    if (ret != MITSUME_SUCCESS)
      MITSUME_INFO("error %lld %d\n", (unsigned long long int)key, ret);
    if (i > 0 && i % 200000 == 0)
      MITSUME_PRINT("%d-%d write\n", thread_metadata->thread_id, i);
  }
  MITSUME_PRINT("%d-finish write\n", thread_metadata->thread_id);
  for (i = 0; i < test_times; i++) {
    key = (uint64_t)read_key[i % 10000];
    mitsume_tool_read(thread_metadata, key, read, &read_size,
                      MITSUME_TOOL_KVSTORE_READ);
  }
  MITSUME_PRINT("%d-finish read\n", thread_metadata->thread_id);
  return NULL;
}

/**
 * mitsume_test_distribution: generate a bunch of thread to test concurrent
 * kvstore read/write (most frequently used test)
 */
/*void *mitsume_test_distribution(void *input_metadata)
{
    struct mitsume_consumer_metadata *thread_metadata = (struct
mitsume_consumer_metadata *)input_metadata; char *write; void *read; int i; int
ret; int test_times = 100; int key_range = MITSUME_TEST_RAND_KEY_RANGE; int
test_size = MITSUME_TEST_SIZE - sizeof(struct mitsume_ptr) * MITSUME_TEST_ONE -
8; int client_id = get_client_id(thread_metadata->local_ctx_clt); struct
thread_local_inf *local_inf = thread_metadata->local_inf; int target_write_set,
target_read_set; char read_string[MITSUME_TEST_WORKLOAD_NAME_LEN]; char
write_string[MITSUME_TEST_WORKLOAD_NAME_LEN];
    //ktime_t test_timer;
    mitsume_key key;
    mitsume_key *write_key=NULL;
    mitsume_key *read_key=NULL;
    uint32_t read_size;

    write = (char *)local_inf->user_input_space[0];
    read = local_inf->user_output_space[0];
    memset(write, 0x31, 8192);


    //target_write_set = thread_metadata->thread_id + (client_id);
    target_write_set = thread_metadata->thread_id +
(client_id)*MITSUME_CLT_TEST_THREAD_NUM; target_read_set =
thread_metadata->thread_id + (client_id)*MITSUME_CLT_TEST_THREAD_NUM;

    if(MITSUME_TEST_MODE == MITSUME_TEST_MODE_UNIFORM)
    {
        sprintf(write_string, MITSUME_TEST_WORKLOAD_UNIFORM_STRING,
target_write_set); sprintf(read_string, MITSUME_TEST_WORKLOAD_UNIFORM_STRING,
target_read_set);
    }
    //else if(MITSUME_TEST_MODE == MITSUME_TEST_MODE_FIX)
    //else if(MITSUME_TEST_MODE == MITSUME_TEST_MODE_DFIX)
    else
    {
        MITSUME_PRINT_ERROR("error test mode %d\n", MITSUME_TEST_MODE);
        return (void *)MITSUME_ERROR;
    }

    read_key = mitsume_test_read_file(read_string);
    write_key = mitsume_test_read_file(write_string);

    if(!read_key || !write_key)
    {
        MITSUME_INFO("check here %d %d", thread_metadata->thread_id, client_id);
    }
    sleep(MITSUME_TEST_SLEEP_TIMEOUT);
    key = 20000;
    ret = mitsume_tool_open(thread_metadata, key, write, test_size,
MITSUME_TEST_ONE); mitsume_sync_barrier_global();

    if(thread_metadata->thread_id == 0)
    {
        MITSUME_PRINT("------\n");
        if(client_id == 0)
        {
            for(i=1;i<=key_range;i++)
            {
                key = (uint64_t)i;
                ret = mitsume_tool_open(thread_metadata, key, write, test_size,
MITSUME_TEST_ONE); if(ret) MITSUME_INFO("fail to open %llu\n", (unsigned long
long int)key); else MITSUME_PRINT("open %d\n", i);
            }
        }
        MITSUME_PRINT("------\n");
    }
    mitsume_sync_barrier_global();

    MITSUME_PRINT("======\n");
    MITSUME_PRINT("test size: %d\n", test_size);

    for(i=0;i<test_times;i++)
    {
        key = (uint64_t)write_key[i%10000];
        ret = mitsume_tool_write(thread_metadata, key, write, test_size,
MITSUME_TOOL_KVSTORE_WRITE); if(ret!=MITSUME_SUCCESS) MITSUME_INFO("error %lld
%d\n", (unsigned long long int)key, ret);
    }
    MITSUME_PRINT("%d-finish write\n", thread_metadata->thread_id);
    for(i=0;i<test_times;i++)
    {
        key = (uint64_t)read_key[i%10000];
        mitsume_tool_read(thread_metadata, key, read, &read_size,
MITSUME_TOOL_KVSTORE_READ);
    }
    MITSUME_PRINT("%d-finish read\n", thread_metadata->thread_id);
    return NULL;
}*/

int mitsume_clt_test(struct mitsume_ctx_clt *local_ctx_clt) {
  // mitsume_test_thread(MITSUME_CLT_TEST_THREAD_NUM, local_ctx_clt,
  // &mitsume_test_open); mitsume_test_thread(MITSUME_CLT_TEST_THREAD_NUM,
  // local_ctx_clt, &mitsume_test_read);
  // mitsume_test_thread(MITSUME_CLT_TEST_THREAD_NUM, local_ctx_clt,
  // &mitsume_test_correctness); mitsume_test_thread(MITSUME_CLT_TEST_THREAD_NUM,
  // local_ctx_clt, &mitsume_test_distribution);

  mitsume_benchmark(local_ctx_clt);
  return MITSUME_SUCCESS;
}
