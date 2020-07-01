#include "mitsume.h"

static pthread_barrier_t local_barrier;
unsigned int local_barrier_count = 0;
unsigned int local_barrier_init_flag = 0;
unsigned int local_barrier_num_global_client = 0;
int local_barrier_client_id;

#define _ASSIGN(dst, src, ...) asm("" : "=r"(dst) : "0"(src), ##__VA_ARGS__)

int stick_this_thread_to_core(int core_id) {
  int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
  if (core_id < 0 || core_id >= num_cores)
    return EINVAL;

  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);

  pthread_t current_thread = pthread_self();
  return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}

void schedule(void) { usleep(10); }
uint32_t hash_min(uint64_t a, unsigned int bits) {
  uint64_t b, c, d;
  /*
   * Encourage GCC to move a dynamic shift to %sar early,
   * thereby freeing up an additional temporary register.
   */
  bits = 64 - bits;

  _ASSIGN(b, a * 5);
  c = a << 13;
  b = (b << 2) + a;
  _ASSIGN(d, a << 17);
  a = b + (a << 1);
  c += d;
  d = a << 10;
  _ASSIGN(a, a << 19);
  d = a - d;
  _ASSIGN(a, a << 4, "X"(d));
  c += b;
  a += b;
  d -= c;
  c += a << 1;
  a += c << 3;
  _ASSIGN(b, b << (7 + 31), "X"(c), "X"(d));
  a <<= 31;
  b += d;
  a += b;
  return a >> bits;
}

void *mitsume_malloc(unsigned long size) {
  return malloc(size);
  // void *temp = numa_alloc_onnode(size, 0);
  // return temp;
}

int get_memory_id(struct configuration_params *input_arg) {
  return input_arg->machine_id -
         (input_arg->num_servers + input_arg->num_clients);
}

int get_client_id(struct configuration_params *input_arg) {
  return input_arg->machine_id - (input_arg->num_servers);
}

int get_client_id(struct mitsume_ctx_clt *client_ctx) {
  return client_ctx->client_id;
}

int get_controller_id(struct configuration_params *input_arg) {
  return input_arg->machine_id;
}

uint64_t mitsume_local_thread_get_wr_id(struct thread_local_inf *local_inf) {
  uint64_t wr_id = local_inf->queue_wr_id.front();
  assert(!local_inf->queue_wr_id.empty());
  // MITSUME_PRINT("get wr_id %llu %d\n", (unsigned long long int)wr_id,
  // (int)local_inf->queue_wr_id.size());
  local_inf->queue_wr_id.pop();
  userspace_wait_wr_table_value(wr_id);
  return wr_id;
}

uint64_t
mitsume_local_thread_get_wr_id_lock(struct thread_local_inf *local_inf) {
  local_inf->queue_wr_id_lock.lock();
  uint64_t wr_id = local_inf->queue_wr_id.front();
  assert(!local_inf->queue_wr_id.empty());
  // MITSUME_PRINT("get wr_id %llu %d\n", (unsigned long long int)wr_id,
  // (int)local_inf->queue_wr_id.size());
  local_inf->queue_wr_id.pop();
  local_inf->queue_wr_id_lock.unlock();

  userspace_wait_wr_table_value(wr_id);
  return wr_id;
}

void mitsume_local_thread_put_wr_id(struct thread_local_inf *local_inf,
                                    uint64_t wr_id) {
  userspace_null_wr_table_value(wr_id);
  local_inf->queue_wr_id.push(wr_id);
  // MITSUME_PRINT("put wr_id %llu %d\n", (unsigned long long int)wr_id,
  // (int)local_inf->queue_wr_id.size());
}

void mitsume_local_thread_put_wr_id_lock(struct thread_local_inf *local_inf,
                                         uint64_t wr_id) {
  userspace_null_wr_table_value(wr_id);
  local_inf->queue_wr_id_lock.lock();
  local_inf->queue_wr_id.push(wr_id);
  local_inf->queue_wr_id_lock.unlock();
  // MITSUME_PRINT("put wr_id %llu %d\n", (unsigned long long int)wr_id,
  // (int)local_inf->queue_wr_id.size());
}

void yield_to_another_coro(struct thread_local_inf *local_inf, int coro_id,
                           coro_yield_t &yield) {
  int next;

  local_inf->coro_queue.push(coro_id);
  next = local_inf->coro_queue.front();
  assert(next < MITSUME_CLT_COROUTINE_NUMBER);
  assert(coro_id < MITSUME_CLT_COROUTINE_NUMBER);
  local_inf->coro_queue.pop();
  // MITSUME_PRINT("yield %d\n", coro_id);
  yield(local_inf->coro_arr[next]);
  // MITSUME_PRINT("gain %d\n", coro_id);
}

unsigned int accumulate_wr_id_base = 0;

struct thread_local_inf *mitsume_local_thread_setup(struct ib_inf *inf,
                                                    int local_thread_id) {
  uint64_t wr_id, wr_id_base;
  int per_coroutine;
  struct thread_local_inf *ret_local_inf = new struct thread_local_inf;
  struct ibv_mr *meshptr_mr;
  struct ibv_mr *crc_mr;
  struct ibv_mr *chase_empty_mr;
  struct ibv_mr *chasing_shortcut_mr;
  struct ibv_mr *write_checking_mr;
  int i = 0;
  UNUSED(wr_id_base);
  UNUSED(i);

  assert(ret_local_inf != 0);
  assert(P15_MAX_BUFF <= P15_THREAD_SEND_BUF_NUM);
  ret_local_inf->thread_id = local_thread_id;

  wr_id_base = (accumulate_wr_id_base)*MITSUME_CLT_MAX_WRID_BATCH + 1000;
  accumulate_wr_id_base++;

  // for(wr_id=wr_id_base;wr_id<wr_id_base+MITSUME_CLT_MAX_WRID_BATCH;wr_id++)
  while (i < MITSUME_CLT_MAX_WRID_BATCH) {
    wr_id = 1024 * (i + 1) + accumulate_wr_id_base;
    i++;
    userspace_init_wr_table_value(wr_id);
    ret_local_inf->queue_wr_id.push(wr_id);
  }

  meshptr_mr =
      ibv_reg_mr(inf->pd, ret_local_inf->meshptr_list,
                 sizeof(struct mitsume_ptr) * MITSUME_CLT_COROUTINE_NUMBER *
                     MITSUME_MAX_REPLICATION * MITSUME_MAX_REPLICATION,
                 MITSUME_MR_PERMISSION);
  crc_mr = ibv_reg_mr(inf->pd, ret_local_inf->crc_base,
                      sizeof(uint64_t) * MITSUME_CLT_COROUTINE_NUMBER *
                          MITSUME_MAX_REPLICATION,
                      MITSUME_MR_PERMISSION);
  chase_empty_mr =
      ibv_reg_mr(inf->pd, ret_local_inf->chase_empty_list,
                 sizeof(struct mitsume_ptr) * MITSUME_CLT_COROUTINE_NUMBER *
                     MITSUME_MAX_REPLICATION,
                 MITSUME_MR_PERMISSION);

  chasing_shortcut_mr =
      ibv_reg_mr(inf->pd, ret_local_inf->chasing_shortcut,
                 sizeof(struct mitsume_shortcut) * MITSUME_CLT_COROUTINE_NUMBER,
                 MITSUME_MR_PERMISSION);

  write_checking_mr =
      ibv_reg_mr(inf->pd, ret_local_inf->write_checking_ptr,
                 sizeof(struct mitsume_ptr) * MITSUME_CLT_COROUTINE_NUMBER *
                     MITSUME_MAX_REPLICATION,
                 MITSUME_MR_PERMISSION);

  for (per_coroutine = 0; per_coroutine < MITSUME_CLT_COROUTINE_NUMBER;
       per_coroutine++) {
    ret_local_inf->input_space[per_coroutine] =
        (struct mitsume_msg *)mitsume_malloc(MITSUME_MAX_MESSAGE_SIZE);
    ret_local_inf->input_mr[per_coroutine] =
        ibv_reg_mr(inf->pd, ret_local_inf->input_space[per_coroutine],
                   MITSUME_MAX_MESSAGE_SIZE, MITSUME_MR_PERMISSION);

    ret_local_inf->output_space[per_coroutine] =
        (struct mitsume_msg *)mitsume_malloc(MITSUME_MAX_MESSAGE_SIZE);
    ret_local_inf->output_mr[per_coroutine] =
        ibv_reg_mr(inf->pd, ret_local_inf->output_space[per_coroutine],
                   MITSUME_MAX_MESSAGE_SIZE, MITSUME_MR_PERMISSION);

    ret_local_inf->user_input_space[per_coroutine] =
        mitsume_malloc(MITSUME_CLT_USER_FILE_MAX_SIZE);
    ret_local_inf->user_input_mr[per_coroutine] =
        ibv_reg_mr(inf->pd, ret_local_inf->user_input_space[per_coroutine],
                   MITSUME_CLT_USER_FILE_MAX_SIZE, MITSUME_MR_PERMISSION);

    ret_local_inf->user_output_space[per_coroutine] =
        mitsume_malloc(MITSUME_CLT_USER_FILE_MAX_SIZE);
    ret_local_inf->user_output_mr[per_coroutine] =
        ibv_reg_mr(inf->pd, ret_local_inf->user_output_space[per_coroutine],
                   MITSUME_CLT_USER_FILE_MAX_SIZE, MITSUME_MR_PERMISSION);

    ret_local_inf->meshptr_mr[per_coroutine].addr =
        &ret_local_inf->meshptr_list[per_coroutine];
    ret_local_inf->meshptr_mr[per_coroutine].rkey = meshptr_mr->rkey;
    ret_local_inf->meshptr_mr[per_coroutine].lkey = meshptr_mr->lkey;

    ret_local_inf->chase_empty_mr[per_coroutine].addr =
        &ret_local_inf->chase_empty_list[per_coroutine];
    ret_local_inf->chase_empty_mr[per_coroutine].rkey = chase_empty_mr->rkey;
    ret_local_inf->chase_empty_mr[per_coroutine].lkey = chase_empty_mr->lkey;

    ret_local_inf->crc_mr[per_coroutine].addr =
        &ret_local_inf->crc_base[per_coroutine];
    ret_local_inf->crc_mr[per_coroutine].rkey = crc_mr->rkey;
    ret_local_inf->crc_mr[per_coroutine].lkey = crc_mr->lkey;

    ret_local_inf->chasing_shortcut_mr[per_coroutine].addr =
        &ret_local_inf->chasing_shortcut[per_coroutine];
    ret_local_inf->chasing_shortcut_mr[per_coroutine].rkey =
        chasing_shortcut_mr->rkey;
    ret_local_inf->chasing_shortcut_mr[per_coroutine].lkey =
        chasing_shortcut_mr->lkey;

    ret_local_inf->write_checking_ptr_mr[per_coroutine].addr =
        &ret_local_inf->write_checking_ptr[per_coroutine];
    ret_local_inf->write_checking_ptr_mr[per_coroutine].rkey =
        write_checking_mr->rkey;
    ret_local_inf->write_checking_ptr_mr[per_coroutine].lkey =
        write_checking_mr->lkey;
  }

  /*if(local_thread_id == 0)
  {
      MITSUME_PRINT("%llx %llx\n", (unsigned long long
  int)chasing_shortcut_mr->addr, (unsigned long long
  int)chasing_shortcut_mr->lkey); MITSUME_PRINT("%llx %llx\n", (unsigned long
  long int)chase_empty_mr->addr, (unsigned long long int)chase_empty_mr->lkey);
      MITSUME_PRINT("%llx %llx\n", (unsigned long long
  int)&ret_local_inf->chase_empty_list[0][0], (unsigned long long
  int)ret_local_inf->chase_empty_mr[0].lkey); MITSUME_PRINT("%llx %llx\n",
  (unsigned long long int)ret_local_inf->user_input_space[0], (unsigned long
  long int)ret_local_inf->user_input_mr[0]->lkey);
  }*/
  ret_local_inf->empty_base =
      mitsume_malloc(MITSUME_CON_ALLOCATOR_SLAB_SIZE_GRANULARITY
                     << MITSUME_CON_ALLOCATOR_SLAB_NUMBER);
  ret_local_inf->empty_mr =
      ibv_reg_mr(inf->pd, ret_local_inf->empty_base,
                 MITSUME_CON_ALLOCATOR_SLAB_SIZE_GRANULARITY
                     << MITSUME_CON_ALLOCATOR_SLAB_NUMBER,
                 MITSUME_MR_PERMISSION);

  ret_local_inf->ori_meshptr_mr = meshptr_mr;
  ret_local_inf->ori_chase_empty_mr = chase_empty_mr;
  ret_local_inf->ori_crc_mr = crc_mr;
  ret_local_inf->ori_chasing_shortcut_mr = chasing_shortcut_mr;
  ret_local_inf->ori_write_checking_ptr_mr = write_checking_mr;
  return ret_local_inf;
}

void mitsume_sync_barrier_global(void) {
  int ret;
  char memcached_string[MEMCACHED_MAX_NAME_LEN];
  unsigned int i;
  void *ret_int;
  assert(local_barrier_init_flag == 1);
  ret = pthread_barrier_wait(&local_barrier);
  if (ret == PTHREAD_BARRIER_SERIAL_THREAD) {
    sprintf(memcached_string, MITSUME_MEMCACHED_BARRIER_STRING,
            local_barrier_count, local_barrier_client_id);
    memcached_publish(memcached_string, &ret, sizeof(int));
    for (i = 0; i < local_barrier_num_global_client; i++) {
      sprintf(memcached_string, MITSUME_MEMCACHED_BARRIER_STRING,
              local_barrier_count, i);
      ret_int = memcached_get_published_size(memcached_string, sizeof(int));
      // MITSUME_PRINT("%s\n", memcached_string);
      free(ret_int);
    }
    local_barrier_count++;
  }
  pthread_barrier_wait(&local_barrier);
  return;
}

void mitsume_sync_barrier_local(void) {
  assert(local_barrier_init_flag == 1);
  pthread_barrier_wait(&local_barrier);
  return;
}

int mitsume_sync_barrier_init(int num_send_thread, int local_client_id,
                              int num_global_client) {
  int ret;
  if (local_barrier_init_flag != 0)
    die_printf("barrier is already initialized\n");
  assert(num_global_client > 0);
  local_barrier_init_flag = 1;
  local_barrier_count = 0;
  local_barrier_client_id = local_client_id;

  local_barrier_num_global_client = num_global_client;
  ret = pthread_barrier_init(&local_barrier, NULL, num_send_thread);
  MITSUME_INFO("client %d: initialize barrier with %d send thread\n",
               local_client_id, num_send_thread);
  return ret;
}

chrono::microseconds get_current_us(void) {
  return chrono::duration_cast<chrono::microseconds>(
      chrono::system_clock::now().time_since_epoch());
}

chrono::nanoseconds get_current_ns(void) {
  return chrono::duration_cast<chrono::nanoseconds>(
      chrono::system_clock::now().time_since_epoch());
}

chrono::milliseconds get_current_ms(void) {
  return chrono::duration_cast<chrono::milliseconds>(
      chrono::system_clock::now().time_since_epoch());
}
// int get_wr_id()
