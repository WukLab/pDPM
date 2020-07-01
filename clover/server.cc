#include "server.h"
#include "memcached.h"
#include <sched.h>
#define DBG_STRING "server"
#include <sys/unistd.h>
#define P15_ROLE SERVER
#include "mitsume.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#define NUM_CORES 22

static struct configuration_params *param_arr;
static struct ib_inf *node_share_inf;
static pthread_t *thread_arr;
static pthread_barrier_t local_barrier;
// static pthread_barrier_t cycle_barrier;

int stick_thread_to_core(int core_id) {
  int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
  if (core_id < 0 || core_id >= num_cores)
    die_printf("%s: core_id %d invalid\n", __func__, core_id);
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);
  pthread_t current_thread = pthread_self();
  return pthread_setaffinity_np(current_thread, sizeof(pthread_t), &cpuset);
}

void *run_server(void *arg) {
  int i;
  struct configuration_params *input_arg = (struct configuration_params *)arg;
  int machine_id = input_arg->machine_id;
  int num_threads = input_arg->total_threads;
  int num_servers = input_arg->num_servers;
  int num_clients = input_arg->num_clients;
  int num_memorys = input_arg->num_memorys;
  int base_port_index = input_arg->base_port_index;
  int ret;
  param_arr = (struct configuration_params *)malloc(
      num_threads * sizeof(struct configuration_params));
  thread_arr = (pthread_t *)malloc(num_threads * sizeof(pthread_t));
  // initialize barrier
  ret = pthread_barrier_init(&local_barrier, NULL, input_arg->total_threads);
  if (ret)
    die_printf("[%s] fail to create barrier %d thread %d\n", __func__, ret,
               input_arg->total_threads);
  // initialize thread
  for (i = num_threads - 1; i >= 0; i--) {
    param_arr[i].global_thread_id = (machine_id << P15_ID_SHIFT) + i;
    param_arr[i].local_thread_id = i;
    param_arr[i].base_port_index = base_port_index;
    param_arr[i].num_servers = num_servers;
    param_arr[i].num_clients = num_clients;
    param_arr[i].num_memorys = num_memorys;
    param_arr[i].machine_id = machine_id;
    param_arr[i].total_threads = num_threads;
    param_arr[i].device_id = input_arg->device_id;
    param_arr[i].num_loopback = input_arg->num_loopback;
    if (i != 0)
      pthread_create(&thread_arr[i], NULL, main_server, &param_arr[i]);
    else
      main_server(&param_arr[0]);
  }
  return NULL;
}

struct mitsume_ctx_con *server_init(struct configuration_params *input_arg) {
  struct mitsume_ctx_con *server_ctx = new struct mitsume_ctx_con;
  server_ctx->controller_id =
      get_controller_id(input_arg); // should be get controller_id
  server_ctx->machine_id = input_arg->machine_id;

  server_ctx->input_arg = input_arg;
  server_ctx->ib_ctx = node_share_inf;
  return server_ctx;
}

int server_get_shortcut(struct mitsume_ctx_con *server_ctx) {
  ptr_attr *shortcut_attr = new ptr_attr[MITSUME_SHORTCUT_NUM];
  ptr_attr *tmp_attr;
  int memory_id;
  int start_allocation, end_allocation;
  char memcached_string[MEMCACHED_MAX_NAME_LEN];
  uint32_t target_shortcut_entry_space;
  int current_index = 0;
  // target_shortcut_entry_space = MITSUME_ROUND_UP(MITSUME_SHORTCUT_NUM,
  // MITSUME_MEM_NUM);
  target_shortcut_entry_space = MITSUME_SHORTCUT_NUM / MITSUME_MEM_NUM;
  for (memory_id = 0; memory_id < MITSUME_MEM_NUM; memory_id++) {
    start_allocation = memory_id * target_shortcut_entry_space;
    end_allocation = (memory_id + 1) * target_shortcut_entry_space - 1;
    memset(memcached_string, 0, MEMCACHED_MAX_NAME_LEN);
    sprintf(memcached_string, MITSUME_MEMCACHED_SHORTCUT_STRING, memory_id);
    tmp_attr = (ptr_attr *)memcached_get_published_size(
        memcached_string,
        sizeof(ptr_attr) * (end_allocation - start_allocation + 1));
    MITSUME_PRINT("%d %llu\n", current_index,
                  (unsigned long long int)end_allocation - start_allocation +
                      1);
    memcpy(&shortcut_attr[current_index], tmp_attr,
           sizeof(ptr_attr) * (end_allocation - start_allocation + 1));
    current_index += end_allocation - start_allocation + 1;
    free(tmp_attr);
  }
  /*for(per_allocation=0;per_allocation<MITSUME_SHORTCUT_NUM;per_allocation++)
  {
      memset(memcached_string, 0, MEMCACHED_MAX_NAME_LEN);
      sprintf(memcached_string, MITSUME_MEMCACHED_SHORTCUT_STRING,
  per_allocation); tmp_attr = memcached_get_published_mr(memcached_string);
      memcpy(&shortcut_attr[per_allocation], tmp_attr, sizeof(ptr_attr));
      free(tmp_attr);
      if(per_allocation==0||per_allocation==1023)
          MITSUME_PRINT("%llx, %ld\n", (unsigned long
  long)shortcut_attr[per_allocation].addr,
  (long)shortcut_attr[per_allocation].rkey);
  }*/
  server_ctx->all_shortcut_attr = shortcut_attr;
  return MITSUME_SUCCESS;
}

int server_get_memory(struct mitsume_ctx_con *server_ctx) {
  ptr_attr *memory_attr = new ptr_attr[MITSUME_MEM_NUM];
  ptr_attr *tmp_attr;
  int per_mem;
  char memcached_string[MEMCACHED_MAX_NAME_LEN];
  for (per_mem = 0; per_mem < MITSUME_MEM_NUM; per_mem++) {
    memset(memcached_string, 0, MEMCACHED_MAX_NAME_LEN);
    sprintf(memcached_string, MITSUME_MEMCACHED_MEMORY_ALLOCATION_STRING,
            per_mem);
    tmp_attr = memcached_get_published_mr(memcached_string);
    memcpy(&memory_attr[per_mem], tmp_attr, sizeof(ptr_attr));
    free(tmp_attr);
    MITSUME_PRINT("%d-(%llx, %ld)\n", per_mem,
                  (unsigned long long)memory_attr[per_mem].addr,
                  (long)memory_attr[per_mem].rkey);
  }
  server_ctx->all_memory_attr = memory_attr;
  return MITSUME_SUCCESS;
}

int server_setup_post_recv(struct configuration_params *input_arg,
                           struct mitsume_ctx_con *context) {
  void *alloc_space;
  ptr_attr *tmp_attr_ptr;
  int per_msg;
  int per_qp;
  uint32_t alloc_size = MITSUME_MAX_MESSAGE_SIZE;
  context->per_qp_mr_attr_list =
      new ptr_attr *[node_share_inf->num_local_rcqps];
  context->per_post_recv_mr_list =
      new struct ibv_mr *[node_share_inf->num_local_rcqps];

  // register a memory space for each qp

  for (per_qp = 0; per_qp < node_share_inf->num_local_rcqps; per_qp++) {
    context->per_qp_mr_attr_list[per_qp] =
        new ptr_attr[MITSUME_CON_MESSAGE_PER_POST];
    alloc_space = mitsume_malloc(alloc_size * MITSUME_CON_MESSAGE_PER_POST);
    context->per_post_recv_mr_list[per_qp] = ibv_reg_mr(
        node_share_inf->pd, alloc_space,
        alloc_size * MITSUME_CON_MESSAGE_PER_POST, MITSUME_MR_PERMISSION);
    tmp_attr_ptr = context->per_qp_mr_attr_list[per_qp];
    for (per_msg = 0; per_msg < MITSUME_CON_MESSAGE_PER_POST; per_msg++) {
      tmp_attr_ptr[per_msg].addr =
          (uint64_t)context->per_post_recv_mr_list[per_qp]->addr +
          (uint64_t)alloc_size * per_msg;
      tmp_attr_ptr[per_msg].rkey = context->per_post_recv_mr_list[per_qp]->rkey;
    }
  }

  // post all memory space into qp
  for (per_qp = 0; per_qp < node_share_inf->num_local_rcqps; per_qp++) {
    ib_post_recv_inf *input_inf =
        new ib_post_recv_inf[MITSUME_CON_MESSAGE_PER_POST];
    for (per_msg = 0; per_msg < MITSUME_CON_MESSAGE_PER_POST; per_msg++) {
      input_inf[per_msg].qp_index = per_qp;
      input_inf[per_msg].length = alloc_size;
      input_inf[per_msg].mr_index = per_msg;
    }
    ib_post_recv_connect_qp(node_share_inf, input_inf,
                            context->per_qp_mr_attr_list[per_qp],
                            MITSUME_CON_MESSAGE_PER_POST);
    free(input_inf);
  }

  return MITSUME_SUCCESS;
}

struct mitsume_ctx_con *server_ctx;

void *main_server(void *arg) {
  // int machine_id, thread_id;

  struct configuration_params *input_arg = (struct configuration_params *)arg;
  // int total_machines;
  // total_machines = input_arg->num_servers+input_arg->num_clients;
  // machine_id = input_arg->machine_id;
  // thread_id = input_arg->local_thread_id;
  // dbg_printf("This is %s %d %d: total %d\n\n", DBG_STRING, machine_id,
  // thread_id, total_machines);

  if (input_arg->local_thread_id == 0) {
    node_share_inf = ib_complete_setup(input_arg, P15_ROLE, DBG_STRING);
    server_ctx = NULL;
  }
  while (node_share_inf == NULL)
    ;
  printf("finish all server initialization\n");

  if (input_arg->local_thread_id == 0) {
    server_ctx = server_init(input_arg);
    CPE(server_get_shortcut(server_ctx), "fail to get correct shortcut\n", 0);
    CPE(server_get_memory(server_ctx), "fail to get correct memory mappin\n",
        0);
    CPE(server_setup_post_recv(input_arg, server_ctx),
        "fail to setup post_recv\n", 0);

    mitsume_stat_init(MITSUME_IS_CONTROLLER);
    // start handling allocation
    mitsume_con_alloc_share_init();
    mitsume_con_alloc_split_space_into_lh(server_ctx); // cut space into lh
    MITSUME_PRINT("finish first split\n");
    if (get_controller_id(input_arg) == MITSUME_MASTER_CON)
      mitsume_con_alloc_populate_lh(server_ctx); // polulate lh into memcache
    else
      mitsume_con_alloc_get_lh(server_ctx, NULL);
    MITSUME_PRINT("finish populate\n");
    mitsume_con_alloc_entry_init(input_arg, server_ctx);

    mitsume_con_thread_metadata_setup(input_arg, server_ctx);
    mitsume_con_thread_init(server_ctx);
  }

  /*
  if(input_arg->machine_id == 0)
      server_code(node_share_inf, node_private_inf, input_arg);
  else
      helper_code(node_share_inf, node_private_inf, input_arg);*/
  printf("ready to press ctrl+c to finish experiment\n");
  while (1)
    ;
}
