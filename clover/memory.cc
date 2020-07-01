#include "memory.h"
#include "memcached.h"
const static char *DBG_STRING = "memory";
#define P15_ROLE MEMORY

static struct configuration_params *param_arr;
static pthread_t *thread_arr;
static pthread_barrier_t local_barrier;
static struct ib_inf *node_share_inf;

void *run_memory(void *arg) {
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
      pthread_create(&thread_arr[i], NULL, main_memory, &param_arr[i]);
    else
      main_memory(&param_arr[0]);
  }
  return NULL;
}

int memory_allocate_shortcut(struct configuration_params *input_arg) {
  int memory_id = get_memory_id(input_arg);
  char memcached_string[MEMCACHED_MAX_NAME_LEN];
  void *shortcut_space;
  struct ibv_mr *shortcut_mr;
  // volatile void **data_space;
  uint32_t target_shortcut_entry_space;
  int per_allocation, start_allocation, end_allocation;
  // target_shortcut_entry_space = MITSUME_ROUND_UP(MITSUME_SHORTCUT_NUM,
  // MITSUME_MEM_NUM);
  target_shortcut_entry_space = MITSUME_SHORTCUT_NUM / MITSUME_MEM_NUM;
  ptr_attr *output_shortcut_attr;

  start_allocation = memory_id * target_shortcut_entry_space;
  end_allocation = (memory_id + 1) * target_shortcut_entry_space - 1;
  output_shortcut_attr = new ptr_attr[end_allocation - start_allocation + 1];
  shortcut_space =
      mitsume_malloc(target_shortcut_entry_space * sizeof(mitsume_shortcut));
  assert(shortcut_space);
  MITSUME_PRINT("allocate %lluB (%d-%d) for %d shortcut(%d)\n",
                (long long unsigned int)target_shortcut_entry_space *
                    sizeof(mitsume_shortcut),
                start_allocation, end_allocation,
                (int)target_shortcut_entry_space,
                (int)sizeof(mitsume_shortcut));
  shortcut_mr =
      ibv_reg_mr(node_share_inf->pd, shortcut_space,
                 target_shortcut_entry_space * sizeof(mitsume_shortcut),
                 MITSUME_MR_PERMISSION);
  assert(shortcut_mr);
  for (per_allocation = start_allocation; per_allocation <= end_allocation;
       per_allocation++) {
    ptr_attr shortcut_attr = {
      addr : (uint64_t)shortcut_space +
          ((per_allocation - start_allocation) * sizeof(mitsume_shortcut)),
      rkey : shortcut_mr->rkey,
      machine_id : (short)input_arg->machine_id
    };
    memcpy(&output_shortcut_attr[per_allocation - start_allocation],
           &shortcut_attr, sizeof(ptr_attr));
  }
  memset(memcached_string, 0, MEMCACHED_MAX_NAME_LEN);
  sprintf(memcached_string, MITSUME_MEMCACHED_SHORTCUT_STRING, memory_id);
  memcached_publish(memcached_string, output_shortcut_attr,
                    sizeof(ptr_attr) * (end_allocation - start_allocation + 1));
  /*for(per_allocation =
  start_allocation;per_allocation<=end_allocation;per_allocation++)
  {
      ptr_attr shortcut_attr = {
          addr: (uint64_t)shortcut_space +
  ((per_allocation-start_allocation)*sizeof(mitsume_shortcut)), rkey:
  shortcut_mr->rkey, machine_id: (short)input_arg->machine_id
      };
      memset(memcached_string, 0, MEMCACHED_MAX_NAME_LEN);
      sprintf(memcached_string, MITSUME_MEMCACHED_SHORTCUT_STRING,
  per_allocation); memcached_publish(memcached_string, &shortcut_attr,
  sizeof(ptr_attr));
  }*/
  return MITSUME_SUCCESS;
}

int memory_allocate_memoryspace(struct configuration_params *input_arg) {
  int memory_id = get_memory_id(input_arg);
  char memcached_string[MEMCACHED_MAX_NAME_LEN];
  void *allocate_space;
  struct ibv_mr *allocate_mr;

  allocate_space = mitsume_malloc(
      (unsigned long long)MITSUME_MEMORY_PER_ALLOCATION_KB * 1024);
  assert(allocate_space);
  MITSUME_PRINT("allocate %lluB memory\n",
                (unsigned long long)MITSUME_MEMORY_PER_ALLOCATION_KB * 1024);
  allocate_mr =
      ibv_reg_mr(node_share_inf->pd, allocate_space,
                 (long long int)MITSUME_MEMORY_PER_ALLOCATION_KB * 1024,
                 MITSUME_MR_PERMISSION);
  assert(allocate_mr);
  ptr_attr allocate_attr = {
    addr : (uint64_t)allocate_space,
    rkey : allocate_mr->rkey,
    machine_id : (short)input_arg->machine_id
  };
  memset(memcached_string, 0, MEMCACHED_MAX_NAME_LEN);
  sprintf(memcached_string, MITSUME_MEMCACHED_MEMORY_ALLOCATION_STRING,
          memory_id);
  memcached_publish(memcached_string, &allocate_attr, sizeof(ptr_attr));
  MITSUME_PRINT("%llx, %ld\n", (unsigned long long)allocate_attr.addr,
                (long)allocate_attr.rkey);
  return MITSUME_SUCCESS;
}

void *main_memory(void *arg) {
  // int machine_id, thread_id;
  struct configuration_params *input_arg = (struct configuration_params *)arg;
  int memory_id = get_memory_id(input_arg);
  node_share_inf = ib_complete_setup(input_arg, P15_ROLE, DBG_STRING);
  assert(node_share_inf != NULL);
  printf("%d-finish all memory setup\n", memory_id);

  memory_allocate_shortcut(input_arg);
  memory_allocate_memoryspace(input_arg);

  printf("ready to press ctrl+c to finish experiment\n");
  while (1)
    ;
}
