#include "client.h"
#include "memory.h"
#include "mitsume.h"
#include "server.h"
#include <getopt.h>
#include <stdio.h>

int MITSUME_CLT_NUM;
int MITSUME_MEM_NUM;

int main(int argc, char *argv[]) {
  int i, c;
  int is_master = -1;
  int num_threads = 1;
  int is_client = -1, machine_id = -1, is_server = -1, is_memory = -1;
  int base_port_index = -1;
  int num_clients, num_servers = MITSUME_CON_NUM, num_memorys;
  int device_id = 0;
  int num_loopback = -1;
  struct configuration_params *param_arr;
  pthread_t *thread_arr;

  static struct option opts[] = {
      {"master", 1, NULL, 'h'},          {"base-port-index", 1, NULL, 'b'},
      {"num-clients", 1, NULL, 'c'},     {"num-servers", 1, NULL, 's'},
      {"num-memorys", 1, NULL, 'm'},     {"is-client", 1, NULL, 'C'},
      {"is-server", 1, NULL, 'S'},       {"is-memory", 1, NULL, 'M'},
      {"machine-id", 1, NULL, 'I'},      {"device-id", 1, NULL, 'd'},
      {"num-loopbackset", 1, NULL, 'L'}, {"memcached-server-ip", 1, NULL, 'X'},
      {NULL, 0, NULL, 0}};

  /* Parse and check arguments */
  while (1) {
    c = getopt_long(argc, argv, "h:b:c:m:s:C:S:I:d:L:M:X:", opts, NULL);
    if (c == -1) {
      break;
    }
    switch (c) {
    case 'h':
      is_master = atoi(optarg);
      assert(is_master == 1);
      break;
    case 'b':
      base_port_index = atoi(optarg);
      break;
    case 'c':
      num_clients = atoi(optarg);
      MITSUME_CLT_NUM = num_clients;
      break;
    case 's':
      num_servers = atoi(optarg);
      break;
    case 'm':
      num_memorys = atoi(optarg);
      MITSUME_MEM_NUM = num_memorys;
      break;
    case 'C':
      is_client = atoi(optarg);
      break;
    case 'S':
      is_server = atoi(optarg);
      break;
    case 'M':
      is_memory = atoi(optarg);
      break;
    case 'I':
      machine_id = atoi(optarg);
      break;
    case 'd':
      device_id = atoi(optarg);
      break;
    case 'L':
      num_loopback = atoi(optarg);
      break;
    case 'X':
      strncpy(MEMCACHED_IP, optarg, sizeof(MEMCACHED_IP));
      printf("%s:%s: memcached-server-ip = %s\n", __FILE__, __func__, MEMCACHED_IP);
      break;
    default:
      printf("Invalid argument %d\n", c);
      assert(0);
    }
  }

  /* Common checks for all (master, workers, clients */
  assert(base_port_index >= 0 && base_port_index <= 8);

  /* Common sanity checks for worker process and per-machine client process */
  assert((is_client + is_server + is_memory) == -1);
  assert((num_loopback) >= 0);

  printf("%s:%s: This is running as [%s]!\n",
         __FILE__, __func__, (is_client == 1) ? "CN" : (is_memory == 1) ? "MN" : "MS");
  printf("%s:%s: num_clients=%d, num_memorys=%d\n",
         __FILE__, __func__, num_clients, num_memorys);

  if (num_clients < 1 || num_servers < 1 || num_memorys < 1) {
    printf("%s:%s: Invalid num_clients=%d, num_memorys=%d num_servers=%d\n",
           __FILE__, __func__, num_clients, num_memorys, num_servers);
    exit(0);
  }

  if (machine_id < 0) {
    printf("Invalid machine_id %d\n", machine_id);
    exit(0);
  }

  if (is_client == 1) {
    assert(num_threads >= 1);
  }

  param_arr = (struct configuration_params *)malloc(num_threads * sizeof(struct configuration_params));
  thread_arr = (pthread_t *)malloc(num_threads * sizeof(pthread_t));
  assert(thread_arr);

  {
    param_arr[0].base_port_index = base_port_index;
    param_arr[0].num_servers = num_servers;
    param_arr[0].num_clients = num_clients;
    param_arr[0].num_memorys = num_memorys;
    param_arr[0].machine_id = machine_id;
    param_arr[0].total_threads = num_threads;
    param_arr[0].device_id = device_id;
    param_arr[0].num_loopback = num_loopback;

    if (is_client >= 0)
      run_client(&param_arr[0]);
    if (is_server >= 0)
      run_server(&param_arr[0]);
    if (is_memory >= 0)
      run_memory(&param_arr[0]);
  }

  while (1)
    ;

  for (i = 0; i < num_threads; i++) {
    pthread_join(thread_arr[i], NULL);
  }
  return 0;
}
