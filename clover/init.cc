
#include "client.h"
#include "memory.h"
#include "mitsume.h"
#include "server.h"
#include <getopt.h>
#include <stdio.h>

//#include "ponefive.h"
int main(int argc, char *argv[]) {
  /* All requests should fit into the master's request region */
  // assert(sizeof(struct mica_op) *	NUM_CLIENTS * NUM_WORKERS * WINDOW_SIZE <
  // RR_SIZE);

  /* Unsignaled completion checks. worker.c does its own check w/ @postlist */
  // assert(UNSIG_BATCH >= WINDOW_SIZE);	/* Pipelining check for clients
  // */ assert(HRD_Q_DEPTH >= 2 * UNSIG_BATCH);	/* Queue capacity check */
  int i, c;
  int is_master = -1;
  int num_threads = 1;
  int is_client = -1, machine_id = -1, is_server = -1, is_memory = -1;
  int base_port_index = -1;
  // int num_clients=-1, num_servers=-1, num_memorys=-1;
  int num_clients = MITSUME_CLT_NUM, num_servers = MITSUME_CON_NUM,
      num_memorys = MITSUME_MEM_NUM;
  int device_id = 0;
  int num_loopback = -1;
  int interaction_mode = 0;
  struct configuration_params *param_arr;
  pthread_t *thread_arr;

  static struct option opts[] = {
      {"master", 1, NULL, 'h'},          {"base-port-index", 1, NULL, 'b'},
      {"num-clients", 1, NULL, 'c'},     {"num-servers", 1, NULL, 's'},
      {"num-memorys", 1, NULL, 'm'},     {"is-client", 1, NULL, 'C'},
      {"is-server", 1, NULL, 'S'},       {"is-memory", 1, NULL, 'M'},
      {"machine-id", 1, NULL, 'I'},      {"device-id", 1, NULL, 'd'},
      {"num-loopbackset", 1, NULL, 'L'}, {NULL, 0, NULL, 0}};

  /* Parse and check arguments */
  while (1) {
    c = getopt_long(argc, argv, "h:b:c:m:s:C:S:I:d:L:M:", opts, NULL);
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
      break;
    case 's':
      num_servers = atoi(optarg);
      break;
    case 'm':
      num_memorys = atoi(optarg);
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
    default:
      printf("Invalid argument %d\n", c);
      assert(0);
    }
  }
  MITSUME_PRINT("size of %d %d\n", (int)sizeof(struct mitsume_msg),
                (int)sizeof(struct mitsume_large_msg));
  /* Common checks for all (master, workers, clients */
  assert(base_port_index >= 0 && base_port_index <= 8);
  if (interaction_mode)
    MITSUME_PRINT("[INTERACTION MODE]\n");

  /* Handle the master process specially */
  /*if(is_master == 1) {
          struct thread_params master_params;
          master_params.num_server_ports = num_server_ports;
          master_params.base_port_index = base_port_index;

          pthread_t master_thread;
          pthread_create(&master_thread,
                  NULL, run_master, (void *) &master_params);
          pthread_join(master_thread, NULL);
          exit(0);
  }*/

  /* Common sanity checks for worker process and per-machine client process */
  assert((is_client + is_server + is_memory) == -1);
  assert((num_loopback) >= 0);

  if (is_client == 1) {
    assert(num_clients >= 1);
    assert(num_servers >= 1);
    assert(num_memorys >= 1);

    assert(num_threads >= 1);
    assert(machine_id >= 0);
  } else if (is_server == 1) {
    // assert(num_threads == -1);	/* Number of server threads is fixed */
    // num_threads = NUM_WORKERS;	/* Needed to allocate thread structs later
    // */
    assert(num_clients >= 1);
    assert(num_servers >= 1);
    assert(num_memorys >= 1);
    assert(machine_id >= 0);
  } else // memory
  {
    assert(num_clients >= 1);
    assert(num_servers >= 1);
    assert(num_memorys >= 1);
    assert(num_clients == MITSUME_CLT_NUM);
    assert(num_servers == MITSUME_CON_NUM);
    assert(num_memorys == MITSUME_MEM_NUM);
    assert(machine_id >= 0);
  }
  param_arr = (struct configuration_params *)malloc(
      num_threads * sizeof(struct configuration_params));
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
