#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

#include "hrd.h"
#include "main.h"
#include "mica.h"

#define DGRAM_BUF_SIZE 4096

/* Generate a random permutation of [0, n - 1] for client @clt_gid */
int *get_random_permutation(int n, int clt_gid, uint64_t *seed) {
  int i, j, temp;
  assert(n > 0);

  /* Each client uses a different range in the cycle space of fastrand */
  for (i = 0; i < clt_gid * HERD_NUM_KEYS; i++) {
    hrd_fastrand(seed);
  }

  printf("client %d: creating a permutation of 0--%d. This takes time..\n",
         clt_gid, n - 1);

  int *log = (int *)malloc(n * sizeof(int));
  assert(log != NULL);
  for (i = 0; i < n; i++) {
    log[i] = i;
  }

  printf("\tclient %d: shuffling..\n", clt_gid);
  for (i = n - 1; i >= 1; i--) {
    j = hrd_fastrand(seed) % (i + 1);
    temp = log[i];
    log[i] = log[j];
    log[j] = temp;
  }
  printf("\tclient %d: done creating random permutation\n", clt_gid);

  return log;
}

/*
 * Added by YS:
 *
 * This is the number of transactions we are going to run.
 * This is the number of lines we are going to read from the workload file.
 */
#define test_times (1000000)

void get_file(int **op, int **key, int thread_id) {
  int *write_key;
  int *op_key;
  FILE *fp;
  char filepath[32];
  char *line = NULL;
  size_t len = 0;
  ssize_t read;
  int i;
  i = 0;

  sprintf(filepath, "workload_trace/ycsb/workloadc_%d", thread_id);
  printf("start reading %s\n", filepath);
  fp = fopen(filepath, "r");
  if (fp == NULL) {
    printf("%s: Fail to open %s\n", __func__, filepath);
    exit(0);
  }

  op_key = malloc(sizeof(int) * test_times);
  write_key = malloc(sizeof(int) * test_times);

  while ((read = getline(&line, &len, fp)) != -1) {
    sscanf(line, "%llu %llu\n", (long long unsigned int *)&op_key[i],
           (long long unsigned int *)&write_key[i]);
    i++;
    if (i == test_times)
      break;
  }
  printf("thread-%d finish reading %d\n", thread_id, i);
  *op = op_key;
  *key = write_key;
}

void *run_client(void *arg) {
  int i;
  struct thread_params params = *(struct thread_params *)arg;
  int clt_gid = params.id; /* Global ID of this client thread */
  int num_client_ports = params.num_client_ports;
  int num_server_ports = params.num_server_ports;
  int update_percentage = params.update_percentage;

  /* This is the only port used by this client */
  int ib_port_index = params.base_port_index + clt_gid % num_client_ports;

  /*
   * The virtual server port index to connect to. This index is relative to
   * the server's base_port_index (that the client does not know).
   */
  int srv_virt_port_index = clt_gid % num_server_ports;

  /*
   * TODO: The client creates a connected buffer because the libhrd API
   * requires a buffer when creating connected queue pairs. This should be
   * fixed in the API.
   */
  struct hrd_ctrl_blk *cb = hrd_ctrl_blk_init(
      clt_gid,                /* local_hid */
      ib_port_index, -1,      /* port_index, numa_node_id */
      1, 1,                   /* #conn qps, uc */
      NULL, 4096, -1,         /* prealloc conn buf, buf size, key */
      1, DGRAM_BUF_SIZE, -1); /* num_dgram_qps, dgram_buf_size, key */

  char mstr_qp_name[HRD_QP_NAME_SIZE];
  sprintf(mstr_qp_name, "master-%d-%d", srv_virt_port_index, clt_gid);

  char clt_conn_qp_name[HRD_QP_NAME_SIZE];
  sprintf(clt_conn_qp_name, "client-conn-%d", clt_gid);
  char clt_dgram_qp_name[HRD_QP_NAME_SIZE];
  sprintf(clt_dgram_qp_name, "client-dgram-%d", clt_gid);

  hrd_publish_conn_qp(cb, 0, clt_conn_qp_name);
  hrd_publish_dgram_qp(cb, 0, clt_dgram_qp_name);
  printf("main: Client %s published conn and dgram. Waiting for master %s\n",
         clt_conn_qp_name, mstr_qp_name);

  struct hrd_qp_attr *mstr_qp = NULL;
  while (mstr_qp == NULL) {
    mstr_qp = hrd_get_published_qp(mstr_qp_name);
    if (mstr_qp == NULL) {
      usleep(200000);
    }
  }

  printf("main: Client %s found master! Connecting..\n", clt_conn_qp_name);
  hrd_connect_qp(cb, 0, mstr_qp);
  hrd_wait_till_ready(mstr_qp_name);

  /* Start the real work */
  uint64_t seed = 0xdeadbeef;
  int *key_arr = get_random_permutation(HERD_NUM_KEYS, clt_gid, &seed);
  int key_i, ret;

  /* Some tracking info */
  int ws[NUM_WORKERS] = {0}; /* Window slot to use for a worker */

  struct mica_op *req_buf = memalign(4096, sizeof(*req_buf));
  assert(req_buf != NULL);
  struct ibv_mr *key_mr = ibv_reg_mr(cb->pd, req_buf, sizeof(struct mica_op),
                                     IBV_ACCESS_LOCAL_WRITE);

  struct ibv_send_wr wr, *bad_send_wr;
  struct ibv_sge sgl;
  struct ibv_wc wc[WINDOW_SIZE];

  struct ibv_recv_wr recv_wr[WINDOW_SIZE], *bad_recv_wr;
  struct ibv_sge recv_sgl[WINDOW_SIZE];

  long long rolling_iter = 0; /* For throughput measurement */
  long long nb_tx = 0;        /* Total requests performed or queued */
  int wn = 0;                 /* Worker number */
  int *write_key, *op_key;

#define _MEASURE_THROUGHPUT_
#ifdef _MEASURE_THROUGHPUT_
  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);
#endif

//#define _MEASURE_LATENCY_
#ifdef _MEASURE_LATENCY_
  struct timespec l_start, l_end;

  unsigned long *rd_latency;
  unsigned long *wr_latency;
  unsigned int rd_idx = 0, wr_idx = 0;
  size_t sz_lat;

  sz_lat = sizeof(unsigned long) * test_times;
  rd_latency = malloc(sz_lat);
  wr_latency = malloc(sz_lat);
  if (rd_latency == NULL || wr_latency == NULL) {
    printf("%s(): OOM\n", __func__);
    exit(0);
  }

  rd_idx = 0;
  wr_idx = 0;
  memset(rd_latency, 0, sz_lat);
  memset(wr_latency, 0, sz_lat);
#endif

  /*
   * Get the workload info from the input workload file.
   * This is per thread.
   */
  get_file(&op_key, &write_key, clt_gid);

  /* Fill the RECV queue */
  for (i = 0; i < WINDOW_SIZE; i++) {
    hrd_post_dgram_recv(cb->dgram_qp[0], (void *)cb->dgram_buf, DGRAM_BUF_SIZE,
                        cb->dgram_buf_mr->lkey);
  }

  /*
   * NOTES added by YS
   *
   * 1. By default, WINDOW_SIZE is 1. That means there can be
   *    only 1 outstanding RPC. Is it a wrong number?
   */
  while (1) {
#ifdef _MEASURE_THROUGHPUT_
    if (rolling_iter >= K_512) {
      clock_gettime(CLOCK_REALTIME, &end);
      double seconds = (end.tv_sec - start.tv_sec) +
                       (double)(end.tv_nsec - start.tv_nsec) / 1000000000;
      printf("main: Client %d: %.2f IOPS. nb_tx = %lld\n", clt_gid,
             K_512 / seconds, nb_tx);

      if (nb_tx >= 2000000)
        break;

      rolling_iter = 0;

      clock_gettime(CLOCK_REALTIME, &start);
    }
#endif

    /* Re-fill depleted RECVs */
    if (nb_tx % WINDOW_SIZE == 0 && nb_tx > 0) {
      for (i = 0; i < WINDOW_SIZE; i++) {
        recv_sgl[i].length = DGRAM_BUF_SIZE;
        recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
        recv_sgl[i].addr = (uintptr_t)&cb->dgram_buf[0];

        recv_wr[i].sg_list = &recv_sgl[i];
        recv_wr[i].num_sge = 1;
        recv_wr[i].next = (i == WINDOW_SIZE - 1) ? NULL : &recv_wr[i + 1];
      }

      ret = ibv_post_recv(cb->dgram_qp[0], &recv_wr[0], &bad_recv_wr);
      CPE(ret, "ibv_post_recv error", ret);
    }

#if WINDOW_SIZE != 1
    if (nb_tx % WINDOW_SIZE == 0 && nb_tx > 0) {
      hrd_poll_cq(cb->dgram_recv_cq[0], WINDOW_SIZE, wc);
    }
#endif

    wn = hrd_fastrand(&seed) % NUM_WORKERS; /* Choose a worker */

    /* Forge the HERD request */
    int is_update = (hrd_fastrand(&seed) % 100 < update_percentage) ? 1 : 0;

    key_i = hrd_fastrand(&seed) % HERD_NUM_KEYS; /* Choose a key */
    if (op_key[nb_tx % test_times] == 1)
      is_update = 1;
    else
      is_update = 0;

    key_i = write_key[nb_tx % test_times];

#ifdef MICA_IF_TEST_LATENCY
    {
      key_i = 1;
      if (MICA_IF_TEST_LATENCY_MODE == MICA_TEST_WRITE)
        is_update = 1;
      else
        is_update = 0;
    }
#endif

    // printf("%d %d\n", is_update, key_i);

    // int is_update = (op_key[nb_tx%test_times]) ? 1 : 0;
    // key_i = write_key[nb_tx%test_times];

    *(uint128 *)req_buf = CityHash128((char *)&key_arr[key_i], 4);
    req_buf->opcode = is_update ? HERD_OP_PUT : HERD_OP_GET;
    req_buf->val_len = is_update ? HERD_VALUE_SIZE : -1;
    req_buf->key.__unused = key_i;

    /* Forge the RDMA work request */
    sgl.length = is_update ? HERD_PUT_REQ_SIZE : HERD_GET_REQ_SIZE;
    sgl.addr = (uint64_t)(uintptr_t)req_buf;
    sgl.lkey = key_mr->lkey;

    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.num_sge = 1;
    wr.next = NULL;
    wr.sg_list = &sgl;

    wr.send_flags = (nb_tx & UNSIG_BATCH_) == 0 ? IBV_SEND_SIGNALED : 0;
    if ((nb_tx & UNSIG_BATCH_) == UNSIG_BATCH_) {
      hrd_poll_cq(cb->conn_cq[0], 1, wc);
    }
    // wr.send_flags |= IBV_SEND_INLINE;

    wr.wr.rdma.remote_addr = mstr_qp->buf_addr + OFFSET(wn, clt_gid, ws[wn]) *
                                                     sizeof(struct mica_op);
    wr.wr.rdma.rkey = mstr_qp->rkey;

#ifdef _MEASURE_LATENCY_
    clock_gettime(CLOCK_REALTIME, &l_start);
#endif

    ret = ibv_post_send(cb->conn_qp[0], &wr, &bad_send_wr);
    CPE(ret, "ibv_post_send error", ret);

    /*
     * HACK!!
     * if window size is 1, we poll after every sending.
     * othwerwise, wait for a batch.
     */
#if WINDOW_SIZE == 1
    hrd_poll_cq(cb->dgram_recv_cq[0], WINDOW_SIZE, wc);
#endif

#ifdef _MEASURE_LATENCY_
    clock_gettime(CLOCK_REALTIME, &l_end);
    unsigned long ns = (l_end.tv_sec - l_start.tv_sec) * 1000000000 +
                       (l_end.tv_nsec - l_start.tv_nsec);

    if (is_update) {
      wr_latency[wr_idx % test_times] = ns;
      wr_idx++;
    } else {
      rd_latency[rd_idx % test_times] = ns;
      rd_idx++;
    }
#endif

    rolling_iter++;
    nb_tx++;
    HRD_MOD_ADD(ws[wn], WINDOW_SIZE);

    if (nb_tx % test_times == 0) {
#ifdef _MEASURE_LATENCY_
      unsigned long rd_total = 0, wr_total = 0;
      int nr_rd = 0, nr_wr = 0;
      for (i = 0; i < test_times; i++) {
        /* Not every slot might be filled */
        if (rd_latency[i]) {
          nr_rd++;
          rd_total += rd_latency[i];
        }
        if (wr_latency[i]) {
          nr_wr++;
          wr_total += wr_latency[i];
        }
      }

      printf("[Thread-ID %2d] NR TX finished: %lld\n", clt_gid, nb_tx);
      printf("[Thread-ID %2d] Read. NR: %10d Avg: %10lu (ns)\n", clt_gid, nr_rd,
             nr_rd ? rd_total / nr_rd : 0);
      printf("[Thread-ID %2d] Writ. NR: %10d Avg: %10lu (ns)\n", clt_gid, nr_wr,
             nr_wr ? wr_total / nr_wr : 0);

      /*
       * Added by YS:
       *
       * The first 1mil and the following tx
       * have different perf numbers..
       * I guess we need to skip the first 1 mil.
       */
      if (nb_tx / test_times == 3) {
        char string[128];
        int fd1 = open("./dump_latency_rd.txt", O_CREAT | O_RDWR, 0644);
        int fd2 = open("./dump_latency_wr.txt", O_CREAT | O_RDWR, 0644);
        if (fd1 < 0 || fd2 < 0) {
          printf("Fail to open file\n");
          return 0;
        }
        for (i = 0; i < test_times; i++) {
          int nr;

          if (rd_latency[i]) {
            nr = sprintf(string, "%3d %10d %20lu\n", clt_gid, i, rd_latency[i]);
            write(fd1, string, nr);
          }
          if (wr_latency[i]) {
            nr = sprintf(string, "%3d %10d %20lu\n", clt_gid, i, wr_latency[i]);
            write(fd2, string, nr);
          }
        }
        fsync(fd1);
        fsync(fd2);

        exit(0);
      }

      rd_idx = 0;
      wr_idx = 0;
      memset(rd_latency, 0, sz_lat);
      memset(wr_latency, 0, sz_lat);
#endif
    }
  }
  return NULL;
}

void *run_memory(void *arg) {
  // int i;
  struct thread_params params = *(struct thread_params *)arg;
  int mem_gid = params.id; /* Global ID of this client thread */
  int num_client_ports = params.num_client_ports;

  /* This is the only port used by this client */
  int ib_port_index = params.base_port_index + mem_gid % num_client_ports;

  /*
   * The virtual server port index to connect to. This index is relative to
   * the server's base_port_index (that the client does not know).
   */
  /*
   * TODO: The client creates a connected buffer because the libhrd API
   * requires a buffer when creating connected queue pairs. This should be
   * fixed in the API.
   */
  struct hrd_ctrl_blk *cb = hrd_ctrl_blk_init(
      mem_gid,           /* local_hid */
      ib_port_index, -1, /* port_index, numa_node_id */
      NUM_WORKERS, 0,    /* #conn qps, uc */
      NULL, HERD_VALUE_SIZE * (HERD_NUM_KEYS + 1) * 2,
      -1,                     /* prealloc conn buf, buf size, key */
      1, DGRAM_BUF_SIZE, -1); /* num_dgram_qps, dgram_buf_size, key */

  char mem_conn_qp_name[HRD_QP_NAME_SIZE];
  int per_worker;
  for (per_worker = 0; per_worker < NUM_WORKERS; per_worker++) {
    sprintf(mem_conn_qp_name, "qp-memory-%d-worker-%d", mem_gid, per_worker);
    hrd_publish_conn_qp(cb, per_worker, mem_conn_qp_name);
    printf("publish %s\n", mem_conn_qp_name);
  }

  printf("main: Memory %d published conn Waiting for worker %d\n", mem_gid,
         NUM_WORKERS);

  struct hrd_qp_attr *mstr_qp = NULL;
  for (per_worker = 0; per_worker < NUM_WORKERS; per_worker++) {
    sprintf(mem_conn_qp_name, "qp-worker-%d-memory-%d", per_worker, mem_gid);
    mstr_qp = NULL;
    do {
      mstr_qp = hrd_get_published_qp(mem_conn_qp_name);
      while (mstr_qp == NULL) {
        usleep(200000);
      }
    } while (mstr_qp == NULL);
    printf("main: Memory %s found worker! Connecting..\n", mem_conn_qp_name);
    hrd_connect_qp(cb, per_worker, mstr_qp);
    printf("main: Memory %s connect worker.\n", mem_conn_qp_name);
  }

  volatile void **shared_memory_space;
  // struct ibv_mr **shared_memory_region;
  volatile void **backup_memory_space;
  // struct ibv_mr **backup_memory_region;
  int i;
  char shared_memory_name[HRD_QP_NAME_SIZE];
  {
    shared_memory_space = malloc(sizeof(uintptr_t *) * HERD_NUM_KEYS);
    // shared_memory_region = malloc(sizeof(struct ibv_mr *) * HERD_NUM_KEYS);
    backup_memory_space = malloc(sizeof(uintptr_t *) * HERD_NUM_KEYS);
    printf("main: Memory %d start publishing %d mr\n", mem_gid, HERD_NUM_KEYS);
    for (i = 0; i <= HERD_NUM_KEYS; i++) {
      struct hrd_qp_attr pub_qp;
      shared_memory_space[i] = cb->conn_buf + (HERD_VALUE_SIZE * i);
      backup_memory_space[i] =
          cb->conn_buf + (HERD_VALUE_SIZE * (i + HERD_NUM_KEYS + 1));
      if (shared_memory_space[i] && backup_memory_space[i]) {
        memset(shared_memory_name, 0, HRD_QP_NAME_SIZE);
        sprintf(shared_memory_name, "memory-%d-mr-%d", mem_gid, i);
        pub_qp.buf_addr = (uintptr_t)shared_memory_space[i];
        pub_qp.rkey = cb->conn_buf_mr->rkey;
        hrd_publish(shared_memory_name, &pub_qp, sizeof(struct hrd_qp_attr));

        memset(shared_memory_name, 0, HRD_QP_NAME_SIZE);
        sprintf(shared_memory_name, "backupmemory-%d-mr-%d", mem_gid, i);
        pub_qp.buf_addr = (uintptr_t)backup_memory_space[i];
        pub_qp.rkey = cb->conn_buf_mr->rkey;
        hrd_publish(shared_memory_name, &pub_qp, sizeof(struct hrd_qp_attr));
      } else {
        printf("main: failed to publish %d\n", i);
      }
    }
    printf("main: Memory %d publish %d mr %s\n", mem_gid, HERD_NUM_KEYS,
           shared_memory_name);
  }
  /*memset(shared_memory_space[0], 0x32, 24);
printf("enter while loop\n");
while(1)
{
printf("current - %s\n", (char *)shared_memory_space[0]);
sleep(1);
}*/
  while (1)
    ;
  return NULL;
}
