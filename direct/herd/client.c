#include "hrd.h"
#include "main.h"
#include "mica.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

#define DGRAM_BUF_SIZE 4096

#define ONE_LOCK 3378

long long send_traffic[NUM_CLIENTS];
long long recv_traffic[NUM_CLIENTS];
static uint32_t crc32_tab[] = {
    0x00000000, 0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f,
    0xe963a535, 0x9e6495a3, 0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988,
    0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91, 0x1db71064, 0x6ab020f2,
    0xf3b97148, 0x84be41de, 0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
    0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec, 0x14015c4f, 0x63066cd9,
    0xfa0f3d63, 0x8d080df5, 0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172,
    0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b, 0x35b5a8fa, 0x42b2986c,
    0xdbbbc9d6, 0xacbcf940, 0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
    0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116, 0x21b4f4b5, 0x56b3c423,
    0xcfba9599, 0xb8bda50f, 0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924,
    0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d, 0x76dc4190, 0x01db7106,
    0x98d220bc, 0xefd5102a, 0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
    0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818, 0x7f6a0dbb, 0x086d3d2d,
    0x91646c97, 0xe6635c01, 0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e,
    0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457, 0x65b0d9c6, 0x12b7e950,
    0x8bbeb8ea, 0xfcb9887c, 0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
    0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 0x4adfa541, 0x3dd895d7,
    0xa4d1c46d, 0xd3d6f4fb, 0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0,
    0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9, 0x5005713c, 0x270241aa,
    0xbe0b1010, 0xc90c2086, 0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
    0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 0x59b33d17, 0x2eb40d81,
    0xb7bd5c3b, 0xc0ba6cad, 0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a,
    0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683, 0xe3630b12, 0x94643b84,
    0x0d6d6a3e, 0x7a6a5aa8, 0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
    0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe, 0xf762575d, 0x806567cb,
    0x196c3671, 0x6e6b06e7, 0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc,
    0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5, 0xd6d6a3e8, 0xa1d1937e,
    0x38d8c2c4, 0x4fdff252, 0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
    0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60, 0xdf60efc3, 0xa867df55,
    0x316e8eef, 0x4669be79, 0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236,
    0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f, 0xc5ba3bbe, 0xb2bd0b28,
    0x2bb45a92, 0x5cb36a04, 0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
    0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a, 0x9c0906a9, 0xeb0e363f,
    0x72076785, 0x05005713, 0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38,
    0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21, 0x86d3d2d4, 0xf1d4e242,
    0x68ddb3f8, 0x1fda836e, 0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
    0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c, 0x8f659eff, 0xf862ae69,
    0x616bffd3, 0x166ccf45, 0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2,
    0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db, 0xaed16a4a, 0xd9d65adc,
    0x40df0b66, 0x37d83bf0, 0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,
    0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6, 0xbad03605, 0xcdd70693,
    0x54de5729, 0x23d967bf, 0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94,
    0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d};

uint32_t crc32(uint32_t crc, const void *buf, size_t size);
uint32_t crc32(uint32_t crc, const void *buf, size_t size) {
  const uint8_t *p;
  p = buf;
  crc = crc ^ ~0U;

  while (size--)
    crc = crc32_tab[(crc ^ *p++) & 0xFF] ^ (crc >> 8);

  return crc ^ ~0U;
}

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
int test_times = 1000000;

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
  sprintf(filepath, "workload/ycsb/workloadc_%d", thread_id);

  printf("start reading %s %d\n", filepath, test_times);
  fflush(stdout);
  fp = fopen(filepath, "r");
  if (!fp) {
    printf("Fail to open file\n");
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
  fflush(stdout);
  *op = op_key;
  *key = write_key;
}

void userspace_one_poll(struct hrd_ctrl_blk *mem_cb, int tar_mem) {
  struct ibv_wc wc[MITSUME_MICA_WINDOW_SIZE];
  hrd_poll_cq(mem_cb->conn_cq[tar_mem], 1, wc);
}

int userspace_one_write(unsigned long long tar_key, int tar_mem,
                        struct hrd_ctrl_blk *mem_cb, struct ibv_mr *mitsume_mr,
                        int request_size, struct hrd_qp_attr ***mr_qp_list,
                        int clt_gid) {
  struct ibv_sge test_sge;
  struct ibv_send_wr wr, *bad_send_wr;
  int ret;
  struct hrd_qp_attr **mr_qp;
  // unsigned long long tar_key = op[I]->key.bkt;
  mr_qp = mr_qp_list[tar_mem];

  test_sge.length = request_size;
  test_sge.addr = (uintptr_t)mitsume_mr->addr;
  test_sge.lkey = mitsume_mr->lkey;
  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.num_sge = 1;
  wr.next = NULL;
  wr.sg_list = &test_sge;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr = mr_qp[tar_key]->buf_addr;
  wr.wr.rdma.rkey = mr_qp[tar_key]->rkey;
  ret = ibv_post_send(mem_cb->conn_qp[tar_mem], &wr, &bad_send_wr);
  CPE(ret, "ibv_post_send error", ret);
  send_traffic[clt_gid] += test_sge.length;
  return tar_mem;
}

int userspace_one_read(unsigned long long tar_key, int tar_mem,
                       struct hrd_ctrl_blk *mem_cb, struct ibv_mr *mitsume_mr,
                       int request_size, struct hrd_qp_attr ***mr_qp_list,
                       int verification, int clt_gid) {
  struct ibv_sge test_sge;
  struct ibv_send_wr wr, *bad_send_wr;
  int ret;
  struct hrd_qp_attr **mr_qp;
  // unsigned long long tar_key = op[I]->key.bkt;
  mr_qp = mr_qp_list[tar_mem];

  if (verification)
    test_sge.length = 8;
  else
    test_sge.length = request_size;
  test_sge.addr = (uintptr_t)mitsume_mr->addr;
  test_sge.lkey = mitsume_mr->lkey;
  wr.opcode = IBV_WR_RDMA_READ;
  wr.num_sge = 1;
  wr.next = NULL;
  wr.sg_list = &test_sge;
  wr.send_flags = IBV_SEND_SIGNALED;
  if (verification)
    wr.wr.rdma.remote_addr = mr_qp[tar_key]->buf_addr + request_size - 8;
  else
    wr.wr.rdma.remote_addr = mr_qp[tar_key]->buf_addr;
  wr.wr.rdma.rkey = mr_qp[tar_key]->rkey;
  ret = ibv_post_send(mem_cb->conn_qp[tar_mem], &wr, &bad_send_wr);
  CPE(ret, "ibv_post_send error", ret);
  recv_traffic[clt_gid] += test_sge.length;
  return tar_mem;
}

int userspace_one_compare_and_swp(unsigned long long tar_key, int tar_mem,
                                  struct hrd_ctrl_blk *mem_cb,
                                  struct ibv_mr *mitsume_mr,
                                  struct hrd_qp_attr ***mr_qp_list,
                                  uint64_t expect_value, uint64_t assign_value,
                                  int clt_gid) {
  struct ibv_sge test_sge;
  struct ibv_send_wr wr, *bad_send_wr;
  int ret;
  struct hrd_qp_attr **mr_qp;

  mr_qp = mr_qp_list[tar_mem];
  test_sge.length = 8;
  test_sge.addr = (uintptr_t)mitsume_mr->addr;
  test_sge.lkey = mitsume_mr->lkey;

  wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
  wr.num_sge = 1;
  wr.next = NULL;
  wr.sg_list = &test_sge;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.atomic.remote_addr = mr_qp[tar_key]->buf_addr;
  wr.wr.atomic.rkey = mr_qp[tar_key]->rkey;
  wr.wr.atomic.compare_add = expect_value;
  wr.wr.atomic.swap = assign_value;

  ret = ibv_post_send(mem_cb->conn_qp[tar_mem], &wr, &bad_send_wr);
  CPE(ret, "ibv_post_send error", ret);
  send_traffic[clt_gid] += test_sge.length;
  return tar_mem;
}

int mitsume_write_one_key_post(int tar_key, struct hrd_ctrl_blk *mem_cb,
                               struct hrd_qp_attr ***mr_qp_list,
                               struct hrd_qp_attr ***backupmr_qp_list,
                               struct ibv_mr *input_mr,
                               struct ibv_mr *output_mr, void *addr,
                               int clt_gid) {
  uint64_t unlock = 0;
  uint64_t lock = ONE_LOCK;
  long long unsigned *read_lock;
  long long unsigned *write_lock;
  int count = 0;
  uint64_t crc = 0;
  int request_size = MICA_HERD_VALUE_SIZE;
  int tar_mem = tar_key % NUM_MEMORY;
  int j;

  if (CRC_MODE) // enable crc
    crc = crc32(~0, addr, request_size);
  write_lock = output_mr->addr;
  read_lock = input_mr->addr;
  *write_lock = lock;
  memcpy(output_mr->addr + 8, addr, request_size);
  memcpy(output_mr->addr + 8 + request_size, &crc, sizeof(uint64_t));
  // 1. lock

  do {
    *read_lock = 0xffffffff;
    userspace_one_compare_and_swp(tar_key, tar_mem, mem_cb, input_mr,
                                  mr_qp_list, unlock, lock, clt_gid);
    userspace_one_poll(mem_cb, tar_mem);
    count++;
  } while (*read_lock != unlock);

  /*// 2. read from space
  userspace_one_read(tar_key, tar_mem, mem_cb, input_mr, request_size+16,
  mr_qp_list, 0, clt_gid); userspace_one_poll(mem_cb, tar_mem);*/

  // 3. write to backup
  for (j = 0; j < NUM_REPLICATION; j++)
    userspace_one_write(tar_key, (tar_mem + j) % NUM_MEMORY, mem_cb, output_mr,
                        request_size, backupmr_qp_list, clt_gid);
  for (j = 0; j < NUM_REPLICATION; j++)
    userspace_one_poll(mem_cb, (tar_mem + j) % NUM_MEMORY);

#ifdef MICA_READ_VERIFICATION
  // 3.1 read to check backup
  for (j = 0; j < NUM_REPLICATION; j++)
    userspace_one_read(tar_key, (tar_mem + j) % NUM_MEMORY, mem_cb, input_mr,
                       request_size, backupmr_qp_list, 1, clt_gid);
  for (j = 0; j < NUM_REPLICATION; j++)
    userspace_one_poll(mem_cb, (tar_mem + j) % NUM_MEMORY);
#endif

  // 4. write to old place
  for (j = 0; j < NUM_REPLICATION; j++)
    userspace_one_write(tar_key, (tar_mem + j) % NUM_MEMORY, mem_cb, output_mr,
                        request_size + 16, mr_qp_list, clt_gid);
  for (j = 0; j < NUM_REPLICATION; j++)
    userspace_one_poll(mem_cb, (tar_mem + j) % NUM_MEMORY);

#ifdef MICA_READ_VERIFICATION
  // 4.1 read to check backup
  for (j = 0; j < NUM_REPLICATION; j++)
    userspace_one_read(tar_key, (tar_mem + j) % NUM_MEMORY, mem_cb, input_mr,
                       request_size + 16, mr_qp_list, 1, clt_gid);
  for (j = 0; j < NUM_REPLICATION; j++)
    userspace_one_poll(mem_cb, (tar_mem + j) % NUM_MEMORY);
#endif

  // 5. write to unlock
  *write_lock = unlock;
  userspace_one_write(tar_key, tar_mem, mem_cb, output_mr, 8, mr_qp_list,
                      clt_gid);
  userspace_one_poll(mem_cb, tar_mem);
  return 0;
}

int mitsume_read_one_key_post(int tar_key, struct hrd_ctrl_blk *mem_cb,
                              struct hrd_qp_attr ***mr_qp_list,
                              struct hrd_qp_attr ***backupmr_qp_list,
                              struct ibv_mr *input_mr, void *addr,
                              int clt_gid) {
  int count = 0;
  uint64_t crc_compute;
  uint64_t *crc_check;
  uint64_t *check;
  uint64_t unlock = 0;
  uint64_t lock = ONE_LOCK;
  int request_size = MICA_HERD_VALUE_SIZE;
  int tar_mem;
  check = (uint64_t *)input_mr->addr;
  // if(!disable_crc)
  tar_mem = tar_key % NUM_MEMORY;
  if (CRC_MODE) {
    do {
      userspace_one_read(tar_key, tar_mem, mem_cb, input_mr, request_size + 16,
                         mr_qp_list, 0, clt_gid);
      userspace_one_poll(mem_cb, tar_mem);
      // userspace_liteapi_rdma_read(undo_key_map[key].primary_key[0],
      // temp_read, request_size+16, 0, LITE_TEST_PW);

      crc_check = (uint64_t *)(input_mr->addr + request_size + 8);
      crc_compute = crc32(~0, input_mr->addr + 8, request_size);

      // enable following two lines and disable above two lines can disable
      // read-crc crc_check = &unlock; crc_compute = 0;
      count++;
    } while (*crc_check != crc_compute || *check == ONE_LOCK);
    // memcpy(addr, input_mr->addr+8, request_size);
  } else {
    // 1. lock
    do {
      *check = 0xffffffff;
      userspace_one_compare_and_swp(tar_key, tar_mem, mem_cb, input_mr,
                                    mr_qp_list, unlock, lock, clt_gid);
      userspace_one_poll(mem_cb, tar_mem);
      // userspace_liteapi_compare_swp(undo_key_map[key].primary_key[0], &read,
      // unlock, lock);
      count++;
    } while (*check != unlock);

    // 2. rdma-read
    userspace_one_read(tar_key, tar_mem, mem_cb, input_mr, request_size + 16,
                       mr_qp_list, 0, clt_gid);
    userspace_one_poll(mem_cb, tar_mem);
    // userspace_liteapi_rdma_read(undo_key_map[key].primary_key[0], temp_read,
    // request_size+16, 0, LITE_TEST_PW);

    // memcpy(addr, input_mr->addr+8, request_size);
    // memcpy(addr, temp_read+8, request_size);

    // 3. write to unlock
    *check = unlock;
    userspace_one_write(tar_key, tar_mem, mem_cb, input_mr, sizeof(uint64_t),
                        mr_qp_list, clt_gid);
    userspace_one_poll(mem_cb, tar_mem);
    // userspace_liteapi_rdma_write(undo_key_map[key].primary_key[0], &unlock,
    // sizeof(uint64_t), 0, LITE_TEST_PW);
  }
  // userspace_liteapi_rdma_read(key_map[key], addr, request_size, 8,
  // LITE_TEST_PW); if(count>1)
  //        printf("read %d\n", count);
  // printf("%llu\n", (unsigned long long) crc_compute);
  return 0;
}

int mitsume_lock_and_update_one_key_post(
    int tar_key, struct hrd_ctrl_blk *mem_cb, struct hrd_qp_attr ***mr_qp_list,
    struct hrd_qp_attr ***backupmr_qp_list, struct ibv_mr *input_mr,
    struct ibv_mr *output_mr, void *addr, int clt_gid) {
  uint64_t unlock = 0;
  uint64_t lock = ONE_LOCK;
  long long unsigned *read_lock;
  long long unsigned *write_lock;
  int count = 0;
  int request_size = MICA_HERD_VALUE_SIZE;
  int tar_mem = tar_key % NUM_MEMORY;
  int j;
  write_lock = output_mr->addr;
  read_lock = input_mr->addr;
  *write_lock = lock;
  // 1. lock

  do {
    *read_lock = 0xffffffff;
    userspace_one_compare_and_swp(tar_key, tar_mem, mem_cb, input_mr,
                                  mr_qp_list, unlock, lock, clt_gid);
    userspace_one_poll(mem_cb, tar_mem);
    count++;
  } while (*read_lock != unlock);

  // 4. write to old place
  for (j = 0; j < NUM_REPLICATION; j++)
    userspace_one_write(tar_key, (tar_mem + j) % NUM_MEMORY, mem_cb, output_mr,
                        request_size + 8, mr_qp_list, clt_gid);
  for (j = 0; j < NUM_REPLICATION; j++)
    userspace_one_poll(mem_cb, (tar_mem + j) % NUM_MEMORY);

  // 5. write to unlock
  *write_lock = unlock;
  userspace_one_write(tar_key, tar_mem, mem_cb, output_mr, 8, mr_qp_list,
                      clt_gid);
  userspace_one_poll(mem_cb, tar_mem);
  return 0;
}

struct hrd_ctrl_blk *
mitsume_setup_connection(int clt_gid, int ib_port_index,
                         struct hrd_qp_attr ***mr_qp_list,
                         struct hrd_qp_attr ***backupmr_qp_list) {

  struct hrd_ctrl_blk *mem_cb;
  struct hrd_qp_attr **mr_qp;
  struct hrd_qp_attr **backupmr_qp;

  int per_memory, per_mr;
  char mem_conn_qp_name[HRD_QP_NAME_SIZE];
  mem_cb = hrd_ctrl_blk_init(
      clt_gid,                /* local_hid */
      ib_port_index, -1,      /* port_index, numa_node_id */
      NUM_MEMORY, 0,          /* #conn qps, uc */
      NULL, 4096, -1,         /* prealloc conn buf, buf size, key */
      1, DGRAM_BUF_SIZE, -1); /* num_dgram_qps, dgram_buf_size, key */

  for (per_memory = 0; per_memory < NUM_MEMORY; per_memory++) {
    sprintf(mem_conn_qp_name, "qp-client-%d-memory-%d", clt_gid, per_memory);
    hrd_publish_conn_qp(mem_cb, per_memory, mem_conn_qp_name);
    printf("%s()-%d: publish %s\n", __func__, __LINE__, mem_conn_qp_name);
  }
  printf("main: client %d published conn\n", clt_gid);

  struct hrd_qp_attr *mstr_qp = NULL;
  for (per_memory = 0; per_memory < NUM_MEMORY; per_memory++) {
    sprintf(mem_conn_qp_name, "qp-memory-%d-client-%d", per_memory, clt_gid);
    mstr_qp = NULL;
    do {
      mstr_qp = hrd_get_published_qp(mem_conn_qp_name);
      if (mstr_qp == NULL) {
        usleep(200000);
      }
    } while (mstr_qp == NULL);

    printf("main: Client %d found memory %s ! Connecting..\n", clt_gid,
           mem_conn_qp_name);
    hrd_connect_qp(mem_cb, per_memory, mstr_qp);
    printf("main: Client %d connect memory %s\n", clt_gid, mem_conn_qp_name);
  }

  for (per_memory = 0; per_memory < NUM_MEMORY; per_memory++) {
    char memory_ready_name[HRD_QP_NAME_SIZE] = {};
    sprintf(memory_ready_name, "memory-%d-ready", per_memory);
    hrd_wait_till_ready(memory_ready_name);
  }
  for (per_memory = 0; per_memory < NUM_MEMORY; per_memory++) {
    mr_qp_list[per_memory] =
        malloc(sizeof(struct hrd_qp_attr *) * HERD_NUM_KEYS);
    mr_qp = mr_qp_list[per_memory];
    backupmr_qp_list[per_memory] =
        malloc(sizeof(struct hrd_qp_attr *) * HERD_NUM_KEYS);
    backupmr_qp = backupmr_qp_list[per_memory];
    printf("main: client %d start fetching mr. Connecting..\n", clt_gid);
    for (per_mr = 0; per_mr <= HERD_NUM_KEYS; per_mr++) {
      char mr_name[HRD_QP_NAME_SIZE];
      char backupmr_name[HRD_QP_NAME_SIZE];
      memset(mr_name, 0, HRD_QP_NAME_SIZE);
      sprintf(mr_name, "memory-%d-mr-%d", per_memory, per_mr);

      memset(backupmr_name, 0, HRD_QP_NAME_SIZE);
      sprintf(backupmr_name, "backupmemory-%d-mr-%d", per_memory, per_mr);
      if (per_mr == 0) {
        do {
          mr_qp[per_mr] = hrd_get_published_qp(mr_name);
          if (mr_qp[per_mr] == NULL) {
            usleep(200000);
          }
        } while (mr_qp[per_mr] == NULL);

        do {
          backupmr_qp[per_mr] = hrd_get_published_qp(backupmr_name);
          if (backupmr_qp[per_mr] == NULL) {
            usleep(1000000);
          }
        } while (backupmr_qp[per_mr] == NULL);
      } else {
        mr_qp[per_mr] = malloc(sizeof(struct hrd_qp_attr));
        backupmr_qp[per_mr] = malloc(sizeof(struct hrd_qp_attr));

        mr_qp[per_mr]->rkey = mr_qp[per_mr - 1]->rkey;
        backupmr_qp[per_mr]->rkey = backupmr_qp[per_mr - 1]->rkey;

        mr_qp[per_mr]->buf_addr = mr_qp[per_mr - 1]->buf_addr + HERD_SPACE_SIZE;
        backupmr_qp[per_mr]->buf_addr =
            backupmr_qp[per_mr - 1]->buf_addr + HERD_SPACE_SIZE;
      }
      if (per_mr % 50000 == 0) {
        printf("main: client %d get memory %d from memory %d. \n", clt_gid,
               per_mr, per_memory);
      }
    }
  }
  printf("client %d finish mitsume setup\n", clt_gid);

  return mem_cb;
}

void *run_client(void *arg) {
  struct thread_params params = *(struct thread_params *)arg;
  int clt_gid = params.id; /* Global ID of this client thread */
  int num_client_ports = params.num_client_ports;
  int num_server_ports = params.num_server_ports;
  // int update_percentage = params.update_percentage;

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
  char ready_name[HRD_QP_NAME_SIZE] = {};
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

  struct hrd_ctrl_blk *mem_cb;
  struct hrd_qp_attr ***mr_qp_list =
      malloc(sizeof(struct hrd_qp_attr **) * NUM_MEMORY);
  struct hrd_qp_attr ***backupmr_qp_list =
      malloc(sizeof(struct hrd_qp_attr **) * NUM_MEMORY);
  mem_cb = mitsume_setup_connection(clt_gid, ib_port_index, mr_qp_list,
                                    backupmr_qp_list);
  void *input_space = memalign(4096, HERD_SPACE_SIZE);
  void *output_space = memalign(4096, HERD_SPACE_SIZE);
  void *read_write_addr = memalign(4096, HERD_SPACE_SIZE);
  memset(read_write_addr, 0x31 + clt_gid, HERD_SPACE_SIZE);
  struct ibv_mr *input_mr = ibv_reg_mr(mem_cb->pd, input_space, HERD_SPACE_SIZE,
                                       IBV_ACCESS_LOCAL_WRITE);
  struct ibv_mr *output_mr = ibv_reg_mr(
      mem_cb->pd, output_space, HERD_SPACE_SIZE, IBV_ACCESS_LOCAL_WRITE);
  int *write_key, *op_key;
  struct timespec start, end;

  get_file(&op_key, &write_key, clt_gid);

  // first node setup all keys
  if (clt_gid == 0) {
    int per_key;
    int per_memory;

    printf("start setup key\n");

    memset(output_space, 0, HERD_SPACE_SIZE);
    for (per_key = 1; per_key <= HERD_NUM_KEYS; per_key++) {
      for (per_memory = 0; per_memory < NUM_MEMORY; per_memory++) {
        // memset
        userspace_one_write(per_key, per_memory, mem_cb, output_mr,
                            HERD_SPACE_SIZE, mr_qp_list, clt_gid);
        userspace_one_poll(mem_cb, per_memory);
      }
      memset(read_write_addr, 0x31 + (per_key) % 20, HERD_SPACE_SIZE);
      mitsume_write_one_key_post(per_key, mem_cb, mr_qp_list, backupmr_qp_list,
                                 input_mr, output_mr, read_write_addr, clt_gid);
    }
    printf("finish setup %d\n", per_key);
  }

  // post ready
  if (clt_gid == 0) {
    int per_client;
    sprintf(ready_name, "%d-ready-start", clt_gid);

    hrd_publish_ready(ready_name);
    for (per_client = 0; per_client < NUM_CLIENTS; per_client++) {
      sprintf(ready_name, "%d-ready-start", per_client);
      hrd_wait_till_ready(ready_name);
    }
    sprintf(ready_name, "%s", "ready-go");
    hrd_publish_ready(ready_name);
  } else {
    sprintf(ready_name, "%d-ready-start", clt_gid);
    hrd_publish_ready(ready_name);
    sprintf(ready_name, "%s", "ready-go");
    hrd_wait_till_ready(ready_name);
  }

  printf("%d finish all setup\n", clt_gid);
  fflush(stdout);

  /* Start the real work */
  // uint64_t seed = 0xdeadbeef;
  // int* key_arr = get_random_permutation(HERD_NUM_KEYS, clt_gid, &seed);
  // int ret;
  int key_i;

  /* Some tracking info */
  // int ws[NUM_WORKERS] = {0}; /* Window slot to use for a worker */

  // struct mica_op* req_buf = memalign(4096, sizeof(*req_buf));
  // assert(req_buf != NULL);
  // struct ibv_mr *key_mr = ibv_reg_mr(cb->pd, req_buf, sizeof(struct mica_op),
  // IBV_ACCESS_LOCAL_WRITE);

  // struct ibv_send_wr wr, *bad_send_wr;
  // struct ibv_sge sgl;
  // struct ibv_wc wc[WINDOW_SIZE];

  // struct ibv_recv_wr recv_wr[WINDOW_SIZE], *bad_recv_wr;
  // struct ibv_sge recv_sgl[WINDOW_SIZE];

  long long rolling_iter = 0; /* For throughput measurement */
  long long nb_tx = 0;        /* Total requests performed or queued */
  // int wn = 0;                 /* Worker number */
  clock_gettime(CLOCK_REALTIME, &start);

  /* Fill the RECV queue */
  // int i;
  /*for (i = 0; i < WINDOW_SIZE; i++) {
    hrd_post_dgram_recv(cb->dgram_qp[0], (void*)cb->dgram_buf, DGRAM_BUF_SIZE,
                        cb->dgram_buf_mr->lkey);
  }*/

  printf("start working %d\n", clt_gid);

  /*
   * Added by YS
   */
#define _MEASURE_LATENCY_
#ifdef _MEASURE_LATENCY_
  struct timespec l_start, l_end;

  unsigned long *rd_latency;
  unsigned long *wr_latency;
  unsigned int rd_idx = 0, wr_idx = 0;
  size_t sz_lat;
  int i = 0;

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

  while (1) {
    if (rolling_iter >= K_512) {
      clock_gettime(CLOCK_REALTIME, &end);
      double seconds = (end.tv_sec - start.tv_sec) +
                       (double)(end.tv_nsec - start.tv_nsec) / 1000000000;

      printf("main: Client %d: %.2f IOPS. nb_tx = %lld "
             "send %.2f recv %.2f \n",
             clt_gid, K_512 / seconds, nb_tx, send_traffic[clt_gid] / seconds,
             recv_traffic[clt_gid] / seconds);
      send_traffic[clt_gid] = 0;
      recv_traffic[clt_gid] = 0;
      rolling_iter = 0;

      clock_gettime(CLOCK_REALTIME, &start);
    }

    int is_update;
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

#ifdef _MEASURE_LATENCY_
    clock_gettime(CLOCK_REALTIME, &l_start);
#endif

    if (is_update) {
#ifdef MICA_TEST_LOCK_AND_UPDATE
      mitsume_lock_and_update_one_key_post(key_i, mem_cb, mr_qp_list,
                                           backupmr_qp_list, input_mr,
                                           output_mr, read_write_addr, clt_gid);
#else
      mitsume_write_one_key_post(key_i, mem_cb, mr_qp_list, backupmr_qp_list,
                                 input_mr, output_mr, read_write_addr, clt_gid);
#endif
    } else {
      mitsume_read_one_key_post(key_i, mem_cb, mr_qp_list, backupmr_qp_list,
                                input_mr, read_write_addr, clt_gid);
    }

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

#ifdef _MEASURE_LATENCY_
    if (nb_tx % test_times == 0) {
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
        // if (0) {
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
    }
#endif
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
  printf("%d %d\n", HERD_NUM_KEYS, HERD_SPACE_SIZE * (HERD_NUM_KEYS + 1) * 2);
  struct hrd_ctrl_blk *cb = hrd_ctrl_blk_init(
      mem_gid,                      /* local_hid */
      ib_port_index, -1,            /* port_index, numa_node_id */
      NUM_WORKERS + NUM_CLIENTS, 0, /* #conn qps, uc */
      NULL, HERD_SPACE_SIZE * (HERD_NUM_KEYS + 1) * 2,
      -1,                     /* prealloc conn buf, buf size, key */
      1, DGRAM_BUF_SIZE, -1); /* num_dgram_qps, dgram_buf_size, key */

  char mem_conn_qp_name[HRD_QP_NAME_SIZE];
  int per_worker, per_client;
  for (per_worker = 0; per_worker < NUM_WORKERS; per_worker++) {
    sprintf(mem_conn_qp_name, "qp-memory-%d-worker-%d", mem_gid, per_worker);
    hrd_publish_conn_qp(cb, per_worker, mem_conn_qp_name);
    printf("publish %s\n", mem_conn_qp_name);
  }
  for (per_client = 0; per_client < NUM_CLIENTS; per_client++) {
    sprintf(mem_conn_qp_name, "qp-memory-%d-client-%d", mem_gid, per_client);
    hrd_publish_conn_qp(cb, per_client + NUM_WORKERS, mem_conn_qp_name);
    printf("publish %s\n", mem_conn_qp_name);
  }

  printf("main: Memory %d published conn\n", mem_gid);

  volatile void **shared_memory_space;
  // struct ibv_mr **shared_memory_region;
  volatile void **backup_memory_space;
  // struct ibv_mr **backup_memory_region;
  int i;
  char *pp = (char *)cb->conn_buf;
  for (i = 0; i < HERD_SPACE_SIZE * (HERD_NUM_KEYS + 1) * 2; i++) {
    pp[i] = 0x0;
  }

  struct hrd_qp_attr *mstr_qp = NULL;
  for (per_worker = 0; per_worker < NUM_WORKERS; per_worker++) {
    sprintf(mem_conn_qp_name, "qp-worker-%d-memory-%d", per_worker, mem_gid);
    mstr_qp = NULL;
    do {
      mstr_qp = hrd_get_published_qp(mem_conn_qp_name);
      if (mstr_qp == NULL) {
        usleep(200000);
      }
    } while (mstr_qp == NULL);
    printf("main: Memory %s found worker! Connecting..\n", mem_conn_qp_name);
    hrd_connect_qp(cb, per_worker, mstr_qp);
    printf("main: Memory %s connect worker.\n", mem_conn_qp_name);
  }
  char shared_memory_name[HRD_QP_NAME_SIZE];
  {
    shared_memory_space = malloc(sizeof(uintptr_t *) * HERD_NUM_KEYS);
    // shared_memory_region = malloc(sizeof(struct ibv_mr *) * HERD_NUM_KEYS);
    backup_memory_space = malloc(sizeof(uintptr_t *) * HERD_NUM_KEYS);
    // backup_memory_region = malloc(sizeof(struct ibv_mr *) * HERD_NUM_KEYS);
    printf("main: Memory %d start publishing %d mr\n", mem_gid, HERD_NUM_KEYS);
    for (i = 0; i <= HERD_NUM_KEYS; i++) {
      struct hrd_qp_attr pub_qp;
      // shared_memory_space[i] = malloc(HERD_SPACE_SIZE);
      shared_memory_space[i] = cb->conn_buf + (HERD_SPACE_SIZE * i);
      // memset(shared_memory_space[i], 0, HERD_SPACE_SIZE);
      // shared_memory_region[i] = ibv_reg_mr(cb->pd, shared_memory_space[i],
      // HERD_SPACE_SIZE,
      // IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_WRITE|IBV_ACCESS_REMOTE_READ|IBV_ACCESS_REMOTE_ATOMIC);
      // backup_memory_space[i] = malloc(HERD_SPACE_SIZE);
      backup_memory_space[i] =
          cb->conn_buf + (HERD_SPACE_SIZE * (i + HERD_NUM_KEYS + 1));
      // memset(backup_memory_space[i], 0, HERD_SPACE_SIZE);
      // backup_memory_region[i] = ibv_reg_mr(cb->pd, backup_memory_space[i],
      // HERD_SPACE_SIZE,
      // IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_WRITE|IBV_ACCESS_REMOTE_READ|IBV_ACCESS_REMOTE_ATOMIC);
      // if(shared_memory_region[i] && backup_memory_region[i])
      if (shared_memory_space[i] && backup_memory_space[i]) {
        memset(shared_memory_name, 0, HRD_QP_NAME_SIZE);
        sprintf(shared_memory_name, "memory-%d-mr-%d", mem_gid, i);
        // pub_qp.buf_addr = (uintptr_t)shared_memory_region[i]->addr;
        // pub_qp.rkey = shared_memory_region[i]->rkey;
        pub_qp.buf_addr = (uintptr_t)shared_memory_space[i];
        pub_qp.rkey = cb->conn_buf_mr->rkey;
        hrd_publish(shared_memory_name, &pub_qp, sizeof(struct hrd_qp_attr));

        memset(shared_memory_name, 0, HRD_QP_NAME_SIZE);
        sprintf(shared_memory_name, "backupmemory-%d-mr-%d", mem_gid, i);
        // pub_qp.buf_addr = (uintptr_t)backup_memory_region[i]->addr;
        // pub_qp.rkey = backup_memory_region[i]->rkey;
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
#ifdef MICA_IF_TEST_LATENCY
  printf("wakeup client now\n");
  sleep(5);
#endif
  for (per_client = 0; per_client < NUM_CLIENTS; per_client++) {
    sprintf(mem_conn_qp_name, "qp-client-%d-memory-%d", per_client, mem_gid);
    printf("main: Memory %d looks for client %d %s..\n", mem_gid, per_client,
           mem_conn_qp_name);
    mstr_qp = NULL;
    do {
      mstr_qp = hrd_get_published_qp(mem_conn_qp_name);
      if (mstr_qp == NULL) {
        usleep(200000);
      }
    } while (mstr_qp == NULL);
    printf("main: Memory %s found client! Connecting..\n", mem_conn_qp_name);
    hrd_connect_qp(cb, per_client + NUM_WORKERS, mstr_qp);
    printf("main: Memory %s connect client.\n", mem_conn_qp_name);
  }
  char memory_ready_name[HRD_QP_NAME_SIZE] = {};
  sprintf(memory_ready_name, "memory-%d-ready", mem_gid);
  hrd_publish_ready(memory_ready_name);
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
