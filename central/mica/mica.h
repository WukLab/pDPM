#include <stdint.h>
#include "city.h"
#include "../libhrd/hrd.h"

#define MITSUME_MICA_WINDOW_SIZE 1
/*
 * The polling logic in HERD requires the following:
 * 1. 0 < MICA_OP_GET < MICA_OP_PUT < HERD_OP_GET < HERD_OP_PUT
 * 2. HERD_OP_GET = MICA_OP_GET + HERD_MICA_OFFSET
 * 3. HERD_OP_PUT = MICA_OP_PUT + HERD_MICA_OFFSET
 *
 * This allows us to detect HERD requests by checking if the request region
 * opcode is more than MICA_OP_PUT. And then we can convert a HERD opcode to
 * a MICA opcode by subtracting HERD_MICA_OFFSET from it.
 */
#define MICA_OP_GET 111
#define MICA_OP_PUT 112
#define MICA_MAX_BATCH_SIZE 32

#define MICA_RESP_PUT_SUCCESS 113
#define MICA_RESP_GET_SUCCESS 114
#define MICA_RESP_GET_FAIL 115

#define MITSUME_UPDATE_PERCENTAGE 0
#define MICA_NUM_MEMORY 4
#define MICA_NUM_REPLICATION 3

#define MICA_TEST_READ 0
#define MICA_TEST_WRITE 1
//#define MICA_IF_TEST_LATENCY
//#define MICA_IF_TEST_LATENCY_MODE MICA_TEST_WRITE
//#define MICA_READ_VERIFICATION
//[CAUTION] remember to disable this during YCSB test

#define MICA_UNDOLOG_MODE 0

#define MICA_HERD_VALUE_SIZE 1024
/* Ensure that a mica_op is cacheline aligned */
#define MICA_MAX_VALUE \
  (1078 - (sizeof(struct mica_key) + sizeof(uint8_t) + sizeof(uint32_t)))
  //((MICA_HERD_VALUE_SIZE+54) - (sizeof(struct mica_key) + sizeof(uint8_t) + sizeof(uint32_t)))
//118 for 64
//2044 for 2K experiment
//1078 for other experiments
//
//latency experiment
//182 for 128
//310 for 256
//566 for 512
//1078 for 1024
//2102 for 2048
//4026 for 3972
#define MICA_LOG_BITS 40

#define MICA_INDEX_SHM_KEY 3185
#define MICA_LOG_SHM_KEY 4185

//#define MICA_USE_CACHE_QUEUE
#define MICA_CACHE_QUEUE_LENGTH 100000
#define MICA_BUCKET 100
#define MITSUME_NOT_IN_QUEUE 3
#define MITSUME_IN_QUEUE 4

/*
 * Debug values:
 * 0: No safety checks on fast path
 * 1: Sanity checks for arguments
 * 2: Pretty print GET/PUT operations
 */
#define MICA_DEBUG 0

struct mica_resp {
  uint8_t type;
  uint32_t val_len;
  uint16_t unused[1]; /* Make val_ptr 8-byte aligned */
  uint8_t unused2[1]; /* Make val_ptr 8-byte aligned */
  uint8_t* val_ptr;
};

/* Fixed-size 16 byte keys */
struct mica_key {
  unsigned long long __unused : 64;
  unsigned int bkt : 32;
  unsigned int server : 16;
  unsigned int tag : 16;
};

struct mica_op {
  struct mica_key key; /* This must be the 1st field and 16B aligned */
  uint8_t opcode;
  uint32_t val_len;
  uint8_t value[MICA_MAX_VALUE];
};

struct mica_slot {
  uint32_t in_use : 1;
  uint32_t tag : (64 - MICA_LOG_BITS - 1);
  uint64_t offset : MICA_LOG_BITS;
};

struct mica_bkt {
  struct mica_slot slots[8];
};

struct mica_kv {
  struct mica_bkt* ht_index;
  uint8_t* ht_log;

  /* Metadata */
  int instance_id; /* ID of this MICA instance. Used for shm keys */
  int node_id;

  int num_bkts; /* Number of buckets requested by user */
  int bkt_mask; /* Mask down from a mica_key's @bkt to a bucket */

  uint64_t log_cap;  /* Capacity of circular log in bytes */
  uint64_t log_mask; /* Mask down from a slot's @offset to a log offset */

  /* State */
  uint64_t log_head;

  /* Stats */
  long long num_get_op;          /* Number of GET requests executed */
  long long num_get_fail;        /* Number of GET requests failed */
  long long num_put_op;          /* Number of PUT requests executed */
  long long num_index_evictions; /* Number of entries evicted from index */
};

void mica_init(struct mica_kv* kv, int instance_id, int node_id, int num_bkts,
               int log_cap);

/* Single-key INSERT */
void mica_insert_one(struct mica_kv* kv, struct mica_op* op,
                     struct mica_resp* res);

/* Batched operation. PUTs can resolve to UPDATE or INSERT */
void mica_batch_op(struct mica_kv* kv, int n, struct mica_op** op,
                   struct mica_resp* resp, struct hrd_ctrl_blk** mem_cb,
                   void **mitsume_space_list, struct ibv_mr **mitsume_mr_list,
                   struct hrd_qp_attr ***mr_qp_list, struct hrd_qp_attr ***backupmr_qp_list, 
                   int wrkr_lid, long long *send_traffic, long long *recv_traffic);
/* Helpers */
uint128* mica_gen_keys(int n);
void mica_populate_fixed_len(struct mica_kv* kv, int n, int val_len);

/* Debug functions */
void mica_print_bucket(struct mica_kv* kv, int bkt_idx);
void mica_print_op(struct mica_op* op);
