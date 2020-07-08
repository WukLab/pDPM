#ifndef RSEC_STRUCT_HEADER
#define RSEC_STRUCT_HEADER

#include "mitsume_macro.h"
#include "mitsume_parameter.h"
#include "third_party_lru.h"
#include <infiniband/verbs.h>

#include <atomic>
#include <deque>
#include <mutex>
#include <queue>
#include <unordered_map>
using namespace std;

// Memcached
#define RSEC_MAX_QP_NAME 256
#define RSEC_RESERVED_NAME_PREFIX "__P15_RESERVED_NAME_PREFIX"

#define RSEC_CQ_DEPTH 2048

#include <boost/coroutine/all.hpp>
#include <boost/thread/shared_mutex.hpp>

using namespace boost::coroutines;
typedef symmetric_coroutine<void>::call_type coro_call_t;
typedef symmetric_coroutine<void>::yield_type coro_yield_t;

enum MITSUME_notify { MITSUME_SUCCESS = 0, MITSUME_ERROR = -1 };

struct return_int {
  long int first;
  long int last;
  long int index_distance;
  long int real_distance;
};

struct ib_qp_attr {
  char name[RSEC_MAX_QP_NAME];

  /* Info about the RDMA buffer associated with this QP */
  uint64_t buf_addr;
  uint32_t buf_size;
  uint32_t rkey;
  int sl;

  int lid;
  int qpn;

  union ibv_gid remote_gid;
};

struct ib_mr_attr {
  uint64_t addr;
  uint32_t rkey;
  short machine_id;
};

typedef struct ib_mr_attr ptr_attr;

struct configuration_params {
  int global_thread_id;
  int local_thread_id;
  int base_port_index;
  int num_servers;
  int num_clients;
  int num_memorys;
  int is_master;
  int machine_id;
  int total_threads;
  int device_id;
  int num_loopback;
};
struct P15_mr_reply {
  void *addr;
  size_t length;
  uint32_t read_rkey;
  uint32_t write_rkey;
  uint32_t memnode;
  uint32_t memid;
};

struct P15_request {
  int request_type;
  // size_t		length;
  uint32_t ret_rkey;
  uint64_t ret_addr;
  uint32_t ask_memnode;
  uint32_t ask_memid;
};

/*
struct P15_message_frame{
        int src;
        int dst;
        volatile int union_type;
        union
        {
                char msg[32];
                struct P15_mr_reply reply;
                struct P15_request request;
        }content;
};
*/
struct P15_message_frame {
  char msg[1 << 15];
};

struct P15_ptr {
  uint16_t memnode : 7;
  uint16_t memid : 9;
  uint64_t offset : 48;
};

struct P15_header {
  uint32_t version;
  volatile uint64_t name;
};

struct P15_metadata {
  struct P15_ptr nextptr; // This points to the next metadata
  struct P15_header header;
  struct P15_ptr dataptr; // THis points to the data
};

struct P15_hashentry {
  uint32_t read_rkey;
  uint32_t write_rkey;
  void *addr;
  uint32_t memnode;
  uint32_t memid;
  uint32_t length;
};

struct mitsume_ptr {
  uint64_t pointer;
};

struct mitsume_shortcut {
  struct mitsume_ptr shortcut_ptr[MITSUME_MAX_REPLICATION];
};

struct ib_inf {

  int local_id; /* Local ID on the machine this process runs on */
  int global_machines;
  int local_threads;

  /* Info about the device/port to use for this control block */
  struct ibv_context *ctx;
  int port_index;   /* User-supplied. 0-based across all devices */
  int device_id;    /* Resovled by libhrd from @port_index */
  int dev_port_id;  /* 1-based within dev @device_id. Resolved by libhrd */
  int numa_node_id; /* NUMA node id */

  struct ibv_pd *pd; /* A protection domain for this control block */

  int role; // SERVER, CLIENT and MEMORY

  int num_servers;
  int num_clients;
  int num_memorys;

  /* Connected QPs */
  int num_rc_qp_to_server;
  int num_rc_qp_to_client;
  int num_rc_qp_to_memory;
  int num_local_rcqps;
  int num_global_rcqps;
  struct ibv_qp **conn_qp;
  struct ibv_cq **conn_cq, *server_recv_cq;
  struct ib_qp_attr **all_rcqps;

  uint64_t *rcqp_buf;
  struct ibv_mr **rcqp_buf_mr;

  volatile uint8_t *conn_buf;
  uint64_t conn_buf_size;
  struct ibv_mr *conn_buf_mr;

  /* Datagram QPs */
  struct ibv_qp **dgram_qp;
  struct ibv_cq **dgram_send_cq, **dgram_recv_cq;
  struct ib_qp_attr **all_udqps;
  struct ibv_ah **dgram_ah;
  int num_local_udqps;
  int num_global_udqps;
  void ***dgram_buf; /* A buffer for RECVs on dgram QPs */
  struct ibv_mr ***dgram_buf_mr;
  int dgram_buf_size;
  // int dgram_buf_shm_key;
  struct ibv_wc *wc; /* Array of work completions */

  /* loopback QPs */
  struct ibv_qp **loopback_in_qp;
  struct ibv_qp **loopback_out_qp;
  struct ibv_cq **loopback_cq;
  struct ib_qp_attr *loopback_in_qp_attr;
  struct ib_qp_attr *loopback_out_qp_attr;
  int num_loopback;

  uint64_t *ud_qp_counter;
  uint64_t *rc_qp_counter;

  uint64_t local_memid;

  // GHashTable *mr_hash_table;
  // GHashTable *file_hash_table;
  pthread_mutex_t hash_lock;

  union ibv_gid local_gid;
};

struct thread_local_inf {
  int thread_id;
  queue<uint64_t> queue_wr_id;
  mutex queue_wr_id_lock;
  struct ibv_mr *output_mr[MITSUME_CLT_COROUTINE_NUMBER];
  struct mitsume_msg *output_space[MITSUME_CLT_COROUTINE_NUMBER];
  struct ibv_mr *input_mr[MITSUME_CLT_COROUTINE_NUMBER];
  struct mitsume_msg *input_space[MITSUME_CLT_COROUTINE_NUMBER];

  void *user_input_space[MITSUME_CLT_COROUTINE_NUMBER];
  struct ibv_mr *user_input_mr[MITSUME_CLT_COROUTINE_NUMBER];
  void *user_output_space[MITSUME_CLT_COROUTINE_NUMBER];
  struct ibv_mr *user_output_mr[MITSUME_CLT_COROUTINE_NUMBER];

  struct mitsume_ptr
      meshptr_list[MITSUME_CLT_COROUTINE_NUMBER]
                  [MITSUME_MAX_REPLICATION * MITSUME_MAX_REPLICATION];
  struct ibv_mr meshptr_mr[MITSUME_CLT_COROUTINE_NUMBER];
  struct ibv_mr *ori_meshptr_mr;

  struct mitsume_ptr chase_empty_list[MITSUME_CLT_COROUTINE_NUMBER]
                                     [MITSUME_MAX_REPLICATION];
  struct ibv_mr chase_empty_mr[MITSUME_CLT_COROUTINE_NUMBER];
  struct ibv_mr *ori_chase_empty_mr;

  uint64_t crc_base[MITSUME_CLT_COROUTINE_NUMBER][MITSUME_MAX_REPLICATION];
  struct ibv_mr crc_mr[MITSUME_CLT_COROUTINE_NUMBER];
  struct ibv_mr *ori_crc_mr;

  struct mitsume_shortcut chasing_shortcut[MITSUME_CLT_COROUTINE_NUMBER];
  struct ibv_mr chasing_shortcut_mr[MITSUME_CLT_COROUTINE_NUMBER];
  struct ibv_mr *ori_chasing_shortcut_mr;

  struct mitsume_ptr write_checking_ptr[MITSUME_CLT_COROUTINE_NUMBER]
                                       [MITSUME_MAX_REPLICATION];
  struct ibv_mr write_checking_ptr_mr[MITSUME_CLT_COROUTINE_NUMBER];
  struct ibv_mr *ori_write_checking_ptr_mr;

  void *empty_base;
  struct ibv_mr *empty_mr;

  queue<int> coro_queue;
  coro_call_t coro_arr[MITSUME_CLT_COROUTINE_NUMBER];

  // input for sending out local data
  // output for getting incoming data
};

struct rsec_malloc_metadata {
  void *addr;
  unsigned long size;
};
//////////////////////////////////////////////////////

typedef uint64_t mitsume_key;
struct mitsume_xact_set {
  mitsume_key key;
  uintptr_t addr;
  int mode;
  uint32_t request_size;
};

/*struct mitsume_xact_base
{
        struct mitsume_xact_set xact_addr[MITSUME_TOOL_MAX_XACT_SIZE];
        int xact_num;
};*/

enum mitsume_xact_request_type {
  MITSUME_READ = 8,
  MITSUME_WRITE = 9,
  MITSUME_READ_WRITE = 10,
  MITSUME_CHASE = 11
};

struct mitsume_misc_request_struct {
  int request_type;
  mitsume_key key;
  int from_version;
  int until_version;
  int gc_count;
  int new_replication_factor;
};

enum mitsume_con_management_type {
  MITSUME_MANAGEMENT_SUCCESS = 125,
  MITSUME_MANAGEMENT_FAIL = 126,
  MITSUME_CON_MANAGEMENT_REQUEST_GC_RECYCLE = 127,
  MITSUME_GC_OLD_VERSION_REQUEST = 128,
  MITSUME_GC_CHANGE_REPLICATION_FACTOR_REQUEST = 129,
  MITSUME_COMPACT_VERSION_REQUEST = 130,
  MITSUME_MIGRATION_VERSION_REQUEST = 131
};

struct mitsume_xactarea {
  uint64_t option;
};

struct mitsume_gc_entry {
  struct mitsume_ptr old_ptr[MITSUME_MAX_REPLICATION];
  // this entry is used with two different ways
  // 1. it would be used as regular gc_entry to contains gc-information
  // 2. it would be used as a pointer to save update request which will be used
  // to send-out
  struct mitsume_ptr new_ptr[MITSUME_MAX_REPLICATION];
  mitsume_key key;
  uint32_t replication_factor;
};

struct mitsume_gc_hashed_entry {
  struct mitsume_gc_entry gc_entry;
  //%struct hlist_node hlist;
  //%struct list_head list;
  int submitted_epoch;
  // this entry is used with two different ways
  // 1. used as a communication flag between allocator and gc thread during
  // recycling
  // 2. it would be used by epoch thread to flag the current recycling epoch
};

enum mitsume_gc_hashed_entry_submitted_epoch_flag {
  MITSUME_GC_SUBMIT_FORGE,
  MITSUME_GC_SUBMIT_CREATE_PASS,
  MITSUME_GC_SUBMIT_UPDATE,
  MITSUME_GC_SUBMIT_REGULAR,
  MITSUME_GC_SUBMIT_MAX,
  MITSUME_GC_SUBMIT_NAMDB
};

struct mitsume_gc_single_entry {
  struct mitsume_ptr old_ptr;
};

struct mitsume_gc_single_hashed_entry {
  struct mitsume_gc_single_entry gc_entry;
  //%struct list_head list;
  int submitted_epoch;
};

struct mitsume_gc_thread_request {
  struct mitsume_gc_entry gc_entry;
  struct mitsume_ptr shortcut_ptr;
  int gc_mode;
  //%struct list_head list;

  int target_controller;
};

struct mitsume_allocator_entry {
  struct mitsume_ptr ptr;
  /*mitsume_allocator_entry(struct mitsume_allocator_entry *input_entry)
  {
      ptr.pointer = input_entry->ptr.pointer;
  }
  mitsume_allocator_entry(){}*/
  //%struct list_head list;
};

struct mitsume_poll {
  uint64_t post_send_wr_id;
  uint64_t post_recv_wr_id;
  int fake_coro;
  mitsume_poll(uint64_t send_id, uint64_t recv_id, int coro) {
    post_send_wr_id = send_id;
    post_recv_wr_id = recv_id;
    fake_coro = coro;
  }
};

struct mitsume_allocator_metadata {
  queue<struct mitsume_poll *> poller_queue;
  queue<int> fake_coro_queue;
  //%spinlock_t **allocator_lock_branch;
  mutex allocator_lock_branch[MITSUME_NUM_REPLICATION_BUCKET]
                             [MITSUME_CON_ALLOCATOR_SLAB_NUMBER];
  // struct mitsume_allocator_entry **allocator_node_branch;
  deque<uint64_t> allocator_node_branch[MITSUME_NUM_REPLICATION_BUCKET]
                                       [MITSUME_CON_ALLOCATOR_SLAB_NUMBER];

  int port;
  int thread_id;
  int node_id;
  struct mitsume_ctx_con *local_ctx_con;

  //%struct list_head *internal_gc_buffer;
  // struct mitsume_gc_hashed_entry internal_gc_buffer;
  queue<struct mitsume_gc_hashed_entry *> internal_gc_buffer;
  uint32_t gc_task_counter;

  ////////
  struct thread_local_inf *local_inf;
};

struct mitsume_controller_gc_thread_metadata {
  int gc_thread_id;
  struct mitsume_ctx_con *local_ctx_con;
  uint64_t gc_allocator_counter;
  struct thread_local_inf *local_inf;
};

struct mitsume_shortcut_entry {
  struct mitsume_ptr ptr;
  /*mitsume_shortcut_entry(struct mitsume_shortcut_entry *input_entry)
  {
      ptr.pointer = input_entry->ptr.pointer;
  }
  mitsume_shortcut_entry(){}*/
  //%struct list_head list;
};

struct mitsume_msg_gc_control {
  int target_thread;
  int target_bucket;
  int target_num;
  int target_size;
  int target_policy;
};

struct mitsume_con_management_request {
  int request_type;
  int target_node;
  union {
    struct mitsume_msg_gc_control gc_control_target;
    struct mitsume_misc_request_struct msg_misc_request;
  } content;
  uintptr_t received_descriptor;
  //%struct list_head list;
};

struct mitsume_controller_epoch_thread_metadata {
  thread_local_inf *local_inf;
  struct mitsume_ctx_con *local_ctx_con;
};

struct mitsume_ctx_con {
  ////////////////////////////
  queue<struct controller_poller *> poller_queue;
  mutex poller_queue_lock;
  ptr_attr *all_shortcut_attr;
  ptr_attr *all_memory_attr;
  ptr_attr *all_lh_attr;
  struct mitsume_allocator_metadata
      thread_metadata[MITSUME_CON_ALLOCATOR_THREAD_NUMBER];
  int controller_id;
  int machine_id;

  // recv related
  ptr_attr **per_qp_mr_attr_list;
  struct ibv_mr **per_post_recv_mr_list;

  ptr_attr *total_entry_attr; // this is divided from all_memory_attr based on
                              // different size

  // queue<struct mitsume_shortcut_entry> *shortcut_lh_list;
  mutex shortcut_lock;
  queue<uint64_t> shortcut_lh_list;

  struct configuration_params *input_arg;
  struct ib_inf *ib_ctx;
  queue<struct mitsume_gc_single_hashed_entry *> *public_epoch_list;
  mutex public_epoch_list_lock;

  pthread_t thread_allocator[MITSUME_CON_ALLOCATOR_THREAD_NUMBER];

  struct mitsume_controller_gc_thread_metadata
      gc_thread_metadata[MITSUME_CON_GC_THREAD_NUMBER];
  pthread_t gc_thread[MITSUME_CON_GC_THREAD_NUMBER];
  queue<struct mitsume_gc_hashed_entry *>
      public_gc_bucket[MITSUME_CON_GC_THREAD_NUMBER];
  mutex gc_bucket_lock[MITSUME_CON_GC_THREAD_NUMBER];

  pthread_t epoch_thread;
  struct mitsume_controller_epoch_thread_metadata epoch_thread_metadata;
  mutex allocator_to_epoch_thread_pipe_lock;
  queue<struct mitsume_msg *> allocator_to_epoch_thread_pipe;

  ////////////////////////////

  uint64_t control;
  uint64_t *lh_holder; // number of lh
  uint64_t *shortcut_lh_holder;
  uint64_t *xactarea_lh_holder;
  // struct mitsume_shortcut_entry shortcut_lh_list;

  //%spinlock_t shortcut_lock;
  int memnode_list_num; // How many memory nodes are using
  int *memnode_list;    // List of memory nodes

  int node_id;

  //%struct task_struct **thread_distribute_entry;

  //%struct task_struct **gc_thread;

  //%struct task_struct *management_thread;
  //%struct list_head *management_request_list;
  //%spinlock_t management_request_list_lock;

#ifdef MITSUME_GC_NAMDB_DESIGN
//%struct task_struct *namdb_gc_thread;
//%struct list_head *namdb_gc_request_list;
//%spinlock_t namdb_gc_request_list_lock;
//%uint64_t *namdb_overflow_region_lh_holder;
//%atomic_t namdb_overflow_region_rr_number;
#endif

  //%struct list_head *public_epoch_list;
  //%spinlock_t public_epoch_list_lock;
  //%atomic_t public_epoch_list_num;
  int gc_current_epoch;

  uint64_t read_bucket_counter[MITSUME_NUM_REPLICATION_BUCKET];
  uint64_t write_bucket_counter[MITSUME_NUM_REPLICATION_BUCKET];
};

struct mitsume_namdb_version_to_gc_msg {
  long long version_mover_time;
  struct mitsume_ptr old_ptr;
  uint64_t overflow_region;
  uint64_t operation_size;
  //%struct list_head list;
};

struct mitsume_consumer_entry_internal {
  struct mitsume_ptr ptr;
  //%struct list_head list;
};

struct mitsume_consumer_entry {
  struct mitsume_ptr ptr;
  uint64_t size;
};

struct mitsume_consumer_metadata {
  // deque<struct mitsume_consumer_entry_internal>
  // consumer_node_branch[MITSUME_NUM_REPLICATION_BUCKET][MITSUME_CON_ALLOCATOR_SLAB_NUMBER];
  queue<uint64_t> consumer_node_branch[MITSUME_NUM_REPLICATION_BUCKET]
                                      [MITSUME_CON_ALLOCATOR_SLAB_NUMBER];

  int thread_id;
  int node_id;
  struct mitsume_ctx_clt *local_ctx_clt;

  uint64_t private_alloc_counter;
  uint64_t rr_allocator_counter;
  uint64_t task_allocator_counter[MITSUME_CON_NUM];
  uint64_t private_bucket_counter;

  std::unordered_map<uint64_t, struct mitsume_private_hash_struct *>
      mitsume_private_hash;

  struct thread_local_inf *local_inf;

  mutex current_running_lock[MITSUME_CLT_COROUTINE_NUMBER];
  mutex entry_operating_lock;

  //%spinlock_t current_running_lock;
  //%spinlock_t entry_operating_lock;

  //%DECLARE_HASHTABLE(mitsume_private_hash, MITSUME_TOOL_READER_HASH_BIT);
};

struct mitsume_consumer_gc_shortcut_update_element {
  struct mitsume_ptr shortcut_ptr;
  struct mitsume_shortcut writing_shortcut;
  mitsume_key key;
  int replication_factor;
  uint64_t wr_id;
  //%struct list_head list;
};

struct mitsume_consumer_gc_metadata {
  // struct mitsume_gc_thread_request *per_thread_gc_processing_queue;
  int gc_thread_id;
  struct mitsume_ctx_clt *local_ctx_clt;

  queue<struct mitsume_consumer_gc_shortcut_update_element *>
      internal_shortcut_buffer_list[MITSUME_CON_NUM];
  queue<struct mitsume_consumer_gc_shortcut_update_element *>
      internal_shortcut_send_list[MITSUME_CON_NUM];

  uint64_t gc_allocator_counter;
  struct thread_local_inf *local_inf;
};

struct mitsume_consumer_epoch_metadata {
  struct mitsume_ctx_clt *local_ctx_clt;
  struct thread_local_inf *local_inf;
};

struct mitsume_consumer_stat_metadata {
  struct mitsume_ctx_clt *local_ctx_clt;
  struct thread_local_inf *local_inf;
};

struct mitsume_ctx_clt {
  ////////////////////////////
  ptr_attr *all_shortcut_attr;
  ptr_attr *all_lh_attr;
  ib_inf *ib_ctx;
  int node_id;
  int client_id;

  // recv related
  ptr_attr **per_qp_mr_attr_list;
  struct ibv_mr **per_post_recv_mr_list;

  pthread_t thread_consumer[MITSUME_CON_ALLOCATOR_THREAD_NUMBER];
  struct mitsume_consumer_metadata thread_metadata[MITSUME_CLT_CONSUMER_NUMBER];

  struct mitsume_consumer_gc_metadata
      gc_thread_metadata[MITSUME_CLT_CONSUMER_GC_THREAD_NUMS];
  queue<struct mitsume_gc_thread_request *>
      gc_processing_queue[MITSUME_CLT_CONSUMER_GC_THREAD_NUMS];
  mutex gc_processing_queue_lock[MITSUME_CLT_CONSUMER_GC_THREAD_NUMS];
  pthread_t thread_gc_thread[MITSUME_CLT_CONSUMER_GC_THREAD_NUMS];
  mutex gc_epoch_block_lock;
  pthread_t thread_epoch;

  pthread_t thread_stat;
  struct mitsume_consumer_stat_metadata stat_thread_metadata;
  atomic<long> read_bucket_counter[MITSUME_NUM_REPLICATION_BUCKET];
  atomic<long> write_bucket_counter[MITSUME_NUM_REPLICATION_BUCKET];
  ////////////////////////////

  uint64_t client;
  uint64_t *global_lh_holder;
  uint64_t *global_shortcut_lh_holder;
  //%struct task_struct **thread_consumer;
  // struct mitsume_consumer_metadata *thread_metadata;

  uint64_t *global_xactarea_lh_holder;
  uint64_t *global_available_xactareas;
  int global_available_xactareas_num;

  struct mitsume_consumer_epoch_metadata epoch_thread_metadata;

  int gc_current_epoch;

  //%spinlock_t thread_metadata_occupy_lock;
  int *thread_metadata_occupy_map;

#ifdef MITSUME_ENABLE_FIFO_LRU_QUEUE
  //%struct list_head **hash_table_lru_list;
  //%spinlock_t **hash_table_lru_lock;
  int *hash_table_lru_count;
#endif

  // DECLARE_HASHTABLE(MITSUME_TOOL_QUERY_HASHTABLE,
  // MITSUME_TOOL_QUERY_HASHTABLE_SIZE_BIT); spinlock_t
  // MITSUME_TOOL_QUERY_HASHTABLE_LOCK[1<<MITSUME_TOOL_QUERY_HASHTABLE_SIZE_BIT];
};
#define MITSUME_DANGEROUS_EPOCH                                                \
  (thread_metadata->local_ctx_clt->gc_current_epoch -                          \
   MITSUME_GC_EPOCH_THRESHOLD)

struct mitsume_xact_record {
  struct mitsume_tool_communication **query_list;
  mitsume_key *key_list;
  int *mode_list;
  struct mitsume_ptr *read_value_list;
  struct mitsume_ptr *guess_value_list;
  struct mitsume_ptr *set_value_list;

  int xactarea_id;
  int xact_num;
  int xact_mode;
  int begin_epoch;
};

enum mitsume_xactarea_flag {
  MITSUME_XACTAREA_SET_READ = 128,
  MITSUME_XACTAREA_SET_OLD = 129,
  MITSUME_XACTAREA_SET_NEW = 130
};

struct mitsume_ctx_mem {
  uint64_t memory;
};

struct mitsume_tool_communication {
  struct mitsume_ptr ptr;
  struct mitsume_ptr replication_ptr[MITSUME_MAX_REPLICATION];
  uint32_t size;
  struct mitsume_ptr shortcut_ptr;
  int replication_factor;
  uint32_t message_size;
  struct mitsume_ptr
      gc_ptr[MITSUME_MAX_REPLICATION]; // this is used for nonlatest&GC in read
                                       // only

  int target_gc_thread;
  int target_gc_controller;

  // variables below are for nonlatest access (pubsub) only
  //========================================================
  int access_once; // this number is used to save one useless tracing
  uint32_t already_read_hop_count; // this number counts the number of read hops
};

struct mitsume_msg_init {
  uint64_t init_number_of_lh;
  uint64_t init_start_lh;
  int available_xactareas_start_num;
  int available_xactareas_num;
};

struct mitsume_msg_entry {
  // uint32_t entry_lh[MITSUME_CLT_CONSUMER_MAX_ENTRY_NUMS];
  // uint64_t entry_offset[MITSUME_CLT_CONSUMER_MAX_ENTRY_NUMS];
  struct mitsume_ptr ptr[MITSUME_CLT_CONSUMER_MAX_ENTRY_NUMS];
  uint32_t entry_size;
  uint32_t entry_number;
  int entry_replication_bucket;
  int already_available_buckets[MITSUME_NUM_REPLICATION_BUCKET];
  // int available_buckets[MITSUME_MAX_REPLICATION];
};

struct mitsume_entry_request {
  uint32_t type;
  mitsume_key key;
  struct mitsume_ptr ptr[MITSUME_MAX_REPLICATION];
  struct mitsume_ptr shortcut_ptr;
  int replication_factor;
  uint32_t version;
  int debug_flag;
};

struct mitsume_gc_request {
  struct mitsume_gc_entry gc_entry[MITSUME_CLT_CONSUMER_MAX_GC_NUMS];
  uint32_t gc_number;
};

struct mitsume_gc_epoch_forward {
  int request_epoch_number;
};

struct mitsume_msg_header {
  int type;
  int src_id;
  int des_id;
  uint32_t thread_id;
  ///////
  ptr_attr reply_attr;
};

struct mitsume_stat_message {
  long int read_bucket_counter[MITSUME_NUM_REPLICATION_BUCKET];
  long int write_bucket_counter[MITSUME_NUM_REPLICATION_BUCKET];
};

struct mitsume_msg {
  struct mitsume_msg_header msg_header; // dont change this position . it has to
                                        // be aligned with mitsume_msg msg_header
  union {
    struct mitsume_msg_init msg_init; // small
    struct mitsume_msg_entry
        msg_entry; // use for allocation and garbage collection
    struct mitsume_entry_request
        msg_entry_request; // use for open specifically //small
    struct mitsume_gc_epoch_forward msg_gc_epoch_forward; // small
    struct mitsume_msg_gc_control msg_gc_control;
    struct mitsume_misc_request_struct
        msg_misc_request; // for misc-usage, such as wrap version, migration
                          // request
    struct mitsume_stat_message msg_stat_message;
    int success_gc_number;
  } content;

  uint64_t option;
  uint64_t end_crc;
};

struct mitsume_large_msg // this is a large message
{
  struct mitsume_msg_header msg_header; // dont change this position . it has to
                                        // be aligned with mitsume_msg msg_header
  union {
    struct mitsume_gc_request msg_gc_request; // large
    struct mitsume_entry_request
        msg_entry_request[MITSUME_GC_MAX_BACK_UPDATE_PER_ENTRY];
  } content;

  uint64_t option;
};

#define MITSUME_MAX_MESSAGE_SIZE                                               \
  (MITSUME_MAX(sizeof(struct mitsume_msg), sizeof(struct mitsume_large_msg)))

struct mitsume_lh_to_node_id_msg {
  int src_id;
  uint32_t allocated_lh_num;
  uint32_t mitsume_lh_to_node_id_mapping;
};

struct mitsume_send_backup_create_request_pass_struct {
  struct mitsume_hash_struct *backup_create_request_content;
  struct mitsume_msg *reply;
  uintptr_t received_descriptor;
  int *target_backup_controller;
};

enum MITSUME_MSG {
  MITSUME_CONTROLLER_INITIALIZATION = 1,
  MITSUME_CONTROLLER_INITIALIZATION_ACK = 2,
  MITSUME_CONTROLLER_ASK_ENTRY = 3,
  MITSUME_CONTROLLER_ASK_ENTRY_ACK = 4,
  MITSUME_ENTRY_REQUEST = 5,
  MITSUME_ENTRY_REQUEST_ACK = 6,
  MITSUME_GC_REQUEST = 7,
  MITSUME_GC_REQUEST_ACK = 8,
  MITSUME_GC_EPOCH_FORWARD = 9,
  MITSUME_GC_EPOCH_FORWARD_ACK = 10,
  MITSUME_GC_RECYCLE_REQUEST = 11,
  MITSUME_GC_RECYCLE_REQUEST_ACK = 12,
  MITSUME_GC_GENERAL_FAULT = 13,
  MITSUME_MISC_REQUEST = 14,
  MITSUME_MISC_REQUEST_ACK = 15,
  MITSUME_MISC_REQUEST_CHANGE_REPLICATION_FACTOR = 16,
  MITSUME_GC_BACKUP_UPDATE_REQUEST = 17,
  MITSUME_GC_BACKUP_UPDATE_REQUEST_ACK = 18,
  MITSUME_STAT_UPDATE = 19,
  MITSUME_STAT_UPDATE_ACK = 20
};

enum MITSUME_MSG_ENTRY_REQUEST_TYPE {
  MITSUME_ENTRY_OPEN = 1,
  MITSUME_ENTRY_QUERY = 2,
  MITSUME_ENTRY_BACKUP_CREATE_REQUEST = 3,
};

enum MITSUME_MSG_OPTION { MITSUME_ACK_FAIL = 1, MITSUME_ACK_SUCCESS = 2 };

struct mitsume_ctx {
  int node_id;
  int role;
  struct mitsume_ctx_con *ctx_con;
  struct mitsume_ctx_clt *ctx_clt;
  struct mitsume_ctx_mem *ctx_mem;
};
typedef struct mitsume_ctx mtc;

enum mitsume_shortcut_chasing_status {
  MITSUME_CHASING_RESULT_INIT = 0,
  MITSUME_SHORTCUT_NEVER_CHASING = 1,
  MITSUME_SHORTCUT_CHASING_ONCE = 2,
  MITSUME_SHORTCUT_OUTDATED = 3,
  MITSUME_HASHTABLE_OUTDATED = 4
};

struct mitsume_hash_struct {
  mitsume_key key;
  // int open_node_id;
  // int open_thread_id;
  int last_epoch;
  int replication_factor;
  uint32_t version;

  struct mitsume_ptr
      ptr[MITSUME_MAX_REPLICATION]; // 0307-modify change to
                                    // ptr[MITSUME_MAX_REPLICATION]
  struct mitsume_ptr shortcut_ptr;
  //%struct hlist_node hlist;

  int target_gc_thread;
  int target_gc_controller;

#ifdef MITSUME_ENABLE_TIME_BASED_CACHE_EVICTION
  long last_jiffies;
#endif

  //%struct list_head list;
  int in_lru;
};

struct mitsume_private_hash_struct {
  mitsume_key key;
  int last_epoch;
  int replication_factor;
  struct mitsume_ptr ptr[MITSUME_MAX_REPLICATION];
  struct mitsume_ptr shortcut_ptr;
  int access_once; // this number is for nonlatest access only which can save
                   // one useless tracing
  uint32_t already_read_hop_count;
  //%struct hlist_node hlist;

  int target_gc_thread;
  int target_gc_controller;
};

/*struct mitsume_header //deprecated after 12/13/17
{
        unsigned int key: 32;
};

struct mitsume_metadata //deprecated after 12/13/17
{
        struct mitsume_ptr next_ptr;
        //struct mitsume_header header;
        //char data; it would follow up with 4B to 1MB data
};*/

enum MITSUME_ALLOC_TYPE {
  MITSUME_ALLOCTYPE_ALLOCATOR_ENTRY = 101,
  MITSUME_ALLOCTYPE_CONSUMER_ENTRY_INTERNAL = 102,
  MITSUME_ALLOCTYPE_CONSUMER_ENTRY = 103,
  MITSUME_ALLOCTYPE_TOOL_COMMUNICATION = 104,
  MITSUME_ALLOCTYPE_HASH_STRUCT = 105,
  MITSUME_ALLOCTYPE_PRIVATE_HASH_STRUCT = 106,
  MITSUME_ALLOCTYPE_GC_THREAD_REQUEST = 107,
  MITSUME_ALLOCTYPE_SHORTCUT_ENTRY = 108,
  MITSUME_ALLOCTYPE_GC_HASHED_ENTRY = 109,
  MITSUME_ALLOCTYPE_GC_SINGLE_HASHED_ENTRY = 110,
  MITSUME_ALLOCTYPE_XACT_RECORD = 111,
  MITSUME_ALLOCTYPE_PTR_REPLICATION = 112,
  MITSUME_ALLOCTYPE_MSG = 113,
  MITSUME_ALLOCTYPE_LARGE_MSG = 114,
  MITSUME_ALLOCTYPE_IB_SGE = 115,
  MITSUME_ALLOCTYPE_CON_MANAGEMENT_REQUEST = 116,
  MITSUME_ALLOCTYPE_KMEMCACHE_REQUEST = 117,
  MITSUME_ALLOCTYPE_GC_SHORTCUT_UPDATE_ELEMENT = 118,

  // namdb_related
  MITSUME_ALLOCTYPE_NAMDB_KMEMCACHE_REQUEST = 201,
  MITSUME_ALLOCTYPE_NAMDB_VERSION_TO_GC_REQUEST = 202
};

#define MITSUME_TOOL_PRINT_POINTER_NULL(entry)                                 \
  mitsume_tool_print_pointer(entry, __func__, __LINE__, 0)

#define MITSUME_TOOL_PRINT_GC_POINTER_NULL(entry, entry_b)                     \
  mitsume_tool_print_gc_pointer(entry, entry_b, __func__, __LINE__, 0)

#define MITSUME_TOOL_PRINT_GC_POINTER_KEY(entry, entry_b, key)                 \
  mitsume_tool_print_gc_pointer(entry, entry_b, __func__, __LINE__, key)

#define MITSUME_TOOL_PRINT_POINTER_KEY(entry, shortcut, key)                   \
  mitsume_tool_print_pointer_key(entry, shortcut, key, __LINE__, __func__)

#define MITSUME_TOOL_PRINT_MSG_HEADER(entry)                                   \
  mitsume_tool_print_message_header(entry, __func__, __LINE__, 0)

#define MITSUME_TOOL_PRINT_POINTER_KEY_REPLICATION(entry, factor, key)         \
  mitsume_tool_print_pointer_key_replication(entry, factor, key, __LINE__,     \
                                             __func__)

#define MITSUME_STRUCT_CHECKNULL_PTR_REPLICATION(src, replication_factor)      \
  mitsume_struct_checknull_ptr_replication(src, replication_factor, __LINE__,  \
                                           __func__)

#define MITSUME_STRUCT_CHECKEQUAL_PTR_REPLICATION(des, src,                    \
                                                  replication_factor)          \
  mitsume_struct_checkequal_ptr_replication(des, src, replication_factor,      \
                                            __LINE__, __func__)

uint64_t mitsume_struct_set_pointer(uint64_t lh, uint32_t next_version,
                                    uint32_t entry_version, uint32_t area,
                                    uint32_t option);
void mitsume_tool_print_pointer(struct mitsume_ptr *entry,
                                const char *function_name, int function_line,
                                uint64_t key);
void mitsume_tool_print_message_header(struct mitsume_msg *input_msg,
                                       const char *function_name,
                                       int function_line, int thread_id);
void mitsume_tool_print_pointer_key_replication(struct mitsume_ptr *entry,
                                                uint32_t replication_factor,
                                                mitsume_key key,
                                                int function_line,
                                                const char *function_name);
int mitsume_struct_copy_ptr_replication(struct mitsume_ptr *des,
                                        struct mitsume_ptr *src,
                                        int replication_factor);
int mitsume_struct_setup_entry_request(struct mitsume_msg *input, int main_type,
                                       int src_id, int des_id, int thread_id,
                                       uint32_t type, mitsume_key key,
                                       struct mitsume_ptr *entry_ptr,
                                       struct mitsume_ptr *shortcut_ptr,
                                       int replication_factor);
int mitsume_struct_set_ptr_replication(struct mitsume_ptr *des,
                                       int replication_factor, uint64_t set);
uint64_t mitsume_struct_move_entry_pointer_to_next_pointer(uint64_t input);
int mitsume_struct_move_replication_entry_pointer_to_next_pointer(
    struct mitsume_ptr *input_ptr, uint32_t replication_factor);
uint64_t mitsume_struct_move_next_pointer_to_entry_pointer(uint64_t input);
int mitsume_struct_move_replication_next_pointer_to_entry_pointer(
    struct mitsume_ptr *input_ptr, uint32_t replication_factor);
void mitsume_tool_print_pointer_key(struct mitsume_ptr *entry,
                                    struct mitsume_ptr *shortcut,
                                    mitsume_key key, int function_line,
                                    const char *function_name);
int mitsume_tool_setup_meshptr_pointer_header_by_newspace(
    struct mitsume_ptr **meshptr, struct mitsume_tool_communication *query,
    struct mitsume_tool_communication *newspace, uint32_t committed_flag);
int mitsume_struct_checknull_ptr_replication(struct mitsume_ptr *src,
                                             int replication_factor,
                                             int function_line,
                                             const char *function_name);
int mitsume_struct_checkequal_ptr_replication(struct mitsume_ptr *des,
                                              struct mitsume_ptr *src,
                                              int replication_factor,
                                              int function_line,
                                              const char *function_name);
void mitsume_tool_print_gc_pointer(struct mitsume_ptr *entry,
                                   struct mitsume_ptr *gc_entry,
                                   const char *function_name, int function_line,
                                   uint64_t key);
#endif
