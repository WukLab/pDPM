#ifndef RFTL_HEADER_FILE
#define RFTL_HEADER_FILE

#include "ibsetup.h"
#include "memcached.h"
#include "mitsume_macro.h"
#include "mitsume_parameter.h"
#include "mitsume_stat.h"
#include "mitsume_struct.h"
#include <assert.h>
#include <infiniband/verbs.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include <numa.h>

#include <chrono>
#include <iostream>
#include <string>
//#include "rsec.h"
#define OFFSET (0x0)
//#include<glib.h>

#include <queue>
using namespace std;

//#define P15_MAXSIZE_PERBLOCK 128
//#define P15_NUM_METADATA_PERALLOC 1024
//#define P15_NUM_DATA_PERALLOC 1024

#define SERVER 128
#define CLIENT 129
#define MEMORY 130
#define UD_SHIFT_SIZE 40

#define P15_ALLOC_SIZE 1024

// IB-related def

#define FORCE_POLL 1

#define P15_MAX_MACHINES 64
#define P15_MAX_ALLOC_SIZE 2147483648

#define P15_ID_SHIFT 10

#define P15_PARALLEL_RC_QPS 8
#define P15_PARALLEL_UD_QPS 1
#define P15_MAX_INLINE 256

#define P15_UD_CQ_WINDOWSIZE 32 // should be 1/4 of the depth

#define P15_UD_QKEY 0x7777
#define P15_UD_PSN 3185
#define P15_UD_SL 0

#define P15_RC_CQ_DEPTH 1000
#define P15_RC_CQ_WINDOWSIZE 32 // should be 1/4 of the depth
#define P15_RC_SL 0

#define P15_UD_POLL_NUM 1

#define P15_UD_POST_RECV_ID_SHIFT 48
#define P15_UD_POST_RECV_GET_ADDR 0x0000ffffffffffff
#define P15_UD_POST_RECV_GET_ID 0xffff000000000000

#define P15_THREAD_SEND_BUF_NUM 16
#define P15_THREAD_RECV_BUF_NUM 16

#define P15_PTR_MEMNODE_MASK 0x7f
#define P15_PTR_MEMID_MASK 0x1ff
#define P15_PTR_OFFSET_MASK 0xffffffffffff

enum message_type {
  P15_MESSAGE_ERROR,
  P15_BENCHMARK_RPC_TEST,
  P15_OWN_NEW_METADATA_MR_CLIENT_ISSUE,
  P15_OWN_NEW_METADATA_MR_SERVER_ISSUE,
  P15_OWN_NEW_DATA_MR_CLIENT_ISSUE,
  P15_OWN_NEW_DATA_MR_SERVER_ISSUE,
  P15_ASK_METADATA_MR_CLIENT_ISSUE,
  P15_ASK_DATA_MR_CLIENT_ISSUE,
  P15_READ_INVALID
};

enum union_type {
  P15_UNION_EMPTY,
  P15_UNION_MSG,
  P15_UNION_MR_REPLY,
  P15_UNION_REQUEST
};

enum p15_use_buffer {
  P15_BUFF_SEND_MSG,
  P15_BUFF_SGE,
  P15_BUFF_ASK,
  P15_BUFF_OPERATION,
  P15_BUFF_CMPSWP,
  P15_MAX_BUFF
};

enum MITSUME_ROLE {
  MITSUME_IS_CONTROLLER = 1,
  MITSUME_IS_CLIENT = 2,
  MITSUME_IS_MEMORY = 3
};

enum MITSUME_LOCAL_QUERY {
  MITSUME_QUERY_FROM_LOCAL = 1,
  MITSUME_QUERY_FROM_REMOTE = 2,
  MITSUME_QUERY_REQUERY = 3,
  MITSUME_QUERY_FAIL = -1,
  MITSUME_CHASING_FAIL = -2
};

enum MITSUME_LOCAL_ALLOCATE {
  MITSUME_ALLOCATE_FROM_LOCAL = 1,
  MITSUME_ALLOCATE_FROM_REMOTE = 2,
  MITSUME_ALLOCATE_FAIL = -1
};

void die_printf(const char *fmt, ...);
void dbg_printf(const char *fmt, ...);

void *mitsume_malloc(unsigned long size);
int get_memory_id(struct configuration_params *input_arg);
int get_client_id(struct configuration_params *input_arg);
int get_client_id(struct mitsume_ctx_clt *client_ctx);
int get_controller_id(struct configuration_params *input_arg);
struct thread_local_inf *mitsume_local_thread_setup(struct ib_inf *inf,
                                                    int local_thread_id);
uint64_t mitsume_local_thread_get_wr_id(struct thread_local_inf *local_inf);
void mitsume_local_thread_put_wr_id(struct thread_local_inf *local_inf,
                                    uint64_t wr_id);
uint64_t
mitsume_local_thread_get_wr_id_lock(struct thread_local_inf *local_inf);
void mitsume_local_thread_put_wr_id_lock(struct thread_local_inf *local_inf,
                                         uint64_t wr_id);

uint32_t hash_min(uint64_t a, unsigned int bits);

void mitsume_sync_barrier_global(void);
void mitsume_sync_barrier_local(void);
int mitsume_sync_barrier_init(int num_send_thread, int local_client_id,
                              int num_global_client);

void schedule(void);
int stick_this_thread_to_core(int core_id);
void yield_to_another_coro(struct thread_local_inf *local_inf, int coro_id,
                           coro_yield_t &yield);

chrono::microseconds get_current_us(void);
chrono::nanoseconds get_current_ns(void);
chrono::milliseconds get_current_ms(void);
#endif
