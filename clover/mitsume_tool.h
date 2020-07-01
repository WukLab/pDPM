
#ifndef MITSUME_TOOL
#define MITSUME_TOOL
#include "mitsume.h"
#include "mitsume_clt_tool.h"
#include "mitsume_tool_cache.h"
#include "mitsume_tool_gc.h"
#include "third_party_lru.h"

#define MITSUME_TOOL_TESTSIZE 256
#define MITSUME_TOOL_QUERY_FORCE_LOCAL 0x1
#define MITSUME_TOOL_QUERY_FORCE_REMOTE 0x2
#define MITSUME_TOOL_QUERY_PRIVATE_HASH 0x4
#define MITSUME_TOOL_QUERY_WITHOUT_UPDATE 0x8
#define MITSUME_TOOL_QUERY_CHASING_HELP 0x10

#define MITSUME_TOOL_FLAG_NONLATEST 0x1
#define MITSUME_TOOL_FLAG_GC 0x2
#define MITSUME_TOOL_FLAG_BLOCK_UNCOMMITTED 0x4

#define MITSUME_TOOL_CHASING_NONLATEST 0x1
#define MITSUME_TOOL_CHASING_BLOCK 0x2

#define MITSUME_TOOL_KVSTORE_READ MITSUME_TOOL_FLAG_BLOCK_UNCOMMITTED
#define MITSUME_TOOL_KVSTORE_WRITE MITSUME_TOOL_FLAG_GC
#define MITSUME_TOOL_MESSAGE_READ                                              \
  MITSUME_TOOL_FLAG_GC | MITSUME_TOOL_FLAG_NONLATEST
#define MITSUME_TOOL_MESSAGE_WRITE 0x0
#define MITSUME_TOOL_PUBSUB_READ MITSUME_TOOL_FLAG_NONLATEST
#define MITSUME_TOOL_PUBSUB_WRITE MITSUME_TOOL_FLAG_NONLATEST

#define MITSUME_TOOL_WITHOUT_PATCH 0
#define MITSUME_TOOL_WITH_PATCH 1

#include <queue>

int tttest();

enum MITSUME_HASHTABLE_PROCESSING_FLAG {
  MITSUME_CHECK_ONLY = 1,
  MITSUME_CHECK_OR_ADD = 2,
  MITSUME_MODIFY = 3,
  MITSUME_MODIFY_OR_ADD = 4,
  MITSUME_ADD_ONLY = 5
};

void mitsume_tool_lru_init(void);

int mitsume_tool_open(struct mitsume_consumer_metadata *thread_metadata,
                      mitsume_key key, void *write_addr, uint32_t size,
                      int replication_factor);
int mitsume_tool_open(struct mitsume_consumer_metadata *thread_metadata,
                      mitsume_key key, void *write_addr, uint32_t size,
                      int replication_factor, int coro_id, coro_yield_t &yield);
int mitsume_tool_read(struct mitsume_consumer_metadata *thread_metadata,
                      mitsume_key key, void *read_addr, uint32_t *read_size,
                      uint64_t optional_flag);
int mitsume_tool_read(struct mitsume_consumer_metadata *thread_metadata,
                      mitsume_key key, void *read_addr, uint32_t *read_size,
                      uint64_t optional_flag, int coro_id, coro_yield_t &yield);
int mitsume_tool_write(struct mitsume_consumer_metadata *thread_metadata,
                       mitsume_key key, void *write_addr, uint32_t size,
                       uint64_t optional_flag);
int mitsume_tool_write(struct mitsume_consumer_metadata *thread_metadata,
                       mitsume_key key, void *write_addr, uint32_t size,
                       uint64_t optional_flag, int coro_id,
                       coro_yield_t &yield);
#endif
