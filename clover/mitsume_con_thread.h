#ifndef MITSUME_CON_THREAD
#define MITSUME_CON_THREAD
#include "mitsume.h"
#include "mitsume_con_alloc.h"
#include "mitsume_tool.h"
#include "mitsume_tool_cache.h"

#include "mutex"
#include "unordered_map"
using namespace std;
void mitsume_con_thread_init(struct mitsume_ctx_con *local_ctx_con);
void *mitsume_con_controller_thread(void *input_arg);
int mitsume_con_thread_metadata_setup(struct configuration_params *input_arg,
                                      struct mitsume_ctx_con *server_ctx);
void *mitsume_con_controller_gcthread(void *input_metadata);
void *mitsume_con_controller_epoch_thread(void *input_metadata);
uint64_t mitsume_con_controller_thread_reply_entry_request(
    struct mitsume_allocator_metadata *thread_metadata,
    struct mitsume_msg *received, int coro_id);
uint64_t mitsume_con_controller_thread_process_entry_request(
    struct mitsume_allocator_metadata *thread_metadata,
    struct mitsume_msg *received);
uint64_t mitsume_con_controller_thread_process_gc(
    struct mitsume_allocator_metadata *thread_metadata,
    struct mitsume_large_msg *received, int coro_id);

#define MITSUME_CON_FIRST_VERSION 1
#endif
