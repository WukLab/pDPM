
#ifndef MITSUME_CLIENT_TOOL
#define MITSUME_CLIENT_TOOL
#include "mitsume.h"
#include "mitsume_con_alloc.h"

int mitsume_clt_consumer_ask_entries_from_controller(
    struct mitsume_consumer_metadata *thread_metadata, int queue_id,
    int numbers, int replication_bucket, int *ret_bucket, int coro_id,
    coro_yield_t &yield);

int mitsume_clt_consumer_get_entry_from_list(
    struct mitsume_consumer_metadata *thread_metadata, int size,
    struct mitsume_consumer_entry *output, int target_replication_bucket,
    int option_flag, int coro_id, coro_yield_t &yield);
int mitsume_clt_consumer_check_entry_from_list(
    struct mitsume_consumer_metadata *thread_metadata, uint32_t size,
    int target_replication_bucket);
#define MITSUME_CLT_CONSUMER_GET_ENTRY_REGULAR 0x0
#define MITSUME_CLT_CONSUMER_GET_ENTRY_DONT_ASK_CONTROLLER 0x1
#endif
