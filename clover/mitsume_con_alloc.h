#ifndef MITSUME_SERVER_ALLOC
#define MITSUME_SERVER_ALLOC
#include "mitsume.h"

enum MITSUME_TOOL_LOAD_BALANCING_SOURCE {
  MITSUME_TOOL_LOAD_BALANCING_SOURCE_READ = 10,
  MITSUME_TOOL_LOAD_BALANCING_SOURCE_WRITE = 11
};

enum MITSUME_TOOL_LOAD_BUCKET_FLAG {
  MITSUME_TOOL_LOAD_BALANCING_BUCKET_ALREADY_HAVE = 10,
  MITSUME_TOOL_LOAD_BALANCING_BUCKET_CAN_GET = 11
};

uint32_t mitsume_con_alloc_gc_key_to_gc_thread(mitsume_key key);
uint32_t mitsume_con_alloc_key_to_controller_id(mitsume_key key);

uint32_t mitsume_con_alloc_lh_to_size(uint64_t lh);
int mitsume_con_alloc_internal_lh_to_list_num(uint64_t lh);
int mitsume_con_alloc_lh_to_list_num(uint64_t lh);
int mitsume_con_alloc_entry_init(struct configuration_params *input_arg,
                                 struct mitsume_ctx_con *server_ctx);
uint32_t mitsume_con_alloc_list_num_to_size(int list_num);
int mitsume_con_alloc_split_space_into_lh(
    struct mitsume_ctx_con *local_ctx_con);
int mitsume_con_alloc_share_init(void);
int mitsume_con_alloc_populate_lh(struct mitsume_ctx_con *local_ctx_con);
int mitsume_con_alloc_get_lh(struct mitsume_ctx_con *local_ctx_con,
                             struct mitsume_ctx_clt *local_ctx_clt);
int mitsume_con_alloc_put_shortcut_into_list(
    struct mitsume_ctx_con *local_ctx_con);

unsigned long long int mitsume_con_alloc_get_total_lh();
uint32_t mitsume_con_alloc_rr_get_controller_id(
    struct mitsume_consumer_metadata *thread_metadata);
int mitsume_con_alloc_size_to_list_num(uint32_t size);
uint32_t mitsume_con_alloc_lh_to_node_id_bucket(uint64_t lh);
uint32_t mitsume_con_alloc_pointer_to_size(uint64_t pointer,
                                           uint32_t replication_factor);
int mitsume_con_alloc_get_shortcut_from_list(
    struct mitsume_allocator_metadata *thread_metadata,
    struct mitsume_shortcut_entry *output);
int mitsume_con_alloc_put_entry_into_thread(
    struct mitsume_ctx_con *local_ctx_con,
    struct mitsume_allocator_entry *input_entry, int tail);
#endif
