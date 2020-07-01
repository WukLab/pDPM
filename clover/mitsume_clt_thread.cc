#include "mitsume_clt_thread.h"

/*mutex
MITSUME_CON_NAMER_HASHTABLE_LOCK[1<<MITSUME_CON_NAMER_HASHTABLE_SIZE_BIT];
std::unordered_map<uint64_t, struct mitsume_hash_struct*>
MITSUME_CON_NAMER_HASHTABLE;

mutex
MITSUME_CON_GC_HASHTABLE_CURRENT_LOCK[1<<MITSUME_CON_NAMER_HASHTABLE_SIZE_BIT];
std::unordered_map<uint64_t, struct mitsume_gc_hashed_entry*>
MITSUME_CON_GC_HASHTABLE_CURRENT;

mutex
MITSUME_CON_GC_HASHTABLE_BUFFER_LOCK[1<<MITSUME_CON_NAMER_HASHTABLE_SIZE_BIT];
std::unordered_map<uint64_t, struct mitsume_gc_hashed_entry*>
MITSUME_CON_GC_HASHTABLE_BUFFER;*/

/**
 * mitsume_clt_consumer_local_init - local initialization of a consumer thread
 * metadata
 * @local_ctx_clt: context of client
 * @thread_metadata: pointer of thread metadata
 * @thread_id: thread_id
 * return: return 0 if success, -1 if fail
 */
int mitsume_clt_consumer_local_init(
    struct configuration_params *input_arg,
    struct mitsume_ctx_clt *local_ctx_clt,
    struct mitsume_consumer_metadata *thread_metadata, int thread_id) {
  // int per_node, per_slab;
  // int per_bucket;
  int i;
  // MITSUME_INFO("mitsume local init %d\n", thread_id);
  thread_metadata->thread_id = thread_id;
  thread_metadata->local_ctx_clt = local_ctx_clt;

  thread_metadata->private_bucket_counter = 0;
  thread_metadata->private_alloc_counter = 0;
  thread_metadata->rr_allocator_counter = 0;

  thread_metadata->thread_id = thread_id;
  thread_metadata->node_id = local_ctx_clt->node_id;

  thread_metadata->local_ctx_clt = local_ctx_clt;
  thread_metadata->local_inf =
      mitsume_local_thread_setup(local_ctx_clt->ib_ctx, thread_id);
  /*if(thread_id == 0)
      MITSUME_PRINT("%llx:%llx:%llx\n",
          (unsigned long long
     int)thread_metadata->local_inf->user_output_space[0], (unsigned long long
     int)thread_metadata->local_inf->user_output_mr[0]->addr, (unsigned long
     long int)thread_metadata->local_inf->user_output_mr[0]->lkey);
  */
  for (i = 0; i < MITSUME_CON_NUM; i++) {
    thread_metadata->task_allocator_counter[i] = 0;
  }
  return MITSUME_SUCCESS;
}

int mitsume_clt_thread_metadata_setup(struct configuration_params *input_arg,
                                      struct mitsume_ctx_clt *local_ctx_clt) {
  // int total_controller_num, controller_id_base;
  int i;
  int ret;

  // total_controller_num = MITSUME_CON_NUM;
  // controller_id_base = MITSUME_FIRST_ID;

  for (i = 0; i < MITSUME_CLT_CONSUMER_NUMBER; i++) {
    ret = mitsume_clt_consumer_local_init(
        input_arg, local_ctx_clt, &(local_ctx_clt->thread_metadata[i]), i);
    CPE(ret, "register consumer metadata fail", ret);
  }
  MITSUME_INFO("complete mitsume global init %d\n",
               MITSUME_CLT_CONSUMER_NUMBER);
  return 0;
  return MITSUME_SUCCESS;
}
