
#include "mitsume_tool.h"

int status_check = 0;
mutex
    MITSUME_TOOL_QUERY_HASHTABLE_LOCK[1
                                      << MITSUME_TOOL_QUERY_HASHTABLE_SIZE_BIT];
std::unordered_map<mitsume_key, struct mitsume_hash_struct *>
    MITSUME_TOOL_QUERY_HASHTABLE[1 << MITSUME_TOOL_QUERY_HASHTABLE_SIZE_BIT];
// cache::lru_cache<mitsume_key, int>
// MITSUME_TOOL_SHARE_LRU_CACHE[1<<MITSUME_LRU_BUCKET_BIT];
queue<mitsume_key> MITSUME_TOOL_SHARE_FIFO_QUEUE[1 << MITSUME_LRU_BUCKET_BIT];
mutex MITSUME_TOOL_SHARE_FIFO_QUEUE_LOCK[1 << MITSUME_LRU_BUCKET_BIT];
/**
 * mitsume_tool_begin: calling this function will prevent concurrency between
 * epoch forwarding and operations
 * @thread_metadata
 * return: return after spin_lock
 */
/**
 * Please be aware that current implementation blocks epoch forwarding during
 * begin-commit (although this is not a good design)
 */
inline void
mitsume_tool_begin(struct mitsume_consumer_metadata *thread_metadata,
                   int coro_id, coro_yield_t &yield) {
  int try_lock;
  do {
    try_lock = thread_metadata->current_running_lock[coro_id].try_lock();
    if (!try_lock && coro_id)
      yield_to_another_coro(thread_metadata->local_inf, coro_id, yield);
  } while (!try_lock);
}

/**
 * mitsume_tool_end: calling this function will allow epoch forwarding
 * @thread_metadata
 * return: return after spin_unlock
 */
inline void mitsume_tool_end(struct mitsume_consumer_metadata *thread_metadata,
                             int coro_id) {
  thread_metadata->current_running_lock[coro_id].unlock();
}

inline int mitsume_tool_load_balancing_record(
    struct mitsume_consumer_metadata *thread_metadata,
    struct mitsume_tool_communication *query, int source) {
  int bucket;
  uint32_t size;
  int per_bucket;
  if (!query) {
    MITSUME_PRINT_ERROR("NULL query input\n");
    return MITSUME_ERROR;
  }
  switch (source) {
  case MITSUME_TOOL_LOAD_BALANCING_SOURCE_READ:
    bucket = mitsume_con_alloc_lh_to_node_id_bucket(
        MITSUME_GET_PTR_LH(query->ptr.pointer));
    size = mitsume_con_alloc_pointer_to_size(query->ptr.pointer,
                                             query->replication_factor);
    thread_metadata->local_ctx_clt->read_bucket_counter[bucket] += size;
    MITSUME_STAT_ARRAY_ADD(bucket, 1);
    break;
  case MITSUME_TOOL_LOAD_BALANCING_SOURCE_WRITE:
    for (per_bucket = 0; per_bucket < query->replication_factor; per_bucket++) {
      bucket = mitsume_con_alloc_lh_to_node_id_bucket(
          MITSUME_GET_PTR_LH(query->replication_ptr[per_bucket].pointer));
      size = mitsume_con_alloc_pointer_to_size(
          query->replication_ptr[per_bucket].pointer,
          query->replication_factor);
      thread_metadata->local_ctx_clt->write_bucket_counter[bucket] += size;
      // record primary only
      break;
    }
    break;
  default:
    MITSUME_PRINT_ERROR("error type of source %d source\n", source);
  }
  return MITSUME_SUCCESS;
}

/**
 * mitsume_tool_get_crc: get crc of a header and full section length (not only
 * data but full entry)
 */
uint64_t mitsume_tool_get_crc(struct mitsume_ptr *header_ptr,
                              int replication_factor, void *data_addr,
                              uint32_t data_size, void *patch_base,
                              uint32_t patch_size) {
// uint32_t crc_tmp = 0;
// int per_replication;
// uint64_t current_version;

//[CAUTION] CRC is disabled since we no longer support message type application
//- it can be solved by 2RTT sleep in controller
#ifdef MITSUME_DISABLE_CRC
  return 0;
#endif
  die_printf("CRC is not implemented in userspace\n");
  return 0;
  /*for(per_replication = 0 ; per_replication < replication_factor ;
  per_replication++)
  {
          current_version =
  MITSUME_GET_PTR_ENTRY_VERSION(header_ptr[per_replication].pointer);
          if(per_replication == 0)
                  crc_tmp = crc32(~0, &current_version, sizeof(uint64_t));
          else
                  crc_tmp = crc32(crc_tmp, &current_version, sizeof(uint64_t));
  }
  crc_tmp = crc32(crc_tmp, data_addr, data_size);
  if(patch_size)
          crc_tmp = crc32(crc_tmp, patch_base, patch_size);
  return crc_tmp;*/
}

/**
 * mitsume_tool_local_hashtable_operations: modify local hashtable (query usage)
 * @thread_metadata: thread metadata
 * @key: query key
 * @input: input hash-entry content (communication usage)
 *      - if the input is query, it should change next to entry before modify
 * hashtable
 *      - if the input is newspace, use it directly (since newspace's entry is
 * already on correct location)
 * @current_hash_ptr: for input usage
 * @operation_flag: MITSUME_CHECK_OR_ADD | MITSUME_CHECK_ONLY | MITSUME_MODIFY
 * return: return a result after operation
 */
int mitsume_tool_local_hashtable_operations(
    struct mitsume_consumer_metadata *thread_metadata, mitsume_key key,
    struct mitsume_tool_communication *input,
    struct mitsume_hash_struct *current_hash_ptr, int operation_flag) {
  struct mitsume_hash_struct *search_hash_ptr;
  // struct hlist_node *temp_hash_ptr;
  int found_result = 0;
  int bucket = hash_min(key, MITSUME_TOOL_QUERY_HASHTABLE_SIZE_BIT);
  struct mitsume_hash_struct *new_entry = NULL;
  int per_replication;

  MITSUME_TOOL_QUERY_HASHTABLE_LOCK[bucket].lock();

  if (MITSUME_TOOL_QUERY_HASHTABLE[bucket].find(key) ==
      MITSUME_TOOL_QUERY_HASHTABLE[bucket].end())
    found_result = 0;
  else {
    found_result = 1;
    search_hash_ptr = MITSUME_TOOL_QUERY_HASHTABLE[bucket][key];
  }

  if (found_result) // If found, either do modification on hashed table content
                    // or do modification on return object
  {
    switch (operation_flag) {
    case MITSUME_CHECK_ONLY:
    case MITSUME_CHECK_OR_ADD:
#ifdef MITSUME_ENABLE_FIFO_LRU_QUEUE
      // if(search_hash_ptr->last_epoch < MITSUME_DANGEROUS_EPOCH ||
      // !MITSUME_TOOL_SHARE_LRU_CACHE[lru_bucket].exists(key))
      if (search_hash_ptr->last_epoch < MITSUME_DANGEROUS_EPOCH ||
          search_hash_ptr->in_lru == 0)
#else
      if (search_hash_ptr->last_epoch < MITSUME_DANGEROUS_EPOCH)
#endif
      {
        found_result = 0;
        MITSUME_TOOL_QUERY_HASHTABLE[bucket].erase(key);
        mitsume_tool_cache_free(search_hash_ptr, MITSUME_ALLOCTYPE_HASH_STRUCT);
      } else {
        mitsume_struct_copy_ptr_replication(
            input->replication_ptr, search_hash_ptr->ptr,
            search_hash_ptr->replication_factor);
        input->ptr.pointer =
            search_hash_ptr->ptr[MITSUME_REPLICATION_PRIMARY].pointer;

        input->shortcut_ptr.pointer = search_hash_ptr->shortcut_ptr.pointer;
        input->replication_factor = search_hash_ptr->replication_factor;

        input->target_gc_thread = search_hash_ptr->target_gc_thread;
        input->target_gc_controller = search_hash_ptr->target_gc_controller;
#ifdef MITSUME_ENABLE_FIFO_LRU_QUEUE
// move the data into the head for LRU
// MITSUME_TOOL_SHARE_LRU_CACHE[lru_bucket].put(key,
// MITSUME_LRU_DEFAULT_UNUSED_VALUE);
#endif
      }
      break;
    case MITSUME_MODIFY:
    case MITSUME_MODIFY_OR_ADD:
      // MITSUME_TOOL_PRINT_POINTER_KEY_REPLICATION(&input->ptr,
      // input->replication_factor, key);
      // MITSUME_TOOL_PRINT_POINTER_KEY_REPLICATION(&search_hash_ptr->ptr[MITSUME_REPLICATION_PRIMARY],
      // search_hash_ptr->replication_factor, key);

      //[CAREFUL] that all hashtable metadata should not incllude xact-area
      //related information search_hash_ptr->ptr.pointer =
      // input->ptr.pointer&(~MITSUME_PTR_MASK_XACT_AREA);
      for (per_replication = 0;
           per_replication < search_hash_ptr->replication_factor;
           per_replication++) {
        search_hash_ptr->ptr[per_replication].pointer =
            input->replication_ptr[per_replication].pointer &
            (~MITSUME_PTR_MASK_XACT_AREA);
      }
      // Don't keep xact area related information
      // only pointer need to be modified since shortcut never changed
      search_hash_ptr->last_epoch =
          thread_metadata->local_ctx_clt->gc_current_epoch;
#ifdef MITSUME_ENABLE_FIFO_LRU_QUEUE
// move the data into the head for LRU
// MITSUME_TOOL_SHARE_LRU_CACHE[lru_bucket].put(key,
// MITSUME_LRU_DEFAULT_UNUSED_VALUE);
#endif
      break;
    }
  } else if (!found_result) // if not found, insert the memory space into
                            // required hashtable
  {
    if (operation_flag == MITSUME_CHECK_OR_ADD ||
        operation_flag == MITSUME_MODIFY_OR_ADD) {
      new_entry = (struct mitsume_hash_struct *)mitsume_tool_cache_alloc(
          MITSUME_ALLOCTYPE_HASH_STRUCT);
      assert(new_entry);
      if (operation_flag == MITSUME_CHECK_OR_ADD) {
        new_entry->key = current_hash_ptr->key;

        for (per_replication = 0;
             per_replication < current_hash_ptr->replication_factor;
             per_replication++) {
          new_entry->ptr[per_replication].pointer =
              current_hash_ptr->ptr[per_replication].pointer &
              (~MITSUME_PTR_MASK_XACT_AREA);
        }

        new_entry->shortcut_ptr.pointer =
            current_hash_ptr->shortcut_ptr.pointer;
        new_entry->last_epoch =
            thread_metadata->local_ctx_clt->gc_current_epoch;
        new_entry->replication_factor = current_hash_ptr->replication_factor;

        new_entry->target_gc_thread =
            mitsume_con_alloc_gc_key_to_gc_thread(current_hash_ptr->key);
        new_entry->target_gc_controller =
            mitsume_con_alloc_key_to_controller_id(current_hash_ptr->key);
      } else {
        new_entry->key = key;
        // new_entry->ptr.pointer =
        // input->ptr.pointer&(~MITSUME_PTR_MASK_XACT_AREA);
        for (per_replication = 0; per_replication < input->replication_factor;
             per_replication++) {
          new_entry->ptr[per_replication].pointer =
              input->replication_ptr[per_replication].pointer &
              (~MITSUME_PTR_MASK_XACT_AREA);
        }
        new_entry->shortcut_ptr.pointer = input->shortcut_ptr.pointer;
        new_entry->last_epoch =
            thread_metadata->local_ctx_clt->gc_current_epoch;
        new_entry->replication_factor = input->replication_factor;
      }
#ifdef MITSUME_ENABLE_FIFO_LRU_QUEUE
      // push into fifo queue and remove the oldset one

      int lru_target = hash_min(key, MITSUME_LRU_BUCKET_BIT);
      MITSUME_TOOL_SHARE_FIFO_QUEUE_LOCK[lru_target].lock();

      MITSUME_TOOL_SHARE_FIFO_QUEUE[lru_target].push(key);
      if (MITSUME_TOOL_SHARE_FIFO_QUEUE[lru_target].size() >
          MITSUME_LRU_PER_QUEUE_SIZE) {
        mitsume_key old_key = MITSUME_TOOL_SHARE_FIFO_QUEUE[lru_target].front();
        int old_bucket =
            hash_min(old_key, MITSUME_TOOL_QUERY_HASHTABLE_SIZE_BIT);
        MITSUME_TOOL_SHARE_FIFO_QUEUE[lru_target].pop();
        MITSUME_TOOL_QUERY_HASHTABLE_LOCK[old_bucket].lock();
        MITSUME_TOOL_QUERY_HASHTABLE[old_bucket][old_key]->in_lru = 0;
        MITSUME_TOOL_QUERY_HASHTABLE_LOCK[old_bucket].unlock();
      }
      MITSUME_TOOL_SHARE_FIFO_QUEUE_LOCK[lru_target].unlock();
      new_entry->in_lru = 1;

// MITSUME_TOOL_SHARE_LRU_CACHE[lru_bucket].put(key,
// MITSUME_LRU_DEFAULT_UNUSED_VALUE); create an item in LRU table, and kick out
// the oldest one
#endif
    }
    switch (operation_flag) {
    case MITSUME_CHECK_OR_ADD:
    case MITSUME_MODIFY_OR_ADD:
      // hash_add(MITSUME_TOOL_QUERY_HASHTABLE, &new_entry->hlist, key);
      MITSUME_TOOL_QUERY_HASHTABLE[bucket][key] = new_entry;

      input->ptr.pointer = new_entry->ptr[MITSUME_REPLICATION_PRIMARY]
                               .pointer; // this value is already created at the
                                         // beginning of this function
      mitsume_struct_copy_ptr_replication(input->replication_ptr,
                                          new_entry->ptr,
                                          new_entry->replication_factor);

      input->shortcut_ptr.pointer = new_entry->shortcut_ptr.pointer;
      input->replication_factor = new_entry->replication_factor;

      input->target_gc_thread = new_entry->target_gc_thread;
      input->target_gc_controller = new_entry->target_gc_controller;
      // only last access epoch is needed to be updated
      new_entry->last_epoch = thread_metadata->local_ctx_clt->gc_current_epoch;
      break;
#ifdef MITSUME_ENABLE_FIFO_LRU_QUEUE
// move the data into the head of LRU queue
// MITSUME_TOOL_SHARE_LRU_CACHE[lru_bucket].put(key,
// MITSUME_LRU_DEFAULT_UNUSED_VALUE);
#endif
    }
  }
  MITSUME_TOOL_QUERY_HASHTABLE_LOCK[bucket].unlock();
  return found_result;
}

/**
 * mitsume_tool_query: query internal local CACHE (hash-table) to find the LH
 * and offset of the input key
 * @key: input key
 * @input: memory space which is going to keep the allocated space information
 * @option_flag: NULL | FORCE_REMOTE, force remote will force the system to
 * query remote controller to get the latest address
 * @debug_flag: 0 | 1, force query to contain debug flag, controller side will
 * show the latest offset in output return: return a number to show how did the
 * memory space be allocated
 */
int mitsume_tool_query(struct mitsume_consumer_metadata *thread_metadata,
                       mitsume_key key,
                       struct mitsume_tool_communication *input,
                       int option_flag, int debug_flag) {
  // for sending message to controller
  /*
  int target_allocator_id;
  int port = MITSUME_ALLOCATOR_PORT;
  struct mitsume_msg query_request, reply;
  */
  int coro_id = MITSUME_CLT_TEMP_COROUTINE_ID;
  int target_controller_id;
  struct mitsume_msg *query_request =
      thread_metadata->local_inf->input_space[coro_id];
  struct mitsume_msg *reply = thread_metadata->local_inf->output_space[coro_id];
  int local_errno = 0;
  int found = 0;

  if (!input) {
    MITSUME_PRINT_ERROR("input is NULL\n");
    goto abort_query;
    return MITSUME_ERROR;
  }
  if (!(option_flag & MITSUME_TOOL_QUERY_FORCE_REMOTE)) {
    /*if(option_flag&MITSUME_TOOL_QUERY_PRIVATE_HASH)
        found = mitsume_tool_local_private_hashtable_operations(thread_metadata,
    key, input, NULL, MITSUME_CHECK_ONLY); else found =
    mitsume_tool_local_hashtable_operations(thread_metadata, key, input, NULL,
    MITSUME_CHECK_ONLY);*/
    found = mitsume_tool_local_hashtable_operations(thread_metadata, key, input,
                                                    NULL, MITSUME_CHECK_ONLY);
    if (option_flag & MITSUME_TOOL_QUERY_FORCE_LOCAL) {
      if (found)
        return MITSUME_SUCCESS;
      else
        return MITSUME_ERROR;
    }
  }

#ifdef MITSUME_DISABLE_LOCAL_QUERY_CACHE
  found = 0;
#endif

  if (option_flag & MITSUME_TOOL_QUERY_FORCE_REMOTE || !found) {
    target_controller_id = mitsume_con_alloc_key_to_controller_id(key);
    // mitsume_tool_end(thread_metadata);
    query_request->msg_header.type = MITSUME_ENTRY_REQUEST;
    query_request->msg_header.src_id = thread_metadata->local_ctx_clt->node_id;
    query_request->msg_header.thread_id = thread_metadata->thread_id;
    query_request->content.msg_entry_request.type = MITSUME_ENTRY_QUERY;
    query_request->content.msg_entry_request.key = key;
    query_request->content.msg_entry_request.debug_flag = debug_flag;

    query_request->msg_header.reply_attr.addr =
        (uint64_t)thread_metadata->local_inf->output_mr[coro_id]->addr;
    query_request->msg_header.reply_attr.rkey =
        thread_metadata->local_inf->output_mr[coro_id]->rkey;
    query_request->msg_header.reply_attr.machine_id =
        thread_metadata->local_ctx_clt->node_id;

    reply->end_crc = MITSUME_WAIT_CRC;
    mitsume_send_full_message(
        thread_metadata->local_ctx_clt->ib_ctx, thread_metadata->local_inf,
        thread_metadata->local_inf->input_mr[coro_id],
        thread_metadata->local_inf->output_mr[coro_id],
        &query_request->msg_header, thread_metadata->local_ctx_clt->node_id,
        target_controller_id, sizeof(mitsume_msg));

    // MITSUME_STAT_ADD(MITSUME_STAT_CLT_QUERY_ENTRY, 1);

    if (reply->msg_header.type != MITSUME_ENTRY_REQUEST_ACK ||
        reply->content.msg_entry_request.type != MITSUME_ENTRY_QUERY) {
      MITSUME_PRINT_ERROR("receive error! type = %d,%d\n",
                          reply->msg_header.type,
                          reply->content.msg_entry_request.type);
      local_errno = __LINE__;
      // mitsume_tool_begin(thread_metadata);
      goto abort_query;
    } else {
      if (MITSUME_GET_PTR_LH(
              reply->content.msg_entry_request.ptr[MITSUME_REPLICATION_PRIMARY]
                  .pointer) == 0) // remote side responses no record
      {
        MITSUME_INFO("key %llu is not created\n", (unsigned long long int)key);
        // mitsume_tool_begin(thread_metadata);
        return MITSUME_ERROR;
      } else if (option_flag & MITSUME_TOOL_QUERY_FORCE_REMOTE) {
        // input->ptr.pointer = reply.content.msg_entry_request.ptr.pointer;
        struct mitsume_tool_communication temp_check;
        int temp_found;
        temp_found = mitsume_tool_local_hashtable_operations(
            thread_metadata, key, &temp_check, NULL, MITSUME_CHECK_ONLY);
        if (temp_found && option_flag & MITSUME_TOOL_QUERY_CHASING_HELP &&
            temp_check.ptr.pointer == reply->content.msg_entry_request
                                          .ptr[MITSUME_REPLICATION_PRIMARY]
                                          .pointer) {
          // MITSUME_STAT_ADD(MITSUME_STAT_TEST2, 1);
        } else {
          input->ptr.pointer =
              reply->content.msg_entry_request.ptr[MITSUME_REPLICATION_PRIMARY]
                  .pointer;
          mitsume_struct_copy_ptr_replication(
              input->replication_ptr, reply->content.msg_entry_request.ptr,
              reply->content.msg_entry_request.replication_factor);

          input->shortcut_ptr.pointer =
              reply->content.msg_entry_request.shortcut_ptr.pointer;
          input->replication_factor =
              reply->content.msg_entry_request.replication_factor;
          if (!(option_flag & MITSUME_TOOL_QUERY_WITHOUT_UPDATE))
          // if this query requires update (most of time requires update)
          // only chasing - migration or get to hop doesn't update hashtable
          {
            mitsume_tool_local_hashtable_operations(
                thread_metadata, key, input, NULL, MITSUME_MODIFY_OR_ADD);
          }
        }
      } else {
        struct mitsume_hash_struct current_hash_ptr;
        current_hash_ptr.key = key;
        // current_hash_ptr.ptr.pointer =
        // reply.content.msg_entry_request.ptr.pointer;
        mitsume_struct_copy_ptr_replication(
            current_hash_ptr.ptr, reply->content.msg_entry_request.ptr,
            reply->content.msg_entry_request.replication_factor);
        current_hash_ptr.shortcut_ptr.pointer =
            reply->content.msg_entry_request.shortcut_ptr.pointer;
        current_hash_ptr.replication_factor =
            reply->content.msg_entry_request.replication_factor;
        current_hash_ptr.version = reply->content.msg_entry_request.version;
        if (!(option_flag & MITSUME_TOOL_QUERY_WITHOUT_UPDATE)) {
          // if this query requires update (most of time requires update)
          // only chasing - migration or get to hop doesn't update hashtable
          found = mitsume_tool_local_hashtable_operations(
              thread_metadata, key, input, &current_hash_ptr,
              MITSUME_CHECK_OR_ADD);
        }
      }
    }
    // mitsume_tool_begin(thread_metadata);
  }

  input->size =
      mitsume_con_alloc_lh_to_size(MITSUME_GET_PTR_LH(input->ptr.pointer));

  // mitsume_tool_cache_free(query_request, MITSUME_ALLOCTYPE_MSG);
  // mitsume_tool_cache_free(reply, MITSUME_ALLOCTYPE_MSG);
  return MITSUME_SUCCESS;

abort_query:
  MITSUME_PRINT_ERROR("abort query with error %d\n", local_errno);

  return MITSUME_ERROR;
}

/**
 * mitsume_tool_setup_meshptr_empty_header_by_newspace: setup a empty mesh
 * pointer based on the new space information The data inside pointer will be
 * entry version only This funcion is used in write/open request
 * @meshptr: pointer to a replication array
 * @newspace: allocated space metadata
 * return: return error or success
 */
inline int mitsume_tool_setup_meshptr_empty_header_by_newspace(
    struct mitsume_ptr **meshptr, struct mitsume_tool_communication *newspace) {
  int per_replication;
  int per_entry;
  int replication_factor = newspace->replication_factor;
  struct mitsume_ptr *tar_ptr_array;
  for (per_replication = 0; per_replication < replication_factor;
       per_replication++) {
    tar_ptr_array = meshptr[per_replication];
    for (per_entry = 0; per_entry < replication_factor; per_entry++) {
      tar_ptr_array[per_entry].pointer = mitsume_struct_set_pointer(
          0, 0,
          MITSUME_GET_PTR_ENTRY_VERSION(
              newspace->replication_ptr[per_replication].pointer),
          0, 0);
    }
  }
  return MITSUME_SUCCESS;
}

/**
 * mitsume_tool_pick_replication_bucket: design which replication bucket should
 * be used upon request
 * @metadata: thread metadata
 * @target: return answer
 * @number_of_bucket: how many buckets are required in this request - basically
 * identical to replication factor return: return a number to show how did the
 * memory space be allocated
 */
int mitsume_tool_pick_replication_bucket(
    struct mitsume_consumer_metadata *thread_metadata, int *target,
    uint32_t number_of_bucket, uint32_t real_size) {
  uint32_t i;
  if (MITSUME_TOOL_LOAD_BALANCING_MODE) {
    long int pick_minimum[MITSUME_NUM_REPLICATION_BUCKET];
    int per_bucket;
    long int min_value;
    int min_index = -1;
    for (per_bucket = 0; per_bucket < MITSUME_NUM_REPLICATION_BUCKET;
         per_bucket++) {
      // if(mitsume_clt_consumer_check_entry_from_list(thread_metadata,
      // real_size, per_bucket))//give available list the highest priority
      pick_minimum[per_bucket] =
          thread_metadata->local_ctx_clt->read_bucket_counter[per_bucket]
              .load() +
          thread_metadata->local_ctx_clt->write_bucket_counter[per_bucket]
              .load();
      // else
      //    pick_minimum[per_bucket] = -1;
    }
    for (i = 0; i < number_of_bucket; i++) {
      min_value = MITSUME_I64_MAX;
      min_index = -1;
      for (per_bucket = 0; per_bucket < MITSUME_NUM_REPLICATION_BUCKET;
           per_bucket++) {
        if (pick_minimum[per_bucket] < min_value &&
            pick_minimum[per_bucket] >=
                0) // give highest priority to available lists
        {
          min_index = per_bucket;
          min_value = pick_minimum[per_bucket];
        }
      }
      if (min_value == MITSUME_I64_MAX) // no available entries
      {
        break;
      }
      if (min_index < 0) {
        MITSUME_PRINT_ERROR("wrong algorithm\n");
      }
      pick_minimum[min_index] = MITSUME_I64_MAX;
      target[i] = min_index;
    }
    for (; i < number_of_bucket; i++) // if there is no available entries, ask
                                      // controller for more, put -1 here
    {
      target[i] = MITSUME_TOOL_LOAD_BALANCING_NOENTRY;
    }
    return MITSUME_SUCCESS;
  }
  thread_metadata->private_bucket_counter++;
  for (i = 0; i < number_of_bucket; i++) {
    thread_metadata->private_alloc_counter++;
    if (thread_metadata->private_alloc_counter ==
        MITSUME_NUM_REPLICATION_BUCKET)
      thread_metadata->private_alloc_counter = 0;
    target[(i + thread_metadata->private_bucket_counter) % number_of_bucket] =
        thread_metadata->private_alloc_counter;
  }
  return MITSUME_SUCCESS;
  // if available number is not enough, it should return failure
  return MITSUME_ERROR;
}

/**
 * mitsume_tool_get_new_space: it works as a slab allocator.
 * @metadata: thread metadata
 * @size: requested size
 * @input: memory space which is going to keep the allocated space information
 * @target_replication_count: number of required replication_count
 * return: return a number to show how did the memory space be allocated
 */
int mitsume_tool_get_new_space(
    struct mitsume_consumer_metadata *thread_metadata, uint32_t size,
    struct mitsume_tool_communication *input, uint32_t target_replication_count,
    int coro_id, coro_yield_t &yield) {
  // uint64_t set_value;
  uint32_t real_size = size +
                       sizeof(struct mitsume_ptr) * target_replication_count +
                       MITSUME_CRC_SIZE; // because of the replication usage it
                                         // needs to maintain x pointers
  struct mitsume_consumer_entry entry_data;
  int replication_target[MITSUME_MAX_REPLICATION];
  uint32_t per_replication;
  int ret;

  if (!input) {
    MITSUME_PRINT_ERROR("input is NULL\n");
    goto get_new_space_fail;
  }

  // spin_lock(&thread_metadata->entry_operating_lock);
  // thread_metadata->entry_operating_lock.lock();

  ret = mitsume_tool_pick_replication_bucket(
      thread_metadata, replication_target, target_replication_count, real_size);
  if (ret) {
    MITSUME_PRINT_ERROR("fail to get correct replication count\n");
    goto get_new_space_fail;
  }

  for (per_replication = 0; per_replication < target_replication_count;
       per_replication++) {
    // MITSUME_PRINT("%d- %d\n", per_replication,
    // replication_target[per_replication]);
    ret = mitsume_clt_consumer_get_entry_from_list(
        thread_metadata, real_size, &entry_data,
        replication_target[per_replication],
        MITSUME_CLT_CONSUMER_GET_ENTRY_REGULAR, coro_id, yield);
    if (ret) {
      MITSUME_PRINT_ERROR("fail to get entry\n");
      goto get_new_space_fail;
    }
    input->replication_ptr[per_replication].pointer = entry_data.ptr.pointer;
  }
  // thread_metadata->entry_operating_lock.unlock();

  input->size = entry_data.size;
  input->replication_factor = target_replication_count;
  input->ptr.pointer =
      input->replication_ptr[MITSUME_REPLICATION_PRIMARY].pointer;

  // MITSUME_TOOL_PRINT_POINTER_KEY_REPLICATION(input->replication_ptr,
  // target_replication_count, 0);

  return MITSUME_SUCCESS;

get_new_space_fail:

  // thread_metadata->entry_operating_lock.unlock();
  return MITSUME_ALLOCATE_FAIL;
}

/**
 * mitsume_tool_pointer_chasing: pointer chasing function - ready to change to
 * multiple entries
 * @thread_metadata: caller's metadata
 * @request_num: number of requests
 * @key_list: list of keys
 * @query_list: query list got from internal hash-table
 * @internal_sge_list: a list of already constructed ib_sge (read request would
 * be modified a little bit to fit the latest read range)
 * @checking_data: for read usage (sicne read doesn't want to have a separate
 * read)
 * @mode_list: list of MITSUME_XACT_READ | MITSUME_XACT_WRITE |
 * MITSUME_XACT_CHASE
 * @hashtable_outdated_list: list of return value which contains which entry
 * should be updated to hashtable
 * @non_latest_flag: list of request to indicate whether this request will only
 * do pointer chasing once (for pub/sub)
 * @chasing_debug: this will trigger the chasing function to output log. It's
 * set by write operation after failing N times.
 * @target_hop_count_list: specifies which requets should trace how many hops.
 * it uses for get_hop implementation (called by management request) return:
 * return a result after pointer chasing
 */
int mitsume_tool_pointer_chasing(
    struct mitsume_consumer_metadata *thread_metadata, mitsume_key key,
    struct mitsume_tool_communication *query, struct ibv_sge *internal_sge,
    uint64_t *checking_data, int mode, int *hashtable_outdated,
    int non_latest_flag, int chasing_debug, int *target_hop_count_list,
    int coro_id, coro_yield_t &yield) {
  /*
int steps_ret_list[MITSUME_MAX_PARALLEL_REQUEST];
int steps_connection_list[MITSUME_MAX_PARALLEL_REQUEST];
int steps_poll_list[MITSUME_MAX_PARALLEL_REQUEST];
  int shortcut_ret_list[MITSUME_MAX_PARALLEL_REQUEST];
int shortcut_connection_list[MITSUME_MAX_PARALLEL_REQUEST];
int shortcut_poll_list[MITSUME_MAX_PARALLEL_REQUEST];
struct mitsume_shortcut chasing_shortcut_list[MITSUME_MAX_PARALLEL_REQUEST];
int move_count[MITSUME_MAX_PARALLEL_REQUEST]={0};
int already_chasing_shortcut_list[MITSUME_MAX_PARALLEL_REQUEST]={0};
int result_list[MITSUME_MAX_PARALLEL_REQUEST]={0};
*/

  // int coro_id = MITSUME_CLT_TEMP_COROUTINE_ID;
  struct thread_local_inf *local_inf = thread_metadata->local_inf;
  struct ib_inf *ib_ctx = thread_metadata->local_ctx_clt->ib_ctx;
  struct mitsume_ctx_clt *local_ctx_clt = thread_metadata->local_ctx_clt;

  uint64_t *target_checking_place;
  struct mitsume_ptr *write_checking_ptr =
      (struct mitsume_ptr *)&local_inf->write_checking_ptr[coro_id];
  ptr_attr *remote_mr;
  ptr_attr *remote_shortcut_mr;
  uint64_t memory_wr_id;
  uint64_t shortcut_wr_id;
  int result = 0;

  int chasing_count = 0;
  int chasing_asking_flag = 0;
  int chasing_flag = 0;

  int already_chasing_shortcut = 0;

  int per_replication;
  int per_replication_check_flag = 0;
  int while_loop_break_flag = 0;
  int local_errno = 0;
  int chasing_threshold = MITSUME_TOOL_CHASING_THRESHOLD;
  int chasing_asking_threshold = MITSUME_TOOL_ASKING_THRESHOLD;
  struct mitsume_shortcut shortcut_ptr;
  uint64_t target_value;

  int asking_controller_count = 0;

  // verify
  if (!query->ptr.pointer) {
    MITSUME_PRINT_ERROR("request: NULL ptr pointer\n");
    local_errno = __LINE__;
    goto abort_chasing;
  }
  if (!query->replication_factor) {
    MITSUME_PRINT_ERROR("request: NULL replication factor\n");
    local_errno = __LINE__;
    goto abort_chasing;
  } else {
    if (!query->shortcut_ptr.pointer) {
      MITSUME_PRINT_ERROR("request: NULL shortcut pointer\n");
      local_errno = __LINE__;
      goto abort_chasing;
    }
  }

  if (mode == MITSUME_READ)
    target_checking_place =
        checking_data; // which will point to variable "empty" which is declared
                       // inside mitsume_tool_ read function
  else if (mode == MITSUME_WRITE)
    target_checking_place = (uint64_t *)write_checking_ptr;
  else {
    die_printf("error mode %d\n", mode);
    return MITSUME_ERROR;
  }

  query->ptr.pointer = query->ptr.pointer & (~MITSUME_PTR_MASK_XACT_AREA);
  query->ptr.pointer =
      mitsume_struct_move_entry_pointer_to_next_pointer(query->ptr.pointer);
  mitsume_struct_move_replication_entry_pointer_to_next_pointer(
      query->replication_ptr, query->replication_factor);
  while (true) {
    while_loop_break_flag = 1;
    if (!result)
      while_loop_break_flag = 0;
    if (while_loop_break_flag)
      break;
    if (!result)
      chasing_count++;

    chasing_asking_flag = 0;
    chasing_flag = 0;

    //=============================
    if (chasing_count == chasing_threshold) {
      // MITSUME_STAT_SET(MITSUME_STAT_CLT_CHASING_BACKUP, 1);
      chasing_flag = 1;
    }
    if (chasing_count > chasing_asking_threshold) {
      chasing_asking_flag = 1;
      if ((non_latest_flag & MITSUME_TOOL_CHASING_NONLATEST) &&
          mode == MITSUME_WRITE) // pubsub-write should not go controller, it
                                 // would generate infinite loop
        chasing_asking_flag = 0;
      chasing_count = 0;
    }
    if ((chasing_flag || chasing_asking_flag)) {
      chasing_flag = 0;
      chasing_asking_flag = 0;
      chasing_count = 0;
    }
    // please read the caution comments above
    //==============================
    if (chasing_flag || chasing_asking_flag) {
      //[KNOWN ISSUE] MITSUME will ask controller to get the latest entry space
      //if it experiences a long pointer chasing. However, if there exists a
      // stale shortcut, which will make the returned-query go backward
      //[SOLUTION] If the return query is identical to the previous one, it
      //should keep pointer chasing in stead of using the value which is
      //returned by controller
      if (chasing_asking_flag) {
        mitsume_tool_query(thread_metadata, key, query,
                           MITSUME_TOOL_QUERY_FORCE_REMOTE |
                               MITSUME_TOOL_QUERY_CHASING_HELP,
                           0);
        query->ptr.pointer = mitsume_struct_move_entry_pointer_to_next_pointer(
            query->ptr.pointer);
        mitsume_struct_move_replication_entry_pointer_to_next_pointer(
            query->replication_ptr, query->replication_factor);
        already_chasing_shortcut = MITSUME_SHORTCUT_CHASING_ONCE;
      } else {
        already_chasing_shortcut = 0;
      }
    }
    if (mode == MITSUME_READ) {
      internal_sge[1].length = mitsume_con_alloc_pointer_to_size(
          query->ptr.pointer, query->replication_factor);

      // 0 for header
      // 1 for data
      // 2 for crc
      // issue rdma read here
      memory_wr_id = mitsume_local_thread_get_wr_id(local_inf);
      remote_mr =
          &local_ctx_clt->all_lh_attr[MITSUME_GET_PTR_LH(query->ptr.pointer)];

#ifdef MITSUME_DISABLE_CRC
      userspace_one_read_sge(ib_ctx, memory_wr_id, remote_mr, 0, internal_sge,
                             2);
#else
      userspace_one_read_sge(ib_ctx, memory_wr_id, remote_mr, 0, internal_sge,
                             3);
#endif
    } else if (mode == MITSUME_WRITE) {
      memory_wr_id = mitsume_local_thread_get_wr_id(local_inf);
      remote_mr =
          &local_ctx_clt->all_lh_attr[MITSUME_GET_PTR_LH(query->ptr.pointer)];

      userspace_one_read(
          ib_ctx, memory_wr_id, &local_inf->write_checking_ptr_mr[coro_id],
          sizeof(mitsume_ptr) * query->replication_factor, remote_mr, 0);
    } else {
      local_errno = __LINE__;
      goto abort_chasing;
    }
    if (!already_chasing_shortcut) {
      shortcut_wr_id = mitsume_local_thread_get_wr_id(local_inf);
      remote_shortcut_mr = &local_ctx_clt->all_shortcut_attr[MITSUME_GET_PTR_LH(
          query->shortcut_ptr.pointer)];
      userspace_one_read(ib_ctx, shortcut_wr_id,
                         &local_inf->chasing_shortcut_mr[coro_id],
                         sizeof(mitsume_shortcut), remote_shortcut_mr, 0);
    }

    if (coro_id)
      yield_to_another_coro(thread_metadata->local_inf, coro_id, yield);

    assert(result != 1);
    // finish poll
    userspace_one_poll(ib_ctx, memory_wr_id, remote_mr);
    mitsume_local_thread_put_wr_id(local_inf, memory_wr_id);
    if (!already_chasing_shortcut) {
      userspace_one_poll(ib_ctx, shortcut_wr_id, remote_shortcut_mr);
      mitsume_local_thread_put_wr_id(local_inf, shortcut_wr_id);
      mitsume_struct_copy_ptr_replication(
          shortcut_ptr.shortcut_ptr,
          local_inf->chasing_shortcut[coro_id].shortcut_ptr,
          query->replication_factor);
      // shortcut data is in shortcut_ptr now
    }
    // MITSUME_PRINT("finish poll\n");

    target_value = *(uint64_t *)(target_checking_place);
    if ((!already_chasing_shortcut &&
         shortcut_ptr.shortcut_ptr[MITSUME_REPLICATION_PRIMARY].pointer ==
             query->ptr.pointer)
        // haven't use shortcut value to overwrite query value, but the shortcut
        // already points to the same place as query
        || already_chasing_shortcut
        // already use shortcut value to overwrite query value
        || (MITSUME_GET_PTR_NEXT_VERSION(query->ptr.pointer) ==
                MITSUME_GET_PTR_ENTRY_VERSION(target_value) &&
            0 == MITSUME_GET_PTR_LH(target_value))) {
      if (MITSUME_GET_PTR_NEXT_VERSION(query->ptr.pointer) !=
          MITSUME_GET_PTR_ENTRY_VERSION(target_value)) {
        asking_controller_count++;
        if (MITSUME_GET_PTR_LH(target_value) ||
            MITSUME_GET_PTR_XACT_AREA(
                target_value)) // this part should be modified to support xact
                               // area
        {
          // MITSUME_STAT_ADD(MITSUME_STAT_TEST5, 1);
        }
        if (chasing_debug || asking_controller_count % 10000 == 0) {
          MITSUME_TOOL_PRINT_POINTER_KEY(
              &query->ptr, (struct mitsume_ptr *)&target_value, key);
          /*if(non_latest_flag[per_request]&MITSUME_TOOL_CHASING_NONLATEST)
              mitsume_tool_query(thread_metadata, key_list[per_request],
          query_list[per_request],
          MITSUME_TOOL_QUERY_FORCE_REMOTE|MITSUME_TOOL_QUERY_PRIVATE_HASH, 1);
          else
              mitsume_tool_query(thread_metadata, key_list[per_request],
          query_list[per_request], MITSUME_TOOL_QUERY_FORCE_REMOTE, 1);*/
          mitsume_tool_query(thread_metadata, key, query,
                             MITSUME_TOOL_QUERY_FORCE_REMOTE, 1);
          MITSUME_TOOL_PRINT_POINTER_KEY(&query->ptr, NULL, key);

          query->ptr.pointer =
              mitsume_struct_move_entry_pointer_to_next_pointer(
                  query->ptr.pointer);
          {
            // struct mitsume_ptr temp_test[MITSUME_MAX_REPLICATION];
            struct ibv_mr *read_mr;
            void *read_space = malloc(40960);
            uint64_t read_wr_id;
            ptr_attr *read_remote_mr =
                &local_ctx_clt
                     ->all_lh_attr[MITSUME_GET_PTR_LH(query->ptr.pointer)];
            read_mr = ibv_reg_mr(ib_ctx->pd, read_space, 40960,
                                 MITSUME_MR_PERMISSION);
            read_wr_id = mitsume_local_thread_get_wr_id(local_inf);
            userspace_one_read(ib_ctx, read_wr_id, read_mr,
                               sizeof(struct mitsume_ptr) *
                                   query->replication_factor,
                               read_remote_mr, 0);
            userspace_one_poll(ib_ctx, read_wr_id, read_remote_mr);
            mitsume_local_thread_put_wr_id(local_inf, read_wr_id);
            MITSUME_TOOL_PRINT_POINTER_KEY((struct mitsume_ptr *)read_space,
                                           NULL, key);
          }

          /*steps_ret_list[per_request] = liteapi_rdma_read_offset(
              MITSUME_GET_PTR_LH(query_list[per_request]->ptr.pointer),
              temp_test, sizeof(struct mitsume_ptr) *
          query_list[per_request]->replication_factor, LOW_PRIORITY,
              MITSUME_GET_PTR_OFFSET(query_list[per_request]->ptr.pointer),
          MITSUME_GLOBAL_PASSWORD, LITE_KERNELSPACE_FLAG,
          &steps_connection_list[per_request], &steps_poll_list[per_request]);
          steps_ret_list[per_request] =
          liteapi_poll_sendcq(steps_connection_list[per_request],
          &steps_poll_list[per_request]);
          MITSUME_TOOL_PRINT_POINTER_KEY(&temp_test[MITSUME_REPLICATION_PRIMARY],
          NULL, key_list[per_request]);*/
          while (1) {
            sleep(1);
          }
        } else {
          mitsume_tool_query(thread_metadata, key, query,
                             MITSUME_TOOL_QUERY_FORCE_REMOTE, 0);
        }
        // Beaware that all query usage should move next pointer to entry
        // pointer
        query->ptr.pointer = mitsume_struct_move_entry_pointer_to_next_pointer(
            query->ptr.pointer);
        mitsume_struct_move_replication_entry_pointer_to_next_pointer(
            query->replication_ptr, query->replication_factor);

        already_chasing_shortcut = MITSUME_SHORTCUT_CHASING_ONCE;

        // MITSUME_STAT_ADD(MITSUME_STAT_TEST6, 1);
      } else if ((mode == MITSUME_WRITE &&
                  MITSUME_GET_PTR_OPTION(target_value)) ||
                 (mode == MITSUME_READ &&
                  MITSUME_GET_PTR_OPTION(
                      target_value &&
                      (non_latest_flag & MITSUME_TOOL_CHASING_BLOCK)))) {
        /*if(mode_list[per_request] == MITSUME_READ &&
        MITSUME_GET_PTR_OPTION(target_value_list[per_request] &&
        (non_latest_flag[per_request]&MITSUME_TOOL_CHASING_BLOCK)))
        {
            MITSUME_STAT_ADD(MITSUME_STAT_TEST5, 1);
        }*/
        ;
      } else if (mode == MITSUME_READ && MITSUME_GET_PTR_OPTION(target_value)) {
        goto pubsub_read_consider_no_further_record;
      } else if (MITSUME_GET_PTR_LH(target_value)) {
        // MITSUME_STAT_ADD(MITSUME_STAT_CLT_CHASING, 1);
        // MITSUME_TOOL_PRINT_POINTER(&query->ptr, &query->shortcut_ptr);

        /*if(chasing_debug && mitsume_stat_read
        (MITSUME_STAT_CLT_CHASING)>10000)
        {
            MITSUME_TOOL_PRINT_POINTER(&query_list[per_request]->ptr, NULL);
            MITSUME_INFO("%#llx\n", target_value_list[per_request]);
        }*/

        // MITSUME_TOOL_PRINT_POINTER(&query->ptr, &target_value);
        query->ptr.pointer = target_value;
        mitsume_struct_copy_ptr_replication(
            query->replication_ptr, (struct mitsume_ptr *)target_checking_place,
            query->replication_factor);

#ifdef MITSUME_TOOL_DEBUG
        MITSUME_TOOL_PRINT_POINTER_KEY_REPLICATION(
            query->replication_ptr, query->replication_factor, key);
        MITSUME_TOOL_PRINT_POINTER(&query->ptr,
                                   (struct mitsume_ptr *)&target_value);
#endif
        // SHORTCUT CHASING only use once now

        // mark the shortcut is outdated
        // MITSUME_TOOL_PRINT_POINTER_KEY(&query_list[per_request]->ptr, NULL,
        // key_list[per_request]);
        already_chasing_shortcut = MITSUME_SHORTCUT_OUTDATED;
        *hashtable_outdated = MITSUME_HASHTABLE_OUTDATED;
      } else // this is the latest version
      {
      pubsub_read_consider_no_further_record:
        // MITSUME_TOOL_PRINT_POINTER_KEY(&query_list[per_request]->ptr, NULL,
        // key_list[per_request]);
        if (chasing_debug)
          MITSUME_TOOL_PRINT_POINTER_NULL(&query->ptr);

        result = 1;
      }
    } else {
      // make sure shortcut replication pointer is clean
      for (per_replication = 0; per_replication < query->replication_factor;
           per_replication++) {
        if (!(MITSUME_GET_PTR_LH(
                shortcut_ptr.shortcut_ptr[per_replication].pointer))) {
          MITSUME_PRINT_ERROR("NULL pointer\n");
          MITSUME_TOOL_PRINT_POINTER_NULL(
              &shortcut_ptr.shortcut_ptr[per_replication]);
        }
      }
#ifdef MITSUME_TOOL_DEBUG
      MITSUME_TOOL_PRINT_POINTER_NULL(&query->ptr, NULL);
      MITSUME_TOOL_PRINT_POINTER_NULL((struct mitsume_ptr *)&target_value);
      MITSUME_TOOL_PRINT_POINTER_NULL((struct mitsume_ptr *)&query->ptr);
      MITSUME_TOOL_PRINT_POINTER_KEY_REPLICATION(
          &shortcut_ptr.shortcut_ptr[MITSUME_REPLICATION_PRIMARY],
          query->replication_factor, 0);
#endif

#ifndef MITSUME_WRITE_OPT_DISABLE_SHORTCUT_UPDATE
      // MITSUME_STAT_ADD(MITSUME_STAT_CLT_SHORTCUT_CHASING, 1);

      query->ptr.pointer =
          shortcut_ptr.shortcut_ptr[MITSUME_REPLICATION_PRIMARY].pointer;
      mitsume_struct_copy_ptr_replication(query->replication_ptr,
                                          shortcut_ptr.shortcut_ptr,
                                          query->replication_factor);

      // Beaware that all query usage should move next pointer to entry pointer
      query->ptr.pointer =
          mitsume_struct_move_entry_pointer_to_next_pointer(query->ptr.pointer);
      mitsume_struct_move_replication_entry_pointer_to_next_pointer(
          query->replication_ptr, query->replication_factor);
#endif

      // MITSUME_TOOL_PRINT_POINTER_KEY(&query->ptr, NULL, key);
      already_chasing_shortcut = MITSUME_SHORTCUT_CHASING_ONCE;
      *hashtable_outdated = MITSUME_HASHTABLE_OUTDATED;
    }
  }

  // return before finish pointer chasing
  // 1. Reverse all query-type pointer into regular data-type pointer
  // 2. verify mode with return value

  // struct mitsume_ptr temp[MITSUME_MAX_REPLICATION];
  // struct mitsume_ptr tempo[MITSUME_MAX_REPLICATION];

  // temp[0].pointer = query_list[per_request]->replication_ptr[0].pointer;
  // tempo[0].pointer = query_list[per_request]->ptr.pointer;
  query->ptr.pointer =
      mitsume_struct_move_next_pointer_to_entry_pointer(query->ptr.pointer);
  mitsume_struct_move_replication_next_pointer_to_entry_pointer(
      query->replication_ptr, query->replication_factor);

  if (result != 1)
    MITSUME_PRINT_ERROR("result %d: doesn't finish\n", result);
  for (per_replication = 0; per_replication < query->replication_factor;
       per_replication++) {
    if (query->ptr.pointer ==
        query->replication_ptr[per_replication].pointer) // this check should
    {
      per_replication_check_flag = 1;
    }
    if (!(MITSUME_GET_PTR_LH(query->ptr.pointer))) {
      MITSUME_PRINT_ERROR("NULL pointer\n");
      MITSUME_TOOL_PRINT_POINTER_NULL(&query->replication_ptr[per_replication]);
    }
  }
  if (!per_replication_check_flag) {
    MITSUME_PRINT_ERROR("pointer doesn't match\n");
    MITSUME_TOOL_PRINT_POINTER_NULL(&query->ptr);
    for (per_replication = 0; per_replication < query->replication_factor;
         per_replication++) {
      MITSUME_TOOL_PRINT_POINTER_NULL(&query->replication_ptr[per_replication]);
    }
  }
  // MITSUME_STAT_ADD(MITSUME_STAT_TEST4, 1);
  return MITSUME_SUCCESS;

abort_chasing:
  MITSUME_PRINT_ERROR("error number: %d\n", local_errno);
  return local_errno;
}

coro_call_t **coro_arr;
queue<int> ready_queue[3];
static int current_num[3] = {0};
static int end_flag[3] = {0};
void master_func(coro_yield_t &yield, tuple<int, int> id_tuple) {
  int next;
  int i;
  int thread_id = get<0>(id_tuple);
  int coro_id = get<1>(id_tuple);
  for (i = 1; i < 5; i++)
    ready_queue[thread_id].push(i);
  while (current_num[thread_id] < 10) {
    ready_queue[thread_id].push(coro_id);
    next = ready_queue[thread_id].front();
    ready_queue[thread_id].pop();
    printf("%d-%d: master pick %d\n", thread_id, current_num[thread_id], next);
    yield(coro_arr[thread_id][next]);
  }
  end_flag[thread_id] = 1;
  printf("===master signal all===\n");
  while (!ready_queue[thread_id].empty()) {
    ready_queue[thread_id].push(coro_id);
    next = ready_queue[thread_id].front();
    ready_queue[thread_id].pop();

    printf("%d-%d: finish: master pick %d\n", thread_id, current_num[thread_id],
           next);
    yield(coro_arr[thread_id][next]);
  }
  printf("master finish\n");
}
void slave_func(coro_yield_t &yield, tuple<int, int> id_tuple) {
  int next;
  int thread_id = get<0>(id_tuple);
  int coro_id = get<1>(id_tuple);
  while (!end_flag[thread_id]) {
    // start processing
    current_num[thread_id]++;

    // add itself - first half
    ready_queue[thread_id].push(coro_id);
    next = ready_queue[thread_id].front();
    ready_queue[thread_id].pop();
    printf("(%d:%d): coro %d pick %d\n", thread_id, current_num[thread_id],
           coro_id, next);
    yield(coro_arr[thread_id][next]);
  }
  printf("%d coro %d finish\n", thread_id, coro_id);
}

void *master_shadow(void *input_thread_id) {
  long thread_id = (long)input_thread_id;
  coro_arr[thread_id][0]();
  printf("thread-finish %ld\n", thread_id);
  return NULL;
}

int tttest() {
  using std::placeholders::_1;
  long i;
  // coroutine<int>::pull_type source[5];
  coro_arr = new coro_call_t *[3];
  for (i = 0; i < 3; i++) {
    coro_arr[i] = new coro_call_t[5];
    for (int coro_i = 0; coro_i < 5; coro_i++) {
      if (coro_i == 0)
        coro_arr[i][coro_i] =
            coro_call_t(bind(master_func, _1, make_tuple(i, coro_i)));
      else
        coro_arr[i][coro_i] =
            coro_call_t(bind(slave_func, _1, make_tuple(i, coro_i)));
      printf("issue %ld-%d\n", i, coro_i);
    }
  }
  pthread_t task[3];
  for (i = 0; i < 3; i++) {
    pthread_create(&task[i], NULL, master_shadow, (void *)i);
  }
  for (i = 0; i < 3; i++) {
    pthread_join(task[i], NULL);
  }

  /*coroutine<int>::pull_type source{std::bind(cooperative, _1, 0)};
  cout<<"here"<<endl;
  std::cout << source.get() << '\n';
  source();
  std::cout << source.get() << '\n';
  source();*/
  return 0;
}

/**
 * mitsume_tool_read: process read request
 * @thread_metadata: respected thread_metadata
 * @key: target key
 * @read_addr: local address
 * @read_size: pointer of return read size, since this is a data-store, the size
 * can't tail before reading
 * @optional_flag: GC, LATEST, KVSTORE, PUBSUB, or MESSAGE
 * @usermode: kernel | userspace
 * return: return success
 */
coro_yield_t test_yield;
int mitsume_tool_read(struct mitsume_consumer_metadata *thread_metadata,
                      mitsume_key key, void *read_addr, uint32_t *read_size,
                      uint64_t optional_flag) {
  return mitsume_tool_read(thread_metadata, key, read_addr, read_size,
                           optional_flag, MITSUME_CLT_TEMP_COROUTINE_ID,
                           test_yield);
}

int mitsume_tool_read(struct mitsume_consumer_metadata *thread_metadata,
                      mitsume_key key, void *read_addr, uint32_t *read_size,
                      uint64_t optional_flag, int coro_id, coro_yield_t &yield)
// int mitsume_tool_read(struct mitsume_consumer_metadata *thread_metadata,
// mitsume_key key, void *read_addr, uint32_t *read_size, coro::caller_type&
// yield, int coro_id)
{
  // chrono::nanoseconds before, after;
  // before = get_current_ns();
  // after = get_current_ns();
  // MITSUME_STAT_ADD(MITSUME_STAT_TEST1, (after-before).count());
  // MITSUME_STAT_ADD(MITSUME_STAT_TEST2, 1);
  // int coro_id = MITSUME_CLT_TEMP_COROUTINE_ID;
  // MITSUME_PRINT("coro_id %d\n", coro_id);

  struct thread_local_inf *local_inf = thread_metadata->local_inf;
  // struct ib_inf *ib_ctx = thread_metadata->local_ctx_clt->ib_ctx;
  // uint64_t wr_id[MITSUME_MAX_REPLICATION];

  struct mitsume_tool_communication query;
  struct mitsume_tool_communication *query_ptr = NULL;
  int query_ret;
  struct mitsume_ptr *empty =
      local_inf->chase_empty_list[coro_id]; // this variable is used to sustain
                                            // ib_sge query in pointer chasing
  uint64_t *crc_base = local_inf->crc_base[MITSUME_MAX_REPLICATION];
  UNUSED(crc_base);
  int local_errno = 0;
  uint64_t *checking_data;
  int hashtable_result = MITSUME_CHASING_RESULT_INIT;
  int mode = MITSUME_READ;
  int chasing_return;

  struct ibv_sge internal_sge[MITSUME_TOOL_MAX_IB_SGE_SIZE];
  struct ibv_sge *internal_sge_ptr;
  int non_latest_flag;
  UNUSED(query_ptr);
  UNUSED(checking_data);
  UNUSED(internal_sge_ptr);

  // MITSUME_PRINT("status check %d\n", status_check++);
  mitsume_tool_begin(thread_metadata, coro_id, yield);

  // MITSUME_STAT_ADD(MITSUME_STAT_CLT_READ, 1);

  if (read_addr != local_inf->user_output_space[coro_id]) {
    MITSUME_PRINT_ERROR("addr %p should match correct coro_id %p\n", read_addr,
                        local_inf->user_output_space[coro_id]);
    local_errno = __LINE__;
    goto abort_read;
  }

  // step0: ask mapping from controller

  checking_data = (uint64_t *)empty;
  // step0-1: get mapping from controller
  // if it's in cache, key should map to a LH, respected OFFSET, and size
  query_ret = mitsume_tool_query(thread_metadata, key, &query, 0, 0);

  if (query_ret) {
    MITSUME_INFO("query fail with key %llu \n", (unsigned long long int)key);
    // MITSUME_STAT_ADD(MITSUME_STAT_CLT_READ_FAIL, 1);
    local_errno = __LINE__;
    goto abort_read;
  }
  // MITSUME_TOOL_PRINT_POINTER_NULL(&query.ptr);

  memset(empty, 0, sizeof(struct mitsume_ptr) * MITSUME_MAX_REPLICATION);
  internal_sge[0].addr = (uintptr_t)empty;
  internal_sge[0].length =
      sizeof(struct mitsume_ptr) * query.replication_factor;
  internal_sge[0].lkey = local_inf->chase_empty_mr[coro_id].lkey;
  // internal_sge[1].addr = (uintptr_t)read_addr;
  internal_sge[1].addr = (uint64_t)local_inf->user_output_space[coro_id];
  internal_sge[1].length = mitsume_con_alloc_pointer_to_size(
      query.ptr.pointer, query.replication_factor);
  internal_sge[1].lkey = local_inf->user_output_mr[coro_id]->lkey;
#ifdef MITSUME_DISABLE_CRC
#else
  internal_sge[2].addr = (uintptr_t)&crc_base[MITSUME_REPLICATION_PRIMARY];
  internal_sge[2].length = MITSUME_CRC_SIZE;
  internal_sge[2].lkey = local_inf->crc_mr[coro_id].lkey;
#endif

#ifdef MITSUME_TOOL_DEBUG
  MITSUME_TOOL_PRINT_POINTER(&query.ptr, &query.shortcut_ptr);
#endif
  // if it's in cache, key should map to a LH, respected OFFSET, and size

  // step1-1: issue a read request
  // step1-2: read shortcut
  query_ptr = &query;
  internal_sge_ptr = internal_sge;

  // if this is in laod balancing mode, pick the best replication to read
  if (MITSUME_TOOL_READ_LOAD_BALANCING_MODE) {
    long int min_value = MITSUME_I64_MAX;
    int target = -1;
    int bucket;
    long int tsum;
    int per_replication;
    for (per_replication = 0; per_replication < query.replication_factor;
         per_replication++) {
      bucket = mitsume_con_alloc_lh_to_node_id_bucket(
          MITSUME_GET_PTR_LH(query.replication_ptr[per_replication].pointer));
      tsum =
          thread_metadata->local_ctx_clt->read_bucket_counter[bucket].load() +
          thread_metadata->local_ctx_clt->write_bucket_counter[bucket].load();
      if (tsum < min_value) {
        min_value = tsum;
        target = per_replication;
      }
      // if(query.replication_factor>1)
      // MITSUME_PRINT("check-%d-%ld\n", bucket, tsum);
    }
    // if(query.replication_factor>1)
    // MITSUME_PRINT("pick-target-%d\n",target);
    query.ptr.pointer = query.replication_ptr[target].pointer;
  }

  non_latest_flag = 0;
  chasing_return = mitsume_tool_pointer_chasing(
      thread_metadata, key, query_ptr, internal_sge_ptr, checking_data, mode,
      &hashtable_result, non_latest_flag, 0, NULL, coro_id, yield);

  // Chasing error happens
  if (chasing_return < 0) {
    local_errno = __LINE__;
    MITSUME_PRINT_ERROR("chasing fail %d\n", -chasing_return);
    goto abort_read;
  }

  // get read size and check crc
  {
    *read_size = mitsume_con_alloc_pointer_to_size(query.ptr.pointer,
                                                   query.replication_factor);
    if (*read_size) // since read_size could be zero in non-latest (pub/sub)
                    // design
    {
#ifdef MITSUME_DISABLE_CRC
#else
      uint64_t crc_check;
      crc_check = mitsume_tool_get_crc(
          empty, query.replication_factor,
          (void *)local_inf->user_output_space[coro_id], *read_size, NULL, 0);
      if (crc_check != crc_base) {
        MITSUME_INFO("CRC doesn't match %lld %lld\n", crc_check, crc_base);
        MITSUME_STAT_ADD(MITSUME_STAT_CLT_CRC_MISMATCH, 1);
      }
#endif
    }
  }

  if (hashtable_result == MITSUME_HASHTABLE_OUTDATED)
    mitsume_tool_local_hashtable_operations(thread_metadata, key, &query, NULL,
                                            MITSUME_MODIFY);

#ifdef MITSUME_TOOL_DEBUG
  MITSUME_TOOL_PRINT_POINTER_KEY_REPLICATION(query.replication_ptr,
                                             query.replication_factor, key);
#endif

  mitsume_tool_end(thread_metadata, coro_id);
  if (MITSUME_TOOL_TRAFFIC_STAT) {
    mitsume_tool_load_balancing_record(thread_metadata, &query,
                                       MITSUME_TOOL_LOAD_BALANCING_SOURCE_READ);
  }

  // mitsume_tool_cache_free(query, MITSUME_ALLOCTYPE_TOOL_COMMUNICATION);
  // mitsume_tool_cache_free(empty, MITSUME_ALLOCTYPE_PTR_REPLICATION);
  // mitsume_tool_cache_free(internal_sge, MITSUME_ALLOCTYPE_IB_SGE);
  return MITSUME_SUCCESS;
  // step2: check next_ptr column to see lkey(offset) and xact_area
  // if lh!=NULL, pointer chasing
  // if xact_area != NULL, check Xact area
  // else, read data

  // step3: get data
abort_read:
  MITSUME_PRINT_ERROR("abort read with error %d\n", local_errno);

  mitsume_tool_end(thread_metadata, coro_id);
  return MITSUME_ERROR;
}

/**
 * mitsume_tool_open: process open request
 * @thread_metadata: respected thread_metadata
 * @key: target key
 * @write_addr: local address
 * @size: request size
 * @replication_factor: target replication factor
 * @usermode: kernel | userspace
 * return: return success
 */
int mitsume_tool_open(struct mitsume_consumer_metadata *thread_metadata,
                      mitsume_key key, void *write_addr, uint32_t size,
                      int replication_factor) {
  return mitsume_tool_open(thread_metadata, key, write_addr, size,
                           replication_factor, MITSUME_CLT_TEMP_COROUTINE_ID,
                           test_yield);
}
int mitsume_tool_open(struct mitsume_consumer_metadata *thread_metadata,
                      mitsume_key key, void *write_addr, uint32_t size,
                      int replication_factor, int coro_id,
                      coro_yield_t &yield) {

  /*int steps_ret[MITSUME_TOOL_STEPS_BUFFERSIZE];
int steps_connection[MITSUME_TOOL_STEPS_BUFFERSIZE];
int steps_poll[MITSUME_TOOL_STEPS_BUFFERSIZE];
int target_allocator_id;
*/

  // for sending RDMA request

  /*int usermode_page_result = 0;
  uintptr_t usermode_page_final_addr=0;*/

  // int coro_id = MITSUME_CLT_TEMP_COROUTINE_ID;
  struct thread_local_inf *local_inf = thread_metadata->local_inf;
  struct mitsume_tool_communication newspace, query;
  int newspace_ret, query_ret;
  int local_errno = 0;
  int per_replication;
  int target_controller_id;

  void *empty_base;

  struct ibv_sge **sge_list;
  struct ibv_sge *tar_sge;
  struct mitsume_ptr **meshptr_list;
  uint64_t *crc_base = local_inf->crc_base[MITSUME_MAX_REPLICATION];
  uint64_t wr_id[MITSUME_MAX_REPLICATION];
  struct ib_inf *ib_ctx = thread_metadata->local_ctx_clt->ib_ctx;

  int use_patch_flag;

  // for sending message to controller
  struct mitsume_msg *open_request, *reply;
  if (replication_factor > MITSUME_NUM_REPLICATION_BUCKET) {
    MITSUME_PRINT_ERROR("replication bucket is not enough to support target "
                        "replication (%d:%d)\n",
                        replication_factor, MITSUME_NUM_REPLICATION_BUCKET);
    goto abort_open;
  }
  if (write_addr != local_inf->user_input_space[coro_id] || !size) {
    MITSUME_PRINT_ERROR("addr %p should match correct coro_id %p and size %d "
                        "should not be zero\n",
                        write_addr,
                        thread_metadata->local_inf->user_input_space[coro_id],
                        size);
    local_errno = __LINE__;
    goto abort_open;
  }

  open_request = thread_metadata->local_inf->input_space[coro_id];
  reply = thread_metadata->local_inf->output_space[coro_id];
  // step0: get available space
  // newspace = mitsume_tool_cache_alloc(MITSUME_ALLOCTYPE_TOOL_COMMUNICATION);
  // MITSUME_PRINT("%llx %llx\n", (unsigned long long
  // int)local_inf->user_input_space[0], (unsigned long long
  // int)local_inf->user_input_mr[0]->lkey);

  newspace_ret = mitsume_tool_get_new_space(thread_metadata, size, &newspace,
                                            replication_factor, 0, test_yield);
  if (MITSUME_TOOL_LOAD_BALANCING_MODE) {
    for (per_replication = 0; per_replication < replication_factor;
         per_replication++) {
      int bucket = mitsume_con_alloc_lh_to_node_id_bucket(MITSUME_GET_PTR_LH(
          newspace.replication_ptr[per_replication].pointer));
      MITSUME_INFO("%llu %d-%d\n", (unsigned long long int)key, per_replication,
                   bucket);
    }
  }

  // MITSUME_TOOL_PRINT_POINTER_NULL(&newspace.ptr);
  // for(i=0;i<newspace.replication_factor;i++)
  //    MITSUME_TOOL_PRINT_POINTER_NULL(&newspace.replication_ptr[i]);
  // MITSUME_PRINT("size:%d\tfactor:%d\n", newspace.size,
  // newspace.replication_factor);
  if (newspace_ret)
    goto abort_open;
  assert(open_request);
  assert(reply);

  // MITSUME_TOOL_PRINT_POINTER_KEY_REPLICATION(newspace.replication_ptr,
  // replication_factor, key);

  sge_list = new struct ibv_sge *[MITSUME_MAX_REPLICATION];
  // meshptr_list = new struct mitsume_ptr*[MITSUME_MAX_REPLICATION];
  meshptr_list = new struct mitsume_ptr *[MITSUME_MAX_REPLICATION];

  for (per_replication = 0; per_replication < MITSUME_MAX_REPLICATION;
       per_replication++) {
    sge_list[per_replication] =
        (struct ibv_sge *)mitsume_tool_cache_alloc(MITSUME_ALLOCTYPE_IB_SGE);
    memset(sge_list[per_replication], 0,
           sizeof(struct ibv_sge) * MITSUME_TOOL_MAX_IB_SGE_SIZE);
    // meshptr_list[per_replication] = (struct
    // mitsume_ptr*)mitsume_tool_cache_alloc(MITSUME_ALLOCTYPE_PTR_REPLICATION);
    meshptr_list[per_replication] =
        &local_inf
             ->meshptr_list[coro_id][per_replication * MITSUME_MAX_REPLICATION];
  }

  mitsume_tool_begin(thread_metadata, coro_id, yield);

  // MITSUME_STAT_ADD(MITSUME_STAT_CLT_OPEN, 1);

  query_ret = mitsume_tool_query(thread_metadata, key, &query,
                                 MITSUME_TOOL_QUERY_FORCE_LOCAL, 0);
  if (!query_ret) // hit local cache
  {
    MITSUME_INFO("local hit: key %lu is already used\n",
                 (long unsigned int)key);
    local_errno = __LINE__;
    goto abort_open;
  }

  // DO NO NEED tO HANDLE page translation anymore
  /*usermode_page_result = mitsume_tool_get_real_address(write_addr,
  &usermode_page_final_addr, usermode, 0, MITSUME_TOOL_PAGE_START,
  MITSUME_TOOL_PAGE_WRITE, size, 0); if(unlikely(!usermode_page_final_addr))
  {
      MITSUME_ERROR("addr %p should not be zero\n", (void
  *)usermode_page_final_addr); local_errno = __LINE__; goto abort_open;
  }*/

#ifdef MITSUME_TOOL_DEBUG
  MITSUME_TOOL_PRINT_POINTER_NULL(&newspace.ptr);
// finish step0
#endif
  mitsume_tool_setup_meshptr_empty_header_by_newspace(meshptr_list, &newspace);

  empty_base = local_inf->empty_base;

  for (per_replication = 0; per_replication < replication_factor;
       per_replication++) {
    tar_sge = sge_list[per_replication];
    tar_sge[0].addr = (uintptr_t)meshptr_list[per_replication];
    tar_sge[0].length = sizeof(struct mitsume_ptr) * (replication_factor);
    tar_sge[0].lkey = local_inf->meshptr_mr[coro_id].lkey;

    tar_sge[1].addr = (uint64_t)local_inf->user_input_space[coro_id];
    tar_sge[1].length = size;
    tar_sge[1].lkey = local_inf->user_input_mr[coro_id]->lkey;

    tar_sge[2].addr = (uint64_t)empty_base;
    tar_sge[2].length = mitsume_con_alloc_pointer_to_size(
                            newspace.replication_ptr[per_replication].pointer,
                            replication_factor) -
                        size;
    tar_sge[2].lkey = local_inf->empty_mr->lkey;

    // setup crc
    crc_base[per_replication] =
        mitsume_tool_get_crc(meshptr_list[per_replication], replication_factor,
                             (void *)local_inf->user_input_space[coro_id], size,
                             empty_base, tar_sge[2].length);
    if (tar_sge[2].length > 0) {
      use_patch_flag = MITSUME_TOOL_WITH_PATCH;
#ifdef MITSUME_DISABLE_CRC
#else
      tar_sge[3].addr = (uintptr_t)&crc_base[per_replication];
      tar_sge[3].length = MITSUME_CRC_SIZE;
      tar_sge[3].lkey = local_inf->crc_mr[coro_id].lkey;
#endif
    } else {
      use_patch_flag = MITSUME_TOOL_WITHOUT_PATCH;
#ifdef MITSUME_DISABLE_CRC
#else
      tar_sge[2].addr = (uintptr_t)&crc_base[per_replication];
      tar_sge[2].length = MITSUME_CRC_SIZE;
      tar_sge[2].lkey = local_inf->crc_mr[coro_id].lkey;
#endif
    }
  }

  // step1: write to new space
  // setup empty based on newspace
  // empty->pointer = mitsume_struct_set_pointer(0, 0, 0,
  // MITSUME_GET_PTR_ENTRY_VERSION(newspace.ptr.pointer), 0, 0);
  ptr_attr *remote_mr[MITSUME_MAX_REPLICATION];
  // get correct ah and addr first
  for (per_replication = 0; per_replication < replication_factor;
       per_replication++) {
    remote_mr[per_replication] =
        &thread_metadata->local_ctx_clt->all_lh_attr[MITSUME_GET_PTR_LH(
            newspace.replication_ptr[per_replication].pointer)];
    wr_id[per_replication] = mitsume_local_thread_get_wr_id(local_inf);
  }

  for (per_replication = 0; per_replication < replication_factor;
       per_replication++) {
    tar_sge = sge_list[per_replication];
    if (use_patch_flag == MITSUME_TOOL_WITH_PATCH) {
#ifdef MITSUME_DISABLE_CRC
      userspace_one_write_sge(ib_ctx, wr_id[per_replication],
                              remote_mr[per_replication], 0, tar_sge, 3);
#else
      userspace_one_write_sge(ib_ctx, wr_id[per_replication],
                              remote_mr[per_replication], 0, tar_sge, 4);
#endif
    } else {
#ifdef MITSUME_DISABLE_CRC
      userspace_one_write_sge(ib_ctx, wr_id[per_replication],
                              remote_mr[per_replication], 0, tar_sge, 2);
#else
      userspace_one_write_sge(ib_ctx, wr_id[per_replication],
                              remote_mr[per_replication], 0, tar_sge, 3);
#endif
    }
  }
  for (per_replication = 0; per_replication < replication_factor;
       per_replication++) {
    userspace_one_poll(ib_ctx, wr_id[per_replication],
                       remote_mr[per_replication]);
    mitsume_local_thread_put_wr_id(local_inf, wr_id[per_replication]);
  }

  // step2: notify controller, check response

  target_controller_id = mitsume_con_alloc_key_to_controller_id(key);

  open_request->msg_header.type = MITSUME_ENTRY_REQUEST;
  open_request->msg_header.src_id = thread_metadata->local_ctx_clt->node_id;
  open_request->msg_header.thread_id = thread_metadata->thread_id;
  open_request->content.msg_entry_request.type = MITSUME_ENTRY_OPEN;
  open_request->content.msg_entry_request.key = key;

  mitsume_struct_copy_ptr_replication(
      open_request->content.msg_entry_request.ptr, newspace.replication_ptr,
      replication_factor);
  open_request->content.msg_entry_request.replication_factor =
      newspace.replication_factor;

  open_request->msg_header.reply_attr.addr =
      (uint64_t)thread_metadata->local_inf->output_mr[coro_id]->addr;
  open_request->msg_header.reply_attr.rkey =
      thread_metadata->local_inf->output_mr[coro_id]->rkey;
  open_request->msg_header.reply_attr.machine_id =
      thread_metadata->local_ctx_clt->node_id;

  reply->end_crc = MITSUME_WAIT_CRC;

  mitsume_send_full_message(
      thread_metadata->local_ctx_clt->ib_ctx, thread_metadata->local_inf,
      thread_metadata->local_inf->input_mr[coro_id],
      thread_metadata->local_inf->output_mr[coro_id], &open_request->msg_header,
      thread_metadata->local_ctx_clt->node_id, target_controller_id,
      sizeof(mitsume_msg));
  // send to controller

  if (reply->msg_header.type != MITSUME_ENTRY_REQUEST_ACK ||
      reply->content.msg_entry_request.type != MITSUME_ENTRY_OPEN) {
    MITSUME_PRINT_ERROR("receive error! type = %d,%d\n", reply->msg_header.type,
                        reply->content.msg_entry_request.type);
    local_errno = __LINE__;
    goto abort_open;
  } else {
    if (MITSUME_GET_PTR_LH(
            reply->content.msg_entry_request.ptr[MITSUME_REPLICATION_PRIMARY]
                .pointer) == 0) {
      MITSUME_INFO("key %lu is already used\n", (long unsigned int)key);
      MITSUME_TOOL_PRINT_POINTER_NULL(&open_request->content.msg_entry_request
                                           .ptr[MITSUME_REPLICATION_PRIMARY]);
      MITSUME_TOOL_PRINT_POINTER_NULL(
          &reply->content.msg_entry_request.ptr[MITSUME_REPLICATION_PRIMARY]);
      // I didn't cache the address if create/open fail, unless the user choose
      // to access it
      local_errno = __LINE__;
      goto abort_open;
    } else {
      struct mitsume_hash_struct current_hash_ptr;
      struct mitsume_tool_communication input;
      int found;
      current_hash_ptr.key = key;
      // current_hash_ptr.ptr.pointer =
      // reply.content.msg_entry_request.ptr.pointer;
      mitsume_struct_copy_ptr_replication(current_hash_ptr.ptr,
                                          reply->content.msg_entry_request.ptr,
                                          newspace.replication_factor);
      current_hash_ptr.shortcut_ptr.pointer =
          reply->content.msg_entry_request.shortcut_ptr.pointer;
      current_hash_ptr.replication_factor = newspace.replication_factor;
      found = mitsume_tool_local_hashtable_operations(thread_metadata, key,
                                                      &input, &current_hash_ptr,
                                                      MITSUME_CHECK_OR_ADD);
      if (found)
        MITSUME_PRINT(
            "key-%llu is already occupied by other concurrent threads\n",
            (unsigned long long int)key);

      // if this part found = true, it means another thread ask controller
      // before the opener gets reply. This is a corner case, but it may happen
    }
  }
#ifdef MITSUME_TOOL_DEBUG
  MITSUME_TOOL_PRINT_POINTER_KEY_REPLICATION(
      reply->content.msg_entry_request.ptr, replication_factor, key);
// finish step0
#endif

  /*
  {
      struct ibv_mr *read_mr;
      char *read_space = new char[4096];
      uint64_t ttwr_id;
      read_mr = ibv_reg_mr(ib_ctx->pd, read_space, 4096, MITSUME_MR_PERMISSION);
      remote_mr[0] =
  &thread_metadata->local_ctx_clt->all_lh_attr[MITSUME_GET_PTR_LH(newspace.replication_ptr[0].pointer)];
      ttwr_id = mitsume_local_thread_get_wr_id(local_inf);
      userspace_one_read(ib_ctx, ttwr_id, read_mr, 128, remote_mr[0], 0);
      userspace_one_poll(ib_ctx, ttwr_id, remote_mr[0]);
      mitsume_local_thread_put_wr_id(local_inf, ttwr_id);
      for(int i=0; i<128; ++i)
          std::cout << std::hex << (int)read_space[i];
      std::cout<<endl;

      uint64_t shortcut_pointer =
  reply->content.msg_entry_request.shortcut_ptr.pointer; uint64_t shortcut_lh =
  MITSUME_GET_PTR_LH(shortcut_pointer); remote_mr[0] =
  &thread_metadata->local_ctx_clt->all_shortcut_attr[shortcut_lh]; ttwr_id =
  mitsume_local_thread_get_wr_id(local_inf); userspace_one_read(ib_ctx, ttwr_id,
  read_mr, sizeof(struct mitsume_shortcut), remote_mr[0], 0);
      userspace_one_poll(ib_ctx, ttwr_id, remote_mr[0]);
      mitsume_local_thread_put_wr_id(local_inf, ttwr_id);
      for(uint32_t i=0; i<sizeof(struct mitsume_shortcut); ++i)
          std::cout << std::hex << (int)read_space[i];
      struct mitsume_ptr *test_ptr = (struct mitsume_ptr*)read_space;
      std::cout<<endl;
      MITSUME_TOOL_PRINT_POINTER_NULL(test_ptr);
  }*/

  mitsume_tool_end(thread_metadata, coro_id);

  for (per_replication = 0; per_replication < MITSUME_MAX_REPLICATION;
       per_replication++) {
    mitsume_tool_cache_free(sge_list[per_replication],
                            MITSUME_ALLOCTYPE_IB_SGE);
  }
  delete sge_list;

  return MITSUME_SUCCESS;

abort_open:
  // MITSUME_STAT_ADD(MITSUME_STAT_CLT_OPEN_FAIL, 1);
  MITSUME_PRINT_ERROR("abort open with error %d\n", local_errno);
  mitsume_tool_end(thread_metadata, coro_id);
  return MITSUME_ERROR;
}

/**
 * mitsume_tool_write: process write request
 * @thread_metadata: respected thread_metadata
 * @key: target key
 * @write_addr: local address
 * @size: request size
 * @optional_flag: GC, LATEST, KVSTORE, PUBSUB, or MESSAGE
 * @usermode: kernel | userspace
 * return: return success
 */

int mitsume_tool_write(struct mitsume_consumer_metadata *thread_metadata,
                       mitsume_key key, void *write_addr, uint32_t size,
                       uint64_t optional_flag) {
  return mitsume_tool_write(thread_metadata, key, write_addr, size,
                            optional_flag, MITSUME_CLT_TEMP_COROUTINE_ID,
                            test_yield);
}
int mitsume_tool_write(struct mitsume_consumer_metadata *thread_metadata,
                       mitsume_key key, void *write_addr, uint32_t size,
                       uint64_t optional_flag, int coro_id,
                       coro_yield_t &yield) {
  /*
      struct mitsume_tool_communication *query_ptr;
      int steps_ret[MITSUME_TOOL_STEPS_BUFFERSIZE];
  int steps_connection[MITSUME_TOOL_STEPS_BUFFERSIZE];
  int steps_poll[MITSUME_TOOL_STEPS_BUFFERSIZE];
  int usermode_page_result = 0;
  uintptr_t usermode_page_final_addr=0;

  struct mitsume_ptr
  meshptr_list_base[MITSUME_MAX_REPLICATION*MITSUME_MAX_REPLICATION]; struct
  mitsume_ptr empty[MITSUME_MAX_REPLICATION]; //this variable is used to sustain
  ib_sge query in pointer chasing


  uint64_t crc_base[MITSUME_MAX_REPLICATION];

  */
  int non_latest_flag = 0;
  uint64_t checking_data;
  int chasing_result;
  int chasing_debug = 0;
  int chasing_return;
  int chasing_count = 0;
  int mode = MITSUME_WRITE;

  uint64_t wr_id[MITSUME_MAX_REPLICATION] = {0};
  struct thread_local_inf *local_inf = thread_metadata->local_inf;
  struct ib_inf *ib_ctx = thread_metadata->local_ctx_clt->ib_ctx;
  uint64_t *crc_base = local_inf->crc_base[MITSUME_MAX_REPLICATION];
  // int coro_id = MITSUME_CLT_TEMP_COROUTINE_ID;
  struct ibv_sge
      sge_list_base[MITSUME_TOOL_MAX_IB_SGE_SIZE * MITSUME_MAX_REPLICATION];
  int per_replication;
  struct mitsume_ptr *meshptr_list[MITSUME_MAX_REPLICATION];
  int local_errno = 0;
  struct ibv_sge *sge_list[MITSUME_MAX_REPLICATION];
  struct ibv_sge *tar_sge;
  int temporary_replication_count;
  struct mitsume_tool_communication query, newspace;
  int query_ret, newspace_ret;
  void *empty_base;
  int use_patch_flag = MITSUME_TOOL_WITHOUT_PATCH;

  ptr_attr *remote_mr[MITSUME_MAX_REPLICATION] = {0};

  struct mitsume_ptr read_value;
  struct mitsume_ptr guess_value;
  struct mitsume_ptr set_value;

  uint64_t cs_id;
  ptr_attr *cs_mr;

#ifdef MITSUME_ENABLE_READ_VERIFICATION
  uint64_t read_wr_id[MITSUME_MAX_REPLICATION];
#endif

  for (per_replication = 0; per_replication < MITSUME_MAX_REPLICATION;
       per_replication++) {
    sge_list[per_replication] =
        &sge_list_base[per_replication * MITSUME_TOOL_MAX_IB_SGE_SIZE];
    meshptr_list[per_replication] =
        &local_inf
             ->meshptr_list[coro_id][per_replication * MITSUME_MAX_REPLICATION];
  }

  if (write_addr != local_inf->user_input_space[coro_id] || !size) {
    MITSUME_PRINT_ERROR("addr %p should match correct coro_id %p and size %d "
                        "should not be zero\n",
                        write_addr,
                        thread_metadata->local_inf->user_input_space[coro_id],
                        size);
    local_errno = __LINE__;
    goto abort_write;
  }

#ifndef MITSUME_DISABLE_LOCAL_QUERY_CACHE
  query_ret = mitsume_tool_query(thread_metadata, key, &query, 0, 0);
  temporary_replication_count = query.replication_factor;
#else
  temporary_replication_count = MITSUME_DEFAULT_REPLICATION_NUM;
#endif

  // MITSUME_STAT_ADD(MITSUME_STAT_CLT_WRITE, 1);
  // step0-1: get new space from local buddy allocator
  // get temporary replication count
  newspace_ret =
      mitsume_tool_get_new_space(thread_metadata, size, &newspace,
                                 temporary_replication_count, coro_id, yield);

  if (newspace_ret) {
    MITSUME_PRINT_ERROR("fail to get new space %d\n", newspace_ret);
    local_errno = __LINE__;
    goto abort_write;
  }

  mitsume_tool_begin(thread_metadata, coro_id, yield);

  // query again after lock the epoch
  query_ret = mitsume_tool_query(thread_metadata, key, &query, 0, 0);
  if (query_ret) {
    MITSUME_INFO("query fail with key %lu \n", (long unsigned int)key);
    // MITSUME_STAT_ADD(MITSUME_STAT_CLT_WRITE_FAIL, 1);
    local_errno = __LINE__;
    goto abort_write;
  }

#ifndef MITSUME_DISABLE_LOCAL_QUERY_CACHE
  if (query.replication_factor != temporary_replication_count ||
      query.replication_factor != newspace.replication_factor) {
    MITSUME_PRINT_ERROR("replication factor is not sync %d, %d\n",
                        query.replication_factor, temporary_replication_count);
  }
#endif

  mitsume_tool_setup_meshptr_empty_header_by_newspace(meshptr_list, &newspace);
  empty_base = local_inf->empty_base;

  // MITSUME_TOOL_PRINT_POINTER_KEY(&query->ptr, &newspace.ptr, key);
  // step0-2: get mapping from controller
  // if it's in cache, key should map to a LH, respected OFFSET, and size

  // MITSUME_TOOL_PRINT_POINTER_KEY_REPLICATION(empty, query.replication_factor,
  // key); MITSUME_TOOL_PRINT_GC_POINTER_KEY(&query.ptr, &newspace.ptr, key);

  for (per_replication = 0; per_replication < query.replication_factor;
       per_replication++) {
    tar_sge = sge_list[per_replication];
    tar_sge[0].addr = (uintptr_t)meshptr_list[per_replication];
    tar_sge[0].length = sizeof(struct mitsume_ptr) * (query.replication_factor);
    tar_sge[0].lkey = local_inf->meshptr_mr[coro_id].lkey;

    tar_sge[1].addr = (uint64_t)local_inf->user_input_space[coro_id];
    tar_sge[1].length = size;
    tar_sge[1].lkey = local_inf->user_input_mr[coro_id]->lkey;

    tar_sge[2].addr = (uintptr_t)empty_base;
    tar_sge[2].length = mitsume_con_alloc_pointer_to_size(
                            newspace.replication_ptr[per_replication].pointer,
                            query.replication_factor) -
                        size;
    tar_sge[2].lkey = local_inf->empty_mr->lkey;

    // setup crc

    crc_base[per_replication] = mitsume_tool_get_crc(
        meshptr_list[per_replication], query.replication_factor,
        (void *)local_inf->user_input_space[coro_id], size, empty_base,
        tar_sge[2].length);
    if (tar_sge[2].length > 0) {
      use_patch_flag = MITSUME_TOOL_WITH_PATCH;
#ifdef MITSUME_DISABLE_CRC
#else
      tar_sge[3].addr = (uintptr_t)&crc_base[per_replication];
      tar_sge[3].length = MITSUME_CRC_SIZE;
      tar_sge[3].lkey = local_inf->crc_mr[coro_id].lkey;
#endif
    } else {
      use_patch_flag = MITSUME_TOOL_WITHOUT_PATCH;
#ifdef MITSUME_DISABLE_CRC
#else
      tar_sge[2].addr = (uintptr_t)&crc_base[per_replication];
      tar_sge[2].length = MITSUME_CRC_SIZE;
      tar_sge[2].lkey = local_inf->crc_mr[coro_id].lkey;
#endif
    }
    // MITSUME_INFO("write crc %lld\n", crc_base[per_replication]);
  }

  // finish step0
  // step1-1: write to a new place

  for (per_replication = 0; per_replication < query.replication_factor;
       per_replication++) {
    remote_mr[per_replication] =
        &thread_metadata->local_ctx_clt->all_lh_attr[MITSUME_GET_PTR_LH(
            newspace.replication_ptr[per_replication].pointer)];
    wr_id[per_replication] = mitsume_local_thread_get_wr_id(local_inf);
  }

  for (per_replication = 0; per_replication < query.replication_factor;
       per_replication++) {
    tar_sge = sge_list[per_replication];
    if (use_patch_flag == MITSUME_TOOL_WITH_PATCH) {
#ifdef MITSUME_DISABLE_CRC
      userspace_one_write_sge(ib_ctx, wr_id[per_replication],
                              remote_mr[per_replication], 0, tar_sge, 3);
#else
      userspace_one_write_sge(ib_ctx, wr_id[per_replication],
                              remote_mr[per_replication], 0, tar_sge, 4);
#endif
    } else {
#ifdef MITSUME_DISABLE_CRC
      userspace_one_write_sge(ib_ctx, wr_id[per_replication],
                              remote_mr[per_replication], 0, tar_sge, 2);
#else
      userspace_one_write_sge(ib_ctx, wr_id[per_replication],
                              remote_mr[per_replication], 0, tar_sge, 3);
#endif
    }
  }
  guess_value.pointer = mitsume_struct_set_pointer(
      0, 0, MITSUME_GET_PTR_ENTRY_VERSION(query.ptr.pointer), 0, 0);
  read_value.pointer = 0xffffffff;

  if (query.replication_factor != 1) {
    // step1-2: setup guess value, initialize read value
    set_value.pointer = mitsume_struct_set_pointer(
        0, 0, MITSUME_GET_PTR_ENTRY_VERSION(query.ptr.pointer), 0, 1);
    // step1-3: issue lock with uncommitted flag

    cs_id = mitsume_local_thread_get_wr_id(local_inf);
    cs_mr = &thread_metadata->local_ctx_clt
                 ->all_lh_attr[MITSUME_GET_PTR_LH(query.ptr.pointer)];
    userspace_one_cs(ib_ctx, cs_id, local_inf->input_mr[coro_id], cs_mr,
                     guess_value.pointer, set_value.pointer);

    // poll step1-1 and 1-3 at the same time
    if (coro_id)
      yield_to_another_coro(thread_metadata->local_inf, coro_id, yield);
    for (per_replication = 0; per_replication < query.replication_factor;
         per_replication++) {
      assert(wr_id[per_replication]);
      userspace_one_poll(ib_ctx, wr_id[per_replication],
                         remote_mr[per_replication]);
      mitsume_local_thread_put_wr_id(local_inf, wr_id[per_replication]);
    }
    userspace_one_poll(ib_ctx, cs_id, cs_mr);
    mitsume_local_thread_put_wr_id(local_inf, cs_id);
    memcpy(&read_value, local_inf->input_space[coro_id],
           sizeof(struct mitsume_ptr));
    // MITSUME_TOOL_PRINT_POINTER_NULL(&read_value);

#ifdef MITSUME_ENABLE_READ_VERIFICATION
    for (per_replication = 0; per_replication < query.replication_factor;
         per_replication++) {
      remote_mr[per_replication] =
          &thread_metadata->local_ctx_clt->all_lh_attr[MITSUME_GET_PTR_LH(
              newspace.replication_ptr[per_replication].pointer)];
      //[CAUTION] we are using the same memory space to read data which will
      //corrupt the data. however, this is a read validation which should be
      // fine
      read_wr_id[per_replication] = mitsume_local_thread_get_wr_id(local_inf);
      userspace_one_read(ib_ctx, read_wr_id[per_replication],
                         local_inf->input_mr[coro_id], 8,
                         remote_mr[per_replication], 0);
    }
    // polling read verification of write request - poll
    for (per_replication = 0; per_replication < query.replication_factor;
         per_replication++) {
      assert(remote_mr[per_replication]);
      userspace_one_poll(ib_ctx, read_wr_id[per_replication],
                         remote_mr[per_replication]);
      mitsume_local_thread_put_wr_id(local_inf, read_wr_id[per_replication]);
    }
#endif
  } else {
    // issue shortcut read to get the latest shortcut pointer
    // this shortcut can be used by the compare and swap
    // since it's still in writing RTT time, the cost in terms of latency is not
    // significant
    set_value.pointer = mitsume_struct_set_pointer(
        MITSUME_GET_PTR_LH(newspace.ptr.pointer),
        MITSUME_GET_PTR_ENTRY_VERSION(newspace.ptr.pointer),
        MITSUME_GET_PTR_ENTRY_VERSION(query.ptr.pointer), 0, 0);

    // poll step1-1 before step 1-3
    if (coro_id)
      yield_to_another_coro(thread_metadata->local_inf, coro_id, yield);
    for (per_replication = 0; per_replication < query.replication_factor;
         per_replication++) {
      assert(wr_id[per_replication]);
      userspace_one_poll(ib_ctx, wr_id[per_replication],
                         remote_mr[per_replication]);
      mitsume_local_thread_put_wr_id(local_inf, wr_id[per_replication]);
    }

    // verification of write requets - issue a read
    /*
    wr_id = mitsume_local_thread_get_wr_id(local_inf);
    userspace_one_read(node_share_inf, wr_id, read_mr, 64,
    &client_ctx->all_shortcut_attr[16], 0); userspace_one_poll(node_share_inf,
    wr_id, &client_ctx->all_shortcut_attr[16]);
    mitsume_local_thread_put_wr_id(local_inf, wr_id);
    */
#ifdef MITSUME_ENABLE_READ_VERIFICATION
    for (per_replication = 0; per_replication < query.replication_factor;
         per_replication++) {
      remote_mr[per_replication] =
          &thread_metadata->local_ctx_clt->all_lh_attr[MITSUME_GET_PTR_LH(
              newspace.replication_ptr[per_replication].pointer)];
      //[CAUTION] we are using the same memory space to read data which will
      //corrupt the data. however, this is a read validation which should be
      // fine
      read_wr_id[per_replication] = mitsume_local_thread_get_wr_id(local_inf);
      userspace_one_read(ib_ctx, read_wr_id[per_replication],
                         local_inf->input_mr[coro_id], 8,
                         remote_mr[per_replication], 0);
    }

    // polling read verification of write request - poll
    if (coro_id)
      yield_to_another_coro(thread_metadata->local_inf, coro_id, yield);

    for (per_replication = 0; per_replication < query.replication_factor;
         per_replication++) {
      assert(remote_mr[per_replication]);
      userspace_one_poll(ib_ctx, read_wr_id[per_replication],
                         remote_mr[per_replication]);
      mitsume_local_thread_put_wr_id(local_inf, read_wr_id[per_replication]);
    }
#endif

    // step1-3: issue lock with pointer directly after polling
    cs_id = mitsume_local_thread_get_wr_id(local_inf);
    cs_mr = &thread_metadata->local_ctx_clt
                 ->all_lh_attr[MITSUME_GET_PTR_LH(query.ptr.pointer)];
    userspace_one_cs(ib_ctx, cs_id, local_inf->input_mr[coro_id], cs_mr,
                     guess_value.pointer, set_value.pointer);

    if (coro_id)
      yield_to_another_coro(thread_metadata->local_inf, coro_id, yield);

    userspace_one_poll(ib_ctx, cs_id, cs_mr);
    mitsume_local_thread_put_wr_id(local_inf, cs_id);
    memcpy(&read_value, local_inf->input_space[coro_id],
           sizeof(struct mitsume_ptr));
    // MITSUME_TOOL_PRINT_POINTER_NULL(&read_value);
  }

  // MITSUME_TOOL_PRINT_POINTER_KEY(&query.ptr, &query.replication_ptr[0], key);
  // MITSUME_TOOL_PRINT_POINTER_NULL(&read_value);
  // MITSUME_TOOL_PRINT_POINTER_NULL(&guess_value);
  // MITSUME_TOOL_PRINT_POINTER_NULL(&set_value);

  // only do pointer chasing when compare and swap fail
  while (read_value.pointer != guess_value.pointer) {
    chasing_return = mitsume_tool_pointer_chasing(
        thread_metadata, key, &query, NULL, &checking_data, mode,
        &chasing_result, non_latest_flag, chasing_debug, NULL, coro_id, yield);
    // MITSUME_TOOL_PRINT_POINTER_KEY(&query.ptr, &newspace.ptr, key);
    if (chasing_return < 0) {
      MITSUME_PRINT_ERROR("chasing fail %d\n", -chasing_return);
      local_errno = __LINE__;
      goto abort_write;
    }
    // Update last query place
    // Both guess and set value needed to be updated
    guess_value.pointer = mitsume_struct_set_pointer(
        0, 0, MITSUME_GET_PTR_ENTRY_VERSION(query.ptr.pointer), 0, 0);
    if (query.replication_factor != 1)
      set_value.pointer = mitsume_struct_set_pointer(
          0, 0, MITSUME_GET_PTR_ENTRY_VERSION(query.ptr.pointer), 0, 1);
    else {
      set_value.pointer = mitsume_struct_set_pointer(
          MITSUME_GET_PTR_LH(newspace.ptr.pointer),
          MITSUME_GET_PTR_ENTRY_VERSION(newspace.ptr.pointer),
          MITSUME_GET_PTR_ENTRY_VERSION(query.ptr.pointer), 0, 0);
    }
    read_value.pointer = 0xffffffff;

// MITSUME_TOOL_PRINT_POINTER_KEY(&query.ptr, NULL, key);
// MITSUME_TOOL_PRINT_POINTER_KEY(&set_value, NULL, key);
#ifdef MITSUME_TOOL_DEBUG
    MITSUME_TOOL_PRINT_POINTER_NULL(&query.ptr);
    MITSUME_TOOL_PRINT_POINTER_NULL(&query.shortcut_ptr);
#endif

    // step1-3: issue lock with pointer directly after polling
    cs_id = mitsume_local_thread_get_wr_id(local_inf);
    cs_mr = &thread_metadata->local_ctx_clt
                 ->all_lh_attr[MITSUME_GET_PTR_LH(query.ptr.pointer)];
    userspace_one_cs(ib_ctx, cs_id, local_inf->input_mr[coro_id], cs_mr,
                     guess_value.pointer, set_value.pointer);
    if (coro_id)
      yield_to_another_coro(thread_metadata->local_inf, coro_id, yield);
    userspace_one_poll(ib_ctx, cs_id, cs_mr);
    mitsume_local_thread_put_wr_id(local_inf, cs_id);
    memcpy(&read_value, local_inf->input_space[coro_id],
           sizeof(struct mitsume_ptr));

#ifdef MITSUME_TOOL_DEBUG
    MITSUME_TOOL_PRINT_POINTER(&read_value, NULL);
    MITSUME_TOOL_PRINT_POINTER(&guess_value, NULL);
#endif

    // MITSUME_TOOL_PRINT_POINTER_NULL(&query.ptr);
    // MITSUME_TOOL_PRINT_POINTER_NULL(&query.shortcut_ptr);
    // MITSUME_TOOL_PRINT_POINTER_NULL(&read_value);
    // MITSUME_TOOL_PRINT_POINTER_NULL(&guess_value);
    // MITSUME_TOOL_PRINT_POINTER_NULL(&set_value);

    chasing_count++;
    if (chasing_count > 10000)
      chasing_debug = 1;
    if (chasing_debug) {
      MITSUME_INFO("chasing %d\n", chasing_count);
      MITSUME_TOOL_PRINT_POINTER_NULL(&query.ptr);
      MITSUME_TOOL_PRINT_POINTER_NULL(&query.shortcut_ptr);
      MITSUME_TOOL_PRINT_POINTER_NULL(&read_value);
      MITSUME_TOOL_PRINT_POINTER_NULL(&guess_value);
      MITSUME_TOOL_PRINT_POINTER_NULL(&set_value);
    }
  }

  // step 3 update pointer to the replication format with uncommitted
  if (query.replication_factor != 1) {
    mitsume_tool_setup_meshptr_pointer_header_by_newspace(meshptr_list, &query,
                                                          &newspace, 1);
    for (per_replication = 0; per_replication < query.replication_factor;
         per_replication++) {
      remote_mr[per_replication] =
          &thread_metadata->local_ctx_clt->all_lh_attr[MITSUME_GET_PTR_LH(
              query.replication_ptr[per_replication].pointer)];
    }
    for (per_replication = 0; per_replication < query.replication_factor;
         per_replication++) {
      wr_id[per_replication] = mitsume_local_thread_get_wr_id(local_inf);

      tar_sge = sge_list[per_replication];
      tar_sge[0].addr = (uintptr_t)meshptr_list[per_replication];
      tar_sge[0].length =
          sizeof(struct mitsume_ptr) * (query.replication_factor);
      tar_sge[0].lkey = local_inf->meshptr_mr[coro_id].lkey;

      userspace_one_write_sge(ib_ctx, wr_id[per_replication],
                              remote_mr[per_replication], 0, tar_sge, 1);
    }
    if (coro_id)
      yield_to_another_coro(thread_metadata->local_inf, coro_id, yield);

    for (per_replication = 0; per_replication < query.replication_factor;
         per_replication++) {
      assert(wr_id[per_replication]);
      userspace_one_poll(ib_ctx, wr_id[per_replication],
                         remote_mr[per_replication]);
      mitsume_local_thread_put_wr_id(local_inf, wr_id[per_replication]);
    }

    // step 4 update pointer to the replication format with committed
    mitsume_tool_setup_meshptr_pointer_header_by_newspace(meshptr_list, &query,
                                                          &newspace, 0);
    for (per_replication = 0; per_replication < query.replication_factor;
         per_replication++) {
      tar_sge = sge_list[per_replication];
      tar_sge[0].addr = (uintptr_t)meshptr_list[per_replication];
      tar_sge[0].length =
          sizeof(struct mitsume_ptr) * (query.replication_factor);
      tar_sge[0].lkey = local_inf->meshptr_mr[coro_id].lkey;
      wr_id[per_replication] = mitsume_local_thread_get_wr_id(local_inf);
      userspace_one_write_sge(ib_ctx, wr_id[per_replication],
                              remote_mr[per_replication], 0, tar_sge, 1);
    }
    if (coro_id)
      yield_to_another_coro(thread_metadata->local_inf, coro_id, yield);
    for (per_replication = 0; per_replication < query.replication_factor;
         per_replication++) {
      assert(wr_id[per_replication]);
      userspace_one_poll(ib_ctx, wr_id[per_replication],
                         remote_mr[per_replication]);
      mitsume_local_thread_put_wr_id(local_inf, wr_id[per_replication]);
    }
  }

#ifndef MITSUME_WRITE_OPT_DISABLE_LOCAL_CACHE_UPDATE
  // Update last query place
  // Since this is a write request, it should use newspace to update local
  // hashtable

  mitsume_tool_local_hashtable_operations(
      thread_metadata, key, &newspace, NULL,
      MITSUME_MODIFY); // it's always the latest or new
#endif

  // step5: gc related operations, submitted a gc request
  //[TODO-START] do GC next

  //#ifdef MITSUME_TOOL_DEBUG
  // MITSUME_TOOL_PRINT_POINTER_KEY_REPLICATION(query.replication_ptr,
  // query.replication_factor, key);
  // MITSUME_TOOL_PRINT_POINTER_KEY_REPLICATION(newspace.replication_ptr,
  // query.replication_factor, key); #endif

  if (optional_flag & MITSUME_TOOL_FLAG_GC) {
    // mitsume_tool_gc_submit_request(thread_metadata, key, query.ptr.pointer,
    // newspace.ptr.pointer, query.shortcut_ptr.pointer);
    mitsume_tool_gc_submit_request(thread_metadata, key, &query, &newspace,
                                   MITSUME_TOOL_GC_REGULAR_PROCESSING);
  } else
    MITSUME_PRINT_ERROR("pub sub mode is no longer supported\n");
  // else if(optional_flag&MITSUME_TOOL_FLAG_NONLATEST)
  //{
  //    mitsume_tool_gc_submit_request(thread_metadata, key, &query, &newspace,
  //    MITSUME_TOOL_GC_UPDATE_SHORTCUT_ONLY);
  //}

  // after this while loop, it means compare and swap successfully pass
  // it can return to userspace here

  /*if(MITSUME_TOOL_TRAFFIC_STAT)
  {
      mitsume_tool_load_balancing_record(thread_metadata, &newspace,
  MITSUME_TOOL_LOAD_BALANCING_SOURCE_WRITE);
  }*/
  //[TODO-END] do GC next

  mitsume_tool_end(thread_metadata, coro_id);
  if (MITSUME_TOOL_TRAFFIC_STAT)
    mitsume_tool_load_balancing_record(
        thread_metadata, &newspace, MITSUME_TOOL_LOAD_BALANCING_SOURCE_WRITE);

  return MITSUME_SUCCESS;

abort_write:
  MITSUME_PRINT_ERROR("abort write with error %d\n", local_errno);

  mitsume_tool_end(thread_metadata, coro_id);
  return MITSUME_ERROR;
}

void mitsume_tool_lru_init(void) {
  /*int i;
  for(i=0;i<1<<MITSUME_LRU_BUCKET_BIT;i++)
  {
      MITSUME_TOOL_SHARE_LRU_CACHE[i]._max_size = MITSUME_LRU_SIZE;
  }*/
}
