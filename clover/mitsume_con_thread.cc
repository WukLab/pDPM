#include "mitsume_con_thread.h"

mutex
    MITSUME_CON_NAMER_HASHTABLE_LOCK[1 << MITSUME_CON_NAMER_HASHTABLE_SIZE_BIT];
std::unordered_map<mitsume_key, struct mitsume_hash_struct *>
    MITSUME_CON_NAMER_HASHTABLE[1 << MITSUME_CON_NAMER_HASHTABLE_SIZE_BIT];

mutex MITSUME_CON_GC_HASHTABLE_CURRENT_LOCK
    [1 << MITSUME_CON_NAMER_HASHTABLE_SIZE_BIT];
std::unordered_map<mitsume_key, struct mitsume_gc_hashed_entry *>
    MITSUME_CON_GC_HASHTABLE_CURRENT[1 << MITSUME_CON_NAMER_HASHTABLE_SIZE_BIT];

mutex MITSUME_CON_GC_HASHTABLE_BUFFER_LOCK
    [1 << MITSUME_CON_NAMER_HASHTABLE_SIZE_BIT];
std::unordered_map<uint64_t, struct mitsume_gc_hashed_entry *>
    MITSUME_CON_GC_HASHTABLE_BUFFER[1 << MITSUME_CON_NAMER_HASHTABLE_SIZE_BIT];
// the key of this table is pointer - not key

inline uint64_t
mitsume_con_get_gc_bucket_buffer_hashkey(mitsume_key key,
                                         struct mitsume_ptr *tar_ptr) {
  return hash_min(tar_ptr->pointer, MITSUME_CON_GC_HASHTABLE_SIZE_BIT);
  // return hash_min(key, MITSUME_CON_GC_HASHTABLE_SIZE_BIT);
}

inline void mitsume_con_controller_set_pointer_into_gc_hashed_structure(
    struct mitsume_gc_hashed_entry *forge_backup_update_request,
    void *forge_backup_update_content) {
  forge_backup_update_request->gc_entry.old_ptr[MITSUME_REPLICATION_PRIMARY]
      .pointer = (uint64_t)forge_backup_update_content;
}

void mitsume_con_thread_init(struct mitsume_ctx_con *local_ctx_con) {
  int ret;
  int i;
  // pthread_t *thread_allocator = new
  // pthread_t[MITSUME_CON_ALLOCATOR_THREAD_NUMBER]; int i;

  // initialize hashtable here

  // initialize epoch

  for (i = 0; i < MITSUME_CON_ALLOCATOR_THREAD_NUMBER; i++) {
    ret = pthread_create(&local_ctx_con->thread_allocator[i], NULL,
                         mitsume_con_controller_thread,
                         &local_ctx_con->thread_metadata[i]);
    CPE(ret, "fail to generate thread\n", i);
  }

  // initialize gc
  //[TODO-START]
  //[CAUTION] GC thread will also handle version-mover tasks if
  //MITSUME_GC_NAMDB_DESIGN is defined
  MITSUME_INFO("enter gc thread\n");
  for (i = 0; i < MITSUME_CON_GC_THREAD_NUMBER; i++) {
    local_ctx_con->gc_thread_metadata[i].gc_thread_id = i;
    local_ctx_con->gc_thread_metadata[i].local_ctx_con = local_ctx_con;
    local_ctx_con->gc_thread_metadata[i].gc_allocator_counter = 0;
    local_ctx_con->gc_thread_metadata[i].local_inf =
        mitsume_local_thread_setup(local_ctx_con->ib_ctx, i);
    pthread_create(&local_ctx_con->gc_thread[i], NULL,
                   mitsume_con_controller_gcthread,
                   &(local_ctx_con->gc_thread_metadata[i]));
  }
  MITSUME_INFO("After generating %d gc_thread\n", i);
  //[TODO-END]

  // wake up epoch thread
  local_ctx_con->epoch_thread_metadata.local_ctx_con = local_ctx_con;
  local_ctx_con->epoch_thread_metadata.local_inf =
      mitsume_local_thread_setup(local_ctx_con->ib_ctx, 0);
  local_ctx_con->public_epoch_list =
      new queue<struct mitsume_gc_single_hashed_entry *>;
  pthread_create(&local_ctx_con->epoch_thread, NULL,
                 mitsume_con_controller_epoch_thread,
                 &local_ctx_con->epoch_thread_metadata);

  // wake up management thread
  /*sprintf(management_thread_name, "management-processor");
  local_ctx_con->management_thread = kthread_create((void
  *)mitsume_con_management_thread, local_ctx_con, management_thread_name);
  spin_lock_init(&(local_ctx_con->management_request_list_lock));
  local_ctx_con->management_request_list = kmalloc(sizeof (struct list_head),
  GFP_KERNEL); INIT_LIST_HEAD(local_ctx_con->management_request_list);

  if(IS_ERR(local_ctx_con->management_thread))
  {
      MITSUME_ERROR("fail to generate management thread\n");
      return MITSUME_GENERAL_FAIL;
  }
  wake_up_process(local_ctx_con->management_thread);*/

  MITSUME_INFO("After generating management_thread\n");
  uint32_t stat_count = 0;
  while (1) {
    stat_count++;
    sleep(5);
    // uint32_t size = MITSUME_CON_GC_HASHTABLE_BUFFER.size();
    uint32_t size = 0;
    for (i = 0; i < 1 << MITSUME_CON_NAMER_HASHTABLE_SIZE_BIT; i++) {
      size += MITSUME_CON_GC_HASHTABLE_BUFFER[i].size();
    }
    MITSUME_PRINT("gc hashtable buffer size %d\n", (int)size);
    local_ctx_con->public_epoch_list_lock.lock();
    MITSUME_PRINT("epoch list size %d\n",
                  (int)local_ctx_con->public_epoch_list->size());
    local_ctx_con->public_epoch_list_lock.unlock();
    // if(stat_count%1==0)
    //    mitsume_stat_show();
    /*for (auto it = MITSUME_CON_GC_HASHTABLE_BUFFER.begin(); it !=
    MITSUME_CON_GC_HASHTABLE_BUFFER.end(); ++it )
    {
        mitsume_ptr output_pointer;
        mitsume_key key;
        struct mitsume_gc_hashed_entry *gc_hash_entry = it->second;
        struct mitsume_hash_struct *namer_check;
        output_pointer.pointer = it->first;
        MITSUME_TOOL_PRINT_POINTER_NULL(&output_pointer);
        MITSUME_TOOL_PRINT_GC_POINTER_KEY(&gc_hash_entry->gc_entry.old_ptr[0],
    &gc_hash_entry->gc_entry.new_ptr[0], gc_hash_entry->gc_entry.key); key =
    gc_hash_entry->gc_entry.key; uint64_t bucket = hash_min(key,
    MITSUME_CON_NAMER_HASHTABLE_SIZE_BIT); namer_check =
    MITSUME_CON_NAMER_HASHTABLE[bucket][key];
        MITSUME_TOOL_PRINT_POINTER_NULL(&namer_check->ptr[0]);
    }
    std::cout << std::endl;*/
  };
}

/**
 * mitsume_con_alloc_size_to_rtt_estimation - return estimated RTT for different
 * size request (mainly used by sleep 2RTT things)
 * @size: input size
 * return: return time number (in us)
 */
inline int mitsume_con_alloc_size_to_rtt_estimation(uint32_t size) {
  // this number is measured by ibv_read_lat -a
  if (size < 1024)
    return 2;
  else if (size < 4096)
    return 3;
  else if (size < 8192)
    return 4;
  else if (size < 16384)
    return 5;
  else
    return 10;
}

int mitsume_con_thread_metadata_setup(struct configuration_params *input_arg,
                                      struct mitsume_ctx_con *server_ctx) {
  int i;
  for (i = 0; i < MITSUME_CON_ALLOCATOR_THREAD_NUMBER; i++) {
    server_ctx->thread_metadata[i].thread_id = i;
    server_ctx->thread_metadata[i].node_id = input_arg->machine_id;

    server_ctx->thread_metadata[i].local_ctx_con = server_ctx;
    server_ctx->thread_metadata[i].local_inf =
        mitsume_local_thread_setup(server_ctx->ib_ctx, i);
  }
  return MITSUME_SUCCESS;
}

/**
 * mitsume_con_alloc_get_entry_from_thread - get several entries from thread
 * @thread_metadata: target thread_metadata
 * @new_entry: return entry list
 * @size: request size
 * @target_replication_bucket: target specific replication bucket
 * @request_num: number of request entry
 * return: return available count
 */
int mitsume_con_alloc_get_entry_from_thread(
    struct mitsume_allocator_metadata *thread_metadata,
    struct mitsume_allocator_entry *new_entry, int size,
    int target_replication_bucket, int request_num) {
  int target_list = mitsume_con_alloc_size_to_list_num(size);
  int available_count = 0;
  // struct mitsume_allocator_entry *target_entry;
  struct mitsume_allocator_entry target_entry;
  MITSUME_STAT_ARRAY_ADD(target_replication_bucket, request_num);
  thread_metadata->allocator_lock_branch[target_replication_bucket][target_list]
      .lock();
  while (!(thread_metadata
               ->allocator_node_branch[target_replication_bucket][target_list]
               .empty()) &&
         available_count < request_num) {
    target_entry.ptr.pointer =
        thread_metadata
            ->allocator_node_branch[target_replication_bucket][target_list]
            .front();
    // MITSUME_PRINT("%d %p\n", available_count, target_entry);
    // MITSUME_TOOL_PRINT_POINTER_NULL(&target_entry->ptr);
    thread_metadata
        ->allocator_node_branch[target_replication_bucket][target_list]
        .pop_front();
    new_entry[available_count].ptr.pointer = target_entry.ptr.pointer;
    available_count++;
  }
  thread_metadata->allocator_lock_branch[target_replication_bucket][target_list]
      .unlock();
  return available_count;
}

inline int mitsume_con_controller_thread_pick_replication_bucket(
    struct mitsume_allocator_metadata *thread_metadata,
    struct mitsume_msg *received) {
  int request_replication_bucket;
  int per_bucket;
  uint64_t min_value = MITSUME_I64_MAX;
  uint64_t temp_sum;
  // request_replication_bucket = temp_temp++%MITSUME_NUM_REPLICATION_BUCKET;
  request_replication_bucket = -1;
  for (per_bucket = 0; per_bucket < MITSUME_NUM_REPLICATION_BUCKET;
       per_bucket++) {
    if (received->content.msg_entry.already_available_buckets[per_bucket] ==
        MITSUME_TOOL_LOAD_BALANCING_BUCKET_ALREADY_HAVE) {
      temp_sum = MITSUME_I64_MAX;
    } else {
      temp_sum =
          thread_metadata->local_ctx_con->read_bucket_counter[per_bucket] +
          thread_metadata->local_ctx_con->write_bucket_counter[per_bucket];
    }
    if (min_value > temp_sum) {
      min_value = temp_sum;
      request_replication_bucket = per_bucket;
    }
  }
  if (request_replication_bucket == -1) {
    MITSUME_PRINT_ERROR("fail to find a bucket after tries\n");
    request_replication_bucket = -1;
  }
  // else
  //    MITSUME_INFO("pick %d\n", request_replication_bucket);
  return request_replication_bucket;
}

/**
 * mitsume_con_controller_thread_reply_entry_request: entry point throws the
 * message into this function when the message is asking for more entries
 * @thread_metadata: allocator thread's metadata
 * @mitsume_msg: received message
 * @received_descriptor: reply descriptor
 * return: return success
 */
uint64_t mitsume_con_controller_thread_reply_entry_request(
    struct mitsume_allocator_metadata *thread_metadata,
    struct mitsume_msg *received, int coro_id) {
  uint32_t assigned_number;
  uint32_t request_size;
  int request_replication_bucket;
  uint32_t i;
  struct mitsume_msg *reply = thread_metadata->local_inf->input_space[coro_id];
  struct mitsume_allocator_entry new_entry[MITSUME_CLT_CONSUMER_PER_ASK_NUMS];
  struct mitsume_ctx_con *local_ctx_con = thread_metadata->local_ctx_con;
  struct thread_local_inf *local_inf = thread_metadata->local_inf;

  uint64_t wr_id;
  uint64_t send_wr_id;

  memset(reply, 0, sizeof(struct mitsume_msg));
  MITSUME_STAT_ADD(MITSUME_STAT_CON_ASKED_ENTRY,
                   received->content.msg_entry.entry_number);

  assigned_number = received->content.msg_entry.entry_number;
  if (assigned_number > MITSUME_CLT_CONSUMER_PER_ASK_NUMS) {
    MITSUME_PRINT_ERROR("asking size too big %d:%d\n",
                        received->content.msg_entry.entry_number,
                        MITSUME_CLT_CONSUMER_PER_ASK_NUMS);
    assigned_number = MITSUME_CLT_CONSUMER_PER_ASK_NUMS;
  }
  request_size = received->content.msg_entry.entry_size;

  request_replication_bucket =
      received->content.msg_entry.entry_replication_bucket;
  if (request_replication_bucket == MITSUME_TOOL_LOAD_BALANCING_NOENTRY)
    request_replication_bucket =
        mitsume_con_controller_thread_pick_replication_bucket(thread_metadata,
                                                              received);

  // MITSUME_PRINT("assigned %d request_size %d bucket %d\n",
  // (int)assigned_number, (int)request_size, request_replication_bucket);
  assigned_number = mitsume_con_alloc_get_entry_from_thread(
      thread_metadata, new_entry, request_size, request_replication_bucket,
      assigned_number);
  //    mitsume_con_alloc_get_entry_from_thread(thread_metadata, new_entry, 16,
  //    0, 16);
  // MITSUME_PRINT("assigned number %d\n", (int)assigned_number);
  for (i = 0; i < assigned_number; i++) {
    reply->content.msg_entry.ptr[i].pointer = new_entry[i].ptr.pointer;
    // MITSUME_TOOL_PRINT_POINTER_NULL(&reply->content.msg_entry.ptr[i]);
  }

  reply->msg_header.type = MITSUME_CONTROLLER_ASK_ENTRY_ACK;
  reply->msg_header.des_id = received->msg_header.src_id;
  reply->content.msg_entry.entry_number = assigned_number;
  reply->content.msg_entry.entry_replication_bucket =
      request_replication_bucket;

  reply->end_crc = MITSUME_REPLY_CRC;
  wr_id = mitsume_local_thread_get_wr_id(local_inf);
  mitsume_reply_full_message(local_ctx_con->ib_ctx, wr_id,
                             &received->msg_header,
                             local_inf->input_mr[coro_id], sizeof(mitsume_msg));
  // userspace_one_poll(local_ctx_con->ib_ctx, wr_id,
  // &received->msg_header.reply_attr); mitsume_local_thread_put_wr_id(local_inf,
  // wr_id);
  send_wr_id = wr_id;
  // return assigned_number;
  return send_wr_id;
}

/**
 * mitsume_con_controller_thread_query_namer_table: query local namer table
 * @key: target key
 * @ret_ptr: return entry-request memory space
 * return: return success
 */
int mitsume_con_controller_thread_query_namer_table(
    mitsume_key key, struct mitsume_entry_request *ret_ptr) {
  int found = 0;
  struct mitsume_hash_struct *search_hash_ptr;
  uint64_t bucket = hash_min(key, MITSUME_CON_NAMER_HASHTABLE_SIZE_BIT);

  MITSUME_CON_NAMER_HASHTABLE_LOCK[bucket].lock();
  if (MITSUME_CON_NAMER_HASHTABLE[bucket].find(key) ==
      MITSUME_CON_NAMER_HASHTABLE[bucket].end())
    found = 0;
  else {
    found = 1;
    search_hash_ptr = MITSUME_CON_NAMER_HASHTABLE[bucket][key];
  }

  if (found) {
    // reply.content.msg_entry_request.ptr.pointer =
    // search_hash_ptr->ptr.pointer;
    mitsume_struct_copy_ptr_replication(ret_ptr->ptr, search_hash_ptr->ptr,
                                        search_hash_ptr->replication_factor);
    ret_ptr->shortcut_ptr.pointer = search_hash_ptr->shortcut_ptr.pointer;
    ret_ptr->replication_factor = search_hash_ptr->replication_factor;
    ret_ptr->version = search_hash_ptr->version;
  } else {
    found = 0;
  }
  MITSUME_CON_NAMER_HASHTABLE_LOCK[bucket].unlock();
  return found;
}

/**
 * mitsume_con_controller_thread_process_entry_request: entry point throws the
 * message into this function when the message is in entry request type
 * @thread_metadata: allocator thread's metadata
 * @mitsume_msg: received message
 * @received_descriptor: reply descriptor
 * return: return success
 */
uint64_t mitsume_con_controller_thread_process_entry_request(
    struct mitsume_allocator_metadata *thread_metadata,
    struct mitsume_msg *received, int coro_id) {
  struct thread_local_inf *local_inf = thread_metadata->local_inf;
  struct mitsume_ctx_con *local_ctx_con = thread_metadata->local_ctx_con;
  struct mitsume_entry_request *request =
      &(received->content.msg_entry_request);
  uint64_t send_wr_id = 0;
  // struct mitsume_ctx_con *local_ctx_con = thread_metadata->local_ctx_con;
  switch (request->type) {
  case MITSUME_ENTRY_QUERY: {
    int found = 0;
    uint64_t wr_id;
    struct mitsume_msg *reply =
        thread_metadata->local_inf->input_space[coro_id];
    found = mitsume_con_controller_thread_query_namer_table(
        request->key, &reply->content.msg_entry_request);
    if (!found) {
      mitsume_struct_set_ptr_replication(
          reply->content.msg_entry_request.ptr, MITSUME_MAX_REPLICATION,
          mitsume_struct_set_pointer(0, 0, 0, 0, 0));
      reply->content.msg_entry_request.shortcut_ptr.pointer =
          mitsume_struct_set_pointer(0, 0, 0, 0, 0);
    }

    // MITSUME_PRINT("query %lu found %d\n", (long unsigned int)request->key,
    // found);
    reply->msg_header.type = MITSUME_ENTRY_REQUEST_ACK;
    reply->msg_header.src_id = thread_metadata->node_id;
    reply->msg_header.des_id = received->msg_header.src_id;
    reply->content.msg_entry_request.type = MITSUME_ENTRY_QUERY;
    reply->content.msg_entry_request.key = request->key;
    MITSUME_STAT_ADD(MITSUME_STAT_CON_QUERY_ENTRY, 1);
    /*if(received->content.msg_entry_request.debug_flag)
    {
        int gc_bucket_buffer;
        struct mitsume_gc_hashed_entry *gc_search_ptr_buffer=NULL;
        int count = 0;

        MITSUME_TOOL_PRINT_POINTER_KEY_REPLICATION(reply.content.msg_entry_request.ptr,
    0, request->key); hash_for_each_rcu(MITSUME_CON_GC_HASHTABLE_BUFFER,
    gc_bucket_buffer, gc_search_ptr_buffer, hlist)
        {
            if(gc_search_ptr_buffer->gc_entry.key == request->key)
            {
                MITSUME_TOOL_PRINT_POINTER_KEY_REPLICATION(gc_search_ptr_buffer->gc_entry.old_ptr,
    0, gc_search_ptr_buffer->gc_entry.key);
                MITSUME_TOOL_PRINT_POINTER_KEY_REPLICATION(gc_search_ptr_buffer->gc_entry.new_ptr,
    0, gc_search_ptr_buffer->gc_entry.key); count++;
            }
        }
        MITSUME_INFO("%d buffer in gc\n", count);
    }*/

    reply->end_crc = MITSUME_REPLY_CRC;
    wr_id = mitsume_local_thread_get_wr_id(local_inf);
    mitsume_reply_full_message(
        local_ctx_con->ib_ctx, wr_id, &received->msg_header,
        local_inf->input_mr[coro_id], sizeof(mitsume_msg));
    // userspace_one_poll(local_ctx_con->ib_ctx, wr_id,
    // &received->msg_header.reply_attr);
    // mitsume_local_thread_put_wr_id(local_inf, wr_id);
    send_wr_id = wr_id;
  } break;
  //[TODO] this part is tricky. Found would happen if there are two threads
  //doing open at the same time, and race condition happens
  case MITSUME_ENTRY_OPEN: {
    int found = 0;
    // struct mitsume_hash_struct *search_hash_ptr;
    struct mitsume_hash_struct *current_hash_ptr,
        *backup_create_request_content;
    struct mitsume_shortcut_entry current_shortcut;
    int bucket = hash_min(request->key, MITSUME_CON_NAMER_HASHTABLE_SIZE_BIT);
    int ret;
    uint32_t reply_lh = 0;
    struct mitsume_msg *reply = local_inf->input_space[coro_id];
    current_hash_ptr = (struct mitsume_hash_struct *)mitsume_tool_cache_alloc(
        MITSUME_ALLOCTYPE_HASH_STRUCT);
    backup_create_request_content =
        (struct mitsume_hash_struct *)mitsume_tool_cache_alloc(
            MITSUME_ALLOCTYPE_HASH_STRUCT);

    current_hash_ptr->key = request->key;
    mitsume_struct_copy_ptr_replication(current_hash_ptr->ptr, request->ptr,
                                        request->replication_factor);
    current_hash_ptr->replication_factor = request->replication_factor;
    current_hash_ptr->version = MITSUME_CON_FIRST_VERSION;

    MITSUME_STAT_ADD(MITSUME_STAT_CON_OPEN_ENTRY, 1);

    ret = mitsume_con_alloc_get_shortcut_from_list(thread_metadata,
                                                   &current_shortcut);
    if (!ret) // alloc shortcut successfully
    {
      current_hash_ptr->shortcut_ptr.pointer = current_shortcut.ptr.pointer;
      MITSUME_CON_NAMER_HASHTABLE_LOCK[bucket].lock();

      if (MITSUME_CON_NAMER_HASHTABLE[bucket].find(request->key) ==
          MITSUME_CON_NAMER_HASHTABLE[bucket].end())
        found = 0;
      else
        found = 1;

      if (!found) {
        MITSUME_CON_NAMER_HASHTABLE[bucket][request->key] = current_hash_ptr;
        reply_lh = MITSUME_GET_PTR_LH(current_shortcut.ptr.pointer);

        if (mitsume_struct_setup_entry_request(
                reply, MITSUME_ENTRY_REQUEST_ACK, thread_metadata->node_id,
                received->msg_header.src_id, thread_metadata->thread_id,
                MITSUME_ENTRY_OPEN, request->key, request->ptr,
                &(current_shortcut.ptr),
                request->replication_factor) != MITSUME_SUCCESS) {
          MITSUME_PRINT_ERROR("error in request setup\n");
        }
        // copy the current hash table, this will be used by kthread_run to send
        // a request to backup controller reply will be send out after backup is
        // created
        memcpy(backup_create_request_content, current_hash_ptr,
               sizeof(struct mitsume_hash_struct));
      }
      MITSUME_CON_NAMER_HASHTABLE_LOCK[bucket].unlock();
      // MITSUME_PRINT("ask %lu lh %lu found %d\n", (long unsigned
      // int)request->key, (long unsigned
      // int)MITSUME_GET_PTR_LH(request->ptr[MITSUME_REPLICATION_PRIMARY].pointer),
      // found);
      if (found) {
        if (mitsume_struct_setup_entry_request(
                reply, MITSUME_ENTRY_REQUEST_ACK, thread_metadata->node_id,
                received->msg_header.src_id, thread_metadata->thread_id,
                MITSUME_ENTRY_OPEN, request->key, 0, 0, 0) != MITSUME_SUCCESS)
          MITSUME_PRINT_ERROR("error in request setup\n");
      }
      if (!found) // if the shortcut is created, write the pointer to the
                  // shortcut, build the first base of gc into gc-table 1
      {
        // shortcut related
        struct mitsume_shortcut *writing_shortcut =
            (struct mitsume_shortcut *)
                local_inf->output_space[coro_id]; // CAUTION - msg_reply already
                                                  // occupy input_space
        uint64_t shortcut_wr_id;
        // that's why writing shortcut uses output space which is unoccupied
        // gc related
        int gc_bucket;
        struct mitsume_gc_hashed_entry *gc_h_entry;
        int gc_found = 0;
        assert(reply_lh);

        // set correct offset (real ptr)
        mitsume_struct_copy_ptr_replication(
            writing_shortcut->shortcut_ptr,
            reply->content.msg_entry_request.ptr,
            reply->content.msg_entry_request.replication_factor);

        shortcut_wr_id = mitsume_local_thread_get_wr_id(local_inf);
        userspace_one_write(local_ctx_con->ib_ctx, shortcut_wr_id,
                            local_inf->output_mr[coro_id],
                            sizeof(mitsume_shortcut),
                            &local_ctx_con->all_shortcut_attr[reply_lh], 0);
        userspace_one_poll(local_ctx_con->ib_ctx, shortcut_wr_id,
                           &local_ctx_con->all_shortcut_attr[reply_lh]);
        mitsume_local_thread_put_wr_id(local_inf, shortcut_wr_id);

        // start processing gc
        //[TODO-START]
        // During open, one base should be pushed into GC-hashtable-1 as a
        // linkedlist head

        gc_h_entry = (struct mitsume_gc_hashed_entry *)mitsume_tool_cache_alloc(
            MITSUME_ALLOCTYPE_GC_HASHED_ENTRY);
        mitsume_struct_set_ptr_replication(gc_h_entry->gc_entry.old_ptr,
                                           request->replication_factor, 0);
        mitsume_struct_copy_ptr_replication(
            gc_h_entry->gc_entry.new_ptr, reply->content.msg_entry_request.ptr,
            request->replication_factor);
        // MITSUME_TOOL_PRINT_POINTER_KEY_REPLICATION(gc_h_entry->gc_entry.new_ptr,
        // request->replication_factor, request->key);
        gc_h_entry->gc_entry.key = reply->content.msg_entry_request.key;
        gc_h_entry->gc_entry.replication_factor = request->replication_factor;

        gc_bucket = hash_min(gc_h_entry->gc_entry.key,
                             MITSUME_CON_NAMER_HASHTABLE_SIZE_BIT);
        MITSUME_CON_GC_HASHTABLE_CURRENT_LOCK[gc_bucket].lock();
        if (MITSUME_CON_GC_HASHTABLE_CURRENT[gc_bucket].find(
                gc_h_entry->gc_entry.key) !=
            MITSUME_CON_GC_HASHTABLE_CURRENT[gc_bucket].end()) {
          gc_found = 1;
        }
        if (!gc_found) {
          MITSUME_CON_GC_HASHTABLE_CURRENT[gc_bucket]
                                          [gc_h_entry->gc_entry.key] =
                                              gc_h_entry;
        }
        MITSUME_CON_GC_HASHTABLE_CURRENT_LOCK[gc_bucket].unlock();
        if (gc_found) {
          MITSUME_PRINT_ERROR("gc collision in gc table 1 with key %ld\n",
                              (long unsigned int)gc_h_entry->gc_entry.key);
          MITSUME_TOOL_PRINT_POINTER_KEY_REPLICATION(
              gc_h_entry->gc_entry.new_ptr, 0, request->key);
        }
        //[TODO-END]
      }
      // pass the reply content, descriptor, and backup create request to
      // generated kthread

      {

        uint64_t reply_wr_id;
        reply->end_crc = MITSUME_REPLY_CRC;

        reply_wr_id = mitsume_local_thread_get_wr_id(local_inf);
        mitsume_reply_full_message(
            local_ctx_con->ib_ctx, reply_wr_id, &received->msg_header,
            local_inf->input_mr[coro_id], sizeof(mitsume_msg));
        // userspace_one_poll(local_ctx_con->ib_ctx, reply_wr_id,
        // &received->msg_header.reply_attr);
        // mitsume_local_thread_put_wr_id(local_inf, reply_wr_id);
        send_wr_id = reply_wr_id;

        //[TODO-START]
        // this part is disable currently. And we force controller thread to
        // reply the open request. This has to be handled by gc thread once we
        // have gc functionality
        /*
        struct mitsume_send_backup_create_request_pass_struct *pass_struct;
        struct mitsume_gc_hashed_entry *forge_backup_create_request;
        int target_gc_thread = mitsume_con_pick_gc_thread(thread_metadata);
        int *backup_message_target = kmalloc(sizeof(int) *
        MITSUME_CON_BACKUP_NUM, GFP_KERNEL);

        pass_struct = kmalloc(sizeof(struct
        mitsume_send_backup_create_request_pass_struct), GFP_KERNEL);
        pass_struct->backup_create_request_content =
        backup_create_request_content; pass_struct->reply = reply;
        pass_struct->received_descriptor = received_descriptor;

        mitsume_con_pick_backup_controller(thread_metadata->local_ctx_con->controller_id,
        backup_message_target); pass_struct->target_backup_controller =
        backup_message_target;

        //forge a backup update request
        forge_backup_create_request =
        mitsume_tool_cache_alloc(MITSUME_ALLOCTYPE_GC_HASHED_ENTRY);
        forge_backup_create_request->submitted_epoch =
        MITSUME_GC_SUBMIT_CREATE_PASS;
        mitsume_con_controller_set_pointer_into_gc_hashed_structure(forge_backup_create_request,
        pass_struct);

        spin_lock(&(thread_metadata->local_ctx_con->gc_bucket_lock[target_gc_thread]));
        list_add_tail(&forge_backup_create_request->list,
        &(thread_metadata->local_ctx_con->public_gc_bucket[target_gc_thread].list));
        spin_unlock(&(thread_metadata->local_ctx_con->gc_bucket_lock[target_gc_thread]));*/
        //[TODO-END]
      }
    } else // get available shortcut fail
    {
      MITSUME_PRINT_ERROR("shortcut is not enough\n");
      if (mitsume_struct_setup_entry_request(
              reply, MITSUME_ENTRY_REQUEST_ACK, thread_metadata->node_id,
              received->msg_header.src_id, thread_metadata->thread_id,
              MITSUME_ENTRY_OPEN, request->key, 0, 0, 0) != MITSUME_SUCCESS) {
        MITSUME_PRINT_ERROR("error in request setup\n");
      }
      // send out the reply message
      uint64_t reply_wr_id;
      reply->end_crc = MITSUME_REPLY_CRC;

      reply_wr_id = mitsume_local_thread_get_wr_id(local_inf);
      mitsume_reply_full_message(
          local_ctx_con->ib_ctx, reply_wr_id, &received->msg_header,
          local_inf->input_mr[coro_id], sizeof(mitsume_msg));
      // userspace_one_poll(local_ctx_con->ib_ctx, reply_wr_id,
      // &received->msg_header.reply_attr);
      // mitsume_local_thread_put_wr_id(local_inf, reply_wr_id);
      send_wr_id = reply_wr_id;
    }
  } break;
  default:
    MITSUME_PRINT_ERROR("received error request type from %d:%d in type %d\n",
                        received->msg_header.src_id,
                        received->msg_header.thread_id, request->type);
  }
  return send_wr_id;
}

uint64_t mitsume_con_controller_thread_process_stat(
    struct mitsume_allocator_metadata *thread_metadata,
    struct mitsume_msg *received, int coro_id) {
  struct mitsume_msg *reply = thread_metadata->local_inf->input_space[coro_id];
  struct thread_local_inf *local_inf = thread_metadata->local_inf;
  int per_bucket;
  // struct mitsume_allocator_entry **new_entry;
  uint64_t wr_id;
  uint64_t send_wr_id;

  memset(reply, 0, sizeof(struct mitsume_msg));

  reply->msg_header.type = MITSUME_STAT_UPDATE_ACK;
  reply->msg_header.des_id = received->msg_header.src_id;
  reply->msg_header.src_id = received->msg_header.des_id;

  reply->end_crc = MITSUME_REPLY_CRC;
  wr_id = mitsume_local_thread_get_wr_id(local_inf);
  mitsume_reply_full_message(thread_metadata->local_ctx_con->ib_ctx, wr_id,
                             &received->msg_header,
                             local_inf->input_mr[coro_id], sizeof(mitsume_msg));
  // userspace_one_poll(local_ctx_con->ib_ctx, wr_id,
  // &received->msg_header.reply_attr); mitsume_local_thread_put_wr_id(local_inf,
  // wr_id);
  send_wr_id = wr_id;

  // liteapi_reply_message(&reply, sizeof(struct mitsume_msg),
  // received_descriptor);
  MITSUME_INFO("from %d\n", received->msg_header.src_id);
  for (per_bucket = 0; per_bucket < MITSUME_NUM_REPLICATION_BUCKET;
       per_bucket++) {
    MITSUME_INFO(
        "bucket-%d: %ld %ld\n", per_bucket,
        received->content.msg_stat_message.read_bucket_counter[per_bucket],
        received->content.msg_stat_message.write_bucket_counter[per_bucket]);
    thread_metadata->local_ctx_con->read_bucket_counter[per_bucket] =
        received->content.msg_stat_message.read_bucket_counter[per_bucket];
    thread_metadata->local_ctx_con->write_bucket_counter[per_bucket] =
        received->content.msg_stat_message.write_bucket_counter[per_bucket];
  }
  MITSUME_INFO("============\n");

  MITSUME_STAT_ADD(MITSUME_STAT_CON_STAT_UPDATE, 1);
  return send_wr_id;
}

/**
 * mitsume_con_controller_thread: entry point of controller thread - similar to
 * LITE's waiting_queue_handler (but it's polling lite receive)
 * @thread_metadata: allocator thread's metadata
 * return: return success
 */
void *mitsume_con_controller_thread(void *input_arg) {
  struct mitsume_allocator_metadata *thread_metadata =
      (struct mitsume_allocator_metadata *)input_arg;

  // struct mitsume_large_msg *received;//since it may receive GC request
  struct mitsume_msg *received_msg_ptr;
  struct mitsume_large_msg *received_large_msg_ptr;
  struct mitsume_ctx_con *local_ctx_con = thread_metadata->local_ctx_con;

  MITSUME_INFO("Enter thread model %d\n", thread_metadata->thread_id);
  stick_this_thread_to_core(2 * thread_metadata->thread_id);
  struct ibv_wc input_wc[RSEC_CQ_DEPTH];
  uint32_t received_length;
  uint64_t received_id;
  long long int target_mr;
  long long int target_qp;
  int num_of_poll;
  uint64_t send_wr_id = 0;
  ptr_attr *target_qp_mr_attr;
  int fake_coro;

  assert(MITSUME_CON_ASYNC_MESSAGE_POLL_SIZE < MITSUME_CLT_COROUTINE_NUMBER);
  for (int i = 0; i < MITSUME_CLT_COROUTINE_NUMBER; i++)
    thread_metadata->fake_coro_queue.push(i);

  /*if(thread_metadata->thread_id == 0)
  {
      int i;
      struct mitsume_allocator_entry **new_entry;
      new_entry = new struct mitsume_allocator_entry*[32];
      mitsume_con_alloc_get_entry_from_thread(thread_metadata, new_entry, 16, 0,
  16); for(i=0;i<16;i++)
      {
          MITSUME_PRINT("%d-%llx\n", i, (unsigned long long
  int)new_entry[i]->ptr.pointer);
      }
  }*/
  while (1) {
    assert(!thread_metadata->fake_coro_queue.empty());
    fake_coro = thread_metadata->fake_coro_queue.front();
    thread_metadata->fake_coro_queue.pop();

    num_of_poll = userspace_one_poll_wr(local_ctx_con->ib_ctx->server_recv_cq,
                                        1, input_wc, 0);
    assert(num_of_poll == 1);
    received_length = input_wc[0].byte_len;
    received_id = input_wc[0].wr_id;

    target_qp = RSEC_ID_TO_QP(received_id);
    target_mr = RSEC_ID_TO_RECV_MR(received_id);
    target_qp_mr_attr = local_ctx_con->per_qp_mr_attr_list[target_qp];

    if (received_length == sizeof(struct mitsume_msg)) {
      // MITSUME_PRINT("thread-%d process-%lld \trequest from: %lld\n",
      // thread_metadata->thread_id, target_mr, target_qp);

      received_msg_ptr =
          (struct mitsume_msg *)target_qp_mr_attr[target_mr].addr;
      assert(received_msg_ptr);
      // MITSUME_TOOL_PRINT_MSG_HEADER(received_msg_ptr);

      switch (received_msg_ptr->msg_header.type) {
      case MITSUME_CONTROLLER_ASK_ENTRY: // check how many blocks should be
                                         // assigned
        send_wr_id = mitsume_con_controller_thread_reply_entry_request(
            thread_metadata, received_msg_ptr, fake_coro);
        break;
        //[TODO] it should do metadata keeping for each distributed block in
        //order to guarantee security
      case MITSUME_ENTRY_REQUEST: // ask to open a new key (data is already
                                  // written to a space)
        send_wr_id = mitsume_con_controller_thread_process_entry_request(
            thread_metadata, received_msg_ptr, fake_coro);
        //[TODO] if controller rejects this request (for example, key is already
        //occupied, it should put this key into garbage collections)
        break;
      case MITSUME_GC_EPOCH_FORWARD: {
        die_printf("this function is currently disabled\n");
        struct mitsume_msg *allocator_to_epoch_msg =
            (struct mitsume_msg *)mitsume_tool_cache_alloc(
                MITSUME_ALLOCTYPE_MSG);
        memcpy(allocator_to_epoch_msg, received_msg_ptr,
               sizeof(struct mitsume_msg));
        local_ctx_con->allocator_to_epoch_thread_pipe_lock.lock();
        local_ctx_con->allocator_to_epoch_thread_pipe.push(
            allocator_to_epoch_msg);
        local_ctx_con->allocator_to_epoch_thread_pipe_lock.unlock();
        break;
      }
      /*case MITSUME_MISC_REQUEST:
          mitsume_con_controller_thread_process_misc_request(thread_metadata,
         received_msg_ptr, received_descriptor); break;*/
      case MITSUME_STAT_UPDATE:
        send_wr_id = mitsume_con_controller_thread_process_stat(
            thread_metadata, received_msg_ptr, fake_coro);
        break;
      default:
        MITSUME_PRINT_ERROR("received error from %d:%d in type %d\n",
                            received_msg_ptr->msg_header.src_id,
                            received_msg_ptr->msg_header.thread_id,
                            received_msg_ptr->msg_header.type);
      }
    } else if (received_length == sizeof(struct mitsume_large_msg)) {
      received_large_msg_ptr =
          (struct mitsume_large_msg *)target_qp_mr_attr[target_mr].addr;
      switch (received_large_msg_ptr->msg_header.type) {
      case MITSUME_GC_REQUEST:
        send_wr_id = mitsume_con_controller_thread_process_gc(
            thread_metadata, received_large_msg_ptr, fake_coro);
        break;
      /*case MITSUME_GC_BACKUP_UPDATE_REQUEST:
          {
              struct mitsume_msg reply;
              reply.msg_header.type = MITSUME_GC_BACKUP_UPDATE_REQUEST_ACK;
              reply.msg_header.src_id = thread_metadata->node_id;
              reply.msg_header.des_id = received->msg_header.src_id;
              liteapi_reply_message(&reply, sizeof(struct mitsume_msg),
         received_descriptor);
              MITSUME_STAT_ADD(MITSUME_STAT_CON_RECEIVED_BACKUP_UPDATE_REQUEST,
         1);
          }
          break;*/
      default:
        MITSUME_PRINT_ERROR("received error from %d:%d in type %d\n",
                            received_large_msg_ptr->msg_header.src_id,
                            received_large_msg_ptr->msg_header.thread_id,
                            received_large_msg_ptr->msg_header.type);
      }
    } else {
      MITSUME_PRINT_ERROR("received size %d error\n", received_length);
    }

    struct mitsume_poll *poller_ptr;
    if (send_wr_id) {
      poller_ptr =
          new struct mitsume_poll(send_wr_id, input_wc[0].wr_id, fake_coro);
      thread_metadata->poller_queue.push(poller_ptr);
    } else {
      thread_metadata->fake_coro_queue.push(fake_coro);
    }
    if (thread_metadata->poller_queue.size() >=
        MITSUME_CON_ASYNC_MESSAGE_POLL_SIZE) {
      struct ibv_wc poller_wc[1];
      poller_ptr = thread_metadata->poller_queue.front();
      thread_metadata->poller_queue.pop();

      received_id = poller_ptr->post_recv_wr_id;

      target_qp = RSEC_ID_TO_QP(received_id);
      target_mr = RSEC_ID_TO_RECV_MR(received_id);
      target_qp_mr_attr = local_ctx_con->per_qp_mr_attr_list[target_qp];
      received_msg_ptr =
          (struct mitsume_msg *)target_qp_mr_attr[target_mr].addr;

      userspace_one_poll(local_ctx_con->ib_ctx, poller_ptr->post_send_wr_id,
                         &received_msg_ptr->msg_header.reply_attr);
      mitsume_local_thread_put_wr_id(thread_metadata->local_inf,
                                     poller_ptr->post_send_wr_id);

      poller_wc[0].wr_id = poller_ptr->post_recv_wr_id;
      userspace_refill_used_postrecv(local_ctx_con->ib_ctx,
                                     local_ctx_con->per_qp_mr_attr_list,
                                     poller_wc, 1, MITSUME_MAX_MESSAGE_SIZE);
      thread_metadata->fake_coro_queue.push(poller_ptr->fake_coro);
    }
    // userspace_refill_used_postrecv(local_ctx_con->ib_ctx,
    // local_ctx_con->per_qp_mr_attr_list, input_wc, 1,
    // MITSUME_MAX_MESSAGE_SIZE);
  }
  /*
  MITSUME_INFO("start releasing thread model %d\n", thread_metadata->thread_id);
  mitsume_tool_cache_free(received, MITSUME_ALLOCTYPE_LARGE_MSG);

  //Release all cache in MITSUME_ALLOCTYPE_ALLOCATOR_ENTRY
  for(target_list=0;target_list<MITSUME_CON_ALLOCATOR_SLAB_NUMBER;target_list++)
  {
      struct mitsume_allocator_entry *new_entry=(struct
  mitsume_allocator_entry*)0x1; int i,count_free=0; int ret_number;
      for(i=0;i<MITSUME_NUM_REPLICATION_BUCKET;i++)
      {
          do
          {
              ret_number =
  mitsume_con_alloc_get_entry_from_thread(thread_metadata, &new_entry,
  mitsume_con_alloc_list_num_to_size(target_list), i, 1); if(ret_number)
              {
                  mitsume_tool_cache_free(new_entry,
  MITSUME_ALLOCTYPE_ALLOCATOR_ENTRY); count_free++;
              }
          }while(ret_number);
      }
      MITSUME_INFO("thread %d Free list-%d with %d remaining entries\n",
  thread_metadata->thread_id, target_list, count_free);
  }
  //Release all cache in shortcut
  {
      struct mitsume_shortcut_entry input;
      while(!mitsume_con_alloc_get_shortcut_from_list(thread_metadata, &input));
  }*/

  // thread_metadata->local_ctx_con->thread_distribute_entry[thread_metadata->thread_id]
  // = NULL;
  MITSUME_INFO("leave allocator thread model %d\n", thread_metadata->thread_id);
  return 0;
}

inline void mitsume_con_pick_backup_controller(int current_controller_id,
                                               int *input)
// For now, I didn't choose backup controller based on key. in order to reduce
// send-reply times. It could extend to different backup controller in the future
{
  int controller_iter = current_controller_id;
  int i;
  for (i = 0; i < MITSUME_CON_BACKUP_NUM; i++) {
    controller_iter++;
    if (controller_iter == MITSUME_CON_NUM)
      controller_iter = 0;
    input[i] = controller_iter + MITSUME_FIRST_ID;
  }
}

/**
 * mitsume_con_controller_gcthread: entry point of gc processing thread - take
 * request from allocator thread, put all the entries into available entry
 * @global_ctx_con: global_controller_metadata
 * return: return success
 */
void *mitsume_con_controller_gcthread(void *input_metadata) {
  /*
  struct list_head *list_iterator;

  //delay related things(for 2RTT) are disabled intentionally after using crc
  //and this crc is not related to current impl since we are key-value store
  only now
  //this crc was designed for message type application
  #ifdef MITSUME_GC_SLEEP_TWO_RTT
  ktime_t before_delay;
  ktime_t after_delay;
  long long real_delay;
  long long expect_delay;
          int steps_ret[MITSUME_GC_CON_PER_PROGRESS];
  int steps_connection[MITSUME_GC_CON_PER_PROGRESS*MITSUME_MAX_REPLICATION];
  int steps_poll[MITSUME_GC_CON_PER_PROGRESS*MITSUME_MAX_REPLICATION];
  struct ib_sge empty_sge;
  int current_process_count;
  struct mitsume_ptr empty_ptr;
  int per_replication;
  #endif*/

  int coro_id = MITSUME_CLT_TEMP_COROUTINE_ID;
  struct mitsume_controller_gc_thread_metadata *gc_thread_metadata =
      (struct mitsume_controller_gc_thread_metadata *)input_metadata;
  struct mitsume_ctx_con *local_ctx_con;
  struct thread_local_inf *local_inf = gc_thread_metadata->local_inf;
  int end_flag = 0;
  uint64_t temp_count;

#ifdef MITSUME_GC_SLEEP_TWO_RTT
  chrono::microseconds memset_before_delay;
  chrono::microseconds memset_after_delay;
  chrono::microseconds before_delay;
  chrono::microseconds after_delay;
  uint64_t expect_delay;
  uint64_t real_delay;
  uint32_t check_max_size = 0;
  long long sum_delay_us;
#endif
  int backup_update_accumulate;

  chrono::microseconds gc_start_time;
  chrono::microseconds gc_end_time;

  // struct mitsume_allocator_entry *recycle_entry;

  // struct mitsume_gc_hashed_entry *public_entry;
  queue<struct mitsume_gc_hashed_entry *> private_entry;
  queue<struct mitsume_gc_hashed_entry *> ready_to_submit_backup_update;

  struct mitsume_gc_hashed_entry *ready_to_recycle;
  struct mitsume_gc_single_hashed_entry *per_replication_pointer;
  // struct mitsume_gc_hashed_entry *tmp_hashed_ptr;

  int backup_message_target[MITSUME_CON_BACKUP_NUM];
  struct mitsume_large_msg *backup_update_msg;
  uint32_t target_gc_thread = gc_thread_metadata->gc_thread_id;
  uint32_t old_gc_version;

  MITSUME_INFO("enter gc thread %d\n", gc_thread_metadata->gc_thread_id);
  local_ctx_con = gc_thread_metadata->local_ctx_con;
  mitsume_con_pick_backup_controller(
      gc_thread_metadata->local_ctx_con->controller_id, backup_message_target);
  backup_update_msg =
      (struct mitsume_large_msg *)local_inf->input_space[coro_id];
  UNUSED(backup_update_msg);

  while (!end_flag) {
    // remote all kthread_should_stop related functions
    temp_count = 0;
    if (local_ctx_con->public_gc_bucket[target_gc_thread].empty()) {
      usleep(MITSUME_GC_CON_SLEEP_TIME);
      schedule();
      continue;
    }

    local_ctx_con->gc_bucket_lock[target_gc_thread].lock();
    if (local_ctx_con->public_gc_bucket[target_gc_thread].empty()) {
      local_ctx_con->gc_bucket_lock[target_gc_thread].unlock();
      schedule();
      continue;
    }

    while (temp_count < MITSUME_GC_CON_PER_PROGRESS &&
           !local_ctx_con->public_gc_bucket[target_gc_thread].empty()) {
      private_entry.push(
          local_ctx_con->public_gc_bucket[target_gc_thread].front());
      local_ctx_con->public_gc_bucket[target_gc_thread].pop();
      temp_count++;
    }

    local_ctx_con->gc_bucket_lock[target_gc_thread].unlock();
    backup_update_accumulate = 0;
    // MITSUME_GC_MAX_BACK_UPDATE_PER_ENTRY

    // since we have crc now, disable delay intentionally
    if (MITSUME_GC_MEASURE_TIME_FLAG)
      memset_before_delay = get_current_us();

#ifdef MITSUME_GC_SLEEP_TWO_RTT
    {
      //[CAUTION] only required for no crc implementation
      //[CAUTION-UPDATE] this is no longer a problem since this is a key-value
      //store only application send out all write request to memset header part
      // of entry
      /*
      current_process_count = 0;
      empty_ptr.pointer = 0;
      empty_sge.addr = (uintptr_t) &empty_ptr;
      empty_sge.length = sizeof(struct mitsume_ptr);
      empty_sge.lkey = LITE_KV_TO_DMA_FLAG;
      list_for_each(list_iterator, &(private_entry->list))
      {
          //retrieve entry only
          if(ready_to_recycle->submitted_epoch == MITSUME_GC_SUBMIT_REGULAR)
          {
              //this is for measuring sleep RTT
              uint32_t primary_lh =
      MITSUME_GET_PTR_LH(ready_to_recycle->gc_entry.old_ptr[MITSUME_REPLICATION_PRIMARY].pointer);
              uint32_t primary_size = mitsume_con_alloc_lh_to_size(primary_lh);
              if(primary_size > check_max_size)
              {
                  check_max_size = primary_size;
              }
              //issue memset request to remote header part in each entry
              for(per_replication=0;per_replication<ready_to_recycle->gc_entry.replication_factor;per_replication++)
              {
                  //build sge
                  steps_ret[current_process_count] = liteapi_rdma_multiplesge(
                      MITSUME_GET_PTR_LH(ready_to_recycle->gc_entry.old_ptr[per_replication].pointer),
                      M_WRITE, &empty_sge, 1, LOW_PRIORITY,
                      MITSUME_GET_PTR_OFFSET(ready_to_recycle->gc_entry.old_ptr[per_replication].pointer),
                      MITSUME_GLOBAL_PASSWORD,
      &steps_connection[current_process_count],
      &steps_poll[current_process_count]); current_process_count++;
              }
          }
      }

      //[CAUTION] only required for no crc implementation
      //all impl are no crc now
      //poll all write request (memset)
      //THROUGHPUT IMPLEMENTATION SHOULD BE DONE AS FOLLOW
      //However, in order to get run-time, I use latency implementation above
      current_process_count = 0;
      list_for_each(list_iterator, &(private_entry->list))
      {
          //retrieve entry only
          if(ready_to_recycle->submitted_epoch == MITSUME_GC_SUBMIT_REGULAR)
          {
              for(per_replication=0;per_replication<ready_to_recycle->gc_entry.replication_factor;per_replication++)
              {
                  steps_ret[current_process_count] =
      liteapi_poll_sendcq(steps_connection[current_process_count],
      &steps_poll[current_process_count]);
                  if(mitsume_tool_checkerror(steps_ret[current_process_count],
      steps_connection[current_process_count], __LINE__))
                  {
                      MITSUME_ERROR("Fail to memset remote header\n");
                  }
                  current_process_count++;
              }
          }
      }*/
      if (MITSUME_GC_MEASURE_TIME_FLAG)
        memset_after_delay = get_current_us();
    }
    before_delay = get_current_us();
#endif

    //[TODO-START]
    // submit gc backup date to other controllers
    UNUSED(backup_update_accumulate);
    /*
    list_for_each(list_iterator, &(private_entry->list))
    {
        ready_to_submit_backup_update = list_entry(list_iterator, struct
    mitsume_gc_hashed_entry, list);
        if(ready_to_submit_backup_update->submitted_epoch ==
    MITSUME_GC_SUBMIT_UPDATE)
        {
            //during sleep time(no more sleep after using crc), send out the
    request

            BEAWARE forge_backup_update_content IS ALREADY DISABLED in
    process_gc function NEED to INITIALIZE BEFORE ENABLING this PART OF CODE

            mitsume_con_controller_gcthread_buffer_backup_update_request(gc_thread_metadata,
    ready_to_submit_backup_update, backup_update_msg, backup_update_accumulate);
            backup_update_accumulate++;
            if(backup_update_accumulate == MITSUME_GC_MAX_BACK_UPDATE_PER_ENTRY)
            {
                mitsume_con_controller_gcthread_send_backup_update_request(gc_thread_metadata,
    backup_update_msg, backup_update_accumulate, backup_message_target);
                backup_update_accumulate = 0;
            }
        }
        else if(ready_to_submit_backup_update->submitted_epoch ==
    MITSUME_GC_SUBMIT_CREATE_PASS)
        {
            struct mitsume_struct_send_backup_create_request_pass_struct
    *pass_struct; pass_struct = (struct
    mitsume_struct_send_backup_create_request_pass_struct
    *)mitsume_con_controller_get_pointer_from_gc_hashed_structure(ready_to_submit_backup_update);
            mitsume_con_controller_gcthread_send_backup_create_request_pass(gc_thread_metadata,
    pass_struct);
        }
    }
    if(backup_update_accumulate)//send the remaining request
    {
        mitsume_con_controller_gcthread_send_backup_update_request(gc_thread_metadata,
    backup_update_msg, backup_update_accumulate, backup_message_target);
        backup_update_accumulate = 0;
    }*/

    if (MITSUME_GC_MEASURE_TIME_FLAG) {
      gc_start_time = get_current_us();
    }

    //[TODO-END]

#ifdef MITSUME_GC_SLEEP_TWO_RTT
    // disable 2RTT delay intentionally since we have crc now which can avoid
    // this 2RTT should have 2RTT again since we don't rely on CRC - message type
    // applications are removed sleep for 2RTT things
    after_delay = get_current_us();
    real_delay = (after_delay - before_delay).count();
    expect_delay = mitsume_con_alloc_size_to_rtt_estimation(check_max_size) * 2;

    if (real_delay < expect_delay) {
      usleep(expect_delay - real_delay);
    }
#endif

    while (!private_entry.empty()) {
      uint32_t replication_factor;
      uint32_t per_replication;
      ready_to_recycle = private_entry.front();
      private_entry.pop();

      replication_factor = ready_to_recycle->gc_entry.replication_factor;
      switch (ready_to_recycle->submitted_epoch) {
      case MITSUME_GC_SUBMIT_UPDATE:
      case MITSUME_GC_SUBMIT_CREATE_PASS:
        // since MITSUME_GC_SUBMIT_UPDATE and MITSUME_GC_SUBMIT_CREATE_PASS are
        // already processed above
        break;
      case MITSUME_GC_SUBMIT_FORGE:
      case MITSUME_GC_SUBMIT_REGULAR: {
        for (per_replication = 0; per_replication < replication_factor;
             per_replication++) {
          if (ready_to_recycle->gc_entry.old_ptr[per_replication].pointer ==
              0) // since zero may happen when a key is shrinked and gc
          {
            // MITSUME_STAT_ADD(MITSUME_STAT_TEST7, 1);
            continue;
          }
          old_gc_version = MITSUME_GET_PTR_ENTRY_VERSION(
              ready_to_recycle->gc_entry.old_ptr[per_replication].pointer);

          // the data structure is changed here to split full replication factor
          // into single entry space
          if (MITSUME_GC_MEASURE_TIME_FLAG)
            gc_end_time = get_current_us();
          if (old_gc_version >=
              MITSUME_GC_WRAPUP_VERSION) //[TODO] wraping up gc version happens
                                         //here.
          {
            // MITSUME_PRINT("=wrap happen=\n");
            // MITSUME_TOOL_PRINT_POINTER_NULL(&ready_to_recycle->gc_entry.old_ptr[MITSUME_REPLICATION_PRIMARY]);
            // MITSUME_PRINT("=============\n");
            per_replication_pointer = (struct mitsume_gc_single_hashed_entry *)
                mitsume_tool_cache_alloc(
                    MITSUME_ALLOCTYPE_GC_SINGLE_HASHED_ENTRY);
            per_replication_pointer->gc_entry.old_ptr.pointer =
                ready_to_recycle->gc_entry.old_ptr[per_replication].pointer;

            local_ctx_con->public_epoch_list_lock.lock();
            local_ctx_con->public_epoch_list->push(per_replication_pointer);
            local_ctx_con->public_epoch_list_lock.unlock();
            MITSUME_STAT_ADD(MITSUME_STAT_CON_EPOCHED_UNPROCESSED_GC, 1);
          } else {
            // MITSUME_TOOL_PRINT_POINTER_NULL(&ready_to_recycle->gc_entry.old_ptr[per_replication]);
            struct mitsume_allocator_entry recycle_entry;
            recycle_entry.ptr.pointer = mitsume_struct_set_pointer(
                MITSUME_GET_PTR_LH(
                    ready_to_recycle->gc_entry.old_ptr[per_replication]
                        .pointer),
                0, old_gc_version + 1, 0, 0);
            // MITSUME_TOOL_PRINT_POINTER_NULL(&recycle_entry.ptr);

#ifdef MITSUME_CON_GC_ADD_TO_HEAD
            mitsume_con_alloc_put_entry_into_thread(
                local_ctx_con, &recycle_entry, MITSUME_CON_THREAD_ADD_TO_HEAD);
#else
            mitsume_con_alloc_put_entry_into_thread(
                local_ctx_con, &recycle_entry, MITSUME_CON_THREAD_ADD_TO_TAIL);
#endif
            MITSUME_STAT_ADD(MITSUME_STAT_CON_PROCESSED_GC, 1);
          }
          if (MITSUME_GC_MEASURE_TIME_FLAG) {
            sum_delay_us = (gc_end_time - gc_start_time).count();
#ifdef MITSUME_GC_SLEEP_TWO_RTT
            sum_delay_us = sum_delay_us +
                           (memset_after_delay - memset_before_delay).count();
#endif
            // MITSUME_STAT_ADD(MITSUME_STAT_TEST1, sum_delay_ns);
            // MITSUME_STAT_ADD(MITSUME_STAT_TEST2, 1);
          }
        }
      } break;
      default:
        MITSUME_PRINT_ERROR("gc-thread wrong type %d\n",
                            ready_to_recycle->submitted_epoch);
      }
      mitsume_tool_cache_free(ready_to_recycle,
                              MITSUME_ALLOCTYPE_GC_HASHED_ENTRY);
    }
    schedule();
  }
  MITSUME_INFO("gc thread %d exit\n", gc_thread_metadata->gc_thread_id);
  return NULL;
}

inline uint32_t
mitsume_con_pick_gc_thread(struct mitsume_allocator_metadata *thread_metadata) {
  thread_metadata->gc_task_counter++;
  if (thread_metadata->gc_task_counter == MITSUME_CON_GC_THREAD_NUMBER)
    thread_metadata->gc_task_counter = 0;
  return thread_metadata->gc_task_counter;
}

/**
 * mitsume_con_controller_thread_process_gc: processing point of a message when
 * it's asking for GC related requests
 * @thread_metadata: allocator thread's metadata
 * @mitsume_msg: received message
 * @received_descriptor: reply descriptor
 * return: return success
 */
uint64_t mitsume_con_controller_thread_process_gc(
    struct mitsume_allocator_metadata *thread_metadata,
    struct mitsume_large_msg *received, int coro_id) {
  // push all the received GC into a linked-tracing hashtable structure for
  // future search During linkedlist search, if the head has been changed,
  // remember to modify gc-hashtable-1
  int received_number = received->content.msg_gc_request.gc_number;
  struct thread_local_inf *local_inf = thread_metadata->local_inf;
  struct mitsume_msg *reply = thread_metadata->local_inf->input_space[coro_id];
  int i;
  int change_replication_factor_request = 0;
  uint32_t target_gc_thread;

  uint64_t send_wr_id = 0;
  if (received->msg_header.type ==
      MITSUME_MISC_REQUEST_CHANGE_REPLICATION_FACTOR)
    change_replication_factor_request = 1;

  // reply.content.msg_gc_request.gc_number = received_number;
  reply->content.success_gc_number = received_number;
  target_gc_thread = mitsume_con_pick_gc_thread(thread_metadata);

  for (i = 0; i < received_number; i++) {
    // if fc_entry.old_ptr.pointer in table 1.s newpointer:
    // find all next pointer one by one in table 2 until not found
    // update query table
    // else:
    // put the pointer into table 2

    // gc related
    uint32_t gc_bucket;
    uint32_t gc_bucket_buffer;
    UNUSED(gc_bucket_buffer);
    struct mitsume_gc_hashed_entry *gc_hash_entry;
    struct mitsume_gc_hashed_entry *gc_search_ptr = NULL;
    struct mitsume_gc_hashed_entry *gc_search_ptr_buffer = NULL;
    int gc_found = 0;
    int version_change = 0;
    uint32_t replication_factor;
    // struct mitsume_gc_hashed_entry *forge_hash_entry;

    replication_factor =
        received->content.msg_gc_request.gc_entry[i].replication_factor;

    if (replication_factor == 0 ||
        replication_factor > MITSUME_MAX_REPLICATION) {
      MITSUME_PRINT_ERROR("%u error replication factor\n", replication_factor);
    }
    gc_hash_entry = (struct mitsume_gc_hashed_entry *)mitsume_tool_cache_alloc(
        MITSUME_ALLOCTYPE_GC_HASHED_ENTRY);
    mitsume_struct_copy_ptr_replication(
        gc_hash_entry->gc_entry.old_ptr,
        received->content.msg_gc_request.gc_entry[i].old_ptr,
        replication_factor);
    mitsume_struct_copy_ptr_replication(
        gc_hash_entry->gc_entry.new_ptr,
        received->content.msg_gc_request.gc_entry[i].new_ptr,
        replication_factor);

    // MITSUME_TOOL_PRINT_GC_POINTER_NULL(&gc_hash_entry->gc_entry.old_ptr[0],
    // &gc_hash_entry->gc_entry.new_ptr[0]);

    // CHANGE_LIFE_CYCLE SHOULD avoid check here since it would contain zero for
    // the target cleaned object and it would have old_ptr only, new_ptr should
    // be empty if this entry doesn't want to be removed (most of time, only
    // primary will be empty)

    MITSUME_STRUCT_CHECKNULL_PTR_REPLICATION(gc_hash_entry->gc_entry.old_ptr,
                                             replication_factor);
    MITSUME_STRUCT_CHECKNULL_PTR_REPLICATION(gc_hash_entry->gc_entry.new_ptr,
                                             replication_factor);

    gc_hash_entry->gc_entry.key =
        received->content.msg_gc_request.gc_entry[i].key;
    gc_hash_entry->gc_entry.replication_factor =
        received->content.msg_gc_request.gc_entry[i].replication_factor;

    // MITSUME_TOOL_PRINT_POINTER_KEY_REPLICATION(gc_hash_entry->gc_entry.old_ptr,
    // replication_factor, gc_hash_entry->gc_entry.key);
    gc_bucket = hash_min(gc_hash_entry->gc_entry.key,
                         MITSUME_CON_NAMER_HASHTABLE_SIZE_BIT);

    MITSUME_CON_GC_HASHTABLE_CURRENT_LOCK[gc_bucket].lock();

    // check table-1
    if (MITSUME_CON_GC_HASHTABLE_CURRENT[gc_bucket].find(
            gc_hash_entry->gc_entry.key) !=
        MITSUME_CON_GC_HASHTABLE_CURRENT[gc_bucket].end()) {
      // MITSUME_TOOL_PRINT_POINTER_KEY_REPLICATION(gc_search_ptr->gc_entry.new_ptr,
      // replication_factor, gc_hash_entry->gc_entry.key);
      gc_search_ptr =
          MITSUME_CON_GC_HASHTABLE_CURRENT[gc_bucket]
                                          [gc_hash_entry->gc_entry.key];
      if (gc_search_ptr->gc_entry.new_ptr[MITSUME_REPLICATION_PRIMARY]
              .pointer ==
          gc_hash_entry->gc_entry.old_ptr[MITSUME_REPLICATION_PRIMARY]
              .pointer) {
        MITSUME_STRUCT_CHECKEQUAL_PTR_REPLICATION(
            gc_search_ptr->gc_entry.new_ptr, gc_hash_entry->gc_entry.old_ptr,
            replication_factor);
        gc_found = 1;
      }
    }
    // MITSUME_PRINT("found or not %d\n", gc_found);

    // forge a gc_hashed_entry which will be submitted into gcthread through
    // public_bucket later this function is disable

    // MITSUME_TOOL_PRINT_GC_POINTER_KEY(&gc_hash_entry->gc_entry.old_ptr[0],
    // &gc_hash_entry->gc_entry.new_ptr[0], gc_hash_entry->gc_entry.key);

    // not found in table-1

    if (!gc_found) {
      // if this is a change replication factor request, it needs further
      // process here. However, this feature is currently disabled insert into
      // table 2
      if (change_replication_factor_request) {
      } else {
        uint64_t target_hash =
            gc_hash_entry->gc_entry.old_ptr[MITSUME_REPLICATION_PRIMARY]
                .pointer;
        gc_bucket_buffer = mitsume_con_get_gc_bucket_buffer_hashkey(
            gc_hash_entry->gc_entry.key,
            &gc_hash_entry->gc_entry.old_ptr[MITSUME_REPLICATION_PRIMARY]);
        MITSUME_CON_GC_HASHTABLE_BUFFER_LOCK[gc_bucket_buffer].lock();
        MITSUME_CON_GC_HASHTABLE_BUFFER[gc_bucket_buffer][target_hash] =
            gc_hash_entry;
        MITSUME_CON_GC_HASHTABLE_BUFFER_LOCK[gc_bucket_buffer].unlock();
        // MITSUME_TOOL_PRINT_GC_POINTER_NULL(&gc_hash_entry->gc_entry.old_ptr[0],
        // &gc_hash_entry->gc_entry.new_ptr[0]);
      }
    }
    if (gc_found) {
      // if this is a change replication factor request, it needs further
      // process here. However, this feature is currently disabled
      if (change_replication_factor_request) {
      } else {
        int link_flag = 1;
        struct mitsume_gc_hashed_entry *next_hash_entry = gc_hash_entry;
        struct mitsume_gc_hashed_entry *ready_to_remove_from_buffer;
        uint64_t target_hash;
        int link_count = 0;
        while (true) {
          // check table 2
          link_flag = 0;
          gc_search_ptr_buffer = NULL;
          gc_bucket_buffer = mitsume_con_get_gc_bucket_buffer_hashkey(
              next_hash_entry->gc_entry.key,
              &next_hash_entry->gc_entry.new_ptr[MITSUME_REPLICATION_PRIMARY]);
          // if(link_count)
          //    MITSUME_TOOL_PRINT_GC_POINTER_NULL(&next_hash_entry->gc_entry.old_ptr[MITSUME_REPLICATION_PRIMARY],
          //    &next_hash_entry->gc_entry.new_ptr[MITSUME_REPLICATION_PRIMARY]);
          target_hash =
              next_hash_entry->gc_entry.new_ptr[MITSUME_REPLICATION_PRIMARY]
                  .pointer;
          MITSUME_CON_GC_HASHTABLE_BUFFER_LOCK[gc_bucket_buffer].lock();
          if (MITSUME_CON_GC_HASHTABLE_BUFFER[gc_bucket_buffer].find(
                  target_hash) !=
              MITSUME_CON_GC_HASHTABLE_BUFFER[gc_bucket_buffer].end())
          // hit
          {
            link_flag = 1;
            gc_search_ptr_buffer =
                MITSUME_CON_GC_HASHTABLE_BUFFER[gc_bucket_buffer][target_hash];
            if ((gc_search_ptr_buffer->gc_entry.key !=
                 received->content.msg_gc_request.gc_entry[i].key) ||
                (gc_search_ptr_buffer->gc_entry.key !=
                 received->content.msg_gc_request.gc_entry[i].key))
              MITSUME_PRINT_ERROR(
                  "key doesn't match %llu\n",
                  (unsigned long long int)received->content.msg_gc_request
                      .gc_entry[i]
                      .key);
            MITSUME_CON_GC_HASHTABLE_BUFFER[gc_bucket_buffer].erase(
                target_hash);
            // MITSUME_TOOL_PRINT_POINTER_NULL(&next_hash_entry->gc_entry.new_ptr[MITSUME_REPLICATION_PRIMARY]);
          }
          MITSUME_CON_GC_HASHTABLE_BUFFER_LOCK[gc_bucket_buffer].unlock();

          /*if(link_count)
          {
              MITSUME_PRINT("link flag%d\n", link_flag);
              MITSUME_TOOL_PRINT_POINTER_NULL((struct mitsume_ptr
          *)&target_hash); std::unordered_map<uint64_t, struct
          mitsume_gc_hashed_entry*>::iterator it =
          MITSUME_CON_GC_HASHTABLE_BUFFER.begin();
              while(it!=MITSUME_CON_GC_HASHTABLE_BUFFER.end())
              {
                  uint64_t test_key = it->first;
                  MITSUME_TOOL_PRINT_POINTER_NULL((struct mitsume_ptr
          *)&test_key);
                  MITSUME_TOOL_PRINT_GC_POINTER_NULL(&it->second->gc_entry.old_ptr[MITSUME_REPLICATION_PRIMARY],
          &it->second->gc_entry.new_ptr[MITSUME_REPLICATION_PRIMARY]);
                  MITSUME_PRINT("test %d\n", test_key == target_hash);
                  it++;
              }
          }*/

          // MITSUME_STAT_ADD(MITSUME_STAT_TEST2, 1);
          if (link_flag) {
            // CHANGE_LIFE_CYCLE SHOULD CHECK HERE SINCE IT SHOULD NOT BE HERE
            if (change_replication_factor_request) {
              MITSUME_PRINT_ERROR("significant error, change replication "
                                  "factor should not be here\n");
            }

            thread_metadata->internal_gc_buffer.push(next_hash_entry);
            // push the collected entry into queue
            version_change++;
            // move to the next
            next_hash_entry = gc_search_ptr_buffer;
            link_count++;
          } else {
            int found = 0;
            struct mitsume_hash_struct *search_hash_ptr;
            uint32_t bucket =
                hash_min(received->content.msg_gc_request.gc_entry[i].key,
                         MITSUME_CON_NAMER_HASHTABLE_SIZE_BIT);

            // struct mitsume_hash_struct *forge_backup_update_content;
            struct mitsume_gc_hashed_entry *forge_backup_update_request;
            // forge_backup_update_content = (struct mitsume_hash_struct
            // *)mitsume_tool_cache_alloc(MITSUME_ALLOCTYPE_HASH_STRUCT);
            forge_backup_update_request =
                (struct mitsume_gc_hashed_entry *)mitsume_tool_cache_alloc(
                    MITSUME_ALLOCTYPE_GC_HASHED_ENTRY);

            if (gc_search_ptr->gc_entry.key !=
                received->content.msg_gc_request.gc_entry[i].key) {
              MITSUME_PRINT_ERROR(
                  "key doesn't match %llu",
                  (unsigned long long int)received->content.msg_gc_request
                      .gc_entry[i]
                      .key);
            }
            // update table-1
            mitsume_struct_copy_ptr_replication(
                gc_search_ptr->gc_entry.old_ptr,
                next_hash_entry->gc_entry.old_ptr, replication_factor);
            mitsume_struct_copy_ptr_replication(
                gc_search_ptr->gc_entry.new_ptr,
                next_hash_entry->gc_entry.new_ptr, replication_factor);
            if (gc_search_ptr->gc_entry.replication_factor !=
                replication_factor) {
              MITSUME_PRINT_ERROR(
                  "replication factor doesn't match %lu:%lu\n",
                  (unsigned long int)gc_search_ptr->gc_entry.replication_factor,
                  (unsigned long int)replication_factor);
            }

            // push the collected entry into queue
            thread_metadata->internal_gc_buffer.push(next_hash_entry);
            version_change++;

            // update query table
            MITSUME_CON_NAMER_HASHTABLE_LOCK[bucket].lock();

            if (MITSUME_CON_NAMER_HASHTABLE[bucket].find(
                    received->content.msg_gc_request.gc_entry[i].key) !=
                MITSUME_CON_NAMER_HASHTABLE[bucket].end()) {
              search_hash_ptr = MITSUME_CON_NAMER_HASHTABLE
                  [bucket][received->content.msg_gc_request.gc_entry[i].key];
              mitsume_struct_copy_ptr_replication(
                  search_hash_ptr->ptr, gc_search_ptr->gc_entry.new_ptr,
                  replication_factor);
              // memcpy(forge_backup_update_content, search_hash_ptr,
              // sizeof(struct mitsume_hash_struct));
              found = 1;
            } else
              MITSUME_PRINT_ERROR(
                  "cannot find %llu in NAMER_TABLE\n",
                  (unsigned long long int)received->content.msg_gc_request
                      .gc_entry[i]
                      .key);
            MITSUME_CON_NAMER_HASHTABLE_LOCK[bucket].unlock();

            // forge a backup update request
            forge_backup_update_request->gc_entry.key =
                received->content.msg_gc_request.gc_entry[i].key;
            forge_backup_update_request->submitted_epoch =
                MITSUME_GC_SUBMIT_UPDATE;
            // mitsume_con_controller_set_pointer_into_gc_hashed_structure(forge_backup_update_request,
            // forge_backup_update_content);

            thread_metadata->local_ctx_con->gc_bucket_lock[target_gc_thread]
                .lock();
            thread_metadata->local_ctx_con->public_gc_bucket[target_gc_thread]
                .push(forge_backup_update_request);
            int push_num = 0;
            while (!thread_metadata->internal_gc_buffer.empty()) {
              ready_to_remove_from_buffer =
                  thread_metadata->internal_gc_buffer.front();
              thread_metadata->internal_gc_buffer.pop();
              ready_to_remove_from_buffer->submitted_epoch =
                  MITSUME_GC_SUBMIT_REGULAR; // all of them should be submitted
                                             // as regular
              thread_metadata->local_ctx_con->public_gc_bucket[target_gc_thread]
                  .push(ready_to_remove_from_buffer);
              push_num++;
            }
            // MITSUME_PRINT("gc %d-%d\n", push_num,
            // (int)MITSUME_CON_GC_HASHTABLE_BUFFER.size()); the last one should
            // be changed into update since it points to the latest table
            // However, the lock is still ocupied by this thread.
            // Therefore it's safe to manipulate the data directly
            thread_metadata->local_ctx_con->gc_bucket_lock[target_gc_thread]
                .unlock();
            if (!found)
              MITSUME_PRINT_ERROR(
                  "key %ld is not found in namer table\n",
                  (long unsigned int)received->content.msg_gc_request
                      .gc_entry[i]
                      .key);
            break;
          }
        }
      }
    }
    MITSUME_CON_GC_HASHTABLE_CURRENT_LOCK[gc_bucket].unlock();
  }
  // reply.content.msg_gc_request.gc_number = received_number;
  reply->msg_header.type = MITSUME_GC_REQUEST_ACK;
  reply->msg_header.des_id = received->msg_header.src_id;
  reply->msg_header.src_id = thread_metadata->node_id;

  // if(received_descriptor)//it could be NULL if the message is generated from
  // local management thread
  if (received->msg_header.reply_attr
          .addr) // it could be NULL if the message is generated from local
                 // management thread
  {
    uint64_t reply_wr_id;
    reply->end_crc = MITSUME_REPLY_CRC;
    reply_wr_id = mitsume_local_thread_get_wr_id(local_inf);
    mitsume_reply_full_message(thread_metadata->local_ctx_con->ib_ctx,
                               reply_wr_id, &received->msg_header,
                               local_inf->input_mr[coro_id],
                               sizeof(mitsume_msg));
    // userspace_one_poll(thread_metadata->local_ctx_con->ib_ctx, reply_wr_id,
    // &received->msg_header.reply_attr);
    // mitsume_local_thread_put_wr_id(local_inf, reply_wr_id);
    send_wr_id = reply_wr_id;
    // MITSUME_PRINT("reply one message\n");
  }
  // MITSUME_PRINT("finish one cycle - %d\n", received_number);
  MITSUME_STAT_ADD(MITSUME_STAT_CON_RECEIVED_GC, received_number);
  return send_wr_id;
}

/**
 * mitsume_con_controller_epoch_thread: this thread process full-recycled
 * (outdated) entry and moves epoch forward when the number of outdated entry is
 * over a threshold
 * @global_ctx_con: global controller's context
 * return: return success
 */
void *mitsume_con_controller_epoch_thread(void *input_metadata) {
  int coro_id = MITSUME_CLT_TEMP_COROUTINE_ID;
  struct mitsume_controller_epoch_thread_metadata *epoch_metadata =
      (struct mitsume_controller_epoch_thread_metadata *)input_metadata;
  struct mitsume_ctx_con *local_ctx_con = epoch_metadata->local_ctx_con;
  struct thread_local_inf *local_inf = epoch_metadata->local_inf;
  int end_flag = 0;
  struct mitsume_msg *send_msg, *received_msg, *pipe_msg;
  int each_client;
  int each_client_start = MITSUME_CON_NUM;
  int each_controller;
  int each_controller_start = MITSUME_FIRST_ID;
  struct mitsume_gc_single_hashed_entry *ready_to_move;
  struct mitsume_gc_single_hashed_entry *ready_to_recycle;
  struct mitsume_allocator_entry recycle_entry;
  queue<struct mitsume_gc_single_hashed_entry *> *next_epoch_list;
  queue<struct mitsume_gc_single_hashed_entry *> *private_epoch_list;
  queue<struct mitsume_gc_single_hashed_entry *> ready_epoch_list;
  int recycle_counter;
  int contiguous_sleep_count = 1;
  int master_controller = 0;
  // int received_length;
  // uintptr_t received_descriptor;

  if (local_ctx_con->controller_id == MITSUME_MASTER_CON)
    master_controller = 1;
  if (master_controller)
    MITSUME_INFO("enter epoch thread master\n");
  else
    MITSUME_INFO("enter epoch thread worker\n");

  send_msg = local_inf->input_space[coro_id];
  received_msg = local_inf->output_space[coro_id];
  while (!end_flag) {
    usleep(MITSUME_GC_CON_EPOCH_SLEEP_TIME *
           1000); // check the epoch queue every 500 ms
    sleep(2);
    if (master_controller) {
      if (local_ctx_con->public_epoch_list->size() * contiguous_sleep_count <
          MITSUME_GC_CON_EPOCH_TRIGGER_SIZE) {
        if (contiguous_sleep_count < MITSUME_GC_CON_EPOCH_TRIGGER_SIZE)
          contiguous_sleep_count++;
        continue;
      }
      contiguous_sleep_count = 1;
      // sync epoch on all nodes
      MITSUME_INFO("epoch %d end\n", local_ctx_con->gc_current_epoch);
      for (each_client = each_client_start;
           each_client < each_client_start + MITSUME_CLT_NUM; each_client++) {
        send_msg->msg_header.type = MITSUME_GC_EPOCH_FORWARD;
        send_msg->content.msg_gc_epoch_forward.request_epoch_number =
            local_ctx_con->gc_current_epoch + MITSUME_GC_EPOCH_STEPSIZE;

        send_msg->msg_header.reply_attr.addr =
            (uint64_t)local_inf->output_mr[coro_id]->addr;
        send_msg->msg_header.reply_attr.rkey =
            local_inf->output_mr[coro_id]->rkey;
        send_msg->msg_header.reply_attr.machine_id = local_ctx_con->node_id;

        received_msg->end_crc = MITSUME_WAIT_CRC;
        // MITSUME_INFO("ready-send to %d\n", each_client);
        mitsume_send_full_message(
            local_ctx_con->ib_ctx, local_inf, local_inf->input_mr[coro_id],
            local_inf->output_mr[coro_id], &send_msg->msg_header,
            local_ctx_con->node_id, each_client, sizeof(mitsume_msg));
        // MITSUME_INFO("get reply from to %d\n", each_client);

        if (received_msg->msg_header.type != MITSUME_GC_EPOCH_FORWARD_ACK) {
          MITSUME_INFO("error type %d\n", received_msg->msg_header.type);
        }
      }
      for (each_controller = each_controller_start;
           each_controller < each_controller_start + MITSUME_CON_NUM - 1;
           each_controller++)
      // minus one since it would not pass to master controller itself
      {
        send_msg->msg_header.type = MITSUME_GC_EPOCH_FORWARD;
        send_msg->content.msg_gc_epoch_forward.request_epoch_number =
            local_ctx_con->gc_current_epoch + MITSUME_GC_EPOCH_STEPSIZE;
        send_msg->msg_header.reply_attr.addr =
            (uint64_t)local_inf->output_mr[coro_id]->addr;
        send_msg->msg_header.reply_attr.rkey =
            local_inf->output_mr[coro_id]->rkey;
        send_msg->msg_header.reply_attr.machine_id = local_ctx_con->node_id;

        received_msg->end_crc = MITSUME_WAIT_CRC;

        mitsume_send_full_message(
            local_ctx_con->ib_ctx, local_inf, local_inf->input_mr[coro_id],
            local_inf->output_mr[coro_id], &send_msg->msg_header,
            local_ctx_con->node_id, each_controller, sizeof(mitsume_msg));
        if (received_msg->msg_header.type != MITSUME_GC_EPOCH_FORWARD_ACK) {
          MITSUME_INFO("error type %d\n", received_msg->msg_header.type);
        }
      }
      local_ctx_con->gc_current_epoch =
          local_ctx_con->gc_current_epoch + MITSUME_GC_EPOCH_STEPSIZE;
    } else {
      while (local_ctx_con->allocator_to_epoch_thread_pipe.size() == 0) {
        schedule();
      }
      local_ctx_con->allocator_to_epoch_thread_pipe_lock.lock();
      pipe_msg = local_ctx_con->allocator_to_epoch_thread_pipe.front();
      local_ctx_con->allocator_to_epoch_thread_pipe.pop();
      local_ctx_con->allocator_to_epoch_thread_pipe_lock.unlock();

      memcpy(received_msg, pipe_msg, sizeof(struct mitsume_msg));
      mitsume_tool_cache_free(pipe_msg, MITSUME_ALLOCTYPE_MSG);
      /*do
      {
          schedule();
          received_length = liteapi_receive_message(port, received_msg,
      (int)sizeof(struct mitsume_msg), &received_descriptor, LITE_UNBLOCK_CALL);
          if(kthread_should_stop() && received_length == 0)
          {
              end_flag = 1;
              break;
          }
      }while(!received_length);*/
      if (received_msg->msg_header.type != MITSUME_GC_EPOCH_FORWARD) {
        MITSUME_PRINT_ERROR("wrong message %d\n",
                            received_msg->msg_header.type);
      } else {
        if (received_msg->content.msg_gc_epoch_forward.request_epoch_number <=
                local_ctx_con->gc_current_epoch ||
            received_msg->content.msg_gc_epoch_forward.request_epoch_number !=
                local_ctx_con->gc_current_epoch + MITSUME_GC_EPOCH_STEPSIZE) {
          MITSUME_PRINT_ERROR(
              "wrong epoch:%d (current:%d)\n",
              received_msg->content.msg_gc_epoch_forward.request_epoch_number,
              local_ctx_con->gc_current_epoch);
          send_msg->content.msg_gc_epoch_forward.request_epoch_number = 0;
          send_msg->msg_header.type = MITSUME_GC_EPOCH_FORWARD_ACK;
          local_ctx_con->gc_current_epoch =
              received_msg->content.msg_gc_epoch_forward.request_epoch_number;
          // continue;
        } else {
          send_msg->content.msg_gc_epoch_forward.request_epoch_number =
              local_ctx_con->gc_current_epoch;
          send_msg->msg_header.type = MITSUME_GC_EPOCH_FORWARD_ACK;

          local_ctx_con->gc_current_epoch =
              received_msg->content.msg_gc_epoch_forward.request_epoch_number;
          // MITSUME_STAT_SET(MITSUME_CURRENT_EPOCH,
          // local_ctx_con->gc_current_epoch);
        }
        uint64_t wr_id;
        send_msg->end_crc = MITSUME_REPLY_CRC;
        wr_id = mitsume_local_thread_get_wr_id(local_inf);
        mitsume_reply_full_message(
            local_ctx_con->ib_ctx, wr_id, &received_msg->msg_header,
            local_inf->input_mr[coro_id], sizeof(mitsume_msg));
        userspace_one_poll(local_ctx_con->ib_ctx, wr_id,
                           &received_msg->msg_header.reply_attr);
        mitsume_local_thread_put_wr_id(local_inf, wr_id);
      }
    }
    MITSUME_INFO("epoch %d start processing\n",
                 local_ctx_con->gc_current_epoch);

    // move public list to private, and allocate a new list for public
    next_epoch_list = new queue<struct mitsume_gc_single_hashed_entry *>;

    local_ctx_con->public_epoch_list_lock.lock();
    private_epoch_list = local_ctx_con->public_epoch_list;
    local_ctx_con->public_epoch_list = next_epoch_list;
    local_ctx_con->public_epoch_list_lock.unlock();

    // move all recycle entry into ready list
    while (!private_epoch_list->empty()) {
      ready_to_move = private_epoch_list->front();
      ready_to_move->submitted_epoch = local_ctx_con->gc_current_epoch;
      private_epoch_list->pop();
      ready_epoch_list.push(ready_to_move);
    }

    // move outdated entry (safe from epoch checking) from ready list to
    // available again
    recycle_counter = 0;
    while (!ready_epoch_list.empty()) {
      ready_to_recycle = ready_epoch_list.front();
      if (ready_to_recycle->submitted_epoch + MITSUME_GC_EPOCH_THRESHOLD + 1 >
          local_ctx_con->gc_current_epoch) {
        break;
      } else {
        ready_epoch_list.pop();
        recycle_entry.ptr.pointer = mitsume_struct_set_pointer(
            MITSUME_GET_PTR_LH(ready_to_recycle->gc_entry.old_ptr.pointer), 0,
            MITSUME_ENTRY_MIN_VERSION, 0, 0);
        mitsume_con_alloc_put_entry_into_thread(local_ctx_con, &recycle_entry,
                                                MITSUME_CON_THREAD_ADD_TO_HEAD);
        MITSUME_STAT_ADD(MITSUME_STAT_CON_PROCESSED_GC, 1);
        // MITSUME_STAT_ADD(MITSUME_STAT_CON_EPOCHED_GC, 1);
        mitsume_tool_cache_free(ready_to_recycle,
                                MITSUME_ALLOCTYPE_GC_SINGLE_HASHED_ENTRY);
        recycle_counter++;
      }
    }
    // atomic_sub(recycle_counter, &(local_ctx_con->public_epoch_list_num));
    MITSUME_STAT_SUB(MITSUME_STAT_CON_EPOCHED_UNPROCESSED_GC, recycle_counter);
    // kfree(private_epoch_list);

    MITSUME_STAT_SET(MITSUME_CURRENT_EPOCH, local_ctx_con->gc_current_epoch);
    // MITSUME_INFO("epoch %d start\n", local_ctx_con->gc_current_epoch);
  }
  MITSUME_INFO("epoch thread exit\n");
  return NULL;
}
