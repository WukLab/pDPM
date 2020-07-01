
#include "mitsume_tool_gc.h"
/**
 * mitsume_tool_gc_submit_request: submit gc request
 * @thread_metadata: respected thread_metadata
 * @key: target key
 * @old_ptr: old mitsume pointer
 * @new_ptr: new mitsume pointer
 * return: return success
 */
// inline int mitsume_tool_gc_submit_request(struct mitsume_consumer_metadata
// *thread_metadata, mitsume_key key, uint64_t old_ptr, uint64_t new_ptr,
// uint64_t shortcut_ptr)
int mitsume_tool_gc_submit_request(
    struct mitsume_consumer_metadata *thread_metadata, mitsume_key key,
    struct mitsume_tool_communication *old_entry,
    struct mitsume_tool_communication *new_entry, int gc_mode) {
  // need to use key to put the request into the correct controller linked-list
  // async processing queue
  struct mitsume_gc_thread_request *request =
      (struct mitsume_gc_thread_request *)mitsume_tool_cache_alloc(
          MITSUME_ALLOCTYPE_GC_THREAD_REQUEST);
  int target_gc_thread_id;

  request->gc_entry.key = key;
  request->gc_mode = gc_mode;

  mitsume_struct_copy_ptr_replication(request->gc_entry.old_ptr,
                                      old_entry->replication_ptr,
                                      old_entry->replication_factor);
  mitsume_struct_copy_ptr_replication(request->gc_entry.new_ptr,
                                      new_entry->replication_ptr,
                                      new_entry->replication_factor);
  if (old_entry->replication_factor != new_entry->replication_factor) {
    MITSUME_PRINT_ERROR("replication factor doesn't match %d:%d\n",
                        (int)old_entry->replication_factor,
                        (int)new_entry->replication_factor);
  }
  request->gc_entry.replication_factor = old_entry->replication_factor;
  MITSUME_STRUCT_CHECKNULL_PTR_REPLICATION(
      request->gc_entry.old_ptr, request->gc_entry.replication_factor);
  MITSUME_STRUCT_CHECKNULL_PTR_REPLICATION(
      request->gc_entry.new_ptr, request->gc_entry.replication_factor);
  // MITSUME_TOOL_PRINT_POINTER_KEY_REPLICATION(request->gc_entry.old_ptr,
  // request->gc_entry.replication_factor, key);
  // MITSUME_TOOL_PRINT_POINTER_KEY_REPLICATION(request->gc_entry.new_ptr,
  // request->gc_entry.replication_factor, key);
  // MITSUME_TOOL_PRINT_GC_POINTER_KEY(&request->gc_entry.old_ptr[0],
  // &request->gc_entry.new_ptr[0], key);
  request->shortcut_ptr.pointer = old_entry->shortcut_ptr.pointer;
  // MITSUME_TOOL_PRINT_POINTER_KEY(&old_ptr, &new_ptr, key);
  // MITSUME_INFO("%lld %llx %llx\n", request->gc_entry.key,
  // request->gc_entry.old_ptr.pointer, request->gc_entry.new_ptr.pointer);

  // target_gc_thread_id = thread_metadata->target_gc_thread_id;

  // target gc thread is determined during hashtable installing. Therefore, it
  // doesn't need to do modular in each request
  target_gc_thread_id = old_entry->target_gc_thread;
  request->target_controller = old_entry->target_gc_controller;

  // MITSUME_PRINT("submit to %d\n", target_gc_thread_id);
  thread_metadata->local_ctx_clt->gc_processing_queue_lock[target_gc_thread_id]
      .lock();
  thread_metadata->local_ctx_clt->gc_processing_queue[target_gc_thread_id].push(
      request);
  thread_metadata->local_ctx_clt->gc_processing_queue_lock[target_gc_thread_id]
      .unlock();
  // MITSUME_PRINT("after lock\n");

  return MITSUME_SUCCESS;
}

/**
 * mitsume_tool_gc_shortcut_buffer: push the shortcut update request into
 * internal shortcut update linked-list
 * @lh: query-lh
 * @offset: query-offset
 * @shortcut_lh: update query info into shortcut-lh
 * @shortcut_offset: update query info into shortcut-offset
 * @replication_factor: number of replication
 * return: return a result after insert to list
 */
int mitsume_tool_gc_shortcut_buffer(
    struct mitsume_consumer_gc_metadata *gc_metadata, mitsume_key key,
    struct mitsume_ptr *ptr, struct mitsume_ptr *shortcut_ptr,
    uint32_t replication_factor, int target_controller_idx) {
  struct mitsume_consumer_gc_shortcut_update_element *target_element;
  target_element = (struct mitsume_consumer_gc_shortcut_update_element *)
      mitsume_tool_cache_alloc(MITSUME_ALLOCTYPE_GC_SHORTCUT_UPDATE_ELEMENT);
  mitsume_struct_copy_ptr_replication(
      target_element->writing_shortcut.shortcut_ptr, ptr, replication_factor);
  target_element->shortcut_ptr.pointer = shortcut_ptr->pointer;
  target_element->key = key;

  gc_metadata->internal_shortcut_buffer_list[target_controller_idx].push(
      target_element);
  return MITSUME_SUCCESS;
}

/**
 * mitsume_tool_gc_shortcut_send: take a entry which is just removed from the
 * list, issue send, and push to poll list
 * @target_element: entry from buffer list
 * return: return a result after send
 */
int mitsume_tool_gc_shortcut_send(
    struct mitsume_consumer_gc_metadata *gc_metadata,
    struct mitsume_consumer_gc_shortcut_update_element *target_element,
    int target_controller_idx) {
  int coro_id = MITSUME_CLT_TEMP_COROUTINE_ID;
  struct mitsume_ptr *shortcut_ptr = &target_element->shortcut_ptr;
  struct mitsume_shortcut *output_ptr =
      (struct mitsume_shortcut *)gc_metadata->local_inf->input_space[coro_id];
  ptr_attr *remote_mr =
      &gc_metadata->local_ctx_clt
           ->all_shortcut_attr[MITSUME_GET_PTR_LH(shortcut_ptr->pointer)];
  memcpy(output_ptr, target_element->writing_shortcut.shortcut_ptr,
         sizeof(struct mitsume_shortcut));

  target_element->wr_id =
      mitsume_local_thread_get_wr_id(gc_metadata->local_inf);
  // MITSUME_PRINT("send %llu\n", (unsigned long long
  // int)target_element->wr_id); MITSUME_("send %llu\n", (unsigned long long
  // int)target_element->wr_id); MITSUME_TOOL_PRINT_POINTER_KEY(target_element)

  userspace_one_write_inline(gc_metadata->local_ctx_clt->ib_ctx,
                             target_element->wr_id,
                             gc_metadata->local_inf->input_mr[coro_id],
                             sizeof(struct mitsume_shortcut), remote_mr, 0);

  gc_metadata->internal_shortcut_send_list[target_controller_idx].push(
      target_element);

  return MITSUME_SUCCESS;
}

/**
 * mitsume_tool_gc_shortcut_poll: take a entry which is just removed from send
 * list, then poll
 * @target_element: entry from send list
 * return: return a result after poll
 */
int mitsume_tool_gc_shortcut_poll(
    struct mitsume_consumer_gc_metadata *gc_metadata,
    struct mitsume_consumer_gc_shortcut_update_element *target_element) {
  struct mitsume_ptr *shortcut_ptr = &target_element->shortcut_ptr;
  ptr_attr *remote_mr =
      &gc_metadata->local_ctx_clt
           ->all_shortcut_attr[MITSUME_GET_PTR_LH(shortcut_ptr->pointer)];

  // MITSUME_PRINT("poll %llu\n", (unsigned long long
  // int)target_element->wr_id);
  userspace_one_poll(gc_metadata->local_ctx_clt->ib_ctx, target_element->wr_id,
                     remote_mr);
  mitsume_local_thread_put_wr_id(gc_metadata->local_inf, target_element->wr_id);

  return MITSUME_SUCCESS;
}

/**
 * mitsume_tool_tc_processing_reqiests: process gc request and send-reply to
 * controller
 * @thread_metadata: respected thread_metadata
 * @gc_entry: target gc entry
 * @gc_number: how many requests inside this entry
 * return: return success
 */
int mitsume_tool_gc_processing_requests(
    struct mitsume_consumer_gc_metadata *gc_thread_metadata,
    struct mitsume_gc_entry *gc_entry, int gc_number,
    int target_controller_id) {

  int coro_id = MITSUME_CLT_TEMP_COROUTINE_ID;
  struct mitsume_large_msg *send;
  struct mitsume_msg *reply;
  int i;

  struct thread_local_inf *local_inf = gc_thread_metadata->local_inf;

  send = (struct mitsume_large_msg *)local_inf->input_space[coro_id];
  reply = local_inf->output_space[coro_id];
  // reply = mitsume_tool_cache_alloc(MITSUME_ALLOCTYPE_LARGE_MSG);

  // MITSUME_PRINT("mitsume ask entries for queue %d by size %d\n", queue_id,
  // request_size);
  send->msg_header.type = MITSUME_GC_REQUEST;
  send->msg_header.src_id = gc_thread_metadata->local_ctx_clt->node_id;
  send->msg_header.des_id = target_controller_id;
  send->msg_header.thread_id = gc_thread_metadata->gc_thread_id;
  send->content.msg_gc_request.gc_number = gc_number;

  send->msg_header.reply_attr.addr =
      (uint64_t)local_inf->output_mr[coro_id]->addr;
  send->msg_header.reply_attr.rkey = local_inf->output_mr[coro_id]->rkey;
  send->msg_header.reply_attr.machine_id =
      gc_thread_metadata->local_ctx_clt->node_id;

  reply->end_crc = MITSUME_WAIT_CRC;

  for (i = 0; i < gc_number; i++) {
    mitsume_struct_copy_ptr_replication(
        send->content.msg_gc_request.gc_entry[i].old_ptr, gc_entry[i].old_ptr,
        gc_entry[i].replication_factor);
    mitsume_struct_copy_ptr_replication(
        send->content.msg_gc_request.gc_entry[i].new_ptr, gc_entry[i].new_ptr,
        gc_entry[i].replication_factor);
    send->content.msg_gc_request.gc_entry[i].key = gc_entry[i].key;
    send->content.msg_gc_request.gc_entry[i].replication_factor =
        gc_entry[i].replication_factor;
    // MITSUME_TOOL_PRINT_GC_POINTER_NULL(&gc_entry[i].old_ptr[0],
    // &gc_entry[i].new_ptr[0]);
  }

  // send the request out
  mitsume_send_full_message(gc_thread_metadata->local_ctx_clt->ib_ctx,
                            local_inf, local_inf->input_mr[coro_id],
                            local_inf->output_mr[coro_id], &send->msg_header,
                            gc_thread_metadata->local_ctx_clt->node_id,
                            target_controller_id, sizeof(mitsume_large_msg));

  if (reply->content.success_gc_number == 0) {
    MITSUME_PRINT_ERROR("check\n");
    for (i = 0; i < gc_number; i++) {
      MITSUME_TOOL_PRINT_POINTER_NULL(
          &send->content.msg_gc_request.gc_entry[i]
               .old_ptr[MITSUME_REPLICATION_PRIMARY]);
      MITSUME_TOOL_PRINT_POINTER_NULL(
          &send->content.msg_gc_request.gc_entry[i]
               .new_ptr[MITSUME_REPLICATION_PRIMARY]);
      /*if(send->content.msg_gc_request.gc_entry[i].old_ptr[MITSUME_REPLICATION_PRIMARY].pointer!=reply->content.msg_gc_request.gc_entry[i].old_ptr[MITSUME_REPLICATION_PRIMARY].pointer)
      {
          MITSUME_TOOL_PRINT_POINTER_NULL(&send->content.msg_gc_request.gc_entry[i].old_ptr[MITSUME_REPLICATION_PRIMARY],
      &reply->content.msg_gc_request.gc_entry[i].old_ptr[MITSUME_REPLICATION_PRIMARY]);
      }
      if(send->content.msg_gc_request.gc_entry[i].new_ptr[MITSUME_REPLICATION_PRIMARY].pointer!=reply->content.msg_gc_request.gc_entry[i].new_ptr[MITSUME_REPLICATION_PRIMARY].pointer)
      {
          MITSUME_TOOL_PRINT_POINTER_NULL(&send->content.msg_gc_request.gc_entry[i].new_ptr[MITSUME_REPLICATION_PRIMARY],
      &reply->content.msg_gc_request.gc_entry[i].new_ptr[MITSUME_REPLICATION_PRIMARY]);
      }
      if(send->content.msg_gc_request.gc_entry[i].key!=reply->content.msg_gc_request.gc_entry[i].key)
      {
          MITSUME_ERROR("check here %lld %lld",
      send->content.msg_gc_request.gc_entry[i].key,
      reply->content.msg_gc_request.gc_entry[i].key);
      }*/
    }
  }
  // MITSUME_PRINT("finish gc %d %d\n", gc_number,
  // reply->content.success_gc_number);
  return MITSUME_SUCCESS;
}

/**
 * mitsume_tool_gc_running_thread: process gc request from queued linked list
 * return: return success
 */
void *mitsume_tool_gc_running_thread(void *input_metadata) {
  struct mitsume_consumer_gc_metadata *gc_thread =
      (struct mitsume_consumer_gc_metadata *)input_metadata;
  int end_flag = 0;
  struct mitsume_gc_entry base_entry[MITSUME_CON_NUM]
                                    [MITSUME_CLT_CONSUMER_MAX_GC_NUMS];
  struct mitsume_gc_thread_request *new_request;
  int accumulate_gc_num[MITSUME_CON_NUM],
      accumulate_shortcut_num[MITSUME_CON_NUM], accumulate_shortcut_total_num;
  int check_flag = 0;
  struct mitsume_consumer_gc_shortcut_update_element *temp_shortcut_struct;
  mitsume_key
      update_shortcut_record[MITSUME_CLT_CONSUMER_MAX_SHORTCUT_UPDATE_NUMS];
  int per_shortcut;
  int target_controller_idx = -1;
  int per_controller_idx;
  int gc_thread_id = gc_thread->gc_thread_id;
  memset(update_shortcut_record, 0,
         sizeof(mitsume_key) * MITSUME_CLT_CONSUMER_MAX_SHORTCUT_UPDATE_NUMS);

  MITSUME_INFO("running gc thread %d\n", gc_thread->gc_thread_id);
  /*for(per_controller_idx=0;per_controller_idx<MITSUME_CON_NUM;per_controller_idx++)
  {
      base_entry[per_controller_idx] = kmalloc(sizeof(struct
  mitsume_gc_entry)*MITSUME_CLT_CONSUMER_MAX_GC_NUMS, GFP_KERNEL);
  }*/
  while (!end_flag) {
    check_flag =
        (gc_thread->local_ctx_clt->gc_processing_queue[gc_thread_id].empty());
    if (!check_flag) {
      gc_thread->local_ctx_clt
          ->gc_processing_queue_lock[gc_thread->gc_thread_id]
          .lock();
      memset(accumulate_gc_num, 0, sizeof(int) * MITSUME_CON_NUM);
      memset(accumulate_shortcut_num, 0, sizeof(int) * MITSUME_CON_NUM);
      accumulate_shortcut_total_num = 0;
      while (!(gc_thread->local_ctx_clt->gc_processing_queue[gc_thread_id]
                   .empty()) &&
             accumulate_shortcut_total_num <
                 MITSUME_CLT_CONSUMER_MAX_SHORTCUT_UPDATE_NUMS) {
        // get one entry from list and makesure the total available shortcut
        // number is still below limitation
        new_request =
            gc_thread->local_ctx_clt->gc_processing_queue[gc_thread_id].front();
        target_controller_idx =
            new_request->target_controller - MITSUME_FIRST_ID;
        // if the next gc requests is already above limitation, abort current
        // entry, move to submit target_controller_idx is used here because there
        // is a mismatch between idx and controller id (id is + MITSUME_FIRST_ID
        // of idx)
        if (accumulate_gc_num[target_controller_idx] ==
                MITSUME_CLT_CONSUMER_MAX_GC_NUMS ||
            accumulate_shortcut_num[target_controller_idx] ==
                MITSUME_CLT_CONSUMER_MAX_SHORTCUT_UPDATE_NUMS)
          break;

        gc_thread->local_ctx_clt->gc_processing_queue[gc_thread_id].pop();
        gc_thread->local_ctx_clt
            ->gc_processing_queue_lock[gc_thread->gc_thread_id]
            .unlock();

        accumulate_shortcut_total_num++;
        accumulate_shortcut_num[target_controller_idx]++;
        if (new_request->gc_mode == MITSUME_TOOL_GC_REGULAR_PROCESSING) {
          // release lock, the speed of this gc thread is not critical.
          mitsume_struct_copy_ptr_replication(
              base_entry[target_controller_idx]
                        [accumulate_gc_num[target_controller_idx]]
                            .old_ptr,
              new_request->gc_entry.old_ptr,
              new_request->gc_entry.replication_factor);
          mitsume_struct_copy_ptr_replication(
              base_entry[target_controller_idx]
                        [accumulate_gc_num[target_controller_idx]]
                            .new_ptr,
              new_request->gc_entry.new_ptr,
              new_request->gc_entry.replication_factor);
          base_entry[target_controller_idx]
                    [accumulate_gc_num[target_controller_idx]]
                        .key = new_request->gc_entry.key;
          base_entry[target_controller_idx]
                    [accumulate_gc_num[target_controller_idx]]
                        .replication_factor =
              new_request->gc_entry.replication_factor;

          if (!new_request->gc_entry.new_ptr[MITSUME_REPLICATION_PRIMARY]
                   .pointer ||
              !new_request->gc_entry.key ||
              !new_request->shortcut_ptr.pointer) {
            MITSUME_PRINT_ERROR("NULL setup for GC request %d ptr %#lx key %ld",
                                accumulate_gc_num[target_controller_idx],
                                (long unsigned int)new_request->gc_entry
                                    .new_ptr[MITSUME_REPLICATION_PRIMARY]
                                    .pointer,
                                (long unsigned int)new_request->gc_entry.key);
            MITSUME_IDLE_HERE;
          } else {
            mitsume_tool_gc_shortcut_buffer(
                gc_thread, new_request->gc_entry.key,
                new_request->gc_entry.new_ptr, &new_request->shortcut_ptr,
                new_request->gc_entry.replication_factor,
                target_controller_idx);
          }
          accumulate_gc_num[target_controller_idx]++;
        } else if (new_request->gc_mode ==
                   MITSUME_TOOL_GC_UPDATE_SHORTCUT_ONLY) {
          MITSUME_PRINT_ERROR("This mode is not supported currently\n");
          MITSUME_IDLE_HERE;
          // mitsume_tool_gc_shortcut_buffer(gc_thread,
          // new_request->gc_entry.key, new_request->gc_entry.new_ptr,
          // &new_request->shortcut_ptr,
          // new_request->gc_entry.replication_factor, target_controller_idx);
        } else {
          MITSUME_PRINT_ERROR("wrong gc mode %d\n", new_request->gc_mode);
        }

        mitsume_tool_cache_free(new_request,
                                MITSUME_ALLOCTYPE_GC_THREAD_REQUEST);
        gc_thread->local_ctx_clt
            ->gc_processing_queue_lock[gc_thread->gc_thread_id]
            .lock();
      }
      gc_thread->local_ctx_clt
          ->gc_processing_queue_lock[gc_thread->gc_thread_id]
          .unlock();
      for (per_controller_idx = 0; per_controller_idx < MITSUME_CON_NUM;
           per_controller_idx++) {
        if (!accumulate_shortcut_num[per_controller_idx])
          continue;
        while (!gc_thread->internal_shortcut_buffer_list[per_controller_idx]
                    .empty()) {
          temp_shortcut_struct =
              gc_thread->internal_shortcut_buffer_list[per_controller_idx]
                  .front();
          gc_thread->internal_shortcut_buffer_list[per_controller_idx].pop();
          for (per_shortcut = 0;
               per_shortcut < MITSUME_CLT_CONSUMER_MAX_SHORTCUT_UPDATE_NUMS;
               per_shortcut++) {
            if (update_shortcut_record[per_shortcut] == 0) {
              update_shortcut_record[per_shortcut] = temp_shortcut_struct->key;
              mitsume_tool_gc_shortcut_send(gc_thread, temp_shortcut_struct,
                                            per_controller_idx);
              break;
            } else if (update_shortcut_record[per_shortcut] ==
                       temp_shortcut_struct->key) {
              // MITSUME_STAT_ADD(MITSUME_STAT_CLT_REMOVE_SHORTCUT_UPDATE, 1);
              mitsume_tool_cache_free(
                  temp_shortcut_struct,
                  MITSUME_ALLOCTYPE_GC_SHORTCUT_UPDATE_ELEMENT);
              break;
            }
          }
        }
        while (!gc_thread->internal_shortcut_send_list[per_controller_idx]
                    .empty()) {
          temp_shortcut_struct =
              gc_thread->internal_shortcut_send_list[per_controller_idx]
                  .front();
          gc_thread->internal_shortcut_send_list[per_controller_idx].pop();
          mitsume_tool_gc_shortcut_poll(gc_thread, temp_shortcut_struct);
          mitsume_tool_cache_free(temp_shortcut_struct,
                                  MITSUME_ALLOCTYPE_GC_SHORTCUT_UPDATE_ELEMENT);
        }
        memset(update_shortcut_record, 0,
               sizeof(mitsume_key) *
                   MITSUME_CLT_CONSUMER_MAX_SHORTCUT_UPDATE_NUMS);
        if (accumulate_gc_num[per_controller_idx])
          mitsume_tool_gc_processing_requests(
              gc_thread, base_entry[per_controller_idx],
              accumulate_gc_num[per_controller_idx],
              per_controller_idx + MITSUME_FIRST_ID);
        // MITSUME_STAT_ARRAY_ADD(gc_thread->gc_thread_id, 1);
      }
    } else {
      schedule();
    }
  }
  MITSUME_INFO("exit gc thread %d\n", gc_thread->gc_thread_id);
  return NULL;
}

/**
 * mitsume_tool_gc_epoch_forwarding: check to see are there any epoch forwarding
 * request
 * @epoch_thread: epoch thread metadata
 * return: return success/fail
 */
void *mitsume_tool_gc_epoch_forwarding(void *input_epoch_thread) {
  int coro_id = MITSUME_CLT_TEMP_COROUTINE_ID;
  struct mitsume_consumer_epoch_metadata *epoch_thread =
      (struct mitsume_consumer_epoch_metadata *)input_epoch_thread;
  struct mitsume_msg *received_message;
  struct mitsume_msg *replied_message;
  uint32_t received_length;
  uint64_t received_id;
  struct mitsume_ctx_clt *local_ctx_clt = epoch_thread->local_ctx_clt;
  struct thread_local_inf *local_inf = epoch_thread->local_inf;
  int end_flag = 0;
  long long int target_mr;
  long long int target_qp;
  int num_of_poll;
  uint64_t wr_id;
  ptr_attr *target_qp_mr_attr;
  int per_thread;
  int per_gc_thread;
  int try_lock;
  replied_message = local_inf->input_space[coro_id];
  struct ibv_wc input_wc[RSEC_CQ_DEPTH];
  while (!end_flag) {
    num_of_poll = userspace_one_poll_wr(local_ctx_clt->ib_ctx->server_recv_cq,
                                        1, input_wc, 1);
    // MITSUME_INFO("get message\n");
    // usleep(MITSUME_GC_CLT_EPOCH_DELAY*1000);
    assert(num_of_poll == 1);
    received_length = input_wc[0].byte_len;
    received_id = input_wc[0].wr_id;

    target_qp = RSEC_ID_TO_QP(received_id);
    target_mr = RSEC_ID_TO_RECV_MR(received_id);
    target_qp_mr_attr = local_ctx_clt->per_qp_mr_attr_list[target_qp];

    received_message = (struct mitsume_msg *)target_qp_mr_attr[target_mr].addr;
    if (received_length == sizeof(struct mitsume_msg)) {
      switch (received_message->msg_header.type) {
      case MITSUME_GC_EPOCH_FORWARD: {
        if (received_message->content.msg_gc_epoch_forward
                    .request_epoch_number <= local_ctx_clt->gc_current_epoch ||
            received_message->content.msg_gc_epoch_forward
                    .request_epoch_number !=
                local_ctx_clt->gc_current_epoch + MITSUME_GC_EPOCH_STEPSIZE) {
          MITSUME_PRINT_ERROR("wrong epoch:%d (current:%d)\n",
                              received_message->content.msg_gc_epoch_forward
                                  .request_epoch_number,
                              local_ctx_clt->gc_current_epoch);
          replied_message->content.msg_gc_epoch_forward.request_epoch_number =
              0;
          replied_message->msg_header.type = MITSUME_GC_EPOCH_FORWARD_ACK;

          local_ctx_clt->gc_current_epoch =
              received_message->content.msg_gc_epoch_forward
                  .request_epoch_number;
          // liteapi_reply_message(replied_message, sizeof(struct mitsume_msg),
          // received_descriptor);

          replied_message->end_crc = MITSUME_REPLY_CRC;
          wr_id = mitsume_local_thread_get_wr_id(local_inf);
          mitsume_reply_full_message(
              local_ctx_clt->ib_ctx, wr_id, &received_message->msg_header,
              local_inf->input_mr[coro_id], sizeof(mitsume_msg));
          userspace_one_poll(local_ctx_clt->ib_ctx, wr_id,
                             &received_message->msg_header.reply_attr);
          mitsume_local_thread_put_wr_id(local_inf, wr_id);
          continue;
        }
        MITSUME_INFO("epoch %d end\n", local_ctx_clt->gc_current_epoch);
        local_ctx_clt->gc_epoch_block_lock.lock();
        // lock the mitsume tool to avoid all the future requests
        // drain all requests

        for (per_thread = 0; per_thread < MITSUME_CLT_CONSUMER_NUMBER;
             per_thread++) {
          // try_spin_lock(&local_ctx_clt->thread_metadata[per_thread].current_running_lock);
          for (int per_coro = 0; per_coro < MITSUME_CLT_COROUTINE_NUMBER;
               per_coro++) {
            do {
              try_lock = local_ctx_clt->thread_metadata[per_thread]
                             .current_running_lock[per_coro]
                             .try_lock();
              schedule();
            } while (!try_lock);
          }
        }

        // drain gc buffer

        for (per_gc_thread = 0;
             per_thread < MITSUME_CLT_CONSUMER_GC_THREAD_NUMS;
             per_gc_thread++) {
          local_ctx_clt->gc_processing_queue_lock[per_gc_thread].lock();
          /*while(!list_empty(&(local_ctx_clt->gc_processing_queue[per_gc_thread].list)))
          {
              schedule();
          }*/
        }

        replied_message->content.msg_gc_epoch_forward.request_epoch_number =
            local_ctx_clt->gc_current_epoch;
        replied_message->msg_header.type = MITSUME_GC_EPOCH_FORWARD_ACK;

        local_ctx_clt->gc_current_epoch =
            received_message->content.msg_gc_epoch_forward.request_epoch_number;
        // MITSUME_STAT_SET(MITSUME_CURRENT_EPOCH,
        // local_ctx_clt->gc_current_epoch);

        replied_message->end_crc = MITSUME_REPLY_CRC;
        wr_id = mitsume_local_thread_get_wr_id(local_inf);
        mitsume_reply_full_message(
            local_ctx_clt->ib_ctx, wr_id, &received_message->msg_header,
            local_inf->input_mr[coro_id], sizeof(mitsume_msg));
        userspace_one_poll(local_ctx_clt->ib_ctx, wr_id,
                           &received_message->msg_header.reply_attr);
        mitsume_local_thread_put_wr_id(local_inf, wr_id);

        for (per_thread = 0; per_thread < MITSUME_CLT_CONSUMER_NUMBER;
             per_thread++) {
          for (int per_coro = 0; per_coro < MITSUME_CLT_COROUTINE_NUMBER;
               per_coro++) {
            local_ctx_clt->thread_metadata[per_thread]
                .current_running_lock[MITSUME_CLT_COROUTINE_NUMBER]
                .unlock();
          }
        }
        for (per_gc_thread = 0;
             per_thread < MITSUME_CLT_CONSUMER_GC_THREAD_NUMS;
             per_gc_thread++) {
          local_ctx_clt->gc_processing_queue_lock[per_gc_thread].unlock();
          /*while(!list_empty(&(local_ctx_clt->gc_processing_queue[per_gc_thread].list)))
          {
              schedule();
          }*/
        }

        local_ctx_clt->gc_epoch_block_lock.unlock();
      } break;
      default:
        MITSUME_PRINT_ERROR("wrong type %d\n",
                            received_message->msg_header.type);
        replied_message->content.msg_gc_epoch_forward.request_epoch_number = 0;
        replied_message->msg_header.type = MITSUME_GC_GENERAL_FAULT;

        local_ctx_clt->gc_current_epoch =
            received_message->content.msg_gc_epoch_forward.request_epoch_number;

        replied_message->end_crc = MITSUME_REPLY_CRC;
        wr_id = mitsume_local_thread_get_wr_id(local_inf);
        mitsume_reply_full_message(
            local_ctx_clt->ib_ctx, wr_id, &received_message->msg_header,
            local_inf->input_mr[coro_id], sizeof(mitsume_msg));
        userspace_one_poll(local_ctx_clt->ib_ctx, wr_id,
                           &received_message->msg_header.reply_attr);
        mitsume_local_thread_put_wr_id(local_inf, wr_id);
        // liteapi_reply_message(replied_message, sizeof(struct mitsume_msg),
        // received_descriptor);
      }
    } else {
      if (received_length) {
        if (received_length != sizeof(struct mitsume_msg)) {
          MITSUME_PRINT_ERROR("wrong current_running value %d\n",
                              received_length);
        }
        replied_message->content.msg_gc_epoch_forward.request_epoch_number = 0;
        replied_message->msg_header.type = MITSUME_GC_GENERAL_FAULT;

        local_ctx_clt->gc_current_epoch =
            received_message->content.msg_gc_epoch_forward.request_epoch_number;

        replied_message->end_crc = MITSUME_REPLY_CRC;
        wr_id = mitsume_local_thread_get_wr_id(local_inf);
        mitsume_reply_full_message(
            local_ctx_clt->ib_ctx, wr_id, &received_message->msg_header,
            local_inf->input_mr[coro_id], sizeof(mitsume_msg));
        userspace_one_poll(local_ctx_clt->ib_ctx, wr_id,
                           &received_message->msg_header.reply_attr);
        mitsume_local_thread_put_wr_id(local_inf, wr_id);
        // liteapi_reply_message(replied_message, sizeof(struct mitsume_msg),
        // received_descriptor);
      }
    }
    userspace_refill_used_postrecv(local_ctx_clt->ib_ctx,
                                   local_ctx_clt->per_qp_mr_attr_list, input_wc,
                                   1, MITSUME_MAX_MESSAGE_SIZE);
  }
  // mitsume_tool_cache_free(received_message, MITSUME_ALLOCTYPE_MSG);
  // mitsume_tool_cache_free(replied_message, MITSUME_ALLOCTYPE_MSG);
  MITSUME_INFO("exit epoch thread\n");

  return NULL;
}

void *mitsume_tool_gc_stat_thread(void *input_metadata) {
  int coro_id = MITSUME_CLT_TEMP_COROUTINE_ID;
  struct mitsume_consumer_stat_metadata *stat_metadata =
      (struct mitsume_consumer_stat_metadata *)input_metadata;
  struct mitsume_msg *send;
  struct mitsume_msg *recv;
  struct mitsume_ctx_clt *local_ctx_clt = stat_metadata->local_ctx_clt;
  struct thread_local_inf *local_inf = stat_metadata->local_inf;
  int end_flag = 0;
  int each_controller;
  int per_bucket;
  long int tmp_read;
  long int tmp_write;
  int record_flag;
  send = local_inf->input_space[coro_id];
  recv = local_inf->output_space[coro_id];
  send->msg_header.src_id = local_ctx_clt->node_id;
  send->msg_header.type = MITSUME_STAT_UPDATE;
  while (!end_flag) {
    usleep(MITSUME_GC_CLT_LOAD_BALANBING_DELAY_MS * 1000);
    record_flag = 0;
    for (per_bucket = 0; per_bucket < MITSUME_NUM_REPLICATION_BUCKET;
         per_bucket++) {
      tmp_read = local_ctx_clt->read_bucket_counter[per_bucket].load();
      tmp_write = local_ctx_clt->write_bucket_counter[per_bucket].load();
      if (tmp_read || tmp_write)
        record_flag = 1;
      local_ctx_clt->read_bucket_counter[per_bucket] -= tmp_read;
      local_ctx_clt->write_bucket_counter[per_bucket] -= tmp_write;
      send->content.msg_stat_message.read_bucket_counter[per_bucket] = tmp_read;
      send->content.msg_stat_message.write_bucket_counter[per_bucket] =
          tmp_write;
    }
    if (record_flag) {
      for (each_controller = MITSUME_FIRST_ID;
           each_controller < MITSUME_FIRST_ID + MITSUME_CON_NUM;
           each_controller++) {
        send->msg_header.des_id = each_controller;

        send->msg_header.reply_attr.addr =
            (uint64_t)local_inf->output_mr[coro_id]->addr;
        send->msg_header.reply_attr.rkey = local_inf->output_mr[coro_id]->rkey;
        send->msg_header.reply_attr.machine_id =
            stat_metadata->local_ctx_clt->node_id;

        recv->end_crc = MITSUME_WAIT_CRC;

        // send the request out
        mitsume_send_full_message(
            stat_metadata->local_ctx_clt->ib_ctx, local_inf,
            local_inf->input_mr[coro_id], local_inf->output_mr[coro_id],
            &send->msg_header, stat_metadata->local_ctx_clt->node_id,
            each_controller, sizeof(mitsume_msg));
        if (recv->msg_header.type != MITSUME_STAT_UPDATE_ACK)
          MITSUME_PRINT_ERROR("reply type doesn't match %d\n",
                              recv->msg_header.type);

        // returned_length = liteapi_send_reply_imm(each_controller, port,
        // &send, sizeof(struct mitsume_msg), &recv, sizeof(struct mitsume_msg));
        // if(recv.msg_header.type != MITSUME_STAT_UPDATE_ACK)
        //    MITSUME_ERROR("reply type doesn't match %d\n",
        //    recv.msg_header.type);
      }
    }
  }
  MITSUME_INFO("exit stat thread\n");
  return NULL;
}

int mitsume_tool_gc_init(struct mitsume_ctx_clt *ctx_clt) {
  int i;
  int running_gc_thread_nums = MITSUME_CLT_CONSUMER_GC_THREAD_NUMS;

  // MITSUME_STAT_SET(MITSUME_CURRENT_EPOCH, ctx_clt->gc_current_epoch);
  // init task_struct, queue, lock
  for (i = 0; i < running_gc_thread_nums; i++) {
    ctx_clt->gc_thread_metadata[i].gc_thread_id = i;
    ctx_clt->gc_thread_metadata[i].local_ctx_clt = ctx_clt;
    ctx_clt->gc_thread_metadata[i].gc_allocator_counter = 0;
    ctx_clt->gc_thread_metadata[i].local_inf =
        mitsume_local_thread_setup(ctx_clt->ib_ctx, i);
  }
  // run thread
  for (i = 0; i < running_gc_thread_nums; i++) {
    pthread_create(&ctx_clt->thread_gc_thread[i], NULL,
                   mitsume_tool_gc_running_thread,
                   &ctx_clt->gc_thread_metadata[i]);
  }

  // run epoch thread
  ctx_clt->epoch_thread_metadata.local_ctx_clt = ctx_clt;
  ctx_clt->epoch_thread_metadata.local_inf =
      mitsume_local_thread_setup(ctx_clt->ib_ctx, 0);
  pthread_create(&ctx_clt->thread_epoch, NULL, mitsume_tool_gc_epoch_forwarding,
                 &ctx_clt->epoch_thread_metadata);
  // ctx_clt->thread_epoch = kthread_create((void
  // *)mitsume_tool_gc_epoch_forwarding, &ctx_clt->epoch_thread_metadata,
  // epoch_thread_name);

  for (i = 0; i < MITSUME_NUM_REPLICATION_BUCKET; i++) {
    ctx_clt->read_bucket_counter[i] = 0;
    ctx_clt->write_bucket_counter[i] = 0;
  }

  // run stat thread
  if (MITSUME_TOOL_TRAFFIC_STAT) {
    ctx_clt->stat_thread_metadata.local_ctx_clt = ctx_clt;
    ctx_clt->stat_thread_metadata.local_inf =
        mitsume_local_thread_setup(ctx_clt->ib_ctx, 0);
    pthread_create(&ctx_clt->thread_stat, NULL, mitsume_tool_gc_stat_thread,
                   &ctx_clt->stat_thread_metadata);
  }

  return MITSUME_SUCCESS;
}
