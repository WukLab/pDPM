#include "mitsume_clt_tool.h"

/**
 * mitsume_clt_consumer_push_entry_into_list - put an available entry into list
 * @thread_metadata: consumer thread metadata
 * @input_entry: target entry (available entry)
 * @target_replication_target: it would push the entry into the respected bucket
 * based on the input return: return 0 if success. return -1 if error happens
 */

int mitsume_clt_consumer_push_entry_into_list(
    struct mitsume_consumer_metadata *thread_metadata,
    struct mitsume_consumer_entry_internal *input_entry,
    int target_replication_bucket) {
  int target_list;

  assert(input_entry->ptr.pointer);
  target_list = mitsume_con_alloc_lh_to_list_num(MITSUME_GET_PTR_LH(
      input_entry->ptr.pointer)); //[TODO setup ask functions at allocator side
                                  //and put the reply into correspondent queue]
  assert(target_list >= 0);
  // MITSUME_PRINT("%d : %d\n", target_replication_bucket, target_list);

  thread_metadata->consumer_node_branch[target_replication_bucket][target_list]
      .push(input_entry->ptr.pointer);
  return MITSUME_SUCCESS;
}

int mitsume_clt_consumer_check_entry_from_list(
    struct mitsume_consumer_metadata *thread_metadata, uint32_t size,
    int target_replication_bucket) {
  int target_list = mitsume_con_alloc_size_to_list_num(size);
  if (thread_metadata
          ->consumer_node_branch[target_replication_bucket][target_list]
          .empty())
    return 0;
  return 1;
}

/**
 * mitsume_clt_consumer_get_entry_from_list - get an available entry based on
 * size and replication bucket
 * @thread_metadata: target thread_metadata
 * @size: request size
 * @output: available entry
 * @target_replication_bucket: destinated bucket (this number is generated from
 * get_new_space function)
 * @option_flag: which is used during clean-up. It would empty the whole list
 * without asking report return: return available entry
 */
int mitsume_clt_consumer_get_entry_from_list(
    struct mitsume_consumer_metadata *thread_metadata, int size,
    struct mitsume_consumer_entry *output, int target_replication_bucket,
    int option_flag, int coro_id, coro_yield_t &yield) {
  int target_list = mitsume_con_alloc_size_to_list_num(size);
  uint32_t round_size = mitsume_con_alloc_list_num_to_size(target_list);
  struct mitsume_consumer_entry_internal new_entry_internal;
  int ret_bucket;
  int count = 0;
  if (target_replication_bucket >= 0) // client decides directly
  {

    while (thread_metadata
               ->consumer_node_branch[target_replication_bucket][target_list]
               .empty()) // if the queue is empty, ask controller again
    // while(list_empty(&(consumer_list[target_list].list)))//if the queue is
    // empty, ask controller again
    {
      int ret_number;
      int request_number = MITSUME_CLT_CONSUMER_PER_ASK_NUMS;
      if (option_flag &
          MITSUME_CLT_CONSUMER_GET_ENTRY_DONT_ASK_CONTROLLER) // if the queue is
                                                              // empty, and flag
                                                              // sets with
                                                              // no-ask-controller
      {
        return MITSUME_ERROR;
      }
      ret_number = mitsume_clt_consumer_ask_entries_from_controller(
          thread_metadata, target_list, request_number,
          target_replication_bucket, &ret_bucket, coro_id, yield);
      if (!ret_number) // if get zero
      {
        // MITSUME_ERROR("failed to get entries\n");
        // schedule();

        // msleep(MITSUME_CLT_GET_ENTRY_RETRY_TIMEOUT);
        usleep(MITSUME_CLT_GET_ENTRY_RETRY_TIMEOUT);
        count++;

        // mitsume_stat_add(MITSUME_STAT_CLT_RETRY_GET_ENTRY, 1);
        if (count == 10000) {
          MITSUME_INFO("ask for size:%u rep:%d too many times\n",
                       (unsigned int)round_size, target_replication_bucket);
        }
        /*if(count > 10 && count %10==0)
        {
                MITSUME_INFO("ask for %d\n", target_replication_bucket);
        }*/
      } else {
        if (ret_bucket != target_replication_bucket)
          MITSUME_PRINT_ERROR("bucket doesn't match %d %d\n", ret_bucket,
                              target_replication_bucket);
        break;
      }
    }
  } else {
    int ret_number;
    int request_number = MITSUME_CLT_CONSUMER_PER_ASK_NUMS;
    while (true) {
      // MITSUME_INFO("%d\n", target_replication_bucket);
      ret_number = mitsume_clt_consumer_ask_entries_from_controller(
          thread_metadata, target_list, request_number,
          target_replication_bucket, &ret_bucket, coro_id, yield);
      if (!ret_number) // if get zero
      {
        schedule();
        usleep(MITSUME_CLT_GET_ENTRY_RETRY_TIMEOUT);
        count++;
        mitsume_stat_add(MITSUME_STAT_CLT_RETRY_GET_ENTRY, 1);
        if (count == 100000) {
          MITSUME_INFO("ask for %u %d\n", round_size,
                       target_replication_bucket);
        }
      } else {
        break;
      }
    }
    // consumer_list = thread_metadata->consumer_node_branch[ret_bucket];
    // consumer_counter =
    // thread_metadata->consumer_node_branch_count[ret_bucket]; MITSUME_INFO("get
    // from %d\n", ret_bucket);
    target_replication_bucket = ret_bucket;
  }

  // spin_lock(&(thread_metadata->allocator_lock[target_list]));

  // new_entry_internal = list_entry(consumer_list[target_list].list.next,
  // struct mitsume_consumer_entry_internal, list);
  new_entry_internal.ptr.pointer =
      thread_metadata
          ->consumer_node_branch[target_replication_bucket][target_list]
          .front();
  thread_metadata->consumer_node_branch[target_replication_bucket][target_list]
      .pop();

  assert(new_entry_internal.ptr.pointer);

  output->ptr.pointer = new_entry_internal.ptr.pointer;
  output->size = round_size;

  // spin_unlock(&(thread_metadata->allocator_lock[target_list]));
  return MITSUME_SUCCESS;
}

/**
 * mitsume_clt_consumer_ask_entries_from_controller - if a queue is below some
 * threshold, issueing this function to get new entries from controller
 * @queue_id: exhausted queue(list) id
 * @numbers: asked numbers of entries
 * @replication_bucket: target replication bucket
 * return: return N as available entries. -1 if connection fail
 */
int mitsume_clt_consumer_ask_entries_from_controller(
    struct mitsume_consumer_metadata *thread_metadata, int queue_id,
    int numbers, int replication_bucket, int *ret_bucket, int coro_id,
    coro_yield_t &yield) {
  uint32_t request_size;
  int target_controller_id;
  struct mitsume_msg *send;
  struct mitsume_msg *reply;
  unsigned int i;
  int ret;
  struct mitsume_consumer_entry_internal input_entry;

  request_size = mitsume_con_alloc_list_num_to_size(queue_id);
  target_controller_id =
      mitsume_con_alloc_rr_get_controller_id(thread_metadata);
  send = thread_metadata->local_inf->input_space[coro_id];
  reply = thread_metadata->local_inf->output_space[coro_id];

  // MITSUME_PRINT("mitsume ask entries for queue %d by size %d\n", queue_id,
  // request_size);
  send->msg_header.type = MITSUME_CONTROLLER_ASK_ENTRY;
  send->msg_header.des_id = target_controller_id;
  send->msg_header.src_id = thread_metadata->local_ctx_clt->node_id;
  send->msg_header.thread_id = thread_metadata->thread_id;
  send->content.msg_entry.entry_size = request_size;
  send->content.msg_entry.entry_number = numbers;
  send->content.msg_entry.entry_replication_bucket = replication_bucket;

  send->msg_header.reply_attr.addr =
      (uint64_t)thread_metadata->local_inf->output_mr[coro_id]->addr;
  send->msg_header.reply_attr.rkey =
      thread_metadata->local_inf->output_mr[coro_id]->rkey;
  send->msg_header.reply_attr.machine_id =
      thread_metadata->local_ctx_clt->node_id;

  // MITSUME_PRINT("%d:%d\n", send->msg_header.src_id,
  // send->msg_header.reply_attr.machine_id);
  reply->end_crc = MITSUME_WAIT_CRC;

  // liteapi_send_reply_imm(target_controller_id, port + target_allocator_id,
  // &send, sizeof(struct mitsume_msg), &reply, sizeof(struct mitsume_msg));

  if (replication_bucket == MITSUME_TOOL_LOAD_BALANCING_NOENTRY) {
    int per_bucket;
    for (per_bucket = 0; per_bucket < MITSUME_NUM_REPLICATION_BUCKET;
         per_bucket++) {
      if (mitsume_clt_consumer_check_entry_from_list(
              thread_metadata, request_size,
              per_bucket)) // give available list the highest priority
        send->content.msg_entry.already_available_buckets[per_bucket] =
            MITSUME_TOOL_LOAD_BALANCING_BUCKET_ALREADY_HAVE;
      else
        send->content.msg_entry.already_available_buckets[per_bucket] =
            MITSUME_TOOL_LOAD_BALANCING_BUCKET_CAN_GET;
    }
    // MITSUME_INFO("bucket %d size %d\n",
    // send.content.msg_entry.entry_replication_bucket,
    // send.content.msg_entry.entry_size);
    // for(per_bucket=0;per_bucket<MITSUME_NUM_REPLICATION_BUCKET;per_bucket++)
    //{
    //        MITSUME_INFO("bucket-%d: %d\n", per_bucket,
    //        send.content.msg_entry.already_available_buckets[per_bucket]);
    //}
  }

  if (MITSUME_CLT_SEND_COROUTINE) {
    mitsume_send_full_message_async(
        thread_metadata->local_ctx_clt->ib_ctx, thread_metadata->local_inf,
        thread_metadata->local_inf->input_mr[coro_id],
        thread_metadata->local_inf->output_mr[coro_id], &send->msg_header,
        thread_metadata->local_ctx_clt->node_id, target_controller_id,
        sizeof(mitsume_msg), coro_id, yield);
  } else {
    mitsume_send_full_message(
        thread_metadata->local_ctx_clt->ib_ctx, thread_metadata->local_inf,
        thread_metadata->local_inf->input_mr[coro_id],
        thread_metadata->local_inf->output_mr[coro_id], &send->msg_header,
        thread_metadata->local_ctx_clt->node_id, target_controller_id,
        sizeof(mitsume_msg));
  }

  // send the request out
  numbers = reply->content.msg_entry.entry_number;
  // numbers = reply.content.msg_entry.entry_number;

  // MITSUME_STAT_ADD(MITSUME_STAT_CLT_ASK_ENTRY, numbers);

  for (i = 0; i < reply->content.msg_entry.entry_number; i++) {
    input_entry.ptr.pointer = reply->content.msg_entry.ptr[i].pointer;
    ret = mitsume_clt_consumer_push_entry_into_list(
        thread_metadata, &input_entry,
        reply->content.msg_entry.entry_replication_bucket);
    if (ret)
      MITSUME_PRINT_ERROR("push entry into list fail %llx\n",
                          (unsigned long long int)input_entry.ptr.pointer);
  }

  *ret_bucket = reply->content.msg_entry.entry_replication_bucket;

  // MITSUME_STAT_ADD(MITSUME_STAT_CLT_GET_ENTRY,
  // reply.content.msg_entry.entry_number);

  // mitsume_tool_cache_free(send, MITSUME_ALLOCTYPE_MSG);
  // mitsume_tool_cache_free(reply, MITSUME_ALLOCTYPE_MSG);

  return numbers;
}
