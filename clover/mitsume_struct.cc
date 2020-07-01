#include "mitsume_struct.h"

/**
 * mitsume_struct_setup_entry_request: setup entry request format
 * @input: input message pointer
 * @main_type: type of main message
 * @src_id: sender id
 * @des_id: destinaion id
 * @thread_id: thread_id
 * @type: type of entry request
 * @key: respected key
 * @entry_ptr: pointer of this key
 * @shortcut_ptr: shortcut pointer of this key
 * @asked_entry_size: size of requested entry
 * @asked_entry_size: number of requested entry (different with size)
 * return: success if format is correct
 */
int mitsume_struct_setup_entry_request(struct mitsume_msg *input, int main_type,
                                       int src_id, int des_id, int thread_id,
                                       uint32_t type, mitsume_key key,
                                       struct mitsume_ptr *entry_ptr,
                                       struct mitsume_ptr *shortcut_ptr,
                                       int replication_factor) {
  if (main_type != MITSUME_ENTRY_REQUEST &&
      main_type != MITSUME_ENTRY_REQUEST_ACK) {
    return MITSUME_ERROR;
  }
  input->msg_header.type = main_type;
  input->msg_header.src_id = src_id;
  input->msg_header.des_id = des_id;
  input->msg_header.thread_id = thread_id;
  input->content.msg_entry_request.type = type;
  input->content.msg_entry_request.key = key;
  input->content.msg_entry_request.replication_factor = replication_factor;
  if (entry_ptr) {
    // input->content.msg_entry_request.ptr.pointer = entry_ptr->pointer;
    mitsume_struct_copy_ptr_replication(input->content.msg_entry_request.ptr,
                                        entry_ptr, replication_factor);
  } else {
    input->content.msg_entry_request.ptr[MITSUME_REPLICATION_PRIMARY].pointer =
        0;
  }
  if (shortcut_ptr)
    input->content.msg_entry_request.shortcut_ptr.pointer =
        shortcut_ptr->pointer;
  else
    input->content.msg_entry_request.shortcut_ptr.pointer = 0;
  // input->content.msg_entry_request.lh = lh;
  // input->content.msg_entry_request.offset = offset;
  // input->content.msg_entry_request.shortcut_lh = shortcut_lh;
  // input->content.msg_entry_request.shortcut_offset = shortcut_offset;
  // input->content.msg_entry_request.asked_entry_size = asked_entry_size;
  // input->content.msg_entry_request.asked_entry_number = asked_entry_number;
  return MITSUME_SUCCESS;
}

/**
 * mitsume_struct_set_pointer: setup pointer based on input
 * @lh: target next-lh
 * @offset: target next-offset
 * @next_version: target next-version
 * @entry_version: target entry version (current version)
 * @area: target xact area num
 * @option: optional column number
 * return: configured pointer
 */
uint64_t mitsume_struct_set_pointer(uint64_t lh, uint32_t next_version,
                                    uint32_t entry_version, uint32_t area,
                                    uint32_t option) {
  return MITSUME_SET_PTR_LH(lh) | MITSUME_SET_PTR_NEXT_VERSION(next_version) |
         MITSUME_SET_PTR_ENTRY_VERSION(entry_version) |
         MITSUME_SET_PTR_XACT_AREA(area) | MITSUME_SET_PTR_OPTION(option);
}

/**
 * mitsume_tool_print_pointer: print mitsume_pointer
 * @input: input pointer
 * @function_name: caller
 * @function_line: caller Line's number
 * @thread_id: output thread's id(embedded inside metadata) to facilitate
 * debugging
 */
void mitsume_tool_print_pointer(struct mitsume_ptr *entry,
                                const char *function_name, int function_line,
                                uint64_t key) {
  uint64_t target;
  if (entry)
    target = entry->pointer;
  else
    target = 0;
  MITSUME_PRINT_BRIEF("(%s:%d)%llu-%#llx lh:%llu, ngc:%#lx, egc:%#lx\n",
                      function_name, function_line, (unsigned long long int)key,
                      (unsigned long long int)target,
                      (unsigned long long int)MITSUME_GET_PTR_LH(target),
                      (long unsigned int)MITSUME_GET_PTR_NEXT_VERSION(target),
                      (long unsigned int)MITSUME_GET_PTR_ENTRY_VERSION(target));
}

/**
 * mitsume_tool_print_pointer: print mitsume_pointer
 * @input: input pointer
 * @function_name: caller
 * @function_line: caller Line's number
 * @thread_id: output thread's id(embedded inside metadata) to facilitate
 * debugging
 */
void mitsume_tool_print_gc_pointer(struct mitsume_ptr *entry,
                                   struct mitsume_ptr *gc_entry,
                                   const char *function_name, int function_line,
                                   uint64_t key) {
  uint64_t target;
  uint64_t second_target;
  if (entry)
    target = entry->pointer;
  else
    target = 0;
  second_target = gc_entry->pointer;
  MITSUME_PRINT_BRIEF(
      "(%s:%d)%llu-%#llx lh:%llu, ngc:%#lx, egc:%#lx; %#llx lh:%llu, ngc:%#lx, "
      "egc:%#lx\n",
      function_name, function_line, (unsigned long long int)key,
      (unsigned long long int)target,
      (unsigned long long int)MITSUME_GET_PTR_LH(target),
      (long unsigned int)MITSUME_GET_PTR_NEXT_VERSION(target),
      (long unsigned int)MITSUME_GET_PTR_ENTRY_VERSION(target),
      (unsigned long long int)second_target,
      (unsigned long long int)MITSUME_GET_PTR_LH(second_target),
      (long unsigned int)MITSUME_GET_PTR_NEXT_VERSION(second_target),
      (long unsigned int)MITSUME_GET_PTR_ENTRY_VERSION(second_target));
}

void mitsume_tool_print_message_header(struct mitsume_msg *input_msg,
                                       const char *function_name,
                                       int function_line, int thread_id) {
  struct mitsume_msg_header *header = &input_msg->msg_header;
  MITSUME_PRINT_BRIEF(
      "(%s:%d) type:%d src:%d des:%d th:%u ; addr:%llx rkey:%lx ma_id:%d\n",
      function_name, function_line, header->type, header->src_id,
      header->des_id, header->thread_id,
      (long long unsigned int)header->reply_attr.addr,
      (long unsigned int)header->reply_attr.rkey,
      header->reply_attr.machine_id);
}

/**
 * mitsume_tool_print_pointer_key_replication: (print whole replication pointer)
 * print mitsume_pointer and key(similar function - but also prints key)
 * @entry: target replication pointer array
 * @key: input key
 * @function_name: caller
 * @function_line: caller Line's number
 */
void mitsume_tool_print_pointer_key_replication(struct mitsume_ptr *entry,
                                                uint32_t replication_factor,
                                                mitsume_key key,
                                                int function_line,
                                                const char *function_name) {
  uint64_t target;
  int i;
  int max_iteration = replication_factor;
  if (!max_iteration) {
    max_iteration = MITSUME_MAX_REPLICATION;
  }
  if (entry) {
    for (i = 0; i < max_iteration; i++) {
      target = 0;
      target = entry[i].pointer;
      MITSUME_PRINT_BRIEF(
          "%s:%d(%lld)(%d:%d):%#llx lh:%lld, ngc:%#lx, egc:%#lx\n",
          function_name, function_line, (unsigned long long int)key, i,
          replication_factor, (unsigned long long int)target,
          (unsigned long long int)MITSUME_GET_PTR_LH(target),
          (long unsigned int)MITSUME_GET_PTR_NEXT_VERSION(target),
          (long unsigned int)MITSUME_GET_PTR_ENTRY_VERSION(target));
    }
  } else {
    MITSUME_PRINT_BRIEF("NULL input\n");
  }
}

/**
 * mitsume_struct_copy_ptr_replication: (for whole replication) setup the des
 * pointer equals to the src ptr (which can be implemented as memcpy also)
 * @des: target place
 * @src: source place
 * @replication_factor: length of the array (number of replication)
 */
int mitsume_struct_copy_ptr_replication(struct mitsume_ptr *des,
                                        struct mitsume_ptr *src,
                                        int replication_factor) {
  int i;
  for (i = 0; i < replication_factor; i++) {
    des[i].pointer = src[i].pointer;
  }
  return MITSUME_SUCCESS;
}

/**
 * mitsume_struct_set_ptr_replication: (for whole replication) setup the des
 * pointer equals to a specific number (which is similar to memset)
 * @des: target place
 * @replication_factor: length of the array (number of replication)
 * @uint64_t: number user want to set
 */
int mitsume_struct_set_ptr_replication(struct mitsume_ptr *des,
                                       int replication_factor, uint64_t set) {
  int i;
  for (i = 0; i < replication_factor; i++) {
    des[i].pointer = set;
  }
  return MITSUME_SUCCESS;
}

/**
 * mitsume_struct_move_entry_pointer_to_next_pointer: move entry pointer to next
 * pointer (mainly used by query processing) All pointer chasing related
 * function are framed with next-pointer type, however, the shortcut, hashtable,
 * and query keeps data-pointer way It needs a transformation between these two
 * modes
 * @lh: target next-lh
 * @offset: target next-offset
 */
uint64_t mitsume_struct_move_entry_pointer_to_next_pointer(uint64_t input) {
  uint64_t temp = input;
  temp = temp & (~MITSUME_PTR_MASK_NEXT_VERSION);
  temp = temp & (~MITSUME_PTR_MASK_ENTRY_VERSION);
  temp =
      temp | MITSUME_SET_PTR_NEXT_VERSION(MITSUME_GET_PTR_ENTRY_VERSION(input));
  return temp;
}

/**
 * mitsume_struct_move_replication_entry_pointer_to_next_pointer: (for whole
 * replication) move entry pointer to next pointer (mainly used by query
 * processing) it basically is a for loop calls
 * mitsume_struct_move_entry_pointer_to_next_pointer
 * @input_ptr: pointer to a array of pointer
 * @replication_factor: length of the array (number of replication)
 */
int mitsume_struct_move_replication_entry_pointer_to_next_pointer(
    struct mitsume_ptr *input_ptr, uint32_t replication_factor) {
  uint32_t i;
  if (!replication_factor || replication_factor > MITSUME_MAX_REPLICATION) {
    MITSUME_PRINT_ERROR("wrong factor %u\n", replication_factor);
    return MITSUME_ERROR;
  }
  for (i = 0; i < replication_factor; i++) {
    input_ptr[i].pointer =
        mitsume_struct_move_entry_pointer_to_next_pointer(input_ptr[i].pointer);
  }
  return MITSUME_SUCCESS;
}

/**
 *
 * mitsume_struct_move_next_pointer_to_entry_pointer: move next pointer to entry
 * pointer (mainly used by query processing) Reverse the modification from
 * move_entry_pointer_to_next_pointer
 * @lh: target next-lh
 * @offset: target next-offset
 */
uint64_t mitsume_struct_move_next_pointer_to_entry_pointer(uint64_t input) {
  uint64_t temp = input;
  temp = temp & (~MITSUME_PTR_MASK_NEXT_VERSION);
  temp = temp & (~MITSUME_PTR_MASK_ENTRY_VERSION);
  temp =
      temp | MITSUME_SET_PTR_ENTRY_VERSION(MITSUME_GET_PTR_NEXT_VERSION(input));
  return temp;
}

/**
 * mitsume_struct_move_replication_next_pointer_to_entry_pointer: (for whole
 * replication) move next pointer to entry pointer (mainly used by query
 * processing) it basically is a for loop calls
 * mitsume_struct_move_next_pointer_to_entry_pointer
 * @input_ptr: pointer to a array of pointer
 * @replication_factor: length of the array (number of replication)
 */
int mitsume_struct_move_replication_next_pointer_to_entry_pointer(
    struct mitsume_ptr *input_ptr, uint32_t replication_factor) {
  uint32_t i;
  if (!replication_factor || replication_factor > MITSUME_MAX_REPLICATION) {
    MITSUME_PRINT_ERROR("wrong factor %u\n", replication_factor);
    return MITSUME_ERROR;
  }
  for (i = 0; i < replication_factor; i++) {
    input_ptr[i].pointer =
        mitsume_struct_move_next_pointer_to_entry_pointer(input_ptr[i].pointer);
  }
  return MITSUME_SUCCESS;
}

/**
 * mitsume_tool_print_pointer_key: print mitsume_pointer and key(similar
 * function - but also prints key)
 * @input: input pointer
 * @shortcut: actually its second pointer
 * @key: input key
 * @function_name: caller
 * @function_line: caller Line's number
 */
void mitsume_tool_print_pointer_key(struct mitsume_ptr *entry,
                                    struct mitsume_ptr *shortcut,
                                    mitsume_key key, int function_line,
                                    const char *function_name) {
  uint64_t target;
  uint64_t shortcut_target;
  if (entry)
    target = entry->pointer;
  else
    target = 0;
  if (shortcut) {
    shortcut_target = shortcut->pointer;
    MITSUME_PRINT_BRIEF(
        "%s:%d(%llu)%#llx lh:%lld, ngc:%#lx, egc:%#lx; %#llx slh:%lld, "
        "sngc:%#lx, segc:%#lx\n",
        function_name, function_line, (unsigned long long int)key,
        (unsigned long long int)target,
        (unsigned long long int)MITSUME_GET_PTR_LH(target),
        (long unsigned int)MITSUME_GET_PTR_NEXT_VERSION(target),
        (long unsigned int)MITSUME_GET_PTR_ENTRY_VERSION(target),
        (unsigned long long int)shortcut_target,
        (unsigned long long int)MITSUME_GET_PTR_LH(shortcut_target),
        (long unsigned int)MITSUME_GET_PTR_NEXT_VERSION(shortcut_target),
        (long unsigned int)MITSUME_GET_PTR_ENTRY_VERSION(shortcut_target));
  } else {
    shortcut_target = 0;
    MITSUME_PRINT_BRIEF(
        "%s:%d(%llu)%#llx lh:%lld, ngc:%#lx, egc:%#lx\n", function_name,
        function_line, (unsigned long long int)key,
        (unsigned long long int)target,
        (unsigned long long int)MITSUME_GET_PTR_LH(target),
        (long unsigned int)MITSUME_GET_PTR_NEXT_VERSION(target),
        (long unsigned int)MITSUME_GET_PTR_ENTRY_VERSION(target));
  }
}

/**
 * mitsume_tool_setup_meshptr_pointer_header_by_newspace: setup a pointer-style
 * mesh pointer based on the new space information The data inside pointer will
 * be entry version only This funcion is used in write request only
 * @meshptr: pointer to a replication array
 * @query: old entry pointer
 * @newspace: allocated space metadata
 * @committed_flag: is this pointer committed or not
 * return: return error or success
 */
int mitsume_tool_setup_meshptr_pointer_header_by_newspace(
    struct mitsume_ptr **meshptr, struct mitsume_tool_communication *query,
    struct mitsume_tool_communication *newspace, uint32_t committed_flag) {
  int per_replication;
  int per_entry;
  int replication_factor = newspace->replication_factor;
  struct mitsume_ptr *tar_ptr_array;
  for (per_replication = 0; per_replication < replication_factor;
       per_replication++) {
    tar_ptr_array = meshptr[per_replication];
    for (per_entry = 0; per_entry < replication_factor; per_entry++) {
      tar_ptr_array[per_entry].pointer = mitsume_struct_set_pointer(
          MITSUME_GET_PTR_LH(newspace->replication_ptr[per_entry].pointer),
          MITSUME_GET_PTR_ENTRY_VERSION(
              newspace->replication_ptr[per_entry].pointer),
          MITSUME_GET_PTR_ENTRY_VERSION(
              query->replication_ptr[per_replication].pointer),
          0, committed_flag);
    }
  }
  return MITSUME_SUCCESS;
}

/**
 * mitsume_struct_checknull_ptr_replication: (for whole replication) check
 * whether there contains number in all replications
 * @src: target check place
 * @replication_factor: length of the array (number of replication)
 * @function_line: caller's line
 * @function_name: caller function's name
 * return: either success or fail (if it exists NULL)
 */
int mitsume_struct_checknull_ptr_replication(struct mitsume_ptr *src,
                                             int replication_factor,
                                             int function_line,
                                             const char *function_name) {
  int i, flag = MITSUME_SUCCESS;
  for (i = 0; i < replication_factor; i++) {
    if (!src[i].pointer) {
      MITSUME_PRINT_BRIEF("%s:%d rep:%d %#llx is NULL\n", function_name,
                          function_line, i,
                          (unsigned long long int)src[i].pointer);
      // MITSUME_TOOL_PRINT_POINTER(&des[i].pointer, &src[i].pointer);
      flag = MITSUME_ERROR;
    }
  }

  return flag;
}

/**
 * mitsume_struct_checknull_ptr_replication: (for whole replication) check
 * whether the src pointer should be identical to des pointer
 * @src: target check place-1
 * @des: target check place-2
 * @replication_factor: length of the array (number of replication)
 * @function_line: caller's line
 * @function_name: caller function's name
 * return: either success or fail (if it exists an unmatch pointer)
 */
int mitsume_struct_checkequal_ptr_replication(struct mitsume_ptr *des,
                                              struct mitsume_ptr *src,
                                              int replication_factor,
                                              int function_line,
                                              const char *function_name) {
  int i;
  for (i = 0; i < replication_factor; i++) {
    if (des[i].pointer != src[i].pointer) {
      MITSUME_PRINT_BRIEF("%s:%d rep:%d %#llx %#llx doesn't match\n",
                          function_name, function_line, i,
                          (unsigned long long int)des[i].pointer,
                          (unsigned long long int)src[i].pointer);
      // MITSUME_TOOL_PRINT_POINTER(&des[i].pointer, &src[i].pointer);
    }
  }
  return MITSUME_SUCCESS;
}
