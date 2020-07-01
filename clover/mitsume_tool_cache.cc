#include "mitsume_tool_cache.h"

void *mitsume_tool_cache_alloc(int request_type) {
  switch (request_type) {
  case MITSUME_ALLOCTYPE_PTR_REPLICATION:
    return new struct mitsume_ptr[MITSUME_MAX_REPLICATION];
  case MITSUME_ALLOCTYPE_IB_SGE:
    return new struct ibv_sge[MITSUME_TOOL_MAX_IB_SGE_SIZE];
  case MITSUME_ALLOCTYPE_HASH_STRUCT:
    return new struct mitsume_hash_struct;
  case MITSUME_ALLOCTYPE_MSG:
    return new struct mitsume_msg;
  case MITSUME_ALLOCTYPE_GC_HASHED_ENTRY:
    return new struct mitsume_gc_hashed_entry;
  case MITSUME_ALLOCTYPE_GC_SINGLE_HASHED_ENTRY:
    return new struct mitsume_gc_single_hashed_entry;
  case MITSUME_ALLOCTYPE_GC_THREAD_REQUEST:
    return new struct mitsume_gc_thread_request;
  case MITSUME_ALLOCTYPE_GC_SHORTCUT_UPDATE_ELEMENT:
    return new struct mitsume_consumer_gc_shortcut_update_element;
  default:
    die_printf("wrong allocation - %d\n", request_type);
  }

  return NULL;
}
void mitsume_tool_cache_free(void *ptr, int request_type) {
  char *del_ptr = (char *)ptr;
  switch (request_type) {
  case MITSUME_ALLOCTYPE_PTR_REPLICATION:
  case MITSUME_ALLOCTYPE_IB_SGE:
  case MITSUME_ALLOCTYPE_HASH_STRUCT:
  case MITSUME_ALLOCTYPE_MSG:
  case MITSUME_ALLOCTYPE_GC_HASHED_ENTRY:
  case MITSUME_ALLOCTYPE_GC_SINGLE_HASHED_ENTRY:
  case MITSUME_ALLOCTYPE_GC_THREAD_REQUEST:
  case MITSUME_ALLOCTYPE_GC_SHORTCUT_UPDATE_ELEMENT:
    delete del_ptr;
    return;
  default:
    die_printf("wrong free - %d\n", request_type);
  }
  return;
}
