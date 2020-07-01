#ifndef MITSUME_TOOL_GC
#define MITSUME_TOOL_GC
#include "mitsume.h"
#include "mitsume_con_alloc.h"
#include "mitsume_tool_cache.h"

#include "mutex"
#include "unordered_map"
using namespace std;

enum mitsume_tool_gc_mode {
  MITSUME_TOOL_GC_REGULAR_PROCESSING,
  MITSUME_TOOL_GC_UPDATE_SHORTCUT_ONLY
};

int mitsume_tool_gc_submit_request(
    struct mitsume_consumer_metadata *thread_metadata, mitsume_key key,
    struct mitsume_tool_communication *old_entry,
    struct mitsume_tool_communication *new_entry, int gc_mode);
int mitsume_tool_gc_init(struct mitsume_ctx_clt *ctx_clt);
#endif
