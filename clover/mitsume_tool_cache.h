
#ifndef MITSUME_TOOL_CACHE
#define MITSUME_TOOL_CACHE
#include "mitsume.h"

void *mitsume_tool_cache_alloc(int request_type);
void mitsume_tool_cache_free(void *ptr, int request_type);
#endif
