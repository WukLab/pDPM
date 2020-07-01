#ifndef MITSUME_CLT_THREAD
#define MITSUME_CLT_THREAD
#include "mitsume.h"

#include "mutex"
#include "unordered_map"
using namespace std;
int mitsume_clt_thread_metadata_setup(struct configuration_params *input_arg,
                                      struct mitsume_ctx_clt *client_ctx);

#endif
