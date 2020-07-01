
#ifndef MITSUME_CLIENT
#define MITSUME_CLIENT
#include "mitsume.h"
#include "mitsume_clt_test.h"
#include "mitsume_clt_thread.h"
#include "mitsume_clt_tool.h"
void *run_client(void *arg);
void *main_client(void *arg);

int client_setup_post_recv(struct configuration_params *input_arg,
                           struct mitsume_ctx_clt *context);

#endif
