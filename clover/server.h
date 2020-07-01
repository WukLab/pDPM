#ifndef MITSUME_SERVER
#define MITSUME_SERVER
#include "mitsume.h"
#include "mitsume_con_alloc.h"
#include "mitsume_con_thread.h"
void *run_server(void *arg);
void *main_server(void *arg);

struct mitsume_ctx_con *server_init(struct configuration_params *input_arg);
int server_get_shortcut(struct mitsume_ctx_con *server_con);
int server_setup_post_recv(struct configuration_params *input_arg,
                           struct mitsume_ctx_con *context);
#endif
