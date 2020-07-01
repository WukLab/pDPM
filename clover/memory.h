
#ifndef MITSUME_MEMORY
#define MITSUME_MEMORY
#include "mitsume.h"
void *run_memory(void *arg);
void *main_memory(void *arg);

int memory_allocate_shortcut(struct configuration_params *input_arg);
int memory_allocate_memoryspace(struct configuration_params *input_arg);
#endif
