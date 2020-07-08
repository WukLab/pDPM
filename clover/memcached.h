#ifndef RSEC_MEMCACHED_HEADER
#define RSEC_MEMCACHED_HEADER

#include "ibsetup.h"
#include "mitsume.h"
#include <libmemcached/memcached.h>

#define MEMCACHED_MAX_NAME_LEN 256

extern char MEMCACHED_IP[64];

void memcached_publish_rcqp(struct ib_inf *inf, int num, const char *qp_name);
void memcached_publish_udqp(struct ib_inf *inf, int num, const char *qp_name);
struct ib_qp_attr *memcached_get_published_qp(const char *qp_name);
int memcached_get_published(const char *key, void **value);
void *memcached_get_published_size(const char *tar_name, int size);
memcached_st *memcached_create_memc(void);
void memcached_publish(const char *key, void *value, int len);
struct ib_mr_attr *memcached_get_published_mr(const char *mr_name);

#endif
