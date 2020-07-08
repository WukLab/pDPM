#include "memcached.h"

__thread memcached_st *memc = NULL;
char MEMCACHED_IP[64];

memcached_st *memcached_create_memc(void) {
  memcached_server_st *servers = NULL;
  memcached_st *memc = memcached_create(NULL);
  memcached_return rc;

  memc = memcached_create(NULL);
  const char *registry_ip = MEMCACHED_IP;

  /* We run the memcached server on the default memcached port */
  servers = memcached_server_list_append(servers, registry_ip,
                                         MEMCACHED_DEFAULT_PORT, &rc);
  rc = memcached_server_push(memc, servers);
  CPE(rc != MEMCACHED_SUCCESS, "Couldn't add memcached server.\n", -1);

  return memc;
}

void memcached_publish(const char *key, void *value, int len) {
  assert(key != NULL && value != NULL && len > 0);
  memcached_return rc;

  if (memc == NULL) {
    memc = memcached_create_memc();
  }

  rc = memcached_set(memc, key, strlen(key), (const char *)value, len,
                     (time_t)0, (uint32_t)0);
  if (rc != MEMCACHED_SUCCESS) {
    const char *registry_ip = MEMCACHED_IP;
    fprintf(stderr,
            "\tHRD: Failed to publish key %s. Error %s. "
            "Reg IP = %s\n",
            key, memcached_strerror(memc, rc), registry_ip);
    exit(-1);
  }
}

void memcached_publish_rcqp(struct ib_inf *inf, int num, const char *qp_name) {
  assert(inf != NULL);
  assert(num >= 0 && num < inf->num_local_rcqps);

  assert(qp_name != NULL && strlen(qp_name) < RSEC_MAX_QP_NAME - 1);
  assert(strstr(qp_name, RSEC_RESERVED_NAME_PREFIX) == NULL);

  int len = strlen(qp_name);
  int i;
  for (i = 0; i < len; i++) {
    if (qp_name[i] == ' ') {
      fprintf(stderr, "Space not allowed in QP name\n");
      exit(-1);
    }
  }
  struct ib_qp_attr qp_attr;
  memcpy(qp_attr.name, qp_name, len);
  qp_attr.name[len] = 0; /* Add the null terminator */
  // qp_attr.buf_addr = (uint64_t)inf->rcqp_buf[num];
  // qp_attr.rkey = (uint32_t)inf->rcqp_buf_mr[num]->rkey;
  qp_attr.lid = ib_get_local_lid(inf->conn_qp[num]->context, inf->dev_port_id);
  qp_attr.qpn = inf->conn_qp[num]->qp_num;
  qp_attr.sl = P15_RC_SL;

  if (RSEC_NETWORK_MODE == RSEC_NETWORK_ROCE) {
    qp_attr.remote_gid = inf->local_gid;
  }
  // printf("rc_publish: %d %s %d %d %lu %lu\n",
  //        num, qp_name, qp_attr.lid, qp_attr.qpn, qp_attr.buf_addr,
  //        qp_attr.rkey);
  memcached_publish(qp_attr.name, &qp_attr, sizeof(struct ib_qp_attr));
}

void memcached_publish_udqp(struct ib_inf *inf, int num, const char *qp_name) {
  assert(inf != NULL);
  assert(num >= 0 && num < inf->num_local_udqps);

  assert(qp_name != NULL && strlen(qp_name) < RSEC_MAX_QP_NAME - 1);
  assert(strstr(qp_name, RSEC_RESERVED_NAME_PREFIX) == NULL);

  int len = strlen(qp_name);
  int i;
  for (i = 0; i < len; i++) {
    if (qp_name[i] == ' ') {
      fprintf(stderr, "Space not allowed in QP name\n");
      exit(-1);
    }
  }
  struct ib_qp_attr qp_attr;
  memcpy(qp_attr.name, qp_name, len);
  qp_attr.name[len] = 0; /* Add the null terminator */
  // qp_attr.buf_addr = (uint64_t) (uintptr_t) cb->conn_buf; //Didn't use buffer
  // in P15
  // qp_attr.buf_size = cb->conn_buf_size;			//Didn't use buffer in
  // P15 qp_attr.rkey = cb->conn_buf_mr->rkey;			//Didn't use
  // buffer in P15
  qp_attr.lid = ib_get_local_lid(inf->dgram_qp[num]->context, inf->dev_port_id);
  qp_attr.qpn = inf->dgram_qp[num]->qp_num;
  qp_attr.sl = P15_UD_SL;
  memcached_publish(qp_attr.name, &qp_attr, sizeof(struct ib_qp_attr));
}

int memcached_get_published(const char *key, void **value) {
  assert(key != NULL);
  if (memc == NULL) {
    memc = memcached_create_memc();
  }
  memcached_return rc;
  size_t value_length;
  uint32_t flags;

  *value = memcached_get(memc, key, strlen(key), &value_length, &flags, &rc);

  if (rc == MEMCACHED_SUCCESS) {
    return (int)value_length;
  } else if (rc == MEMCACHED_NOTFOUND) {
    assert(*value == NULL);
    return -1;
  } else {
    const char *registry_ip = MEMCACHED_IP;
    fprintf(stderr,
            "Error finding value for key \"%s\": %s. "
            "Reg IP = %s\n",
            key, memcached_strerror(memc, rc), registry_ip);
    exit(-1);
  }
  /* Never reached */
  assert(false);
}

struct ib_qp_attr *memcached_get_published_qp(const char *qp_name) {
  struct ib_qp_attr *ret;
  assert(qp_name != NULL && strlen(qp_name) < RSEC_MAX_QP_NAME - 1);
  assert(strstr(qp_name, RSEC_RESERVED_NAME_PREFIX) == NULL);

  int len = strlen(qp_name);
  int i;
  int ret_len;
  for (i = 0; i < len; i++) {
    if (qp_name[i] == ' ') {
      fprintf(stderr, "Space not allowed in QP name\n");
      exit(-1);
    }
  }
  do {
    ret_len = memcached_get_published(qp_name, (void **)&ret);
  } while (ret_len <= 0);
  /*
   * The registry lookup returns only if we get a unique QP for @qp_name, or
   * if the memcached lookup succeeds but we don't have an entry for @qp_name.
   */
  assert(ret_len == sizeof(struct ib_qp_attr) || ret_len == -1);

  return ret;
}

struct ib_mr_attr *memcached_get_published_mr(const char *mr_name) {
  struct ib_mr_attr *ret;
  assert(mr_name != NULL && strlen(mr_name) < RSEC_MAX_QP_NAME - 1);
  assert(strstr(mr_name, RSEC_RESERVED_NAME_PREFIX) == NULL);

  int len = strlen(mr_name);
  int i;
  int ret_len;
  for (i = 0; i < len; i++) {
    if (mr_name[i] == ' ') {
      fprintf(stderr, "Space not allowed in QP name\n");
      exit(-1);
    }
  }
  do {
    ret_len = memcached_get_published(mr_name, (void **)&ret);
  } while (ret_len <= 0);
  /*
   * The registry lookup returns only if we get a unique QP for @qp_name, or
   * if the memcached lookup succeeds but we don't have an entry for @qp_name.
   */
  assert(ret_len == sizeof(struct ib_mr_attr) || ret_len == -1);

  return ret;
}

void *memcached_get_published_size(const char *tar_name, int size) {
  void *ret;
  assert(tar_name != NULL && strlen(tar_name) < RSEC_MAX_QP_NAME - 1);
  assert(strstr(tar_name, RSEC_RESERVED_NAME_PREFIX) == NULL);

  int len = strlen(tar_name);
  int i;
  int ret_len;
  for (i = 0; i < len; i++) {
    if (tar_name[i] == ' ') {
      fprintf(stderr, "Space not allowed in QP name\n");
      exit(-1);
    }
  }
  do {
    ret_len = memcached_get_published(tar_name, (void **)&ret);
  } while (ret_len <= 0);
  /*
   * The registry lookup returns only if we get a unique QP for @qp_name, or
   * if the memcached lookup succeeds but we don't have an entry for @qp_name.
   */
  if (ret_len != size) {
    fprintf(stderr, "%llu:%llu size doesn't match\n",
            (unsigned long long int)ret_len, (unsigned long long int)size);
    assert(ret_len == size || ret_len == -1);
  }
  assert(ret_len == size || ret_len == -1);

  return ret;
}
