#include "ibsetup.h"

std::unordered_map<uint64_t, unsigned int> WR_ID_WAITING_TABLE;

void *p15_malloc(size_t length) {
  void *ret = malloc(length);
  assert(ret != 0);
  return ret;
}

int p15_ib_post_recv_ud_qp(struct ib_inf *inf, int udqp_index,
                           int post_recv_base, int post_recv_num) {
  int i, count = 0;
  struct ibv_recv_wr input_wr, *bad_wr;
  struct ibv_sge sge[2];
  int ret;
  if (post_recv_num + post_recv_base > RSEC_CQ_DEPTH) {
    dbg_printf("[%s] post too big nums as %d (%d)\n", __func__,
               post_recv_num + post_recv_base, RSEC_CQ_DEPTH);
    return MITSUME_ERROR;
  }
  assert(post_recv_num > 0 && post_recv_base >= 0);
  for (i = post_recv_base; i < post_recv_num + post_recv_base; i++) {
    sge[0].addr = (uintptr_t)inf->dgram_buf_mr[udqp_index][i]->addr;
    sge[0].length = inf->dgram_buf_mr[udqp_index][i]->length;
    sge[0].lkey = inf->dgram_buf_mr[udqp_index][i]->lkey;
    // if(i==0)
    //      dbg_printf("[%s] %lx %lx %lx\n", __func__, sge[0].addr, (long
    //      unsigned int)sge[0].length, (long unsigned int)sge[0].lkey);
    input_wr.next = NULL;
    input_wr.sg_list = sge;
    input_wr.wr_id =
        ((uint64_t)i << P15_UD_POST_RECV_ID_SHIFT) + (uint64_t)sge[0].addr;
    input_wr.num_sge = 1;
    ret = ibv_post_recv(inf->dgram_qp[udqp_index], &input_wr, &bad_wr);
    if (ret) {
      dbg_printf("[%s] QP %d index %d fail to post_recv. ret %d\n", __func__,
                 udqp_index, i, ret);
    } else
      count++;
  }
  return count;
}

struct ibv_device *ib_get_device(struct ib_inf *inf, int port) {
  struct ibv_device **dev_list;
  struct ibv_context *ctx;
  struct ibv_device_attr device_attr;
  struct ibv_port_attr port_attr;
  int i;
  int num_devices;
  dev_list = ibv_get_device_list(&num_devices);
  if (num_devices ==
      0) // assuming we only have one device now, need to modify this part later
    die_printf("%s: num_devices==0\n", __func__);
  if (num_devices <= inf->device_id)
    die_printf("%s: device_id:%d overflow available num_devices:%d\n", __func__,
               inf->device_id, num_devices);
  i = inf->device_id;
  {
    ctx = ibv_open_device(dev_list[i]);
    if (ibv_query_device(ctx, &device_attr))
      die_printf("%s: failed to query device %d\n", __func__, i);

    MITSUME_PRINT("running on %s\n", ibv_get_device_name(dev_list[i]));
    if (device_attr.phys_port_cnt < port)
      die_printf("%s: port not enough %d:%d\n", __func__, port,
                 device_attr.phys_port_cnt);
    if (ibv_query_port(ctx, port, &port_attr))
      die_printf("%s: can't query port %d\n", __func__, port);
    inf->device_id = i;
    inf->dev_port_id = port;
    return dev_list[i];
  }
  return NULL;
}

union ibv_gid ib_get_gid(struct ibv_context *context, int port_index) {
  union ibv_gid ret_gid;
  int ret;
  ret = ibv_query_gid(context, port_index, RSEC_SGID_INDEX, &ret_gid);
  if (ret)
    fprintf(stderr, "get GID fail\n");

  fprintf(stderr, "GID: Interface id = %lld subnet prefix = %lld\n",
          (long long)ret_gid.global.interface_id,
          (long long)ret_gid.global.subnet_prefix);

  return ret_gid;
}

uint16_t ib_get_local_lid(struct ibv_context *ctx, int dev_port_id) {
  assert(ctx != NULL && dev_port_id >= 1);

  struct ibv_port_attr attr;
  if (ibv_query_port(ctx, dev_port_id, &attr)) {
    die_printf("HRD: ibv_query_port on port %d of device %s failed! Exiting.\n",
               dev_port_id, ibv_get_device_name(ctx->device));
    assert(0);
  }

  return attr.lid;
}

void ib_create_udqps(struct ib_inf *inf)
/*
   1. dgram_send\recv_cq: create cqs
   1. dgram_qp: change all qp to RTS
   2. dgram_buf[num_local_udpqs][UD_CQ_DEPTH]: malloc all elements and register
   memory
   */
{
  int i, j;
  assert(inf->dgram_qp != NULL && inf->dgram_send_cq != NULL &&
         inf->dgram_recv_cq != NULL && inf->pd != NULL && inf->ctx != NULL);
  assert(inf->num_local_udqps >= 1 && inf->dev_port_id >= 1);

  for (i = 0; i < inf->num_local_udqps; i++) {
    struct ibv_qp_init_attr create_attr;
    struct ibv_qp_attr init_attr;
    struct ibv_qp_attr rtr_attr;
    inf->dgram_send_cq[i] =
        ibv_create_cq(inf->ctx, RSEC_CQ_DEPTH, NULL, NULL, 0);
    assert(inf->dgram_send_cq[i] != NULL);

    inf->dgram_recv_cq[i] =
        ibv_create_cq(inf->ctx, RSEC_CQ_DEPTH, NULL, NULL, 0);
    assert(inf->dgram_recv_cq[i] != NULL);

    /* Initialize creation attributes */
    memset((void *)&create_attr, 0, sizeof(struct ibv_qp_init_attr));
    create_attr.send_cq = inf->dgram_send_cq[i];
    create_attr.recv_cq = inf->dgram_recv_cq[i];
    // dbg_printf("[%s] %lx %lx\n", __func__, (long unsigned
    // int)inf->dgram_send_cq[i], (long unsigned int)inf->dgram_recv_cq[i]);
    create_attr.qp_type = IBV_QPT_UD;

    create_attr.cap.max_send_wr = RSEC_CQ_DEPTH;
    create_attr.cap.max_recv_wr = RSEC_CQ_DEPTH;
    create_attr.cap.max_send_sge = MITSUME_TOOL_MAX_IB_SGE_SIZE;
    create_attr.cap.max_recv_sge = MITSUME_TOOL_MAX_IB_SGE_SIZE;
    create_attr.cap.max_inline_data = P15_MAX_INLINE;
    create_attr.sq_sig_all = 0;

    inf->dgram_qp[i] = ibv_create_qp(inf->pd, &create_attr);
    assert(inf->dgram_qp[i] != NULL);

    /* INIT state */
    memset((void *)&init_attr, 0, sizeof(struct ibv_qp_attr));
    init_attr.qp_state = IBV_QPS_INIT;
    init_attr.pkey_index = 0;
    init_attr.port_num = inf->dev_port_id;
    init_attr.qkey = P15_UD_QKEY;

    if (ibv_modify_qp(inf->dgram_qp[i], &init_attr,
                      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                          IBV_QP_QKEY)) {
      fprintf(stderr, "Failed to modify dgram QP to INIT\n");
      return;
    }

    /* RTR state */
    memset((void *)&rtr_attr, 0, sizeof(struct ibv_qp_attr));
    rtr_attr.qp_state = IBV_QPS_RTR;

    if (ibv_modify_qp(inf->dgram_qp[i], &rtr_attr, IBV_QP_STATE)) {
      fprintf(stderr, "Failed to modify dgram QP to RTR\n");
      exit(-1);
    }

    /* Reuse rtr_attr for RTS */
    rtr_attr.qp_state = IBV_QPS_RTS;
    rtr_attr.sq_psn = P15_UD_PSN;

    if (ibv_modify_qp(inf->dgram_qp[i], &rtr_attr,
                      IBV_QP_STATE | IBV_QP_SQ_PSN)) {
      fprintf(stderr, "Failed to modify dgram QP to RTS\n");
      exit(-1);
    }
    // create recv_buf for ud QPs
    inf->dgram_buf[i] = (void **)malloc(sizeof(void **) * RSEC_CQ_DEPTH);
    inf->dgram_buf_mr[i] =
        (struct ibv_mr **)malloc(sizeof(struct ibv_mr *) * RSEC_CQ_DEPTH);
    for (j = 0; j < RSEC_CQ_DEPTH; j++) {
      inf->dgram_buf[i][j] =
          p15_malloc(sizeof(struct P15_message_frame) + UD_SHIFT_SIZE);
      assert(inf->dgram_buf[i][j] != NULL);
      inf->dgram_buf_mr[i][j] =
          ibv_reg_mr(inf->pd, inf->dgram_buf[i][j],
                     sizeof(struct P15_message_frame) + UD_SHIFT_SIZE,
                     IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                         IBV_ACCESS_REMOTE_READ);
      assert(inf->dgram_buf_mr[i][j] != NULL);
    }
  }
}

void ib_create_rcqps(struct ib_inf *inf, int role_int) {
  int i;
  assert(inf->conn_qp != NULL && inf->conn_cq != NULL && inf->pd != NULL &&
         inf->ctx != NULL);
  assert(inf->num_local_rcqps >= 1 && inf->dev_port_id >= 1);
  inf->server_recv_cq = ibv_create_cq(
      inf->ctx, RSEC_CQ_DEPTH * inf->num_local_rcqps, NULL, NULL, 0);

  for (i = 0; i < inf->num_local_rcqps; i++) {
    inf->conn_cq[i] = ibv_create_cq(inf->ctx, RSEC_CQ_DEPTH, NULL, NULL, 0);
    assert(inf->conn_cq[i] != NULL);
    struct ibv_qp_init_attr create_attr;
    memset(&create_attr, 0, sizeof(struct ibv_qp_init_attr));
    create_attr.send_cq = inf->conn_cq[i];
    create_attr.recv_cq = inf->server_recv_cq;
    // create_attr.recv_cq = inf->conn_cq[i];
    create_attr.qp_type = IBV_QPT_RC;

    create_attr.cap.max_send_wr = RSEC_CQ_DEPTH;
    create_attr.cap.max_recv_wr = RSEC_CQ_DEPTH;
    create_attr.cap.max_send_sge = MITSUME_TOOL_MAX_IB_SGE_SIZE;
    create_attr.cap.max_recv_sge = MITSUME_TOOL_MAX_IB_SGE_SIZE;
    create_attr.cap.max_inline_data = P15_MAX_INLINE;
    create_attr.sq_sig_all = 0;

    inf->conn_qp[i] = ibv_create_qp(inf->pd, &create_attr);
    assert(inf->conn_qp[i] != NULL);

    struct ibv_qp_attr init_attr;
    memset(&init_attr, 0, sizeof(struct ibv_qp_attr));
    init_attr.qp_state = IBV_QPS_INIT;
    init_attr.pkey_index = 0;
    init_attr.port_num = inf->dev_port_id;
    init_attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE |
                                IBV_ACCESS_REMOTE_READ |
                                IBV_ACCESS_REMOTE_ATOMIC;
    if (ibv_modify_qp(inf->conn_qp[i], &init_attr,
                      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                          IBV_QP_ACCESS_FLAGS)) {
      fprintf(stderr, "Failed to modify conn QP to INIT\n");
      exit(-1);
    }
    // fprintf(stdout, "generate qp-%d: qpn:%lu\n", i, inf->conn_qp[i]->qp_num);
    /*
    FILE *fd;
    char buffer[100];
    fd = fopen("/proc/phyaddr", "r");
    fgets(buffer, 100, fd);
    uint64_t phy_addr = atol(buffer);
    inf->rcqp_buf[i] = phy_addr;
    */
    /*uint64_t phy_addr = 1ULL << 32;
    inf->rcqp_buf[i] = phy_addr;
    uint64_t access_flag = IBV_EXP_ACCESS_LOCAL_WRITE |
        IBV_EXP_ACCESS_REMOTE_WRITE |
        IBV_EXP_ACCESS_REMOTE_READ |
        IBV_EXP_ACCESS_PHYSICAL_ADDR;
    struct ibv_exp_reg_mr_in phy={
        .pd = inf->pd,
        .addr = NULL,
        .length = 0,
        .exp_access = access_flag
    };
    inf->rcqp_buf_mr[i] = ibv_exp_reg_mr(&phy);
    assert(inf->rcqp_buf_mr[i] != NULL);
    fprintf(stdout, "%s: to qp%d, phy_addr:%lu, rkey:%lu\n",
            __func__, i, phy_addr, (unsigned long) inf->rcqp_buf_mr[i]->rkey);*/
  }
}

struct ib_inf *ib_setup(int id, int port, int num_rcqp_to_server,
                        int num_rcqp_to_client, int num_udqps, int num_loopback,
                        int total_machines, int device_id, int role_int) {
  /*
   * Check arguments for sanity.
   * @local_hid can be anything: it's just control block identifier that is
   * useful in printing debug info.
   */
  struct ib_inf *inf;
  struct ibv_device *ib_dev;
  int num_conn_qps;
  assert(port >= 0 && port <= 16);
  // assert(numa_node_id >= -1 && numa_node_id <= 8);
  assert(num_rcqp_to_server >= 0 &&
         num_rcqp_to_client > 0); // at least one client/one memory
  assert(num_udqps > 0);          // at least one ud to server (one server)

  // assert(dgram_buf_size >= 0 && dgram_buf_size <= M_1024);

  if (num_udqps == 0) {
    die_printf("%s: error UDqps\n", __func__);
  }

  inf = (struct ib_inf *)malloc(sizeof(struct ib_inf));
  memset(inf, 0, sizeof(struct ib_inf));

  /* Fill in the control block */
  inf->local_id = id;
  inf->port_index = port;
  inf->device_id = device_id;
  inf->num_rc_qp_to_server = num_rcqp_to_server;
  inf->num_rc_qp_to_client = num_rcqp_to_client;

  num_conn_qps = num_rcqp_to_server + num_rcqp_to_client;
  inf->global_machines = total_machines;

  inf->num_local_rcqps = num_conn_qps;
  inf->num_global_rcqps = inf->global_machines * inf->num_local_rcqps;
  inf->num_local_udqps = num_udqps;
  inf->num_global_udqps = inf->global_machines * inf->num_local_udqps;

  /* Get the device to use. This fills in cb->device_id and cb->dev_port_id */
  ib_dev = ib_get_device(inf, port);
  CPE(!ib_dev, "IB device not found", 0);

  /* Use a single device context and PD for all QPs */
  inf->ctx = ibv_open_device(ib_dev);
  CPE(!inf->ctx, "Couldn't get context", 0);

  inf->pd = ibv_alloc_pd(inf->ctx);
  CPE(!inf->pd, "Couldn't allocate PD", 0);

  /* Create an array in cb for holding work completions */
  inf->wc = (struct ibv_wc *)malloc(RSEC_CQ_DEPTH * sizeof(struct ibv_wc));
  assert(inf->wc != NULL);
  memset(inf->wc, 0, RSEC_CQ_DEPTH * sizeof(struct ibv_wc));
  inf->all_rcqps = (struct ib_qp_attr **)malloc(inf->num_global_rcqps *
                                                sizeof(struct ib_qp_attr *));

  inf->rcqp_buf = (uint64_t *)malloc(sizeof(uint64_t *) * inf->num_local_rcqps);
  inf->rcqp_buf_mr =
      (struct ibv_mr **)malloc(sizeof(struct ibv_mr **) * inf->num_local_rcqps);

  inf->all_udqps = (struct ib_qp_attr **)malloc(inf->num_global_udqps *
                                                sizeof(struct ib_qp_attr *));
  inf->dgram_ah =
      (struct ibv_ah **)malloc(inf->num_global_udqps * sizeof(struct ibv_ah *));

  inf->dgram_buf = (void ***)malloc(sizeof(void **) * inf->num_local_udqps);
  inf->dgram_buf_mr = (struct ibv_mr ***)malloc(sizeof(struct ibv_mr **) *
                                                inf->num_local_udqps);

  /*
   * Create datagram QPs and transition them RTS.
   * Create and register datagram RDMA buffer.
   */
  if (inf->num_local_udqps >= 1) {
    inf->dgram_qp = (struct ibv_qp **)malloc(inf->num_local_udqps *
                                             sizeof(struct ibv_qp *));
    inf->dgram_send_cq = (struct ibv_cq **)malloc(inf->num_local_udqps *
                                                  sizeof(struct ibv_cq *));
    inf->dgram_recv_cq = (struct ibv_cq **)malloc(inf->num_local_udqps *
                                                  sizeof(struct ibv_cq *));

    assert(inf->dgram_qp != NULL && inf->dgram_send_cq != NULL &&
           inf->dgram_recv_cq != NULL);
    ib_create_udqps(inf);
  }
  /*
   * Create connected QPs and transition them to RTS.
   * Create and register connected QP RDMA buffer.
   */
  if (inf->num_local_rcqps >= 1) {
    inf->conn_qp = (struct ibv_qp **)malloc(inf->num_local_rcqps *
                                            sizeof(struct ibv_qp *));
    inf->conn_cq = (struct ibv_cq **)malloc(inf->num_local_rcqps *
                                            sizeof(struct ibv_cq *));
    assert(inf->conn_qp != NULL && inf->conn_cq != NULL);
    ib_create_rcqps(inf, role_int);
  }
  // Create counter
  inf->ud_qp_counter =
      (uint64_t *)malloc(sizeof(uint64_t) * inf->num_local_udqps);
  memset(inf->ud_qp_counter, 0, sizeof(uint64_t) * inf->num_local_udqps);
  inf->rc_qp_counter =
      (uint64_t *)malloc(sizeof(uint64_t) * inf->num_local_rcqps);
  memset(inf->rc_qp_counter, 0, sizeof(uint64_t) * inf->num_local_rcqps);

  // setup gid which would be used by RoCE
  if (RSEC_NETWORK_MODE == RSEC_NETWORK_ROCE) {
    inf->local_gid = ib_get_gid(inf->ctx, inf->port_index);
  }

  return inf;
}

int ib_post_recv_connect_qp(struct ib_inf *context,
                            ib_post_recv_inf *post_recv_inf_list,
                            struct ib_mr_attr *input_mr_array,
                            int input_mr_array_length) {
  int i;
  int ret;
  for (i = 0; i < input_mr_array_length;
       i++) // need optimization to remove multiple post-recves
  {
    struct ibv_recv_wr recv_wr, *bad_wr;
    struct ibv_sge recv_sge;
    recv_sge.addr = input_mr_array[i].addr;
    recv_sge.lkey = input_mr_array[i].rkey;
    recv_sge.length = post_recv_inf_list[i].length;
    recv_wr.wr_id = RSEC_ID_COMBINATION(post_recv_inf_list[i].qp_index,
                                        post_recv_inf_list[i].mr_index);
    recv_wr.sg_list = &recv_sge;
    recv_wr.num_sge = 1;
    recv_wr.next = NULL;
    assert(context->conn_qp[post_recv_inf_list[i].qp_index] != 0);
    assert(recv_sge.addr);
    assert(recv_sge.lkey);

    ret = ibv_post_recv(context->conn_qp[post_recv_inf_list[i].qp_index],
                        &recv_wr, &bad_wr);
    CPE(ret, "ibv_post_recv error", ret);
    if (ret)
      return -ret;
  }
  return i;
}

struct ib_inf *ib_complete_setup(struct configuration_params *input_arg,
                                 int role_int, const char *role_str) {
  int machine_id;
  struct ib_inf *node_share_inf;
  int i, j;
  int cumulative_id = 0, total_machines, total_qp_count = 0;

  total_machines =
      input_arg->num_servers + input_arg->num_clients + input_arg->num_memorys;
  machine_id = input_arg->machine_id;
  node_share_inf = ib_setup(input_arg->machine_id, input_arg->base_port_index,
                            input_arg->num_servers * P15_PARALLEL_RC_QPS,
                            (input_arg->num_clients + input_arg->num_memorys) *
                                P15_PARALLEL_RC_QPS,
                            P15_PARALLEL_UD_QPS, input_arg->num_loopback,
                            total_machines, input_arg->device_id, role_int);

  node_share_inf->num_servers = input_arg->num_servers;
  node_share_inf->num_clients = input_arg->num_clients;
  node_share_inf->num_memorys = input_arg->num_memorys;
  node_share_inf->device_id = input_arg->device_id;
  node_share_inf->role = role_int;

  // post all rc qps
  for (i = 0; i < node_share_inf->num_local_rcqps; i++) {
    char srv_name[RSEC_MAX_QP_NAME];
    sprintf(srv_name, "machine-rc-%d-%d", machine_id, i);
    memcached_publish_rcqp(node_share_inf, i, srv_name);
    // MITSUME_PRINT("publish %s\n", srv_name);
  }
  // get all published rc qps
  for (cumulative_id = 0; cumulative_id < total_machines; cumulative_id++) {
    for (i = 0; i < node_share_inf->num_local_rcqps; i++) {
      char srv_name[RSEC_MAX_QP_NAME];
      sprintf(srv_name, "machine-rc-%d-%d", cumulative_id, i);
      node_share_inf->all_rcqps[total_qp_count] =
          memcached_get_published_qp(srv_name);
      total_qp_count++;
    }
    MITSUME_PRINT("get machine %d/%d\n", cumulative_id, total_machines - 1);
  }
  // connected all rc queue pairs
  total_qp_count = 0;
  for (i = 0; i < total_machines; i++) {
    for (j = 0; j < P15_PARALLEL_RC_QPS; j++) {
      if (i == machine_id) {
        total_qp_count++;
        continue;
      }
      int target_qp_num = i * total_machines * P15_PARALLEL_RC_QPS +
                          machine_id * P15_PARALLEL_RC_QPS + j;
      /*dbg_printf(
              "connect %d(%d): lid:%d qpn:%d sl:%d rkey:%lu\n",
              total_qp_count,
              target_qp_num,
              node_share_inf->all_rcqps[target_qp_num]->lid,
              node_share_inf->all_rcqps[target_qp_num]->qpn,
              node_share_inf->all_rcqps[target_qp_num]->sl,
              node_share_inf->all_rcqps[target_qp_num]->rkey
              );*/
      ib_connect_qp(node_share_inf, total_qp_count,
                    node_share_inf->all_rcqps[target_qp_num]);
      total_qp_count++;
    }
  }

  total_qp_count = 0;
  // post all ud qp
  for (i = 0; i < node_share_inf->num_local_udqps; i++) {
    char srv_name[RSEC_MAX_QP_NAME];
    sprintf(srv_name, "machine-ud-%d-%d", machine_id, i);
    memcached_publish_udqp(node_share_inf, i, srv_name);
  }
  // get all published ud qps
  for (cumulative_id = 0; cumulative_id < total_machines; cumulative_id++) {
    for (i = 0; i < node_share_inf->num_local_udqps; i++) {
      char srv_name[RSEC_MAX_QP_NAME];
      sprintf(srv_name, "machine-ud-%d-%d", cumulative_id, i);
      node_share_inf->all_udqps[total_qp_count] =
          memcached_get_published_qp(srv_name);
      total_qp_count++;
    }
    // dbg_printf("get machine UD %d\n", cumulative_id);
  }
  // connected all UD queue pairs

  /*for(i=0;i<node_share_inf->num_global_udqps;i++)
  {
      //dbg_printf("UD: list %d: %d %d %d\n", i,
  node_share_inf->all_udqps[i]->lid, node_share_inf->all_udqps[i]->qpn,
  node_share_inf->all_udqps[i]->sl); node_share_inf->dgram_ah[i] =
  ib_create_ah_for_ud(node_share_inf, i, node_share_inf->all_udqps[i]);
  }*/
  MITSUME_PRINT("done %s\n", role_str);
  // post_recv for local UD
  for (i = 0; i < node_share_inf->num_local_udqps; i++) {
    int ret;
    ret = p15_ib_post_recv_ud_qp(node_share_inf, i, 0, RSEC_CQ_DEPTH);
    if (ret != RSEC_CQ_DEPTH) {
      die_printf("[%s] fail to post recv UD QP %d ret %d\n", __func__, i, ret);
      exit(1);
    }
  }
  node_share_inf->local_memid = 0;

  return node_share_inf;
}

int ib_connect_qp(struct ib_inf *inf, int qp_index, struct ib_qp_attr *dest)
/*
   1.change conn_qp to RTS
   */
{
  struct ibv_qp_attr attr;
  attr.qp_state = IBV_QPS_RTR;
  attr.path_mtu =
      (RSEC_NETWORK_MODE == RSEC_NETWORK_ROCE) ? IBV_MTU_1024 : IBV_MTU_4096;
  attr.dest_qp_num = dest->qpn;
  attr.rq_psn = P15_UD_PSN;
  attr.max_dest_rd_atomic = 10;
  attr.min_rnr_timer = 12;
  attr.ah_attr.is_global = (RSEC_NETWORK_MODE == RSEC_NETWORK_ROCE) ? 1 : 0;
  attr.ah_attr.dlid = (RSEC_NETWORK_MODE == RSEC_NETWORK_ROCE) ? 0 : dest->lid;
  attr.ah_attr.sl = dest->sl;
  attr.ah_attr.src_path_bits = 0;
  attr.ah_attr.port_num = inf->port_index;

  if (RSEC_NETWORK_MODE == RSEC_NETWORK_ROCE) {
    // attr.ah_attr.grh.dgid.global.interface_id =
    // dest->remote_gid.global.interface_id;
    // attr.ah_attr.grh.dgid.global.subnet_prefix =
    // dest->remote_gid.global.subnet_prefix;
    attr.ah_attr.grh.dgid = dest->remote_gid;
    attr.ah_attr.grh.sgid_index = RSEC_SGID_INDEX;
    attr.ah_attr.grh.hop_limit = 1;
  }
  if (ibv_modify_qp(inf->conn_qp[qp_index], &attr,
                    IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                        IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                        IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER)) {
    fprintf(stderr, "[%s] Failed to modify QP to RTR\n", __func__);
    return 1;
  }
  attr.qp_state = IBV_QPS_RTS;
  attr.timeout = 14;
  attr.retry_cnt = 7;
  attr.rnr_retry = 7;
  attr.sq_psn = P15_UD_PSN;
  attr.max_rd_atomic = 16;
  attr.max_dest_rd_atomic = 16;
  if (ibv_modify_qp(inf->conn_qp[qp_index], &attr,
                    IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                        IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                        IBV_QP_MAX_QP_RD_ATOMIC)) {
    fprintf(stderr, "[%s] Failed to modify QP to RTS\n", __func__);
    return 2;
  }
  return 0;
}

struct ibv_ah *ib_create_ah_for_ud(struct ib_inf *inf, int ah_index,
                                   struct ib_qp_attr *dest) {
  struct ibv_ah_attr ah_attr;
  ah_attr.is_global = (RSEC_NETWORK_MODE == RSEC_NETWORK_ROCE) ? 1 : 0;
  ah_attr.dlid = (RSEC_NETWORK_MODE == RSEC_NETWORK_ROCE) ? 0 : dest->lid;
  ah_attr.sl = P15_UD_SL;
  ah_attr.src_path_bits = 0;
  ah_attr.port_num = inf->port_index;

  struct ibv_ah *tar_ah = ibv_create_ah(inf->pd, &ah_attr);
  return tar_ah;
}

inline int hrd_poll_cq(struct ibv_cq *cq, int num_comps, struct ibv_wc *wc,
                       int sleep_flag) {
  int comps = 0;

  while (comps < num_comps) {
    int new_comps = ibv_poll_cq(cq, num_comps - comps, &wc[comps]);
    if (new_comps != 0) {
      // Ideally, we should check from comps -> new_comps - 1
      if (wc[comps].status != 0) {
        fprintf(stderr, "Bad wc status %d\n", wc[comps].status);
        exit(0);
        return 1;
        // exit(0);
      }
      comps += new_comps;
    }
    if (sleep_flag)
      usleep(MITSUME_POLLING_SLEEP_TIME);
  }
  return MITSUME_SUCCESS;
}

void userspace_init_wr_table_value(uint64_t wr_id) {
  if (WR_ID_WAITING_TABLE.find(wr_id) != WR_ID_WAITING_TABLE.end()) {
    die_printf("%llu is already existed\n", (unsigned long long int)wr_id);
  }
  WR_ID_WAITING_TABLE[wr_id] = MITSUME_WR_NULL;
}

void userspace_wait_wr_table_value(uint64_t wr_id) {
  assert(wr_id < 1000000);
  if (WR_ID_WAITING_TABLE[wr_id] != MITSUME_WR_NULL) {
    MITSUME_PRINT_ERROR("wait %llu:%llx\n", (unsigned long long int)wr_id,
                        (unsigned long long int)WR_ID_WAITING_TABLE[wr_id]);
    assert(WR_ID_WAITING_TABLE[wr_id] == MITSUME_WR_NULL);
  }
  WR_ID_WAITING_TABLE[wr_id] = MITSUME_WR_WAIT;
}

inline void userspace_done_wr_table_value(uint64_t wr_id) {
  // MITSUME_PRINT("done %llu\n", (unsigned long long int)wr_id);
  assert(wr_id < 1000000);
  assert(WR_ID_WAITING_TABLE[wr_id] == MITSUME_WR_WAIT);
  WR_ID_WAITING_TABLE[wr_id] = MITSUME_WR_DONE;
}

inline int userspace_check_done_wr_table(uint64_t wr_id) {
  if (WR_ID_WAITING_TABLE[wr_id] == MITSUME_WR_DONE)
    return 1;
  else
    return 0;
}

void userspace_null_wr_table_value(uint64_t wr_id) {
  assert(wr_id < 1000000);
  // MITSUME_PRINT("null %llu\n", (unsigned long long int)wr_id);
  assert(WR_ID_WAITING_TABLE[wr_id] == MITSUME_WR_DONE);
  WR_ID_WAITING_TABLE[wr_id] = MITSUME_WR_NULL;
}

int userspace_one_send(struct ibv_qp *qp, struct ibv_mr *local_mr,
                       int request_size, uint64_t wr_id) {
  struct ibv_sge test_sge;
  struct ibv_send_wr wr, *bad_send_wr;
  int ret;
  test_sge.length = request_size;
  test_sge.addr = (uintptr_t)local_mr->addr;
  test_sge.lkey = local_mr->lkey;
  wr.wr_id = wr_id;
  wr.opcode = IBV_WR_SEND;
  wr.num_sge = 1;
  wr.next = NULL;
  wr.sg_list = &test_sge;
  wr.send_flags = IBV_SEND_SIGNALED;
  ret = ibv_post_send(qp, &wr, &bad_send_wr);
  if (ret)
    die_printf("%s-%d: ibv_post_send fail %llu\n", __func__, __LINE__,
               (unsigned long long int)wr.wr_id);
#ifdef MITSUME_TRAFFIC_IBLAYER_STAT
  MITSUME_STAT_FORCE_ADD(MITSUME_STAT_IB_RPC, request_size);
// MITSUME_STAT_TRAFFIC_ADD(remote_mr->machine_id, traffic);
#endif
  return 0;
}

inline int wr_id_to_qp_index(uint64_t wr_id, int remote_machine_id) {
  int ret = (remote_machine_id * P15_PARALLEL_RC_QPS) + (wr_id & 0x7);
  MITSUME_STAT_ARRAY_ADD(wr_id & 0x7, 1);
  return ret;
  // return (remote_machine_id*P15_PARALLEL_RC_QPS)+(wr_id&0x7);
}

int userspace_one_read(struct ib_inf *ib_ctx, uint64_t wr_id,
                       struct ibv_mr *local_mr, int request_size,
                       struct ib_mr_attr *remote_mr,
                       unsigned long long offset) {
  struct ibv_sge test_sge;
  struct ibv_send_wr wr, *bad_send_wr;
  int ret;
  test_sge.length = request_size;
  test_sge.addr = (uintptr_t)local_mr->addr;
  test_sge.lkey = local_mr->lkey;
  wr.opcode = IBV_WR_RDMA_READ;
  wr.wr_id = wr_id;
  wr.num_sge = 1;
  wr.next = NULL;
  wr.sg_list = &test_sge;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr = remote_mr->addr + offset;
  wr.wr.rdma.rkey = remote_mr->rkey;
  // MITSUME_PRINT("%llu:%llx:%llx\n", (unsigned long long int)wr.wr_id,
  // (unsigned long long int) test_sge.addr, (unsigned long long int)
  // test_sge.lkey); ret = ibv_post_send(ib_ctx->conn_qp[remote_mr->machine_id],
  // &wr, &bad_send_wr);
  int qp_idx = wr_id_to_qp_index(wr_id, remote_mr->machine_id);
  ret = ibv_post_send(ib_ctx->conn_qp[qp_idx], &wr, &bad_send_wr);
  if (ret)
    die_printf("%s-%d: ibv_post_send fail %d:%llu\n", __func__, __LINE__, ret,
               (unsigned long long int)wr.wr_id);
  CPE(ret, "ibv_post_send-read error", ret);
  return 0;
}

int userspace_one_cs(struct ib_inf *ib_ctx, uint64_t wr_id,
                     struct ibv_mr *local_mr, struct ib_mr_attr *remote_mr,
                     unsigned long long guess_value,
                     unsigned long long set_value) {
  struct ibv_sge test_sge;
  struct ibv_send_wr wr, *bad_send_wr;
  int ret;
  test_sge.length = sizeof(uint64_t);
  test_sge.addr = (uintptr_t)local_mr->addr;
  test_sge.lkey = local_mr->lkey;
  wr.wr_id = wr_id;
  wr.sg_list = &test_sge;
  wr.num_sge = 1;
  wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.next = NULL;
  wr.wr.atomic.remote_addr = remote_mr->addr;
  wr.wr.atomic.rkey = remote_mr->rkey;
  wr.wr.atomic.compare_add = guess_value;
  wr.wr.atomic.swap = set_value;
  // ret = ibv_post_send(ib_ctx->conn_qp[remote_mr->machine_id], &wr,
  // &bad_send_wr);
  int qp_idx = wr_id_to_qp_index(wr_id, remote_mr->machine_id);
  ret = ibv_post_send(ib_ctx->conn_qp[qp_idx], &wr, &bad_send_wr);
  if (ret)
    die_printf("%s-%d: ibv_post_send fail %d:%llu\n", __func__, __LINE__, ret,
               (unsigned long long int)wr.wr_id);
  return 0;
}

int userspace_one_write(struct ib_inf *ib_ctx, uint64_t wr_id,
                        struct ibv_mr *local_mr, int request_size,
                        struct ib_mr_attr *remote_mr,
                        unsigned long long offset) {
  struct ibv_sge test_sge;
  struct ibv_send_wr wr, *bad_send_wr;
  int ret;
  test_sge.length = request_size;
  test_sge.addr = (uintptr_t)local_mr->addr;
  test_sge.lkey = local_mr->lkey;
  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.num_sge = 1;
  wr.next = NULL;
  wr.sg_list = &test_sge;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr_id = wr_id;
  wr.wr.rdma.remote_addr = remote_mr->addr + offset;
  wr.wr.rdma.rkey = remote_mr->rkey;
  // ret = ibv_post_send(ib_ctx->conn_qp[remote_mr->machine_id], &wr,
  // &bad_send_wr);
  int qp_idx = wr_id_to_qp_index(wr_id, remote_mr->machine_id);
  ret = ibv_post_send(ib_ctx->conn_qp[qp_idx], &wr, &bad_send_wr);

  if (ret)
    die_printf("%s-%d: ibv_post_send fail %d:%llu\n", __func__, __LINE__, ret,
               (unsigned long long int)wr.wr_id);
  return 0;
}

int userspace_one_write_inline(struct ib_inf *ib_ctx, uint64_t wr_id,
                               struct ibv_mr *local_mr, int request_size,
                               struct ib_mr_attr *remote_mr,
                               unsigned long long offset) {
  struct ibv_sge test_sge;
  struct ibv_send_wr wr, *bad_send_wr;
  int ret;
  test_sge.length = request_size;
  test_sge.addr = (uintptr_t)local_mr->addr;
  test_sge.lkey = local_mr->lkey;
  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.num_sge = 1;
  wr.next = NULL;
  wr.sg_list = &test_sge;
  wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
  wr.wr_id = wr_id;
  wr.wr.rdma.remote_addr = remote_mr->addr + offset;
  wr.wr.rdma.rkey = remote_mr->rkey;
  // ret = ibv_post_send(ib_ctx->conn_qp[remote_mr->machine_id], &wr,
  // &bad_send_wr);
  int qp_idx = wr_id_to_qp_index(wr_id, remote_mr->machine_id);
  ret = ibv_post_send(ib_ctx->conn_qp[qp_idx], &wr, &bad_send_wr);

  if (ret)
    die_printf("%s-%d: ibv_post_send fail %d:%llu\n", __func__, __LINE__, ret,
               (unsigned long long int)wr.wr_id);
  return 0;
}

int userspace_one_write_sge(struct ib_inf *ib_ctx, uint64_t wr_id,
                            struct ib_mr_attr *remote_mr,
                            unsigned long long offset,
                            struct ibv_sge *input_sge, int input_sge_length) {
  struct ibv_send_wr wr, *bad_send_wr;
  int ret;
  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.num_sge = input_sge_length;
  wr.next = NULL;
  wr.sg_list = input_sge;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr_id = wr_id;
  wr.wr.rdma.remote_addr = remote_mr->addr + offset;
  wr.wr.rdma.rkey = remote_mr->rkey;
  // ret = ibv_post_send(ib_ctx->conn_qp[remote_mr->machine_id], &wr,
  // &bad_send_wr);
  int qp_idx = wr_id_to_qp_index(wr_id, remote_mr->machine_id);
  ret = ibv_post_send(ib_ctx->conn_qp[qp_idx], &wr, &bad_send_wr);
  if (ret)
    die_printf("%s-%d: ibv_post_send fail %d:%llu\n", __func__, __LINE__, ret,
               (unsigned long long int)wr.wr_id);
#ifdef MITSUME_TRAFFIC_IBLAYER_STAT
  long traffic = 0;
  for (int i = 0; i < input_sge_length; i++)
    traffic += input_sge[i].length;
  MITSUME_STAT_FORCE_ADD(MITSUME_STAT_IB_WRITE, traffic);
  MITSUME_STAT_TRAFFIC_ADD(remote_mr->machine_id, traffic);
#endif
  return 0;
}

int userspace_one_read_sge(struct ib_inf *ib_ctx, uint64_t wr_id,
                           struct ib_mr_attr *remote_mr,
                           unsigned long long offset, struct ibv_sge *input_sge,
                           int input_sge_length) {
  struct ibv_send_wr wr, *bad_send_wr;
  int ret;
  wr.opcode = IBV_WR_RDMA_READ;
  wr.num_sge = input_sge_length;
  wr.next = NULL;
  wr.sg_list = input_sge;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr_id = wr_id;
  wr.wr.rdma.remote_addr = remote_mr->addr + offset;
  wr.wr.rdma.rkey = remote_mr->rkey;
  // for(int i=0;i<input_sge_length;i++)
  //    MITSUME_PRINT("%d:%llx:%llx\n", i, (unsigned long long int)
  //    input_sge[i].addr, (unsigned long long int) input_sge[i].lkey);
  // ret = ibv_post_send(ib_ctx->conn_qp[remote_mr->machine_id], &wr,
  // &bad_send_wr);
  int qp_idx = wr_id_to_qp_index(wr_id, remote_mr->machine_id);
  ret = ibv_post_send(ib_ctx->conn_qp[qp_idx], &wr, &bad_send_wr);
  if (ret)
    die_printf("%s-%d: ibv_post_send fail %d:%llu\n", __func__, __LINE__, ret,
               (unsigned long long int)wr.wr_id);
#ifdef MITSUME_TRAFFIC_IBLAYER_STAT
  long traffic = 0;
  for (int i = 0; i < input_sge_length; i++)
    traffic += input_sge[i].length;
  MITSUME_STAT_FORCE_ADD(MITSUME_STAT_IB_READ, traffic);
  MITSUME_STAT_TRAFFIC_ADD(remote_mr->machine_id, traffic);
#endif
  return 0;
}

int userspace_one_poll(struct ib_inf *ib_ctx, uint64_t wr_id,
                       struct ib_mr_attr *remote_mr) {
  struct ibv_wc wc[RSEC_CQ_DEPTH];
  int comps = 0;
  int num_comps = 1;
  int count = 0;
  while (comps < num_comps) {
    if (userspace_check_done_wr_table(wr_id))
      break;
    // int new_comps = ibv_poll_cq(ib_ctx->conn_cq[remote_mr->machine_id],
    // num_comps - comps, &wc[comps]);
    int qp_idx = wr_id_to_qp_index(wr_id, remote_mr->machine_id);
    int new_comps =
        ibv_poll_cq(ib_ctx->conn_cq[qp_idx], num_comps - comps, &wc[comps]);
    count++;
    if (new_comps != 0) {
      // MITSUME_PRINT("poll %llu\n", (unsigned long long int) wc[comps].wr_id);
      // Ideally, we should check from comps -> new_comps - 1
      if (wc[comps].status != 0) {
        fprintf(stderr, "Bad wc status %d %llu\n", wc[comps].status,
                (unsigned long long int)wc[comps].wr_id);
        exit(0);
        return 1;
        // exit(0);
      }
      // comps += new_comps;
      // MITSUME_PRINT("check %llu\n", (unsigned long long int)wc[comps].wr_id);
      userspace_done_wr_table_value(wc[comps].wr_id);
    }
    if (count > 10000 && count % 100000 == 0) {
      // printf("%d %llu\n", count, (unsigned long long int)wr_id);
      MITSUME_PRINT_ERROR("polling too many times %d %llu\n", count,
                          (unsigned long long int)wr_id);
      // MITSUME_IDLE_HERE;
    }
    if (userspace_check_done_wr_table(wr_id))
      break;
  }
  // uint64_t poll_id;
  // poll_id = wc[0].wr_id;
  // MITSUME_PRINT("poll-%lld\n", (long long int)poll_id);
  return MITSUME_SUCCESS;
}

int userspace_one_poll_wr(struct ibv_cq *cq, int tar_mem,
                          struct ibv_wc *input_wc, int sleep_flag)
// this one should only be used to poll recv_cq
{
  hrd_poll_cq(cq, tar_mem, input_wc, sleep_flag);
  return tar_mem;
}

int userspace_refill_used_postrecv(struct ib_inf *ib_ctx,
                                   ptr_attr **per_qp_mr_attr_list,
                                   struct ibv_wc *input_wc, int length,
                                   uint32_t data_length) {
  int per_wc;
  struct ib_mr_attr *input_mr_list;
  unsigned long long target_qp, target_mr;
  ib_post_recv_inf *input_inf;

  input_inf = new ib_post_recv_inf[length];
  input_mr_list = new ib_mr_attr[length];
  for (per_wc = 0; per_wc < length; per_wc++) {
    target_qp = RSEC_ID_TO_QP(input_wc[per_wc].wr_id);
    target_mr = RSEC_ID_TO_RECV_MR(input_wc[per_wc].wr_id);

    input_mr_list[per_wc].addr = per_qp_mr_attr_list[target_qp][target_mr].addr;
    input_mr_list[per_wc].rkey = per_qp_mr_attr_list[target_qp][target_mr].rkey;

    input_inf[per_wc].mr_index = target_mr;
    input_inf[per_wc].qp_index = target_qp;
    input_inf[per_wc].length = data_length;
  }
  ib_post_recv_connect_qp(ib_ctx, input_inf, input_mr_list, length);

  delete (input_mr_list);
  delete (input_inf);

  return length;
}

int mitsume_send_full_message(struct ib_inf *ib_ctx,
                              struct thread_local_inf *local_inf,
                              struct ibv_mr *send_mr, struct ibv_mr *recv_mr,
                              struct mitsume_msg_header *header_ptr,
                              int source_machine, int target_machine,
                              uint32_t send_size) {
  // verify send message
  uint64_t wr_id = mitsume_local_thread_get_wr_id(local_inf);

  volatile struct mitsume_msg *check_msg_ptr;
  check_msg_ptr = (struct mitsume_msg *)recv_mr->addr;
  assert(check_msg_ptr->end_crc == MITSUME_WAIT_CRC);

  ptr_attr tmp_ptr_attr;

  header_ptr->reply_attr.addr = (uint64_t)recv_mr->addr;
  header_ptr->reply_attr.rkey = recv_mr->rkey;
  header_ptr->reply_attr.machine_id = source_machine;
  // header_ptr->thread_id = recv_thread_id;

  tmp_ptr_attr.machine_id = target_machine;
  // MITSUME_TOOL_PRINT_MSG_HEADER((struct mitsume_msg *)send_mr->addr);

  // send out message
  // userspace_one_send(ib_ctx->conn_qp[target_machine], send_mr, send_size,
  // wr_id);
  int qp_idx = wr_id_to_qp_index(wr_id, target_machine);
  userspace_one_send(ib_ctx->conn_qp[qp_idx], send_mr, send_size, wr_id);
  userspace_one_poll(ib_ctx, wr_id, &tmp_ptr_attr);

  mitsume_local_thread_put_wr_id(local_inf, wr_id);

  while (check_msg_ptr->end_crc != MITSUME_REPLY_CRC)
    ; // wait for reply

  return MITSUME_SUCCESS;
}

int mitsume_send_full_message_async(
    struct ib_inf *ib_ctx, struct thread_local_inf *local_inf,
    struct ibv_mr *send_mr, struct ibv_mr *recv_mr,
    struct mitsume_msg_header *header_ptr, int source_machine,
    int target_machine, uint32_t send_size, int coro_id, coro_yield_t &yield) {
  // verify send message
  uint64_t wr_id = mitsume_local_thread_get_wr_id(local_inf);

  volatile struct mitsume_msg *check_msg_ptr;
  check_msg_ptr = (struct mitsume_msg *)recv_mr->addr;
  assert(check_msg_ptr->end_crc == MITSUME_WAIT_CRC);

  ptr_attr tmp_ptr_attr;

  header_ptr->reply_attr.addr = (uint64_t)recv_mr->addr;
  header_ptr->reply_attr.rkey = recv_mr->rkey;
  header_ptr->reply_attr.machine_id = source_machine;
  // header_ptr->thread_id = recv_thread_id;

  tmp_ptr_attr.machine_id = target_machine;
  // MITSUME_TOOL_PRINT_MSG_HEADER((struct mitsume_msg *)send_mr->addr);

  // send out message
  // userspace_one_send(ib_ctx->conn_qp[target_machine], send_mr, send_size,
  // wr_id);
  int qp_idx = wr_id_to_qp_index(wr_id, target_machine);
  userspace_one_send(ib_ctx->conn_qp[qp_idx], send_mr, send_size, wr_id);

  if (coro_id)
    yield_to_another_coro(local_inf, coro_id, yield);

  userspace_one_poll(ib_ctx, wr_id, &tmp_ptr_attr);

  mitsume_local_thread_put_wr_id(local_inf, wr_id);

  if (coro_id)
    yield_to_another_coro(local_inf, coro_id, yield);
  while (check_msg_ptr->end_crc != MITSUME_REPLY_CRC)
    ; // wait for reply

  return MITSUME_SUCCESS;
}

int mitsume_reply_full_message(struct ib_inf *ib_ctx, uint64_t wr_id,
                               struct mitsume_msg_header *recv_header_ptr,
                               struct ibv_mr *local_mr, int reply_size) {
  struct ibv_sge test_sge;
  struct ibv_send_wr wr, *bad_send_wr;
  int ret;

  // verify reply message
  struct mitsume_msg *check_msg_ptr;
  check_msg_ptr = (struct mitsume_msg *)local_mr->addr;
  assert(check_msg_ptr->end_crc == MITSUME_REPLY_CRC);

  test_sge.length = reply_size;
  test_sge.addr = (uintptr_t)local_mr->addr;
  test_sge.lkey = local_mr->lkey;
  wr.opcode = IBV_WR_RDMA_WRITE;
  // wr.imm_data = recv_header_ptr->thread_id;
  wr.num_sge = 1;
  wr.next = NULL;
  wr.sg_list = &test_sge;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr_id = wr_id;
  wr.wr.rdma.remote_addr = recv_header_ptr->reply_attr.addr;
  wr.wr.rdma.rkey = recv_header_ptr->reply_attr.rkey;
  // MITSUME_PRINT("send back to %d %llx %llx\n",
  // recv_header_ptr->reply_attr.machine_id, (unsigned long long
  // int)wr.wr.rdma.remote_addr, (unsigned long long int)wr.wr.rdma.rkey); ret =
  // ibv_post_send(ib_ctx->conn_qp[recv_header_ptr->reply_attr.machine_id], &wr,
  // &bad_send_wr);
  int qp_idx = wr_id_to_qp_index(wr_id, recv_header_ptr->reply_attr.machine_id);
  ret = ibv_post_send(ib_ctx->conn_qp[qp_idx], &wr, &bad_send_wr);
  CPE(ret, "ibv_post_send-write error", ret);
#ifdef MITSUME_TRAFFIC_IBLAYER_STAT
  MITSUME_STAT_FORCE_ADD(MITSUME_STAT_IB_RPC, reply_size);
  MITSUME_STAT_TRAFFIC_ADD(recv_header_ptr->reply_attr.machine_id, reply_size);
#endif

  return 0;
}
