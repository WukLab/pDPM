#ifndef RSEC_IBSETUP_HEADER
#define RSEC_IBSETUP_HEADER
#include "memcached.h"
#include "mitsume_stat.h"
#include "mitsume_struct.h"
#include <infiniband/verbs.h>

#include <unistd.h>

#include <unordered_map>
using namespace std;

#define RSEC_NETWORK_IB 1
#define RSEC_NETWORK_ROCE 2
//#define RSEC_NETWORK_MODE RSEC_NETWORK_IB
#define RSEC_NETWORK_MODE RSEC_NETWORK_ROCE

#define RSEC_SGID_INDEX 3

#define RSEC_ID_COMBINATION(qp_index, i) ((qp_index << 16) + i)
#define RSEC_ID_TO_QP(wr_id) (wr_id >> 16)
#define RSEC_ID_TO_RECV_MR(wr_id) (wr_id & 0xffff)

#define MITSUME_WR_DONE 0xC0DED00D
#define MITSUME_WR_WAIT 0xCEFAEDFE
#define MITSUME_WR_NULL 0xC00010FF

// NULL->WAIT->DONE->NULL

struct ib_post_recv_inf {
  uint64_t mr_index;
  int qp_index;
  int length;
};
typedef struct ib_post_recv_inf ib_post_recv_inf;

int test(int);
union ibv_gid ib_get_gid(struct ibv_context *context, int port_index);
struct ibv_device *ib_get_device(struct ib_inf *inf, int port);
void ib_create_udqps(struct ib_inf *inf);
struct ib_inf *ib_setup(int id, int port, int num_rcqp_to_server,
                        int num_rcqp_to_client, int num_udqps, int num_loopback,
                        int total_machines, int device_id, int role_int);
uint16_t ib_get_local_lid(struct ibv_context *ctx, int dev_port_id);
int ib_connect_qp(struct ib_inf *inf, int qp_index, struct ib_qp_attr *qp_attr);
struct ibv_ah *ib_create_ah_for_ud(struct ib_inf *inf, int ah_index,
                                   struct ib_qp_attr *dest);
struct ib_inf *ib_complete_setup(struct configuration_params *input_arg,
                                 int role_int, const char *role_str);
void ib_create_rcqps(struct ib_inf *inf, int role_int);
void ib_create_attackqps(struct ib_inf *inf);
int p15_ib_post_recv_ud_qp(struct ib_inf *inf, int udqp_index,
                           int post_recv_base, int post_recv_num);

int p15_ib_post_send_rc_qp(struct ib_inf *, int rcqp_index, int post_send_base,
                           int post_send_num);
int p15_ib_post_write_rc_qp(struct ib_inf *, int rcqp_index,
                            int post_write_base, int post_write_num,
                            struct ib_qp_attr *);
int ib_post_recv_connect_qp(struct ib_inf *context,
                            ib_post_recv_inf *post_recv_inf_list,
                            struct ib_mr_attr *input_mr_array,
                            int input_mr_array_length);

inline int hrd_poll_cq(struct ibv_cq *cq, int num_comps, struct ibv_wc *wc,
                       int sleep_flag);
int userspace_one_send(struct ibv_qp *qp, struct ibv_mr *local_mr,
                       int request_size);
int userspace_one_poll_wr(struct ibv_cq *cq, int tar_mem,
                          struct ibv_wc *input_wc, int sleep_flag);

////////////////////////////
int userspace_one_read(struct ib_inf *ib_ctx, uint64_t wr_id,
                       struct ibv_mr *local_mr, int request_size,
                       struct ib_mr_attr *remote_mr, unsigned long long offset);
int userspace_one_read_sge(struct ib_inf *ib_ctx, uint64_t wr_id,
                           struct ib_mr_attr *remote_mr,
                           unsigned long long offset, struct ibv_sge *input_sge,
                           int input_sge_length);
int userspace_one_write(struct ib_inf *ib_ctx, uint64_t wr_id,
                        struct ibv_mr *local_mr, int request_size,
                        struct ib_mr_attr *remote_mr,
                        unsigned long long offset);
int userspace_one_write_inline(struct ib_inf *ib_ctx, uint64_t wr_id,
                               struct ibv_mr *local_mr, int request_size,
                               struct ib_mr_attr *remote_mr,
                               unsigned long long offset);
int userspace_one_write_sge(struct ib_inf *ib_ctx, uint64_t wr_id,
                            struct ib_mr_attr *remote_mr,
                            unsigned long long offset,
                            struct ibv_sge *input_sge, int input_sge_length);
int userspace_one_poll(struct ib_inf *ib_ctx, uint64_t wr_id,
                       struct ib_mr_attr *remote_mr);
void *p15_malloc(size_t length);
int userspace_refill_used_postrecv(struct ib_inf *ib_ctx,
                                   ptr_attr **per_qp_mr_attr_list,
                                   struct ibv_wc *input_wc, int length,
                                   uint32_t data_length);
int mitsume_send_full_message(struct ib_inf *ib_ctx,
                              struct thread_local_inf *local_inf,
                              struct ibv_mr *send_mr, struct ibv_mr *recv_mr,
                              struct mitsume_msg_header *header_ptr,
                              int source_machine, int target_machine,
                              uint32_t send_size);
int mitsume_send_full_message_async(
    struct ib_inf *ib_ctx, struct thread_local_inf *local_inf,
    struct ibv_mr *send_mr, struct ibv_mr *recv_mr,
    struct mitsume_msg_header *header_ptr, int source_machine,
    int target_machine, uint32_t send_size, int coro_id, coro_yield_t &yield);
int mitsume_reply_full_message(struct ib_inf *ib_ctx, uint64_t wr_id,
                               struct mitsume_msg_header *recv_header_ptr,
                               struct ibv_mr *local_mr, int reply_size);
int userspace_one_cs(struct ib_inf *ib_ctx, uint64_t wr_id,
                     struct ibv_mr *local_mr, struct ib_mr_attr *remote_mr,
                     unsigned long long guess_value,
                     unsigned long long set_value);
void userspace_init_wr_table_value(uint64_t wr_id);
void userspace_wait_wr_table_value(uint64_t wr_id);
void userspace_null_wr_table_value(uint64_t wr_id);
#endif
