#ifndef MITSUME_STAT_HEADER
#define MITSUME_STAT_HEADER

#include "mitsume.h"
#include "mitsume_parameter.h"
#include "mitsume_struct.h"
#include <atomic>
using namespace std;

//#define MITSUME_TRAFFIC_IBLAYER_STAT
//#define MITSUME_STAT_ENABLE
#ifdef MITSUME_STAT_ENABLE
#define MITSUME_STAT_ADD(type, num) mitsume_stat_add(type, num)
#define MITSUME_STAT_SUB(type, num) mitsume_stat_sub(type, num)
#define MITSUME_STAT_SET(type, num) mitsume_stat_set(type, num)
#define MITSUME_STAT_ARRAY_ADD(index, num) mitsume_stat_array_add(index, num)
#define MITSUME_STAT_ARRAY_SUB(index, num) mitsume_stat_array_sub(index, num)
#define MITSUME_STAT_SUM(index, num) mitsume_stat_sum(index, num)
#else
#define MITSUME_STAT_ADD(a, b)
#define MITSUME_STAT_SUB(a, b)
#define MITSUME_STAT_SET(a, b)
#define MITSUME_STAT_ARRAY_ADD(index, num)
#define MITSUME_STAT_ARRAY_SUB(index, num)
#define MITSUME_STAT_SUM(index, num)
#endif
#define MITSUME_STAT_FORCE_ADD(type, num) mitsume_stat_add(type, num)
#define MITSUME_STAT_FORCE_SUB(type, num) mitsume_stat_sub(type, num)

#define MITSUME_STAT_TRAFFIC_ADD(machine, num)                                 \
  mitsume_traffic_array_add(machine, num)

#define MITSUME_STAT_GET_TIME_START(a) mitsume_stat_get_time_start(a)
#define MITSUME_STAT_GET_TIME_END(a)                                           \
  mitsume_stat_get_time_end(a, __LINE__, __FILE__, thread_metadata->thread_id)
#define MITSUME_STAT_GET_TIME_END_NULLID(a)                                    \
  mitsume_stat_get_time_end(a, __LINE__, __FILE__, 0)

#define MITSUME_STAT_ARRAY_LENGTH 8
#define MITSUME_STAT_SUM_ARRAY_LENGTH 2

void mitsume_stat_sum(uint32_t index, int num);
void mitsume_stat_add(uint32_t type, int num);
void mitsume_stat_sub(uint32_t type, int num);
void mitsume_stat_set(uint32_t type, int num);
void mitsume_stat_array_add(uint32_t index, int num);
void mitsume_stat_array_sub(uint32_t index, int num);
inline int mitsume_stat_read(int target);
int mitsume_stat_show(void);
int mitsume_traffic_array_add(int bucket, long num);
inline long mitsume_traffic_array_read(int target);

int mitsume_stat_init(int input_mitsume_role);

enum mitsume_stat_type {
  MITSUME_STAT_BEGIN,
  MITSUME_CURRENT_EPOCH,
  ____MITSUME_SEPARATE____1,
  MITSUME_STAT_CLT_OPEN,
  MITSUME_STAT_CLT_OPEN_FAIL,
  MITSUME_STAT_CLT_READ,
  MITSUME_STAT_CLT_READ_FAIL,
  MITSUME_STAT_CLT_WRITE,
  MITSUME_STAT_CLT_WRITE_FAIL,
  MITSUME_STAT_CLT_CHASE,
  //        MITSUME_STAT_CLT_XACT_BEGIN,
  //        MITSUME_STAT_CLT_XACT_BEGIN_WAIT,
  //        MITSUME_STAT_CLT_XACT_BEGIN_FAIL,
  //        MITSUME_STAT_CLT_XACT_COMMIT,
  //        MITSUME_STAT_CLT_XACT_COMMIT_FAIL,
  //        MITSUME_STAT_CLT_XACT_COMMIT_UNMATCH,
  MITSUME_STAT_CLT_READ_USERSPACE_CPY,
  MITSUME_STAT_CLT_WRITE_USERSPACE_CPY,
  MITSUME_STAT_CLT_CHASING,
  MITSUME_STAT_CLT_SHORTCUT_CHASING,
  MITSUME_STAT_CLT_CHASING_BACKUP,
  MITSUME_STAT_CLT_QUERY_ENTRY,
  MITSUME_STAT_CLT_ASK_ENTRY,
  MITSUME_STAT_CLT_GET_ENTRY,
  MITSUME_STAT_CLT_RETRY_GET_ENTRY,
  MITSUME_STAT_CLT_SEND_GC,
  MITSUME_STAT_CLT_REMOVE_SHORTCUT_UPDATE,
  MITSUME_STAT_CLT_OUTDATED_HASH,
  MITSUME_STAT_CLT_CRC_MISMATCH,
  ____MITSUME_SEPARATE____2,
  MITSUME_STAT_CON_ASKED_ENTRY,
  MITSUME_STAT_CON_DIST_ENTRY,
  MITSUME_STAT_CON_QUERY_ENTRY,
  MITSUME_STAT_CON_OPEN_ENTRY,
  MITSUME_STAT_CON_BACKUP_OPEN_ENTRY,
  MITSUME_STAT_CON_RECEIVED_GC,
  MITSUME_STAT_CON_PROCESSED_GC,
  MITSUME_STAT_CON_RECYCLED_GC,
  MITSUME_STAT_CON_EPOCHED_UNPROCESSED_GC,
  MITSUME_STAT_CON_EPOCHED_GC,
  MITSUME_STAT_CON_SEND_BACKUP_UPDATE_REQUEST,
  MITSUME_STAT_CON_RECEIVED_BACKUP_UPDATE_REQUEST,
  MITSUME_STAT_CON_MAX_GCVERSION,
  MITSUME_STAT_CON_MANAGEMENT_REQUEST,
  MITSUME_STAT_CON_STAT_UPDATE,
  ____MITSUME_TEST____,
  MITSUME_STAT_IB_WRITE,
  MITSUME_STAT_IB_READ,
  MITSUME_STAT_IB_RPC,
  MITSUME_STAT_TEST1,
  MITSUME_STAT_TEST2,
  MITSUME_STAT_TEST3,
  MITSUME_STAT_TEST4,
  MITSUME_STAT_TEST5,
  MITSUME_STAT_TEST6,
  MITSUME_STAT_TEST7,
  MITSUME_STAT_TEST8,
  MITSUME_STAT_TEST_KMEMCACHE,
  MITSUME_STAT_LENGTH
};

static const char *const mitsume_stat_text[] = {
    "------Mitsume Stats------", "[gen] current epoch",
    "=================================", "[clt] # of open request",
    "[clt] # of open-fail", "[clt] # of read request", "[clt] # of read-fail",
    "[clt] # of write request", "[clt] # of write-fail", "[clt] # of chase",
    //        "[clt] # of xact-begin request",
    //        "[clt] # of xact-begin wait",
    //        "[clt] # of xact-begin fail",
    //        "[clt] # of xact-commit request",
    //        "[clt] # of xact-commit fail",
    //        "[clt] # of xact-commit doesn't match",
    "[clt] # of read-address userspace memcpy",
    "[clt] # of write-address userspace memcpy", "[clt] # of pointer chasing",
    "[clt] # of shortcut chasing", "[clt] # of chasing backup",
    "[clt] # of queried entries", "[clt] # of asked entries",
    "[clt] # of received entries", "[clt] # of retry asked entries times",
    "[clt] # of sent GC requests", "[clt] # of removed shortcut updates",
    "[clt] # of outdated query", "[clt] # of mismatch crc check",
    "=================================", "[con] # of asked entries",
    "[con] # of distributed entries", "[con] # of queried entries",
    "[con] # of open entries", "[con] # of backup-open entries",
    "[con] # of received GC requests", "[con] # of processed GC entries",
    "[con] # of recycled GC entries",
    "[con] # of epoched-unprocessed GC entries",
    "[con] # of epoched GC entries", "[con] # of send backup update request",
    "[con] # of received backup update request", "[con] # of max GC version",
    "[con] # of management request", "[con] # of stat update",
    "=================================", "[IB] write", "[IB] read", "[IB] RPC",
    "[tes] # of test1", "[tes] # of test2", "[tes] # of test3",
    "[tes] # of test4", "[tes] # of test5", "[tes] # of test6",
    "[tes] # of test7", "[tes] # of test8", "[tes] # of test-kmemcache",
    "------End------"};

#endif
