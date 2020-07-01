#ifndef RSEC_UTIL_HEADER
#define RSEC_UTIL_HEADER
#include <cstdio>
void RSEC_PRINT_TMP(const char *fmt, ...);
#define RSEC_WHOLE_DEBUG
#ifdef RSEC_WHOLE_DEBUG
//#define RSEC_PRINT(inputstring, args...)
//        printf("[R_SECURITY] %s:%d :\t" inputstring, __func__, __LINE__,
//        ##args) printf("[R_SECURITY] %s:%d :\t" inputstring, __func__,
//        __LINE__, ##args)
// my_printf(inputstring, args...)
#define MITSUME_PRINT(inputstring, args...)                                    \
  printf("[DBG-%s] %s:%d :\t" inputstring, __FILE__, __func__, __LINE__, ##args)
#define RSEC_FPRINT(fp, string, args...)
//        fprintf(fp, "[R_SECURITY] %s:%d :\t" string, __func__, __LINE__,
//        ##args)
#define MITSUME_PRINT_BRIEF(string, args...) printf("[BRIEF] " string, ##args)
#define MITSUME_PRINT_ERROR(string, args...)                                   \
  printf("[R_SECURITY-ERROR] %s:%d : " string, __func__, __LINE__, ##args)
#define MITSUME_INFO(inputstring, args...)                                     \
  printf("[INFO] %s:%d : " inputstring, __func__, __LINE__, ##args)
#else
#define MITSUME_PRINT(string, args...)
#define RSEC_PRINT_BRIEF(string, args...)
#define RSEC_ERROR(string, args...) printf("[R_SECURITY-ERROR] " string, ##args)
#define MITSUME_INFO(inputstring, args...) printf("[INFO] " inputstring, ##args)
#endif

#define CPE(val, msg, err_code)                                                \
  if (val) {                                                                   \
    fprintf(stderr, msg);                                                      \
    fprintf(stderr, " Error %d \n", err_code);                                 \
    exit(err_code);                                                            \
  }
#define MITSUME_IDLE_HERE                                                      \
  while (true)                                                                 \
    ;

#define RSEC_MIN(a, b) (((a) < (b)) ? (a) : (b))
#define MITSUME_MAX(a, b) (((a) > (b)) ? (a) : (b))
#define MITSUME_ROUND_UP(N, S) ((((N) + (S)-1) / (S)) * (S))
#define UNUSED(expr)                                                           \
  do {                                                                         \
    (void)(expr);                                                              \
  } while (0)

const static char MITSUME_MEMCACHED_SHORTCUT_STRING[] = "shortcut-%d";
const static char MITSUME_MEMCACHED_MEMORY_ALLOCATION_STRING[] =
    "memory-alloc-%d";
const static char MITSUME_MEMCACHED_LH_STRING[] = "lh-%llu";

const static char MITSUME_MEMCACHED_BARRIER_STRING[] = "barrier-%u-client-%u";

#endif
