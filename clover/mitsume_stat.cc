#include "mitsume_stat.h"

atomic<long> mitsume_stat_counter[MITSUME_STAT_LENGTH];
atomic<long> mitsume_stat_array_counter[MITSUME_STAT_ARRAY_LENGTH];
int mitsume_role;
atomic<long> mitsume_stat_sum_array_number[MITSUME_STAT_SUM_ARRAY_LENGTH];
atomic<long> mitsume_stat_sum_array_counter[MITSUME_STAT_SUM_ARRAY_LENGTH];

/*
 * XXX
 * Hack by Yizhou.
 * 
 * Since I changed macros to integers, this no longer work.
 * An upperbind should work.
 */
atomic<long> mitsume_stat_traffic_array_counter[128];
//atomic<long> mitsume_stat_traffic_array_counter[(
//    MITSUME_CON_NUM + MITSUME_CLT_NUM + MITSUME_MEM_NUM)];

/**
 * mitsume_stat_show: called by `cat /proc/mitsume` - printout all counter
 * return: 0
 */
int mitsume_stat_show(void) {

  int i;
  int print_flag = 0;
  for (i = 0; i <= MITSUME_STAT_LENGTH; i++) {
    print_flag = 1;
    if (i == 0 || i == MITSUME_STAT_LENGTH || mitsume_stat_text[i][0] == '=')
      printf("%s\n", mitsume_stat_text[i]);
    else {
      if (memcmp(mitsume_stat_text[i], "[con]", 5) == 0) {
        if (mitsume_role != MITSUME_IS_CONTROLLER)
          print_flag = 0;
      }
      if (memcmp(mitsume_stat_text[i], "[clt]", 5) == 0) {
        if (mitsume_role != MITSUME_IS_CLIENT)
          print_flag = 0;
      }
      if (print_flag)
        printf("%s: %lu\n", mitsume_stat_text[i],
               (unsigned long int)mitsume_stat_counter[i].load());
    }
  }

  /*for(i=0;i<MITSUME_STAT_ARRAY_LENGTH;i++)
  {
      printf("array-%d: %lu\n", i, (long unsigned
  int)mitsume_stat_array_counter[i].load());
  }*/
  for (i = 0; i < (MITSUME_CON_NUM + MITSUME_CLT_NUM + MITSUME_MEM_NUM); i++) {
    printf("bucket-%d: %lu\n", i,
           (long unsigned int)mitsume_traffic_array_read(i));
  }

  printf("===========\n");
  for (i = 0; i < MITSUME_STAT_SUM_ARRAY_LENGTH; i++) {
    if (mitsume_stat_sum_array_counter[i].load()) {
      printf("%d summation: %lu\n", i,
             (long unsigned int)mitsume_stat_sum_array_number[i].load());
      printf("%d count: %lu\n", i,
             (long unsigned int)mitsume_stat_sum_array_counter[i].load());
      printf("%d average: %lu\n", i,
             (long unsigned int)mitsume_stat_sum_array_number[i].load() /
                 mitsume_stat_sum_array_counter[i].load());
    }
  }
  return 0;
}

/**
 * mitsume_stat_read: take a type and read the value
 * return: return respected value
 */
inline int mitsume_stat_read(int target) {
  // return atomic_read(&mitsume_stat_counter[target]);
  return mitsume_stat_counter[target].load();
}

/**
 * mitsume_stat_add: take a type and a input to add on the respected value
 * return: NULL
 */
void mitsume_stat_add(uint32_t type, int num) {
  mitsume_stat_counter[type] += num;
}

/**
 * mitsume_stat_array_add: take an index and a input to add on the respected
 * array value return: NULL
 */
void mitsume_stat_array_add(uint32_t index, int num) {
  mitsume_stat_array_counter[index] += num;
}

/**
 * mitsume_stat_sub: take a type and a input to sub on the respected value
 * return: NULL
 */
void mitsume_stat_sub(uint32_t type, int num) {
  mitsume_stat_counter[type] -= num;
}

/**
 * mitsume_stat_array_sub: take an index and a input to add on the respected
 * array value return: NULL
 */
void mitsume_stat_array_sub(uint32_t index, int num) {
  mitsume_stat_array_counter[index] -= num;
}

inline int mitsume_stat_array_read(int target) {
  return mitsume_stat_array_counter[target].load();
}

int mitsume_traffic_array_add(int machine, long num) {
  return mitsume_stat_traffic_array_counter[machine] += num;
}

inline long mitsume_traffic_array_read(int target) {
  return mitsume_stat_traffic_array_counter[target].load();
}

/**
 * mitsume_stat_add: take a type and a input to set on the respected value
 * return: NULL
 */
void mitsume_stat_set(uint32_t type, int num) {
  mitsume_stat_counter[type] = num;
}

int mitsume_stat_init(int input_mitsume_role) {
  int i;

  mitsume_role = input_mitsume_role;
  for (i = 0; i < MITSUME_STAT_LENGTH; i++) {
    mitsume_stat_counter[i] = 0;
  }

  for (i = 0; i < MITSUME_STAT_ARRAY_LENGTH; i++) {
    mitsume_stat_array_counter[i] = 0;
  }

  for (i = 0; i < MITSUME_STAT_SUM_ARRAY_LENGTH; i++) {
    mitsume_stat_sum_array_number[i] = 0;
    mitsume_stat_sum_array_counter[i] = 0;
  }
  for (i = 0; i < (MITSUME_CON_NUM + MITSUME_CLT_NUM + MITSUME_MEM_NUM); i++) {
    mitsume_stat_traffic_array_counter[i] = 0;
  }

  return 0;
}
