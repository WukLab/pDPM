#include "mitsume.h"

void die_printf(const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  vfprintf(stderr, fmt, args);
  va_end(args);
  exit(1);
}

void dbg_printf(const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  vfprintf(stderr, fmt, args);
  va_end(args);
}

void MITSUME_PRINT_TMP(const char *fmt, ...) {
  va_list args;
  char output_string[1024] = {0};
  va_start(args, fmt);
  sprintf(output_string, "[dbg] %s", fmt);
  vfprintf(stderr, output_string, args);
  va_end(args);
}
