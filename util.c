#include "util.h"
#include "errno.h"
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>

__thread int ed_errno = 0;

uint64_t
strn2uint64(const char *str, size_t n, char **stop)
{
  size_t i = 0;
  uint64_t num = 0, digit;
  for (i = 0; i < n && ed_isspace(*str); i++, str++)
    ;
  if (i == n)
    {
      *stop = (char *)str;
      return num;
    }
  for (; i < n; i++, str++)
    {
      digit = *str - '0';
      if (digit > 9)
        {
          *stop = (char *)str;
          return num;
        }
      if (__builtin_expect(__builtin_mul_overflow(num, 10, &num), 0))
        {
          *stop = (char *)str;
          return UINT64_MAX;
        }
      if (__builtin_expect(__builtin_add_overflow(num, digit, &num), 0))
        {
          *stop = (char *)str;
          return UINT64_MAX;
        }
    }
  *stop = (char *)str;
  return num;
}

uint32_t
strn2uint32(const char *str, size_t n, char **stop)
{
  size_t i = 0;
  uint32_t num = 0, digit;
  for (i = 0; i < n && ed_isspace(*str); i++, str++)
    ;
  if (i == n)
    {
      *stop = (char *)str;
      return num;
    }
  for (; i < n; i++, str++)
    {
      digit = *str - '0';
      if (digit > 9)
        {
          *stop = (char *)str;
          return num;
        }
      if (__builtin_expect(__builtin_mul_overflow(num, 10, &num), 0))
        {
          *stop = (char *)str;
          return UINT32_MAX;
        }
      if (__builtin_expect(__builtin_add_overflow(num, digit, &num), 0))
        {
          *stop = (char *)str;
          return UINT32_MAX;
        }
    }
  *stop = (char *)str;
  return num;
}

uint16_t
strn2uint16(const char *str, size_t n, char **stop)
{
  size_t i = 0;
  uint16_t num = 0, digit;
  for (i = 0; i < n && ed_isspace(*str); i++, str++)
    ;
  if (i == n)
    {
      *stop = (char *)str;
      return num;
    }
  for (; i < n; i++, str++)
    {
      digit = *str - '0';
      if (digit > 9)
        {
          *stop = (char *)str;
          return num;
        }
      if (__builtin_expect(__builtin_mul_overflow(num, 10, &num), 0))
        {
          *stop = (char *)str;
          return UINT16_MAX;
        }
      if (__builtin_expect(__builtin_add_overflow(num, digit, &num), 0))
        {
          *stop = (char *)str;
          return UINT16_MAX;
        }
    }
  *stop = (char *)str;
  return num;
}
