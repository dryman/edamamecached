#ifndef EDAMAME_UTIL_H_
#define EDAMAME_UTIL_H_ 1

#include <arpa/inet.h>
#include <stdbool.h>
#include <stdint.h>

#ifdef __SSE4_1__
#include <pmmintrin.h>
#include <smmintrin.h>
#endif
#ifdef __SSE2__
#include <emmintrin.h>
#endif

#ifndef htonll
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define htonll(bytes) __builtin_bswap64 (bytes)
#else
#define htonll(bytes) (bytes)
#endif
#endif // htonll

#ifndef ntohll
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define ntohll(bytes) __builtin_bswap64 (bytes)
#else
#define ntohll(bytes) (bytes)
#endif
#endif // ntohll

static inline bool
memeq (const void *ptr1, const void *ptr2, size_t num)
{
  uintptr_t p1, p2;
  p1 = (uintptr_t)ptr1;
  p2 = (uintptr_t)ptr2;

#ifdef __SSE4_1__
  for (; num >= 16; num -= 16, p1 += 16, p2 += 16)
    {
      __m128i d1 = _mm_lddqu_si128 ((void *)p1);
      __m128i d2 = _mm_lddqu_si128 ((void *)p2);
      __m128i cmp = _mm_cmpeq_epi64 (d1, d2);
      int result = _mm_movemask_pd ((__m128d)cmp);
      if (result != 0x3)
        return false;
    }
#elif defined(__SSE2__)
  for (; num >= 16; num -= 16, p1 += 16, p2 += 16)
    {
      __m128i d1 = _mm_loadu_si128 ((void *)p1);
      __m128i d2 = _mm_loadu_si128 ((void *)p2);
      __m128i cmp = _mm_cmpeq_epi32 (d1, d2);
      int result = _mm_movemask_epi8 (cmp);
      if (result != 0xFFFF)
        return false;
    }
#else
  for (; num >= 16; num -= 8, p1 += 8, p2 += 8)
    {
      uint64_t *d1 = (uint64_t *)p1;
      uint64_t *d2 = (uint64_t *)p2;
      if (*d1 != *d2)
        return false;
    }
#endif

  uint64_t *d1_64, *d2_64, *e1_64, *e2_64;
  uint32_t *d1_32, *d2_32, *e1_32, *e2_32;
  uint16_t *d1_16, *d2_16;
  uint8_t *d1_8, *d2_8;

  switch (num)
    {
    case 15:
      d1_64 = (uint64_t *)p1;
      d2_64 = (uint64_t *)p2;
      e1_64 = (uint64_t *)(p1 + 7);
      e2_64 = (uint64_t *)(p2 + 7);
      return *d1_64 == *d2_64 && *e1_64 == *e2_64;
    case 14:
      d1_64 = (uint64_t *)p1;
      d2_64 = (uint64_t *)p2;
      e1_64 = (uint64_t *)(p1 + 6);
      e2_64 = (uint64_t *)(p2 + 6);
      return *d1_64 == *d2_64 && *e1_64 == *e2_64;
    case 13:
      d1_64 = (uint64_t *)p1;
      d2_64 = (uint64_t *)p2;
      e1_64 = (uint64_t *)(p1 + 5);
      e2_64 = (uint64_t *)(p2 + 5);
      return *d1_64 == *d2_64 && *e1_64 == *e2_64;
    case 12:
      d1_64 = (uint64_t *)p1;
      d2_64 = (uint64_t *)p2;
      e1_64 = (uint64_t *)(p1 + 4);
      e2_64 = (uint64_t *)(p2 + 4);
      return *d1_64 == *d2_64 && *e1_64 == *e2_64;
    case 11:
      d1_64 = (uint64_t *)p1;
      d2_64 = (uint64_t *)p2;
      e1_64 = (uint64_t *)(p1 + 3);
      e2_64 = (uint64_t *)(p2 + 3);
      return *d1_64 == *d2_64 && *e1_64 == *e2_64;
    case 10:
      d1_64 = (uint64_t *)p1;
      d2_64 = (uint64_t *)p2;
      e1_64 = (uint64_t *)(p1 + 2);
      e2_64 = (uint64_t *)(p2 + 2);
      return *d1_64 == *d2_64 && *e1_64 == *e2_64;
    case 9:
      d1_64 = (uint64_t *)p1;
      d2_64 = (uint64_t *)p2;
      e1_64 = (uint64_t *)(p1 + 1);
      e2_64 = (uint64_t *)(p2 + 1);
      return *d1_64 == *d2_64 && *e1_64 == *e2_64;
    case 8:
      d1_64 = (uint64_t *)p1;
      d2_64 = (uint64_t *)p2;
      return *d1_64 == *d2_64;
    case 7:
      d1_32 = (uint32_t *)p1;
      d2_32 = (uint32_t *)p2;
      e1_32 = (uint32_t *)(p1 + 3);
      e2_32 = (uint32_t *)(p2 + 3);
      return *d1_32 == *d2_32 && *e1_32 == *e2_32;
    case 6:
      d1_32 = (uint32_t *)p1;
      d2_32 = (uint32_t *)p2;
      e1_32 = (uint32_t *)(p1 + 2);
      e2_32 = (uint32_t *)(p2 + 2);
      return *d1_32 == *d2_32 && *e1_32 == *e2_32;
    case 5:
      d1_32 = (uint32_t *)p1;
      d2_32 = (uint32_t *)p2;
      e1_32 = (uint32_t *)(p1 + 1);
      e2_32 = (uint32_t *)(p2 + 1);
      return *d1_32 == *d2_32 && *e1_32 == *e2_32;
    case 4:
      d1_32 = (uint32_t *)p1;
      d2_32 = (uint32_t *)p2;
      return *d1_32 == *d2_32;
    case 3:
      d1_16 = (uint16_t *)p1;
      d2_16 = (uint16_t *)p2;
      d1_8 = (uint8_t *)(p1 + 2);
      d2_8 = (uint8_t *)(p2 + 2);
      return *d1_16 == *d2_16 && *d1_8 == *d2_8;
    case 2:
      d1_16 = (uint16_t *)p1;
      d2_16 = (uint16_t *)p2;
      return *d1_16 == *d2_16;
    case 1:
      d1_8 = (uint8_t *)p1;
      d2_8 = (uint8_t *)p2;
      return *d1_8 == *d2_8;
    default:
      return true;
    }
}

extern __thread int ed_errno;

static inline bool
ed_isspace (char chr)
{
  return chr == ' ' || chr == '\t' || chr == '\n' || chr == '\v' || chr == '\f'
         || chr == '\r';
}

#define round_up_div(X, Y) ((X) + (Y)-1) / (Y)

uint64_t strn2uint64 (const char *str, size_t n, char **stop);
uint32_t strn2uint32 (const char *str, size_t n, char **stop);
uint16_t strn2uint16 (const char *str, size_t n, char **stop);

#endif
