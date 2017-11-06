// Copyright (c) 2017 Felix Chern (Add extern "C" to make it compatible with C
// and C++)
// Copyright (c) 2015 Jason Schulz (Rewritten in C)
// Copyright (c) 2011 Google, Inc. Geoff Pike and Jyrki Alakuijala
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.
//
// CityHash, by Geoff Pike and Jyrki Alakuijala
//
// http://code.google.com/p/cityhash/
//
#ifndef LARGE_INT_H
#define LARGE_INT_H 1

#include <stdint.h>

struct uint128_t
{
  uint64_t a;
  uint64_t b;
};

typedef struct uint128_t uint128_t;

static inline uint64_t
uint128_t_low64(const uint128_t x)
{
  return x.a;
}
static inline uint64_t
uint128_t_high64(const uint128_t x)
{
  return x.b;
}

static inline uint128_t
make_uint128_t(uint64_t lo, uint64_t hi)
{
  uint128_t x = { lo, hi };
  return x;
}

// conditionally include declarations for versions of city that require SSE4.2
// instructions to be available
#if defined(__SSE4_2__) && defined(__x86_64)

struct uint256_t
{
  uint64_t a;
  uint64_t b;
  uint64_t c;
  uint64_t d;
};

typedef struct uint256_t uint256_t;

#endif

#endif
