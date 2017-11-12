/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef EDAMAME_LRU_H_
#define EDAMAME_LRU_H_ 1

#include "cmd_parser.h"
#include <inttypes.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>

typedef struct lru_t lru_t;
typedef struct lru_val_t lru_val_t;
typedef struct swiper_t swiper_t;

#define PROBE_STATS_SIZE 512

struct lru_t
{
  uint8_t capacity_clz;
  uint8_t capacity_ms4b;
  size_t inline_keylen;
  size_t inline_vallen;

  atomic_uint longest_probes;
  atomic_ullong objcnt;
  atomic_ullong txid;
  atomic_uint probe_stats[PROBE_STATS_SIZE];

  atomic_ullong inline_acc_keylen;
  atomic_ullong inline_acc_vallen;
  atomic_uint ninline_keycnt;
  atomic_uint ninline_valcnt;
  atomic_ullong ninline_keylen;
  atomic_ullong ninline_vallen;

  atomic_ullong tmp_bucket_bmap;
  uint8_t *buckets;
  uint8_t *tmp_buckets;
};

struct lru_val_t
{
  enum cmd_errcode errcode;
  bool is_numeric_val;
  size_t vallen;
  void *value;
  uint64_t cas;
  uint16_t flags;
};

lru_t *lru_init(uint64_t num_objects, size_t inline_keylen,
                size_t inline_vallen);
void lru_cleanup(lru_t *lru);
uint64_t lru_capacity(lru_t *lru);
bool lru_get(lru_t *lru, cmd_handler *cmd, lru_val_t *lru_val);
bool lru_upsert(lru_t *lru, cmd_handler *cmd, lru_val_t *lru_val);
void lru_delete(lru_t *lru, cmd_handler *cmd);

struct swiper_t
{
  lru_t *lru;
  uint32_t pqueue_size;
  uint32_t pqueue_used;
  // pqueue[x][0] is idx of the bucket
  // pqueue[x][1] is txid
  uint64_t pqueue[0][2];
  // TODO add mutex for flush_all
};

swiper_t *swiper_init(lru_t *lru, uint32_t pq_size);
void pq_add(swiper_t *swiper, uint64_t idx, uint64_t txid);
void pq_pop_add(swiper_t *swiper, uint64_t idx, uint64_t txid);
void pq_sort(swiper_t *swiper);
void lru_swipe(swiper_t *swiper);

#endif
