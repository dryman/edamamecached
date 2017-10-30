#ifndef EDAMAME_LRU_H_
#define EDAMAME_LRU_H_ 1

#include <stdint.h>
#include <stdatomic.h>
#include "cmd_parser.h" 

typedef struct lru_t lru_t;
typedef struct lru_val_t lru_val_t;
typedef struct scavenger_t scavenger_t;

#define PROBE_STATS_SIZE 512
// Need to record the probe stats!

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
  uint8_t* buckets;
  uint8_t* tmp_buckets;
};

struct lru_val_t
{
  enum cmd_errcode errcode;
  bool is_numeric_val;
  size_t vallen;
  void* value;
  uint64_t cas;
  uint16_t flags;
};

lru_t* lru_init(uint64_t num_objects, size_t inline_keylen, size_t inline_vallen);
void lru_cleanup(lru_t* lru);
bool lru_get(lru_t* lru, cmd_handler* cmd, lru_val_t* lru_val);
bool lru_upsert(lru_t* lru, cmd_handler* cmd, lru_val_t* lru_val);

#endif
