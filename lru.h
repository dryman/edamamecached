#ifndef EDAMAME_LRU_H_
#define EDAMAME_LRU_H_ 1

#include <stdint.h>

typedef struct lru_t lru_t;
typedef struct lru_val_t lru_val_t;
typedef struct scavenger_t scavenger_t;

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
bool lru_get(lru_t* lru, void* key, size_t keylen, lru_val_t* lru_val);
bool lru_upsert(lru_t* lru, void* key, size_t keylen, lru_val_t* lru_val);

#endif
