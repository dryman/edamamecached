#include <urcu.h>
#include "cmd_parser.h"


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
  uint8_t* buckets;
  uint8_t* tmp_buckets;
};

struct inner_bucket
{
  bool is_numeric_val;
  uint16_t flags;
  uint64_t timestamp;
  uint64_t cas;
  size_t keylen;
  size_t vallen;
  uint8_t data[0];
} __attribute__((packed));

struct bucket
{
  atomic_char magic;
  volatile uint64_t txid;
  struct inner_bucket ibucket;
} __attribute__((packed));

// We probably need to encode different write types in lru_val:
// such as set, append, prepend, incr, decr, etc.
struct lru_val
{
  bool is_numeric_val;
  size_t vallen;
  void* value;
  uint64_t cas;
  uint16_t flags;
};

struct scavenger_t
{
  lru_t* lru;
  unsigned int iter;
  unsigned int prio_queue[0];
};

uint64_t lru_capacity(uint8_t capacity_clz, uint8_t capacity_ms4b)
{
  return (1ULL << (64 - capacity_clz - 4)) * capacity_ms4b;
}

struct inner_bucket* alloc_tmpbucket(lru_t* lru, uint8_t* idx)
{
  inline_keylen = lru->inline_keylen;
  inline_vallen = lru->inline_vallen;
  ibucket_size = sizeof(inner_bucket) + inline_keylen + inline_vallen;

reload:
  bmap = atomic_load_explicit(&lru->tmp_bucket_bmap, memory_order_acquire);
  do
    {
      if (~bmap)
        goto reload;
      new_bmap = bmap + 1;
      bmbit = __builtin_ctzl(new_bmap);
      new_bmap |= bmap;
    } while(!atomic_compare_exchange_strong_explicit
            (&lru->tmp_bucket_bmap, &bmap, new_bmap,
             memory_order_acq_rel,
             memory_order_acquire));
  *idx = bmbit;
  return (struct inner_bucket*)&lru->tmp_buckets[ibucket_size * bmbit];
}

void free_tmpbucket(lru_t* lru, uint8_t idx)
{
  atomic_fetch_and_explicit(&lru->tmp_bucket_bmap, ~(1ULL << idx),
                            memory_order_release);
}

lru_t* lru_init(uint64_t num_objects, size_t inline_keylen, size_t inline_vallen)
{
  lru_t* lru;
  uint64_t capacity;
  uint32_t capacity_clz, capacity_ms4b, capacity_msb;
  size_t ibucket_size, bucket_size;

  lru = calloc(sizeof(lru_t), 1);

  capacity = num_objects * 7 / 10;
  capacity_clz = __builtin_clzl(capacity);
  capacity_msb = 64 - capacity_clz;
  capacity_ms4b = round_up_div(capacity, 1UL << (capacity_msb - 4));
  capacity = lru_capacity(capacity_clz, capacity_ms4b);

  inline_keylen = inline_keylen > 8 ? inline_keylen : 8;
  inline_vallen = inline_vallen > 8 ? inline_vallen : 8;

  bucket_size = sizeof(bucket) + inline_keylen + inline_vallen;
  ibucket_size = sizeof(inner_bucket) + inline_keylen + inline_vallen;

  lru->capacity_clz = capacity_clz;
  lru->capacity_ms4b = capacity_ms4b;
  lru->inline_keylen = inline_keylen;
  lru->inline_vallen = inline_vallen;
  lru->buckets = calloc(bucket_size, capacity);
  lru->tmp_buckets = calloc(ibucket_size, 64);

  return lru;
}

// The whole lru_get is wrapped by rcu_read_lock()
bool lru_get(lru_t* lru, void* key, size_t keylen, lru_val_t* lru_val)
{
  size_t inline_keylen, inline_vallen, ibucket_size, bucket_size;
  uint64_t hashed_key;
  inline_keylen = lru->inline_keylen;
  inline_vallen = lru->inline_vallen;
  bucket_size = sizeof(bucket) + inline_keylen + inline_vallen;
  ibucket_size = sizeof(inner_bucket) + inline_keylen + inline_vallen;
  longest_probes = atomic_load_explicit(&lru->longest_probes,
                                        memory_order_acquire);
  buckets = lru->buckets;

  capacity = lru_capacity(lru->capacity_clz, lru->capacity_ms4b);
  mask = (1ULL << (64 - lru->capacity_clz)) - 1;
  up32key = hashed_key >> 32;

  probing_key = hashed_key;
  idx = fast_mod_scale(probing_key, mask, lru->capacity_ms4b);
  probing_key += up32key;
  idx_next = fast_mod_scale(probing_key, mask, lru->capacity_ms4b);
  for (int probe = 0; probe <= longest_probes; probe++)
    {
      int i = 0;
      __builtin_prefetch(&buckets[idx_next * bucket_size], 0, 0);
      while (true)
        {
          bucket = (struct bucket*)&buckets[idx * bucket_size];
          magic = atomic_load_explicit(&bucket->magic, memory_order_acquire);
          if (magic & 0x3 == 0) 
            return false;
          if (magic & 0x3 == 2)
            goto next_iter;
          if (magic == 1)
            {
              if (bucket->ibucket.keylen != keylen)
                goto next_iter;
              keyptr = keylen > inline_keylen ?
               *((void**)bucket->ibucket.data[0]) :
               &bucket->ibucket.data[0];
              if (!memeq(keyptr, key, keylen))
                goto next_iter;
              txid = atomic_load_explicit(&lru->txid,
                                          memory_order_acquire);
              bucket->txid = txid;
              lru_val->value = bucket->ibucket.data + keylen;
              lru_val->valen = bucket->ibucket.vallen;
              lru_val->cas = bucket->ibucket.cas;
              lru_val->flags = bucket->ibucket.flags;
              return true;
            }
          else
            {
              tmp_idx = magic >> 2;
              ibucket = (struct inner_bucket*)
               &tmp_buckets[tmp_idx * ibucket_size];
              if (ibucket->keylen != keylen)
                goto next_iter;
              keyptr = ibucket->data;
              if (!memeq(keyptr, key, keylen))
                goto next_iter;
              txid = atomic_load_explicit(&lru->txid,
                                          memory_order_acquire);
              bucket->txid = txid;
              lru_val->value = ibucket->data + keylen;
              lru_val->valen = ibucket->vallen;
              lru_val->cas = bucket->ibucket.cas;
              lru_val->flags = ibucket->flags;
              return true;
            }
next_iter:
          if (++i == 4) break;
          idx++;
          if (idx >= capacity)
            idx = 0;
          probe++;
        }
      idx = idx_next;
      probing_key += up32_key;
      idx_next = fast_mod_scale(probing_key, mask, lru->capacity_ms4b);
    }
  return false;
}

bool lru_upsert(lru_t* lru, void* key, size_t keylen,
                lru_val* valref)
{
  capacity = lru_capacity(lru->capacity_clz, lru->capacity_ms4b);
  mask = (1ULL << (64 - lru->capacity_clz)) - 1;
  up32key = hashed_key >> 32;

  probing_key = hashed_key;
  idx = fast_mod_scale(probing_key, mask, lru->capacity_ms4b);
  probing_key += up32key;
  idx_next = fast_mod_scale(probing_key, mask, lru->capacity_ms4b);
  for (int probe = 0; probe < PROBE_STATS_SIZE; probe++)
    {
      int i = 0;
      __builtin_prefetch(&buckets[idx_next * bucket_size], 0, 0);
      while (true)
        {
          bucket = (struct bucket*)&buckets[idx * bucket_size];
          magic = atomic_load_explicit(&bucket->magic, memory_order_acquire);
check_magic:
          if (magic == 0x80 || magic == 0x82 || magic & 0x3 == 0x3) 
            continue;
          // insert case
          if (magic == 0 || magic == 2)
            {
              new_magic = magic | 0x80;
              if (!atomic_compare_exchange_strong(&bucket->magic, &magic, new_magic))
                goto check_magic;
              // setup keylen etc.

              return true;
            }
          if (bucket->ibucket.keylen != keylen)
            goto next_iter;
          keyptr = keylen > inline_keylen ?
           *(void**)&bucket->ibucket.data :
           &bucket->ibucket.data;
          if (!memeq(keyptr, key, keylen))
            goto next_iter;
          tmp_idx;
          ibucket = alloc_tmpbucket(lru, &tmp_idx);
          new_magic = magic | (tmp_idx << 2);
          if (!atomic_compare_exchange_strong(&bucket->magic, &magic, new_magic))
            {
              free_tmpbucket(lru, tmp_idx);
              goto check_magic;
            }
          memcpy(ibucket, &bucket->ibucket, ibucket_size);
          synchronize_rcu();

          // TODO: update bucket
          atomic_store_explicit(&bucket->magic, 1, memory_order_release);
          synchronize_rcu();
          free_tmpbucket(lru, tmp_idx);
          return true;
next_iter:
          if (++i == 4) break;
          idx++;
          if (idx >= capacity)
            idx = 0;
          probe++;
        }
    }
  return false;
}

bool lru_write_ibucket(lru_t* lru, struct bucket* bucket, cmd_handler* cmd, bool write_key)
{
  // switch on cmd->req.op to perform tasks
  // add should always be write_key
  // replace -> write_key = false
  switch(cmd->req.op)
    {
    case PROTOCOL_BINARY_CMD_SET:
    case PROTOCOL_BINARY_CMD_SETQ:
      if (cmd->req.cas > 0 && cmd->req.cas != bucket->ibucket.cas)
        return false;
      if (bucket->is_numeric_val)
        {
          // set/add
          // numeric speicific checks
          return;
        }
      if (write_key)
        {
          keyptr = (void**)&bucket->data;
        }
      if (bucket->vallen > inline_vallen) {
        valptr = (void**)&bucket->data[inline_keylen];
        if (*valptr)
          free(*valptr);
      }
      bucket->vallen = cmd->value_stored;
      if (bucket->vallen > inline_vallen) {
        valptr = (void**)&bucket->data[inline_keylen];
        *valptr = malloc(bucket->vallen);
        memcpy(*valptr, cmd->value, bucket->vallen);
      } else {
        memcpy(&bucket->data[inline_keylen], cmd->value, bucket->vallen);
      }
      bucket->flags = cmd->extra.twoval.flags;
      bucket->timestamp = gettimeofday(); // TODO
      return;
    case PROTOCOL_BINARY_CMD_ADD:
    case PROTOCOL_BINARY_CMD_ADDQ:
    case PROTOCOL_BINARY_CMD_REPLACE:
    case PROTOCOL_BINARY_CMD_REPLACEQ:
    case PROTOCOL_BINARY_CMD_APPEND:
    case PROTOCOL_BINARY_CMD_APPENDQ:
    case PROTOCOL_BINARY_CMD_PREPEND:
    case PROTOCOL_BINARY_CMD_PREPENDQ:
    case PROTOCOL_BINARY_CMD_INCREMENT:
    case PROTOCOL_BINARY_CMD_INCREMENTQ:
    case PROTOCOL_BINARY_CMD_DECREMENT:
    case PROTOCOL_BINARY_CMD_DECREMENTQ:
    case PROTOCOL_BINARY_CMD_TOUCH:
    }
}

void lru_delete(lru_t* lru, void* key, size_t keylen)
{
}

void lru_delete_idx(lru_t* lru, unsigned int idx)
{
  bucket = (struct bucket*)&buckets[idx * bucket_size];
  do {
    magic = atomic_load_explicit(&bucket->magic, memory_order_acquire);
  } while (magic == 0x80 || magic == 0x82 || magic & 0x3 == 0x3);

  if (magic == 0 || magic == 2)
    return;
  keylen = bucket->ibucket.keylen;
  vallen = bucket->ibucket.vallen;
  keyptr = keylen > inline_keylen ? *(void**)&bucket->ibucket.data : NULL;
  valptr = vallen > inline_vallen ?
   *(void**)&bucket->ibucket.data[inline_keylen] : NULL;
  atomic_store_explicit(&bucket->magic, 0, memory_order_release);
  synchronize_rcu();
  if (keyptr)
    free(keyptr);
  if (valptr)
    free(valptr);
  // TODO find its probe length and update probe stat
}
