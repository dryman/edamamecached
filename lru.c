#include "lru.h"
#include "cityhash.h"
#include "cmd_parser.h"
#include "util.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <tgmath.h>
#include <time.h>
#include <urcu.h>

struct inner_bucket
{
  bool is_numeric_val;
  uint16_t flags;
  uint16_t probe;
  time_t epoch;
  uint64_t cas;
  size_t keylen;
  size_t vallen;
  uint8_t data[0];
} __attribute__((packed));

struct bucket
{
  atomic_uchar magic;
  volatile uint64_t txid;
  struct inner_bucket ibucket;
} __attribute__((packed));

void lru_write_empty_bucket(lru_t *lru, struct bucket *bucket,
                            cmd_handler *cmd, lru_val_t *lru_val);
bool lru_update_bucket(lru_t *lru, struct bucket *bucket, cmd_handler *cmd,
                       lru_val_t *lru_val);

uint64_t
lru_capacity_(uint8_t capacity_clz, uint8_t capacity_ms4b)
{
  return (1ULL << (64 - capacity_clz - 4)) * capacity_ms4b;
}

uint64_t
lru_capacity(lru_t *lru)
{
  return lru_capacity_(lru->capacity_clz, lru->capacity_ms4b);
}

lru_t *
lru_init(uint64_t num_objects, size_t inline_keylen, size_t inline_vallen)
{
  lru_t *lru;
  uint64_t capacity;
  uint32_t capacity_clz, capacity_ms4b, capacity_msb;
  size_t ibucket_size, bucket_size;

  lru = calloc(sizeof(lru_t), 1);

  capacity = num_objects * 10 / 7;
  capacity_clz = __builtin_clzl(capacity);
  capacity_msb = 64 - capacity_clz;
  capacity_ms4b = round_up_div(capacity, 1UL << (capacity_msb - 4));
  capacity = lru_capacity_(capacity_clz, capacity_ms4b);

  inline_keylen = inline_keylen > 8 ? inline_keylen : 8;
  inline_vallen = inline_vallen > 8 ? inline_vallen : 8;

  bucket_size = sizeof(struct bucket) + inline_keylen + inline_vallen;
  ibucket_size = sizeof(struct inner_bucket) + inline_keylen + inline_vallen;

  lru->capacity_clz = capacity_clz;
  lru->capacity_ms4b = capacity_ms4b;
  lru->inline_keylen = inline_keylen;
  lru->inline_vallen = inline_vallen;
  lru->buckets = calloc(bucket_size, capacity);
  lru->tmp_buckets = calloc(ibucket_size, 64);
  lru->txid = 1;

  return lru;
}

swiper_t *
swiper_init(lru_t *lru, uint32_t pq_size)
{
  size_t swiper_size;
  swiper_size = sizeof(swiper_t) + pq_size * sizeof(uint64_t[2]);
  swiper_t *swiper = calloc(1, swiper_size);
  swiper->lru = lru;
  swiper->pqueue_size = pq_size;
  return swiper;
}

void
lru_cleanup(lru_t *lru)
{
  size_t inline_keylen, inline_vallen, ibucket_size, bucket_size;
  uint64_t capacity;
  uint8_t *buckets, magic, new_magic;
  struct bucket *bucket;
  void *keyptr, *valptr;

  inline_keylen = lru->inline_keylen;
  inline_vallen = lru->inline_vallen;
  bucket_size = sizeof(struct bucket) + inline_keylen + inline_vallen;
  ibucket_size = sizeof(struct inner_bucket) + inline_keylen + inline_vallen;
  buckets = lru->buckets;
  capacity = lru_capacity_(lru->capacity_clz, lru->capacity_ms4b);

  for (size_t idx = 0; idx < capacity; idx++)
    {
      bucket = (struct bucket *)&buckets[bucket_size * idx];
      while (true)
        {
          magic = atomic_load_explicit(&bucket->magic, memory_order_acquire);
          if (magic == 0x00 || magic == 0x02)
            goto next_loop;
          if (magic == 0x80 || magic == 0x82 || (magic & 0x3) == 0x3)
            continue;
          new_magic = 0x82;
          if (atomic_compare_exchange_strong_explicit(
                  &bucket->magic, &magic, new_magic, memory_order_acq_rel,
                  memory_order_acquire))
            break;
        }
      if (bucket->ibucket.keylen > inline_keylen)
        {
          keyptr = *((void **)&bucket->ibucket.data[0]);
          free(keyptr);
          atomic_fetch_sub_explicit(&lru->ninline_keycnt, 1,
                                    memory_order_relaxed);
          atomic_fetch_sub_explicit(&lru->ninline_keylen,
                                    bucket->ibucket.keylen,
                                    memory_order_relaxed);
        }
      else
        {
          atomic_fetch_sub_explicit(&lru->inline_acc_keylen,
                                    bucket->ibucket.keylen,
                                    memory_order_relaxed);
        }
      if (!bucket->ibucket.is_numeric_val)
        {
          if (bucket->ibucket.vallen > inline_vallen)
            {
              valptr = *((void **)&bucket->ibucket.data[inline_keylen]);
              free(valptr);
              atomic_fetch_sub_explicit(&lru->ninline_valcnt, 1,
                                        memory_order_relaxed);
              atomic_fetch_sub_explicit(&lru->ninline_vallen,
                                        bucket->ibucket.vallen,
                                        memory_order_relaxed);
            }
          else
            {
              atomic_fetch_sub_explicit(&lru->inline_acc_vallen,
                                        bucket->ibucket.vallen,
                                        memory_order_relaxed);
            }
        }
      atomic_fetch_sub_explicit(&lru->objcnt, 1, memory_order_relaxed);
      atomic_store_explicit(&bucket->magic, 0x02, memory_order_release);
    next_loop:
      (void)0;
    }
  free(lru->buckets);
  free(lru->tmp_buckets);
}

struct inner_bucket *
alloc_tmpbucket(lru_t *lru, uint8_t *idx)
{
  size_t inline_keylen, inline_vallen, ibucket_size;
  uint64_t bmap, new_bmap;
  int bmbit;

  inline_keylen = lru->inline_keylen;
  inline_vallen = lru->inline_vallen;
  ibucket_size = sizeof(struct inner_bucket) + inline_keylen + inline_vallen;

reload:
  bmap = atomic_load_explicit(&lru->tmp_bucket_bmap, memory_order_acquire);
  do
    {
      if (!(~bmap))
        goto reload;
      new_bmap = bmap + 1;
      bmbit = __builtin_ctzl(new_bmap);
      new_bmap |= bmap;
    }
  while (!atomic_compare_exchange_strong_explicit(
      &lru->tmp_bucket_bmap, &bmap, new_bmap, memory_order_acq_rel,
      memory_order_acquire));
  *idx = bmbit;
  return (struct inner_bucket *)&lru->tmp_buckets[ibucket_size * bmbit];
}

void
free_tmpbucket(lru_t *lru, uint8_t idx)
{
  atomic_fetch_and_explicit(&lru->tmp_bucket_bmap, ~(1ULL << idx),
                            memory_order_release);
}

static inline uint64_t
fast_mod_scale(uint64_t probed_hash, uint64_t mask, uint64_t scale)
{
  return (probed_hash & mask) * scale >> 4;
}

// The whole lru_get is wrapped by rcu_read_lock()
bool
lru_get(lru_t *lru, cmd_handler *cmd, lru_val_t *lru_val)
{
  size_t inline_keylen, inline_vallen, keylen, ibucket_size, bucket_size;
  uint64_t capacity, hashed_key, probing_key, mask, up32key, idx, idx_next,
      tmp_idx, txid;
  uint32_t longest_probes;
  uint8_t *buckets, magic;
  struct bucket *bucket;
  struct inner_bucket *ibucket;
  void *keyptr;

  inline_keylen = lru->inline_keylen;
  inline_vallen = lru->inline_vallen;
  keylen = cmd->req.keylen;
  bucket_size = sizeof(struct bucket) + inline_keylen + inline_vallen;
  ibucket_size = sizeof(struct inner_bucket) + inline_keylen + inline_vallen;
  longest_probes
      = atomic_load_explicit(&lru->longest_probes, memory_order_acquire);
  buckets = lru->buckets;

  capacity = lru_capacity_(lru->capacity_clz, lru->capacity_ms4b);
  mask = (1ULL << (64 - lru->capacity_clz)) - 1;
  hashed_key = cityhash64((uint8_t *)cmd->key, keylen);
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
          bucket = (struct bucket *)&buckets[idx * bucket_size];
          magic = atomic_load_explicit(&bucket->magic, memory_order_acquire);
          if ((magic & 0x3) == 0)
            {
              lru_val->errcode = STATUS_KEY_NOT_FOUND;
              return false;
            }
          if ((magic & 0x3) == 2)
            goto next_iter;
          if (magic == 1)
            {
              if (bucket->ibucket.keylen != keylen)
                goto next_iter;
              keyptr = keylen > inline_keylen
                           ? *((void **)&bucket->ibucket.data[0])
                           : &bucket->ibucket.data[0];
              if (!memeq(keyptr, cmd->key, keylen))
                goto next_iter;
              txid = atomic_load_explicit(&lru->txid, memory_order_relaxed);
              bucket->txid = txid;
              lru_val->errcode = STATUS_NOERROR;
              lru_val->is_numeric_val = bucket->ibucket.is_numeric_val;
              lru_val->vallen = bucket->ibucket.vallen;
              if (!lru_val->is_numeric_val)
                {
                  lru_val->value
                      = lru_val->vallen > inline_vallen
                            ? *((void **)&bucket->ibucket.data[inline_keylen])
                            : &bucket->ibucket.data[inline_keylen];
                }
              lru_val->cas = bucket->ibucket.cas;
              lru_val->flags = bucket->ibucket.flags;
              return true;
            }
          else
            {
              tmp_idx = magic >> 2;
              // obtain tmp_bucket
              ibucket = (struct inner_bucket *)&lru
                            ->tmp_buckets[tmp_idx * ibucket_size];
              if (ibucket->keylen != keylen)
                goto next_iter;
              keyptr = ibucket->data;
              if (!memeq(keyptr, cmd->key, keylen))
                goto next_iter;
              txid = atomic_load_explicit(&lru->txid, memory_order_relaxed);
              bucket->txid = txid;
              lru_val->errcode = STATUS_NOERROR;
              lru_val->is_numeric_val = bucket->ibucket.is_numeric_val;
              lru_val->vallen = ibucket->vallen;
              if (!lru_val->is_numeric_val)
                {
                  lru_val->value
                      = lru_val->vallen > inline_vallen
                            ? *((void **)&bucket->ibucket.data[inline_keylen])
                            : &bucket->ibucket.data[inline_keylen];
                }
              lru_val->cas = bucket->ibucket.cas;
              lru_val->flags = ibucket->flags;
              return true;
            }
        next_iter:
          if (++i == 4)
            break;
          idx++;
          if (idx >= capacity)
            idx = 0;
          probe++;
        }
      idx = idx_next;
      probing_key += up32key;
      idx_next = fast_mod_scale(probing_key, mask, lru->capacity_ms4b);
    }
  lru_val->errcode = STATUS_KEY_NOT_FOUND;
  return false;
}

bool
lru_upsert(lru_t *lru, cmd_handler *cmd, lru_val_t *lru_val)
{
  size_t inline_keylen, inline_vallen, ibucket_size, bucket_size, keylen;
  uint64_t capacity, hashed_key, probing_key, mask, up32key, idx, idx_next;
  uint32_t longest_probes;
  uint8_t *buckets, magic, new_magic;
  struct bucket *bucket;
  struct inner_bucket *ibucket;
  void *keyptr;
  bool ret;

  inline_keylen = lru->inline_keylen;
  inline_vallen = lru->inline_vallen;
  keylen = cmd->req.keylen;
  bucket_size = sizeof(struct bucket) + inline_keylen + inline_vallen;
  ibucket_size = sizeof(struct inner_bucket) + inline_keylen + inline_vallen;
  buckets = lru->buckets;

  capacity = lru_capacity_(lru->capacity_clz, lru->capacity_ms4b);
  mask = (1ULL << (64 - lru->capacity_clz)) - 1;
  hashed_key = cityhash64((uint8_t *)cmd->key, keylen);
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
          rcu_read_lock();
          bucket = (struct bucket *)&buckets[idx * bucket_size];
          magic = atomic_load_explicit(&bucket->magic, memory_order_acquire);
          if (magic == 0x80 || magic == 0x82 || (magic & 0x3) == 0x3)
            {
              rcu_read_unlock();
              continue;
            }
          // insert case
          if (magic == 0 || magic == 2)
            {
              // We're not going to read the value rcu read lock
              // is protecting, so we can release the read lock here.
              rcu_read_unlock();
              switch (cmd->req.op)
                {
                case PROTOCOL_BINARY_CMD_SET:
                case PROTOCOL_BINARY_CMD_SETQ:
                case PROTOCOL_BINARY_CMD_ADD:
                case PROTOCOL_BINARY_CMD_ADDQ:
                  break;
                case PROTOCOL_BINARY_CMD_INCREMENT:
                case PROTOCOL_BINARY_CMD_INCREMENTQ:
                case PROTOCOL_BINARY_CMD_DECREMENT:
                case PROTOCOL_BINARY_CMD_DECREMENTQ:
                  if (cmd->extra.numeric.init_value == UINT64_MAX)
                    {
                      // TODO document UINT64_MAX behavior
                      lru_val->errcode = STATUS_KEY_NOT_FOUND;
                      return false;
                    }
                  break;
                case PROTOCOL_BINARY_CMD_REPLACE:
                case PROTOCOL_BINARY_CMD_REPLACEQ:
                case PROTOCOL_BINARY_CMD_APPEND:
                case PROTOCOL_BINARY_CMD_APPENDQ:
                case PROTOCOL_BINARY_CMD_PREPEND:
                case PROTOCOL_BINARY_CMD_PREPENDQ:
                  lru_val->errcode = STATUS_ITEM_NOT_STORED;
                  return false;
                case PROTOCOL_BINARY_CMD_TOUCH:
                case PROTOCOL_BINARY_CMD_TOUCHQ:
                  lru_val->errcode = STATUS_KEY_NOT_FOUND;
                  return false;
                default:
                  lru_val->errcode = STATUS_INTERNAL_ERR;
                  return false;
                }
              new_magic = magic | 0x80;
              if (!atomic_compare_exchange_strong_explicit(
                      &bucket->magic, &magic, new_magic, memory_order_acq_rel,
                      memory_order_acquire))
                continue;
              lru_write_empty_bucket(lru, bucket, cmd, lru_val);
              bucket->ibucket.probe = probe;
              atomic_store_explicit(&bucket->magic, 1, memory_order_release);
              atomic_fetch_add_explicit(&lru->probe_stats[probe], 1,
                                        memory_order_relaxed);

              longest_probes = atomic_load_explicit(&lru->longest_probes,
                                                    memory_order_acquire);
              do
                {
                  if (probe <= longest_probes)
                    break;
                }
              while (!atomic_compare_exchange_strong_explicit(
                  &lru->longest_probes, &longest_probes, probe,
                  memory_order_release, memory_order_relaxed));
              return true;
            }
          // This is where we read the value rcu read lock is protecting
          if (bucket->ibucket.keylen != keylen)
            {
              rcu_read_unlock();
              goto next_iter;
            }
          keyptr = keylen > inline_keylen ? *(void **)&bucket->ibucket.data
                                          : &bucket->ibucket.data;
          if (!memeq(keyptr, cmd->key, keylen))
            {
              rcu_read_unlock();
              goto next_iter;
            }
          // accessing key finished, now we free the rcu
          // read lock.
          rcu_read_unlock();
          if (cmd->req.op == PROTOCOL_BINARY_CMD_ADD
              || cmd->req.op == PROTOCOL_BINARY_CMD_ADDQ)
            {
              lru_val->errcode = STATUS_ITEM_NOT_STORED;
              return false;
            }
          uint8_t tmp_idx;
          ibucket = alloc_tmpbucket(lru, &tmp_idx);
          memcpy(ibucket, &bucket->ibucket, ibucket_size);
          if (!atomic_compare_exchange_strong_explicit(
                  &bucket->magic, &magic, (tmp_idx << 2) | 0x3,
                  memory_order_acq_rel, memory_order_acquire))
            {
              free_tmpbucket(lru, tmp_idx);
              continue;
            }
          synchronize_rcu();
          ret = lru_update_bucket(lru, bucket, cmd, lru_val);
          atomic_store_explicit(&bucket->magic, 1, memory_order_release);
          synchronize_rcu();
          free_tmpbucket(lru, tmp_idx);
          return ret;
        next_iter:
          if (++i == 4)
            break;
          idx++;
          if (idx >= capacity)
            idx = 0;
          probe++;
        }
    }
  lru_val->errcode = STATUS_BUSY;
  return false;
}

void
lru_write_empty_bucket(lru_t *lru, struct bucket *bucket, cmd_handler *cmd,
                       lru_val_t *lru_val)
{
  size_t inline_keylen, inline_vallen, keylen, vallen;
  uint64_t txid;
  time_t now;

  inline_keylen = lru->inline_keylen;
  inline_vallen = lru->inline_vallen;

  time(&now);
  txid = atomic_fetch_add_explicit(&lru->txid, 1, memory_order_relaxed);
  keylen = cmd->req.keylen;

  switch (cmd->req.op)
    {
    case PROTOCOL_BINARY_CMD_SET:
    case PROTOCOL_BINARY_CMD_SETQ:
    case PROTOCOL_BINARY_CMD_ADD:
    case PROTOCOL_BINARY_CMD_ADDQ:
      bucket->txid = txid;
      bucket->ibucket.is_numeric_val = false;
      bucket->ibucket.flags = cmd->extra.twoval.flags;
      bucket->ibucket.epoch = now + cmd->extra.twoval.expiration;
      bucket->ibucket.cas = txid;

      vallen = cmd->value_stored;
      bucket->ibucket.keylen = keylen;
      bucket->ibucket.vallen = vallen;

      if (keylen > inline_keylen)
        {
          void **keyptr = (void **)&bucket->ibucket.data[0];
          *keyptr = malloc(keylen);
          memcpy(*keyptr, cmd->key, keylen);
          atomic_fetch_add_explicit(&lru->ninline_keycnt, 1,
                                    memory_order_relaxed);
          atomic_fetch_add_explicit(&lru->ninline_keylen, keylen,
                                    memory_order_relaxed);
        }
      else
        {
          memcpy(&bucket->ibucket.data[0], cmd->key, keylen);
          atomic_fetch_add_explicit(&lru->inline_acc_keylen, keylen,
                                    memory_order_relaxed);
        }

      if (vallen > inline_vallen)
        {
          void **valptr = (void **)&bucket->ibucket.data[inline_keylen];
          *valptr = malloc(vallen);
          memcpy(*valptr, cmd->value, vallen);
          atomic_fetch_add_explicit(&lru->ninline_valcnt, 1,
                                    memory_order_relaxed);
          atomic_fetch_add_explicit(&lru->ninline_vallen, vallen,
                                    memory_order_relaxed);
        }
      else
        {
          memcpy(&bucket->ibucket.data[inline_keylen], cmd->value, vallen);
          atomic_fetch_add_explicit(&lru->inline_acc_vallen, vallen,
                                    memory_order_relaxed);
        }
      atomic_fetch_add_explicit(&lru->objcnt, 1, memory_order_relaxed);
      lru_val->errcode = STATUS_NOERROR;
      lru_val->is_numeric_val = false;
      return;
    case PROTOCOL_BINARY_CMD_INCREMENT:
    case PROTOCOL_BINARY_CMD_INCREMENTQ:
    case PROTOCOL_BINARY_CMD_DECREMENT:
    case PROTOCOL_BINARY_CMD_DECREMENTQ:
      bucket->txid = txid;
      bucket->ibucket.is_numeric_val = true;
      bucket->ibucket.cas = txid;
      bucket->ibucket.epoch = now + cmd->extra.twoval.expiration;
      bucket->ibucket.keylen = keylen;
      if (keylen > inline_keylen)
        {
          void **keyptr = (void **)&bucket->ibucket.data[0];
          *keyptr = malloc(keylen);
          memcpy(*keyptr, cmd->key, keylen);
          atomic_fetch_add_explicit(&lru->ninline_keycnt, 1,
                                    memory_order_relaxed);
          atomic_fetch_add_explicit(&lru->ninline_keylen, keylen,
                                    memory_order_relaxed);
        }
      else
        {
          memcpy(&bucket->ibucket.data[0], cmd->key, keylen);
          atomic_fetch_add_explicit(&lru->inline_acc_keylen, keylen,
                                    memory_order_relaxed);
        }
      bucket->ibucket.vallen = cmd->extra.numeric.init_value;
      if (cmd->req.op == PROTOCOL_BINARY_CMD_INCREMENT
          || cmd->req.op == PROTOCOL_BINARY_CMD_INCREMENTQ)
        bucket->ibucket.vallen += cmd->extra.numeric.addition_value;
      else if (cmd->extra.numeric.addition_value > bucket->ibucket.vallen)
        bucket->ibucket.vallen = 0;
      else
        bucket->ibucket.vallen -= cmd->extra.numeric.addition_value;
      atomic_fetch_add_explicit(&lru->objcnt, 1, memory_order_relaxed);
      lru_val->errcode = STATUS_NOERROR;
      lru_val->is_numeric_val = true;
      lru_val->vallen = bucket->ibucket.vallen;
      return;
    default:
      lru_val->errcode = STATUS_INTERNAL_ERR;
    }
}

bool
lru_update_bucket(lru_t *lru, struct bucket *bucket, cmd_handler *cmd,
                  lru_val_t *lru_val)
{
  uint64_t txid;
  size_t inline_keylen, inline_vallen, vallen, current_vallen;
  time_t now;
  uint8_t *valiter;
  void *newval;

  time(&now);
  inline_keylen = lru->inline_keylen;
  inline_vallen = lru->inline_vallen;
  vallen = cmd->value_stored;

  switch (cmd->req.op)
    {
    case PROTOCOL_BINARY_CMD_SET:
    case PROTOCOL_BINARY_CMD_SETQ:
      // Set may be a cas request. Other than cas logic it is
      // same as replace logic.
      if (cmd->req.cas > 0 && cmd->req.cas != bucket->ibucket.cas)
        {
          lru_val->errcode = STATUS_KEY_EXISTS;
          return false;
        }
    case PROTOCOL_BINARY_CMD_REPLACE:
    case PROTOCOL_BINARY_CMD_REPLACEQ:
      txid = atomic_fetch_add_explicit(&lru->txid, 1, memory_order_relaxed);
      bucket->txid = txid;
      bucket->ibucket.is_numeric_val = false;
      bucket->ibucket.flags = cmd->extra.twoval.flags;
      bucket->ibucket.epoch = now + cmd->extra.twoval.expiration;
      bucket->ibucket.cas = txid;
      if (!bucket->ibucket.is_numeric_val)
        {
          if (bucket->ibucket.vallen > inline_vallen)
            {
              void **valptr = (void **)&bucket->ibucket.data[inline_keylen];
              free(*valptr);
              atomic_fetch_sub_explicit(&lru->ninline_valcnt, 1,
                                        memory_order_relaxed);
              atomic_fetch_sub_explicit(&lru->ninline_vallen,
                                        bucket->ibucket.vallen,
                                        memory_order_relaxed);
            }
          else
            {
              atomic_fetch_sub_explicit(&lru->inline_acc_vallen,
                                        bucket->ibucket.vallen,
                                        memory_order_relaxed);
            }
        }
      bucket->ibucket.vallen = vallen;
      if (vallen > inline_vallen)
        {
          void **valptr = (void **)&bucket->ibucket.data[inline_keylen];
          *valptr = malloc(vallen);
          memcpy(*valptr, cmd->value, vallen);
          atomic_fetch_add_explicit(&lru->ninline_valcnt, 1,
                                    memory_order_relaxed);
          atomic_fetch_add_explicit(&lru->ninline_vallen, vallen,
                                    memory_order_relaxed);
        }
      else
        {
          memcpy(&bucket->ibucket.data[inline_keylen], cmd->value, vallen);
          atomic_fetch_add_explicit(&lru->inline_acc_vallen, vallen,
                                    memory_order_relaxed);
        }
      lru_val->errcode = STATUS_NOERROR;
      return true;
    case PROTOCOL_BINARY_CMD_APPEND:
    case PROTOCOL_BINARY_CMD_APPENDQ:
    case PROTOCOL_BINARY_CMD_PREPEND:
    case PROTOCOL_BINARY_CMD_PREPENDQ:
      txid = atomic_fetch_add_explicit(&lru->txid, 1, memory_order_relaxed);
      bucket->txid = txid;
      bucket->ibucket.flags = cmd->extra.twoval.flags;
      bucket->ibucket.epoch = now + cmd->extra.twoval.expiration;
      bucket->ibucket.cas = txid;

      if (bucket->ibucket.is_numeric_val)
        {
          if (bucket->ibucket.vallen == 0)
            current_vallen = 1;
          else
            current_vallen = floor(log10(bucket->ibucket.vallen)) + 1;
          bucket->ibucket.is_numeric_val = false;

          if (current_vallen + vallen > inline_vallen)
            {
              void **valptr = (void **)&bucket->ibucket.data[inline_keylen];
              *valptr = malloc(vallen + current_vallen);
              valiter = *valptr;
              atomic_fetch_add_explicit(&lru->ninline_valcnt, 1,
                                        memory_order_relaxed);
              atomic_fetch_add_explicit(&lru->ninline_vallen,
                                        vallen + current_vallen,
                                        memory_order_relaxed);
            }
          else
            {
              valiter = &bucket->ibucket.data[inline_keylen];
              atomic_fetch_add_explicit(&lru->inline_acc_vallen,
                                        current_vallen + vallen,
                                        memory_order_relaxed);
            }
          if (cmd->req.op == PROTOCOL_BINARY_CMD_APPEND
              || cmd->req.op == PROTOCOL_BINARY_CMD_APPENDQ)
            {
              valiter
                  += sprintf((char *)valiter, "%zu", bucket->ibucket.vallen);
              memcpy(valiter, cmd->value, vallen);
              bucket->ibucket.vallen = current_vallen + vallen;
            }
          else
            { // prepend
              memcpy(valiter, cmd->value, vallen);
              valiter += vallen;
              sprintf((char *)valiter, "%zu", bucket->ibucket.vallen);
              bucket->ibucket.vallen = current_vallen + vallen;
            }
          lru_val->errcode = STATUS_NOERROR;
          return true;
        }

      // Non numerical value case
      current_vallen = bucket->ibucket.vallen;

      if (current_vallen > inline_vallen)
        {
          void **valptr = (void **)&bucket->ibucket.data[inline_keylen];
          newval = valiter = malloc(vallen + current_vallen);
          if (cmd->req.op == PROTOCOL_BINARY_CMD_APPEND
              || cmd->req.op == PROTOCOL_BINARY_CMD_APPENDQ)
            {
              memcpy(valiter, *valptr, current_vallen);
              valiter += current_vallen;
              memcpy(valiter, cmd->value, vallen);
            }
          else
            {
              memcpy(valiter, cmd->value, vallen);
              valiter += vallen;
              memcpy(valiter, *valptr, current_vallen);
            }
          free(*valptr);
          *valptr = newval;
          atomic_fetch_add_explicit(&lru->ninline_vallen, vallen,
                                    memory_order_relaxed);
          bucket->ibucket.vallen = current_vallen + vallen;
          return true;
        }
      else if (current_vallen + vallen > inline_vallen)
        {
          void **valptr = (void **)&bucket->ibucket.data[inline_keylen];
          newval = valiter = malloc(vallen + current_vallen);
          if (cmd->req.op == PROTOCOL_BINARY_CMD_APPEND
              || cmd->req.op == PROTOCOL_BINARY_CMD_APPENDQ)
            {
              memcpy(valiter, &bucket->ibucket.data[inline_keylen],
                     current_vallen);
              valiter += current_vallen;
              memcpy(valiter, cmd->value, vallen);
            }
          else
            {
              memcpy(valiter, cmd->value, vallen);
              valiter += vallen;
              memcpy(valiter, &bucket->ibucket.data[inline_keylen],
                     current_vallen);
            }
          *valptr = newval;
          atomic_fetch_sub_explicit(&lru->inline_acc_vallen, current_vallen,
                                    memory_order_relaxed);
          atomic_fetch_add_explicit(&lru->ninline_valcnt, 1,
                                    memory_order_relaxed);
          atomic_fetch_add_explicit(&lru->ninline_vallen,
                                    vallen + current_vallen,
                                    memory_order_relaxed);
          bucket->ibucket.vallen = current_vallen + vallen;
          lru_val->errcode = STATUS_NOERROR;
          return true;
        }
      else
        {
          if (cmd->req.op == PROTOCOL_BINARY_CMD_APPEND
              || cmd->req.op == PROTOCOL_BINARY_CMD_APPENDQ)
            {
              memcpy(&bucket->ibucket.data[inline_keylen + current_vallen],
                     cmd->value, vallen);
            }
          else
            {
              void *tmp = alloca(current_vallen);
              memcpy(tmp, &bucket->ibucket.data[inline_keylen],
                     current_vallen);
              memcpy(&bucket->ibucket.data[inline_keylen], cmd->value, vallen);
              memcpy(&bucket->ibucket.data[inline_keylen + vallen], tmp,
                     current_vallen);
            }
          atomic_fetch_add_explicit(&lru->inline_acc_vallen, vallen,
                                    memory_order_relaxed);
          bucket->ibucket.vallen = current_vallen + vallen;
          lru_val->errcode = STATUS_NOERROR;
          return true;
        }
    case PROTOCOL_BINARY_CMD_INCREMENT:
    case PROTOCOL_BINARY_CMD_INCREMENTQ:
    case PROTOCOL_BINARY_CMD_DECREMENT:
    case PROTOCOL_BINARY_CMD_DECREMENTQ:
      if (!bucket->ibucket.is_numeric_val)
        {
          ed_errno = 0;
          // BUG: valptr may not be inline
          uint64_t numeric_val;
          void *valptr;
          valptr = bucket->ibucket.vallen > inline_vallen
                       ? *((void **)&bucket->ibucket.data[inline_keylen])
                       : &bucket->ibucket.data[inline_keylen];

          numeric_val
              = strn2uint64(valptr, bucket->ibucket.vallen, (char **)&valiter);
          if (ed_errno
              || (valiter - (uint8_t *)valptr) != bucket->ibucket.vallen)
            {
              syslog(LOG_ERR,
                     "cannot increment or decrement non-numeric value");
              lru_val->errcode = STATUS_NON_NUMERIC;
              return false;
            }
          if (bucket->ibucket.vallen > inline_vallen)
            {
              free(valptr);
              atomic_fetch_sub_explicit(&lru->ninline_valcnt, 1,
                                        memory_order_relaxed);
              atomic_fetch_sub_explicit(&lru->ninline_vallen,
                                        bucket->ibucket.vallen,
                                        memory_order_relaxed);
            }
          else
            {
              atomic_fetch_sub_explicit(&lru->inline_acc_vallen,
                                        bucket->ibucket.vallen,
                                        memory_order_relaxed);
            }
          bucket->ibucket.is_numeric_val = true;
          bucket->ibucket.vallen = numeric_val;
        }
      txid = atomic_fetch_add_explicit(&lru->txid, 1, memory_order_relaxed);
      bucket->txid = txid;
      bucket->ibucket.cas = txid;
      if (cmd->req.op == PROTOCOL_BINARY_CMD_INCREMENT
          || cmd->req.op == PROTOCOL_BINARY_CMD_INCREMENTQ)
        bucket->ibucket.vallen += cmd->extra.numeric.addition_value;
      else if (cmd->extra.numeric.addition_value > bucket->ibucket.vallen)
        bucket->ibucket.vallen = 0;
      else
        bucket->ibucket.vallen -= cmd->extra.numeric.addition_value;
      // Numeric expiration is only exposed to the binary API.
      // We use UINT64_MAX to mark if a numeric command is parsed from
      // ascii protocol or from binary protocol (UINT64_MAX => ascii).
      // When it is binary, we add the expiration, otherwise it inherits
      // the expiration.
      if (cmd->extra.numeric.init_value != UINT64_MAX)
        bucket->ibucket.epoch = now + cmd->extra.numeric.expiration;
      lru_val->errcode = STATUS_NOERROR;
      lru_val->is_numeric_val = true;
      lru_val->vallen = bucket->ibucket.vallen;
      return true;
    case PROTOCOL_BINARY_CMD_TOUCH:
    case PROTOCOL_BINARY_CMD_TOUCHQ:
      txid = atomic_load_explicit(&lru->txid, memory_order_relaxed);
      bucket->txid = txid;
      bucket->ibucket.epoch = now + cmd->extra.twoval.expiration;
      lru_val->errcode = STATUS_NOERROR;
      return true;
    default:
      break;
    }
  syslog(LOG_ERR, "Unknown op %d", cmd->req.op);
  lru_val->errcode = STATUS_INTERNAL_ERR;
  return false;
}

void
lru_delete(lru_t *lru, cmd_handler *cmd)
{
  size_t inline_keylen, inline_vallen, keylen, vallen, ibucket_size,
      bucket_size;
  uint64_t capacity, hashed_key, probing_key, mask, up32key, idx, idx_next;
  uint32_t longest_probes;
  uint8_t *buckets, magic;
  struct bucket *bucket;
  void *keyptr;

  inline_keylen = lru->inline_keylen;
  inline_vallen = lru->inline_vallen;
  keylen = cmd->req.keylen;
  bucket_size = sizeof(struct bucket) + inline_keylen + inline_vallen;
  ibucket_size = sizeof(struct inner_bucket) + inline_keylen + inline_vallen;
  longest_probes
      = atomic_load_explicit(&lru->longest_probes, memory_order_acquire);
  buckets = lru->buckets;

  capacity = lru_capacity_(lru->capacity_clz, lru->capacity_ms4b);
  mask = (1ULL << (64 - lru->capacity_clz)) - 1;
  hashed_key = cityhash64((uint8_t *)cmd->key, keylen);
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
          rcu_read_lock();
          bucket = (struct bucket *)&buckets[idx * bucket_size];
          magic = atomic_load_explicit(&bucket->magic, memory_order_acquire);

          if (magic == 0)
            {
              rcu_read_unlock();
              return;
            }
          if (magic == 2)
            {
              rcu_read_unlock();
              goto next_iter;
            }
          if (magic == 0x80 || magic == 0x82 || (magic & 0x3) == 0x3)
            {
              rcu_read_unlock();
              continue;
            }
          if (bucket->ibucket.keylen != keylen)
            {
              rcu_read_unlock();
              goto next_iter;
            }
          keyptr = keylen > inline_keylen ? *(void **)&bucket->ibucket.data
                                          : &bucket->ibucket.data;
          if (!memeq(keyptr, cmd->key, keylen))
            {
              rcu_read_unlock();
              goto next_iter;
            }
          // finished reading the key, so now we can release the rcu read
          // lock.
          rcu_read_unlock();
          // Mutate the magic to exclude other write access.
          // From readers view, this is as if the value is deleted in
          // an atomic operation.
          if (!atomic_compare_exchange_strong_explicit(
                  &bucket->magic, &magic, 0x82, memory_order_release,
                  memory_order_acquire))
            continue;
          // Drain remaining readers that might be accessing it.
          synchronize_rcu();
          keylen = bucket->ibucket.keylen;
          vallen = bucket->ibucket.vallen;
          probe = bucket->ibucket.probe;
          atomic_fetch_sub_explicit(&lru->probe_stats[probe], 1,
                                    memory_order_relaxed);
          if (keylen > inline_keylen)
            {
              atomic_fetch_sub_explicit(&lru->ninline_keycnt, 1,
                                        memory_order_relaxed);
              atomic_fetch_sub_explicit(&lru->ninline_keylen, keylen,
                                        memory_order_relaxed);
              free(*((void **)&bucket->ibucket.data[0]));
            }
          else
            {
              atomic_fetch_sub_explicit(&lru->inline_acc_keylen, keylen,
                                        memory_order_relaxed);
            }
          if (!bucket->ibucket.is_numeric_val)
            {
              if (vallen > inline_vallen)
                {
                  atomic_fetch_sub_explicit(&lru->ninline_valcnt, 1,
                                            memory_order_relaxed);
                  atomic_fetch_sub_explicit(&lru->ninline_vallen, vallen,
                                            memory_order_relaxed);
                  free(*(void **)&bucket->ibucket.data[inline_keylen]);
                }
              else
                {
                  atomic_fetch_sub_explicit(&lru->inline_acc_vallen, vallen,
                                            memory_order_relaxed);
                }
            }

          atomic_fetch_sub_explicit(&lru->objcnt, 1, memory_order_relaxed);
          atomic_store_explicit(&bucket->magic, 2, memory_order_release);
          return;
        next_iter:
          if (++i == 4)
            break;
          idx++;
          if (idx >= capacity)
            idx = 0;
          probe++;
        }
      idx = idx_next;
      probing_key += up32key;
      idx_next = fast_mod_scale(probing_key, mask, lru->capacity_ms4b);
    }
}

bool
lru_delete_bucket(lru_t *lru, struct bucket *bucket, uint64_t txid)
{
  size_t inline_keylen, inline_vallen, keylen, vallen;
  uint16_t probe;
  uint8_t magic;

  inline_keylen = lru->inline_keylen;
  inline_vallen = lru->inline_vallen;

reload_magic:
  magic = atomic_load_explicit(&bucket->magic, memory_order_acquire);
  do
    {
      // If the bucket was touched by other thread, do not delete the
      // bucket.
      if (bucket->txid > txid)
        return false;
      if (magic == 0 || magic == 2)
        return false;
      if (magic == 0x80 || magic == 0x82 || (magic & 0x3) == 0x3)
        goto reload_magic;
    }
  while (!atomic_compare_exchange_strong_explicit(&bucket->magic, &magic, 0x82,
                                                  memory_order_acq_rel,
                                                  memory_order_acquire));

  // drain readers accessing this bucket
  synchronize_rcu();
  keylen = bucket->ibucket.keylen;
  vallen = bucket->ibucket.vallen;
  probe = bucket->ibucket.probe;
  atomic_fetch_sub_explicit(&lru->probe_stats[probe], 1, memory_order_relaxed);
  if (keylen > inline_keylen)
    {
      atomic_fetch_sub_explicit(&lru->ninline_keycnt, 1, memory_order_relaxed);
      atomic_fetch_sub_explicit(&lru->ninline_keylen, keylen,
                                memory_order_relaxed);
      free(*((void **)&bucket->ibucket.data[0]));
    }
  else
    {
      atomic_fetch_sub_explicit(&lru->inline_acc_keylen, keylen,
                                memory_order_relaxed);
    }
  if (!bucket->ibucket.is_numeric_val)
    {
      if (vallen > inline_vallen)
        {
          atomic_fetch_sub_explicit(&lru->ninline_valcnt, 1,
                                    memory_order_relaxed);
          atomic_fetch_sub_explicit(&lru->ninline_vallen, vallen,
                                    memory_order_relaxed);
          free(*(void **)&bucket->ibucket.data[inline_keylen]);
        }
      else
        {
          atomic_fetch_sub_explicit(&lru->inline_acc_vallen, vallen,
                                    memory_order_relaxed);
        }
    }

  atomic_fetch_sub_explicit(&lru->objcnt, 1, memory_order_relaxed);
  atomic_store_explicit(&bucket->magic, 2, memory_order_release);
  return true;
}

void pq_swap(uint64_t (*a)[2], uint64_t (*b)[2])
{
  uint64_t tmp;
  tmp = (*a)[0];
  (*a)[0] = (*b)[0];
  (*b)[0] = tmp;
  tmp = (*a)[1];
  (*a)[1] = (*b)[1];
  (*b)[1] = tmp;
}

void
pq_add(swiper_t *swiper, uint64_t idx, uint64_t txid)
{
  uint64_t pq_idx, pq_idx_parent;

  pq_idx = swiper->pqueue_used++;
  swiper->pqueue[pq_idx][0] = idx;
  swiper->pqueue[pq_idx][1] = txid;
  while (pq_idx > 0)
    {
      pq_idx_parent = (pq_idx - 1) / 2;
      if (swiper->pqueue[pq_idx_parent][1] < swiper->pqueue[pq_idx][1])
        {
          pq_swap(&swiper->pqueue[pq_idx_parent], &swiper->pqueue[pq_idx]);
          pq_idx = pq_idx_parent;
        }
      else
        break;
    }
}

void
pq_pop_add(swiper_t *swiper, uint64_t idx, uint64_t txid)
{
  uint64_t pq_idx, pq_idx_l, pq_idx_r, max_idx;
  pq_idx = 0;
  if (txid >= swiper->pqueue[pq_idx][1])
    return;
  swiper->pqueue[pq_idx][0] = idx;
  swiper->pqueue[pq_idx][1] = txid;
  while (pq_idx < swiper->pqueue_size)
    {
      pq_idx_l = pq_idx * 2 + 1;
      pq_idx_r = pq_idx * 2 + 2;
      max_idx = pq_idx;
      if (pq_idx_l < swiper->pqueue_size
          && swiper->pqueue[pq_idx_l][1] > swiper->pqueue[max_idx][1])
        max_idx = pq_idx_l;
      if (pq_idx_r < swiper->pqueue_size
          && swiper->pqueue[pq_idx_r][1] > swiper->pqueue[max_idx][1])
        max_idx = pq_idx_r;
      if (max_idx != pq_idx)
        {
          pq_swap(&swiper->pqueue[pq_idx], &swiper->pqueue[max_idx]);
          pq_idx = max_idx;
        }
      else
        break;
    }
}

int
pq_cmp(const void *_a, const void *_b)
{
  const int64_t(*a)[2] = _a;
  const int64_t(*b)[2] = _b;
  return (*a)[1] - (*b)[1];
}

void
pq_sort(swiper_t *swiper)
{
  qsort(swiper->pqueue, swiper->pqueue_used, sizeof(uint64_t[2]), pq_cmp);
}

// This method can only be executed by a single thread.
void
lru_swipe(swiper_t *swiper)
{
  lru_t *lru;
  size_t inline_keylen, inline_vallen, ibucket_size, bucket_size;
  uint64_t idx, capacity, txid, pq_idx, threshold, num_to_del, num_deleted,
      objcnt;
  unsigned int longest_probes, new_lp;
  time_t now, epoch;
  uint8_t *buckets;
  struct bucket *bucket;
  uint8_t magic;

  lru = swiper->lru;
  capacity = lru_capacity_(lru->capacity_clz, lru->capacity_ms4b);
  threshold = capacity * 7 / 10;
  inline_keylen = lru->inline_keylen;
  inline_vallen = lru->inline_vallen;
  bucket_size = sizeof(struct bucket) + inline_keylen + inline_vallen;
  ibucket_size = sizeof(struct inner_bucket) + inline_keylen + inline_vallen;
  time(&now);
  buckets = lru->buckets;

  // O(N * log(k)). k = priority queue size
  // Scan through the lru table and delete items with outdated epoch.
  // Enqueue idx and txid into the priority queue. When the priority
  // queue is full, pop the max item in the queue so that we get a
  // set of indexes which has the smallest txids.
  for (idx = 0; idx < capacity; idx++)
    {
      rcu_read_lock();
      bucket = (struct bucket *)&buckets[idx * bucket_size];
      magic = atomic_load_explicit(&bucket->magic, memory_order_acquire);
      // we only delete old enough items
      // items in update state, insert state, empty state or detele state
      // can all be ignored.
      if (magic != 1)
        continue;
      epoch = bucket->ibucket.epoch;
      txid = bucket->txid;
      rcu_read_unlock();

      if (epoch < now)
        {
          lru_delete_bucket(lru, bucket, UINT64_MAX);
        }
      else
        {
          if (swiper->pqueue_used <= swiper->pqueue_size)
            pq_add(swiper, idx, txid);
          else
            pq_pop_add(swiper, idx, txid);
        }
    }

  objcnt = atomic_load_explicit(&lru->objcnt, memory_order_relaxed);
  if (objcnt > threshold)
    {
      num_to_del = objcnt - threshold;
      pq_sort(swiper);
      for (num_deleted = 0, pq_idx = 0;
           num_deleted < num_to_del && pq_idx < swiper->pqueue_size; pq_idx++)
        {
          bucket = (struct bucket
                        *)&buckets[swiper->pqueue[pq_idx][0] * bucket_size];
          if (lru_delete_bucket(lru, bucket, swiper->pqueue[pq_idx][1]))
            num_deleted++;
        }
    }
  swiper->pqueue_used = 0;

  // update the longest probe. longest probe can only be decreased
  // by this thread, this method, so we won't have the aba problem
  longest_probes
      = atomic_load_explicit(&lru->longest_probes, memory_order_acquire);
  do
    {
      new_lp = 0;
      for (idx = 0; idx < PROBE_STATS_SIZE; idx++)
        {
          if (atomic_load_explicit(&lru->probe_stats[idx],
                                   memory_order_relaxed))
            new_lp = idx;
        }
      if (new_lp >= longest_probes)
        break;
    }
  while (atomic_compare_exchange_strong_explicit(
      &lru->longest_probes, &longest_probes, new_lp, memory_order_release,
      memory_order_acquire));
}
