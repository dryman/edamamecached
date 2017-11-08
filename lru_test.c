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

#include "lru.h"
#include <limits.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <urcu.h>
#include <cmocka.h>

static void
test_init_cleanup(void **context)
{
  lru_t *lru;
  lru = lru_init(100, 0, 24);
  assert_int_equal(8, lru->inline_keylen);
  assert_int_equal(24, lru->inline_vallen);
  lru_cleanup(lru);
  assert_int_equal(0, lru->objcnt);
  assert_int_equal(0, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);
  free(lru);
}

static void
test_insert_update(void **context)
{
  lru_t *lru;
  cmd_handler cmd;
  lru_val_t lru_val;
  lru = lru_init(100, 8, 8);

  cmd.state = ASCII_CMD_READY;
  memcpy(&cmd.buffer, "abc", 3);
  cmd.key = &cmd.buffer[0];
  cmd.buf_used = 3;
  cmd.req.keylen = 3;
  cmd.req.op = PROTOCOL_BINARY_CMD_SET;
  cmd.value = "abc";
  cmd.value_stored = 3;
  cmd.req.cas = 0;

  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(3, lru->inline_acc_keylen);
  assert_int_equal(3, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);

  cmd.req.op = PROTOCOL_BINARY_CMD_GET;
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_false(lru_val.is_numeric_val);
  assert_int_equal(3, lru_val.vallen);
  assert_int_equal(1, lru_val.cas);
  assert_memory_equal(cmd.value, lru_val.value, 3);

  cmd.req.op = PROTOCOL_BINARY_CMD_SET;
  cmd.value = "xyz";
  cmd.value_stored = 3;
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(3, lru->inline_acc_keylen);
  assert_int_equal(3, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);

  cmd.req.op = PROTOCOL_BINARY_CMD_GET;
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_false(lru_val.is_numeric_val);
  assert_int_equal(3, lru_val.vallen);
  assert_int_equal(2, lru_val.cas);
  assert_memory_equal(cmd.value, lru_val.value, 3);

  cmd.req.op = PROTOCOL_BINARY_CMD_SET;
  cmd.value = "0123456789";
  cmd.value_stored = 10;
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(3, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(1, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(10, lru->ninline_vallen);

  cmd.req.op = PROTOCOL_BINARY_CMD_GET;
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_false(lru_val.is_numeric_val);
  assert_int_equal(10, lru_val.vallen);
  assert_int_equal(3, lru_val.cas);
  assert_memory_equal(cmd.value, lru_val.value, 10);

  lru_cleanup(lru);
  assert_int_equal(0, lru->objcnt);
  assert_int_equal(0, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);
  free(lru);
}

static void
test_numeric_val(void **context)
{
  lru_t *lru;
  cmd_handler cmd;
  lru_val_t lru_val;
  lru = lru_init(100, 8, 8);

  cmd.state = ASCII_CMD_READY;
  memcpy(&cmd.buffer, "abc", 3);
  cmd.key = &cmd.buffer[0];
  cmd.buf_used = 3;
  cmd.req.keylen = 3;
  cmd.req.op = PROTOCOL_BINARY_CMD_SET;
  cmd.value = "5";
  cmd.value_stored = 1;
  cmd.req.cas = 0;

  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(3, lru->inline_acc_keylen);
  assert_int_equal(1, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);

  cmd.req.op = PROTOCOL_BINARY_CMD_INCREMENT;
  cmd.extra.numeric.addition_value = 5;
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(3, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);
  assert_int_equal(10, lru_val.vallen);

  cmd.req.op = PROTOCOL_BINARY_CMD_GET;
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_true(lru_val.is_numeric_val);
  assert_int_equal(10, lru_val.vallen);
  assert_int_equal(2, lru_val.cas);

  // decrement larger than stored val yields zero
  cmd.req.op = PROTOCOL_BINARY_CMD_DECREMENT;
  cmd.extra.numeric.addition_value = 20;
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(3, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);
  assert_int_equal(0, lru_val.vallen);

  cmd.req.op = PROTOCOL_BINARY_CMD_GET;
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_true(lru_val.is_numeric_val);
  assert_int_equal(0, lru_val.vallen);
  assert_int_equal(3, lru_val.cas);

  memcpy(&cmd.buffer, "xyz", 3);
  cmd.req.op = PROTOCOL_BINARY_CMD_INCREMENT;
  cmd.extra.numeric.addition_value = 5;
  cmd.extra.numeric.init_value = UINT64_MAX;
  assert_false(lru_upsert(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_KEY_NOT_FOUND, lru_val.errcode);

  cmd.extra.numeric.init_value = 10;
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_int_equal(15, lru_val.vallen);

  cmd.req.op = PROTOCOL_BINARY_CMD_GET;
  cmd.req.cas = 0;
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_true(lru_val.is_numeric_val);
  assert_int_equal(15, lru_val.vallen);
  assert_int_equal(4, lru_val.cas);

  lru_cleanup(lru);
  assert_int_equal(0, lru->objcnt);
  assert_int_equal(0, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);
  free(lru);
}

static void
test_add_replace(void **context)
{
  lru_t *lru;
  cmd_handler cmd;
  lru_val_t lru_val;
  lru = lru_init(100, 8, 8);

  cmd.state = ASCII_CMD_READY;
  memcpy(&cmd.buffer, "abc", 3);
  cmd.key = &cmd.buffer[0];
  cmd.buf_used = 3;
  cmd.req.keylen = 3;
  cmd.req.op = PROTOCOL_BINARY_CMD_ADD;
  cmd.value = "5";
  cmd.value_stored = 1;
  cmd.req.cas = 0;

  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  cmd.value = "01";
  cmd.value_stored = 2;
  // add should fail when the key exists
  assert_false(lru_upsert(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_ITEM_NOT_STORED, lru_val.errcode);

  // check stored value is still what we stored initialily.
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_false(lru_val.is_numeric_val);
  assert_int_equal(1, lru_val.vallen);
  assert_int_equal(1, lru_val.cas);
  assert_memory_equal("5", lru_val.value, 1);

  // check global state
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(3, lru->inline_acc_keylen);
  assert_int_equal(1, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);

  // replace on exisiting key should succeed
  cmd.req.op = PROTOCOL_BINARY_CMD_REPLACE;
  cmd.value = "01";
  cmd.value_stored = 2;
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);

  // check stored value replaced to new val
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_false(lru_val.is_numeric_val);
  assert_int_equal(2, lru_val.vallen);
  assert_int_equal(2, lru_val.cas);
  assert_memory_equal("01", lru_val.value, 2);

  // check global state
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(3, lru->inline_acc_keylen);
  assert_int_equal(2, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);

  // replace should fail when the key does not exist
  cmd.req.op = PROTOCOL_BINARY_CMD_REPLACE;
  memcpy(&cmd.buffer, "xyz", 3);
  assert_false(lru_upsert(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_ITEM_NOT_STORED, lru_val.errcode);

  lru_cleanup(lru);
  assert_int_equal(0, lru->objcnt);
  assert_int_equal(0, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);
  free(lru);
}

static void
test_append_prepend(void **context)
{
  lru_t *lru;
  cmd_handler cmd;
  void *tmpval;
  lru_val_t lru_val;
  lru = lru_init(100, 8, 8);

  cmd.state = ASCII_CMD_READY;
  memcpy(&cmd.buffer, "abc", 3);
  cmd.key = &cmd.buffer[0];
  cmd.buf_used = 3;
  cmd.req.keylen = 3;
  cmd.req.op = PROTOCOL_BINARY_CMD_SET;
  cmd.value = "";
  cmd.value_stored = 0;
  cmd.req.cas = 0;
  assert_true(lru_upsert(lru, &cmd, &lru_val));

  cmd.req.op = PROTOCOL_BINARY_CMD_APPEND;
  cmd.value = "456";
  cmd.value_stored = 3;
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_false(lru_val.is_numeric_val);
  assert_int_equal(3, lru_val.vallen);
  assert_int_equal(2, lru_val.cas);
  assert_memory_equal("456", lru_val.value, 3);
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(3, lru->inline_acc_keylen);
  assert_int_equal(3, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);

  cmd.req.op = PROTOCOL_BINARY_CMD_PREPEND;
  cmd.value = "123";
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_false(lru_val.is_numeric_val);
  assert_int_equal(6, lru_val.vallen);
  assert_int_equal(3, lru_val.cas);
  assert_memory_equal("123456", lru_val.value, 6);
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(3, lru->inline_acc_keylen);
  assert_int_equal(6, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);

  cmd.req.op = PROTOCOL_BINARY_CMD_APPEND;
  cmd.value = "789";
  cmd.value_stored = 3;
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_false(lru_val.is_numeric_val);
  assert_int_equal(9, lru_val.vallen);
  assert_int_equal(4, lru_val.cas);
  assert_memory_equal("123456789", lru_val.value, 9);
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(3, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(1, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(9, lru->ninline_vallen);

  tmpval = lru_val.value;
  cmd.req.op = PROTOCOL_BINARY_CMD_PREPEND;
  cmd.value = "000";
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_false(lru_val.is_numeric_val);
  assert_int_equal(12, lru_val.vallen);
  assert_int_equal(5, lru_val.cas);
  assert_memory_equal("000123456789", lru_val.value, 12);
  assert_ptr_not_equal(tmpval, lru_val.value);
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(3, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(1, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(12, lru->ninline_vallen);

  lru_cleanup(lru);
  assert_int_equal(0, lru->objcnt);
  assert_int_equal(0, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);
  free(lru);
}

static void
test_numeric_append_prepend(void **context)
{
  lru_t *lru;
  cmd_handler cmd;
  lru_val_t lru_val;
  lru = lru_init(100, 8, 8);

  memcpy(&cmd.buffer, "xyz", 3);
  cmd.req.op = PROTOCOL_BINARY_CMD_INCREMENT;
  cmd.extra.numeric.addition_value = 0;
  cmd.extra.numeric.init_value = 0;
  cmd.key = &cmd.buffer[0];
  cmd.buf_used = 3;
  cmd.req.keylen = 3;
  cmd.req.cas = 0;
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_true(lru_val.is_numeric_val);
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(3, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);

  // converts to str
  cmd.req.op = PROTOCOL_BINARY_CMD_PREPEND;
  cmd.value = "1";
  cmd.value_stored = 1;
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_false(lru_val.is_numeric_val);
  assert_int_equal(2, lru_val.vallen);
  assert_int_equal(2, lru_val.cas);
  assert_memory_equal("10", lru_val.value, 2);

  // coverts back to numerical
  cmd.req.op = PROTOCOL_BINARY_CMD_INCREMENT;
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_true(lru_val.is_numeric_val);
  assert_int_equal(10, lru_val.vallen);
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(3, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);

  // converts to str
  cmd.req.op = PROTOCOL_BINARY_CMD_APPEND;
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_false(lru_val.is_numeric_val);
  assert_int_equal(3, lru_val.vallen);
  assert_int_equal(4, lru_val.cas);
  assert_memory_equal("101", lru_val.value, 3);
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(3, lru->inline_acc_keylen);
  assert_int_equal(3, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);

  // coverts back to numerical
  cmd.req.op = PROTOCOL_BINARY_CMD_INCREMENT;
  cmd.extra.numeric.addition_value = 10000000;
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_true(lru_val.is_numeric_val);
  assert_int_equal(10000101, lru_val.vallen);
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(3, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);

  // converts to str
  cmd.req.op = PROTOCOL_BINARY_CMD_APPEND;
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_false(lru_val.is_numeric_val);
  assert_int_equal(9, lru_val.vallen);
  assert_int_equal(6, lru_val.cas);
  assert_memory_equal("100001011", lru_val.value, 9);
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(3, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(1, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(9, lru->ninline_vallen);

  // coverts back to numerical
  cmd.req.op = PROTOCOL_BINARY_CMD_INCREMENT;
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_true(lru_val.is_numeric_val);
  assert_int_equal(110001011, lru_val.vallen);
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(3, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);

  // converts to str
  cmd.req.op = PROTOCOL_BINARY_CMD_PREPEND;
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_false(lru_val.is_numeric_val);
  assert_int_equal(10, lru_val.vallen);
  assert_int_equal(8, lru_val.cas);
  assert_memory_equal("1110001011", lru_val.value, 10);
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(3, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(1, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(10, lru->ninline_vallen);

  lru_cleanup(lru);
  assert_int_equal(0, lru->objcnt);
  assert_int_equal(0, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);
  free(lru);
}

static void
test_lru_full(void **context)
{
  lru_t *lru;
  cmd_handler cmd;
  lru_val_t lru_val;
  lru = lru_init(70, 8, 8);

  cmd.state = ASCII_CMD_READY;
  cmd.req.op = PROTOCOL_BINARY_CMD_SET;
  cmd.key = &cmd.buffer[0];
  cmd.buf_used = 3;
  cmd.req.keylen = 3;
  cmd.req.cas = 0;
  cmd.value_stored = 0;

  int i;
  for (i = 0; i < 120; i++)
    {
      sprintf(&cmd.buffer[0], "%03d", i);
      if (!lru_upsert(lru, &cmd, &lru_val))
        break;
    }
  assert_int_not_equal(120, i);
  assert_int_equal(STATUS_BUSY, lru_val.errcode);

  lru_cleanup(lru);
  assert_int_equal(0, lru->objcnt);
  assert_int_equal(0, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);
  free(lru);
}

static void
test_lru_delete(void **context)
{
  lru_t *lru;
  cmd_handler cmd;
  lru_val_t lru_val;
  lru = lru_init(100, 8, 8);

  // short key, short value
  cmd.state = ASCII_CMD_READY;
  cmd.req.op = PROTOCOL_BINARY_CMD_SET;
  memcpy(&cmd.buffer, "xyz", 3);
  cmd.key = &cmd.buffer[0];
  cmd.buf_used = 3;
  cmd.req.keylen = 3;
  cmd.req.cas = 0;
  cmd.value_stored = 3;
  cmd.value = "abc";
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(3, lru->inline_acc_keylen);
  assert_int_equal(3, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);

  // delete short key, short value
  lru_delete(lru, &cmd);
  assert_false(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_KEY_NOT_FOUND, lru_val.errcode);
  assert_int_equal(0, lru->objcnt);
  assert_int_equal(0, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);

  // short key, long value
  cmd.value_stored = 10;
  cmd.value = "0123456789";
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(3, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(1, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(10, lru->ninline_vallen);

  lru_delete(lru, &cmd);
  assert_false(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_KEY_NOT_FOUND, lru_val.errcode);
  assert_int_equal(0, lru->objcnt);
  assert_int_equal(0, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);

  // long key, long value
  memcpy(&cmd.buffer, "0123456789", 10);
  cmd.buf_used = 10;
  cmd.req.keylen = 10;
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(0, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(1, lru->ninline_keycnt);
  assert_int_equal(1, lru->ninline_valcnt);
  assert_int_equal(10, lru->ninline_keylen);
  assert_int_equal(10, lru->ninline_vallen);

  lru_delete(lru, &cmd);
  assert_false(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_KEY_NOT_FOUND, lru_val.errcode);
  assert_int_equal(0, lru->objcnt);
  assert_int_equal(0, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);

  // long key, short value
  cmd.value_stored = 3;
  cmd.value = "abc";
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(0, lru->inline_acc_keylen);
  assert_int_equal(3, lru->inline_acc_vallen);
  assert_int_equal(1, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(10, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);

  lru_delete(lru, &cmd);
  assert_false(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_KEY_NOT_FOUND, lru_val.errcode);
  assert_int_equal(0, lru->objcnt);
  assert_int_equal(0, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);

  // long key, numeric value
  cmd.req.op = PROTOCOL_BINARY_CMD_INCREMENT;
  cmd.extra.numeric.addition_value = 0;
  cmd.extra.numeric.init_value = 12345;
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  assert_true(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_NOERROR, lru_val.errcode);
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(0, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(1, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(10, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);

  lru_delete(lru, &cmd);
  assert_false(lru_get(lru, &cmd, &lru_val));
  assert_int_equal(STATUS_KEY_NOT_FOUND, lru_val.errcode);
  assert_int_equal(0, lru->objcnt);
  assert_int_equal(0, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);

  lru_cleanup(lru);
  assert_int_equal(0, lru->objcnt);
  assert_int_equal(0, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);
  free(lru);
}

static void
test_swiper_pqueue(void **context)
{
  swiper_t *swiper = swiper_init(NULL, 4);
  pq_add(swiper, 0, 3);
  pq_add(swiper, 1, 7);
  pq_add(swiper, 2, 1);
  pq_add(swiper, 3, 5);
  assert_int_equal(4, swiper->pqueue_used);

  assert_int_equal(7, swiper->pqueue[0][1]);
  pq_pop_add(swiper, 4, 2);
  assert_int_equal(5, swiper->pqueue[0][1]);
  pq_pop_add(swiper, 0, 9);
  assert_int_equal(5, swiper->pqueue[0][1]);
  // now the set contains (2,1), (4,2), (0,3), (3,5)
  pq_sort(swiper);
  assert_int_equal(2, swiper->pqueue[0][0]);
  assert_int_equal(1, swiper->pqueue[0][1]);
  assert_int_equal(4, swiper->pqueue[1][0]);
  assert_int_equal(2, swiper->pqueue[1][1]);
  assert_int_equal(0, swiper->pqueue[2][0]);
  assert_int_equal(3, swiper->pqueue[2][1]);
  assert_int_equal(3, swiper->pqueue[3][0]);
  assert_int_equal(5, swiper->pqueue[3][1]);

  free(swiper);
}

static void
test_swiper_epoch(void **context)
{
  lru_t *lru;
  swiper_t *swiper;
  cmd_handler cmd;
  lru_val_t lru_val;
  lru = lru_init(100, 8, 8);
  swiper = swiper_init(lru, 20);

  cmd.state = ASCII_CMD_READY;
  cmd.req.op = PROTOCOL_BINARY_CMD_SET;
  cmd.key = &cmd.buffer[0];
  cmd.buf_used = 3;
  cmd.req.keylen = 3;
  cmd.req.cas = 0;
  cmd.value_stored = 0;
  cmd.extra.twoval.expiration = 0;
  memcpy(&cmd.buffer, "abc", 3);
  assert_true(lru_upsert(lru, &cmd, &lru_val));

  sleep(1);
  // now all epoch is out of date
  lru_swipe(swiper);
  assert_int_equal(0, lru->objcnt);
  assert_int_equal(0, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);

  cmd.extra.twoval.expiration = 900;
  memcpy(&cmd.buffer, "xyz", 3);
  assert_true(lru_upsert(lru, &cmd, &lru_val));

  sleep(1);
  lru_swipe(swiper);
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(3, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);

  lru_cleanup(lru);
  assert_int_equal(0, lru->objcnt);
  assert_int_equal(0, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);
  free(lru);
  free(swiper);
}

static void
test_swiper_txid(void **context)
{
  lru_t *lru;
  swiper_t *swiper;
  cmd_handler cmd;
  lru_val_t lru_val;
  uint64_t objcnt;
  lru = lru_init(70, 8, 8);
  swiper = swiper_init(lru, 40);

  cmd.state = ASCII_CMD_READY;
  cmd.req.op = PROTOCOL_BINARY_CMD_SET;
  cmd.key = &cmd.buffer[0];
  cmd.buf_used = 3;
  cmd.req.keylen = 3;
  cmd.req.cas = 0;
  cmd.value_stored = 0;
  cmd.extra.twoval.expiration = 900;

  for (int i = 0; i < 120; i++)
    {
      sprintf(&cmd.buffer[0], "%03d", i);
      lru_upsert(lru, &cmd, &lru_val);
    }
  objcnt = lru->objcnt;
  printf("before swipe: %" PRIu64 "\n", objcnt);

  lru_swipe(swiper);
  assert_int_not_equal(0, lru->objcnt);
  assert_true(objcnt > lru->objcnt);
  printf("after swipe: %llu\n", lru->objcnt);

  // all earliest keys should get deleted already
  for (int i = 0; i < 10; i++)
    {
      sprintf(&cmd.buffer[0], "%03d", i);
      assert_false(lru_get(lru, &cmd, &lru_val));
      assert_int_equal(STATUS_KEY_NOT_FOUND, lru_val.errcode);
    }

  lru_cleanup(lru);
  assert_int_equal(0, lru->objcnt);
  assert_int_equal(0, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);
  free(lru);
  free(swiper);
}

static void
test_touch(void **context)
{
  lru_t *lru;
  swiper_t *swiper;
  cmd_handler cmd;
  lru_val_t lru_val;
  lru = lru_init(100, 8, 8);
  swiper = swiper_init(lru, 20);

  cmd.state = ASCII_CMD_READY;
  cmd.req.op = PROTOCOL_BINARY_CMD_SET;
  cmd.key = &cmd.buffer[0];
  cmd.buf_used = 3;
  cmd.req.keylen = 3;
  cmd.req.cas = 0;
  cmd.value_stored = 0;
  cmd.extra.twoval.expiration = 0;
  memcpy(&cmd.buffer, "abc", 3);
  assert_true(lru_upsert(lru, &cmd, &lru_val));

  cmd.req.op = PROTOCOL_BINARY_CMD_TOUCH;
  cmd.extra.twoval.expiration = 900;
  assert_true(lru_upsert(lru, &cmd, &lru_val));
  sleep(1);
  lru_swipe(swiper);
  assert_int_equal(1, lru->objcnt);
  assert_int_equal(3, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);

  lru_cleanup(lru);
  assert_int_equal(0, lru->objcnt);
  assert_int_equal(0, lru->inline_acc_keylen);
  assert_int_equal(0, lru->inline_acc_vallen);
  assert_int_equal(0, lru->ninline_keycnt);
  assert_int_equal(0, lru->ninline_valcnt);
  assert_int_equal(0, lru->ninline_keylen);
  assert_int_equal(0, lru->ninline_vallen);
  free(lru);
  free(swiper);
}

int
main(void)
{
  const struct CMUnitTest lru_tests[] = {
    cmocka_unit_test(test_init_cleanup),
    cmocka_unit_test(test_insert_update),
    cmocka_unit_test(test_numeric_val),
    cmocka_unit_test(test_add_replace),
    cmocka_unit_test(test_append_prepend),
    cmocka_unit_test(test_numeric_append_prepend),
    cmocka_unit_test(test_lru_full),
    cmocka_unit_test(test_lru_delete),
    cmocka_unit_test(test_swiper_pqueue),
    cmocka_unit_test(test_swiper_epoch),
    cmocka_unit_test(test_swiper_txid),
    cmocka_unit_test(test_touch),
  };
  return cmocka_run_group_tests(lru_tests, NULL, NULL);
}
