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

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/mman.h>
#include <cmocka.h>

#include "cmd_protocol.h"

static void
test_sizes(void **context)
{
  assert_int_equal(1, sizeof(cmd_opcode));
  assert_int_equal(2, sizeof(cmd_rescode));
  assert_int_equal(24, sizeof(cmd_req_header));
  assert_int_equal(24, sizeof(cmd_res_header));
}

static void
test_req_endianess(void **context)
{
  cmd_req_header req;
  req.keylen = 0xaabb;
  req.vbucket = 0xaabb;
  req.bodylen = 0xaabbccdd;
  req.cas = 0x1122334455667788;
  cmd_req_ntoh(&req);
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  assert_int_equal(0xbbaa, req.keylen);
  assert_int_equal(0xbbaa, req.vbucket);
  assert_int_equal(0xddccbbaa, req.bodylen);
  assert_int_equal(0x8877665544332211, req.cas);
#else
  assert_int_equal(0xaabb, req.keylen);
  assert_int_equal(0xaabb, req.vbucket);
  assert_int_equal(0xaabbccdd, req.bodylen);
  assert_int_equal(0x1122334455667788, req.cas);
#endif
}

static void
test_res_endianess(void **context)
{
  cmd_res_header res;
  res.keylen = 0xaabb;
  res.status = 0xaabb; // does this work?
  res.bodylen = 0xaabbccdd;
  res.cas = 0x1122334455667788;
  cmd_res_hton(&res);
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  assert_int_equal(0xbbaa, res.keylen);
  assert_int_equal(0xbbaa, res.status);
  assert_int_equal(0xddccbbaa, res.bodylen);
  assert_int_equal(0x8877665544332211, res.cas);
#else
  assert_int_equal(0xaabb, res.keylen);
  assert_int_equal(0xaabb, res.status);
  assert_int_equal(0xaabbccdd, res.bodylen);
  assert_int_equal(0x1122334455667788, res.cas);
#endif
}

int
main(void)
{
  const struct CMUnitTest cmd_protocol_tests[] = {
    cmocka_unit_test(test_sizes), cmocka_unit_test(test_req_endianess),
    cmocka_unit_test(test_res_endianess),
  };
  return cmocka_run_group_tests(cmd_protocol_tests, NULL, NULL);
}
