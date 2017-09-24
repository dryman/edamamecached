#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <stdint.h>
#include <sys/mman.h>
#include <string.h>
#include <cmocka.h>

#include "cmd_protocol.h"

static void
test_sizes(void** context)
{
  assert_int_equal(1, sizeof(cmd_opcode));
  assert_int_equal(2, sizeof(cmd_rescode));
  assert_int_equal(24, sizeof(cmd_req_header));
  assert_int_equal(24, sizeof(cmd_res_header));
}

static void
test_req_endianess(void** context)
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
test_res_endianess(void** context)
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

int main(void)
{
  const struct CMUnitTest cmd_protocol_tests[] =
    {
      cmocka_unit_test(test_sizes),
      cmocka_unit_test(test_req_endianess),
      cmocka_unit_test(test_res_endianess),
    };
  return cmocka_run_group_tests(cmd_protocol_tests, NULL, NULL);
}