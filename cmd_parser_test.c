#include "cmd_parser.h"
#include "util.h"
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <cmocka.h>

static void
test_parse_uint(void **context)
{
  uint32_t u32;
  uint64_t u64;
  char str1[] = "  234 ";
  char str2[] = "  abc1";
  char str3[] = "  -1";
  char *ptr;

  ptr = str1;
  assert_true(parse_uint32(&u32, &ptr));
  assert_int_equal(234, u32);
  assert_ptr_equal(&str1[5], ptr);
  ptr = str2;
  assert_false(parse_uint32(&u32, &ptr));
  ptr = str3;
  assert_false(parse_uint32(&u32, &ptr));

  ptr = str1;
  assert_true(parse_uint64(&u64, &ptr));
  assert_int_equal(234, u64);
  assert_ptr_equal(&str1[5], ptr);
  ptr = str2;
  assert_false(parse_uint64(&u64, &ptr));
  ptr = str3;
  assert_false(parse_uint64(&u64, &ptr));
}

static void
test_parse_ascii_value(void **context)
{
  char buf1[] = "01234\r\n789";
  char buf2[] = "0123456\r\n9";
  char buf3[] = "\r\n2345";
  cmd_handler cmd = {};
  cmd.req.bodylen = 5;
  cmd.state = ASCII_PENDING_VALUE;
  assert_int_equal(2, cmd_parse_ascii_value(&cmd, 2, buf1, NULL));
  assert_int_equal(2, cmd.value_stored);
  assert_true(cmd.val_copied);
  assert_int_equal(ASCII_PENDING_VALUE, cmd.state);
  assert_int_equal(5, cmd_parse_ascii_value(&cmd, 8, &buf1[2], NULL));
  assert_int_equal(ASCII_CMD_READY, cmd.state);

  reset_cmd_handler(&cmd);
  cmd.req.bodylen = 5;
  cmd.state = ASCII_PENDING_VALUE;
  assert_int_equal(7, cmd_parse_ascii_value(&cmd, 10, buf1, NULL));
  assert_int_equal(5, cmd.value_stored);
  assert_false(cmd.val_copied);
  assert_int_equal(ASCII_CMD_READY, cmd.state);

  reset_cmd_handler(&cmd);
  cmd.req.bodylen = 5;
  cmd.state = ASCII_PENDING_VALUE;
  assert_int_equal(2, cmd_parse_ascii_value(&cmd, 2, buf2, NULL));
  assert_int_equal(2, cmd.value_stored);
  assert_true(cmd.val_copied);
  assert_int_equal(ASCII_PENDING_VALUE, cmd.state);
  assert_int_equal(5, cmd_parse_ascii_value(&cmd, 8, &buf2[2], NULL));
  assert_true(cmd.skip_until_newline);
  assert_int_equal(2, cmd_parse_ascii_value(&cmd, 3, &buf2[7], NULL));
  assert_int_equal(CMD_CLEAN, cmd.state);

  reset_cmd_handler(&cmd);
  cmd.req.bodylen = 5;
  cmd.state = ASCII_PENDING_VALUE;
  assert_int_equal(7, cmd_parse_ascii_value(&cmd, 10, buf2, NULL));
  assert_true(cmd.skip_until_newline);
  assert_int_equal(2, cmd_parse_ascii_value(&cmd, 3, &buf2[7], NULL));
  assert_int_equal(CMD_CLEAN, cmd.state);

  reset_cmd_handler(&cmd);
  cmd.req.bodylen = 0;
  cmd.state = ASCII_PENDING_VALUE;
  assert_int_equal(2, cmd_parse_ascii_value(&cmd, 5, buf3, NULL));
  assert_int_equal(ASCII_CMD_READY, cmd.state);
}

static void
test_ascii_cpbuf(void **context)
{
  cmd_handler cmd = {};
  cmd.state = CMD_CLEAN;
  char buf1[] = "01234\r\n789";
  char buf2[] = "\r\n 345\r\n89";
  char buf3[] = "\r\n get 7xxx";
  char buf4[] = "\r\n gets 8yyy";

  // test parse stream in two pass
  assert_int_equal(2, ascii_cpbuf(&cmd, 2, buf1, NULL));
  assert_int_equal(2, cmd.buf_used);
  assert_int_equal(ASCII_PENDING_RAWBUF, cmd.state);
  assert_int_equal(5, ascii_cpbuf(&cmd, 8, &buf1[2], NULL));
  assert_int_equal(7, cmd.buf_used);
  assert_memory_equal(cmd.buffer, buf1, 7);
  assert_int_equal(ASCII_PENDING_PARSE_CMD, cmd.state);

  // test parse stream in one pass
  reset_cmd_handler(&cmd);
  assert_int_equal(7, ascii_cpbuf(&cmd, 10, buf1, NULL));
  assert_int_equal(7, cmd.buf_used);
  assert_memory_equal(cmd.buffer, buf1, 7);
  assert_int_equal(ASCII_PENDING_PARSE_CMD, cmd.state);

  // test parse stream in two pass, but cut on \r
  // we need to wait second pass because we can't include
  // \n in value if the following state requires value.
  reset_cmd_handler(&cmd);
  assert_int_equal(6, ascii_cpbuf(&cmd, 6, buf1, NULL));
  assert_int_equal(6, cmd.buf_used);
  assert_int_equal(ASCII_PENDING_RAWBUF, cmd.state);
  assert_int_equal(1, ascii_cpbuf(&cmd, 4, &buf1[6], NULL));
  assert_int_equal(7, cmd.buf_used);
  assert_memory_equal(cmd.buffer, buf1, 7);
  assert_int_equal(ASCII_PENDING_PARSE_CMD, cmd.state);

  // test parse stream with space chars on front
  reset_cmd_handler(&cmd);
  assert_int_equal(4, ascii_cpbuf(&cmd, 4, buf2, NULL));
  assert_int_equal(1, cmd.buf_used);
  assert_int_equal(ASCII_PENDING_RAWBUF, cmd.state);
  assert_int_equal(4, ascii_cpbuf(&cmd, 6, &buf2[4], NULL));
  assert_int_equal(5, cmd.buf_used);
  assert_memory_equal(cmd.buffer, &buf2[3], 5);
  assert_int_equal(ASCII_PENDING_PARSE_CMD, cmd.state);

  reset_cmd_handler(&cmd);
  assert_int_equal(7, ascii_cpbuf(&cmd, 10, buf3, NULL));
  assert_int_equal(ASCII_PENDING_GET_MULTI, cmd.state);

  reset_cmd_handler(&cmd);
  assert_int_equal(8, ascii_cpbuf(&cmd, 10, buf4, NULL));
  assert_int_equal(ASCII_PENDING_GET_CAS_MULTI, cmd.state);
}

static void
test_ascii_parse_cmd_set(void **context)
{
  cmd_handler cmd = {};
  char key[] = "mykey";
  char set_cmd1[] = "set mykey 1 2 3 \r\n";
  char set_cmd2[] = "set mykey 1 2 3 noreply \r\n";
  char set_cmd3[] = "set 0 1 2 3\r\n"; // 0 is valid key!
  char set_cmd4[] = "set mykey -1 2 3\r\n";
  char set_cmd5[] = "set mykey x 2 3\r\n";
  char set_cmd6[] = "set mykey 1 2 3 4\r\n";
  char set_cmd7[] = "set mykey 1 2\r\n";

  // regular set
  strcpy(cmd.buffer, set_cmd1);
  cmd.buf_used = sizeof(set_cmd1) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_SET, cmd.req.op);
  assert_int_equal(5, cmd.req.keylen);
  assert_memory_equal(key, cmd.key, 5);
  assert_int_equal(1, cmd.extra.twoval.flags);
  assert_int_equal(2, cmd.extra.twoval.expiration);
  assert_int_equal(3, cmd.req.bodylen);
  assert_int_equal(ASCII_PENDING_VALUE, cmd.state);

  // set noreply
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, set_cmd2);
  cmd.buf_used = sizeof(set_cmd2) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_SETQ, cmd.req.op);
  assert_int_equal(5, cmd.req.keylen);
  assert_memory_equal(key, cmd.key, 5);
  assert_int_equal(1, cmd.extra.twoval.flags);
  assert_int_equal(2, cmd.extra.twoval.expiration);
  assert_int_equal(3, cmd.req.bodylen);
  assert_int_equal(ASCII_PENDING_VALUE, cmd.state);

  // number is allowed to be used as key
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, set_cmd3);
  cmd.buf_used = sizeof(set_cmd3) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_SET, cmd.req.op);
  assert_int_equal(1, cmd.req.keylen);
  assert_memory_equal("0", cmd.key, 1);
  assert_int_equal(1, cmd.extra.twoval.flags);
  assert_int_equal(2, cmd.extra.twoval.expiration);
  assert_int_equal(3, cmd.req.bodylen);
  assert_int_equal(ASCII_PENDING_VALUE, cmd.state);

  // negative number should yield error
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, set_cmd4);
  cmd.buf_used = sizeof(set_cmd4) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, set_cmd5);
  cmd.buf_used = sizeof(set_cmd5) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, set_cmd6);
  cmd.buf_used = sizeof(set_cmd6) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, set_cmd7);
  cmd.buf_used = sizeof(set_cmd7) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);
}

static void
test_ascii_parse_cmd_add(void **context)
{
  cmd_handler cmd = {};
  char key[] = "mykey";
  char add_cmd1[] = "add mykey 1 2 3 \r\n";
  char add_cmd2[] = "add mykey 1 2 3 noreply \r\n";
  char add_cmd3[] = "add 0 1 2 3\r\n"; // 0 is valid key!
  char add_cmd4[] = "add mykey -1 2 3\r\n";
  char add_cmd5[] = "add mykey x 2 3\r\n";
  char add_cmd6[] = "add mykey 1 2 3 4\r\n";
  char add_cmd7[] = "add mykey 1 2\r\n";

  // regular add
  strcpy(cmd.buffer, add_cmd1);
  cmd.buf_used = sizeof(add_cmd1) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_ADD, cmd.req.op);
  assert_int_equal(5, cmd.req.keylen);
  assert_memory_equal(key, cmd.key, 5);
  assert_int_equal(1, cmd.extra.twoval.flags);
  assert_int_equal(2, cmd.extra.twoval.expiration);
  assert_int_equal(3, cmd.req.bodylen);
  assert_int_equal(ASCII_PENDING_VALUE, cmd.state);

  // add noreply
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, add_cmd2);
  cmd.buf_used = sizeof(add_cmd2) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_ADDQ, cmd.req.op);
  assert_int_equal(5, cmd.req.keylen);
  assert_memory_equal(key, cmd.key, 5);
  assert_int_equal(1, cmd.extra.twoval.flags);
  assert_int_equal(2, cmd.extra.twoval.expiration);
  assert_int_equal(3, cmd.req.bodylen);
  assert_int_equal(ASCII_PENDING_VALUE, cmd.state);

  // number is allowed to be used as key
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, add_cmd3);
  cmd.buf_used = sizeof(add_cmd3) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_ADD, cmd.req.op);
  assert_int_equal(1, cmd.req.keylen);
  assert_memory_equal("0", cmd.key, 1);
  assert_int_equal(1, cmd.extra.twoval.flags);
  assert_int_equal(2, cmd.extra.twoval.expiration);
  assert_int_equal(3, cmd.req.bodylen);
  assert_int_equal(ASCII_PENDING_VALUE, cmd.state);

  // negative number should yield error
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, add_cmd4);
  cmd.buf_used = sizeof(add_cmd4) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, add_cmd5);
  cmd.buf_used = sizeof(add_cmd5) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, add_cmd6);
  cmd.buf_used = sizeof(add_cmd6) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, add_cmd7);
  cmd.buf_used = sizeof(add_cmd7) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);
}

static void
test_ascii_parse_cmd_replace(void **context)
{
  cmd_handler cmd = {};
  char key[] = "mykey";
  char replace_cmd1[] = "replace mykey 1 2 3 \r\n";
  char replace_cmd2[] = "replace mykey 1 2 3 noreply\r\n";
  char replace_cmd3[] = "replace 0 1 2 3\r\n"; // 0 is valid key!
  char replace_cmd4[] = "replace mykey -1 2 3\r\n";
  char replace_cmd5[] = "replace mykey x 2 3\r\n";
  char replace_cmd6[] = "replace mykey 1 2 3 4\r\n";
  char replace_cmd7[] = "replace mykey 1 2\r\n";

  // regular replace
  strcpy(cmd.buffer, replace_cmd1);
  cmd.buf_used = sizeof(replace_cmd1) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_REPLACE, cmd.req.op);
  assert_int_equal(5, cmd.req.keylen);
  assert_memory_equal(key, cmd.key, 5);
  assert_int_equal(1, cmd.extra.twoval.flags);
  assert_int_equal(2, cmd.extra.twoval.expiration);
  assert_int_equal(3, cmd.req.bodylen);
  assert_int_equal(ASCII_PENDING_VALUE, cmd.state);

  // replace noreply
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, replace_cmd2);
  cmd.buf_used = sizeof(replace_cmd2) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_REPLACEQ, cmd.req.op);
  assert_int_equal(5, cmd.req.keylen);
  assert_memory_equal(key, cmd.key, 5);
  assert_int_equal(1, cmd.extra.twoval.flags);
  assert_int_equal(2, cmd.extra.twoval.expiration);
  assert_int_equal(3, cmd.req.bodylen);
  assert_int_equal(ASCII_PENDING_VALUE, cmd.state);

  // number is allowed to be used as key
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, replace_cmd3);
  cmd.buf_used = sizeof(replace_cmd3) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_REPLACE, cmd.req.op);
  assert_int_equal(1, cmd.req.keylen);
  assert_memory_equal("0", cmd.key, 1);
  assert_int_equal(1, cmd.extra.twoval.flags);
  assert_int_equal(2, cmd.extra.twoval.expiration);
  assert_int_equal(3, cmd.req.bodylen);
  assert_int_equal(ASCII_PENDING_VALUE, cmd.state);

  // negative number should yield error
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, replace_cmd4);
  cmd.buf_used = sizeof(replace_cmd4) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, replace_cmd5);
  cmd.buf_used = sizeof(replace_cmd5) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, replace_cmd6);
  cmd.buf_used = sizeof(replace_cmd6) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, replace_cmd7);
  cmd.buf_used = sizeof(replace_cmd7) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);
}

static void
test_ascii_parse_cmd_append(void **context)
{
  cmd_handler cmd = {};
  char key[] = "mykey";
  char append_cmd1[] = "append mykey 1 2 3 \r\n";
  char append_cmd2[] = "append mykey 1 2 3 noreply\r\n";
  char append_cmd3[] = "append 0 1 2 3\r\n"; // 0 is valid key!
  char append_cmd4[] = "append mykey -1 2 3\r\n";
  char append_cmd5[] = "append mykey x 2 3\r\n";
  char append_cmd6[] = "append mykey 1 2 3 4\r\n";
  char append_cmd7[] = "append mykey 1 2\r\n";

  // regular append
  strcpy(cmd.buffer, append_cmd1);
  cmd.buf_used = sizeof(append_cmd1) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_APPEND, cmd.req.op);
  assert_int_equal(5, cmd.req.keylen);
  assert_memory_equal(key, cmd.key, 5);
  assert_int_equal(1, cmd.extra.twoval.flags);
  assert_int_equal(2, cmd.extra.twoval.expiration);
  assert_int_equal(3, cmd.req.bodylen);
  assert_int_equal(ASCII_PENDING_VALUE, cmd.state);

  // append noreply
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, append_cmd2);
  cmd.buf_used = sizeof(append_cmd2) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_APPENDQ, cmd.req.op);
  assert_int_equal(5, cmd.req.keylen);
  assert_memory_equal(key, cmd.key, 5);
  assert_int_equal(1, cmd.extra.twoval.flags);
  assert_int_equal(2, cmd.extra.twoval.expiration);
  assert_int_equal(3, cmd.req.bodylen);
  assert_int_equal(ASCII_PENDING_VALUE, cmd.state);

  // number is allowed to be used as key
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, append_cmd3);
  cmd.buf_used = sizeof(append_cmd3) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_APPEND, cmd.req.op);
  assert_int_equal(1, cmd.req.keylen);
  assert_memory_equal("0", cmd.key, 1);
  assert_int_equal(1, cmd.extra.twoval.flags);
  assert_int_equal(2, cmd.extra.twoval.expiration);
  assert_int_equal(3, cmd.req.bodylen);
  assert_int_equal(ASCII_PENDING_VALUE, cmd.state);

  // negative number should yield error
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, append_cmd4);
  cmd.buf_used = sizeof(append_cmd4) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, append_cmd5);
  cmd.buf_used = sizeof(append_cmd5) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, append_cmd6);
  cmd.buf_used = sizeof(append_cmd6) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, append_cmd7);
  cmd.buf_used = sizeof(append_cmd7) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);
}

static void
test_ascii_parse_cmd_prepend(void **context)
{
  cmd_handler cmd = {};
  char key[] = "mykey";
  char prepend_cmd1[] = "prepend mykey 1 2 3 \r\n";
  char prepend_cmd2[] = "prepend mykey 1 2 3 noreply \r\n";
  char prepend_cmd3[] = "prepend 0 1 2 3\r\n"; // 0 is valid key!
  char prepend_cmd4[] = "prepend mykey -1 2 3\r\n";
  char prepend_cmd5[] = "prepend mykey x 2 3\r\n";
  char prepend_cmd6[] = "prepend mykey 1 2 3 4\r\n";
  char prepend_cmd7[] = "prepend mykey 1 2\r\n";

  // regular prepend
  strcpy(cmd.buffer, prepend_cmd1);
  cmd.buf_used = sizeof(prepend_cmd1) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_PREPEND, cmd.req.op);
  assert_int_equal(5, cmd.req.keylen);
  assert_memory_equal(key, cmd.key, 5);
  assert_int_equal(1, cmd.extra.twoval.flags);
  assert_int_equal(2, cmd.extra.twoval.expiration);
  assert_int_equal(3, cmd.req.bodylen);
  assert_int_equal(ASCII_PENDING_VALUE, cmd.state);

  // prepend noreply
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, prepend_cmd2);
  cmd.buf_used = sizeof(prepend_cmd2) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_PREPENDQ, cmd.req.op);
  assert_int_equal(5, cmd.req.keylen);
  assert_memory_equal(key, cmd.key, 5);
  assert_int_equal(1, cmd.extra.twoval.flags);
  assert_int_equal(2, cmd.extra.twoval.expiration);
  assert_int_equal(3, cmd.req.bodylen);
  assert_int_equal(ASCII_PENDING_VALUE, cmd.state);

  // number is allowed to be used as key
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, prepend_cmd3);
  cmd.buf_used = sizeof(prepend_cmd3) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_PREPEND, cmd.req.op);
  assert_int_equal(1, cmd.req.keylen);
  assert_memory_equal("0", cmd.key, 1);
  assert_int_equal(1, cmd.extra.twoval.flags);
  assert_int_equal(2, cmd.extra.twoval.expiration);
  assert_int_equal(3, cmd.req.bodylen);
  assert_int_equal(ASCII_PENDING_VALUE, cmd.state);

  // negative number should yield error
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, prepend_cmd4);
  cmd.buf_used = sizeof(prepend_cmd4) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, prepend_cmd5);
  cmd.buf_used = sizeof(prepend_cmd5) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, prepend_cmd6);
  cmd.buf_used = sizeof(prepend_cmd6) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, prepend_cmd7);
  cmd.buf_used = sizeof(prepend_cmd7) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);
}

static void
test_ascii_parse_cmd_cas(void **context)
{
  cmd_handler cmd = {};
  char key[] = "mykey";
  char cas_cmd1[] = "cas mykey 1 2 3 4 \r\n";
  char cas_cmd2[] = "cas mykey 1 2 3 4 noreply \r\n";
  char cas_cmd3[] = "cas 0 1 2 3 4\r\n"; // 0 is valid key!
  char cas_cmd4[] = "cas mykey -1 2 3 4\r\n";
  char cas_cmd5[] = "cas mykey x 2 3 4\r\n";
  char cas_cmd6[] = "cas mykey 1 2 3 4 5 \r\n";
  char cas_cmd7[] = "cas mykey 1 2\r\n";

  // regular cas
  strcpy(cmd.buffer, cas_cmd1);
  cmd.buf_used = sizeof(cas_cmd1) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_SET, cmd.req.op);
  assert_int_equal(5, cmd.req.keylen);
  assert_memory_equal(key, cmd.key, 5);
  assert_int_equal(1, cmd.extra.twoval.flags);
  assert_int_equal(2, cmd.extra.twoval.expiration);
  assert_int_equal(3, cmd.req.bodylen);
  assert_int_equal(4, cmd.req.cas);
  assert_int_equal(ASCII_PENDING_VALUE, cmd.state);

  // cas noreply
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, cas_cmd2);
  cmd.buf_used = sizeof(cas_cmd2) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_SETQ, cmd.req.op);
  assert_int_equal(5, cmd.req.keylen);
  assert_memory_equal(key, cmd.key, 5);
  assert_int_equal(1, cmd.extra.twoval.flags);
  assert_int_equal(2, cmd.extra.twoval.expiration);
  assert_int_equal(3, cmd.req.bodylen);
  assert_int_equal(4, cmd.req.cas);
  assert_int_equal(ASCII_PENDING_VALUE, cmd.state);

  // number is allowed to be used as key
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, cas_cmd3);
  cmd.buf_used = sizeof(cas_cmd3) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_SET, cmd.req.op);
  assert_int_equal(1, cmd.req.keylen);
  assert_memory_equal("0", cmd.key, 1);
  assert_int_equal(1, cmd.extra.twoval.flags);
  assert_int_equal(2, cmd.extra.twoval.expiration);
  assert_int_equal(3, cmd.req.bodylen);
  assert_int_equal(4, cmd.req.cas);
  assert_int_equal(ASCII_PENDING_VALUE, cmd.state);

  // negative number should yield error
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, cas_cmd4);
  cmd.buf_used = sizeof(cas_cmd4) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, cas_cmd5);
  cmd.buf_used = sizeof(cas_cmd5) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, cas_cmd6);
  cmd.buf_used = sizeof(cas_cmd6) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, cas_cmd7);
  cmd.buf_used = sizeof(cas_cmd7) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);
}

static void
test_ascii_parse_cmd_delete(void **context)
{
  cmd_handler cmd = {};
  char key[] = "mykey";
  char delete_cmd1[] = "delete mykey  \r\n";
  char delete_cmd2[] = "delete mykey  noreply \r\n";
  char delete_cmd3[] = "delete 0 \r\n"; // 0 is valid key!
  char delete_cmd4[] = "delete mykey 1 \r\n";
  char delete_cmd5[] = "delete mykey noreply x\r\n";

  // regular delete
  strcpy(cmd.buffer, delete_cmd1);
  cmd.buf_used = sizeof(delete_cmd1) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_DELETE, cmd.req.op);
  assert_int_equal(5, cmd.req.keylen);
  assert_memory_equal(key, cmd.key, 5);

  // delete noreply
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, delete_cmd2);
  cmd.buf_used = sizeof(delete_cmd2) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_DELETEQ, cmd.req.op);
  assert_int_equal(5, cmd.req.keylen);
  assert_memory_equal(key, cmd.key, 5);
  assert_int_equal(ASCII_CMD_READY, cmd.state);

  // number is allowed to be used as key
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, delete_cmd3);
  cmd.buf_used = sizeof(delete_cmd3) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_DELETE, cmd.req.op);
  assert_int_equal(1, cmd.req.keylen);
  assert_memory_equal("0", cmd.key, 1);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, delete_cmd4);
  cmd.buf_used = sizeof(delete_cmd4) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, delete_cmd5);
  cmd.buf_used = sizeof(delete_cmd5) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);
}

static void
test_ascii_parse_cmd_incr(void **context)
{
  cmd_handler cmd = {};
  char key[] = "mykey";
  char incr_cmd1[] = "incr mykey 1 \r\n";
  char incr_cmd2[] = "incr mykey 1 noreply \r\n";
  char incr_cmd3[] = "incr 0 1 \r\n"; // 0 is valid key!
  char incr_cmd4[] = "incr mykey -1 \r\n";
  char incr_cmd5[] = "incr mykey x \r\n";
  char incr_cmd6[] = "incr mykey 1 2 \r\n";
  char incr_cmd7[] = "incr mykey \r\n";

  // regular incr
  strcpy(cmd.buffer, incr_cmd1);
  cmd.buf_used = sizeof(incr_cmd1) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_INCREMENT, cmd.req.op);
  assert_int_equal(5, cmd.req.keylen);
  assert_memory_equal(key, cmd.key, 5);
  assert_int_equal(1, cmd.extra.numeric.addition_value);
  assert_int_equal(ASCII_CMD_READY, cmd.state);

  // incr noreply
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, incr_cmd2);
  cmd.buf_used = sizeof(incr_cmd2) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_INCREMENTQ, cmd.req.op);
  assert_int_equal(5, cmd.req.keylen);
  assert_memory_equal(key, cmd.key, 5);
  assert_int_equal(1, cmd.extra.numeric.addition_value);
  assert_int_equal(ASCII_CMD_READY, cmd.state);

  // number is allowed to be used as key
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, incr_cmd3);
  cmd.buf_used = sizeof(incr_cmd3) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_INCREMENT, cmd.req.op);
  assert_int_equal(1, cmd.req.keylen);
  assert_memory_equal("0", cmd.key, 1);
  assert_int_equal(1, cmd.extra.numeric.addition_value);
  assert_int_equal(ASCII_CMD_READY, cmd.state);

  // negative number should yield error
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, incr_cmd4);
  cmd.buf_used = sizeof(incr_cmd4) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, incr_cmd5);
  cmd.buf_used = sizeof(incr_cmd5) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, incr_cmd6);
  cmd.buf_used = sizeof(incr_cmd6) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, incr_cmd7);
  cmd.buf_used = sizeof(incr_cmd7) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);
}

static void
test_ascii_parse_cmd_decr(void **context)
{
  cmd_handler cmd = {};
  char key[] = "mykey";
  char decr_cmd1[] = "decr mykey 1 \r\n";
  char decr_cmd2[] = "decr mykey 1 noreply \r\n";
  char decr_cmd3[] = "decr 0 1 \r\n"; // 0 is valid key!
  char decr_cmd4[] = "decr mykey -1 \r\n";
  char decr_cmd5[] = "decr mykey x \r\n";
  char decr_cmd6[] = "decr mykey 1 2 \r\n";
  char decr_cmd7[] = "decr mykey \r\n";

  // regular decr
  strcpy(cmd.buffer, decr_cmd1);
  cmd.buf_used = sizeof(decr_cmd1) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_DECREMENT, cmd.req.op);
  assert_int_equal(5, cmd.req.keylen);
  assert_memory_equal(key, cmd.key, 5);
  assert_int_equal(1, cmd.extra.numeric.addition_value);
  assert_int_equal(ASCII_CMD_READY, cmd.state);

  // decr noreply
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, decr_cmd2);
  cmd.buf_used = sizeof(decr_cmd2) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_DECREMENTQ, cmd.req.op);
  assert_int_equal(5, cmd.req.keylen);
  assert_memory_equal(key, cmd.key, 5);
  assert_int_equal(1, cmd.extra.numeric.addition_value);
  assert_int_equal(ASCII_CMD_READY, cmd.state);

  // number is allowed to be used as key
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, decr_cmd3);
  cmd.buf_used = sizeof(decr_cmd3) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_DECREMENT, cmd.req.op);
  assert_int_equal(1, cmd.req.keylen);
  assert_memory_equal("0", cmd.key, 1);
  assert_int_equal(1, cmd.extra.numeric.addition_value);
  assert_int_equal(ASCII_CMD_READY, cmd.state);

  // negative number should yield error
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, decr_cmd4);
  cmd.buf_used = sizeof(decr_cmd4) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, decr_cmd5);
  cmd.buf_used = sizeof(decr_cmd5) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, decr_cmd6);
  cmd.buf_used = sizeof(decr_cmd6) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, decr_cmd7);
  cmd.buf_used = sizeof(decr_cmd7) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);
}

static void
test_ascii_parse_cmd_touch(void **context)
{
  cmd_handler cmd = {};
  char key[] = "mykey";
  char touch_cmd1[] = "touch mykey 1 \r\n";
  char touch_cmd2[] = "touch mykey 1 noreply \r\n";
  char touch_cmd3[] = "touch 0 1 \r\n"; // 0 is valid key!
  char touch_cmd4[] = "touch mykey -1 \r\n";
  char touch_cmd5[] = "touch mykey x \r\n";
  char touch_cmd6[] = "touch mykey 1 2 \r\n";
  char touch_cmd7[] = "touch mykey \r\n";

  // regular touch
  strcpy(cmd.buffer, touch_cmd1);
  cmd.buf_used = sizeof(touch_cmd1) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_TOUCH, cmd.req.op);
  assert_int_equal(5, cmd.req.keylen);
  assert_memory_equal(key, cmd.key, 5);
  assert_int_equal(1, cmd.extra.oneval.expiration);
  assert_int_equal(ASCII_CMD_READY, cmd.state);

  // touch noreply
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, touch_cmd2);
  cmd.buf_used = sizeof(touch_cmd2) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_TOUCHQ, cmd.req.op);
  assert_int_equal(5, cmd.req.keylen);
  assert_memory_equal(key, cmd.key, 5);
  assert_int_equal(1, cmd.extra.oneval.expiration);
  assert_int_equal(ASCII_CMD_READY, cmd.state);

  // number is allowed to be used as key
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, touch_cmd3);
  cmd.buf_used = sizeof(touch_cmd3) - 1;
  ascii_parse_cmd(&cmd, NULL);
  assert_int_equal(PROTOCOL_BINARY_CMD_TOUCH, cmd.req.op);
  assert_int_equal(1, cmd.req.keylen);
  assert_memory_equal("0", cmd.key, 1);
  assert_int_equal(1, cmd.extra.oneval.expiration);
  assert_int_equal(ASCII_CMD_READY, cmd.state);

  // negative number should yield error
  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, touch_cmd4);
  cmd.buf_used = sizeof(touch_cmd4) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, touch_cmd5);
  cmd.buf_used = sizeof(touch_cmd5) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, touch_cmd6);
  cmd.buf_used = sizeof(touch_cmd6) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);

  reset_cmd_handler(&cmd);
  strcpy(cmd.buffer, touch_cmd7);
  cmd.buf_used = sizeof(touch_cmd7) - 1;
  ascii_parse_cmd(&cmd, NULL);
  // assert_int_equal(ASCII_ERROR, cmd.state);
}

void
process_cmd_get(cmd_handler *cmd, ed_writer *writer)
{
  // do nothing for now
  printf("process_cmd_get called\n");
}

void
writer_init(ed_writer *writer, size_t size)
{
}
bool
writer_reserve(ed_writer *writer, size_t nbyte)
{
  return true;
}
bool
writer_append(ed_writer *writer, const void *buf, size_t nbyte)
{
  return true;
}
bool
writer_flush(ed_writer *writer, int fd)
{
  return true;
}

static void
test_cmd_parse_get(void **context)
{
  char buf1[] = "012 456 890\r\n";
  cmd_handler cmd = {};
  cmd.state = ASCII_PENDING_GET_MULTI;
  assert_int_equal(3, cmd_parse_get(&cmd, 4, buf1, NULL));
  assert_int_equal(ASCII_PENDING_GET_MULTI, cmd.state);
  assert_ptr_equal(&buf1[0], cmd.key);
  assert_int_equal(3, cmd.req.keylen);

  assert_int_equal(3, cmd_parse_get(&cmd, 3, &buf1[3], NULL));
  assert_int_equal(2, cmd.buf_used);
  assert_memory_equal(cmd.buffer, &buf1[4], 2);
  assert_int_equal(ASCII_PENDING_GET_MULTI, cmd.state);

  assert_int_equal(1, cmd_parse_get(&cmd, 7, &buf1[6], NULL));
  assert_int_equal(3, cmd.req.keylen);
  assert_memory_equal(&buf1[4], cmd.key, 3);
  assert_int_equal(0, cmd.buf_used);
  assert_int_equal(ASCII_PENDING_GET_MULTI, cmd.state);

  assert_int_equal(5, cmd_parse_get(&cmd, 6, &buf1[7], NULL));
  assert_ptr_equal(&buf1[8], cmd.key);
  assert_int_equal(3, cmd.req.keylen);
  assert_int_equal(CMD_CLEAN, cmd.state);
}

int
main(void)
{
  const struct CMUnitTest cmd_parser_tests[] = {
    cmocka_unit_test(test_parse_uint),
    cmocka_unit_test(test_parse_ascii_value),
    cmocka_unit_test(test_ascii_cpbuf),
    cmocka_unit_test(test_ascii_parse_cmd_set),
    cmocka_unit_test(test_ascii_parse_cmd_add),
    cmocka_unit_test(test_ascii_parse_cmd_replace),
    cmocka_unit_test(test_ascii_parse_cmd_append),
    cmocka_unit_test(test_ascii_parse_cmd_prepend),
    cmocka_unit_test(test_ascii_parse_cmd_cas),
    cmocka_unit_test(test_ascii_parse_cmd_delete),
    cmocka_unit_test(test_ascii_parse_cmd_incr),
    cmocka_unit_test(test_ascii_parse_cmd_decr),
    cmocka_unit_test(test_ascii_parse_cmd_touch),
    cmocka_unit_test(test_cmd_parse_get),
  };
  return cmocka_run_group_tests(cmd_parser_tests, NULL, NULL);
}
