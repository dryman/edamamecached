#include <stdio.h>
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <stdint.h>
#include <sys/mman.h>
#include <string.h>
#include <cmocka.h>
#include <urcu.h>
#include "lru.h"

static void
test_init_cleanup(void** context)
{
  lru_t* lru;
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

int main(void)
{
  const struct CMUnitTest lru_tests[] =
    {
      cmocka_unit_test(test_init_cleanup),
    };
  return cmocka_run_group_tests(lru_tests, NULL, NULL);
}
