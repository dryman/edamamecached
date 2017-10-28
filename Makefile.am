TESTS = cmd_protocol_test cmd_parser_test
check_PROGRAMS = cmd_protocol_test cmd_parser_test
bin_PROGRAMS = edamamecached

cmd_protocol_test_SOURCES = cmd_protocol_test.c cmd_protocol.c
cmd_protocol_test_CFLAGS = @cmocka_CFLAGS@
cmd_protocol_test_LDADD = @cmocka_LIBS@
#cmd_protocol_test_LDFLAGS = -static

cmd_parser_test_SOURCES = \
  cmd_protocol.c \
  cmd_parser.c \
  cmd_parser.h \
  cmd_parser_test.c \
  util.c
cmd_parser_test_CFLAGS = @cmocka_CFLAGS@
cmd_parser_test_LDADD = @cmocka_LIBS@

edamamecached_SOURCES = \
  server.c \
  cmd_protocol.c \
  cmd_protocol.h \
  cmd_parser.c \
  cmd_parser.h \
  writer.c \
  writer.h \
  cmd_reader.c \
  cmd_reader.h \
  util.h \
  util.c

