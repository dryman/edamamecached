#ifndef EDAMAME_PARSER_H_
#define EDAMAME_PARSER_H_ 1

#include <stdbool.h>
#include <uv.h>
#include "cmd_protocol.h"

#define CMD_BUF_SIZE 512
#define KEY_MAX_SIZE 250

typedef struct cmd_handler cmd_handler;
typedef enum cmd_state cmd_state;

enum cmd_state
{
  CMD_CLEAN,
  ASCII_PENDING_RAWBUF,
  ASCII_PENDING_PARSE_CMD,
  ASCII_PENDING_GET_MULTI,
  ASCII_PENDING_GET_CAS_MULTI,
  ASCII_PENDING_VALUE,
  ASCII_CMD_READY,
  ASCII_ERROR,
  BINARY_PENDING_RAWBUF,
  BINARY_PENDING_PARSE_CMD,
  BINARY_PENDING_PARSE_EXTRA,
  BINARY_PENDING_PARSE_KEY,
  BINARY_PENDING_VALUE,
  BINARY_CMD_READY,
};

struct cmd_handler
{
  uv_tcp_t tcp;
  cmd_state state;
  char buffer[CMD_BUF_SIZE];
  ssize_t buf_used;
  bool has_pending_newline;
  cmd_extra extra;
  cmd_req_header req;
  // point to cmd_handler.buffer
  char* key;
  bool val_copied;
  char* value;
  size_t value_stored;
};

#endif
