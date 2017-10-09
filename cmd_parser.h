#ifndef EDAMAME_PARSER_H_
#define EDAMAME_PARSER_H_ 1

#include <unistd.h>
#include <stddef.h>
#include <stdbool.h>
#include "cmd_protocol.h"

#define CMD_BUF_SIZE 512
#define KEY_MAX_SIZE 250

typedef struct cmd_handler cmd_handler;
typedef enum cmd_state cmd_state;

void reset_cmd_handler(cmd_handler* cmd);
ssize_t ascii_cmd_error(cmd_handler* cmd, ssize_t nbyte, char* buf);
ssize_t ascii_cpbuf(cmd_handler* cmd, ssize_t nbyte, char* buf);
bool parse_uint32(uint32_t* dest, char** iter);
bool parse_uint64(uint64_t* dest, char** iter);
void ascii_parse_cmd(cmd_handler* cmd);
ssize_t cmd_parse_ascii_value(cmd_handler* cmd, ssize_t nbyte, char* buf);
ssize_t cmd_parse_get(cmd_handler* cmd, ssize_t nbyte, char* buf);
void process_cmd_get(cmd_handler* cmd);
ssize_t binary_cpbuf(cmd_handler* cmd, ssize_t nbyte, char* buf);
ssize_t binary_cmd_parse_extra(cmd_handler* cmd, ssize_t nbyte, char* buf);
ssize_t binary_cmd_parse_key(cmd_handler* cmd, ssize_t nbyte, char* buf);
ssize_t binary_cmd_parse_value(cmd_handler* cmd, ssize_t nbyte, char* buf);

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
  BINARY_PENDING_PARSE_EXTRA,
  BINARY_PENDING_PARSE_KEY,
  BINARY_PENDING_VALUE,
  BINARY_CMD_READY,
};

struct cmd_handler
{
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
