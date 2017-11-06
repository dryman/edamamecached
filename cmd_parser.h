#ifndef EDAMAME_PARSER_H_
#define EDAMAME_PARSER_H_ 1

#include "cmd_protocol.h"
#include "writer.h"
#include <stdbool.h>
#include <stddef.h>
#include <unistd.h>

#define CMD_BUF_SIZE 512
#define KEY_MAX_SIZE 250

typedef struct cmd_handler cmd_handler;
typedef enum cmd_state cmd_state;

bool parse_uint32(uint32_t *dest, char **iter);
bool parse_uint64(uint64_t *dest, char **iter);

void reset_cmd_handler(cmd_handler *cmd);
ssize_t ascii_cmd_error(cmd_handler *cmd, ssize_t nbyte, char *buf);
ssize_t ascii_cpbuf(cmd_handler *cmd, ssize_t nbyte, char *buf,
                    ed_writer *writer);
void ascii_parse_cmd(cmd_handler *cmd, ed_writer *writer);
ssize_t cmd_parse_ascii_value(cmd_handler *cmd, ssize_t nbyte, char *buf,
                              ed_writer *writer);
ssize_t cmd_parse_get(cmd_handler *cmd, ssize_t nbyte, char *buf,
                      ed_writer *writer);
void process_cmd_get(cmd_handler *cmd, ed_writer *writer);
ssize_t binary_cpbuf(cmd_handler *cmd, ssize_t nbyte, char *buf,
                     ed_writer *writer);
ssize_t binary_cmd_parse_extra(cmd_handler *cmd, ssize_t nbyte, char *buf,
                               ed_writer *writer);
ssize_t binary_cmd_parse_key(cmd_handler *cmd, ssize_t nbyte, char *buf,
                             ed_writer *writer);
ssize_t binary_cmd_parse_value(cmd_handler *cmd, ssize_t nbyte, char *buf,
                               ed_writer *writer);

enum cmd_state
{
  CMD_CLEAN = 0,
  ASCII_PENDING_RAWBUF = 1,
  ASCII_PENDING_PARSE_CMD = 2,
  ASCII_PENDING_GET_MULTI = 3,
  ASCII_PENDING_GET_CAS_MULTI = 4,
  ASCII_PENDING_VALUE = 5,
  ASCII_CMD_READY = 6,
  BINARY_PENDING_RAWBUF = 8,
  BINARY_PENDING_PARSE_EXTRA = 9,
  BINARY_PENDING_PARSE_KEY = 10,
  BINARY_PENDING_VALUE = 11,
  BINARY_CMD_READY = 12,
};

struct cmd_handler
{
  cmd_state state;
  char buffer[CMD_BUF_SIZE];
  ssize_t buf_used;
  bool skip_until_newline;
  cmd_extra extra;
  cmd_req_header req;
  // point to cmd_handler.buffer
  char *key;
  bool val_copied;
  char *value;
  size_t value_stored;
};

#endif
