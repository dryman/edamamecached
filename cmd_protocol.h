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

#ifndef EDAMAME_CMD_PROTOCOL_H_
#define EDAMAME_CMD_PROTOCOL_H_ 1

#include <stddef.h>
#include <stdint.h>

typedef union cmd_extra cmd_extra;
typedef enum cmd_opcode cmd_opcode;
typedef enum cmd_rescode cmd_rescode;
typedef struct cmd_req_header cmd_req_header;
typedef struct cmd_res_header cmd_res_header;

void cmd_req_ntoh(cmd_req_header *req);
void cmd_res_hton(cmd_res_header *res);

/*
 * Multibytes value endianess rules:
 * a. When receiving from and sending to wire, values are in network byte
 *    order (big endian).
 * b. Before using the strut/union in memory, we iterate over each multibyte
 *    value and convert it to host byte order.
 * c. When constructing the struct/union from parsing ascii protocol, we use
 *    host byte order.
 */

enum cmd_rescode
{
  PROTOCOL_BINARY_RESPONSE_SUCCESS = 0x00,
  PROTOCOL_BINARY_RESPONSE_KEY_ENOENT = 0x01,
  PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS = 0x02,
  PROTOCOL_BINARY_RESPONSE_E2BIG = 0x03,
  PROTOCOL_BINARY_RESPONSE_EINVAL = 0x04,
  PROTOCOL_BINARY_RESPONSE_NOT_STORED = 0x05,
  PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL = 0x06,
  PROTOCOL_BINARY_RESPONSE_AUTH_ERROR = 0x20,
  PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE = 0x21,
  PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND = 0x81,
  PROTOCOL_BINARY_RESPONSE_ENOMEM = 0x82,
  // force rescode to be 2 bytes
  PROTOCOL_BINARY_RESPONSE_RESERVED = 0xffff,
} __attribute__((packed));

enum cmd_opcode
{
  PROTOCOL_BINARY_CMD_GET = 0x00,
  PROTOCOL_BINARY_CMD_SET = 0x01,
  PROTOCOL_BINARY_CMD_ADD = 0x02,
  PROTOCOL_BINARY_CMD_REPLACE = 0x03,
  PROTOCOL_BINARY_CMD_DELETE = 0x04,
  PROTOCOL_BINARY_CMD_INCREMENT = 0x05,
  PROTOCOL_BINARY_CMD_DECREMENT = 0x06,
  PROTOCOL_BINARY_CMD_QUIT = 0x07,
  PROTOCOL_BINARY_CMD_FLUSH = 0x08,
  PROTOCOL_BINARY_CMD_GETQ = 0x09,
  PROTOCOL_BINARY_CMD_NOOP = 0x0a,
  PROTOCOL_BINARY_CMD_VERSION = 0x0b,
  PROTOCOL_BINARY_CMD_GETK = 0x0c,
  PROTOCOL_BINARY_CMD_GETKQ = 0x0d,
  PROTOCOL_BINARY_CMD_APPEND = 0x0e,
  PROTOCOL_BINARY_CMD_PREPEND = 0x0f,
  PROTOCOL_BINARY_CMD_STAT = 0x10,
  PROTOCOL_BINARY_CMD_SETQ = 0x11,
  PROTOCOL_BINARY_CMD_ADDQ = 0x12,
  PROTOCOL_BINARY_CMD_REPLACEQ = 0x13,
  PROTOCOL_BINARY_CMD_DELETEQ = 0x14,
  PROTOCOL_BINARY_CMD_INCREMENTQ = 0x15,
  PROTOCOL_BINARY_CMD_DECREMENTQ = 0x16,
  PROTOCOL_BINARY_CMD_QUITQ = 0x17,
  PROTOCOL_BINARY_CMD_FLUSHQ = 0x18,
  PROTOCOL_BINARY_CMD_APPENDQ = 0x19,
  PROTOCOL_BINARY_CMD_PREPENDQ = 0x1a,
  PROTOCOL_BINARY_CMD_TOUCH = 0x1c,
  PROTOCOL_BINARY_CMD_GAT = 0x1d,
  PROTOCOL_BINARY_CMD_GATQ = 0x1e,
  PROTOCOL_BINARY_CMD_GATK = 0x23,
  PROTOCOL_BINARY_CMD_GATKQ = 0x24,
  // edamame specific
  PROTOCOL_BINARY_CMD_TOUCHQ = 0x1f,
} __attribute__((packed));

enum cmd_errcode
{
  STATUS_NOERROR = 0x0000,
  STATUS_KEY_NOT_FOUND = 0x001,
  STATUS_KEY_EXISTS = 0x002,
  STATUS_VAL_TOO_LARGE = 0x003,
  STATUS_INVALID_ARG = 0x0004,
  STATUS_ITEM_NOT_STORED = 0x0005,
  STATUS_NON_NUMERIC = 0x0006,
  STATUS_VBUCKET_IN_OTHER_SERV = 0x0007,
  STATUS_AUTH_ERROR = 0x0008,
  STATUS_AUTH_CONT = 0x0009,
  STATUS_UNKNOWN_CMD = 0x0081,
  STATUS_OUT_OF_MEM = 0x0082,
  STATUS_NOT_SUPPORTED = 0x0083,
  STATUS_INTERNAL_ERR = 0x0084,
  STATUS_BUSY = 0x0085,
  STATUS_TMP_FAILURE = 0x0086,
};

struct cmd_req_header
{
  uint8_t magic;
  cmd_opcode op;
  uint16_t keylen;
  uint8_t extralen;
  uint8_t data_type;
  uint16_t vbucket;
  uint32_t bodylen;
  uint32_t opaque;
  uint64_t cas;
};

struct cmd_res_header
{
  uint8_t magic;
  cmd_opcode op;
  uint16_t keylen;
  uint8_t extralen;
  uint8_t data_type;
  cmd_rescode status;
  uint32_t bodylen;
  uint32_t opaque;
  uint64_t cas;
};

union cmd_extra
{
  union
  {
    uint32_t flags;
    uint32_t expiration;
    uint32_t verbosity;
  } oneval;
  struct
  {
    uint32_t flags;
    uint32_t expiration;
  } twoval;
  struct
  {
    uint64_t addition_value;
    uint64_t init_value;
    uint32_t expiration;
  } numeric;
};

void get_errstr(const char **ptr, size_t *len, enum cmd_errcode code);

#endif
