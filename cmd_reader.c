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

#include "cmd_reader.h"
#include "cmd_parser.h"
#include "util.h"
#include "writer.h"
#include <ctype.h>
#include <inttypes.h> // PRIu64
#include <syslog.h>
#include <urcu.h>

char EOL[] = "\r\n";
char ascii_ok[] = "ASCII OK\r\n";
char binary_ok[] = "BINARY OK\r\n";
char txt_stored[] = "STORED\r\n";

void process_ascii_cmd(lru_t *lru, cmd_handler *cmd, ed_writer *writer,
                       bool *close_fd);
void process_cmd_get(void *lru, cmd_handler *cmd, ed_writer *writer);

void
edamame_read(lru_t *lru, cmd_handler *cmd, int nbyte, char *data,
             ed_writer *writer, bool *close_fd)
{
  int idx = 0;

  while (idx < nbyte)
    {
    advance_state:
      switch (cmd->state)
        {
        case CMD_CLEAN:
          if (idx < nbyte)
            {
              if (data[idx] == '\x80')
                {
                  idx += binary_cpbuf(cmd, nbyte - idx, &data[idx], writer);
                  if (cmd->state == BINARY_PENDING_PARSE_EXTRA
                      || cmd->state == BINARY_PENDING_PARSE_KEY
                      || cmd->state == BINARY_PENDING_VALUE
                      || cmd->state == BINARY_CMD_READY)
                    goto advance_state;
                }
              else
                {
                  idx += ascii_cpbuf(cmd, nbyte - idx, &data[idx], writer);
                  if (cmd->state == ASCII_PENDING_PARSE_CMD
                      || cmd->state == ASCII_PENDING_GET_MULTI
                      || cmd->state == ASCII_PENDING_GET_CAS_MULTI)
                    goto advance_state;
                }
            }
          break;
        case ASCII_PENDING_RAWBUF:
          // syslog(LOG_DEBUG, "Enter ASCII_PENDING_RAWBUF");
          idx += ascii_cpbuf(cmd, nbyte - idx, &data[idx], writer);
          if (cmd->state == ASCII_PENDING_PARSE_CMD
              || cmd->state == ASCII_PENDING_GET_MULTI
              || cmd->state == ASCII_PENDING_GET_CAS_MULTI)
            goto advance_state;
          break;
        case ASCII_PENDING_PARSE_CMD:
          // syslog(LOG_DEBUG, "Enter ASCII_PENDING_PARSE_CMD");
          ascii_parse_cmd(cmd, writer);
          // syslog(LOG_DEBUG, "CMD parsed");
          if (cmd->state == ASCII_CMD_READY)
            goto advance_state;
          break;
        case ASCII_PENDING_GET_MULTI:
        case ASCII_PENDING_GET_CAS_MULTI:
          // syslog(LOG_DEBUG, "Enter get");
          idx += cmd_parse_get(cmd, nbyte - idx, &data[idx], lru, writer);
          break;
        case ASCII_PENDING_VALUE:
          idx += cmd_parse_ascii_value(cmd, nbyte - idx, &data[idx], writer);
          // syslog(LOG_DEBUG, "got value");
          if (cmd->state == ASCII_CMD_READY)
            goto advance_state;
          break;
        case ASCII_CMD_READY:
          // syslog(LOG_DEBUG, "enter cmd ready");
          process_ascii_cmd(lru, cmd, writer, close_fd);
          reset_cmd_handler(cmd);
          break;
        case BINARY_PENDING_RAWBUF:
          idx += binary_cpbuf(cmd, nbyte - idx, &data[idx], writer);
          break;
        case BINARY_PENDING_PARSE_EXTRA:
          idx += binary_cmd_parse_extra(cmd, nbyte - idx, &data[idx], writer);
        case BINARY_PENDING_PARSE_KEY:
          idx += binary_cmd_parse_key(cmd, nbyte - idx, &data[idx], writer);
          break;
        case BINARY_PENDING_VALUE:
          idx += binary_cmd_parse_value(cmd, nbyte - idx, &data[idx], writer);
          break;
        case BINARY_CMD_READY:
          writer_reserve(writer, sizeof(binary_ok) - 1);
          writer_append(writer, binary_ok, sizeof(binary_ok) - 1);
          reset_cmd_handler(cmd);
        }
    }
}

void
process_ascii_cmd(lru_t *lru, cmd_handler *cmd, ed_writer *writer,
                  bool *close_fd)
{
  lru_val_t lru_val;
  const char *errstr;
  size_t errlen, write_len;

  switch (cmd->req.op)
    {
    case PROTOCOL_BINARY_CMD_SET:
    case PROTOCOL_BINARY_CMD_ADD:
    case PROTOCOL_BINARY_CMD_REPLACE:
    case PROTOCOL_BINARY_CMD_INCREMENT:
    case PROTOCOL_BINARY_CMD_DECREMENT:
    case PROTOCOL_BINARY_CMD_APPEND:
    case PROTOCOL_BINARY_CMD_PREPEND:
      if (lru_upsert(lru, cmd, &lru_val))
        {
          if (cmd->req.op == PROTOCOL_BINARY_CMD_INCREMENT
              || cmd->req.op == PROTOCOL_BINARY_CMD_DECREMENT)
            {
              write_len = size_t_str_len(lru_val.vallen);
              writer_reserve(writer, write_len + 2);
              writer_snprintf(writer, write_len, "%zu\r\n", lru_val.vallen);
            }
          else
            {
              writer_reserve(writer, sizeof(txt_stored) - 1);
              writer_append(writer, txt_stored, sizeof(txt_stored) - 1);
            }
        }
      else
        {
          get_errstr(&errstr, &errlen, lru_val.errcode);
          writer_reserve(writer, errlen);
          writer_append(writer, errstr, errlen);
          // TODO form is different to memcached
          // need \r\n
        }
      break;
    case PROTOCOL_BINARY_CMD_SETQ:
    case PROTOCOL_BINARY_CMD_ADDQ:
    case PROTOCOL_BINARY_CMD_REPLACEQ:
    case PROTOCOL_BINARY_CMD_INCREMENTQ:
    case PROTOCOL_BINARY_CMD_DECREMENTQ:
    case PROTOCOL_BINARY_CMD_APPENDQ:
    case PROTOCOL_BINARY_CMD_PREPENDQ:
      if (!lru_upsert(lru, cmd, &lru_val))
        {
          // TODO log error
        }
      break;
    case PROTOCOL_BINARY_CMD_QUIT:
      *close_fd = true;
      break;
    case PROTOCOL_BINARY_CMD_FLUSH:
      break;
    case PROTOCOL_BINARY_CMD_FLUSHQ:
      break;
    default:
      // TODO GAT is also kind of write
      break;
    }
}

// TODO rename as ascii_cmd_get?
void
process_cmd_get(void *lru_, cmd_handler *cmd, ed_writer *writer)
{
  lru_t *lru = lru_;
  lru_val_t lru_val;
  size_t header_len, vallen;
retry:
  rcu_read_lock();
  if (lru_get(lru, cmd, &lru_val))
    {
      vallen = lru_val.is_numeric_val ? size_t_str_len(lru_val.vallen)
                                      : lru_val.vallen;

      header_len = 5;  // "VALUE"
      header_len += 3; // 3 spaces
      header_len += 2; // \r\n
      header_len += size_t_str_len(cmd->req.keylen);
      header_len += size_t_str_len(lru_val.flags);
      header_len += size_t_str_len(vallen);
      header_len += cmd->state == ASCII_PENDING_GET_CAS_MULTI
                        ? 1 + size_t_str_len(lru_val.cas)
                        : 0;

      // Format size, key len, is_numeric_val, has cas or not..
      // Need to calculate format size
      if (!writer_reserve(writer, header_len + vallen + 2))
        {
          rcu_read_unlock();
          goto retry;
        }
      writer_append(writer, "VALUE ", sizeof("VALUE ") - 1);
      writer_append(writer, cmd->key, cmd->req.keylen);
      if (cmd->state == ASCII_PENDING_GET_CAS_MULTI)
        {
          // flag, vallen, cas
          writer_snprintf(writer, header_len, " %u %zu %" PRIu64 "\r\n",
                          lru_val.flags, vallen, lru_val.cas);
        }
      else
        {
          // flag, vallen
          writer_snprintf(writer, header_len, " %u %zu\r\n", lru_val.flags,
                          vallen);
        }
      if (lru_val.is_numeric_val)
        {
          writer_snprintf(writer, vallen + 2, "%zu\r\n", lru_val.vallen);
        }
      else
        {
          writer_append(writer, lru_val.value, vallen);
          writer_append(writer, EOL, sizeof(EOL) - 1);
        }
      rcu_read_unlock();
    }
  else
    {
      rcu_read_unlock();
    }
}
