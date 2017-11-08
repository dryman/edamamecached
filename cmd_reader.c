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
#include "writer.h"
#include <ctype.h>
#include <syslog.h>

char EOL[] = "\r\n";
char ascii_ok[] = "ASCII OK\r\n";
char binary_ok[] = "BINARY OK\r\n";
char txt_stored[] = "STORED\r\n";

void process_ascii_cmd(lru_t *lru, cmd_handler *cmd, ed_writer *writer);
void process_cmd_get(void *lru, cmd_handler *cmd, ed_writer *writer);

void
edamame_read(lru_t *lru, cmd_handler *cmd, int nbyte, char *data,
             ed_writer *writer)
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
                cmd->state = BINARY_PENDING_RAWBUF;
              else
                cmd->state = ASCII_PENDING_RAWBUF;
              goto advance_state;
            }
          break;
        case ASCII_PENDING_RAWBUF:
          syslog(LOG_DEBUG, "Enter ASCII_PENDING_RAWBUF");
          idx += ascii_cpbuf(cmd, nbyte - idx, &data[idx], writer);
          if (cmd->state == ASCII_PENDING_PARSE_CMD
              || cmd->state == ASCII_PENDING_GET_MULTI
              || cmd->state == ASCII_PENDING_GET_CAS_MULTI)
            goto advance_state;
          break;
        case ASCII_PENDING_PARSE_CMD:
          syslog(LOG_DEBUG, "Enter ASCII_PENDING_PARSE_CMD");
          ascii_parse_cmd(cmd, writer);
          syslog(LOG_DEBUG, " CMD parsed");
          if (cmd->state == ASCII_PENDING_VALUE
              || cmd->state == ASCII_CMD_READY)
            goto advance_state;
          break;
        case ASCII_PENDING_GET_MULTI:
        case ASCII_PENDING_GET_CAS_MULTI:
          idx += cmd_parse_get(cmd, nbyte - idx, &data[idx], lru, writer);
          break;
        case ASCII_PENDING_VALUE:
          idx += cmd_parse_ascii_value(cmd, nbyte - idx, &data[idx], writer);
          syslog(LOG_DEBUG, "got value");
          if (cmd->state == ASCII_CMD_READY)
            goto advance_state;
          break;
        case ASCII_CMD_READY:
          // TODO process ascii cmd
          writer_reserve(writer, sizeof(ascii_ok) - 1);
          writer_append(writer, ascii_ok, sizeof(ascii_ok) - 1);
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
process_ascii_cmd(lru_t *lru, cmd_handler *cmd, ed_writer *writer)
{
  switch (cmd->req.op)
    {
    case PROTOCOL_BINARY_CMD_SET:
    case PROTOCOL_BINARY_CMD_ADD:
    case PROTOCOL_BINARY_CMD_REPLACE:
    case PROTOCOL_BINARY_CMD_INCREMENT:
    case PROTOCOL_BINARY_CMD_DECREMENT:
    case PROTOCOL_BINARY_CMD_APPEND:
    case PROTOCOL_BINARY_CMD_PREPEND:
      break;
    }
}

void
process_cmd_get(void *lru_, cmd_handler *cmd, ed_writer *writer)
{
  lru_t *lru = lru_;
  writer_reserve(writer, cmd->req.keylen);
  writer_append(writer, cmd->key, cmd->req.keylen);
  writer_reserve(writer, sizeof(EOL) - 1);
  writer_append(writer, EOL, sizeof(EOL) - 1);
  // writer_reserve(writer, sizeof(txt_stored) - 1);
  // writer_append(writer, txt_stored, sizeof(txt_stored) - 1);
  // do nothing for now
}
