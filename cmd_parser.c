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

#include "cmd_parser.h"
#include "util.h"
#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <syslog.h>

static char CMD_STR_SET[4] = "set ";
static char CMD_STR_ADD[4] = "add ";
static char CMD_STR_REPLACE[8] = "replace ";
static char CMD_STR_APPEND[7] = "append ";
static char CMD_STR_PREPEND[8] = "prepend ";
static char CMD_STR_CAS[4] = "cas ";
static char CMD_STR_GET[4] = "get ";
static char CMD_STR_GETS[5] = "gets ";
static char CMD_STR_DELETE[7] = "delete ";
static char CMD_STR_INCR[5] = "incr ";
static char CMD_STR_DECR[5] = "decr ";
static char CMD_STR_TOUCH[6] = "touch ";
static char CMD_STR_NOREPLY[7] = "noreply";
static char CMD_STR_QUIT[4] = "quit";
static char CMD_STR_FLUSH[9] = "flush_all";

static char BAD_DATA_ERROR[] = "CLIENT_ERROR bad data chunk\r\n";
static char BAD_CMD_ERROR[] = "CLIENT_ERROR bad command line format\r\n";
static char LINE_TOO_LONG_ERROR[] = "ERROR line too long\r\n";

bool
parse_uint32(uint32_t *dest, char **iter)
{
  const char *buf;
  while (ed_isspace(**iter))
    (*iter)++;
  if (**iter == '-')
    return false;

  buf = *iter;
  errno = 0;
  unsigned long int tmp = strtoul(buf, iter, 0);
  if (errno)
    return false;
  if (tmp > UINT32_MAX)
    return false;
  *dest = (uint32_t)tmp;
  return true;
}

bool
parse_uint64(uint64_t *dest, char **iter)
{
  const char *buf;
  while (ed_isspace(**iter))
    (*iter)++;
  if (**iter == '-')
    return false;

  buf = *iter;
  errno = 0;
  unsigned long long int tmp = strtoull(buf, iter, 0);
  if (errno)
    return false;
  *dest = (uint64_t)tmp;
  return true;
}

// need a ascii flush error handler
void
reset_cmd_handler(cmd_handler *cmd)
{
  syslog(LOG_DEBUG, "reset cmd");
  cmd->state = CMD_CLEAN;
  cmd->buf_used = 0;
  cmd->skip_until_newline = false;
  memset(&cmd->req, 0x00, sizeof(cmd_req_header));
  memset(&cmd->extra, 0x00, sizeof(cmd_extra));
  cmd->key = NULL;
  if (cmd->val_copied && cmd->value)
    free(cmd->value);
  cmd->val_copied = false;
  cmd->value = NULL;
  cmd->value_stored = 0;
}

ssize_t
ascii_cmd_error(cmd_handler *cmd, ssize_t nbyte, char *buf)
{
  syslog(LOG_DEBUG, "ascii_cmd_error");
  ssize_t idx = 0;
  while (idx < nbyte && buf[idx] != '\r')
    idx++;

  // we reached to the end of buffer, but hasn't find '\r'
  // state remain ASCII_ERROR
  if (idx == nbyte)
    return idx;

  // We found '\r', now reset the state to CMD_CLEAN
  reset_cmd_handler(cmd);
  // There might be an additional '\n'
  if (idx < nbyte - 1 && buf[idx + 1] == '\n')
    return idx + 1;
  return idx;
}

ssize_t
ascii_cpbuf(cmd_handler *cmd, ssize_t nbyte, char *buf, ed_writer *writer)
{
  ssize_t idx = 0, linebreak;
  // TODO check this logic
  if (cmd->skip_until_newline)
    {
      while (idx < nbyte && buf[idx] != '\n')
        idx++;
      if (idx == nbyte)
        return idx;
      reset_cmd_handler(cmd);
      return idx + 1;
    }
  if (cmd->state == CMD_CLEAN)
    {
      while (idx < nbyte && ed_isspace(buf[idx]))
        idx++;
      if (idx == nbyte)
        return nbyte;
      if (memeq(&buf[idx], CMD_STR_GET, sizeof(CMD_STR_GET)))
        {
          linebreak = idx + sizeof(CMD_STR_GET);
          while (linebreak < nbyte && ed_isspace(buf[linebreak]))
            linebreak++;
          if (linebreak == nbyte)
            {
              writer_reserve(writer, sizeof("ERROR\r\n") - 1);
              writer_append(writer, "ERROR\r\n", sizeof("ERROR\r\n") - 1);
              reset_cmd_handler(cmd);
              return linebreak;
            }
          cmd->state = ASCII_PENDING_GET_MULTI;
          return idx + sizeof(CMD_STR_GET);
        }
      if (memeq(&buf[idx], CMD_STR_GETS, sizeof(CMD_STR_GETS)))
        {
          linebreak = idx + sizeof(CMD_STR_GET);
          while (linebreak < nbyte && ed_isspace(buf[linebreak]))
            linebreak++;
          if (linebreak == nbyte)
            {
              writer_reserve(writer, sizeof("ERROR\r\n") - 1);
              writer_append(writer, "ERROR\r\n", sizeof("ERROR\r\n") - 1);
              reset_cmd_handler(cmd);
              return linebreak;
            }
          cmd->state = ASCII_PENDING_GET_CAS_MULTI;
          return idx + sizeof(CMD_STR_GETS);
        }
      linebreak = idx;
    }
  else
    {
      linebreak = 0;
    }
  while (linebreak < nbyte && buf[linebreak] != '\n')
    linebreak++;
  if (linebreak == nbyte)
    {
      if (linebreak - idx - cmd->buf_used >= CMD_BUF_SIZE)
        {
          writer_reserve(writer, sizeof(LINE_TOO_LONG_ERROR) - 1);
          writer_append(writer, LINE_TOO_LONG_ERROR,
                        sizeof(LINE_TOO_LONG_ERROR) - 1);
          cmd->skip_until_newline = true;
          return linebreak;
        }
      // In the middle of reading input. Store the input in buf and
      // wait for the next read.
      cmd->state = ASCII_PENDING_RAWBUF;
      memcpy(&cmd->buffer[cmd->buf_used], &buf[idx], linebreak - idx);
      cmd->buf_used += linebreak - idx;
      return linebreak;
    }
  // Normally we should check if buf[linebreak - 1] is '\r'.
  // However, memcached doesn't check it here, so we follow the behavior.
  cmd->state = ASCII_PENDING_PARSE_CMD;
  memcpy(&cmd->buffer[cmd->buf_used], &buf[idx], linebreak - idx + 1);
  cmd->buf_used += linebreak - idx + 1;
  return linebreak + 1;
}

void
ascii_parse_cmd(cmd_handler *cmd, ed_writer *writer)
{
  char *iter1, *iter2;
  iter1 = cmd->buffer;
  syslog(LOG_DEBUG, "entering parse_cmd: %s", cmd->buffer);
  if (memeq(cmd->buffer, CMD_STR_SET, sizeof(CMD_STR_SET)))
    {
      cmd->req.op = PROTOCOL_BINARY_CMD_SET;
      iter1 += sizeof(CMD_STR_SET);
      while (*iter1 == ' ')
        iter1++;
      iter2 = iter1;
      while (isgraph(*iter2))
        iter2++;
      cmd->key = iter1;
      cmd->req.keylen = iter2 - iter1;
      iter1 = iter2;
      if (!parse_uint32(&cmd->extra.twoval.flags, &iter1))
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      if (!parse_uint32(&cmd->extra.twoval.expiration, &iter1))
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      if (!parse_uint32(&cmd->req.bodylen, &iter1))
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      while (*iter1 == ' ')
        iter1++;
      if (memeq(iter1, CMD_STR_NOREPLY, sizeof(CMD_STR_NOREPLY)))
        {
          cmd->req.op = PROTOCOL_BINARY_CMD_SETQ;
          iter1 += sizeof(CMD_STR_NOREPLY);
          while (*iter1 == ' ')
            iter1++;
        }
      if (*iter1 != '\r' && *iter1 != '\n')
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      cmd->state = ASCII_PENDING_VALUE;
      return;
    }
  if (memeq(cmd->buffer, CMD_STR_ADD, sizeof(CMD_STR_ADD)))
    {
      cmd->req.op = PROTOCOL_BINARY_CMD_ADD;
      iter1 += sizeof(CMD_STR_ADD);
      while (*iter1 == ' ')
        iter1++;
      iter2 = iter1;
      while (isgraph(*iter2))
        iter2++;
      cmd->key = iter1;
      cmd->req.keylen = iter2 - iter1;
      iter1 = iter2;
      if (!parse_uint32(&cmd->extra.twoval.flags, &iter1))
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      if (!parse_uint32(&cmd->extra.twoval.expiration, &iter1))
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      if (!parse_uint32(&cmd->req.bodylen, &iter1))
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      while (*iter1 == ' ')
        iter1++;
      if (memeq(iter1, CMD_STR_NOREPLY, sizeof(CMD_STR_NOREPLY)))
        {
          cmd->req.op = PROTOCOL_BINARY_CMD_ADDQ;
          iter1 += sizeof(CMD_STR_NOREPLY);
          while (*iter1 == ' ')
            iter1++;
        }
      if (*iter1 != '\r' && *iter1 != '\n')
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      cmd->state = ASCII_PENDING_VALUE;
      return;
    }
  if (memeq(cmd->buffer, CMD_STR_REPLACE, sizeof(CMD_STR_REPLACE)))
    {
      cmd->req.op = PROTOCOL_BINARY_CMD_REPLACE;
      iter1 += sizeof(CMD_STR_REPLACE);
      while (*iter1 == ' ')
        iter1++;
      iter2 = iter1;
      while (isgraph(*iter2))
        iter2++;
      cmd->key = iter1;
      cmd->req.keylen = iter2 - iter1;
      iter1 = iter2;
      if (!parse_uint32(&cmd->extra.twoval.flags, &iter1))
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      if (!parse_uint32(&cmd->extra.twoval.expiration, &iter1))
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      if (!parse_uint32(&cmd->req.bodylen, &iter1))
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      while (*iter1 == ' ')
        iter1++;
      if (memeq(iter1, CMD_STR_NOREPLY, sizeof(CMD_STR_NOREPLY)))
        {
          cmd->req.op = PROTOCOL_BINARY_CMD_REPLACEQ;
          iter1 += sizeof(CMD_STR_NOREPLY);
          while (*iter1 == ' ')
            iter1++;
        }
      if (*iter1 != '\r' && *iter1 != '\n')
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      cmd->state = ASCII_PENDING_VALUE;
      return;
    }
  if (memeq(cmd->buffer, CMD_STR_APPEND, sizeof(CMD_STR_APPEND)))
    {
      cmd->req.op = PROTOCOL_BINARY_CMD_APPEND;
      iter1 += sizeof(CMD_STR_APPEND);
      while (*iter1 == ' ')
        iter1++;
      iter2 = iter1;
      while (isgraph(*iter2))
        iter2++;
      cmd->key = iter1;
      cmd->req.keylen = iter2 - iter1;
      iter1 = iter2;
      if (!parse_uint32(&cmd->extra.twoval.flags, &iter1))
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      if (!parse_uint32(&cmd->extra.twoval.expiration, &iter1))
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      if (!parse_uint32(&cmd->req.bodylen, &iter1))
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      while (*iter1 == ' ')
        iter1++;
      if (memeq(iter1, CMD_STR_NOREPLY, sizeof(CMD_STR_NOREPLY)))
        {
          cmd->req.op = PROTOCOL_BINARY_CMD_APPENDQ;
          iter1 += sizeof(CMD_STR_NOREPLY);
          while (*iter1 == ' ')
            iter1++;
        }
      if (*iter1 != '\r' && *iter1 != '\n')
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      cmd->state = ASCII_PENDING_VALUE;
      return;
    }
  if (memeq(cmd->buffer, CMD_STR_PREPEND, sizeof(CMD_STR_PREPEND)))
    {
      cmd->req.op = PROTOCOL_BINARY_CMD_PREPEND;
      iter1 += sizeof(CMD_STR_PREPEND);
      while (*iter1 == ' ')
        iter1++;
      iter2 = iter1;
      while (isgraph(*iter2))
        iter2++;
      cmd->key = iter1;
      cmd->req.keylen = iter2 - iter1;
      iter1 = iter2;
      if (!parse_uint32(&cmd->extra.twoval.flags, &iter1))
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      if (!parse_uint32(&cmd->extra.twoval.expiration, &iter1))
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      if (!parse_uint32(&cmd->req.bodylen, &iter1))
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      while (*iter1 == ' ')
        iter1++;
      if (memeq(iter1, CMD_STR_NOREPLY, sizeof(CMD_STR_NOREPLY)))
        {
          cmd->req.op = PROTOCOL_BINARY_CMD_PREPENDQ;
          iter1 += sizeof(CMD_STR_NOREPLY);
          while (*iter1 == ' ')
            iter1++;
        }
      if (*iter1 != '\r' && *iter1 != '\n')
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      cmd->state = ASCII_PENDING_VALUE;
      return;
    }
  if (memeq(cmd->buffer, CMD_STR_CAS, sizeof(CMD_STR_CAS)))
    {
      // CAS is equivalent to set but with CAS value
      cmd->req.op = PROTOCOL_BINARY_CMD_SET;
      iter1 += sizeof(CMD_STR_CAS);
      while (*iter1 == ' ')
        iter1++;
      iter2 = iter1;
      while (isgraph(*iter2))
        iter2++;
      cmd->key = iter1;
      cmd->req.keylen = iter2 - iter1;
      iter1 = iter2;
      if (!parse_uint32(&cmd->extra.twoval.flags, &iter1))
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      if (!parse_uint32(&cmd->extra.twoval.expiration, &iter1))
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      if (!parse_uint32(&cmd->req.bodylen, &iter1))
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      if (!parse_uint64(&cmd->req.cas, &iter1))
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      while (*iter1 == ' ')
        iter1++;
      if (memeq(iter1, CMD_STR_NOREPLY, sizeof(CMD_STR_NOREPLY)))
        {
          cmd->req.op = PROTOCOL_BINARY_CMD_SETQ;
          iter1 += sizeof(CMD_STR_NOREPLY);
          while (*iter1 == ' ')
            iter1++;
        }
      if (*iter1 != '\r' && *iter1 != '\n')
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      cmd->state = ASCII_PENDING_VALUE;
      return;
    }
  if (memeq(cmd->buffer, CMD_STR_DELETE, sizeof(CMD_STR_DELETE)))
    {
      cmd->req.op = PROTOCOL_BINARY_CMD_DELETE;
      iter1 += sizeof(CMD_STR_DELETE);
      while (*iter1 == ' ')
        iter1++;
      iter2 = iter1;
      while (isgraph(*iter2))
        iter2++;
      cmd->key = iter1;
      cmd->req.keylen = iter2 - iter1;
      iter1 = iter2;
      while (*iter1 == ' ')
        iter1++;
      if (memeq(iter1, CMD_STR_NOREPLY, sizeof(CMD_STR_NOREPLY)))
        {
          cmd->req.op = PROTOCOL_BINARY_CMD_DELETEQ;
          iter1 += sizeof(CMD_STR_NOREPLY);
          while (*iter1 == ' ')
            iter1++;
        }
      if (*iter1 != '\r')
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      cmd->state = ASCII_CMD_READY;
      return;
    }
  if (memeq(cmd->buffer, CMD_STR_INCR, sizeof(CMD_STR_INCR)))
    {
      cmd->req.op = PROTOCOL_BINARY_CMD_INCREMENT;
      iter1 += sizeof(CMD_STR_INCR);
      while (*iter1 == ' ')
        iter1++;
      iter2 = iter1;
      while (isgraph(*iter2))
        iter2++;
      cmd->key = iter1;
      cmd->req.keylen = iter2 - iter1;
      iter1 = iter2;
      if (!parse_uint64(&cmd->extra.numeric.addition_value, &iter1))
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      cmd->extra.numeric.init_value = 0;
      cmd->extra.numeric.expiration = 0;
      while (*iter1 == ' ')
        iter1++;
      if (memeq(iter1, CMD_STR_NOREPLY, sizeof(CMD_STR_NOREPLY)))
        {
          cmd->req.op = PROTOCOL_BINARY_CMD_INCREMENTQ;
          iter1 += sizeof(CMD_STR_NOREPLY);
          while (*iter1 == ' ')
            iter1++;
        }
      if (*iter1 != '\r' && *iter1 != '\n')
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      cmd->state = ASCII_CMD_READY;
      return;
    }
  if (memeq(cmd->buffer, CMD_STR_DECR, sizeof(CMD_STR_DECR)))
    {
      cmd->req.op = PROTOCOL_BINARY_CMD_DECREMENT;
      iter1 += sizeof(CMD_STR_DECR);
      while (*iter1 == ' ')
        iter1++;
      iter2 = iter1;
      while (isgraph(*iter2))
        iter2++;
      cmd->key = iter1;
      cmd->req.keylen = iter2 - iter1;
      iter1 = iter2;
      if (!parse_uint64(&cmd->extra.numeric.addition_value, &iter1))
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      cmd->extra.numeric.init_value = 0;
      cmd->extra.numeric.expiration = 0;
      while (*iter1 == ' ')
        iter1++;
      if (memeq(iter1, CMD_STR_NOREPLY, sizeof(CMD_STR_NOREPLY)))
        {
          cmd->req.op = PROTOCOL_BINARY_CMD_DECREMENTQ;
          iter1 += sizeof(CMD_STR_NOREPLY);
          while (*iter1 == ' ')
            iter1++;
        }
      if (*iter1 != '\r' && *iter1 != '\n')
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      cmd->state = ASCII_CMD_READY;
      return;
    }
  if (memeq(cmd->buffer, CMD_STR_TOUCH, sizeof(CMD_STR_TOUCH)))
    {
      cmd->req.op = PROTOCOL_BINARY_CMD_TOUCH;
      iter1 += sizeof(CMD_STR_TOUCH);
      while (*iter1 == ' ')
        iter1++;
      iter2 = iter1;
      while (isgraph(*iter2))
        iter2++;
      cmd->key = iter1;
      cmd->req.keylen = iter2 - iter1;
      iter1 = iter2;
      if (!parse_uint32(&cmd->extra.oneval.expiration, &iter1))
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      while (*iter1 == ' ')
        iter1++;
      if (memeq(iter1, CMD_STR_NOREPLY, sizeof(CMD_STR_NOREPLY)))
        {
          cmd->req.op = PROTOCOL_BINARY_CMD_TOUCHQ;
          iter1 += sizeof(CMD_STR_NOREPLY);
          while (*iter1 == ' ')
            iter1++;
        }
      if (*iter1 != '\r' && *iter1 != '\n')
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      cmd->state = ASCII_CMD_READY;
      return;
    }
  if (memeq(cmd->buffer, CMD_STR_QUIT, sizeof(CMD_STR_QUIT)))
    {
      iter1 += sizeof(CMD_STR_QUIT);
      while (*iter1 == ' ')
        iter1++;
      if (*iter1 != '\r' && *iter1 != '\n')
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      cmd->req.op = PROTOCOL_BINARY_CMD_QUIT;
      cmd->state = ASCII_CMD_READY;
      return;
    }
  if (memeq(cmd->buffer, CMD_STR_FLUSH, sizeof(CMD_STR_FLUSH)))
    {
      iter1 += sizeof(CMD_STR_FLUSH);
      while (*iter1 == ' ')
        iter1++;
      cmd->req.op = PROTOCOL_BINARY_CMD_FLUSH;
      if (memeq(iter1, CMD_STR_NOREPLY, sizeof(CMD_STR_NOREPLY)))
        {
          cmd->req.op = PROTOCOL_BINARY_CMD_FLUSHQ;
          iter1 += sizeof(CMD_STR_NOREPLY);
          while (*iter1 == ' ')
            iter1++;
        }
      if (*iter1 != '\r' && *iter1 != '\n')
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          reset_cmd_handler(cmd);
          return;
        }
      cmd->state = ASCII_CMD_READY;
      return;
    }
  syslog(LOG_DEBUG, "cannot parse %s", cmd->buffer);
  writer_reserve(writer, sizeof("ERROR\r\n") - 1);
  writer_append(writer, "ERROR\r\n", sizeof("ERROR\r\n") - 1);
  reset_cmd_handler(cmd);
}

ssize_t
cmd_parse_ascii_value(cmd_handler *cmd, ssize_t nbyte, char *buf,
                      ed_writer *writer)
{
  ssize_t partial_len, idx;

  if (cmd->skip_until_newline)
    {
      idx = 0;
      while (idx < nbyte && buf[idx] != '\n')
        idx++;
      if (idx == nbyte)
        return idx;
      reset_cmd_handler(cmd);
      return idx + 1;
    }
  if (cmd->value_stored == 0 && nbyte > cmd->req.bodylen + 2)
    {
      if (buf[cmd->req.bodylen + 1] == '\n' && buf[cmd->req.bodylen] == '\r')
        {
          cmd->value = buf;
          cmd->val_copied = false;
          cmd->value_stored = cmd->req.bodylen;
          cmd->state = ASCII_CMD_READY;
          return cmd->req.bodylen + 2;
        }
      else
        {
          writer_reserve(writer, sizeof(BAD_DATA_ERROR) - 1);
          writer_append(writer, BAD_DATA_ERROR, sizeof(BAD_DATA_ERROR) - 1);
          cmd->skip_until_newline = true;
          return cmd->req.bodylen + 2;
        }
    }
  if (cmd->req.bodylen == 0)
    {
      cmd->state = ASCII_CMD_READY;
      return 0;
    }
  if (cmd->value == NULL)
    {
      cmd->value = malloc(cmd->req.bodylen);
      cmd->val_copied = true;
    }
  partial_len = cmd->req.bodylen - cmd->value_stored;
  if (nbyte < partial_len + 2)
    {
      memcpy(&cmd->value[cmd->value_stored], buf, nbyte);
      cmd->value_stored += nbyte;
      return nbyte;
    }
  else if (buf[partial_len + 1] == '\n' && buf[partial_len] == '\r')
    {
      memcpy(&cmd->value[cmd->value_stored], buf, partial_len);
      cmd->value_stored = cmd->req.bodylen;
      cmd->state = ASCII_CMD_READY;
      return partial_len + 2;
    }
  writer_reserve(writer, sizeof(BAD_DATA_ERROR) - 1);
  writer_append(writer, BAD_DATA_ERROR, sizeof(BAD_DATA_ERROR) - 1);
  cmd->skip_until_newline = true;
  return partial_len + 2;
}

ssize_t
cmd_parse_get(cmd_handler *cmd, ssize_t nbyte, char *buf, void *lru,
              ed_writer *writer)
{
  ssize_t idx1, idx2;
  // idx1 for scanning space
  // idx2 for scanning key
  idx1 = idx2 = 0;

  // pending value from last scan
  if (cmd->buf_used > 0)
    {
      while (idx2 < nbyte && isgraph(buf[idx2]))
        idx2++;
      if (idx2 == nbyte)
        {
          if (idx2 + cmd->buf_used >= KEY_MAX_SIZE)
            {
              // TODO check case where we already processed whole buffer
              writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
              writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
              cmd->skip_until_newline = true;
              return idx2;
            }
          memcpy(&cmd->buffer[cmd->buf_used], buf, idx2);
          cmd->buf_used += idx2;
          return idx2;
        }
      if (buf[idx2] == '\r')
        {
          memcpy(&cmd->buffer[cmd->buf_used], buf, idx2);
          cmd->buf_used += idx2;
          cmd->req.keylen = cmd->buf_used;
          cmd->key = cmd->buffer;
          process_cmd_get(lru, cmd, writer);
          writer_reserve(writer, sizeof("END\r\n") - 1);
          writer_append(writer, "END\r\n", sizeof("END\r\n") - 1);
          reset_cmd_handler(cmd);
          if (idx2 < nbyte && buf[idx2 + 1] == '\n')
            return idx2 + 2;
          return idx2 + 1;
        }
      if (buf[idx2] != ' ')
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          // TODO while till newline
          reset_cmd_handler(cmd);
          return idx2;
        }
      memcpy(&cmd->buffer[cmd->buf_used], buf, idx2);
      cmd->buf_used += idx2;
      cmd->req.keylen = cmd->buf_used;
      cmd->key = cmd->buffer;
      // process get, by GET/GET_CAS
      process_cmd_get(lru, cmd, writer);
      cmd->buf_used = 0;
      return idx2;
    }

  while (idx1 < nbyte && buf[idx1] == ' ')
    idx1++;
  if (idx1 == nbyte)
    return idx1;
  if (buf[idx1] == '\r')
    {
      writer_reserve(writer, sizeof("END\r\n") - 1);
      writer_append(writer, "END\r\n", sizeof("END\r\n") - 1);
      reset_cmd_handler(cmd);
      if (idx1 < nbyte - 1 && buf[idx1 + 1] == '\n')
        return idx1 + 2;
      return idx1 + 1;
    }
  idx2 = idx1;
  while (idx2 < nbyte && isgraph(buf[idx2]))
    idx2++;
  if (idx2 == idx1)
    {
      writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
      writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
      cmd->skip_until_newline = true;
      // TODO check case where we already processed whole buffer
      return idx2;
    }
  if (idx2 == nbyte)
    {
      if (idx2 - idx1 >= KEY_MAX_SIZE)
        {
          writer_reserve(writer, sizeof(BAD_CMD_ERROR) - 1);
          writer_append(writer, BAD_CMD_ERROR, sizeof(BAD_CMD_ERROR) - 1);
          cmd->skip_until_newline = true;
          // TODO check case where we already processed whole buffer
          return idx2;
        }
      memcpy(cmd->buffer, &buf[idx1], idx2 - idx1);
      cmd->buf_used = idx2 - idx1;
      return idx2;
    }
  if (buf[idx2] == '\r')
    {
      cmd->req.keylen = idx2 - idx1;
      cmd->key = &buf[idx1];
      // process get/gets
      process_cmd_get(lru, cmd, writer);
      writer_reserve(writer, sizeof("END\r\n") - 1);
      writer_append(writer, "END\r\n", sizeof("END\r\n") - 1);
      reset_cmd_handler(cmd);
      if (idx2 < nbyte - 1 && buf[idx2 + 1] == '\n')
        return idx2 + 2;
      return idx2 + 1;
    }
  cmd->req.keylen = idx2 - idx1;
  cmd->key = &buf[idx1];
  // process get/gets
  process_cmd_get(lru, cmd, writer);
  return idx2;
}

ssize_t
binary_cpbuf(cmd_handler *cmd, ssize_t nbyte, char *buf, ed_writer *writer)
{
  ssize_t cpbyte = 24 - cmd->buf_used;
  if (nbyte < cpbyte)
    {
      memcpy(cmd->buffer, buf, nbyte);
      cmd->buf_used += nbyte;
      cmd->state = BINARY_PENDING_RAWBUF;
      return nbyte;
    }
  memcpy(&cmd->buffer[cmd->buf_used], buf, cpbyte);
  memcpy(&cmd->req, cmd->buffer, 24);
  cmd->buf_used = 0;
  cmd_req_ntoh(&cmd->req);
  switch (cmd->req.op)
    {
    case PROTOCOL_BINARY_CMD_GET:
    case PROTOCOL_BINARY_CMD_GETQ:
    case PROTOCOL_BINARY_CMD_GETK:
    case PROTOCOL_BINARY_CMD_GETKQ:
      cmd->state = BINARY_PENDING_PARSE_KEY;
      break;
    case PROTOCOL_BINARY_CMD_SET:
    case PROTOCOL_BINARY_CMD_ADD:
    case PROTOCOL_BINARY_CMD_REPLACE:
    case PROTOCOL_BINARY_CMD_SETQ:
    case PROTOCOL_BINARY_CMD_ADDQ:
    case PROTOCOL_BINARY_CMD_REPLACEQ:
      cmd->state = BINARY_PENDING_PARSE_EXTRA;
      break;
    case PROTOCOL_BINARY_CMD_DELETE:
    case PROTOCOL_BINARY_CMD_DELETEQ:
      cmd->state = BINARY_PENDING_PARSE_KEY;
      break;
    case PROTOCOL_BINARY_CMD_INCREMENT:
    case PROTOCOL_BINARY_CMD_DECREMENT:
    case PROTOCOL_BINARY_CMD_INCREMENTQ:
    case PROTOCOL_BINARY_CMD_DECREMENTQ:
    case PROTOCOL_BINARY_CMD_FLUSH:
    case PROTOCOL_BINARY_CMD_FLUSHQ:
      cmd->state = BINARY_PENDING_PARSE_EXTRA;
      break;
    case PROTOCOL_BINARY_CMD_QUIT:
    case PROTOCOL_BINARY_CMD_QUITQ:
    case PROTOCOL_BINARY_CMD_VERSION:
    case PROTOCOL_BINARY_CMD_NOOP:
      cmd->state = BINARY_CMD_READY;
      break;
    case PROTOCOL_BINARY_CMD_APPEND:
    case PROTOCOL_BINARY_CMD_PREPEND:
    case PROTOCOL_BINARY_CMD_APPENDQ:
    case PROTOCOL_BINARY_CMD_PREPENDQ:
      cmd->state = BINARY_PENDING_PARSE_KEY;
      break;
    case PROTOCOL_BINARY_CMD_STAT:
      cmd->state = BINARY_PENDING_PARSE_KEY;
      break;
    case PROTOCOL_BINARY_CMD_TOUCH:
    case PROTOCOL_BINARY_CMD_GAT:
    case PROTOCOL_BINARY_CMD_GATQ:
    case PROTOCOL_BINARY_CMD_GATK:
    case PROTOCOL_BINARY_CMD_GATKQ:
      cmd->state = BINARY_PENDING_PARSE_EXTRA;
      break;
    default:
      // Unknown type, close socket
      cmd->req.op = PROTOCOL_BINARY_CMD_QUIT;
      cmd->state = BINARY_CMD_READY;
    }
  return cpbyte;
}

ssize_t
binary_cmd_parse_extra(cmd_handler *cmd, ssize_t nbyte, char *buf,
                       ed_writer *writer)
{
  ssize_t exbyte, cpbyte;
  switch (cmd->req.op)
    {
    case PROTOCOL_BINARY_CMD_SET:
    case PROTOCOL_BINARY_CMD_ADD:
    case PROTOCOL_BINARY_CMD_REPLACE:
    case PROTOCOL_BINARY_CMD_SETQ:
    case PROTOCOL_BINARY_CMD_ADDQ:
    case PROTOCOL_BINARY_CMD_REPLACEQ:
      exbyte = 8;
      break;
    case PROTOCOL_BINARY_CMD_INCREMENT:
    case PROTOCOL_BINARY_CMD_DECREMENT:
    case PROTOCOL_BINARY_CMD_INCREMENTQ:
    case PROTOCOL_BINARY_CMD_DECREMENTQ:
      exbyte = 20;
      break;
    case PROTOCOL_BINARY_CMD_FLUSH:
    case PROTOCOL_BINARY_CMD_TOUCH:
    case PROTOCOL_BINARY_CMD_GAT:
    case PROTOCOL_BINARY_CMD_GATQ:
    case PROTOCOL_BINARY_CMD_GATK:
    case PROTOCOL_BINARY_CMD_GATKQ:
      exbyte = 4;
      break;
    default:
      cmd->req.op = PROTOCOL_BINARY_CMD_QUIT;
      cmd->state = BINARY_CMD_READY;
      return 0;
    }

  cpbyte = exbyte - cmd->buf_used;
  if (nbyte < cpbyte)
    {
      memcpy(&cmd->buffer[cmd->buf_used], buf, nbyte);
      cmd->buf_used += nbyte;
      return nbyte;
    }
  memcpy(&cmd->buffer[cmd->buf_used], buf, cpbyte);
  memcpy(&cmd->extra, cmd->buffer, cpbyte);
  cmd->buf_used = 0;

  switch (cmd->req.op)
    {
    case PROTOCOL_BINARY_CMD_SET:
    case PROTOCOL_BINARY_CMD_ADD:
    case PROTOCOL_BINARY_CMD_REPLACE:
    case PROTOCOL_BINARY_CMD_SETQ:
    case PROTOCOL_BINARY_CMD_ADDQ:
    case PROTOCOL_BINARY_CMD_REPLACEQ:
      cmd->extra.twoval.flags = ntohl(cmd->extra.twoval.flags);
      cmd->extra.twoval.expiration = ntohl(cmd->extra.twoval.expiration);
      cmd->state = BINARY_PENDING_PARSE_KEY;
      break;
    case PROTOCOL_BINARY_CMD_INCREMENT:
    case PROTOCOL_BINARY_CMD_DECREMENT:
    case PROTOCOL_BINARY_CMD_INCREMENTQ:
    case PROTOCOL_BINARY_CMD_DECREMENTQ:
      cmd->extra.numeric.addition_value
          = ntohll(cmd->extra.numeric.addition_value);
      cmd->extra.numeric.init_value = ntohll(cmd->extra.numeric.init_value);
      cmd->extra.numeric.expiration = ntohl(cmd->extra.numeric.expiration);
      cmd->state = BINARY_PENDING_PARSE_KEY;
      break;
    case PROTOCOL_BINARY_CMD_FLUSH:
      cmd->extra.oneval.expiration = ntohl(cmd->extra.oneval.expiration);
      cmd->state = BINARY_CMD_READY;
      break;
    case PROTOCOL_BINARY_CMD_TOUCH:
    case PROTOCOL_BINARY_CMD_GAT:
    case PROTOCOL_BINARY_CMD_GATQ:
    case PROTOCOL_BINARY_CMD_GATK:
    case PROTOCOL_BINARY_CMD_GATKQ:
      cmd->extra.oneval.expiration = ntohl(cmd->extra.oneval.expiration);
      cmd->state = BINARY_PENDING_PARSE_KEY;
      break;
    default:
      cmd->req.op = PROTOCOL_BINARY_CMD_QUIT;
      cmd->state = BINARY_CMD_READY;
    }
  return cpbyte;
}

ssize_t
binary_cmd_parse_key(cmd_handler *cmd, ssize_t nbyte, char *buf,
                     ed_writer *writer)
{
  ssize_t cpbyte;
  cpbyte = cmd->req.keylen - cmd->buf_used;
  if (nbyte < cpbyte)
    {
      memcpy(&cmd->buffer[cmd->buf_used], buf, nbyte);
      cmd->buf_used += nbyte;
      return nbyte;
    }

  memcpy(&cmd->buffer[cmd->buf_used], buf, cpbyte);
  cmd->buf_used += cpbyte;
  cmd->key = cmd->buffer;
  switch (cmd->req.op)
    {
    case PROTOCOL_BINARY_CMD_GET:
    case PROTOCOL_BINARY_CMD_GETQ:
    case PROTOCOL_BINARY_CMD_GETK:
    case PROTOCOL_BINARY_CMD_GETKQ:
      cmd->state = BINARY_CMD_READY;
      break;
    case PROTOCOL_BINARY_CMD_SET:
    case PROTOCOL_BINARY_CMD_ADD:
    case PROTOCOL_BINARY_CMD_REPLACE:
    case PROTOCOL_BINARY_CMD_SETQ:
    case PROTOCOL_BINARY_CMD_ADDQ:
    case PROTOCOL_BINARY_CMD_REPLACEQ:
      cmd->state = BINARY_PENDING_VALUE;
      break;
    case PROTOCOL_BINARY_CMD_DELETE:
    case PROTOCOL_BINARY_CMD_DELETEQ:
    case PROTOCOL_BINARY_CMD_INCREMENT:
    case PROTOCOL_BINARY_CMD_DECREMENT:
    case PROTOCOL_BINARY_CMD_INCREMENTQ:
    case PROTOCOL_BINARY_CMD_DECREMENTQ:
      cmd->state = BINARY_CMD_READY;
      break;
    case PROTOCOL_BINARY_CMD_APPEND:
    case PROTOCOL_BINARY_CMD_PREPEND:
    case PROTOCOL_BINARY_CMD_APPENDQ:
    case PROTOCOL_BINARY_CMD_PREPENDQ:
      cmd->state = BINARY_PENDING_VALUE;
      break;
    case PROTOCOL_BINARY_CMD_STAT:
    case PROTOCOL_BINARY_CMD_TOUCH:
    case PROTOCOL_BINARY_CMD_GAT:
    case PROTOCOL_BINARY_CMD_GATQ:
    case PROTOCOL_BINARY_CMD_GATK:
    case PROTOCOL_BINARY_CMD_GATKQ:
      cmd->state = BINARY_CMD_READY;
      break;
    default:
      // Unknown type, close socket
      cmd->req.op = PROTOCOL_BINARY_CMD_QUIT;
      cmd->state = BINARY_CMD_READY;
    }
  return cpbyte;
}

ssize_t
binary_cmd_parse_value(cmd_handler *cmd, ssize_t nbyte, char *buf,
                       ed_writer *writer)
{
  ssize_t partial_len;
  if (cmd->value_stored == 0 && nbyte >= cmd->req.bodylen)
    {
      cmd->value = buf;
      cmd->val_copied = false;
      cmd->value_stored = cmd->req.bodylen;
      cmd->state = BINARY_CMD_READY;
      return cmd->req.bodylen;
    }
  if (cmd->value == NULL)
    {
      cmd->value = malloc(cmd->req.bodylen);
      cmd->val_copied = true;
    }
  partial_len = cmd->req.bodylen - cmd->value_stored;
  if (nbyte <= partial_len)
    {
      memcpy(&cmd->value[cmd->value_stored], buf, nbyte);
      cmd->value_stored += nbyte;
      return nbyte;
    }
  memcpy(&cmd->value[cmd->value_stored], buf, partial_len);
  cmd->value_stored = cmd->req.bodylen;
  cmd->state = BINARY_CMD_READY;
  return partial_len;
}
