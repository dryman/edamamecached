#include "cmd_parser.h"
#include "util.h"
#include <ctype.h>

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

void reset_cmd_handler(cmd_handler* cmd);
ssize_t ascii_cmd_error(cmd_handler* cmd, ssize_t nread, char* buf);
ssize_t ascii_cpbuf(cmd_handler* cmd, ssize_t nread, char* buf);
bool parse_uint32(uint32_t* dest, char** iter);
bool parse_uint64(uint64_t* dest, char** iter);
void ascii_parse_cmd(cmd_handler* cmd);
ssize_t cmd_parse_ascii_value(cmd_handler* cmd, ssize_t nread, char* buf);
void cmd_process_ascii_ready(cmd_handler* cmd);
ssize_t cmd_parse_get(cmd_handler* cmd, ssize_t nread, char* buf);

extern void process_cmd_get(cmd_handler* cmd);

void read_cb(uv_stream_t* client, ssize_t nread, const uv_buf_t* buf)
{
  cmd_handler* cmd = (cmd_handler*)client;
  ssize_t idx = 0;

  if (nread == -1)
    {
      reset_cmd_handler(cmd);
      // recycle buf
    }

  if (nread == 0)
    {
      // recycle buf
    }

  while (idx < nread)
    {
      /*
      switch (cmd->state)
        {
        case CMD_CLEAN:
        case ASCII_PENDING_RAWBUF:
        case ASCII_PENDING_GET_MULTI:
        case ASCII_PENDING_VALUE:
        case ASCII_ERROR:
        case BINARY_PENDING_RAWBUF:
        case BINARY_PENDING_VALUE:
        }
        */
    }
  // recycle buf
}

// need a ascii flush error handler
void reset_cmd_handler(cmd_handler* cmd)
{
  cmd->state = CMD_CLEAN;
  cmd->buf_used = 0;
  cmd->has_pending_newline = false;
  memset(&cmd->req, 0x00, sizeof(cmd_req_header));
  memset(&cmd->extra, 0x00, sizeof(cmd_extra));
  cmd->key = NULL;
  if (cmd->val_copied && cmd->value)
    free(cmd->value);
  cmd->val_copied = false;
  cmd->value = NULL;
  cmd->value_stored = 0;
}

ssize_t ascii_cmd_error(cmd_handler* cmd, ssize_t nread, char* buf)
{
  ssize_t idx = 0;
  while (idx < nread && buf[idx] != '\r') idx++;

  // we reached to the end of buffer, but hasn't find '\r'
  // state remain ASCII_ERROR
  if (idx == nread) return idx;

  // We found '\r', now reset the state to CMD_CLEAN
  reset_cmd_handler(cmd);
  // There might be an additional '\n'
  if (idx < nread - 1 && buf[idx + 1] == '\n') return idx + 1;
  return idx;
}

ssize_t ascii_cpbuf(cmd_handler* cmd, ssize_t nread, char* buf)
{
  ssize_t idx = 0, linebreak;
  if (cmd->state == CMD_CLEAN)
    {
      while (idx < nread && isspace(buf[idx])) idx++;
      if (idx == nread) return nread;
      if (memeq(&buf[idx], CMD_STR_GET, sizeof(CMD_STR_GET)))
        {
          cmd->state = ASCII_PENDING_GET_MULTI;
          return idx + sizeof(CMD_STR_GET);
        }
      if (memeq(&buf[idx], CMD_STR_GETS, sizeof(CMD_STR_GETS)))
        {
          cmd->state = ASCII_PENDING_GET_CAS_MULTI;
          return idx + sizeof(CMD_STR_GETS);
        }
      linebreak = idx;
    }
  else
    {
      linebreak = 0;
    }
  if (cmd->has_pending_newline)
    {
      if (buf[0] == '\n')
        {
          cmd->state = ASCII_PENDING_PARSE_CMD;
          cmd->buffer[cmd->buf_used] = '\n';
          cmd->buf_used++;
          cmd->has_pending_newline = false;
        }
      else
        {
          cmd->state = ASCII_ERROR;
        }
      return 1;
    }
  while (linebreak < nread && isprint(buf[linebreak]))
    linebreak++;
  if (linebreak == nread)
    {
      if (linebreak - idx - cmd->buf_used >= CMD_BUF_SIZE)
        {
          // TODO emit error line too long
          cmd->state = ASCII_ERROR;
          return linebreak;
        }
      cmd->state = ASCII_PENDING_RAWBUF;
      cmd->has_pending_newline = false;
      memcpy(&cmd->buffer[cmd->buf_used], &buf[idx], linebreak - idx);
      cmd->buf_used += linebreak - idx;
      return linebreak;
    }
  if (buf[linebreak] != '\r')
    {
      cmd->state = ASCII_ERROR;
      return linebreak;
    }
  if (linebreak == nread - 1)
    {
      cmd->state = ASCII_PENDING_RAWBUF;
      cmd->has_pending_newline = true;
      memcpy(&cmd->buffer[cmd->buf_used], &buf[idx], linebreak - idx + 1);
      cmd->buf_used += linebreak - idx + 1;
      return linebreak + 1;
    }
  if (buf[linebreak+1] != '\n')
    {
      cmd->state = ASCII_ERROR;
      return linebreak + 2;
    }
  cmd->state = ASCII_PENDING_PARSE_CMD;
  memcpy(&cmd->buffer[cmd->buf_used], &buf[idx], linebreak - idx + 2);
  cmd->buf_used += linebreak - idx + 2;
  return linebreak + 2;
}

bool parse_uint32(uint32_t* dest, char** iter)
{
  const char* buf;
  while (isspace(**iter)) (*iter)++;
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

bool parse_uint64(uint64_t* dest, char** iter)
{
  const char* buf;
  while (isspace(**iter)) (*iter)++;
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

void ascii_parse_cmd(cmd_handler* cmd)
{
  char *iter1, *iter2;
  iter1 = cmd->buffer;
  if (memeq(cmd->buffer, CMD_STR_SET, sizeof(CMD_STR_SET)))
    {
      cmd->req.op = PROTOCOL_BINARY_CMD_SET;
      iter1 += sizeof(CMD_STR_SET);
      while (*iter1 == ' ') iter1++;
      iter2 = iter1;
      while (isgraph(*iter2)) iter2++;
      cmd->key = iter1;
      cmd->req.keylen = iter2 - iter1;
      iter1 = iter2;
      if (!parse_uint32(&cmd->extra.twoval.flags, &iter1))
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      if (!parse_uint32(&cmd->extra.twoval.expiration, &iter1))
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      if (!parse_uint32(&cmd->req.bodylen, &iter1))
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      while (*iter1 == ' ') iter1++;
      if (memeq(iter1, CMD_STR_NOREPLY, sizeof(CMD_STR_NOREPLY)))
        {
          cmd->req.op = PROTOCOL_BINARY_CMD_SETQ;
          iter1 += sizeof(CMD_STR_NOREPLY);
          while (*iter1 == ' ') iter1++;
        }
      if (*iter1 != '\r')
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      cmd->state = ASCII_PENDING_VALUE;
      return;
    }
  if (memeq(cmd->buffer, CMD_STR_ADD, sizeof(CMD_STR_ADD)))
    {
      cmd->req.op = PROTOCOL_BINARY_CMD_ADD;
      iter1 += sizeof(CMD_STR_ADD);
      while (*iter1 == ' ') iter1++;
      iter2 = iter1;
      while (isgraph(*iter2)) iter2++;
      cmd->key = iter1;
      cmd->req.keylen = iter2 - iter1;
      iter1 = iter2;
      if (!parse_uint32(&cmd->extra.twoval.flags, &iter1))
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      if (!parse_uint32(&cmd->extra.twoval.expiration, &iter1))
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      if (!parse_uint32(&cmd->req.bodylen, &iter1))
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      while (*iter1 == ' ') iter1++;
      if (memeq(iter1, CMD_STR_NOREPLY, sizeof(CMD_STR_NOREPLY)))
        {
          cmd->req.op = PROTOCOL_BINARY_CMD_ADDQ;
          iter1 += sizeof(CMD_STR_NOREPLY);
          while (*iter1 == ' ') iter1++;
        }
      if (*iter1 != '\r')
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      cmd->state = ASCII_PENDING_VALUE;
      return;
    }
  if (memeq(cmd->buffer, CMD_STR_REPLACE, sizeof(CMD_STR_REPLACE)))
    {
      cmd->req.op = PROTOCOL_BINARY_CMD_REPLACE;
      iter1 += sizeof(CMD_STR_REPLACE);
      while (*iter1 == ' ') iter1++;
      iter2 = iter1;
      while (isgraph(*iter2)) iter2++;
      cmd->key = iter1;
      cmd->req.keylen = iter2 - iter1;
      iter1 = iter2;
      if (!parse_uint32(&cmd->extra.twoval.flags, &iter1))
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      if (!parse_uint32(&cmd->extra.twoval.expiration, &iter1))
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      if (!parse_uint32(&cmd->req.bodylen, &iter1))
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      while (*iter1 == ' ') iter1++;
      if (memeq(iter1, CMD_STR_NOREPLY, sizeof(CMD_STR_NOREPLY)))
        {
          cmd->req.op = PROTOCOL_BINARY_CMD_REPLACEQ;
          iter1 += sizeof(CMD_STR_NOREPLY);
          while (*iter1 == ' ') iter1++;
        }
      if (*iter1 != '\r')
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      cmd->state = ASCII_PENDING_VALUE;
      return;
    }
  if (memeq(cmd->buffer, CMD_STR_APPEND, sizeof(CMD_STR_APPEND)))
    {
      cmd->req.op = PROTOCOL_BINARY_CMD_APPEND;
      iter1 += sizeof(CMD_STR_APPEND);
      while (*iter1 == ' ') iter1++;
      iter2 = iter1;
      while (isgraph(*iter2)) iter2++;
      cmd->key = iter1;
      cmd->req.keylen = iter2 - iter1;
      iter1 = iter2;
      if (!parse_uint32(&cmd->extra.twoval.flags, &iter1))
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      if (!parse_uint32(&cmd->extra.twoval.expiration, &iter1))
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      if (!parse_uint32(&cmd->req.bodylen, &iter1))
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      while (*iter1 == ' ') iter1++;
      if (memeq(iter1, CMD_STR_NOREPLY, sizeof(CMD_STR_NOREPLY)))
        {
          cmd->req.op = PROTOCOL_BINARY_CMD_APPENDQ;
          iter1 += sizeof(CMD_STR_NOREPLY);
          while (*iter1 == ' ') iter1++;
        }
      if (*iter1 != '\r')
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      cmd->state = ASCII_PENDING_VALUE;
      return;
    }
  if (memeq(cmd->buffer, CMD_STR_PREPEND, sizeof(CMD_STR_PREPEND)))
    {
      cmd->req.op = PROTOCOL_BINARY_CMD_PREPEND;
      iter1 += sizeof(CMD_STR_PREPEND);
      while (*iter1 == ' ') iter1++;
      iter2 = iter1;
      while (isgraph(*iter2)) iter2++;
      cmd->key = iter1;
      cmd->req.keylen = iter2 - iter1;
      iter1 = iter2;
      if (!parse_uint32(&cmd->extra.twoval.flags, &iter1))
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      if (!parse_uint32(&cmd->extra.twoval.expiration, &iter1))
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      if (!parse_uint32(&cmd->req.bodylen, &iter1))
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      while (*iter1 == ' ') iter1++;
      if (memeq(iter1, CMD_STR_NOREPLY, sizeof(CMD_STR_NOREPLY)))
        {
          cmd->req.op = PROTOCOL_BINARY_CMD_PREPENDQ;
          iter1 += sizeof(CMD_STR_NOREPLY);
          while (*iter1 == ' ') iter1++;
        }
      if (*iter1 != '\r')
        {
          cmd->state = ASCII_ERROR;
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
      while (*iter1 == ' ') iter1++;
      iter2 = iter1;
      while (isgraph(*iter2)) iter2++;
      cmd->key = iter1;
      cmd->req.keylen = iter2 - iter1;
      iter1 = iter2;
      if (!parse_uint32(&cmd->extra.twoval.flags, &iter1))
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      if (!parse_uint32(&cmd->extra.twoval.expiration, &iter1))
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      if (!parse_uint32(&cmd->req.bodylen, &iter1))
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      if (!parse_uint64(&cmd->req.cas, &iter1))
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      while (*iter1 == ' ') iter1++;
      if (memeq(iter1, CMD_STR_NOREPLY, sizeof(CMD_STR_NOREPLY)))
        {
          cmd->req.op = PROTOCOL_BINARY_CMD_SETQ;
          iter1 += sizeof(CMD_STR_NOREPLY);
          while (*iter1 == ' ') iter1++;
        }
      if (*iter1 != '\r')
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      cmd->state = ASCII_PENDING_VALUE;
      return;
    }
  if (memeq(cmd->buffer, CMD_STR_DELETE, sizeof(CMD_STR_DELETE)))
    {
      cmd->req.op = PROTOCOL_BINARY_CMD_DELETE;
      iter1 += sizeof(CMD_STR_DELETE);
      while (*iter1 == ' ') iter1++;
      iter2 = iter1;
      while (isgraph(*iter2)) iter2++;
      cmd->key = iter1;
      cmd->req.keylen = iter2 - iter1;
      iter1 = iter2;
      while (*iter1 == ' ') iter1++;
      if (memeq(iter1, CMD_STR_NOREPLY, sizeof(CMD_STR_NOREPLY)))
        {
          cmd->req.op = PROTOCOL_BINARY_CMD_DELETEQ;
          iter1 += sizeof(CMD_STR_NOREPLY);
          while (*iter1 == ' ') iter1++;
        }
      if (*iter1 != '\r')
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      cmd->state = ASCII_CMD_READY;
      return;
    }
  if (memeq(cmd->buffer, CMD_STR_INCR, sizeof(CMD_STR_INCR)))
    {
      cmd->req.op = PROTOCOL_BINARY_CMD_INCREMENT;
      iter1 += sizeof(CMD_STR_INCR);
      while (*iter1 == ' ') iter1++;
      iter2 = iter1;
      while (isgraph(*iter2)) iter2++;
      cmd->key = iter1;
      cmd->req.keylen = iter2 - iter1;
      iter1 = iter2;
      if (!parse_uint64(&cmd->extra.numeric.addition_value, &iter1))
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      cmd->extra.numeric.init_value = 0;
      cmd->extra.numeric.expiration = 0;
      while (*iter1 == ' ') iter1++;
      if (memeq(iter1, CMD_STR_NOREPLY, sizeof(CMD_STR_NOREPLY)))
        {
          cmd->req.op = PROTOCOL_BINARY_CMD_INCREMENTQ;
          iter1 += sizeof(CMD_STR_NOREPLY);
          while (*iter1 == ' ') iter1++;
        }
      if (*iter1 != '\r')
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      cmd->state = ASCII_CMD_READY;
      return;
    }
  if (memeq(cmd->buffer, CMD_STR_DECR, sizeof(CMD_STR_DECR)))
    {
      cmd->req.op = PROTOCOL_BINARY_CMD_DECREMENT;
      iter1 += sizeof(CMD_STR_DECR);
      while (*iter1 == ' ') iter1++;
      iter2 = iter1;
      while (isgraph(*iter2)) iter2++;
      cmd->key = iter1;
      cmd->req.keylen = iter2 - iter1;
      iter1 = iter2;
      if (!parse_uint64(&cmd->extra.numeric.addition_value, &iter1))
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      cmd->extra.numeric.init_value = 0;
      cmd->extra.numeric.expiration = 0;
      while (*iter1 == ' ') iter1++;
      if (memeq(iter1, CMD_STR_NOREPLY, sizeof(CMD_STR_NOREPLY)))
        {
          cmd->req.op = PROTOCOL_BINARY_CMD_DECREMENTQ;
          iter1 += sizeof(CMD_STR_NOREPLY);
          while (*iter1 == ' ') iter1++;
        }
      if (*iter1 != '\r')
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      cmd->state = ASCII_CMD_READY;
      return;
    }
  if (memeq(cmd->buffer, CMD_STR_TOUCH, sizeof(CMD_STR_TOUCH)))
    {
      cmd->req.op = PROTOCOL_BINARY_CMD_TOUCH;
      iter1 += sizeof(CMD_STR_TOUCH);
      while (*iter1 == ' ') iter1++;
      iter2 = iter1;
      while (isgraph(*iter2)) iter2++;
      cmd->key = iter1;
      cmd->req.keylen = iter2 - iter1;
      iter1 = iter2;
      if (!parse_uint32(&cmd->extra.oneval.expiration, &iter1))
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      while (*iter1 == ' ') iter1++;
      if (memeq(iter1, CMD_STR_NOREPLY, sizeof(CMD_STR_NOREPLY)))
        {
          cmd->req.op = PROTOCOL_BINARY_CMD_TOUCHQ;
          iter1 += sizeof(CMD_STR_NOREPLY);
          while (*iter1 == ' ') iter1++;
        }
      if (*iter1 != '\r')
        {
          cmd->state = ASCII_ERROR;
          return;
        }
      cmd->state = ASCII_CMD_READY;
      return;
    }
}

ssize_t cmd_parse_ascii_value(cmd_handler* cmd, ssize_t nread, char* buf)
{
  ssize_t partial_len;
  if (cmd->value_stored == 0 && nread > cmd->req.bodylen + 1)
    {
      if (buf[cmd->req.bodylen] == '\r')
        {
          cmd->value = buf;
          cmd->val_copied = false;
          cmd->value_stored = cmd->req.bodylen;
          cmd->state = ASCII_CMD_READY;
          return cmd->req.bodylen + 1;
        }
      else
        {
          cmd->state = ASCII_ERROR;
          return cmd->req.bodylen + 1;
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
  if (nread <= partial_len)
    {
      memcpy(&cmd->value[cmd->value_stored], buf, nread);
      cmd->value_stored += nread;
      return nread;
    }
  else if (buf[partial_len] == '\r')
    {
      memcpy(&cmd->value[cmd->value_stored],
             buf, partial_len);
      cmd->value_stored = cmd->req.bodylen;
      cmd->state = ASCII_CMD_READY;
      return partial_len + 1;
    }
  cmd->state = ASCII_ERROR;
  return partial_len + 1;
}

ssize_t cmd_parse_get(cmd_handler* cmd, ssize_t nread, char* buf)
{
  ssize_t idx1, idx2;
  // idx1 for scanning space
  // idx2 for scanning key
  idx1 = idx2 = 0;
  if (cmd->buf_used > 0)
    {
      while (idx2 < nread && isgraph(buf[idx2]))
        idx2++;
      if (idx2 == nread)
        {
          if (idx2 + cmd->buf_used >= KEY_MAX_SIZE)
            {
              cmd->buf_used = 0;
              cmd->state = ASCII_ERROR;
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
          // process get, by GET/GET_CAS
          process_cmd_get(cmd);
          cmd->buf_used = 0;
          cmd->state = CMD_CLEAN;
          return idx2;
        }
      if (buf[idx2] != ' ')
        {
          cmd->buf_used = 0;
          cmd->state = ASCII_ERROR;
          return idx2;
        }
      memcpy(&cmd->buffer[cmd->buf_used], buf, idx2);
      cmd->buf_used += idx2;
      cmd->req.keylen = cmd->buf_used;
      cmd->key = cmd->buffer;
      // process get, by GET/GET_CAS
      process_cmd_get(cmd);
      cmd->buf_used = 0;
      return idx2;
    }

  while (idx1 < nread && buf[idx1] == ' ')
    idx1++;
  if (idx1 == nread)
    return idx1;
  if (buf[idx1] == '\r')
    {
      cmd->state = CMD_CLEAN;
      return idx1 + 1;
    }
  idx2 = idx1;
  while (idx2 < nread && isgraph(buf[idx2]))
    idx2++;
  if (idx2 == idx1)
    {
      cmd->state = ASCII_ERROR;
      return idx2;
    }
  if (idx2 == nread)
    {
      if (idx2 - idx1 >= KEY_MAX_SIZE)
        {
          cmd->state = ASCII_ERROR;
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
      process_cmd_get(cmd);
      cmd->state = CMD_CLEAN;
      return idx2 + 1;
    }
  cmd->req.keylen = idx2 - idx1;
  cmd->key = &buf[idx1];
  // process get/gets
  process_cmd_get(cmd);
  return idx2;
}

void cmd_process_ascii_ready(cmd_handler* cmd)
{
  // in testing we simply reset the cmd
  reset_cmd_handler(cmd);
}

