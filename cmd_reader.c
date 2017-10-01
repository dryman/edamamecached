#include <uv.h>
#include <ctype.h>
#include "cmd_parser.h"
#include "util.h"

void edamame_server_close(uv_handle_t*);

void edamame_client_close(uv_handle_t* client);
void edamame_client_shutdown(uv_shutdown_t*, int);

void edamame_client_read(uv_stream_t* client, ssize_t nbyte, const uv_buf_t* buf)
{
  cmd_handler* cmd = (cmd_handler*)client;
  ssize_t idx = 0;
  uv_shutdown_t *shutdown_req;

  if (nbyte == -1)
    {
      reset_cmd_handler(cmd);
      // TODO faster malloc for small obj
      free(buf->base);
      shutdown_req = malloc(sizeof(uv_shutdown_t));
      uv_shutdown(shutdown_req, client, edamame_client_shutdown);
      return;
    }

  if (nbyte == 0)
    {
      // TODO faster malloc for small obj
      free(buf->base);
      return;
    }

  while (idx < nbyte)
    {
      switch (cmd->state)
        {
        case CMD_CLEAN:
        case ASCII_PENDING_RAWBUF:
          idx += ascii_cpbuf(cmd, nbyte - idx, &buf->base[idx]);
          break;
        case ASCII_PENDING_PARSE_CMD:
          ascii_parse_cmd(cmd);
          break;
        case ASCII_PENDING_GET_MULTI:
          idx += cmd_parse_get(cmd, nbyte - idx, &buf->base[idx]);
          break;
        case ASCII_PENDING_VALUE:
          idx += cmd_parse_ascii_value(cmd, nbyte - idx, &buf->base[idx]);
          break;
        case ASCII_CMD_READY:
          // TODO process ascii cmd
          printf("ascii cmd parsed\n");
          break;
        case ASCII_ERROR:
          idx += ascii_cmd_error(cmd, nbyte - idx, &buf->base[idx]);
          break;
        case BINARY_PENDING_RAWBUF:
          idx += binary_cpbuf(cmd, nbyte - idx, &buf->base[idx]);
          break;
        case BINARY_PENDING_PARSE_EXTRA:
          idx += binary_cmd_parse_extra(cmd, nbyte - idx, &buf->base[idx]);
        case BINARY_PENDING_PARSE_KEY:
          idx += binary_cmd_parse_key(cmd, nbyte - idx, &buf->base[idx]);
          break;
        case BINARY_PENDING_VALUE:
          idx += binary_cmd_parse_value(cmd, nbyte - idx, &buf->base[idx]);
          break;
        case BINARY_CMD_READY:
          printf("binary cmd parsed\n");
        }
    }
  free(buf->base);
}

void cmd_process_ascii_ready(cmd_handler* cmd)
{
  // in testing we simply reset the cmd
  reset_cmd_handler(cmd);
}
