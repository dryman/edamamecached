#include "cmd_parser.h"
#include "util.h"
#include <ctype.h>

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

void cmd_process_ascii_ready(cmd_handler* cmd)
{
  // in testing we simply reset the cmd
  reset_cmd_handler(cmd);
}
