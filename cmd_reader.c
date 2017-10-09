#include <ctype.h>
#include "cmd_parser.h"
#include "writer.h"

char* ascii_ok = "ASCII OK\r\n";
char* binary_ok = "BINARY OK\r\n";

void edamame_read(cmd_handler* cmd, int nbyte, char* data, ed_writer* writer)
{
  int idx = 0;

  while (idx < nbyte)
    {
      switch (cmd->state)
        {
        case CMD_CLEAN:
          // TODO Check first byte for binary or ascii.
        case ASCII_PENDING_RAWBUF:
          idx += ascii_cpbuf(cmd, nbyte - idx, &data[idx]);
          break;
        case ASCII_PENDING_PARSE_CMD:
          ascii_parse_cmd(cmd);
          break;
        case ASCII_PENDING_GET_MULTI:
        case ASCII_PENDING_GET_CAS_MULTI:
          idx += cmd_parse_get(cmd, nbyte - idx, &data[idx]);
          break;
        case ASCII_PENDING_VALUE:
          idx += cmd_parse_ascii_value(cmd, nbyte - idx, &data[idx]);
          break;
        case ASCII_CMD_READY:
          // TODO process ascii cmd
          writer_reserve(writer, sizeof(ascii_ok) - 1);
          writer_append(writer, ascii_ok, sizeof(ascii_ok) - 1);
          reset_cmd_handler(cmd);
          break;
        case ASCII_ERROR:
          idx += ascii_cmd_error(cmd, nbyte - idx, &data[idx]);
          break;
        case BINARY_PENDING_RAWBUF:
          idx += binary_cpbuf(cmd, nbyte - idx, &data[idx]);
          break;
        case BINARY_PENDING_PARSE_EXTRA:
          idx += binary_cmd_parse_extra(cmd, nbyte - idx, &data[idx]);
        case BINARY_PENDING_PARSE_KEY:
          idx += binary_cmd_parse_key(cmd, nbyte - idx, &data[idx]);
          break;
        case BINARY_PENDING_VALUE:
          idx += binary_cmd_parse_value(cmd, nbyte - idx, &data[idx]);
          break;
        case BINARY_CMD_READY:
          writer_reserve(writer, sizeof(binary_ok) - 1);
          writer_append(writer, binary_ok, sizeof(binary_ok) - 1);
          reset_cmd_handler(cmd);
        }
    }
}

void process_cmd_get(cmd_handler* cmd)
{
  // do nothing for now
}
