#include <syslog.h>
#include <ctype.h>
#include "cmd_parser.h"
#include "writer.h"

char EOL[] = "\r\n";
char ascii_ok[] = "ASCII OK\r\n";
char binary_ok[] = "BINARY OK\r\n";
char txt_stored[] = "STORED\r\n";

void edamame_read(cmd_handler* cmd, int nbyte, char* data, ed_writer* writer)
{
  int idx = 0;

  while (idx < nbyte || cmd->state != CMD_CLEAN)
    {
      switch (cmd->state)
        {
        case CMD_CLEAN:
          if (data[0] == '\x80')
            {
              idx += binary_cpbuf(cmd, nbyte - idx, &data[idx], writer);
            }
          else
            {
              idx += ascii_cpbuf(cmd, nbyte - idx, &data[idx], writer);
            }
          syslog(LOG_DEBUG, "current state: %d", cmd->state);
          break;
        case ASCII_PENDING_RAWBUF:
          idx += ascii_cpbuf(cmd, nbyte - idx, &data[idx], writer);
          syslog(LOG_DEBUG, "current state: %d", cmd->state);
          break;
        case ASCII_PENDING_PARSE_CMD:
          ascii_parse_cmd(cmd, writer);
          break;
        case ASCII_PENDING_GET_MULTI:
        case ASCII_PENDING_GET_CAS_MULTI:
          idx += cmd_parse_get(cmd, nbyte - idx, &data[idx], writer);
          break;
        case ASCII_PENDING_VALUE:
          idx += cmd_parse_ascii_value(cmd, nbyte - idx, &data[idx], writer);
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

void process_cmd_get(cmd_handler* cmd, ed_writer* writer)
{
  writer_reserve(writer, cmd->req.keylen);
  writer_append(writer, cmd->key, cmd->req.keylen);
  writer_reserve(writer, sizeof(EOL) - 1);
  writer_append(writer, EOL, sizeof(EOL) - 1);
  //writer_reserve(writer, sizeof(txt_stored) - 1);
  //writer_append(writer, txt_stored, sizeof(txt_stored) - 1);
  // do nothing for now
}
