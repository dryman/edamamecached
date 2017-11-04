#ifndef EDAMAME_CMD_READER_H_
#define EDAMAME_CMD_READER_H_ 1

#include "writer.h"

void edamame_read (cmd_handler *cmd, int nbyte, char *data, ed_writer *writer);

#endif
