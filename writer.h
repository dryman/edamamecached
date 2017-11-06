#ifndef EDAMAME_WRITER_H_
#define EDAMAME_WRITER_H_ 1

#include <stdbool.h>
#include <stddef.h>
#include <sys/types.h>

#define WRITER_DEFAULT_SIZE 65536

typedef struct ed_writer ed_writer;
typedef struct ed_buffer ed_buffer;

void writer_init(ed_writer *writer, size_t size);
bool writer_reserve(ed_writer *writer, size_t nbyte);
bool writer_append(ed_writer *writer, const void *buf, size_t nbyte);
bool writer_flush(ed_writer *writer, int fd);

struct ed_writer
{
  ed_buffer *head;
  ed_buffer *end;
  size_t writer_default_size;
};

#endif
