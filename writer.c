#include <stdio.h>

#define WRITER_DEFAULT_SIZE 65536

typedef struct ed_buffer ed_buffer;

struct ed_buffer
{
  char* buffer;
  size_t size;
  size_t sent_idx;
  size_t filled_idx;
  ed_buffer* next;
};

struct ed_writer
{
  ed_buffer* head;
  ed_buffer* end;
};

void buffer_init(ed_buffer* buffer, size_t size)
{
  buf->buffer = malloc(size);
  buf->size = size;
  buf->sent_idx = buf->filled_idx = 0;
  buf->next = NULL;
}

void writer_init(ed_writer* writer)
{
  ed_buffer* buffer = malloc(sizeof(ed_buffer));
  buffer_init(buffer, WRITER_DEFAULT_SIZE);
  writer->head = writer->end = buf;
}

bool writer_reserve(ed_writer* writer, size_t nbyte)
{
  ed_buffer* buffer = writer->end;
  if (nbyte > buffer->size - buffer->filled_idx)
    {
      ed_buffer* new_buffer = malloc(sizeof(ed_buffer));
      size_t buf_size = nbyte > WRITER_DEFAULT_SIZE ? nbyte :
       WRITER_DEFAULT_SIZE;
      buffer_init(new_buffer, buf_size);
      buffer->next = new_buffer;
      writer->end = new_buffer;
      buffer = new_buffer;
      return false;
    }
  return true;
}

bool writer_append(ed_writer* writer, const void* buf, size_t nbyte)
{
  ed_buffer* buffer = writer->end;
  if (nbyte > buffer->size - buffer->filled_idx)
    return false;
  memcpy(&buffer->buffer[buffer->filled_idx], buf, nbyte);
  buffer->filled_idx += nbyte;
  return true;
}

bool writer_flush(ed_writer* writer, int fd)
{
  ed_buffer** iter = &writer->head;
  ssize_t written;
  while (true)
    {
      if ((*iter)->sent_idx == (*iter)->filled_idx) {
        if (*iter == writer->end)
          {
            (*iter)->sent_idx = 0;
            (*iter)->filled_idx = 0;
            return true;
          }
        ed_buffer* tmp = *iter;
        *iter = (*iter)->next;
        free(tmp);
      }
      written = write(fd, &(*iter)->buffer[(*iter)->sent_idx],
                      (*iter)->filled_idx - (*iter)->sent_idx);
      if (written < 0)
        {
          if (errno != EWOULDBLOCK)
            return false;
          return true;
        }
      (*iter)->sent_idx += written;
    }
}
