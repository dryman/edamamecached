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

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <unistd.h>

#include "writer.h"

struct ed_buffer
{
  char *buffer;
  size_t size;
  size_t sent_idx;
  size_t filled_idx;
  ed_buffer *next;
};

void
buffer_init(ed_buffer *buffer, size_t size)
{
  buffer->buffer = malloc(size);
  buffer->size = size;
  buffer->sent_idx = buffer->filled_idx = 0;
  buffer->next = NULL;
}

void
writer_init(ed_writer *writer, size_t size)
{
  if (!writer->head)
    {
      ed_buffer *buffer = malloc(sizeof(ed_buffer));
      buffer_init(buffer, size);
      writer->head = writer->end = buffer;
      writer->writer_default_size = size;
      return;
    }
  for (ed_buffer *iter = writer->head; iter != writer->end;)
    {
      ed_buffer *tmp = iter;
      iter = iter->next;
      free(tmp);
    }
  writer->head->sent_idx = 0;
  writer->head->filled_idx = 0;
}

bool
writer_reserve(ed_writer *writer, size_t nbyte)
{
  ed_buffer *buffer = writer->end;
  if (nbyte > buffer->size - buffer->filled_idx)
    {
      ed_buffer *new_buffer = malloc(sizeof(ed_buffer));
      size_t buf_size = nbyte > writer->writer_default_size
                            ? nbyte
                            : writer->writer_default_size;
      buffer_init(new_buffer, buf_size);
      buffer->next = new_buffer;
      writer->end = new_buffer;
      buffer = new_buffer;
      return false;
    }
  return true;
}

bool
writer_append(ed_writer *writer, const void *buf, size_t nbyte)
{
  ed_buffer *buffer = writer->end;
  if (nbyte > buffer->size - buffer->filled_idx)
    return false;
  memcpy(&buffer->buffer[buffer->filled_idx], buf, nbyte);
  buffer->filled_idx += nbyte;
  return true;
}

bool
writer_flush(ed_writer *writer, int fd)
{
  ed_buffer **iter = &writer->head;
  ssize_t written;
  while (true)
    {
      if ((*iter)->sent_idx == (*iter)->filled_idx)
        {
          if (*iter == writer->end)
            {
              (*iter)->sent_idx = 0;
              (*iter)->filled_idx = 0;
              return true;
            }
          ed_buffer *tmp = *iter;
          *iter = (*iter)->next;
          free(tmp);
          continue;
        }
      written = write(fd, &(*iter)->buffer[(*iter)->sent_idx],
                      (*iter)->filled_idx - (*iter)->sent_idx);
      if (written < 0)
        {
          if (errno != EWOULDBLOCK)
            return false;
          // TODO need to register poll
          return true;
        }
      (*iter)->sent_idx += written;
    }
}
