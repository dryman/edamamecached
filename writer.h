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
