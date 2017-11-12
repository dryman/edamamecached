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

#include "cmd_protocol.h"
#include "util.h"
#include <arpa/inet.h>
#include <string.h>

void
cmd_req_ntoh(cmd_req_header *req)
{
  req->keylen = ntohs(req->keylen);
  req->vbucket = ntohs(req->vbucket);
  req->bodylen = ntohl(req->bodylen);
  req->cas = ntohll(req->cas);
}

void
cmd_res_hton(cmd_res_header *res)
{
  res->keylen = htons(res->keylen);
  res->status = htons(res->status);
  res->bodylen = htonl(res->bodylen);
  res->cas = htonll(res->cas);
}

#define EMAP(x) (((x)&0xf) | (((x)&0x80) >> 3))

const char *errstring[] = {
  [EMAP(STATUS_NOERROR)] = "No error",
  [EMAP(STATUS_KEY_NOT_FOUND)] = "Key not found",
  [EMAP(STATUS_KEY_EXISTS)] = "Key exists",
  [EMAP(STATUS_VAL_TOO_LARGE)] = "Value too large",
  [EMAP(STATUS_INVALID_ARG)] = "Invalid arguments",
  [EMAP(STATUS_ITEM_NOT_STORED)] = "Item not stored",
  [EMAP(STATUS_NON_NUMERIC)] = "Incr/Decr on non-numeric value",
  [EMAP(STATUS_VBUCKET_IN_OTHER_SERV)]
  = "The vbucket belongs to another server",
  [EMAP(STATUS_AUTH_ERROR)] = "Authentication error",
  [EMAP(STATUS_AUTH_CONT)] = "Authentication continue",
  [EMAP(STATUS_UNKNOWN_CMD)] = "Unknown command",
  [EMAP(STATUS_OUT_OF_MEM)] = "Out of memory",
  [EMAP(STATUS_NOT_SUPPORTED)] = "Not supported",
  [EMAP(STATUS_INTERNAL_ERR)] = "Internal error",
  [EMAP(STATUS_BUSY)] = "Busy",
  [EMAP(STATUS_TMP_FAILURE)] = "Temporary failure",
};

void
get_errstr(const char **ptr, size_t *len, enum cmd_errcode code)
{
  const char *err = errstring[code];
  *len = strlen(err);
  *ptr = err;
}
