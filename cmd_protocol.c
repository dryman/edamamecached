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
  [EMAP(PROTOCOL_BINARY_RESPONSE_SUCCESS)] = "No error",
  [EMAP(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT)] = "Key not found",
  [EMAP(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS)] = "Key exists",
  [EMAP(PROTOCOL_BINARY_RESPONSE_E2BIG)] = "Value too large",
  [EMAP(PROTOCOL_BINARY_RESPONSE_EINVAL)] = "Invalid arguments",
  [EMAP(PROTOCOL_BINARY_RESPONSE_NOT_STORED)] = "Item not stored",
  [EMAP(PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL)] = "Incr/Decr on non-numeric value",
  [EMAP(PROTOCOL_BINARY_RESPONSE_VBUCKET_IN_OTHER_SERV)]
  = "The vbucket belongs to another server",
  [EMAP(PROTOCOL_BINARY_RESPONSE_AUTH_ERROR)] = "Authentication error",
  [EMAP(PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE)] = "Authentication continue",
  [EMAP(PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND)] = "Unknown command",
  [EMAP(PROTOCOL_BINARY_RESPONSE_ENOMEM)] = "Out of memory",
  [EMAP(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED)] = "Not supported",
  [EMAP(PROTOCOL_BINARY_RESPONSE_INTERNAL_ERR)] = "Internal error",
  [EMAP(PROTOCOL_BINARY_RESPONSE_BUSY)] = "Busy",
  [EMAP(PROTOCOL_BINARY_RESPONSE_TMP_FAILURE)] = "Temporary failure",
};

void
get_errstr(const char **ptr, size_t *len, enum cmd_rescode code)
{
  const char *err = errstring[code];
  *len = strlen(err);
  *ptr = err;
}
