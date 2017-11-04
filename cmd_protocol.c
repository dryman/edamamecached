#include "cmd_protocol.h"
#include "util.h"
#include <arpa/inet.h>

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

struct nstr
{
  size_t len;
  const char *str;
};

#define EMAP(x) (((x)&0xf) | (((x)&0x80) >> 3))
#define NSTR(STR)                                                             \
  (struct nstr) { .len = sizeof(STR), .str = STR }

struct nstr errstring[] = {
  [EMAP(STATUS_NOERROR)] = NSTR("No error"),
  [EMAP(STATUS_KEY_NOT_FOUND)] = NSTR("Key not found"),
  [EMAP(STATUS_KEY_EXISTS)] = NSTR("Key exists"),
  [EMAP(STATUS_VAL_TOO_LARGE)] = NSTR("Value too large"),
  [EMAP(STATUS_INVALID_ARG)] = NSTR("Invalid arguments"),
  [EMAP(STATUS_ITEM_NOT_STORED)] = NSTR("Item not stored"),
  [EMAP(STATUS_NON_NUMERIC)] = NSTR("Incr/Decr on non-numeric value"),
  [EMAP(STATUS_VBUCKET_IN_OTHER_SERV)]
  = NSTR("The vbucket belongs to another server"),
  [EMAP(STATUS_AUTH_ERROR)] = NSTR("Authentication error"),
  [EMAP(STATUS_AUTH_CONT)] = NSTR("Authentication continue"),
  [EMAP(STATUS_UNKNOWN_CMD)] = NSTR("Unknown command"),
  [EMAP(STATUS_OUT_OF_MEM)] = NSTR("Out of memory"),
  [EMAP(STATUS_NOT_SUPPORTED)] = NSTR("Not supported"),
  [EMAP(STATUS_INTERNAL_ERR)] = NSTR("Internal error"),
  [EMAP(STATUS_BUSY)] = NSTR("Busy"),
  [EMAP(STATUS_TMP_FAILURE)] = NSTR("Temporary failure"),
};

void
get_errstr(const char **ptr, size_t *len, enum cmd_errcode code)
{
  struct nstr err = errstring[code];
  *len = err.len;
  *ptr = err.str;
}
