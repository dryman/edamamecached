#include <arpa/inet.h>
#include "cmd_protocol.h"
#include "util.h"

void cmd_req_ntoh(cmd_req_header* req)
{
  req->keylen = ntohs(req->keylen);
  req->vbucket = ntohs(req->vbucket);
  req->bodylen = ntohl(req->bodylen);
  req->cas = ntohll(req->cas);
}

void cmd_res_hton(cmd_res_header* res)
{
  res->keylen = htons(res->keylen);
  res->status = htons(res->status);
  res->bodylen = htonl(res->bodylen);
  res->cas = htonll(res->cas);
}
