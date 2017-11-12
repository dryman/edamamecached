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

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <syslog.h>
#include <unistd.h>

#include "cmd_parser.h"
#include "cmd_reader.h"
#include "lru.h"
#include "writer.h"

#define BUF_SIZE 65536
#define POLL_TIMEOUT 1000

static int port_num = 7500;
// TODO different system has different max value.
static int poll_fd_max = 256;
static lru_t *lru;
static swiper_t *swiper;

struct thread_pipe
{
  int thread_id;
  int pipefd[2];
};

void *
ev_loop(void *context)
{
  struct thread_pipe *tp = (struct thread_pipe *)context;
  int fdbuf[256];
  int rc, poll_fd_num, poll_fd_cnum, fdbuf_num;
  struct pollfd *clientfds = calloc(sizeof(struct pollfd), poll_fd_max);
  cmd_handler *cmds = calloc(sizeof(cmd_handler), poll_fd_max);
  ed_writer *writers = calloc(sizeof(ed_writer), poll_fd_max);
  char buffer[BUF_SIZE];

  clientfds[0].fd = tp->pipefd[0];
  clientfds[0].events = POLLIN;
  for (int i = 1; i < poll_fd_max; i++)
    {
      clientfds[i].fd = -1;
    }
  poll_fd_num = 1;

  syslog(LOG_INFO, "init thread %d", tp->thread_id);

  while (1)
    {
      rc = poll(clientfds, poll_fd_num, POLL_TIMEOUT);
      if (rc < 0)
        {
          syslog(LOG_ERR, "poll error: %s", strerror(errno));
          continue;
        }
      if (rc == 0)
        continue;
      poll_fd_cnum = poll_fd_num;

      // First, check for accept
      if (clientfds[0].revents & POLLIN)
        {
          syslog(LOG_DEBUG, "accepting fd in thread %d", tp->thread_id);
          rc = read(clientfds[0].fd, fdbuf, sizeof(fdbuf));
          fdbuf_num = rc / sizeof(int);
          int j = 1;
          for (int i = 0; i < fdbuf_num; i++)
            {
              while (j < poll_fd_max && clientfds[j].fd >= 0)
                j++;
              // TODO j might overflow
              clientfds[j].fd = fdbuf[i];
              clientfds[j].events = POLLIN;
              reset_cmd_handler(&cmds[j]);
              writer_init(&writers[j], WRITER_DEFAULT_SIZE);
              if (j >= poll_fd_num)
                poll_fd_num = j + 1;
            }
        }

      // Read/Write from rest of fds
      for (int i = 1; i < poll_fd_cnum; i++)
        {
          if (clientfds[i].fd < 0)
            continue;
          if (!(clientfds[i].revents & POLLIN))
            continue;
          bool close_fd = false;
          rc = recv(clientfds[i].fd, buffer, sizeof(buffer), 0);
          if (rc > 0)
            {
              edamame_read(lru, &cmds[i], rc, buffer, &writers[i], &close_fd);
              writer_flush(&writers[i], clientfds[i].fd);
            }
          if (rc <= 0 || close_fd)
            {
              // We only read once per poll per fd, so we should not see
              // EWOULDBLOCK here.
              // This load balances each handle, but performance may suffer
              // from poll implementation. More poll system calls in this case.
              close(clientfds[i].fd);
              clientfds[i].fd = -1;
              syslog(LOG_DEBUG, "connection fd %d closed in thread %d", i,
                     tp->thread_id);
              if (i == poll_fd_num - 1)
                {
                  for (int j = i; j > 0; j--)
                    {
                      if (clientfds[j].fd >= 0)
                        {
                          poll_fd_num = j + 1;
                          break;
                        }
                    }
                }
            }
        }
    }
  return NULL;
}

int
main(int argc, char **argv)
{
  int c, num_threads = 1;
  struct sockaddr_in addr;
  const int on = 1;
  int listen_fd, rc, round_robin = 0;
  struct pollfd listen_poll[1];

  while ((c = getopt(argc, argv, "t:p:")) != -1)
    {
      switch (c)
        {
        case 't':
          num_threads = atoi(optarg);
          // TODO only linux benefits from multithread. Print warning message
          // for setting threads in other system.
          break;
        case 'p':
          port_num = atoi(optarg);
          break;
        default:
          printf("Usage: %s -t thread_num -p port\n", argv[0]);
          exit(-1);
        }
    }
  openlog("edamame", LOG_PERROR, LOG_USER);
  // setlogmask(LOG_UPTO(LOG_ERR));

  lru = lru_init(1 << 25, 20, 4096);
  swiper = swiper_init(lru, 1 << 22);

  pthread_t threads[num_threads];
  struct thread_pipe tpipes[num_threads];
  int fdbuf[num_threads][256];
  int fdbuf_cnt[num_threads];
  for (int i = 0; i < num_threads; i++)
    {
      tpipes[i].thread_id = i;
      pipe(tpipes[i].pipefd);
      pthread_create(&threads[i], NULL, ev_loop, &tpipes[i]);
    }

  addr = (struct sockaddr_in){
    .sin_family = AF_INET,
    .sin_port = htons(port_num),
    .sin_addr.s_addr = htonl(INADDR_ANY),
  };
  if ((listen_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
      syslog(LOG_ERR, "Create ipv4 tcp socket failed: %s", strerror(errno));
      exit(-1);
    }
  if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)))
    {
      syslog(LOG_ERR, "Set socket opt SO_REUSEADDR failed: %s",
             strerror(errno));
      exit(-1);
    }
  rc = fcntl(listen_fd, F_GETFL, 0);
  fcntl(listen_fd, F_SETFL, rc | O_NONBLOCK);
  rc = fcntl(listen_fd, F_GETFL, 0);
  if (!(rc & O_NONBLOCK))
    {
      syslog(LOG_ERR, "Unable to set nonblocking socket");
      exit(-1);
    }
  if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)))
    {
      syslog(LOG_ERR, "Bind failed: %s", strerror(errno));
      exit(-1);
    }
  if (listen(listen_fd, SOMAXCONN))
    {
      syslog(LOG_ERR, "listen failed: %s", strerror(errno));
      exit(-1);
    }
  listen_poll[0].fd = listen_fd;
  listen_poll[0].events = POLLIN;

  while (1)
    {
      rc = poll(listen_poll, 1, POLL_TIMEOUT);
      if (rc < 0)
        {
          perror("poll err");
          exit(-1);
        }
      if (rc == 0)
        continue;

      // get as much accept as possible
      for (int i = 0; i < num_threads; i++)
        fdbuf_cnt[i] = 0;
      while (1)
        {
          int clientfd = accept(listen_poll[0].fd, NULL, NULL);
          if (clientfd < 0)
            {
              if (errno != EWOULDBLOCK)
                {
                  syslog(LOG_ERR, "Accept failed: %s", strerror(errno));
                }
              // flush clientfds to client threads
              for (int i = 0; i < num_threads; i++)
                {
                  if (fdbuf_cnt[i] == 0)
                    continue;
                  syslog(LOG_DEBUG, "Forward fd to thread %d", i);
                  write(tpipes[i].pipefd[1], &fdbuf[i][0],
                        sizeof(int) * fdbuf_cnt[i]);
                }
              break;
            }
          fdbuf[round_robin][fdbuf_cnt[round_robin]] = clientfd;
          fdbuf_cnt[round_robin]++;
          if (++round_robin >= num_threads)
            round_robin = 0;
        }
    }
}
