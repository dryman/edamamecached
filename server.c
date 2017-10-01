#include <stdio.h>
#include <stdlib.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>

static int port_num = 7500;
// max value: 32767
static int poll_fd_max = 256;

#define BUF_SIZE 65536
#define POLL_TIMEOUT 1000

void* ev_loop(void* context) {
  int thread_id, listen_fd, rc,
      poll_fd_num, poll_fd_cnum;
  const int on = 1;
  struct sockaddr_in addr;
  struct pollfd* poll_fds;
  char buffer[BUF_SIZE];
  
  thread_id = (int)(size_t)context;

  // TODO support ipv6 bindings from options
  addr = (struct sockaddr_in) {
    .sin_family = AF_INET,
    .sin_port = htons(port_num),
    .sin_addr.s_addr = htonl(INADDR_ANY),
  };
  if ((listen_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    // TODO use syslog and strerror for error msg
    perror("create socket on localhost failed");
    exit(-1);
  }
  if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))) {
    perror("set socket opt SO_REUSEADDR failed");
    exit(-1);
  }
  if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on))) {
    perror("set socket opt SO_REUSEPORT failed");
    exit(-1);
  }
  rc = fcntl(listen_fd, F_GETFL, 0);
  fcntl(listen_fd, F_SETFL, rc | O_NONBLOCK);
  rc = fcntl(listen_fd, F_GETFL, 0);
  if (!(rc & O_NONBLOCK)) {
    fprintf(stderr, "setting non blocking socket failed\n");
    exit(-1);
  }
  if (bind(listen_fd, (struct sockaddr*)&addr, sizeof(addr))) {
    perror("bind localhost failed");
    exit(-1);
  }
  if (listen(listen_fd, SOMAXCONN)) {
    perror("listen failed");
    exit(-1);
  }
  printf("listening from thread id: %d\n", thread_id);

  poll_fds = calloc(poll_fd_max, sizeof(struct pollfd));
  for (int i = 0; i < poll_fd_max; i++)
    poll_fds[i].fd = -1;
  poll_fds[0].fd = listen_fd;
  poll_fds[0].events = POLLIN;
  poll_fd_num = 1;

  while (1) {
    rc = poll(poll_fds, poll_fd_num, POLL_TIMEOUT);
    if (rc < 0) {
      // TODO may need to handle signal here.
      perror("poll error");
      exit(-1);
    }
    poll_fd_cnum = poll_fd_num;

    // First, check for accept
    if (poll_fds[0].revents & POLLIN) {
      for (int i = 1; i < poll_fd_max; i++) {
        if (poll_fds[i].fd >= 0) continue;
        poll_fds[i].fd = accept(listen_fd, NULL, NULL);
        if (poll_fds[i].fd < 0) {
          if (errno != EWOULDBLOCK) {
            perror("accept failed");
            exit(-1);
          }
          break;
          // EWOULDBLOCK means we connected to all pending accept.
        }
        printf("accepting conn %d in thread %d\n", i, thread_id);
        rc = fcntl(poll_fds[i].fd, F_GETFL, 0);
        fcntl(poll_fds[i].fd, F_SETFL, rc | O_NONBLOCK);
        rc = fcntl(poll_fds[i].fd, F_GETFL, 0);
        if (!(rc & O_NONBLOCK)) {
          fprintf(stderr, "setting nonblock client fd failed\n");
          exit(-1);
        }
        poll_fds[i].events = POLLIN;
        printf("new conn %d estabilshed in thread %d\n", i, thread_id);
        if (i >= poll_fd_num)
          poll_fd_num = i + 1;
      }
      // print running out of poll_fds
    }

    // Read/Write from rest of fds
    for (int i = 1; i < poll_fd_cnum; i++)
      {
        if (poll_fds[i].fd < 0) continue;
        if (!(poll_fds[i].revents & POLLIN)) continue;
        rc = recv(poll_fds[i].fd, buffer, sizeof(buffer), 0);
        if (rc <= 0) {
          // We only read once per poll per fd, so we should not see
          // EWOULDBLOCK here.
          // This load balances each handle, but performance may suffer from
          // poll implementation. More poll system calls in this case.
          close(poll_fds[i].fd);
          poll_fds[i].fd = -1;
          printf("connection fd %d closed in thread %d\n", i, thread_id);
          if (i == poll_fd_num - 1) {
            for (int j = i; j > 0; j--) {
              if (poll_fds[i].fd >= 0) {
                poll_fd_num = j + 1;
                break;
              }
            }
          }
        }
        // TODO handle read data.
      }
  }
  return NULL;
}

int main(int argc, char** argv)
{
  int c, num_threads = 1;

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
          printf("Usage: %s -t thread_num\n", argv[0]);
          exit(-1);
        }
    }

  pthread_t threads[num_threads];
  for (size_t i = 0; i < num_threads; i++)
    pthread_create(&threads[i], NULL, ev_loop, (void*)i);

  // TODO where do I handle signals?
  for (int i = 0; i < num_threads; i++)
    pthread_join(threads[i], NULL);
  return 0;
}
