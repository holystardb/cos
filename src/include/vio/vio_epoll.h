#ifndef _VIO_EPOLL_H
#define _VIO_EPOLL_H

#include "cm_type.h"
#include "vio_socket.h"

#ifndef __WIN__
#include <sys/epoll.h>
#include <unistd.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif


#ifdef __WIN__

#define EPOLLIN          0x0001
#define EPOLLPRI         0x0002
#define EPOLLOUT         0x0004
#define EPOLLRDNORM      0x0040
#define EPOLLRDBAND      0x0080
#define EPOLLWRNORM      0x0100
#define EPOLLWRBAND      0x0200
#define EPOLLMSG         0x0400
#define EPOLLERR         0x0008
#define EPOLLHUP         0x0010
#define EPOLLRDHUP       0x2000
#define EPOLLWAKEUP      (1u << 29)
#define EPOLLONESHOT     (1u << 30)
#define EPOLLET          (1u << 31)

#define EPOLL_CTL_ADD    1
#define EPOLL_CTL_MOD    2
#define EPOLL_CTL_DEL    3

typedef union epoll_data
{
    void      *ptr;
    my_socket  fd;
    uint32     u32;
    uint64     u64;
} epoll_data_t;

struct epoll_event
{
    uint32          events;
    epoll_data_t    data;
};

int epoll_init();
int epoll_ctl(int epfd, int op, my_socket fd, struct epoll_event *event);
int epoll_create1(int flags);
int epoll_wait(int epfd, struct epoll_event *events, int maxevents, int timeout_ms);

#endif

#define EPOLLTIMEOUT      (1u << 28)


int epoll_close(int epfd);

#ifdef __cplusplus
}
#endif

#endif  /* _VIO_EPOLL_H */
