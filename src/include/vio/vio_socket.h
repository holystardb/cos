#ifndef _VIO_SOCKET_H
#define _VIO_SOCKET_H

#include "cm_type.h"


#ifdef __WIN__
#include <winsock2.h>
#include <mstcpip.h>
#include <Ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")

typedef SOCKET                  my_socket;
typedef ulong                   tcp_option_t;
typedef int32                   socklen_t;
#define close_socket            closesocket
#define ioctl_socket            ioctlsocket
#define socket_errno            WSAGetLastError()

#define SOCKET_EINTR            WSAEINTR
#define SOCKET_EAGAIN           WSAEWOULDBLOCK
#define SOCKET_EINPROGRESS      WSAEINPROGRESS
#define SOCKET_ETIMEDOUT        WSAETIMEDOUT
#define SOCKET_EWOULDBLOCK      WSAEWOULDBLOCK
#define SOCKET_EADDRINUSE       WSAEADDRINUSE
#define SOCKET_ECONNRESET       WSAECONNRESET
#define SOCKET_ENFILE           ENFILE
#define SOCKET_EMFILE           EMFILE

#define SOCKBUF_T               char

#else

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h> 
#include <poll.h>
#include <sys/socket.h>

typedef int                     my_socket;
typedef int32                   tcp_option_t;
#define close_socket            close
#define ioctl_socket            ioctl
#define socket_errno            errno

#define SOCKET_EINTR            EINTR
#define SOCKET_EAGAIN           EAGAIN
#define SOCKET_EINPROGRESS      EINPROGRESS
#define SOCKET_ETIMEDOUT        ETIMEDOUT
#define SOCKET_EWOULDBLOCK      EWOULDBLOCK
#define SOCKET_EADDRINUSE       EADDRINUSE
#define SOCKET_ECONNRESET       ECONNRESET
#define SOCKET_ENFILE           ENFILE
#define SOCKET_EMFILE           EMFILE
#define INVALID_SOCKET          -1

#define SOCKBUF_T               void

#endif  /*__WIN__*/


#define M_TCP_SNDBUF_SIZE           (1024 * 1024 * 8)
#define M_TCP_RCVBUF_SIZE           (1024 * 1024 * 8)
#define M_TCP_KEEP_IDLE             120  /* seconds */
#define M_TCP_KEEP_INTERVAL         5
#define M_TCP_KEEP_COUNT            3



#define M_POLL_WAIT_TIME            50  /* mili-seconds*/

#define VIO_CLOSE                   -2
#define VIO_ERROR                   -1
#define VIO_SUCCESS                  0



#ifdef __cplusplus
extern "C" {
#endif

/**
  VIO I/O events.
*/
enum enum_vio_io_event
{
    VIO_IO_EVENT_READ,
    VIO_IO_EVENT_WRITE,
    VIO_IO_EVENT_CONNECT
};


typedef struct st_vio
{
    my_socket                 sock_fd;
    
    struct sockaddr_storage   local;      /* Local internet address */
    socklen_t                 local_len;
    struct sockaddr_storage   remote;     /* Remote internet address */
    socklen_t                 remote_len;

    uint32                    retry_count;
    int32                     read_timeout;   /* Timeout value (ms) for read ops. */
    int32                     write_timeout;  /* Timeout value (ms) for write ops. */

    bool8                     inactive; /* Connection inactive (has been shutdown) */

    int                       error_no;

} Vio;

int vio_ip4_to_sockaddr(const char* host, uint16 port, struct sockaddr_in* sockaddr, socklen_t* sockaddr_len);

int vio_connect(Vio* vio, const char* host, uint16 port, const char* bind_host, int timeout_sec);
int vio_connect_by_addr(Vio* vio, struct sockaddr_in *addr, socklen_t len, int timeout_sec);
int vio_connect_async(Vio* vio, const char* host, uint16 port, const char* bind_host);
int vio_connect_by_addr_async(Vio* vio, struct sockaddr_in *addr, socklen_t len);
int vio_check_connection_status(Vio* vio);

void vio_disconnect(Vio* vio);
bool32 vio_is_connected(Vio* vio);

int vio_socket_poll(struct pollfd* fds, uint32 nfds, int32 timeout_ms);
int vio_io_wait(Vio* vio, enum enum_vio_io_event event, int32 timeout_ms);

int vio_socket_write(Vio* vio, const char* buf, uint32 size);
int vio_write(Vio* vio, const char* buf, uint32 size);

int vio_socket_read(Vio* vio, const char* buf, uint32 size);
int vio_read(Vio* vio, const char* buf, uint32 size);
int vio_try_read(Vio* vio, const char* buf, uint32 buf_size, uint32* recv_size);

int vio_socket_init();
void vio_init(Vio* vio);
int vio_set_blocking(my_socket sock_fd, bool32 non_block, bool32 no_delay);
void vio_set_buffer_size(my_socket sock_fd, uint32 sndbuf_size, uint32 rcvbuf_size);
void vio_set_keep_alive(my_socket sock_fd, uint32 idle, uint32 interval, uint32 count);
void vio_set_linger(my_socket sock_fd, int32 l_onoff, int32 l_linger);

my_socket vio_socket_listen(const char* host, uint16 port);
void vio_close_socket(Vio* vio);

int vio_getnameinfo(const struct sockaddr *sa, char *hostname, uint32 hostname_size, char *port, uint32 port_size, int flags);

#ifdef __cplusplus
}
#endif

#endif  /* _VIO_SOCKET_H */
