#include "vio_socket.h"
#include "cm_log.h"
#include "cm_thread.h"

#ifdef __cplusplus
extern "C" {
#endif

int vio_socket_init()
{
#ifdef __WIN__
    struct WSAData wd;
    uint16 version = MAKEWORD(1, 1);
    if (WSAStartup(version, &wd) != 0) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "error for WSAStartup, errno=%d", socket_errno);
        return -1;
    }
#endif
    return 0;
}

my_socket vio_create_socket(int domain, int type, int protocol)
{
    //my_socket sock_fd = (my_socket)socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    my_socket sock_fd = (my_socket)socket(domain, type, protocol);
    if (INVALID_SOCKET == sock_fd) {
        return sock_fd;
    }
    return sock_fd;
}

void vio_close_socket(Vio* vio)
{
    if  (INVALID_SOCKET != vio->sock_fd) {
        close_socket(vio->sock_fd);
        vio->sock_fd = INVALID_SOCKET;
    }
    vio->inactive = TRUE;
    vio->error_no = 0;
}

int vio_set_blocking(my_socket sock_fd, bool32 non_block, bool32 no_delay)
{
#ifdef __WIN__
    {
        int nodelay = 1;
        ulong arg = non_block ? 1 : 0;
        if (ioctlsocket(sock_fd, FIONBIO, &arg)) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_set_blocking: sock fd %d, error %d", sock_fd, socket_errno);
            return -1;
        }
        if (setsockopt(sock_fd, IPPROTO_TCP, TCP_NODELAY, (const char*)&nodelay, sizeof(nodelay))) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_set_blocking: sock fd %d, error %d", sock_fd, socket_errno);
            return -1;
        }
    }
#else
    {
        int flags;
        int nodelay = 1;

        if ((flags= fcntl(sock_fd, F_GETFL, NULL)) < 0) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_set_blocking: sock fd %d, error %d", sock_fd, socket_errno);
            return -1;
        }

        if (non_block)
          flags |= O_NONBLOCK;
        else
          flags &= ~O_NONBLOCK;
    
        if (fcntl(sock_fd, F_SETFL, flags) == -1) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_set_blocking: sock fd %d, error %d", sock_fd, socket_errno);
            return -1;
        }
        if (setsockopt(sock_fd, IPPROTO_TCP, TCP_NODELAY, (void*)&nodelay, sizeof(nodelay)))  {
            LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_set_blocking: sock fd %d, error %d", sock_fd, socket_errno);
            return -1;
        }
    }
#endif
    return 0;
}

void vio_set_buffer_size(my_socket sock_fd, uint32 sndbuf_size, uint32 rcvbuf_size)
{
    (void)setsockopt(sock_fd, SOL_SOCKET, SO_SNDBUF, (char *)&sndbuf_size, sizeof(uint32));
    (void)setsockopt(sock_fd, SOL_SOCKET, SO_RCVBUF, (char *)&rcvbuf_size, sizeof(uint32));
}

void vio_set_socket_timeout(my_socket sock_fd, uint32 timeout_ms)
{
#ifdef __WIN__
    int timeout = timeout_ms;
    (void)setsockopt(sock_fd, SOL_SOCKET, SO_SNDTIMEO, (char *)&timeout, sizeof(int));
    (void)setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, (char *)&timeout, sizeof(int));
#else
    struct timeval tv = { timeout_ms / 1000, 0};
    (void)setsockopt(sock_fd, SOL_SOCKET, SO_SNDTIMEO, (char *)&tv, sizeof(tv));
    (void)setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv, sizeof(tv));
#endif
}

void vio_set_keep_alive(my_socket sock_fd, uint32 idle, uint32 interval, uint32 count)
{
#ifdef __WIN__
    struct tcp_keepalive vals;
    DWORD  bytes;

    vals.keepaliveinterval = interval;
    vals.keepalivetime = idle;
    vals.onoff = 1;
    (void)WSAIoctl(sock_fd, SIO_KEEPALIVE_VALS, &vals, sizeof(vals), NULL, 0, &bytes, NULL, NULL);
#else
    int32 option = 1;

    (void)setsockopt(sock_fd, SOL_SOCKET, SO_KEEPALIVE, (void *)&option, sizeof(int32));
    (void)setsockopt(sock_fd, SOL_TCP, TCP_KEEPIDLE, (void *)&idle, sizeof(int32));
    (void)setsockopt(sock_fd, SOL_TCP, TCP_KEEPINTVL, (void *)&interval, sizeof(int32));
    (void)setsockopt(sock_fd, SOL_TCP, TCP_KEEPCNT, (void *)&count, sizeof(int32));
#endif
}

void vio_set_linger(my_socket sock_fd, int32 l_onoff, int32 l_linger)
{
    struct linger lger;
    lger.l_onoff = l_onoff;
    lger.l_linger = l_linger;
    (void)setsockopt(sock_fd, SOL_SOCKET, SO_LINGER, (char *)&lger, sizeof(struct linger));
}

int vio_set_reuse_addr(my_socket sock_fd)
{
#ifndef __WIN__
    int on = 1;
    // avoid: Address already in use
    int result = setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, (char*)&on, sizeof(on) );
    if (-1 == result) {
        return -1;
    }
#endif

    return 0;
}

bool32 vio_should_retry(Vio *vio)
{
    return (vio->error_no == SOCKET_EINTR);
}

bool32 vio_was_timeout(Vio *vio)
{
    return (vio->error_no == SOCKET_ETIMEDOUT);
}


int vio_get_ip_version(const char* host)
{
    return AF_INET;
}

int vio_ip6_to_sockaddr(const char* host, uint16 port, struct sockaddr_in6* sockaddr, socklen_t* sockaddr_len)
{
    return VIO_SUCCESS;
}

int vio_ip4_to_sockaddr(const char* host, uint16 port, struct sockaddr_in* sockaddr, socklen_t* sockaddr_len)
{
    if (sockaddr_len) {
        *sockaddr_len = sizeof(struct sockaddr_in);
    }
    memset(sockaddr, 0, sizeof(struct sockaddr_in));
    sockaddr->sin_family = AF_INET;
    sockaddr->sin_port = htons(port);
    
#ifdef __WIN__
    if (InetPton(AF_INET, host, &sockaddr->sin_addr.s_addr) != 1)
#else
    sockaddr->sin_addr.s_addr = inet_addr(host);
    if (sockaddr->sin_addr.s_addr == (in_addr_t)(-1) ||
        (inet_pton(AF_INET, host, &sockaddr->sin_addr.s_addr) != 1))
#endif
    {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_ip4_to_sockaddr: error for convert %s to sockaddr, error=%d", host, socket_errno);
        return VIO_ERROR;
    }

    return VIO_SUCCESS;
}


int vio_ip_to_sockaddr(const char* host, uint16 port, int *domain, struct sockaddr* sockaddr, socklen_t* sockaddr_len)
{
    *domain = vio_get_ip_version(host);
    
    if (*domain == AF_INET)
    {
        return vio_ip4_to_sockaddr(host, port, (struct sockaddr_in*)sockaddr, sockaddr_len);
    }

    return vio_ip6_to_sockaddr(host, port, (struct sockaddr_in6*)sockaddr, sockaddr_len);
}

static int vio_connect_poll(Vio* vio, int timeout_ms)
{
    int           ret;
    struct pollfd fds;

    fds.fd = vio->sock_fd;
    fds.events = POLLOUT | POLLERR | POLLHUP;
    ret = vio_socket_poll(&fds, 1, timeout_ms);
    if (ret == 0) {
        vio->error_no = SOCKET_ETIMEDOUT;
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_connect_by_addr: timeout for connect");
        goto err_exit;
    }
    if (ret <= 0) {
        vio->error_no = socket_errno;
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_connect_by_addr: error for vio_socket_poll, error %d", socket_errno);
        goto err_exit;
    }
    if (fds.revents & (POLLERR | POLLHUP)) {
        vio->error_no = socket_errno;
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_connect_by_addr: can not connect to server, error %d", socket_errno);
        goto err_exit;
    }

    return VIO_SUCCESS;

err_exit:

    vio_close_socket(vio);

    return VIO_ERROR;
}

int vio_connect(Vio* vio, const char* host, uint16 port, const char* bind_host, int timeout_sec)
{
    if (VIO_SUCCESS != vio_connect_async(vio, host, port, bind_host)) {
        return VIO_ERROR;
    }
    if (VIO_SUCCESS != vio_connect_poll(vio, timeout_sec * 1000)) {
        return VIO_ERROR;
    }
    return vio_check_connection_status(vio);
}

int vio_connect_async(Vio* vio, const char* host, uint16 port, const char* bind_host)
{
    int      ret, domain;

    vio->sock_fd = INVALID_SOCKET;
    if (bind_host != NULL && bind_host[0] != '\0') {
        ret = vio_ip_to_sockaddr(bind_host, 0, &domain, (struct sockaddr *)&vio->local, &vio->local_len);
        if (VIO_SUCCESS != ret) {
            return VIO_ERROR;
        }
        
        vio->sock_fd = vio_create_socket(domain, SOCK_STREAM, 0);
        if (INVALID_SOCKET == vio->sock_fd) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_connect_async: can not create socket");
            return VIO_ERROR;
        }
        if (bind(vio->sock_fd, (struct sockaddr *)&vio->local, vio->local_len) != 0) {
            vio_close_socket(vio);
            LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_connect_async: can not bind ipaddress(%s)", bind_host);
            return VIO_ERROR;
        }
    }

    ret = vio_ip_to_sockaddr(host, port, &domain, (struct sockaddr *)&vio->remote, &vio->remote_len);
    if (VIO_SUCCESS != ret) {
        vio_close_socket(vio);
        return VIO_ERROR;
    }
    if (INVALID_SOCKET == vio->sock_fd) {
        vio->sock_fd = vio_create_socket(domain, SOCK_STREAM, 0);
        if (INVALID_SOCKET == vio->sock_fd) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_connect_async: can not create socket");
            return VIO_ERROR;
        }
    }

    vio_set_blocking(vio->sock_fd, TRUE, TRUE);
    vio_set_buffer_size(vio->sock_fd, M_TCP_SNDBUF_SIZE, M_TCP_RCVBUF_SIZE);
    ret = connect(vio->sock_fd, (struct sockaddr *)&vio->remote, vio->remote_len);
    if (0 != ret) {
        vio->error_no = socket_errno;
        if (vio->error_no == SOCKET_EINPROGRESS || vio->error_no == SOCKET_EAGAIN ||
            vio->error_no == SOCKET_EWOULDBLOCK) {
            vio->error_no = 0;
        } else {
            vio_close_socket(vio);
            LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_connect_async: can not connect to server(%s:%d), error %d",
                host, port, vio->error_no);
            return VIO_ERROR;
        }
    }

    return VIO_SUCCESS;
}

int vio_connect_by_addr(Vio* vio, struct sockaddr_in *addr, socklen_t len, int timeout_sec)
{
    if (VIO_SUCCESS != vio_connect_by_addr_async(vio, addr, len)) {
        return VIO_ERROR;
    }
    if (VIO_SUCCESS != vio_connect_poll(vio, timeout_sec * 1000)) {
        return VIO_ERROR;
    }
    return vio_check_connection_status(vio);
}

int vio_connect_by_addr_async(Vio* vio, struct sockaddr_in *addr, socklen_t len)
{
    int    ret;
    
    vio->sock_fd = vio_create_socket(AF_INET, SOCK_STREAM, 0);
    if (INVALID_SOCKET == vio->sock_fd) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_connect_by_addr_async: can not create socket");
        return VIO_ERROR;
    }
    vio_set_blocking(vio->sock_fd, TRUE, TRUE);
    vio_set_buffer_size(vio->sock_fd, M_TCP_SNDBUF_SIZE, M_TCP_RCVBUF_SIZE);
    ret = connect(vio->sock_fd, (struct sockaddr *)addr, len);
    if (0 != ret) {
        vio->error_no = socket_errno;
        if (vio->error_no == SOCKET_EINPROGRESS || vio->error_no == SOCKET_EAGAIN ||
            vio->error_no == SOCKET_EWOULDBLOCK) {
            vio->error_no = 0;
        } else {
            LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_connect_by_addr_async: can not connect to server, error %d", vio->error_no);
            vio_close_socket(vio);
            return VIO_ERROR;
        }
    }

    return VIO_SUCCESS;
}

int vio_check_connection_status(Vio* vio)
{
    int           err, err_len;

    err_len = sizeof(err);
    if (getsockopt(vio->sock_fd, SOL_SOCKET, SO_ERROR, (char *)&err, (socklen_t*)&err_len) == -1) {
        vio->error_no = socket_errno;
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_check_connection_status: error for getsockopt, error %d", socket_errno);
        goto err_exit;
    }
    if (err != 0) {//check the SO_ERROR state
        vio->error_no = err;
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_check_connection_status: SO_ERROR, error %d", err);
        goto err_exit;
    }

    vio_set_keep_alive(vio->sock_fd, M_TCP_KEEP_IDLE, M_TCP_KEEP_INTERVAL, M_TCP_KEEP_COUNT);
    vio_set_linger(vio->sock_fd, 1, 1);
    vio->inactive = FALSE;

    return VIO_SUCCESS;

err_exit:

    return VIO_ERROR;
}


void vio_disconnect(Vio* vio)
{
    vio_close_socket(vio);
}

bool32 vio_is_connected(Vio* vio)
{
    if  (INVALID_SOCKET == vio->sock_fd || vio->inactive) {
        return FALSE;
    }
    
    return TRUE;
}

my_socket vio_socket_listen(  const char* host, uint16 port)
{
    int                         domain;
    my_socket                   sock_fd;
    int                         backlog;
    struct sockaddr_storage     remote;
    socklen_t                   remote_len;

    if (VIO_SUCCESS != vio_ip_to_sockaddr(host, port, &domain, (struct sockaddr *)&remote, &remote_len)) {
        return INVALID_SOCKET;
    }
    
    sock_fd = vio_create_socket(domain, SOCK_STREAM, 0);
    if (INVALID_SOCKET == sock_fd) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_socket_listen: can not create socket, errno %d", socket_errno);
        return INVALID_SOCKET;
    }
    
    vio_set_blocking(sock_fd, TRUE, TRUE);
    vio_set_linger(sock_fd, 1, 1);
    
    bind(sock_fd, (struct sockaddr *)&remote, remote_len);

    backlog = 5;
    if (listen(sock_fd, backlog) < 0) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_socket_listen: can not listen on host(%s:%d), errno %d", host, port, socket_errno);
        close_socket(sock_fd);
        return INVALID_SOCKET;
    }

    return sock_fd;
}

int vio_socket_poll(struct pollfd* fds, uint32 nfds, int32 timeout_ms)
{
    int32 ret = 0;

#ifdef __WIN__
    fd_set wfds;
    fd_set rfds;
    fd_set efds;
    uint32 i = 0;
    struct pollfd* pfds = fds;
    struct timeval tv, * tvptr = NULL;
    if (nfds >= FD_SETSIZE) {
        return -1;
    }

    FD_ZERO(&rfds);
    FD_ZERO(&wfds);
    FD_ZERO(&efds);
    if (timeout_ms >= 0) {
        tv.tv_sec = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        tvptr = &tv;
    }

    for (i = 0; i < nfds; i++, pfds++)
    {
        if (pfds->events & POLLIN) {
            FD_SET(pfds->fd, &rfds);
        }
        if (pfds->events & POLLOUT) {
            FD_SET(pfds->fd, &wfds);
        }
        FD_SET(pfds->fd, &efds);
    }

    ret = select(0, &rfds, &wfds, &efds, tvptr);
    if (ret == 0) {
        WSASetLastError(SOCKET_ETIMEDOUT);
    }
    if (ret <= 0) {
        return ret;
    }

    /* The requested I/O event is ready? */
    pfds = fds;
    for (uint32 i = 0; i < nfds; i++, pfds++)
    {
        pfds->revents = 0;
        if (pfds->events & POLLIN) {
            if (FD_ISSET(pfds->fd, &rfds)) {
                pfds->revents |= POLLIN;
            }
        }
        if (pfds->events & POLLOUT) {
            if (FD_ISSET(pfds->fd, &wfds)) {
                pfds->revents |= POLLOUT;
            }
        }
        if (FD_ISSET(pfds->fd, &efds)) {
            pfds->revents |= POLLERR;
        }
    }
    
    return ret;
#else
    /*
      Wait for the I/O event and return early in case of error or timeout.
    */
    switch ((ret = poll(fds, nfds, timeout_ms)))
    {
        case -1:
          /* On error, -1 is returned. */
          break;
        case 0:
          /*
            Set errno to indicate a timeout error.
            (This is not compiled in on WIN32.)
          */
          errno = SOCKET_ETIMEDOUT;
          break;
        default:
          /* Ensure that the requested I/O event has completed. */
          break;
    }

    return ret;
#endif
}

int vio_io_wait(Vio* vio, enum enum_vio_io_event event, int32 timeout_ms)
{
    int             status = VIO_SUCCESS;
    struct pollfd   fd;
    int32           ret;
    int32           tv;

    if (vio->inactive) {
        return VIO_ERROR;
    }
    tv = (timeout_ms < 0 ? -1 : timeout_ms);
    fd.fd = vio->sock_fd;
    fd.revents = 0;
    if (event == VIO_IO_EVENT_WRITE) {
        fd.events = POLLOUT;
    } else {
        fd.events = POLLIN;
    }
    switch ((ret = vio_socket_poll(&fd, 1, tv)))
    {
        case -1:
          /* On error, -1 is returned. */
          status = VIO_ERROR;
          break;
        case 0:
          /*
            Set errno to indicate a timeout error.
            (This is not compiled in on WIN32.)
          */
          status = VIO_ERROR;
          break;
        default:
          /* Ensure that the requested I/O event has completed. */
          status = VIO_SUCCESS;
          break;
    }

    return status;
}

int vio_socket_write(Vio* vio, const char* buf, uint32 size)
{
    int ret;
    
    while ((ret = send(vio->sock_fd, buf, size, 0)) == -1)
    {
        vio->error_no = socket_errno;
        if (vio->error_no != SOCKET_EWOULDBLOCK && vio->error_no != SOCKET_EAGAIN && vio->error_no != SOCKET_EINTR) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_socket_write: fd %d errno %d", vio->sock_fd, vio->error_no);
            break;
        }
        if (vio->write_timeout == 0) {
            break;
        }
        if (vio->write_timeout == -1) {
            os_thread_sleep(100);
            continue;
        }
        /* Wait for the output buffer to become writable.*/
        LOGGER_DEBUG(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_socket_read: wait for write, vio fd %d", vio->sock_fd);
        if (VIO_ERROR == vio_io_wait(vio, VIO_IO_EVENT_WRITE, vio->write_timeout)) {
            break;
        }
    }

    return ret;
}

int vio_write(Vio* vio, const char* buf, uint32 size)
{
    unsigned int retry_count= 0;

    while (size)
    {
        int sentcnt = vio_socket_write(vio, buf, size);
        /* VIO_ERROR (-1) indicates an error. */
        if (sentcnt == VIO_ERROR) {
            /* A recoverable I/O error occurred? */
            if (retry_count < vio->retry_count && vio_should_retry(vio)) {
                retry_count++;
                continue;
            }
            break;
        }
        size -= sentcnt;
        buf += sentcnt;
    }
    /* On failure, propagate the error code. */
    if (size) {
        /* Socket should be closed. */
        return (VIO_CLOSE == SOCKET_ECONNRESET) ? VIO_CLOSE : VIO_ERROR;
    }

    return VIO_SUCCESS;
}

int vio_write_timed(Vio* vio, const char* buf, uint32 size, uint32 timeout)
{
    uint32 retry_count = 0;

    while (size)
    {
        int sentcnt = vio_socket_write(vio, buf, size);
        /* VIO_SOCKET_ERROR (-1) indicates an error. */
        if (sentcnt == VIO_ERROR) {
            /* A recoverable I/O error occurred? */
            if (retry_count < vio->retry_count && vio_should_retry(vio)) {
                retry_count++;
                continue;
            } else {
                break;
            }
        }
        size -= sentcnt;
        buf += sentcnt;
    }
    /* On failure, propagate the error code. */
    if (size) {
        if (vio_was_timeout(vio)) {
        }
        /* Socket should be closed. */
        return VIO_ERROR;
    }
    return VIO_SUCCESS;
}

int vio_socket_read(Vio* vio, const char* buf, uint32 size)
{
    int ret;
    
    vio->error_no = 0;
    while ((ret = recv(vio->sock_fd, (SOCKBUF_T *)buf, size, 0)) == -1)
    {
        vio->error_no = socket_errno;
        if (vio->error_no != SOCKET_EWOULDBLOCK && vio->error_no != SOCKET_EAGAIN && vio->error_no != SOCKET_EINTR) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_socket_read: fd %d errno %d", vio->sock_fd, vio->error_no);
            break;
        }
        if (vio->read_timeout == 0) {
            break;
        }
        if (vio->read_timeout == -1) {
            os_thread_sleep(100);
            continue;
        }
        /* Wait for the output buffer to become readable.*/
        LOGGER_DEBUG(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_socket_read: wait for read, vio fd %d", vio->sock_fd);
        if (VIO_ERROR == vio_io_wait(vio, VIO_IO_EVENT_READ, vio->read_timeout)) {
            break;
        }
    }
    return ret;
}

int vio_read(Vio* vio, const char* buf, uint32 buf_size)
{
    uint32          retry_count = 0;

    while (buf_size)
    {
        int recvcnt = vio_socket_read(vio, buf, buf_size);
        if (recvcnt == VIO_ERROR) {
            if (retry_count < vio->retry_count && vio_should_retry(vio)) {
                retry_count++;
                LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_socket_read: retry read, fd %d retry %d", vio->sock_fd, retry_count);
                continue;
            }
            return (VIO_CLOSE == SOCKET_ECONNRESET) ? VIO_CLOSE : VIO_ERROR;;
        } else if (recvcnt == 0) {/* Zero indicates end of file. */
            return VIO_CLOSE;
        }
        buf_size -= recvcnt;
        buf += recvcnt;
    }
    return VIO_SUCCESS;
}

int vio_try_read(Vio* vio, const char* buf, uint32 buf_size, uint32* recv_size)
{
    uint32          retry_count = 0;

retry_read:

    *recv_size = vio_socket_read(vio, buf, buf_size);
    if (*recv_size == VIO_ERROR) {
        if (vio->error_no == SOCKET_EAGAIN || vio->error_no == SOCKET_EWOULDBLOCK) {
            *recv_size = 0;
            return VIO_SUCCESS;
        }
        if (retry_count < vio->retry_count && vio_should_retry(vio)) {
            retry_count++;
            LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_SOCKET, "vio_socket_read: retry read, fd %d retry %d", vio->sock_fd, retry_count);
            goto retry_read;
        }
        return VIO_ERROR;
    } else if (*recv_size == 0) {
        return VIO_CLOSE;
    }
    return VIO_SUCCESS;
}

void vio_init(Vio* vio)
{
    vio->retry_count = 0;
    vio->read_timeout = -1;
    vio->write_timeout = -1;
    vio->error_no = 0;
    vio->sock_fd = INVALID_SOCKET;
    vio->inactive = TRUE;
}

int vio_getnameinfo(const struct sockaddr *sa,
    char *hostname, uint32 hostname_size,
    char *port, uint32 port_size,
    int flags)
{
    int sa_length = 0;

    switch (sa->sa_family) {
    case AF_INET:
        sa_length = sizeof(struct sockaddr_in);
#ifdef HAVE_SOCKADDR_IN_SIN_LEN
        ((struct sockaddr_in *) sa)->sin_len = sa_length;
#endif /* HAVE_SOCKADDR_IN_SIN_LEN */
        break;

    case AF_INET6:
        sa_length = sizeof(struct sockaddr_in6);
# ifdef HAVE_SOCKADDR_IN6_SIN6_LEN
        ((struct sockaddr_in6 *) sa)->sin6_len = sa_length;
# endif /* HAVE_SOCKADDR_IN6_SIN6_LEN */
        break;
    }

    return getnameinfo(sa, sa_length, hostname, hostname_size, port, port_size, flags);
}

#ifdef __cplusplus
}
#endif
