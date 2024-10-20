#define _WINSOCK_DEPRECATED_NO_WARNINGS

#include "cm_type.h"
#include "cm_biqueue.h"
#include "cm_thread.h"
#include "cm_thread_pool.h"
#include "cm_thread_group.h"
#include "cm_log.h"
#include "cm_config.h"
#include "cm_getopt.h"
#include "cm_file.h"
#include "cm_rbt.h"
#include "cm_util.h"
#include "cm_encrypt.h"
#include "cm_md5.h"
#include "cm_base64.h"
#include "vio_pat.h"
#include "vio_epoll.h"
#include "vio_epoll_reactor.h"
#include "vio_socket.h"

#define CMD_FIRST_CONTENT               1
#define CMD_OTHER_CONTENT               2
#define CMD_CLOSE_CONN                  3
#define CMD_SUCCESS                     4

#define L_PROXY_PNO                     1
#define R_PROXY_PNO                     2

#define EVENT_CLOSE_CONN                (EVENT_USER + 1)

#define ENCRYPT_LEN                     16
#define ENCRYPT_KEY                     0x86

typedef enum {
    SOCKS_PROXY = 0,
    L_PROXY     = 1,
    R_PROXY     = 2,
} proxy_type_t;

typedef enum {
    ENCRYPT_NONE = 0,
    ENCRYPT_AES = 1,
    ENCRYPT_XOR = 2,
} encrypt_type_t;

typedef enum {
    HOST_IPV4     = 1,
    HOST_DOMAIN   = 3,
    HOST_IPV6     = 4,
} host_type_t;

typedef enum
{
    SOCKS_CONN                = 0,
    SOCKS_AUTH                = 1,
    SOCKS_HOST                = 2,
    SOCKS_CONN_SERVER         = 3,
    SOCKS_CONN_SERVER_CHECK   = 4,
    SOCKS_CONTENT             = 5,
    L_PROXY_CONTENT           = 6,
    R_PROXY_CONTENT           = 7,
    R_PROXY_CONN_SERVER_CHECK = 8,
} socks_status_t;

typedef struct st_connection connection_t;
struct st_connection
{
    uint32             id;
    Vio                client_vio;
    Vio                server_vio;
    reactor_t         *reactor;
    char              *buf;
    uint32             buf_size;
    uint32             buf_len;
    socks_status_t     status;
    bool32             is_first_content;
    host_type_t        atype;
    char               host[256];
    uint8              host_len;
    uint16             port;
    uint32             offset;
    UT_LIST_NODE_T(connection_t) list_node;
};

typedef struct
{
    uint32             seq_index;
    uint32             thread_count;
    reactor_pool_t*    reator_pool;

    char               local_host[256];
    char               remote_host[256];
    uint16             local_port;
    uint16             remote_port;

    char               user[256];
    char               password[256];
    uint8              user_len;
    uint8              passwd_len;

    uint32             data_buf_size;
    uint8              connect_timeout_sec;
    uint8              poll_timeout_sec;
    proxy_type_t       type;
    encrypt_type_t     encrypt_type;
    spinlock_t         lock;
    int                log_level;
    int                log_type;
    uint8              encrypt_key[16];
    uint8              auth_md5_buf[MD5_BLOCK_SIZE];
    
    // statistics
    uint32             conn_alloc_count;
    uint32             working_threads;
    uint32             req_count;

    UT_LIST_BASE_NODE_T(connection_t) free_list;
} socks_mgr_t;

socks_mgr_t         g_socks_mgr;
static char         g_config_file[255];


/* 128 bit key */
uint8 encrypt_init_key[] = {
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 
    0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f};

static bool32 lproxy_handle_contents_req(uint32 events, connection_t *conn);
static bool32 lproxy_handle_contents_rsp(uint32 events, connection_t *conn);
static bool32 rproxy_handle_contents_req(uint32 events, connection_t *conn);
static bool32 rproxy_handle_contents_rsp(uint32 events, connection_t *conn);


static connection_t* srv_alloc_conn(reactor_t* reactor)
{
    uint32           id;
    connection_t    *conn;

    spin_lock(&g_socks_mgr.lock, NULL);
    id = ++g_socks_mgr.seq_index;
    conn = UT_LIST_GET_FIRST(g_socks_mgr.free_list);
    if (conn != NULL) {
        UT_LIST_REMOVE(list_node, g_socks_mgr.free_list, conn);
    } else {
        g_socks_mgr.conn_alloc_count++;
    }
    spin_unlock(&g_socks_mgr.lock);

    if (conn) {
        conn->id = id;
        conn->reactor = reactor;
        vio_init(&conn->client_vio);
        vio_init(&conn->server_vio);
        conn->buf_len = 0;
        conn->status = SOCKS_CONN;
        return conn;
    }

    conn = (connection_t *)malloc(sizeof(connection_t));
    conn->id = id;
    conn->reactor = reactor;
    vio_init(&conn->client_vio);
    vio_init(&conn->server_vio);
    conn->buf_len = 0;
    conn->status = SOCKS_CONN;
    conn->buf = (char *)malloc(g_socks_mgr.data_buf_size + 20);
    conn->buf_size = g_socks_mgr.data_buf_size;

    return conn;
}

static void srv_free_conn(connection_t* conn)
{
    spin_lock(&g_socks_mgr.lock, NULL);
    UT_LIST_ADD_LAST(list_node, g_socks_mgr.free_list, conn);
    spin_unlock(&g_socks_mgr.lock);
}

static void srv_close_socket(connection_t* conn, Vio* vio)
{
    if (vio->sock_fd != INVALID_SOCKET) {
        LOGGER_DEBUG(LOGGER, "srv_close_socket: connection(%d : %d)", conn->id, vio->sock_fd);
        vio_close_socket(vio);
    }
}

static void srv_encrypt_xor(connection_t *conn)
{
    uint32 i;
    for(i = 4; i < conn->buf_len; i++) {
        conn->buf[i] = conn->buf[i] ^ ENCRYPT_KEY;
    }
}

static void srv_decrypt_xor(connection_t *conn)
{
    uint32 i;
    for(i = 0; i < conn->buf_len; i++) {
        conn->buf[i] = conn->buf[i] ^ ENCRYPT_KEY;
    }
}

static uint32 pad_buffer(char* buf, uint32 len)
{
    uint32 n, pad_count;
    uint32 i;

    n = (len + 4) % 16;
    pad_count = 16 - n;

    for (i = 0; i < pad_count + 4; i++) {
        buf[len + i] = 0;
    }

    return len + pad_count + 4;
}

static void srv_encrypt_aes(connection_t *conn)
{
    uint32 pad_data_len;

    pad_data_len = pad_buffer(conn->buf + 4, conn->buf_len - 4);
    mach_write_to_4((uint8*)conn->buf + pad_data_len, conn->buf_len - 4);
    //aes_encrypt_ecb(AES_CYPHER_128, (uint8*)conn->buf + 4, pad_data_len, encrypt_init_key);
    //
    conn->buf_len = pad_data_len + 4;
    mach_write_to_4((uint8*)conn->buf, pad_data_len);
}

static void srv_decrypt_aes(connection_t *conn)
{
    //aes_decrypt_ecb(AES_CYPHER_128, (uint8*)conn->buf, conn->buf_len, encrypt_init_key);
    conn->buf_len = mach_read_from_4((uint8*)conn->buf + conn->buf_len - 4);
}

static void srv_encrypt(connection_t *conn)
{
    if (g_socks_mgr.encrypt_type == ENCRYPT_XOR) {
        srv_encrypt_xor(conn);
    } else if (g_socks_mgr.encrypt_type == ENCRYPT_AES) {
        srv_encrypt_aes(conn);
    }
}

static void srv_decrypt(connection_t *conn)
{
    if (g_socks_mgr.encrypt_type == ENCRYPT_XOR) {
        srv_decrypt_xor(conn);
    } else if (g_socks_mgr.encrypt_type == ENCRYPT_AES) {
        srv_decrypt_aes(conn);
    }
}

static bool32 srv_epoll_accept(reactor_t* reactor, my_socket listen_fd, my_socket accept_fd)
{
    epoll_data_t     epoll_data;
    connection_t    *conn;
    
    conn = srv_alloc_conn(reactor);
    if (conn == NULL) {
        LOGGER_ERROR(LOGGER, "epoll_accept: error for alloc conn, accept fd %d", accept_fd);
        return FALSE;
    }
    
    LOGGER_DEBUG(LOGGER, "epoll_accept: connection(id=%d, fd=%d) added event to epoll", conn->id, accept_fd);

    conn->client_vio.sock_fd = accept_fd;
    conn->client_vio.inactive = FALSE;
    if (g_socks_mgr.type == SOCKS_PROXY || g_socks_mgr.type == L_PROXY) {
        conn->status = SOCKS_CONN;
        epoll_data.ptr = conn;
        if (-1 == reactor_add_read(conn->reactor, accept_fd, &epoll_data, g_socks_mgr.poll_timeout_sec)) {
            LOGGER_ERROR(LOGGER, "epoll_accept: connection(id=%d, fd=%d) can not add event to epoll", conn->id, accept_fd);
            srv_free_conn(conn);
            return FALSE;
        }
    } else if (g_socks_mgr.type == R_PROXY) {
        conn->status = R_PROXY_CONTENT;
        epoll_data.ptr = conn;
        if (-1 == reactor_epoll_add_read(conn->reactor, accept_fd, &epoll_data)) {
            LOGGER_ERROR(LOGGER, "epoll_accept: connection(id=%d, fd=%d) can not add event to epoll", conn->id, accept_fd);
            srv_free_conn(conn);
            return FALSE;
        }
    }

    return TRUE;
}

static bool32 socks_handle_contents_req(uint32 events, connection_t *conn)
{
    int           ret;
    epoll_data_t  epoll_data;

    if (events & EPOLLTIMEOUT) {
        LOGGER_ERROR(LOGGER, "socks_handle_contents_req: timeout for client, connection(%d : %d)",
            conn->id, conn->server_vio.sock_fd);
        goto err_exit;
    }

    conn->buf_len = 0;
    ret = vio_try_read(&conn->client_vio, conn->buf, conn->buf_size, &conn->buf_len);
    if (VIO_SUCCESS != ret) {
        if (VIO_CLOSE == ret) {
            LOGGER_DEBUG(LOGGER, "socks_handle_contents_req: connection(%d : %d) is closed by client.",
            conn->id, conn->client_vio.sock_fd);
        } else {
            LOGGER_ERROR(LOGGER, "socks_handle_contents_req: error for read from client. connection(%d : %d), error %d",
                conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
        }
        goto err_exit;
    }
    //
    ret = vio_write(&conn->server_vio, conn->buf, conn->buf_len);
    if (VIO_SUCCESS != ret)
    {
        LOGGER_ERROR(LOGGER, "socks_handle_contents_req: error for send request to server, connection(%d : %d), error %d",
            conn->id, conn->server_vio.sock_fd, conn->server_vio.error_no);
        goto err_exit;
    }

    //
    conn->status = SOCKS_CONTENT;
    epoll_data.ptr = conn;
    reactor_mod_read_oneshot(conn->reactor, conn->client_vio.sock_fd, &epoll_data, g_socks_mgr.poll_timeout_sec);

    return TRUE;

err_exit:

    reactor_del(conn->reactor, conn->client_vio.sock_fd);
    srv_close_socket(conn, &conn->client_vio);
    reactor_epoll_del(conn->reactor, conn->server_vio.sock_fd);
    srv_close_socket(conn, &conn->server_vio);
    srv_free_conn(conn);

    return FALSE;
}

static bool32 socks_handle_contents_rsp(uint32 events, connection_t *conn)
{
    int           ret;
    epoll_data_t  epoll_data;

    conn->buf_len = 0;
    ret = vio_try_read(&conn->server_vio, conn->buf, conn->buf_size, &conn->buf_len);
    if (VIO_SUCCESS != ret) {
        if (VIO_CLOSE == ret) {
            LOGGER_DEBUG(LOGGER, "socks_handle_contents_rsp: connection(%d : %d) is closed by server.",
                conn->id, conn->server_vio.sock_fd);
        } else {
            LOGGER_ERROR(LOGGER, "socks_handle_contents_rsp: error for read from server. connection(%d : %d), error %d",
                conn->id, conn->server_vio.sock_fd, conn->server_vio.error_no);
        }
        goto err_exit;
    }

    ret = vio_write(&conn->client_vio, conn->buf, conn->buf_len);
    if (VIO_SUCCESS != ret) {
        LOGGER_ERROR(LOGGER, "socks_handle_contents_rsp: error for send result to client, connection(%d : %d), error %d",
            conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
        goto err_exit;
    }

    conn->status = SOCKS_CONTENT;
    epoll_data.ptr = conn;
    reactor_epoll_mod_oneshot(conn->reactor, conn->server_vio.sock_fd, &epoll_data);

    return TRUE;

err_exit:

    reactor_del(conn->reactor, conn->client_vio.sock_fd);
    srv_close_socket(conn, &conn->client_vio);
    reactor_epoll_del(conn->reactor, conn->server_vio.sock_fd);
    srv_close_socket(conn, &conn->server_vio);
    srv_free_conn(conn);

    return FALSE;
}

static bool32 do_check_connection_status(uint32 events, connection_t *conn)
{
    char           *host;
    uint16          port;
    int             ret;

    if (g_socks_mgr.type == L_PROXY) {
        host = g_socks_mgr.remote_host;
        port = g_socks_mgr.remote_port;
    } else {
        host = conn->host;
        port = conn->port;
    }

    if (events & EPOLLTIMEOUT) {
        LOGGER_ERROR(LOGGER, "do_check_connection_status: timeout for connect server(%s:%d), connection(%d : %d)",
            host, port, conn->id, conn->server_vio.sock_fd);
        return FALSE;
    }
    
    ret = vio_check_connection_status(&conn->server_vio);
    if (ret != VIO_SUCCESS) {
        LOGGER_ERROR(LOGGER, "do_check_connection_status: error for connect server(%s:%d), connection(%d : %d), error %d",
            host, port, conn->id, conn->server_vio.sock_fd, conn->server_vio.error_no);
        return FALSE;
    }

    LOGGER_DEBUG(LOGGER, "do_check_connection_status: connected to server(%s:%d), connection(%d : %d)",
        host, port, conn->id, conn->server_vio.sock_fd);

    return TRUE;
}

static bool32 socks_check_connection_status(uint32 events, connection_t *conn)
{
    epoll_data_t    epoll_data;

    if (do_check_connection_status(events, conn) == FALSE) {
        goto err_exit;
    }

    if (g_socks_mgr.type == SOCKS_PROXY) {
        conn->status = SOCKS_CONTENT;
    } else if (g_socks_mgr.type == L_PROXY) {
        conn->status = L_PROXY_CONTENT;
        conn->is_first_content = TRUE;
    }
    epoll_data.ptr = conn;
    reactor_mod_read_oneshot(conn->reactor, conn->client_vio.sock_fd, &epoll_data, g_socks_mgr.poll_timeout_sec);
    reactor_del(conn->reactor, conn->server_vio.sock_fd);
    reactor_epoll_add_read(conn->reactor, conn->server_vio.sock_fd, &epoll_data);

    return TRUE;
    
err_exit:

    reactor_del(conn->reactor, conn->client_vio.sock_fd);
    srv_close_socket(conn, &conn->client_vio);
    reactor_del(conn->reactor, conn->server_vio.sock_fd);
    srv_close_socket(conn, &conn->server_vio);
    srv_free_conn(conn);

    return FALSE;
}

static void socks_async_connect_server(connection_t *conn)
{
    epoll_data_t  epoll_data;
    int           ret;

    if (g_socks_mgr.type == SOCKS_PROXY) {
        if (conn->atype == HOST_IPV4) {
            if (VIO_SUCCESS != vio_connect_async(&conn->server_vio, conn->host, conn->port, NULL))
            {
                LOGGER_ERROR(LOGGER, "socks_async_connect_server: error for connect server(%s:%d), connection(%d), error %d",
                    conn->host, conn->port, conn->id, conn->server_vio.error_no);
                goto err_exit;
            }
        } else {
            struct sockaddr_in  sin;
            struct hostent     *phost;

            phost = gethostbyname(conn->host);
            //getaddrinfo()
            if (phost == NULL) {
                LOGGER_ERROR(LOGGER, "socks_async_connect_server: error for convert domain(%s:%d), connection(%d)",
                    conn->host, conn->port, conn->id);
                goto err_exit;
            }
            memset(&sin, 0, sizeof(struct sockaddr_in));
            sin.sin_family = AF_INET;
            memcpy(&sin.sin_addr, phost->h_addr_list[0], phost->h_length);
            sin.sin_port = htons(conn->port);
            ret = vio_connect_by_addr_async(&conn->server_vio, (struct sockaddr_in *)&sin, sizeof(struct sockaddr_in));
            if (VIO_SUCCESS != ret)
            {
                LOGGER_ERROR(LOGGER, "socks_async_connect_server: error for connect server(%s:%d), connection(%d), error %d",
                    conn->host, conn->port, conn->id, conn->server_vio.error_no);
                goto err_exit;
            }
        }
    } 
    else if (g_socks_mgr.type == L_PROXY) {
        //if (vio_is_connected(&conn->server_vio)) {
        //    callback_data_t callback_data;
        //    conn->is_first_content = TRUE;
        //    callback_data.ptr = conn;
        //    reactor_del(conn->reactor, conn->client_vio.sock_fd);
        //    append_to_thread_group(g_socks_mgr.thread_groups, 0, &callback_data, lproxy_handle_contents, FALSE);
        //    return;
        //}

        ret = vio_connect_async(&conn->server_vio, g_socks_mgr.remote_host, g_socks_mgr.remote_port, NULL);
        if (VIO_SUCCESS != ret)
        {
            LOGGER_ERROR(LOGGER, "socks_async_connect_server: error for connect rproxy(%s:%d), connection(%d), error %d",
                g_socks_mgr.remote_host, g_socks_mgr.remote_port, conn->id, conn->server_vio.error_no);
            goto err_exit;
        }
    }
    
    LOGGER_DEBUG(LOGGER, "socks_async_connect_server: connecting to server(%s:%d), connection(%d : %d)",
        conn->host, conn->port, conn->id, conn->server_vio.sock_fd);

    conn->status = SOCKS_CONN_SERVER_CHECK;
    epoll_data.ptr = conn;
    reactor_add_write(conn->reactor, conn->server_vio.sock_fd, &epoll_data, g_socks_mgr.connect_timeout_sec);

    return;
    
err_exit:

    reactor_del(conn->reactor, conn->client_vio.sock_fd);
    srv_close_socket(conn, &conn->client_vio);
    srv_free_conn(conn);
}

static void socks_handle_conn(connection_t *conn)
{
    epoll_data_t  epoll_data;
    int           ret;
    bool32        is_need_auth = FALSE, is_allowed_auth = FALSE;

    conn->buf_len = 2;
    ret = vio_read(&conn->client_vio, conn->buf, conn->buf_len);
    if (VIO_SUCCESS != ret) {
        if (VIO_CLOSE == ret) {
            LOGGER_ERROR(LOGGER, "socks_handle_conn: connection(%d : %d) is closed by client.",
                conn->id, conn->client_vio.sock_fd);
        } else {
            LOGGER_ERROR(LOGGER, "socks_handle_conn: error for read from client, connection(%d : %d), error %d",
                conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
        }
        goto err_exit;
    }
    if (conn->buf[0] != 0x05 || conn->buf[1] == 0) {
        LOGGER_ERROR(LOGGER, "socks_handle_conn: invalid request of socks, connection(%d : %d)",
            conn->id, conn->client_vio.sock_fd);
        goto err_exit;
    }
    
    conn->buf_len = conn->buf[1];
    ret = vio_read(&conn->client_vio, conn->buf, conn->buf_len);
    if (VIO_SUCCESS != ret) {
        LOGGER_ERROR(LOGGER, "socks_handle_conn: error for read from client, connection(%d : %d), error %d",
            conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
        goto err_exit;
    }
    if (g_socks_mgr.type == SOCKS_PROXY) {
        is_need_auth = g_socks_mgr.user_len > 0 ? TRUE : FALSE;
    }
    if (is_need_auth) {
        uint32    i;
        for (i = 0; i < conn->buf_len; i++) {
            if (conn->buf[i] == 0x02) {
                is_allowed_auth = TRUE;
                break;
            }
        }
        if (is_allowed_auth == FALSE) {
            LOGGER_ERROR(LOGGER, "socks_handle_conn: invalid authentication, connection(%d : %d), error %d",
                conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
            goto err_exit;
        }
        conn->buf[1] = 0x02;
    } else {
        conn->buf[1] = 0;
    }
    conn->buf[0] = 0x05;
    conn->buf_len = 2;
    ret = vio_write(&conn->client_vio, conn->buf, conn->buf_len);
    if (VIO_SUCCESS != ret) {
        LOGGER_ERROR(LOGGER, "socks_handle_conn: error for send result of socks, connection(%d : %d), error %d",
            conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
        goto err_exit;
    }
    //
    if (is_need_auth) {
        conn->status = SOCKS_AUTH;
    } else {
        conn->status = SOCKS_HOST;
    }
    epoll_data.ptr = conn;
    reactor_mod_read_oneshot(conn->reactor, conn->client_vio.sock_fd, &epoll_data, g_socks_mgr.poll_timeout_sec);

    return;

err_exit:

    reactor_del(conn->reactor, conn->client_vio.sock_fd);
    srv_close_socket(conn, &conn->client_vio);
    srv_free_conn(conn);
}

static void socks_handle_auth(connection_t *conn)
{
    epoll_data_t  epoll_data;
    int           ret;
    uint8         version, user_len, passwd_len;
    char          user[256], pwd[256];

    conn->buf_len = 2;
    ret = vio_read(&conn->client_vio, conn->buf, conn->buf_len);
    if (VIO_SUCCESS != ret) {
        if (VIO_CLOSE == ret) {
            LOGGER_ERROR(LOGGER, "socks_handle_auth: connection(%d : %d) is closed by client.",
                conn->id, conn->client_vio.sock_fd);
        } else {
            LOGGER_ERROR(LOGGER, "socks_handle_auth: error for read from client, connection(%d : %d), error %d",
                conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
        }
        goto err_exit;
    }
    version = conn->buf[0];
    // user
    if (conn->buf[1] == 0) {
        LOGGER_ERROR(LOGGER, "socks_handle_auth: invalid request of socks, connection(%d : %d), error %d",
            conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
        goto err_exit;
    }
    user_len = conn->buf[1];
    ret = vio_read(&conn->client_vio, user, user_len);
    if (VIO_SUCCESS != ret) {
        LOGGER_ERROR(LOGGER, "socks_handle_auth: error for read from client, connection(%d : %d), error %d",
            conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
        goto err_exit;
    }
    user[user_len] = '\0';
    // password
    conn->buf_len = 1;
    ret = vio_read(&conn->client_vio, conn->buf, conn->buf_len);
    if (VIO_SUCCESS != ret) {
        LOGGER_ERROR(LOGGER, "socks_handle_auth: error for read from client, connection(%d : %d), error %d",
            conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
        goto err_exit;
    }
    if (conn->buf[0] == 0) {
        LOGGER_ERROR(LOGGER, "socks_handle_auth: invalid request of socks, connection(%d : %d), error %d",
            conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
        goto err_exit;
    }
    passwd_len = conn->buf[0];
    ret = vio_read(&conn->client_vio, pwd, passwd_len);
    if (VIO_SUCCESS != ret) {
        LOGGER_ERROR(LOGGER, "socks_handle_auth: error for read from client, connection(%d : %d), error %d",
            conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
        goto err_exit;
    }
    pwd[passwd_len] = '\0';

    conn->buf[0] = version;
    if (user_len == g_socks_mgr.user_len && passwd_len == g_socks_mgr.passwd_len &&
        strncmp((const char *)user, g_socks_mgr.user, user_len) == 0 &&
        strncmp((const char *)pwd, g_socks_mgr.password, passwd_len) == 0) {
        conn->buf[1] = 0;
    } else {
        conn->buf[1] = 0x01;
    }
    conn->buf_len = 2;
    ret = vio_write(&conn->client_vio, conn->buf, conn->buf_len);
    if (VIO_SUCCESS != ret) {
        LOGGER_ERROR(LOGGER, "socks_handle_auth: error for send result of socks, connection(%d : %d), error %d",
            conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
        goto err_exit;
    }
    // 
    conn->status = SOCKS_HOST;
    epoll_data.ptr = conn;
    reactor_mod_read_oneshot(conn->reactor, conn->client_vio.sock_fd, &epoll_data, g_socks_mgr.poll_timeout_sec);

    return;

err_exit:

    reactor_del(conn->reactor, conn->client_vio.sock_fd);
    srv_close_socket(conn, &conn->client_vio);
    srv_free_conn(conn);
}

static void socks_handle_host(connection_t *conn)
{
    epoll_data_t    epoll_data;
    int             ret;
    uint8           cmd, atyp;

    conn->buf_len = 4;
    ret = vio_read(&conn->client_vio, conn->buf, conn->buf_len);
    if (VIO_SUCCESS != ret) {
        if (VIO_CLOSE == ret) {
            LOGGER_ERROR(LOGGER, "socks_handle_host: connection(%d : %d) is closed by client.",
                conn->id, conn->client_vio.sock_fd);
        } else {
            LOGGER_ERROR(LOGGER, "socks_handle_host: error for read from client. connection(%d : %d), error %d",
                conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
        }
        goto err_exit;
    }
    if (conn->buf[0] != 0x05) {
        LOGGER_ERROR(LOGGER, "socks_handle_host: invalid request of socks. connection(%d : %d), error %d",
            conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
        goto err_exit;
    }
    
    cmd = conn->buf[1];  // 1:tcp   3:udp
    if (cmd != 0x01) {
        LOGGER_ERROR(LOGGER, "socks_handle_host: UDP is not supported. connection(%d : %d), error %d",
            conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
        goto err_exit;
    }

    atyp = conn->buf[3];
    switch (atyp) {
    case HOST_IPV4: // ipv4
    {
        conn->buf_len = 6;
        ret = vio_read(&conn->client_vio, conn->buf, conn->buf_len);
        if (VIO_SUCCESS != ret) {
            LOGGER_ERROR(LOGGER, "socks_handle_host: error for read domain from client. connection(%d : %d), error %d",
                conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
            goto err_exit;
        }
        ret = sprintf_s(conn->host, 256, "%d.%d.%d.%d", conn->buf[4], conn->buf[5], conn->buf[6], conn->buf[7]);
        conn->host[ret] = '\0';
        conn->port = conn->buf[8] * 256 + conn->buf[9];
        break;
    }
    case HOST_DOMAIN:
    {
        conn->buf_len = 1;
        ret = vio_read(&conn->client_vio, conn->buf, conn->buf_len);
        if (VIO_SUCCESS != ret) {
            LOGGER_ERROR(LOGGER, "socks_handle_host: error for read domain from client. connection(%d : %d), error %d",
                conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
            goto err_exit;
        }
        if (conn->buf[0] == 0) {
            LOGGER_ERROR(LOGGER, "socks_handle_host: invalid request of socks. connection(%d : %d), error %d",
                conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
            goto err_exit;
        }
        conn->buf_len = conn->buf[0] + 2;
        ret = vio_read(&conn->client_vio, conn->host, conn->buf_len);
        if (VIO_SUCCESS != ret) {
            LOGGER_ERROR(LOGGER, "socks_handle_host: error for read domain from client, connection(%d : %d), error %d",
                conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
            goto err_exit;
        }
        conn->port = ((uint8)conn->host[conn->buf_len - 2]) * 256 + ((uint8)conn->host[conn->buf_len - 1]);
        conn->host[conn->buf_len - 2] = '\0';
        break;
    }
    case HOST_IPV6:
        conn->buf_len = 16;
        ret = vio_read(&conn->client_vio, conn->buf, conn->buf_len);
        if (VIO_SUCCESS != ret) {
            LOGGER_ERROR(LOGGER, "socks_handle_host: error for read domain from client. connection(%d : %d), error %d",
                conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
            goto err_exit;
        }
        LOGGER_ERROR(LOGGER, "socks_handle_host: addr/ipv6 is not supported. connection(%d : %d), error %d",
            conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
        goto err_exit;
    }
    conn->atype = (host_type_t)atyp;
    //
    conn->buf[0] = 0x05;
    conn->buf[1] = 0;
    conn->buf[2] = 0;
    conn->buf[3] = 0x01;
    conn->buf[4] = 0;
    conn->buf[5] = 0;
    conn->buf[6] = 0;
    conn->buf[7] = 0;
    conn->buf[8] = 0;
    conn->buf[9] = 0;
    conn->buf_len = 10;
    ret = vio_write(&conn->client_vio, conn->buf, conn->buf_len);
    if (VIO_SUCCESS != ret)
    {
        LOGGER_ERROR(LOGGER, "socks_handle_host: error for send result of socks, connection(%d : %d), error %d",
            conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
        goto err_exit;
    }
    // 
    conn->status = SOCKS_CONN_SERVER;
    epoll_data.ptr = conn;
    reactor_mod_read_oneshot(conn->reactor, conn->client_vio.sock_fd, &epoll_data, g_socks_mgr.poll_timeout_sec);

    return;

err_exit:

    reactor_del(conn->reactor, conn->client_vio.sock_fd);
    srv_close_socket(conn, &conn->client_vio);
    srv_close_socket(conn, &conn->server_vio);
    srv_free_conn(conn);
}

static bool32 lproxy_handle_contents_req(uint32 events, connection_t *conn)
{
    epoll_data_t epoll_data;
    int          ret;
    uint32       pos;

    if (events & EPOLLTIMEOUT) {
        LOGGER_ERROR(LOGGER, "lproxy_handle_contents_req: timeout for client, connection(%d : %d)",
            conn->id, conn->client_vio.sock_fd);
        goto err_exit;
    }

    pos = 5 + MD5_BLOCK_SIZE;
    if (conn->is_first_content) {
        pos += 1 + conn->host_len + 2;
    }
    conn->buf_len = 0;
    ret = vio_try_read(&conn->client_vio, conn->buf + pos, conn->buf_size, &conn->buf_len);
    if (VIO_SUCCESS != ret) {
        if (VIO_CLOSE == ret) {
            LOGGER_DEBUG(LOGGER, "lproxy_handle_contents_req: connection(%d : %d) is closed by client.",
            conn->id, conn->client_vio.sock_fd);
        } else {
            LOGGER_ERROR(LOGGER, "lproxy_handle_contents_req: error for read from client. connection(%d : %d), error %d",
                conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
        }
        goto err_exit;
    }
    //
    conn->buf_len += pos;
    mach_write_to_4((unsigned char*)conn->buf, conn->buf_len - 4);
    pos = 4;
    mach_write_to_1((unsigned char*)conn->buf + pos, conn->is_first_content);
    pos += 1;
    memcpy(conn->buf + pos, g_socks_mgr.auth_md5_buf, MD5_BLOCK_SIZE);
    pos += MD5_BLOCK_SIZE;
    if (conn->is_first_content) {
        conn->is_first_content = FALSE;
        mach_write_to_1((unsigned char*)conn->buf + pos, conn->host_len);
        pos += 1;
        memcpy(conn->buf + pos, conn->host, conn->host_len);
        pos += conn->host_len;
        mach_write_to_2((unsigned char*)conn->buf + pos, conn->port);
        pos += 2;
        LOGGER_DEBUG(LOGGER, "lproxy_handle_contents_req: server(%s:%d)", conn->host, conn->port);
    }

    srv_encrypt(conn);
    ret = vio_write(&conn->server_vio, conn->buf, conn->buf_len);
    if (VIO_SUCCESS != ret)
    {
        LOGGER_ERROR(LOGGER, "lproxy_handle_contents_req: error for send request to rproxy, connection(%d : %d), error %d",
            conn->id, conn->server_vio.sock_fd, conn->server_vio.error_no);
        goto err_exit;
    }

    //
    conn->status = L_PROXY_CONTENT;
    epoll_data.ptr = conn;
    reactor_mod_read_oneshot(conn->reactor, conn->client_vio.sock_fd, &epoll_data, g_socks_mgr.poll_timeout_sec);

    return TRUE;

err_exit:

    reactor_del(conn->reactor, conn->client_vio.sock_fd);
    srv_close_socket(conn, &conn->client_vio);
    reactor_epoll_del(conn->reactor, conn->server_vio.sock_fd);
    srv_close_socket(conn, &conn->server_vio);
    srv_free_conn(conn);

    return FALSE;
}

static bool32 lproxy_handle_contents_rsp(uint32 events, connection_t *conn)
{
    epoll_data_t epoll_data;
    int          ret;

    conn->buf_len = 4;
    ret = vio_read(&conn->server_vio, conn->buf, conn->buf_len);
    if (VIO_SUCCESS != ret) {
        if (VIO_CLOSE == ret) {
            LOGGER_DEBUG(LOGGER, "lproxy_handle_contents_rsp: connection(%d : %d) is closed by rproxy.",
                conn->id, conn->server_vio.sock_fd);
        } else {
            LOGGER_ERROR(LOGGER, "lproxy_handle_contents_rsp: error for read from rproxy. connection(%d : %d), error %d",
                conn->id, conn->server_vio.sock_fd, conn->server_vio.error_no);
        }
        goto err_exit;
    }
    
    conn->buf_len = mach_read_from_4((unsigned char*)conn->buf);
    ret = vio_read(&conn->server_vio, conn->buf, conn->buf_len);
    if (VIO_SUCCESS != ret) {
        if (VIO_CLOSE == ret) {
            LOGGER_ERROR(LOGGER, "lproxy_handle_contents_rsp: connection(%d : %d) is closed by rproxy.",
                conn->id, conn->server_vio.sock_fd);
        } else {
            LOGGER_ERROR(LOGGER, "lproxy_handle_contents_rsp: error for read from rproxy. connection(%d : %d), error %d",
                conn->id, conn->server_vio.sock_fd, conn->server_vio.error_no);
        }
        goto err_exit;
    }

    srv_decrypt(conn);
    ret = vio_write(&conn->client_vio, conn->buf, conn->buf_len);
    if (VIO_SUCCESS != ret) {
        LOGGER_ERROR(LOGGER, "lproxy_handle_contents_rsp: error for send result to client, connection(%d : %d), error %d",
            conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
        goto err_exit;
    }

    conn->status = L_PROXY_CONTENT;
    epoll_data.ptr = conn;
    reactor_epoll_mod_oneshot(conn->reactor, conn->server_vio.sock_fd, &epoll_data);

    return TRUE;

err_exit:

    reactor_del(conn->reactor, conn->client_vio.sock_fd);
    srv_close_socket(conn, &conn->client_vio);
    reactor_epoll_del(conn->reactor, conn->server_vio.sock_fd);
    srv_close_socket(conn, &conn->server_vio);
    srv_free_conn(conn);

    return FALSE;
}

static bool32 rproxy_parse_contents_req(connection_t *conn, bool32 *is_first_content)
{
    int      ret;
    uint32   pos;

    conn->buf_len = 4;
    ret = vio_read(&conn->client_vio, conn->buf, conn->buf_len);
    if (VIO_SUCCESS != ret) {
        if (VIO_CLOSE == ret) {
            LOGGER_DEBUG(LOGGER, "rproxy_parse_contents_req: connection(%d : %d) is closed by lproxy.",
            conn->id, conn->client_vio.sock_fd);
        } else {
            LOGGER_ERROR(LOGGER, "rproxy_parse_contents_req: error for read from lproxy. connection(%d : %d), error %d",
                conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
        }
        goto err_exit;
    }
    
    conn->buf_len = mach_read_from_4((unsigned char*)conn->buf);
    ret = vio_read(&conn->client_vio, conn->buf, conn->buf_len);
    if (VIO_SUCCESS != ret) {
        if (VIO_CLOSE == ret) {
            LOGGER_ERROR(LOGGER, "rproxy_parse_contents_req: connection(%d : %d) is closed by lproxy.",
            conn->id, conn->client_vio.sock_fd);
        } else {
            LOGGER_ERROR(LOGGER, "rproxy_parse_contents_req: error for read from lproxy. connection(%d : %d), error %d",
                conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
        }
        goto err_exit;
    }

    srv_decrypt(conn);
    *is_first_content = mach_read_from_1((unsigned char*)conn->buf);
    pos = 1;
    if (memcmp(g_socks_mgr.auth_md5_buf, conn->buf + pos, MD5_BLOCK_SIZE) != 0) {
        LOGGER_ERROR(LOGGER, "rproxy_parse_contents_req: invalid user or password. connection(%d : %d)",
            conn->id, conn->client_vio.sock_fd);
        goto err_exit;
    }
    pos += MD5_BLOCK_SIZE;
    if (*is_first_content != 0) {
        conn->host_len = mach_read_from_1((unsigned char*)conn->buf + pos);
        pos += 1;
        memcpy(conn->host, conn->buf + pos, conn->host_len);
        conn->host[conn->host_len] = '\0';
        pos += conn->host_len;
        conn->port = mach_read_from_2((unsigned char*)conn->buf + pos);
        pos += 2;
        LOGGER_DEBUG(LOGGER, "rproxy_parse_contents_req: connection(%d : %d) SERVER(%s : %d)",
            conn->id, conn->client_vio.sock_fd, conn->host, conn->port);
    }
    
    conn->offset = pos;
    
    LOGGER_DEBUG(LOGGER, "rproxy_parse_contents_req: connection(%d : %d : %d) SERVER(%s : %d)",
        conn->id, conn->client_vio.sock_fd, conn->server_vio.sock_fd, conn->host, conn->port);

    return TRUE;

err_exit:

    return FALSE;
}

static bool32 rproxy_async_connect_server(connection_t *conn)
{
    int                  ret;
    struct sockaddr_in   sin;
    struct hostent      *phost;

    phost = gethostbyname(conn->host);
    if (phost == NULL) {
        LOGGER_ERROR(LOGGER, "rproxy_async_connect_server: error for convert domain, connection(%d : %d)",
            conn->id, conn->client_vio.sock_fd);
        goto err_exit;
    }
    memset(&sin, 0, sizeof(struct sockaddr_in));
    sin.sin_family = AF_INET;
    memcpy(&sin.sin_addr, phost->h_addr_list[0], phost->h_length);
    sin.sin_port = htons(conn->port);

    ret = vio_connect_by_addr_async(&conn->server_vio, (struct sockaddr_in *)&sin, sizeof(struct sockaddr_in));
    if (VIO_SUCCESS != ret)
    {
        LOGGER_ERROR(LOGGER, "rproxy_async_connect_server: error for connect server, connection(%d), error %d",
            conn->id, conn->server_vio.error_no);
        goto err_exit;
    }

    LOGGER_DEBUG(LOGGER, "rproxy_async_connect_server: connecting to server(%s:%d), connection(%d : %d)",
        conn->host, conn->port, conn->id, conn->server_vio.sock_fd);

    return TRUE;

err_exit:

    return FALSE;
}

static bool32 rproxy_handle_contents_req(uint32 events, connection_t *conn)
{
    epoll_data_t epoll_data;
    int          ret;
    bool32       is_first_content;

    if (!rproxy_parse_contents_req(conn, &is_first_content)) {
        goto err_exit;
    }

    if (is_first_content)
    {
        if (!rproxy_async_connect_server(conn)) {
            goto err_exit;
        }

        conn->status = R_PROXY_CONN_SERVER_CHECK;
        epoll_data.ptr = conn;
        reactor_add_write(conn->reactor, conn->server_vio.sock_fd, &epoll_data, g_socks_mgr.connect_timeout_sec);
    }
    else
    {
        ret = vio_write(&conn->server_vio, conn->buf + conn->offset, conn->buf_len - conn->offset);
        if (VIO_SUCCESS != ret)
        {
            LOGGER_ERROR(LOGGER, "rproxy_handle_contents_req: error for send request to server, connection(%d : %d), error %d",
                conn->id, conn->server_vio.sock_fd, conn->server_vio.error_no);
            goto err_exit;
        }
        
        //
        conn->status = R_PROXY_CONTENT;
        epoll_data.ptr = conn;
        reactor_epoll_mod_oneshot(conn->reactor, conn->client_vio.sock_fd, &epoll_data);
        reactor_mod_read_oneshot(conn->reactor, conn->server_vio.sock_fd, &epoll_data, g_socks_mgr.poll_timeout_sec);
    }

    return TRUE;

err_exit:

    reactor_epoll_del(conn->reactor, conn->client_vio.sock_fd);
    srv_close_socket(conn, &conn->client_vio);
    if (conn->server_vio.sock_fd != INVALID_SOCKET) {
        reactor_del(conn->reactor, conn->server_vio.sock_fd);
        srv_close_socket(conn, &conn->server_vio);
    }
    srv_free_conn(conn);

    return FALSE;
}

static bool32 rproxy_check_connection_status(uint32 events, connection_t *conn)
{
    epoll_data_t epoll_data;
    int          ret;
    
    if (!do_check_connection_status(events, conn)) {
        goto err_exit;
    }
    
    ret = vio_write(&conn->server_vio, conn->buf + conn->offset, conn->buf_len - conn->offset);
    if (VIO_SUCCESS != ret)
    {
        LOGGER_ERROR(LOGGER, "rproxy_check_connection_status: error for send request to server, connection(%d : %d), error %d",
            conn->id, conn->server_vio.sock_fd, conn->server_vio.error_no);
        goto err_exit;
    }
    
    //
    conn->status = R_PROXY_CONTENT;
    epoll_data.ptr = conn;
    reactor_epoll_mod_oneshot(conn->reactor, conn->client_vio.sock_fd, &epoll_data);
    reactor_mod_read_oneshot(conn->reactor, conn->server_vio.sock_fd, &epoll_data, g_socks_mgr.poll_timeout_sec);

    return TRUE;

err_exit:

    reactor_epoll_del(conn->reactor, conn->client_vio.sock_fd);
    srv_close_socket(conn, &conn->client_vio);
    reactor_del(conn->reactor, conn->server_vio.sock_fd);
    srv_close_socket(conn, &conn->server_vio);
    srv_free_conn(conn);
    
    return FALSE;
}

static bool32 rproxy_handle_contents_rsp(uint32 events, connection_t *conn)
{
    epoll_data_t epoll_data;
    int          ret;

    if (events & EPOLLTIMEOUT) {
        LOGGER_ERROR(LOGGER, "rproxy_handle_contents_rsp: timeout for server, connection(%d : %d)",
            conn->id, conn->server_vio.sock_fd);
        goto err_exit;
    }

    conn->buf_len = 0;
    ret = vio_try_read(&conn->server_vio, conn->buf + 4, conn->buf_size, &conn->buf_len);
    if (VIO_SUCCESS != ret) {
        if (VIO_CLOSE == ret) {
            LOGGER_DEBUG(LOGGER, "rproxy_handle_contents_rsp: connection(%d : %d) is closed by server.",
                conn->id, conn->server_vio.sock_fd);
        } else {
            LOGGER_ERROR(LOGGER, "rproxy_handle_contents_rsp: error for read from server. connection(%d : %d), error %d",
                conn->id, conn->server_vio.sock_fd, conn->server_vio.error_no);
        }
        goto err_exit;
    }
    mach_write_to_4((unsigned char*)conn->buf, conn->buf_len);
    conn->buf_len += 4;

    srv_encrypt(conn);
    ret = vio_write(&conn->client_vio, conn->buf, conn->buf_len);
    if (VIO_SUCCESS != ret) {
        LOGGER_ERROR(LOGGER, "rproxy_handle_contents_rsp: error for send result to client, connection(%d : %d), error %d",
            conn->id, conn->client_vio.sock_fd, conn->client_vio.error_no);
        goto err_exit;
    }

    conn->status = R_PROXY_CONTENT;
    epoll_data.ptr = conn;
    reactor_mod_read_oneshot(conn->reactor, conn->server_vio.sock_fd, &epoll_data, g_socks_mgr.poll_timeout_sec);

    return TRUE;

err_exit:

    reactor_epoll_del(conn->reactor, conn->client_vio.sock_fd);
    srv_close_socket(conn, &conn->client_vio);
    reactor_del(conn->reactor, conn->server_vio.sock_fd);
    srv_close_socket(conn, &conn->server_vio);
    srv_free_conn(conn);

    return FALSE;
}

static void socks_epoll_events(my_socket fd, struct epoll_event* event)
{
    connection_t *conn = (connection_t *)event->data.ptr;

    spin_lock(&g_socks_mgr.lock, NULL);
    g_socks_mgr.working_threads++;
    g_socks_mgr.req_count++;
    spin_unlock(&g_socks_mgr.lock);

    LOGGER_DEBUG(LOGGER, "socks_epoll_events: connection(%d : %d : %d), fd %d status %d",
        conn->id, conn->client_vio.sock_fd, conn->server_vio.sock_fd, fd, conn->status);

    switch (conn->status)
    {
    case SOCKS_CONN:
        socks_handle_conn(conn);
        break;
    case SOCKS_AUTH:
        socks_handle_auth(conn);
        break;
    case SOCKS_HOST:
        socks_handle_host(conn);
        break;
    case SOCKS_CONN_SERVER:
        socks_async_connect_server(conn);
        break;
    case SOCKS_CONN_SERVER_CHECK:
        socks_check_connection_status(event->events, conn);
        break;
    case SOCKS_CONTENT:
        if (fd == conn->client_vio.sock_fd) {
            socks_handle_contents_req(event->events, conn);
        } else {
            socks_handle_contents_rsp(event->events, conn);
        }
        break;
    case L_PROXY_CONTENT:
        if (fd == conn->client_vio.sock_fd) {
            lproxy_handle_contents_req(event->events, conn);
        } else {
            lproxy_handle_contents_rsp(event->events, conn);
        }
        break;
    case R_PROXY_CONTENT:
        if (fd == 0) {
            rproxy_handle_contents_req(event->events, conn);
        } else {
            rproxy_handle_contents_rsp(event->events, conn);
        }
        break;
    case R_PROXY_CONN_SERVER_CHECK:
        rproxy_check_connection_status(event->events, conn);
        break;
    default:
        break;
    }

    spin_lock(&g_socks_mgr.lock, NULL);
    g_socks_mgr.working_threads--;
    spin_unlock(&g_socks_mgr.lock);
}

static int get_options(int argc, char **argv)
{
    struct option long_options[] = {
         { "config",   required_argument,   NULL,    'c'},
         { "help",     no_argument,         NULL,    'h'},
         {      0,     0,                   0,       0},
    };

    int ch;
    while ((ch = getopt_long(argc, argv, "c:h", long_options, NULL)) != -1)
    {
        switch (ch)
        {
        case 'c':
            strcpy_s(g_config_file, 255, optarg);
            break;
        case 'h':
            printf("socks -c socks.ini\n");
            break;
        default:
            printf("unknow option:%c\n", ch);
        }
    }
    return 0;
} /* get_options */

static void srv_create_auth_md5()
{
    MD5_CTX ctx;

    memset(g_socks_mgr.auth_md5_buf, 0, MD5_BLOCK_SIZE);
    if (g_socks_mgr.user_len == 0) {
        return;
    }

    md5_init(&ctx);
    md5_update(&ctx, (uint8*)g_socks_mgr.user, g_socks_mgr.user_len);
    md5_update(&ctx, (uint8*)g_socks_mgr.password, g_socks_mgr.passwd_len);
    md5_final(&ctx, g_socks_mgr.auth_md5_buf);
}

static void reload_config()
{
    g_socks_mgr.log_level = get_private_profile_int("general", "log_level", 7, g_config_file);
    g_socks_mgr.log_type = get_private_profile_int("general", "log_type", 1, g_config_file);
    get_private_profile_string("general", "username", "", g_socks_mgr.user, 255, g_config_file);
    g_socks_mgr.user_len = (uint8)strlen(g_socks_mgr.user);
    get_private_profile_string("general", "password", "", g_socks_mgr.password, 255, g_config_file);
    g_socks_mgr.passwd_len = (uint8)strlen(g_socks_mgr.password);
    g_socks_mgr.connect_timeout_sec = get_private_profile_int("general", "connect_timeout", 3, g_config_file);
    g_socks_mgr.poll_timeout_sec = get_private_profile_int("general", "poll_timeout", 10, g_config_file);

    srv_create_auth_md5();
    LOGGER.log_init(g_socks_mgr.log_level, NULL, NULL);
}

static bool32 load_config(int argc, char **argv)
{
    char        path[256];

    get_options(argc, argv);
    if (g_config_file[0] == '\0') {
        printf("invalid config\n");
        return FALSE;
    }

    g_socks_mgr.type = (proxy_type_t)get_private_profile_int("general", "type", 0, g_config_file);
    g_socks_mgr.encrypt_type = (encrypt_type_t)get_private_profile_int("general", "encrypt_type", 0, g_config_file);
    g_socks_mgr.thread_count = get_private_profile_int("general", "thread_count", 10, g_config_file);
    get_private_profile_string("general", "bind-address", "0.0.0.0", g_socks_mgr.local_host, 255, g_config_file);
    g_socks_mgr.local_port = get_private_profile_int("general", "port", 1080, g_config_file);
    g_socks_mgr.data_buf_size = get_private_profile_int("general", "socket_buf_size", 20480, g_config_file);
    //
    g_socks_mgr.log_level = get_private_profile_int("general", "log_level", 7, g_config_file);
    g_socks_mgr.log_type = get_private_profile_int("general", "log_type", 1, g_config_file);
    get_private_profile_string("general", "username", "", g_socks_mgr.user, 255, g_config_file);
    g_socks_mgr.user_len = (uint8)strlen(g_socks_mgr.user);
    get_private_profile_string("general", "password", "", g_socks_mgr.password, 255, g_config_file);
    g_socks_mgr.passwd_len = (uint8)strlen(g_socks_mgr.password);
    g_socks_mgr.connect_timeout_sec = get_private_profile_int("general", "connect_timeout", 3, g_config_file);
    g_socks_mgr.poll_timeout_sec = get_private_profile_int("general", "poll_timeout", 10, g_config_file);

    if (g_socks_mgr.type == L_PROXY) {
        get_private_profile_string("remote", "host", "0.0.0.0", g_socks_mgr.remote_host, 255, g_config_file);
        g_socks_mgr.remote_port = get_private_profile_int("remote", "port", 1090, g_config_file);
    }

    srv_create_auth_md5();
    
    get_app_path(path);
    LOGGER.log_init(g_socks_mgr.log_level, path, "socks");

    return TRUE;
}

static bool32 service_initialize()
{
    my_socket   listen_fd;

    g_socks_mgr.reator_pool = reactor_pool_create(g_socks_mgr.thread_count);
    if (NULL == g_socks_mgr.reator_pool) {
        return FALSE;
    }

    listen_fd = vio_socket_listen(g_socks_mgr.local_host, g_socks_mgr.local_port);
    if (listen_fd == INVALID_SOCKET) {
        printf("can not create socket for listen\n");
        return FALSE;
    }
    reactor_register_listen(g_socks_mgr.reator_pool, listen_fd);
    if (FALSE == reactor_start_listen(g_socks_mgr.reator_pool, srv_epoll_accept)) {
        return FALSE;
    }
    reactor_start_poll(g_socks_mgr.reator_pool, socks_epoll_events);

    return TRUE;
}

uint32  g_pre_req_count = 0;
static void print_statistics()
{
    uint32   alloc_count, inprocess_count;
    uint32   free_threads, working_threads;
    uint32   req_count, tps;

    spin_lock(&g_socks_mgr.lock, NULL);
    alloc_count = g_socks_mgr.conn_alloc_count;
    inprocess_count = alloc_count - UT_LIST_GET_LEN(g_socks_mgr.free_list);
    working_threads = g_socks_mgr.working_threads;
    free_threads = g_socks_mgr.thread_count - working_threads;
    req_count = g_socks_mgr.req_count;
    tps = req_count - g_pre_req_count;
    g_pre_req_count = g_socks_mgr.req_count;
    spin_unlock(&g_socks_mgr.lock);

    LOGGER_ERROR(LOGGER, "-----------------------------------------------------------------------");
    LOGGER_ERROR(LOGGER, "| connection count |   total % 8d     |   inprocess % 4d          |", alloc_count, inprocess_count);
    LOGGER_ERROR(LOGGER, "-----------------------------------------------------------------------");
    LOGGER_ERROR(LOGGER, "| thread count     |   free  % 8d     |   working   % 4d          |", free_threads, working_threads);
    LOGGER_ERROR(LOGGER, "-----------------------------------------------------------------------");
    LOGGER_ERROR(LOGGER, "| client request   |   count % 8d     |   tps       % 4d          |", req_count, tps);
    LOGGER_ERROR(LOGGER, "-----------------------------------------------------------------------");
}

int main1(int argc, char *argv[])
{
    memset(&g_socks_mgr, 0, sizeof(socks_mgr_t));

    vio_socket_init();

    if (load_config(argc, argv) == FALSE) {
        printf("invalid config\n");
        return -1;
    }
    if (service_initialize() == FALSE) {
        printf("failed to initialize tcp service\n");
        return -1;
    }

    LOGGER_INFO(LOGGER, "service is started. host %s:%d", g_socks_mgr.local_host, g_socks_mgr.local_port);
    
    while(1)
    {
        os_thread_sleep(1000000);
        print_statistics();
        reload_config();
    }

    return 0;
}

