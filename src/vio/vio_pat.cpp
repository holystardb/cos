#include "cm_log.h"
#include "cm_util.h"
#include "cm_thread.h"
#include "cm_mutex.h"
#include "cm_timewheel.h"
#include "cm_biqueue.h"
#include "cm_datetime.h"
#include "vio_pat.h"
#include "vio_socket.h"
#include "vio_epoll_reactor.h"


#define MAX_PAT_COUNT        255
#define PAT_DATA_BUF_SIZE    1024
#define TIMEOUT_SECONDS      3
#define PAT_USER_NAME_LEN    64
#define PAT_PWD_NAME_LEN     64
#define PAT_HOST_LEN         64

typedef enum
{
    IS_UNINIT   = 0,
    IS_INITED   = 1,
    IS_CLIENT   = 2,
    IS_SERVER   = 3,
} pat_type_t;

typedef enum 
{
    PAT_AUTH_REQ      = 1,
    PAT_AUTH_RSP      = 2,
    PAT_CONTENT       = 3,
} conn_status_t;

typedef struct st_pat_data
{
    uint8*          data;
    uint32          len;
    uint16          event;
    uint8           pno;
    UT_LIST_NODE_T(struct st_pat_data) list_node;
} pat_data_t;

typedef struct
{
    uint8                   pno;
    uint8                   client_pno;
    uint16                  event;
    pat_callback_func       func_ptr;

    uint32                  count100ms;
    time_wheel_t            time_wheel;

    pat_type_t              type;
    char                    host[PAT_HOST_LEN];
    uint16                  port;

    os_thread_id_t          thread_id;
    os_thread_t             handle;

    spinlock_t              lock;
    os_event_t              os_event;
    UT_LIST_BASE_NODE_T(pat_data_t) send_list;
    UT_LIST_BASE_NODE_T(pat_data_t) recv_list;
} pat_t;

typedef struct
{
    biqueue_node_t      node;
    uint32              id;
    Vio                 vio;
    reactor_t          *reactor;
    struct sockaddr_in  sin;

    uint8              *buf;
    uint32              buf_size;
    uint32              buf_len;

    conn_status_t       status;
    pat_t              *pat;
} pat_conn_t;

typedef struct
{
    bool32                is_exited;
    
    pat_t                 pats[MAX_PAT_COUNT];
    uint32                count100ms;

    biqueue_t            *conn_pool;
    reactor_pool_t       *reator_pool;
    uint8                 reactor_count;

    char                  user[PAT_USER_NAME_LEN];
    char                  password[PAT_PWD_NAME_LEN];
    uint32                user_len;
    uint32                passwd_len;

    memory_pool_t        *mpool;
    UT_LIST_BASE_NODE_T(memory_page_t) used_pages;
} pat_mgr_t;


pat_mgr_t                     g_pat_mgr;
THREAD_LOCAL uint8            my_current_pno = 0xFF;

static void pat_handle_auth_req(pat_conn_t *conn);
static void pat_handle_auth_rsp(pat_conn_t *conn);
static void pat_handle_content(pat_conn_t *conn);
static pat_t* pat_init(uint8 pno, pat_callback_func func_ptr);
static void pat_destroy(pat_t* pat);


uint8 CURR_PNO()
{
    return my_current_pno;
}

uint16 CURR_EVENT()
{
    if (my_current_pno == INVALID_PNO) {
        return INVALID_EVENT;
    }

    return g_pat_mgr.pats[my_current_pno].event;
}

static void* timer_thread_entry(void *arg)
{
    date_clock_t clock;
    uint64       micro_seconds, ms;
    uint32       count100ms;
    
    current_clock(&clock);
    CLOCK_TO_MICRO_SECONDS(&clock, &micro_seconds);
    g_pat_mgr.count100ms = 0;
    count100ms = 0;
    while (!g_pat_mgr.is_exited) {
        os_thread_sleep(100000);

        current_clock(&clock);
        CLOCK_TO_MICRO_SECONDS(&clock, &ms);
        count100ms = (uint32)((ms - micro_seconds) / MICROSECS_PER_MILLISECOND / 100);
        if (g_pat_mgr.count100ms >= count100ms) {
            g_pat_mgr.count100ms++;
        } else {
            g_pat_mgr.count100ms = count100ms;
        }
    }
    return NULL;
}

static pat_conn_t* pat_alloc_conn(reactor_t* reactor)
{
    biqueue_node_t *node;
    pat_conn_t     *conn;

    node = biqueue_alloc(g_pat_mgr.conn_pool);
    if (node == NULL) {
        return NULL;
    }

    conn = OBJECT_OF_QUEUE_NODE(pat_conn_t, node, node);
    conn->reactor = reactor;
    conn->pat = NULL;
    vio_init(&conn->vio);
    conn->buf_len = 0;
    conn->status = PAT_AUTH_REQ;
    if (conn->buf == NULL) {
        conn->buf = (uint8 *)malloc(PAT_DATA_BUF_SIZE);
        conn->buf_size = PAT_DATA_BUF_SIZE;
    }

    LOGGER_DEBUG(LOGGER, LOG_MODULE_VIO_PAT, "pat_alloc_conn: connection(%d)", conn->id);

    return conn;
}

static void pat_free_conn(pat_conn_t* conn)
{
    biqueue_node_t *node;
    LOGGER_DEBUG(LOGGER, LOG_MODULE_VIO_PAT, "pat_free_conn: connection(%d)", conn->id);
    node = QUEUE_NODE_OF_OBJECT(conn, node);
    biqueue_free(g_pat_mgr.conn_pool, node);
}

static bool32 pat_epoll_accept(reactor_t* reactor, my_socket listen_fd, my_socket accept_fd)
{
    epoll_data_t   epoll_data;
    pat_conn_t    *conn;

    conn = pat_alloc_conn(reactor);
    if (conn == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "pat_epoll_accept: error for alloc conn, accept fd %d", accept_fd);
        return FALSE;
    }

    conn->vio.sock_fd = accept_fd;
    conn->vio.inactive = FALSE;
    epoll_data.ptr = conn;
    if (-1 == reactor_add_read(conn->reactor, accept_fd, &epoll_data, 0)) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "pat_epoll_accept: connection(id=%d, fd=%d) can not add event to epoll", conn->id, accept_fd);
        pat_free_conn(conn);
        return FALSE;
    }

    LOGGER_DEBUG(LOGGER, LOG_MODULE_VIO_PAT, "pat_epoll_accept: connection(id=%d, fd=%d) added event to epoll", conn->id, accept_fd);

    return TRUE;
}

static void pat_epoll_events(my_socket fd, struct epoll_event* event)
{
    pat_conn_t *conn = (pat_conn_t *)event->data.ptr;

    switch (conn->status)
    {
    case PAT_AUTH_REQ:
        pat_handle_auth_req(conn);
        break;
    case PAT_AUTH_RSP:
        pat_handle_auth_rsp(conn);
        break;
    case PAT_CONTENT:
        pat_handle_content(conn);
        break;
    default:
        break;
    }
}

bool32 pat_pool_init(uint8 reactor_count, memory_pool_t *mpool)
{
    uint32              i;
    os_thread_t         handle;
    os_thread_id_t      thr_id;

    memset(&g_pat_mgr, 0x00, sizeof(pat_mgr_t));

    for (i = 0; i < MAX_PAT_COUNT; i++) {
        g_pat_mgr.pats[i].pno = INVALID_PNO;
        g_pat_mgr.pats[i].client_pno = INVALID_PNO;
    }
    g_pat_mgr.user_len = 0;
    g_pat_mgr.passwd_len = 0;
    g_pat_mgr.mpool = mpool;

    vio_socket_init();

    g_pat_mgr.conn_pool = biqueue_init(sizeof(pat_conn_t), 1024, mpool);
    g_pat_mgr.reator_pool = reactor_pool_create(reactor_count, mpool);
    if (NULL == g_pat_mgr.reator_pool) {
        return FALSE;
    }
    g_pat_mgr.reactor_count = reactor_count;

    reactor_start_poll(g_pat_mgr.reator_pool, pat_epoll_events);

    g_pat_mgr.is_exited = FALSE;
    handle = os_thread_create(&timer_thread_entry, NULL, &thr_id);
    if (!os_thread_is_valid(handle)) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "pat_pool_init: Error for create_thread");
        return FALSE;
    }

    return TRUE;
}

void pat_pool_destroy()
{
    g_pat_mgr.is_exited = TRUE;
}

bool32 pat_pool_set_auth(char *user, char* password)
{
    g_pat_mgr.user_len = (int)strlen(user);
    g_pat_mgr.user_len = g_pat_mgr.user_len > PAT_USER_NAME_LEN ? PAT_USER_NAME_LEN : g_pat_mgr.user_len;
    strncpy_s(g_pat_mgr.user, PAT_USER_NAME_LEN, user, g_pat_mgr.user_len);
    g_pat_mgr.user[g_pat_mgr.user_len] = '\0';
    g_pat_mgr.passwd_len = (int)strlen(password);
    g_pat_mgr.passwd_len = g_pat_mgr.passwd_len > PAT_PWD_NAME_LEN ? PAT_PWD_NAME_LEN : g_pat_mgr.passwd_len;
    strncpy_s(g_pat_mgr.password, PAT_PWD_NAME_LEN, password, g_pat_mgr.passwd_len);
    g_pat_mgr.password[g_pat_mgr.passwd_len] = '\0';

    return TRUE;
}

static tw_timer_t* timer_alloc(void *arg)
{
    //pat_t              *pat = (pat_t *)arg;
    tw_timer_t         *timer;

    timer = (tw_timer_t *)malloc(sizeof(tw_timer_t));
    return timer;
}

static void timer_free(void *arg, tw_timer_t* timer)
{
    //pat_t          *pat = (pat_t *)arg;
    free(timer); 
}

static void pat_retry_connect(pat_conn_t *conn)
{
    epoll_data_t epoll_data;

    if (VIO_SUCCESS != vio_connect(&conn->vio, conn->pat->host, conn->pat->port, NULL, TIMEOUT_SECONDS)) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "pat_retry_connect: can not connect to server(pno %d %s:%d), error %d",
            conn->pat->pno, conn->pat->host, conn->pat->port, conn->vio.error_no);
        goto err_exit;
    } else {
        LOGGER_DEBUG(LOGGER, LOG_MODULE_VIO_PAT, "pat_retry_connect: connected to server(pno %d %s:%d), connection(%d : %d)",
            conn->pat->pno, conn->pat->host, conn->pat->port, conn->id, conn->vio.sock_fd);
    }
    
    // 4byte(len) + 1byte(client pno) + 1byte(user len) + nbyte + 1byte(password len) + nbyte
    conn->buf_len = 4;
    mach_write_to_1(conn->buf + conn->buf_len, conn->pat->client_pno);
    conn->buf_len += 1;
    mach_write_to_1(conn->buf + conn->buf_len, g_pat_mgr.user_len);
    conn->buf_len += 1;
    if (g_pat_mgr.user_len > 0) {
        memcpy(conn->buf + conn->buf_len, g_pat_mgr.user, g_pat_mgr.user_len);
        conn->buf_len += g_pat_mgr.user_len;
        mach_write_to_1(conn->buf + conn->buf_len, g_pat_mgr.passwd_len);
        conn->buf_len += 1;
        memcpy(conn->buf + conn->buf_len, g_pat_mgr.password, g_pat_mgr.passwd_len);
        conn->buf_len += g_pat_mgr.passwd_len;
    }
    mach_write_to_4(conn->buf, conn->buf_len - 4);
    if (VIO_SUCCESS != vio_write(&conn->vio, (const char *)conn->buf, conn->buf_len)) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "pat_retry_connect: error for send auth to server(pno %d %s:%d), connection(%d : %d), error %d",
            conn->pat->pno, conn->pat->host, conn->pat->port, conn->id, conn->vio.sock_fd, conn->vio.error_no);
        vio_close_socket(&conn->vio);
        goto err_exit;
    }
    conn->status = PAT_AUTH_RSP;
    epoll_data.ptr = conn;
    reactor_add_read(conn->reactor, conn->vio.sock_fd, &epoll_data, 0);

    return;

err_exit:

    os_thread_sleep(1000000);
}

static void pat_close_connection(pat_conn_t         *conn)
{
    spin_lock(&conn->pat->lock, NULL);
    if (conn->vio.inactive == FALSE) {
        reactor_del(conn->reactor, conn->vio.sock_fd);
        vio_close_socket(&conn->vio);
    }
    spin_unlock(&conn->pat->lock);
}

static void* pat_send_thread_entry(void *arg)
{
    pat_conn_t      *conn = (pat_conn_t *)arg;
    pat_t           *pat = conn->pat;
    pat_data_t      *data = NULL;

    while (pat->type == IS_CLIENT || (pat->type == IS_SERVER && conn->vio.inactive == FALSE))
    {
        if (conn->vio.sock_fd == INVALID_SOCKET && pat->type == IS_CLIENT) {
            pat_retry_connect(conn);
            os_thread_sleep(100000);
            continue;
        }

        //
        if (data == NULL) {
            spin_lock(&pat->lock, NULL);
            if ((data = UT_LIST_GET_FIRST(pat->send_list)))  {
                UT_LIST_REMOVE(list_node, pat->send_list, data);
                spin_unlock(&pat->lock);
            } else {
                spin_unlock(&pat->lock);
                os_event_wait_time(pat->os_event, 100000);
                os_event_reset(pat->os_event);
                continue;
            }
        }

        if (pat->pno != data->pno) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "pat_send_thread_entry: Error, dest pno(%d) is invalid, current pat pno(%d)",
                data->pno, pat->pno);
            free(data);
            data = NULL;
            continue;
        }

        if (VIO_SUCCESS != vio_write(&conn->vio, (const char*)data->data, data->len))
        {
            LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "pat_send_thread_entry: error for send data to pno(%d), connection(%d : %d), error %d",
                pat->pno, conn->id, conn->vio.sock_fd, conn->vio.error_no);
            pat_close_connection(conn);
        } else {
            LOGGER_DEBUG(LOGGER, LOG_MODULE_VIO_PAT, "pat_send_thread_entry: sended data to pno(%d), connection(%d : %d)",
                pat->pno, conn->id, conn->vio.sock_fd);

            free(data);
            data = NULL;
        }
    }

    spin_lock(&conn->pat->lock, NULL);
    if (data != NULL) {
        UT_LIST_ADD_FIRST(list_node, pat->send_list, data);
    }
    spin_unlock(&conn->pat->lock);

    pat_close_connection(conn);
    pat_free_conn(conn);

    return NULL;
}

static void pat_remove_timer(pat_t *pat, uint32 timer_id)
{
    pat_data_t      *data;

    spin_lock(&pat->lock, NULL);
    tw_timer_t* timer = time_wheel_get_timer(&pat->time_wheel, timer_id);
    if (timer == NULL) {
        return;
    }
    time_wheel_del_timer(&pat->time_wheel, timer);

    data = UT_LIST_GET_FIRST(pat->recv_list);
    while (data != NULL) {
        if (data->event >= EVENT_TIMER1 && data->event <= EVENT_TIMER1 && data->len == timer_id) {
            UT_LIST_REMOVE(list_node, pat->recv_list, data);
            break;
        }
        data = UT_LIST_GET_NEXT(list_node, data);
    }
    spin_unlock(&pat->lock);
}

static void pat_timer_tick(pat_t *pat)
{
    uint32      count100ms, count;

    count100ms = g_pat_mgr.count100ms;
    if (pat->count100ms > count100ms) {
        count = 0xFFFFFFFF - pat->count100ms + count100ms + 1;
        pat->count100ms = count100ms;
    } else {
        count = count100ms - pat->count100ms;
        pat->count100ms = count100ms;
    }
    while (count > 0) {
        spin_lock(&pat->lock, NULL);
        time_wheel_tick(&pat->time_wheel);
        spin_unlock(&pat->lock);
        count--;
    }
}

static void* pat_worker_thread_entry(void *arg)
{
    pat_t           *pat = (pat_t *)arg;
    pat_data_t      *data;

    my_current_pno = pat->pno;

    // init
    pat->count100ms = g_pat_mgr.count100ms;
    pat->event = EVENT_INIT;
    pat->func_ptr(pat->pno, EVENT_INIT, NULL, 0);

    while(1)
    {
        pat_timer_tick(pat);
        //
        spin_lock(&pat->lock, NULL);
        data = UT_LIST_GET_FIRST(pat->recv_list);
        if (data != NULL) {
            UT_LIST_REMOVE(list_node, pat->recv_list, data);
            spin_unlock(&pat->lock);
        } else {
            spin_unlock(&pat->lock);
            os_event_wait_time(pat->os_event, 100000);
            os_event_reset(pat->os_event);
            continue;
        }
        //
        if (EVENT_DEL_TIMER == data->event) {
            pat_remove_timer(pat, data->len);
        } else {
            pat->event = data->event;
            pat->func_ptr(data->pno, data->event, data->data, data->len);
        }
        //
        free(data);
    }
    return NULL;
}

static void pat_handle_auth_req(pat_conn_t *conn)
{
    epoll_data_t epoll_data;
    int          ret;
    uint8       *user = NULL, *passwd = NULL;
    uint8        client_pno, user_len, passwd_len = 0;

    conn->buf_len = 4;
    ret = vio_read(&conn->vio, (const char*)conn->buf, conn->buf_len);
    if (VIO_SUCCESS != ret) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "pat_handle_auth_req: error for read from lproxy, connection(%d : %d), error %d",
            conn->id, conn->vio.sock_fd, conn->vio.error_no);
        goto err_exit;
    }
    conn->buf_len = mach_read_from_4(conn->buf);
    ret = vio_read(&conn->vio, (const char*)conn->buf, conn->buf_len);
    if (VIO_SUCCESS != ret) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "pat_handle_auth_req: error for read from lproxy, connection(%d : %d), error %d",
            conn->id, conn->vio.sock_fd, conn->vio.error_no);
        goto err_exit;
    }

    // decryption

    // 4byte(len) + 1byte(client pno) + 1byte(user len) + nbyte + 1byte(password len) + nbyte
    client_pno = mach_read_from_1(conn->buf);
    if (INVALID_PNO == client_pno) {
        LOGGER_DEBUG(LOGGER, LOG_MODULE_VIO_PAT, "pat_handle_auth_req: invalid pno, connection(%d : %d)",
            conn->id, conn->vio.sock_fd);
        goto err_exit;
    }
    user_len = mach_read_from_1(conn->buf + 1);
    if (user_len > 0) {
        user = conn->buf + 2;
        passwd_len = mach_read_from_1(conn->buf + 2 + user_len);
        passwd = conn->buf + 3 + user_len;
    } else {
        passwd_len = 0;
    }
    
    // check auth
    if (g_pat_mgr.user_len > 0) {
        if (user_len != g_pat_mgr.user_len || passwd_len != g_pat_mgr.passwd_len ||
            strncmp((const char*)g_pat_mgr.user, (const char*)user, user_len) != 0 ||
            strncmp((const char*)g_pat_mgr.password, (const char*)passwd, passwd_len) != 0) {
            LOGGER_DEBUG(LOGGER, LOG_MODULE_VIO_PAT, "pat_handle_auth_req: client pno %d is invalid user or password, connection(%d : %d)",
                client_pno, conn->id, conn->vio.sock_fd);
            goto err_exit;
        } else {
            LOGGER_DEBUG(LOGGER, LOG_MODULE_VIO_PAT, "pat_handle_auth_req: client pno(%d) is authenticated by user/password. connection(%d : %d)",
                client_pno, conn->id, conn->vio.sock_fd);
        }
    } else {
        LOGGER_DEBUG(LOGGER, LOG_MODULE_VIO_PAT, "pat_handle_auth_req: client pno(%d) is authenticated without user/password. connection(%d : %d)",
            client_pno, conn->id, conn->vio.sock_fd);
    }
    conn->buf[0] = 0;
    conn->buf_len = 1;
    ret = vio_write(&conn->vio, (const char*)conn->buf, conn->buf_len);
    if (VIO_SUCCESS != ret) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "pat_handle_auth_req: error for send result of authenticated to client pno %d by connection(%d : %d), error %d",
            client_pno, conn->id, conn->vio.sock_fd, conn->vio.error_no);
        goto err_exit;
    }

    if (g_pat_mgr.pats[client_pno].pno == INVALID_PNO) {
        if ((conn->pat = pat_init(client_pno, NULL)) == NULL) {
            goto err_exit;
        }
        conn->pat->type = IS_SERVER;
    } else {
        conn->pat = &g_pat_mgr.pats[client_pno];
    }

    conn->pat->handle = os_thread_create(pat_send_thread_entry, conn, &conn->pat->thread_id);
    if (!os_thread_is_valid(conn->pat->handle)) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "pat_handle_auth_req: failed to create thread for client pno %d", client_pno);
        goto err_exit;
    }

    conn->status = PAT_CONTENT;
    epoll_data.ptr = conn;
    reactor_mod_read_oneshot(conn->reactor, conn->vio.sock_fd, &epoll_data, 0);

    return;

err_exit:

    reactor_del(conn->reactor, conn->vio.sock_fd);
    vio_close_socket(&conn->vio);
    pat_free_conn(conn);
}

static void pat_handle_auth_rsp(pat_conn_t *conn)
{
    epoll_data_t epoll_data;
    int          ret;

    conn->buf_len = 1;
    ret = vio_read(&conn->vio, (const char*)conn->buf, conn->buf_len);
    if (VIO_SUCCESS != ret) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "handle_auth_rsp: error for read, pno %d connection(%d : %d), error %d",
            conn->pat->pno, conn->id, conn->vio.sock_fd, conn->vio.error_no);
        goto err_exit;
    }
    if (conn->buf[0] != 0) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "handle_auth_rsp: invalid user or password, pno %d connection(%d : %d)",
            conn->pat->pno, conn->id, conn->vio.sock_fd);
        goto err_exit;
    } else {
        LOGGER_DEBUG(LOGGER, LOG_MODULE_VIO_PAT, "handle_auth_rsp: pno %d authenticated, connection(%d : %d)",
            conn->pat->pno, conn->id, conn->vio.sock_fd);
    }
    // 
    conn->status = PAT_CONTENT;
    epoll_data.ptr = conn;
    reactor_mod_read_oneshot(conn->reactor, conn->vio.sock_fd, &epoll_data, 0);

    return;

err_exit:

    reactor_del(conn->reactor, conn->vio.sock_fd);
    vio_close_socket(&conn->vio);
}

static void pat_handle_content(pat_conn_t *conn)
{
    epoll_data_t epoll_data;
    int          ret;
    uint8        dest_pno, src_pno;

    conn->buf_len = 4;
    ret = vio_read(&conn->vio, (const char*)conn->buf, conn->buf_len);
    if (VIO_SUCCESS != ret) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "handle_content: error for read lenght of package, pno %d connection(%d : %d), error %d",
            conn->pat->pno, conn->id, conn->vio.sock_fd, conn->vio.error_no);
        goto err_exit;
    }
    
    conn->buf_len = mach_read_from_4(conn->buf);
    ret = vio_read(&conn->vio, (const char*)conn->buf, conn->buf_len);
    if (VIO_SUCCESS != ret) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "handle_content: error for read data, pno %d connection(%d : %d), error %d",
            conn->pat->pno, conn->id, conn->vio.sock_fd, conn->vio.error_no);
        goto err_exit;
    }

    dest_pno = mach_read_from_1(conn->buf);
    src_pno = mach_read_from_1(conn->buf + 1);
    if (pat_append_to_list(dest_pno, src_pno, EVENT_RECV, conn->buf + 2, conn->buf_len - 2) == FALSE) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "handle_content: error for append_to_list, dest pno %d src pno %d connection(%d : %d)",
            dest_pno, src_pno, conn->id, conn->vio.sock_fd);
    }

    // 
    conn->status = PAT_CONTENT;
    epoll_data.ptr = conn;
    reactor_mod_read_oneshot(conn->reactor, conn->vio.sock_fd, &epoll_data, 0);

    return;

err_exit:

    pat_close_connection(conn);
}

static pat_t* pat_init(uint8 pno, pat_callback_func func_ptr)
{
    pat_t       *pat;

    if (INVALID_PNO == pno || g_pat_mgr.pats[pno].pno != INVALID_PNO) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "pat_init: Error, pno(%d) is invalid or already exists", pno);
        return NULL;
    }

    pat = &g_pat_mgr.pats[pno];
    if (pat->type == IS_UNINIT) {
        pat->pno = pno;
        pat->client_pno = INVALID_PNO;
        pat->func_ptr = func_ptr;
        pat->event = EVENT_INIT;
        pat->type = IS_INITED;
        spin_lock_init(&pat->lock);
        pat->os_event = os_event_create(NULL);
        time_wheel_create(&pat->time_wheel, 1024, g_pat_mgr.mpool);
        UT_LIST_INIT(pat->send_list);
        UT_LIST_INIT(pat->recv_list);
    }

    return pat;
}

static void pat_destroy(pat_t* pat)
{
    if (pat->os_event) {
        os_event_destroy(pat->os_event);
        pat->os_event = NULL;
    }
    time_wheel_destroy(&pat->time_wheel);
    spin_lock_init(&pat->lock);
    pat->pno = INVALID_PNO;
    pat->type = IS_UNINIT;
}

bool32 pat_startup_as_server(uint8 pno, pat_callback_func func_ptr, char *host, uint16 port)
{
    pat_t       *pat;
    my_socket    listen_fd = INVALID_SOCKET;

    pat = pat_init(pno, func_ptr);
    if (NULL == pat) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "pat_startup_as_server: Error for pat_init, pno %d", pno);
        return FALSE;
    }

    listen_fd = vio_socket_listen(host, port);
    if (listen_fd == INVALID_SOCKET) {
        goto err_exit;
    }

    if (FALSE == reactor_register_listen(g_pat_mgr.reator_pool, listen_fd)) {
        goto err_exit;
    }

    if (FALSE == reactor_start_listen(g_pat_mgr.reator_pool, pat_epoll_accept)) {
        goto err_exit;
    }

    pat->handle = os_thread_create(pat_worker_thread_entry, pat, &pat->thread_id);
    if (!os_thread_is_valid(pat->handle)) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "pat_startup_as_server: failed to create thread for pno %d", pno);
        goto err_exit;
    }

    return TRUE;

err_exit:

    if (listen_fd == INVALID_SOCKET) {
        close_socket(listen_fd);
    }
    pat_destroy(pat);

    return FALSE;
}

bool32 pat_set_server_info(uint8 pno, char *host, uint16 port)
{
    pat_t       *pat;
    
    pat = pat_init(pno, NULL);
    if (NULL == pat) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "pat_set_server_info: Error for pat_init, pno %d", pno);
        return FALSE;
    }
    pat->type = IS_CLIENT;
    strcpy_s(pat->host, PAT_HOST_LEN, host);
    pat->port = port;

    return TRUE;
}

bool32 pat_startup_as_client(uint8 pno, pat_callback_func func_ptr)
{
    uint32       i;
    pat_conn_t  *conn;
    pat_t       *pat;

    pat = pat_init(pno, func_ptr);
    if (NULL == pat) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "pat_startup_as_client: Error for pat_init, pno %d", pno);
        return FALSE;
    }

    pat->handle = os_thread_create(pat_worker_thread_entry, pat, &pat->thread_id);
    if (!os_thread_is_valid(pat->handle)) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "pat_startup_as_client: failed to create thread for pno %d", pno);
        pat_destroy(pat);
        return FALSE;
    }

    for (i = 0; i < MAX_PAT_COUNT; i++) {
        if (g_pat_mgr.pats[i].pno == INVALID_PNO || g_pat_mgr.pats[i].type != IS_CLIENT) {
            continue;
        }
        if (g_pat_mgr.pats[i].client_pno != INVALID_PNO) {
            continue;
        }

        conn = pat_alloc_conn(get_roubin_reactor(g_pat_mgr.reator_pool));
        if (conn == NULL) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "pat_startup_as_client: failed to alloc conn for pno %d", g_pat_mgr.pats[i].pno);
            goto err_exit;
        }
        conn->pat = &g_pat_mgr.pats[i];
        conn->pat->client_pno = pno;
        
        g_pat_mgr.pats[i].handle = os_thread_create(pat_send_thread_entry, conn, &g_pat_mgr.pats[i].thread_id);
        if (!os_thread_is_valid(g_pat_mgr.pats[i].handle)) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_PAT, "pat_startup_as_client: failed to create thread for pno %d", g_pat_mgr.pats[i].pno);
            goto err_exit;
        }
    }

    return TRUE;

err_exit:

    return FALSE;

}

static bool32 add_data_to_pat(uint8 receiver, uint8 sender, uint16 event, void* data, uint32 len, bool32 is_first)
{
    pat_t               *pat;
    pat_data_t          *pdata;

    if (INVALID_PNO == receiver || g_pat_mgr.pats[receiver].pno == INVALID_PNO ||
        (INVALID_PNO != sender && g_pat_mgr.pats[sender].pno == INVALID_PNO)) {
        return FALSE;
    }
    pat = &g_pat_mgr.pats[receiver];

    pdata = (pat_data_t *)malloc(sizeof(pat_data_t) + len);
    pdata->event = event;
    pdata->pno = sender;
    pdata->len = len;
    pdata->data = (uint8 *)pdata + sizeof(pat_data_t);
    memcpy(pdata->data, data, len);

    spin_lock(&pat->lock, NULL);
    if (is_first) {
        UT_LIST_ADD_FIRST(list_node, pat->recv_list, pdata);
    } else {
        UT_LIST_ADD_LAST(list_node, pat->recv_list, pdata);
    }
    if (1 == UT_LIST_GET_LEN(pat->recv_list)) {
        spin_unlock(&pat->lock);
        os_event_set_signal(pat->os_event);
    } else {
        spin_unlock(&pat->lock);
    }

    return TRUE;
}

bool32 pat_append_to_list(uint8 receiver, uint8 sender, uint16 event, void* data, uint32 len)
{
    return add_data_to_pat(receiver, sender, event, data, len, FALSE);
}

bool32 pat_send_data(uint8 receiver, uint8 sender, char* data, uint32 len)
{
    pat_t               *pat;
    pat_data_t          *pdata;

    if (INVALID_PNO == receiver || g_pat_mgr.pats[receiver].pno == INVALID_PNO ||
        INVALID_PNO == sender || g_pat_mgr.pats[sender].pno == INVALID_PNO) {
        return FALSE;
    }
    pat = &g_pat_mgr.pats[receiver];

    // 4byte(totoal len) + 1byte(dest pno) + 1byte(src pno) + nbyte(data)
    pdata = (pat_data_t *)malloc(sizeof(pat_data_t) + len + 6);
    pdata->pno = receiver;
    pdata->len = len + 6;
    pdata->data = (uint8 *)pdata + sizeof(pat_data_t);
    mach_write_to_4(pdata->data, len + 2);
    mach_write_to_1(pdata->data + 4, receiver);
    mach_write_to_1(pdata->data + 5, sender);
    memcpy((char *)pdata->data + 6, data, len);

    spin_lock(&pat->lock, NULL);
    UT_LIST_ADD_LAST(list_node, pat->send_list, pdata);
    if (1 == UT_LIST_GET_LEN(pat->send_list)) {
        spin_unlock(&pat->lock);
        os_event_set_signal(pat->os_event);
    } else {
        spin_unlock(&pat->lock);
    }

    return TRUE;
}

void* timer_cb_func(tw_timer_t* timer, void *arg)
{
    pat_t *pat = (pat_t *)arg;
    pat_append_to_list(pat->pno, INVALID_PNO, timer->event, timer->arg, timer->timer_id);
    return NULL;
}

uint32 pat_set_timer(uint8 pno, uint16 event, uint32 count100ms, void* arg)
{
    pat_t              *pat;
    tw_timer_t         *timer;
    
    if (INVALID_PNO == pno || g_pat_mgr.pats[pno].pno == INVALID_PNO) {
        return INVALID_TIMER;
    }
    pat = &g_pat_mgr.pats[pno];

    spin_lock(&pat->lock, NULL);
    timer = time_wheel_set_timer(&pat->time_wheel, count100ms, timer_cb_func, event, pat);
    spin_unlock(&pat->lock);
    if (timer == NULL) {
        return INVALID_TIMER;
    }
    return timer->timer_id;
}

bool32 pat_del_timer(uint8 pno, uint32 timer_id)
{
    if (INVALID_PNO == pno || g_pat_mgr.pats[pno].pno == INVALID_PNO || INVALID_TIMER == timer_id) {
        return FALSE;
    }
    return add_data_to_pat(pno, INVALID_PNO, EVENT_DEL_TIMER, NULL, timer_id, TRUE);
}

