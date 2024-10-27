#include "vio_epoll.h"
#include "cm_biqueue.h"
#include "cm_rbt.h"
#include "cm_thread.h"
#include "cm_log.h"
#include "cm_util.h"

#ifdef __WIN__
#include <winsock2.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

memory_pool_t *g_mem_pool = NULL;
uint32         g_event_count = 4096;


#ifdef __WIN__

typedef struct epoll_event  epoll_event_t;

typedef struct st_fd_entry
{
    my_socket      fd;
    epoll_event_t  event;
    bool32         oneshot_flag;
    bool32         oneshot_enable;
} fd_entry_t;

typedef struct st_epfd_entry
{
    biqueue_node_t   queue_node;
    uint32           epfd;
    ib_rbt_t        *fd_entry_rbt;
    spinlock_t       fd_entry_rbt_lock;
} epfd_entry_t;

typedef struct st_entry_pool
{
    biqueue_t   *epfd_entry_pool;
} entry_pool_t;

static entry_pool_t  g_epfd_pool = { 0 };


int epoll_init(memory_pool_t *mpool, uint32 event_count)
{
    g_mem_pool = mpool;
    g_event_count = event_count;

    if (g_epfd_pool.epfd_entry_pool == NULL) {
        g_epfd_pool.epfd_entry_pool = biqueue_init(sizeof(epfd_entry_t), g_event_count, mpool);
    }
    return 0;
}

static int epoll_ctl_add(epfd_entry_t* epfd_entry, my_socket fd, struct epoll_event* event)
{
    const ib_rbt_node_t  *rbt_node;
    fd_entry_t            fd_entry;

    fd_entry.event = *event;
    fd_entry.fd = fd;
    if (fd_entry.event.events & EPOLLONESHOT) {
        fd_entry.oneshot_enable = TRUE;
        fd_entry.oneshot_flag = TRUE;
    } else {
        fd_entry.oneshot_flag = FALSE;
    }

    spin_lock(&epfd_entry->fd_entry_rbt_lock, NULL);
    rbt_node = rbt_insert(epfd_entry->fd_entry_rbt, &fd_entry, &fd_entry);
    spin_unlock(&epfd_entry->fd_entry_rbt_lock);

    if (rbt_node == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_EPOLL, "epoll_ctl_add: error for malloc fd_entry, epoll fd %d, fd_entry %d", epfd_entry->epfd, fd);
        return -1;
    }

    return 0;
}

static int epoll_ctl_mod(epfd_entry_t* epfd_entry, my_socket fd, struct epoll_event* event)
{
    const ib_rbt_node_t  *rbt_node;
    fd_entry_t           *fd_entry = NULL, key;

    key.fd = (uint32)fd;
    spin_lock(&epfd_entry->fd_entry_rbt_lock, NULL);
    rbt_node = rbt_lookup(epfd_entry->fd_entry_rbt, &key);
    if (rbt_node != NULL) {
        fd_entry = (fd_entry_t *)rbt_value(fd_entry_t, rbt_node);
    }
    spin_unlock(&epfd_entry->fd_entry_rbt_lock);

    if (fd_entry == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_EPOLL, "epoll_ctl_mod: error for find fd_entry, epoll fd %d, fd_entry %d", epfd_entry->epfd, fd);
        return -1;
    }

    fd_entry->event = *event;
    if (fd_entry->event.events & EPOLLONESHOT) {
        fd_entry->oneshot_enable = TRUE;
        fd_entry->oneshot_flag = TRUE;
    } else {
        fd_entry->oneshot_flag = FALSE;
    }

    return 0;
}

static int epoll_ctl_del(epfd_entry_t* epfd_entry, my_socket fd, struct epoll_event* event)
{
    bool32                deleted;
    fd_entry_t            key;

    key.fd = (uint32)fd;
    spin_lock(&epfd_entry->fd_entry_rbt_lock, NULL);
    deleted = rbt_delete(epfd_entry->fd_entry_rbt, &key);
    spin_unlock(&epfd_entry->fd_entry_rbt_lock);

    if (!deleted) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_EPOLL, "epoll_ctl_del: error for free fd_entry, epoll fd %d, fd_entry %d", epfd_entry->epfd, fd);
        return -1;
    }

    return 0;
}

int epoll_ctl(int epfd, int op, my_socket fd, struct epoll_event *event)
{
    biqueue_node_t *node;
    epfd_entry_t   *entry;

    node = biqueue_get_node(g_epfd_pool.epfd_entry_pool, epfd);
    if (node == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_EPOLL, "epoll_ctl: error for get epfd_entry, epoll fd %d, fd %d", epfd, fd);
        return -1;
    }
    entry = OBJECT_OF_QUEUE_NODE(epfd_entry_t, node, queue_node);

    if (event != NULL) {
        BIT_RESET(event->events, EPOLLRDHUP);
        if (event->events == 0) {
            return 0;
        }
    }

    switch (op)
    {
        case EPOLL_CTL_ADD:
            return epoll_ctl_add(entry, fd, event);
        case EPOLL_CTL_MOD:
            return epoll_ctl_mod(entry, fd, event);
        case EPOLL_CTL_DEL:
            return epoll_ctl_del(entry, fd, event);
        default:
        {
            LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_EPOLL, "epoll_ctl: invalid op, epoll fd %d, fd %d op %d", epfd, fd, op);
            return -1;
        }
    }

    return 0;
}

static int fd_entry_cmp(const void *p1, const void *p2)
{
    const fd_entry_t*   fd_entry1 = (const fd_entry_t*) p1;
    const fd_entry_t*   fd_entry2 = (const fd_entry_t*) p2;

    return ((int)(fd_entry1->fd - fd_entry2->fd));
}

int epoll_create1(int flags)
{
    epfd_entry_t   *entry;
    biqueue_node_t *node;

    node = biqueue_alloc(g_epfd_pool.epfd_entry_pool); 
    if (node == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_EPOLL, "epoll_create1: error for malloc epoll_entry");
        return -1;
    }

    entry = OBJECT_OF_QUEUE_NODE(epfd_entry_t, node, queue_node);
    entry->epfd = node->node_id;
    spin_lock_init(&entry->fd_entry_rbt_lock);
    entry->fd_entry_rbt = rbt_create(sizeof(fd_entry_t), fd_entry_cmp, g_mem_pool);

    return entry->epfd;
}

static int epoll_wait_fd(int epfd, int maxevents, uint32 *loop, fd_entry_t *fds[FD_SETSIZE],
                              fd_set *rfds, fd_set *wfds, fd_set *efds)
{
    epfd_entry_t              *epfd_entry;
    const ib_rbt_node_t       *node;
    biqueue_node_t            *queue_node;

    queue_node = biqueue_get_node(g_epfd_pool.epfd_entry_pool, epfd);
    if (queue_node == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIO_EPOLL, "epoll_ctl: error for get epfd_entry, epoll fd %d", epfd);
        return -1;
    }
    epfd_entry = OBJECT_OF_QUEUE_NODE(epfd_entry_t, queue_node, queue_node);

    FD_ZERO(rfds);
    FD_ZERO(wfds);
    FD_ZERO(efds);

    spin_lock(&epfd_entry->fd_entry_rbt_lock, NULL);
    for (node = rbt_first(epfd_entry->fd_entry_rbt);
         node != NULL;
         node = rbt_next(epfd_entry->fd_entry_rbt, node))
    {
        bool32            is_need_set = FALSE;
        const fd_entry_t *fd_entry;

        fd_entry = (fd_entry_t *)rbt_value(fd_entry_t, node);
        fds[*loop] = (fd_entry_t *)fd_entry;
        
        if ((fd_entry->event.events & EPOLLIN) && (!fd_entry->oneshot_flag || fd_entry->oneshot_enable)) {
            FD_SET(fds[*loop]->fd, rfds);
            is_need_set = TRUE;
        }
        if (fd_entry->event.events & EPOLLOUT) {
            FD_SET(fds[*loop]->fd, wfds);
            is_need_set = TRUE;
        }
        if (is_need_set) {
            FD_SET(fds[*loop]->fd, efds);
            ++(*loop);
        }
    }
    spin_unlock(&epfd_entry->fd_entry_rbt_lock);

    return 0;
}

int epoll_wait(int epfd, struct epoll_event *events, int maxevents, int timeout_ms)
{
    uint32          loop,
                    nfds,
                    selected;
    fd_entry_t     *fds[FD_SETSIZE];
    fd_set          rfds, wfds, efds;
    bool32          rfdsetted, wfdsetted, efdsetted;
    int             ret;
    struct timeval  tv;

    loop = 0;
    if (epoll_wait_fd(epfd, maxevents, &loop, fds, &rfds, &wfds, &efds) != 0) {
        return -1;
    }

    if (loop == 0) {
        os_thread_sleep(1000);
        return 0;
    }

    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    ret = select(0, &rfds, &wfds, &efds, &tv);
    if (ret <= 0) {
        return 0;
    }

    nfds = loop;
    selected = 0;
    for (loop = 0; loop < nfds; ++loop)
    {
        rfdsetted = FD_ISSET(fds[loop]->fd, &rfds);
        wfdsetted = FD_ISSET(fds[loop]->fd, &wfds);
        efdsetted = FD_ISSET(fds[loop]->fd, &efds);
        if (rfdsetted || wfdsetted || efdsetted) {
            events[selected].events = 0;
            events[selected].events |= rfdsetted ? EPOLLIN : 0;
            events[selected].events |= wfdsetted ? EPOLLOUT : 0;
            events[selected].events |= efdsetted ? EPOLLERR : 0;
            events[selected].data = fds[loop]->event.data;
            if (fds[loop]->oneshot_flag) {
                fds[loop]->oneshot_enable = FALSE;
            }
            selected++;
        }
    }

    return selected;
}

#endif

int epoll_close(int epfd)
{
#ifdef __WIN__
    biqueue_node_t      *node;
    epfd_entry_t        *epfd_entry;

    node = biqueue_get_node(g_epfd_pool.epfd_entry_pool, epfd);
    if (node == NULL) {
        return -1;
    }
    epfd_entry = OBJECT_OF_QUEUE_NODE(epfd_entry_t, node, queue_node);

    rbt_free(epfd_entry->fd_entry_rbt);
    epfd_entry->fd_entry_rbt = NULL;

    spin_lock_init(&epfd_entry->fd_entry_rbt_lock);

    biqueue_free(g_epfd_pool.epfd_entry_pool, node);

    return 0;
#else
    return close(epfd);
#endif
}


#ifdef __cplusplus
}
#endif

