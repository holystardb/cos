#include "vio_epoll_reactor.h"
#include "cm_log.h"
#include "cm_datetime.h"

#define EPOLL_WAIT_TIMEOUT         1  // millisecond
#define EPOLL_MAX_EVENTS           256


#ifdef __cplusplus
extern "C" {
#endif

static void reactor_set_timer(reactor_t *reactor, reactor_data_t *timer, uint32 count100ms)
{
    uint32 ticks, rotation, ts;

    LOG_PRINT_DEBUG("reactor_set_timer: epoll_fd %d fd=%d", reactor->epoll_fd_with_timeout, timer->fd);
    
    if (timer->timeout_100ms > 0) {
        OS_ASSERT(timer->timeout_100ms == 0);
    }
    timer->timeout_100ms = count100ms;

    if(count100ms < REACTOR_TIME_WHEEL_SI) {
        ticks = 1;
    } else {
        ticks = count100ms / REACTOR_TIME_WHEEL_SI;
    }
    /*计算待插入的定时器z在时间轮转动多少圈后被触发*/
    rotation = ticks / REACTOR_TIME_WHEEL_N;
    /*计算待插入的定时器应该被插入哪个槽中*/
    ts = (reactor->cur_slot + (ticks % REACTOR_TIME_WHEEL_N)) % REACTOR_TIME_WHEEL_N;
    /* 创建新的定时器, 他在时间轮转动rotation圈滞后被触发，且位于第ts个槽上 */
    timer->rotation = rotation;
    timer->time_slot = ts;
    timer->prev = NULL;
    timer->next = NULL;
    //如果ts个槽中尚无定时器，则把新建的定时器插入其中，并将该定时器设置为该槽的头结点
    if(!reactor->slots[ts]) {
        reactor->slots[ts] = timer;
    } else { /*否则,将定时器插入ts槽中*/
        timer->next = reactor->slots[ts];
        reactor->slots[ts]->prev = timer;
        reactor->slots[ts] = timer;
    }
}

static void reactor_del_timer(reactor_t *reactor, reactor_data_t *timer)
{
    uint32 ts;

    LOG_PRINT_DEBUG("reactor_del_timer: epoll_fd %d fd=%d", reactor->epoll_fd_with_timeout, timer->fd);
    
    if (timer->timeout_100ms == 0) {
        OS_ASSERT(timer->timeout_100ms > 0);
    }
    timer->timeout_100ms = 0;

    ts = timer->time_slot;
    /*slots[ts] 是目标定时器所在槽的头结点。
    * 如果目标定时器就是该头结点, 则需要重置第ts个槽头结点
    */
    if(timer == reactor->slots[ts]) {
        reactor->slots[ts] = reactor->slots[ts]->next;
        if(reactor->slots[ts]) {
            reactor->slots[ts]->prev = NULL;
        }
    } else {
        OS_ASSERT(timer->prev != NULL || timer->next != NULL);

        timer->prev->next = timer->next;
        if(timer->next) {
            timer->next->prev = timer->prev;
        }
    }
}

static void reactor_reset_timer(reactor_t *reactor, reactor_data_t *timer, uint32 count100ms)
{
    if (timer->timeout_100ms > 0) {
        reactor_del_timer(reactor, timer);
    }
    reactor_set_timer(reactor, timer, count100ms);
}

static reactor_data_t* reactor_append_timeout_list(reactor_t *reactor, reactor_data_t* head, reactor_data_t* timer)
{
    if (timer == NULL) {
        return head;
    }

    if (head == NULL) {
        head = timer;
        timer->next = NULL;
    } else {
        timer->next = head;
    }
    return timer;
}

/*SI 时间到后，调用该函数，时间轮向前滚动一个槽的间隔*/
/* 使用方法，每隔一秒执行一次这函数 */
reactor_data_t* reactor_time_wheel_tick(reactor_t *reactor)
{
    reactor_data_t *timer = NULL, *tmp, *tmp2;
    
    tmp = reactor->slots[reactor->cur_slot];
    while(tmp) {
        /*如果定时器的rotation值大于0，则他在这一轮不起作用*/
        if(tmp->rotation > 0) {
            tmp->rotation--;
            tmp = tmp->next;
        } else { /*否则，说明定时器已经到期了，于是执行定时任务，然后删除定时器*/
            tmp2 = tmp->next;
            reactor_del_timer(reactor, tmp);
            timer = reactor_append_timeout_list(reactor, timer, tmp);
            tmp = tmp2;
        }
    }
    /*更新当前时间轮的槽，以反应时间轮的转动*/
    reactor->cur_slot++;
    reactor->cur_slot = reactor->cur_slot % REACTOR_TIME_WHEEL_N;

    return timer;
}

static reactor_data_t* alloc_reactor_data(reactor_t *reactor, my_socket fd)
{
    const ib_rbt_node_t  *rbt_node;
    reactor_data_t        key, *data = NULL;

    key.fd = fd;
    rbt_node = rbt_insert(reactor->data_rbt, &key, &key);
    if (rbt_node != NULL) {
        data = (reactor_data_t *)rbt_value(reactor_data_t, rbt_node);
        data->timeout_100ms = 0;
    }
    return data;
}

static reactor_data_t* get_reactor_data(reactor_t *reactor, my_socket fd)
{
    const ib_rbt_node_t  *rbt_node;
    reactor_data_t        key, *data = NULL;
    
    key.fd = fd;
    rbt_node = rbt_lookup(reactor->data_rbt, &key);
    if (rbt_node != NULL) {
        data = (reactor_data_t *)rbt_value(reactor_data_t, rbt_node);
    }
    return data;
}

static void free_reactor_data(reactor_t *reactor, my_socket fd)
{
    reactor_data_t key;
    key.fd = fd;
    rbt_delete(reactor->data_rbt, &key);
}

static int reactor_add(reactor_t *reactor, my_socket fd, epoll_data_t *data, uint32 events, uint16 timeout_sec)
{
    struct epoll_event  ev;
    reactor_data_t     *r_data;

    LOG_PRINT_DEBUG("reactor_add: epoll_fd %d fd %d", reactor->epoll_fd_with_timeout, fd);

    spin_lock(&reactor->lock, NULL);
    r_data = alloc_reactor_data(reactor, fd);
    if (r_data != NULL) {
        r_data->data = *data;
        if (timeout_sec > 0) {
            reactor_set_timer(reactor, r_data, timeout_sec * 10);
        }
    }
    spin_unlock(&reactor->lock);

    if (r_data == NULL) {
        return -1;
    }

    ev.events = events;
    ev.data.ptr = r_data;
    if (epoll_ctl(reactor->epoll_fd_with_timeout, EPOLL_CTL_ADD, fd, &ev) != 0)
    {
        spin_lock(&reactor->lock, NULL);
        if (timeout_sec > 0) {
            reactor_del_timer(reactor, r_data);
        }
        free_reactor_data(reactor, fd);
        spin_unlock(&reactor->lock);
        return -1;
    }
    return 0;
}

int reactor_add_read(reactor_t *reactor, my_socket fd, epoll_data_t *data, uint16 timeout_sec)
{
    uint32 events;
    events = EPOLLIN | EPOLLONESHOT;
    return reactor_add(reactor, fd, data, events, timeout_sec);
}

int reactor_add_write(reactor_t *reactor, my_socket fd, epoll_data_t *data, uint16 timeout_sec)
{
    uint32 events;
    events = EPOLLOUT | EPOLLONESHOT;
    return reactor_add(reactor, fd, data, events, timeout_sec);
}

int reactor_del(reactor_t *reactor, my_socket fd)
{
    reactor_data_t     *r_data;
    
    LOG_PRINT_DEBUG("reactor_del: epoll_fd %d fd %d", reactor->epoll_fd_with_timeout, fd);

    if (epoll_ctl(reactor->epoll_fd_with_timeout, EPOLL_CTL_DEL, fd, NULL) != 0) {
        return -1;
    }

    spin_lock(&reactor->lock, NULL);
    r_data = get_reactor_data(reactor, fd);
    if (r_data != NULL) {
        if (r_data->timeout_100ms > 0) {
            reactor_del_timer(reactor, r_data);
        }
        free_reactor_data(reactor, fd);
        spin_unlock(&reactor->lock);
    } else {
        spin_unlock(&reactor->lock);
        LOG_PRINT_ERROR("reactor_del: epoll_fd %d can not found fd %d", reactor->epoll_fd_with_timeout, fd);
    }
    
    return 0;
}

static int reactor_mod_oneshot(reactor_t *reactor, my_socket fd, epoll_data_t *data, uint32 events, uint16 timeout_sec)
{
    struct epoll_event  ev;
    reactor_data_t     *r_data;

    LOG_PRINT_DEBUG("reactor_mod: epoll_fd %d fd %d", reactor->epoll_fd_with_timeout, fd);

    spin_lock(&reactor->lock, NULL);
    r_data = get_reactor_data(reactor, fd);
    spin_unlock(&reactor->lock);
    if (r_data == NULL) {
        LOG_PRINT_ERROR("reactor_mod: epoll_fd %d can not found fd %d", reactor->epoll_fd_with_timeout, fd);
        return -1;
    }
    r_data->data = *data;

    ev.events = events;
    ev.data.ptr = r_data;
    if (epoll_ctl(reactor->epoll_fd_with_timeout, EPOLL_CTL_MOD, fd, &ev) != 0) {
        return -1;
    }

    if (timeout_sec > 0) {
        spin_lock(&reactor->lock, NULL);
        reactor_reset_timer(reactor, r_data, timeout_sec * 10);
        spin_unlock(&reactor->lock);
    } else if (r_data->timeout_100ms > 0) {
        spin_lock(&reactor->lock, NULL);
        reactor_del_timer(reactor, r_data);
        spin_unlock(&reactor->lock);
    }

    return 0;
}

int reactor_mod_read_oneshot(reactor_t *reactor, my_socket fd, epoll_data_t *data, uint16 timeout_sec)
{
    uint32  events;
    events = EPOLLIN | EPOLLONESHOT;
    return reactor_mod_oneshot(reactor, fd, data, events, timeout_sec);
}

int reactor_mod_write_oneshot(reactor_t *reactor, my_socket fd, epoll_data_t *data, uint16 timeout_sec)
{
    uint32  events;
    events = EPOLLOUT | EPOLLONESHOT;
    return reactor_mod_oneshot(reactor, fd, data, events, timeout_sec);
}

int reactor_epoll_add_read(reactor_t *reactor, my_socket fd, epoll_data_t *data)
{
    struct epoll_event  ev;
    ev.events = EPOLLIN | EPOLLONESHOT;
    ev.data = *data;
    if (epoll_ctl(reactor->epoll_fd_without_timeout, EPOLL_CTL_ADD, fd, &ev) != 0) {
        return -1;
    }
    return 0;
}

int reactor_epoll_del(reactor_t *reactor, my_socket fd)
{
    if (epoll_ctl(reactor->epoll_fd_without_timeout, EPOLL_CTL_DEL, fd, NULL) != 0) {
        return -1;
    }
    return 0;
}

int reactor_epoll_mod_oneshot(reactor_t *reactor, my_socket fd, epoll_data_t *data)
{
    struct epoll_event  ev;
    ev.events = EPOLLIN | EPOLLONESHOT;
    ev.data = *data;
    if (epoll_ctl(reactor->epoll_fd_without_timeout, EPOLL_CTL_MOD, fd, &ev) != 0) {
        return -1;
    }
    return 0;
}

static int reactor_data_cmp(const void *p1, const void *p2)
{
    const reactor_data_t*   data1 = (const reactor_data_t*) p1;
    const reactor_data_t*   data2 = (const reactor_data_t*) p2;
    return ((int)(data1->fd - data2->fd));
}

reactor_pool_t* reactor_pool_create(uint32 reactor_count)
{
    uint32          i, j;
    reactor_pool_t* pool;

#ifdef __WIN__
    if (epoll_init()) {
        return NULL;
    }
#endif

    pool = (reactor_pool_t *)malloc(sizeof(reactor_pool_t) + sizeof(reactor_t) * reactor_count);
    pool->reactor_count = reactor_count;
    pool->reactors = (reactor_t *)((char *)pool + sizeof(reactor_pool_t));
    pool->round_roubin = 0;
    spin_lock_init(&pool->lock);
    pool->is_end = FALSE;
    pool->acpt_epoll_fd = -1;
    //
    for (i = 0; i < reactor_count; i++) {
        reactor_t *reactor = pool->reactors + i;
        reactor->is_end = FALSE;
        spin_lock_init(&reactor->lock);
        //
        reactor->cur_slot = 0;
        reactor->micro_second_ticks = 0;
        for(j = 0; j < REACTOR_TIME_WHEEL_N; j++) {
            reactor->slots[j] = NULL;  /*初始化每个槽的头结点*/
        }
        
        reactor->data_rbt = rbt_create(sizeof(reactor_data_t), reactor_data_cmp);
        reactor->epoll_fd_with_timeout = epoll_create1(0);
        reactor->epoll_fd_without_timeout = epoll_create1(0);
        if (reactor->epoll_fd_without_timeout == -1 || reactor->epoll_fd_with_timeout == -1) {
            pool->reactor_count = i;
            goto err_exit;
        }
    }
    //
    pool->acpt_epoll_fd = epoll_create1(0);
    if (pool->acpt_epoll_fd == -1) {
        goto err_exit;
    }
#ifdef __WIN__
    pool->acpt_thread = NULL;
#else
    pool->acpt_thread = 0;
#endif
    //
    pool->reactor_pool_destroy = reactor_pool_destroy;
    pool->get_roubin_reactor = get_roubin_reactor;
    pool->reactor_start_listen = reactor_start_listen;
    pool->reactor_start_poll = reactor_start_poll;

    return pool;

err_exit:

    reactor_pool_destroy(pool);
    return NULL;
}

void reactor_pool_destroy(reactor_pool_t* pool)
{
    uint32      i;
    
    for (i = 0; i < pool->reactor_count; i++) {
        reactor_t *reactor = pool->reactors + i;
        epoll_close(reactor->epoll_fd_with_timeout);
        epoll_close(reactor->epoll_fd_without_timeout);
    }
    if (pool->acpt_epoll_fd != -1) {
        epoll_close(pool->acpt_epoll_fd);
    }
    free(pool);
}

reactor_t* get_roubin_reactor(reactor_pool_t* pool)
{
    reactor_t *reactor;
    
    spin_lock(&pool->lock, NULL);
    reactor = pool->reactors + pool->round_roubin;
    pool->round_roubin++;
    if (pool->round_roubin >= pool->reactor_count) {
        pool->round_roubin = 0;
    }
    spin_unlock(&pool->lock);
    return reactor;
}

static void reactor_handle_accept(reactor_pool_t* pool, my_socket listen_fd)
{
    reactor_t           *reactor;
    my_socket            accept_fd;
    char                 ip_addr[INET6_ADDRSTRLEN];
    struct sockaddr      in_addr = { 0 };
    socklen_t            in_addr_len = sizeof (in_addr);

    while (!pool->is_end)
    {
        accept_fd = accept(listen_fd, &in_addr, &in_addr_len);
        if (INVALID_SOCKET == accept_fd) {
            int error_no = socket_errno;
            if (error_no != SOCKET_EINTR && error_no != SOCKET_EAGAIN && error_no != SOCKET_EWOULDBLOCK) {
                LOG_PRINT_ERROR("reactor_handle_accept: error for accept, error %d", error_no);
            }
            return;
        }

        if (vio_getnameinfo(&in_addr, ip_addr, sizeof(ip_addr), NULL, 0, NI_NUMERICHOST)) {
            LOG_PRINT_ERROR("reactor_handle_accept: fails to print out IP-address, fd %d", accept_fd);
            close_socket(accept_fd);
            continue;
        }
    
        if (-1 == vio_set_blocking(accept_fd, TRUE, TRUE)) {
            LOG_PRINT_ERROR("reactor_handle_accept: error for vio_set_blocking, fd %d", accept_fd);
            close_socket(accept_fd);
            continue;
        }
        vio_set_buffer_size(accept_fd, M_TCP_SNDBUF_SIZE, M_TCP_RCVBUF_SIZE);
        vio_set_keep_alive(accept_fd, M_TCP_KEEP_IDLE, M_TCP_KEEP_INTERVAL, M_TCP_KEEP_COUNT);
        vio_set_linger(accept_fd, 1, 1);

        //
        reactor = get_roubin_reactor(pool);
        if (FALSE == pool->acpt_func(reactor, listen_fd, accept_fd)) {
            LOG_PRINT_ERROR("reactor_handle_accept: socket(fd %d) is closed", accept_fd);
            close_socket(accept_fd);
        }
    }
}

static void* reactor_accept_entry(void *arg)
{
    reactor_pool_t         *pool = (reactor_pool_t *)arg;
    struct epoll_event      events[EPOLL_MAX_EVENTS];
    int                     nfds, i;

    while (!pool->is_end)
    {
        nfds = epoll_wait(pool->acpt_epoll_fd, events, EPOLL_MAX_EVENTS, EPOLL_WAIT_TIMEOUT);
        for (i = 0 ; i < nfds; i++) {
            if ((events[i].events & EPOLLERR) || (events[i].events & EPOLLHUP) || !(events[i].events & EPOLLIN)) {
                close_socket(events[i].data.fd);
                continue;
            }
            reactor_handle_accept(pool, events[i].data.fd);
        }
    }

    return NULL;
}

bool32 reactor_register_listen(reactor_pool_t* pool, my_socket listen_fd)
{
    struct epoll_event  ev;
    ev.events   = EPOLLIN;
    ev.data.fd  = listen_fd;
    if (epoll_ctl(pool->acpt_epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev) == -1) {
        return FALSE;
    }

    return TRUE;
}

bool32 reactor_start_listen(reactor_pool_t* pool, accept_callback_func func)
{
    if (os_thread_is_valid(pool->acpt_thread)) {
        return FALSE;
    }
    
    pool->acpt_func = func;
    pool->acpt_thread = os_thread_create(&reactor_accept_entry, pool, &pool->acpt_thread_id);
    if (!os_thread_is_valid(pool->acpt_thread)) {
        return FALSE;
    }

    return TRUE;
}

static reactor_data_t* reactor_get_timeout_data(reactor_t *reactor)
{
    reactor_data_t *r_data = NULL;
    date_clock_t    clock;
    uint64          micro_second_ticks, count;

    current_clock(&clock);
    CLOCK_TO_MICRO_SECONDS(&clock, &micro_second_ticks);

    if (micro_second_ticks >= reactor->micro_second_ticks) {
        count = (micro_second_ticks - reactor->micro_second_ticks) / 100000;
    } else {
        count = 1;
    }
    if (count > 0) {
        reactor->micro_second_ticks = micro_second_ticks;
    }

    spin_lock(&reactor->lock, NULL);
    while (count > 0) {
        r_data = reactor_append_timeout_list(reactor, r_data, reactor_time_wheel_tick(reactor));
        count--;
    }
    spin_unlock(&reactor->lock);

    return r_data;
}

static void reactor_handle_events_with_timeout(reactor_t *reactor)
{
    int nfds, loop;
    reactor_data_t     *r_data;
    struct epoll_event  events[EPOLL_MAX_EVENTS];

    // check timeout
    r_data = reactor_get_timeout_data(reactor);
    while (r_data != NULL) {
        events[0].events = EPOLLTIMEOUT;
        events[0].data = r_data->data;
        LOG_PRINT_DEBUG("reactor_handle_events: timeout, reactor epoll_fd %d fd %d",
            reactor->epoll_fd_with_timeout, r_data->fd);
        reactor->func(r_data->fd, &events[0]);
        r_data = r_data->next;
    }

    //
    nfds = epoll_wait(reactor->epoll_fd_with_timeout, events, EPOLL_MAX_EVENTS, EPOLL_WAIT_TIMEOUT);
    if (nfds == -1) {
        LOG_PRINT_ERROR("reactor_handle_events: error for epoll_wait, reactor epoll_fd = %d",
            reactor->epoll_fd_with_timeout);
        return;
    }
    if (nfds == 0) {
        return;
    }

    // delete timer
    spin_lock(&reactor->lock, NULL);
    for (loop = 0; loop < nfds; ++loop) {
        r_data = (reactor_data_t *)events[loop].data.ptr;
        if (r_data->timeout_100ms > 0) {
            LOG_PRINT_DEBUG("reactor_handle_events: delete timer, reactor epoll_fd %d fd %d",
                reactor->epoll_fd_with_timeout, r_data->fd);
            reactor_del_timer(reactor, r_data);
        }
    }
    spin_unlock(&reactor->lock);

    for (loop = 0; loop < nfds; ++loop) {
        r_data = (reactor_data_t *)events[loop].data.ptr;
        events[loop].data = r_data->data;
        reactor->func(r_data->fd, &events[loop]);
    }
}

static void reactor_handle_events(reactor_t *reactor)
{
    int nfds, loop;
    struct epoll_event events[EPOLL_MAX_EVENTS];
    
    nfds = epoll_wait(reactor->epoll_fd_without_timeout, events, EPOLL_MAX_EVENTS, EPOLL_WAIT_TIMEOUT);
    if (nfds == -1) {
        LOG_PRINT_ERROR("reactor_handle_events: error for epoll_wait, reactor epoll_fd = %d",
            reactor->epoll_fd_without_timeout);
        return;
    }
    if  (nfds == 0) {
        return;
    }

    for (loop = 0; loop < nfds; ++loop) {
        reactor->func(0, &events[loop]);
    }
}

static void* reactor_entry(void *arg)
{
    reactor_t *reactor = (reactor_t *)arg;
    while (!reactor->is_end)
    {
        reactor_handle_events_with_timeout(reactor);
        reactor_handle_events(reactor);
    }
    return NULL;
}

void reactor_start_poll(reactor_pool_t* pool, epoll_event_callback_func func)
{
    uint32          i;
    reactor_t      *reactor;
    date_clock_t    clock;
    uint64          micro_second_ticks;

    current_clock(&clock);
    CLOCK_TO_MICRO_SECONDS(&clock, &micro_second_ticks);

    for (i = 0; i < pool->reactor_count; i++) {
        reactor = pool->reactors + i;
        reactor->func = func;
        reactor->micro_second_ticks = micro_second_ticks;
        reactor->thread = os_thread_create(&reactor_entry, reactor, &reactor->thread_id);
    }
}

#ifdef __cplusplus
}
#endif

