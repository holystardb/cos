#ifndef _VIO_EPOLL_REACTOR_H
#define _VIO_EPOLL_REACTOR_H

#include "cm_type.h"
#include "cm_mutex.h"
#include "cm_thread.h"
#include "cm_rbt.h"
#include "vio_epoll.h"
#include "vio_socket.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ʱ�����ϲ۵����� */
#define REACTOR_TIME_WHEEL_N            600

/* ÿ100msʱ����ת��һ�Σ����ۼ��Ϊ100ms */
#define REACTOR_TIME_WHEEL_SI           1

typedef void (*epoll_event_callback_func) (my_socket fd, struct epoll_event* event);

typedef struct st_reactor_data reactor_data_t;
struct st_reactor_data
{
    epoll_data_t          data;  /*���������λ*/
    my_socket             fd;
    uint16                timeout_100ms;
    
    // The following variables are not allowed to be modified, only internal use
    uint32                rotation;  /*��¼��ʱ����ʱ����ת����Ȧ����Ч*/
    uint32                time_slot;  /*��¼��ʱ������ʱ�����ϵ��ĸ���(��Ӧ��������ͬ)*/
    reactor_data_t       *prev;
    reactor_data_t       *next;
};

typedef struct st_reactor
{
    int                         epoll_fd_without_timeout;
    int                         epoll_fd_with_timeout;
    epoll_event_callback_func   func;
    os_thread_t                 thread;
    os_thread_id_t              thread_id;
    spinlock_t                  lock;
    bool32                      is_end;
    /*ʱ���ֵĲۣ�����ÿ��Ԫ��ָ��һ����ʱ��������������*/
    reactor_data_t             *slots[REACTOR_TIME_WHEEL_N];
    uint32                      cur_slot; /*ʱ���ֵĵ�ǰ��*/
    uint64                      micro_second_ticks;
    ib_rbt_t                   *data_rbt;
} reactor_t;

typedef bool32 (*accept_callback_func) (reactor_t* reactor, my_socket listen_fd, my_socket accept_fd);

typedef struct st_reactor_pool
{
    uint32                reactor_count;
    uint32                round_roubin;
    reactor_t            *reactors;
    spinlock_t            lock;
    bool32                is_end;
    int                   acpt_epoll_fd;
    os_thread_t           acpt_thread;
    os_thread_id_t        acpt_thread_id;
    accept_callback_func  acpt_func;
    memory_pool_t        *mem_pool;

    void (*reactor_pool_destroy) (struct st_reactor_pool*);
    reactor_t* (*get_roubin_reactor) (struct st_reactor_pool*);
    bool32 (*reactor_register_listen) (struct st_reactor_pool*, my_socket);
    bool32 (*reactor_start_listen) (struct st_reactor_pool*, accept_callback_func);
    void (*reactor_start_poll) (struct st_reactor_pool*, epoll_event_callback_func);
} reactor_pool_t;

int reactor_add_read(reactor_t *reactor, my_socket fd, epoll_data_t *data, uint16 timeout_sec);
int reactor_add_write(reactor_t *reactor, my_socket fd, epoll_data_t *data, uint16 timeout_sec);
int reactor_del(reactor_t *reactor, my_socket fd);
int reactor_mod_read_oneshot(reactor_t *reactor, my_socket fd, epoll_data_t *data, uint16 timeout_sec);
int reactor_mod_write_oneshot(reactor_t *reactor, my_socket fd, epoll_data_t *data, uint16 timeout_sec);

int reactor_epoll_add_read(reactor_t *reactor, my_socket fd, epoll_data_t *data);
int reactor_epoll_del(reactor_t *reactor, my_socket fd);
int reactor_epoll_mod_oneshot(reactor_t *reactor, my_socket fd, epoll_data_t *data);

reactor_pool_t* reactor_pool_create(uint32 reactor_count, memory_pool_t *mem_pool = NULL);
void reactor_pool_destroy(reactor_pool_t* pool);

reactor_t* get_roubin_reactor(reactor_pool_t* pool);

bool32 reactor_register_listen(reactor_pool_t* pool, my_socket listen_fd);

bool32 reactor_start_listen(reactor_pool_t* pool, accept_callback_func func);

void reactor_start_poll(reactor_pool_t* pool, epoll_event_callback_func func);


#ifdef __cplusplus
}
#endif

#endif  /* _VIO_EPOLL_REACTOR_H */

