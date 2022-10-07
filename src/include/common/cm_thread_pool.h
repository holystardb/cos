#ifndef _CM_THREAD_POOL_H
#define _CM_THREAD_POOL_H

#include "cm_type.h"
#include "cm_thread.h"
#include "cm_mutex.h"
#include "cm_list.h"
#include "cm_queue.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct st_thread_task thread_task_t;
struct st_thread_task
{
    os_callback_func      func; 
    callback_data_t       data;
    UT_LIST_NODE_T(thread_task_t) list_node;
};

typedef struct st_thread_worker thread_worker_t;
struct st_thread_worker
{
    os_event_t          event;
    spinlock_t          lock;
    UT_LIST_NODE_T(thread_worker_t) list_node;
    UT_LIST_BASE_NODE_T(thread_task_t) high_task_list;
    UT_LIST_BASE_NODE_T(thread_task_t) task_list;
    UT_LIST_BASE_NODE_T(thread_task_t) free_list;
}; 

typedef struct
{
    uint16              thread_count;
    spinlock_t          lock;
    char               *workers;
    UT_LIST_BASE_NODE_T(thread_worker_t) free_list;
} thread_pool_t; 



thread_pool_t* thread_pool_create(uint16 thread_count);
thread_worker_t* thread_pool_get_worker(thread_pool_t* pool);
void thread_pool_release_worker(thread_pool_t* pool, thread_worker_t* worker);
bool32 thread_worker_add_task(thread_worker_t *worker,
                                       callback_data_t *data, os_callback_func func, bool32 is_high_priority);


#ifdef __cplusplus
}
#endif

#endif  /* _CM_THREAD_POOL_H */

