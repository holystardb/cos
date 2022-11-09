#ifndef _CM_THREAD_POOL_H
#define _CM_THREAD_POOL_H

#include "cm_type.h"
#include "cm_thread.h"
#include "cm_mutex.h"
#include "cm_list.h"
#include "cm_queue.h"
#include "cm_memory.h"


#ifdef __cplusplus
extern "C" {
#endif

typedef struct st_thread_task thread_task_t;
struct st_thread_task
{
    callback_func         func; 
    callback_data_t       data;
    UT_LIST_NODE_T(thread_task_t) list_node;
};

typedef struct st_thread_worker thread_worker_t;

typedef struct
{
    memory_pool_t      *mem_pool;
    os_event_t          event;
    spinlock_t          lock;
    uint16              thread_count;
    UT_LIST_BASE_NODE_T(thread_worker_t) free_workers;
} thread_pool_t; 

struct st_thread_worker
{
    UT_LIST_NODE_T(thread_worker_t) list_node;
    thread_pool_t      *pool;
    os_event_t          task_event;
    os_event_t          join_event;
    spinlock_t          lock;
    uint32              ref_count;
    UT_LIST_BASE_NODE_T(thread_task_t) running_tasks;
    UT_LIST_BASE_NODE_T(thread_task_t) free_tasks;
}; 

thread_pool_t* thread_pool_create(memory_pool_t *mem_pool, uint16 thread_count);
thread_worker_t* thread_pool_get_worker(thread_pool_t* pool, uint32 wait_microseconds);
void thread_pool_release_worker(thread_pool_t* pool, thread_worker_t* worker);
bool32 thread_worker_task_start(thread_worker_t *worker, callback_data_t *data, callback_func func);
void thread_worker_task_join(thread_worker_t *worker);


#ifdef __cplusplus
}
#endif

#endif  /* _CM_THREAD_POOL_H */

