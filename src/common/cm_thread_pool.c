#include "cm_thread_pool.h"

#ifdef __cplusplus
extern "C" {
#endif

void* thread_worker_routine(void *arg); 

thread_pool_t* thread_pool_create(uint16 thread_count)
{
    uint32                i;
    os_thread_id_t        thread_id;
    thread_pool_t*        thread_pool;
    
    thread_pool = (thread_pool_t *)malloc(sizeof(thread_pool_t) + sizeof(thread_worker_t) * thread_count);
    thread_pool->thread_count = thread_count;
    thread_pool->lock = 0;
    UT_LIST_INIT(thread_pool->free_list);
    thread_pool->workers = (char *)thread_pool + sizeof(thread_pool_t);
    for (i = 0; i < thread_pool->thread_count; i++)
    {
        thread_worker_t *worker = (thread_worker_t *)(thread_pool->workers + sizeof(thread_pool_t) * i);
        worker->lock = 0;
        worker->event = os_event_create(NULL);
        UT_LIST_INIT(worker->task_list);
        UT_LIST_INIT(worker->high_task_list);
        UT_LIST_INIT(worker->free_list);
        os_thread_create(&thread_worker_routine, worker, &thread_id);
        
        UT_LIST_ADD_LAST(list_node, thread_pool->free_list, worker);
    }

    return thread_pool;
}

thread_worker_t* thread_pool_get_worker(thread_pool_t* pool)
{
    thread_worker_t* worker;
    
    spin_lock(&pool->lock, NULL);
    worker = UT_LIST_GET_FIRST(pool->free_list);
    if (worker != NULL) {
        UT_LIST_REMOVE(list_node, pool->free_list, worker);
    }
    spin_unlock(&pool->lock);

    return worker;
}

void thread_pool_release_worker(thread_pool_t* pool, thread_worker_t* worker)
{
    spin_lock(&pool->lock, NULL);
    UT_LIST_ADD_LAST(list_node, pool->free_list, worker);
    spin_unlock(&pool->lock);
}

bool32 thread_worker_add_task(thread_worker_t *worker, callback_data_t *data, os_callback_func func, bool32 is_high_priority)
{
    uint32           size;
    thread_task_t   *task;

    spin_lock(&worker->lock, NULL);
    task = UT_LIST_GET_FIRST(worker->free_list);
    if (task != NULL) {
        UT_LIST_REMOVE(list_node, worker->free_list, task);
        
        task->func = func;
        task->data = *data;
        
        if (is_high_priority) {
            UT_LIST_ADD_LAST(list_node, worker->high_task_list, task);
        } else {
            UT_LIST_ADD_LAST(list_node, worker->task_list, task);
        }
        size = UT_LIST_GET_LEN(worker->high_task_list) + UT_LIST_GET_LEN(worker->task_list);
    }
    spin_unlock(&worker->lock);

    if (task == NULL) {
        task = (thread_task_t *)malloc(sizeof(thread_task_t));
        task->func = func;
        task->data = *data;
        
        spin_lock(&worker->lock, NULL);
        if (is_high_priority) {
            UT_LIST_ADD_LAST(list_node, worker->high_task_list, task);
        } else {
            UT_LIST_ADD_LAST(list_node, worker->task_list, task);
        }
        size = UT_LIST_GET_LEN(worker->high_task_list) + UT_LIST_GET_LEN(worker->task_list);
        spin_unlock(&worker->lock);
    }

    if (1 == size) {
        os_event_set_signal(worker->event);
    }

    return TRUE;
}

void* thread_worker_routine(void *arg)
{
    thread_worker_t *worker = (thread_worker_t *)arg;
    thread_task_t   *task;
    
    while (1)
    {
        spin_lock(&worker->lock, NULL);
        task = UT_LIST_GET_FIRST(worker->high_task_list);
        if (task != NULL) {
            UT_LIST_REMOVE(list_node, worker->high_task_list, task);
        } else {
            task = UT_LIST_GET_FIRST(worker->task_list);
            if (task != NULL) {
                UT_LIST_REMOVE(list_node, worker->task_list, task);
            }
        }
        spin_unlock(&worker->lock);

        if (task == NULL) {
            os_event_wait_time(worker->event, 1000000);
            os_event_reset(worker->event);
            continue;
        }

        (*(task->func)) (&task->data);
        
        spin_lock(&worker->lock, NULL);
        UT_LIST_ADD_LAST(list_node, worker->free_list, task);
        spin_unlock(&worker->lock);
    }

    return NULL;
} 


#ifdef __cplusplus
}
#endif
