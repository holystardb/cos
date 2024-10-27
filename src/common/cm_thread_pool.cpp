#include "cm_thread_pool.h"

#ifdef __cplusplus
extern "C" {
#endif


void* thread_worker_routine(void *arg); 

thread_pool_t* thread_pool_create(memory_pool_t *mem_pool, uint16 thread_count)
{
    os_thread_id_t   thread_id;
    thread_pool_t   *thread_pool;
    uint32           task_count_pre_worker = 8;

    thread_pool = (thread_pool_t *)ut_malloc_zero(ut_align8(sizeof(thread_pool_t)) +
        ut_align8(sizeof(thread_worker_t)) * thread_count +
        ut_align8(sizeof(thread_task_t)) * task_count_pre_worker * thread_count);
    if (thread_pool == NULL) {
        return NULL;
    }

    thread_pool->thread_count = thread_count;
    spin_lock_init(&thread_pool->lock);
    thread_pool->event = os_event_create(NULL);
    thread_pool->mem_pool = mem_pool;
    UT_LIST_INIT(thread_pool->free_workers);

    char *workers_ptr = (char *)thread_pool + ut_align8(sizeof(thread_pool_t));
    char *tasks_ptr = (char *)workers_ptr + ut_align8(sizeof(thread_worker_t)) * thread_count;
    uint32 task_index = 0;

    for (uint32 i = 0; i < thread_pool->thread_count; i++)
    {
        thread_worker_t *worker = (thread_worker_t *)(workers_ptr + ut_align8(sizeof(thread_pool_t)) * i);
        spin_lock_init(&worker->lock);
        worker->task_event = os_event_create(NULL);
        worker->join_event = os_event_create(NULL);
        UT_LIST_INIT(worker->running_tasks);
        UT_LIST_INIT(worker->free_tasks);
        os_thread_create(&thread_worker_routine, worker, &thread_id);
        UT_LIST_ADD_LAST(list_node, thread_pool->free_workers, worker);

        for (uint32 j = 0; j < task_count_pre_worker; j++)
        {
            thread_task_t *task = (thread_task_t *)(tasks_ptr + ut_align8(sizeof(thread_task_t)) * task_index);
            task_index++;
            UT_LIST_ADD_LAST(list_node, worker->free_tasks, task);
        }
    }

    return thread_pool;
}

/*
    wait_microseconds = 0: no wait
*/
thread_worker_t* thread_pool_get_worker(thread_pool_t* pool, uint32 wait_microseconds)
{
    thread_worker_t *worker = NULL;
    bool32 is_first = TRUE;

    while (worker == NULL) {
        spin_lock(&pool->lock, NULL);
        worker = UT_LIST_GET_FIRST(pool->free_workers);
        if (worker != NULL) {
            UT_LIST_REMOVE(list_node, pool->free_workers, worker);
        }
        spin_unlock(&pool->lock);

        if (worker || wait_microseconds == 0 || !is_first) {
            break;
        }

        os_event_wait_time(pool->event, wait_microseconds);
        os_event_reset(pool->event);
        is_first = FALSE;
    }

    return worker;
}

void thread_pool_release_worker(thread_pool_t* pool, thread_worker_t* worker)
{
    uint32 size;

    spin_lock(&pool->lock, NULL);
    UT_LIST_ADD_LAST(list_node, pool->free_workers, worker);
    size = UT_LIST_GET_LEN(pool->free_workers);
    spin_unlock(&pool->lock);

    if (1 == size) {
        os_event_set_signal(pool->event);
    }
}

bool32 thread_worker_task_start(thread_worker_t *worker, callback_data_t *data, callback_func func)
{
    uint32           size;
    thread_task_t   *task;

    spin_lock(&worker->lock, NULL);
    task = UT_LIST_GET_FIRST(worker->free_tasks);
    if (task) {
        UT_LIST_REMOVE(list_node, worker->free_tasks, task);

        task->func = func;
        task->data = *data;
        UT_LIST_ADD_LAST(list_node, worker->running_tasks, task);
        size = UT_LIST_GET_LEN(worker->running_tasks);

        worker->ref_count++;
    }
    spin_unlock(&worker->lock);

    if (task && 1 == size) {
        os_event_set_signal(worker->task_event);
    }

    return task ? TRUE : FALSE;
}

void thread_worker_task_join(thread_worker_t *worker)
{
    bool32 is_running = TRUE;

    while (is_running) {
        spin_lock(&worker->lock, NULL);
        if (worker->ref_count == 0) {
            is_running = FALSE;
        }
        spin_unlock(&worker->lock);

        if (is_running) {
            os_event_wait_time(worker->join_event, 1000000);
            os_event_reset(worker->join_event);
        }
    }
}

static void* thread_worker_routine(void *arg)
{
    thread_worker_t *worker = (thread_worker_t *)arg;
    thread_task_t   *task;

    while (1)
    {
        spin_lock(&worker->lock, NULL);
        task = UT_LIST_GET_FIRST(worker->running_tasks);
        if (task != NULL) {
            UT_LIST_REMOVE(list_node, worker->running_tasks, task);
        }
        spin_unlock(&worker->lock);

        if (task == NULL) {
            os_event_set_signal(worker->join_event);

            os_event_wait_time(worker->task_event, 1000000);
            os_event_reset(worker->task_event);
            continue;
        }

        (*(task->func)) (&task->data);

        spin_lock(&worker->lock, NULL);
        UT_LIST_ADD_LAST(list_node, worker->free_tasks, task);
        if (worker->ref_count > 0) {
            worker->ref_count--;
        }
        spin_unlock(&worker->lock);
    }

    return NULL;
} 

#ifdef __cplusplus
}
#endif
