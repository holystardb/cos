#include "cm_thread_group.h"

#ifdef __cplusplus
extern "C" {
#endif

void* thread_group_routine(void *arg); 

thread_groups_t* thread_groups_create(uint8 group_count, uint8 thread_count_per_group)
{
    uint32                i, j;
    os_thread_id_t        thread_id;
    thread_groups_t*      groups;
    
    groups = (thread_groups_t *)malloc(sizeof(thread_groups_t));
    groups->group_count = group_count;
    groups->thread_count_per_group = thread_count_per_group;
    for (i = 0; i < groups->group_count; i++)
    {
        group_worker_t *worker = &groups->workers[i];
        worker->lock = 0;
        worker->task_total_count = 0;
        worker->event = os_event_create(NULL);
        UT_LIST_INIT(worker->task_list);
        UT_LIST_INIT(worker->high_task_list);
        UT_LIST_INIT(worker->free_list);
        for (j = 0; j < groups->thread_count_per_group; j++) {
            os_thread_create(&thread_group_routine, worker, &thread_id);
        }
    }

    return groups;
}

bool32 append_to_thread_group(thread_groups_t* groups, uint8 group_idx,
                                       callback_data_t *data, os_callback_func func, bool32 is_high_priority)
{
    uint32           size;
    group_task_t    *task;
    group_worker_t  *worker;

    if (group_idx >= groups->group_count) {
        return FALSE;
    }

    worker = &groups->workers[group_idx];

    spin_lock(&worker->lock, NULL);
    task = UT_LIST_GET_FIRST(worker->free_list);
    if (task != NULL) {
        UT_LIST_REMOVE(list_node, worker->free_list, task);
        
        task->data = *data;
        task->func = func;
        
        if (is_high_priority) {
            UT_LIST_ADD_LAST(list_node, worker->high_task_list, task);
        } else {
            UT_LIST_ADD_LAST(list_node, worker->task_list, task);
        }
        size = UT_LIST_GET_LEN(worker->high_task_list) + UT_LIST_GET_LEN(worker->task_list);
    }
    spin_unlock(&worker->lock);

    if (task == NULL) {
        task = (group_task_t *)malloc(sizeof(group_task_t));
        task->data = *data;
        task->func = func;
        
        spin_lock(&worker->lock, NULL);
        worker->task_total_count++;
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

void* thread_group_routine(void *arg)
{
    group_worker_t *worker = (group_worker_t *)arg;
    group_task_t   *task;
    
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
