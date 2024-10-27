#include "cm_thread_group.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FREE_TASK_COUNT_PRE_GROUP       8

void* thread_group_routine(void *arg); 

thread_groups_t* thread_groups_create(memory_pool_t      * mem_pool, uint8 group_count, uint8 thread_count_per_group)
{
    os_thread_id_t      thread_id;
    thread_groups_t*    groups;
    uint32              task_count = FREE_TASK_COUNT_PRE_GROUP * group_count;

    groups = (thread_groups_t *)ut_malloc_zero(ut_align8(sizeof(thread_groups_t)) + ut_align8(sizeof(group_task_t)) * task_count);
    spin_lock_init(&groups->lock);
    groups->group_count = group_count;
    groups->thread_count_per_group = thread_count_per_group;
    groups->mem_pool = mem_pool;
    UT_LIST_INIT(groups->free_tasks);

    for (uint32 i = 0; i < groups->group_count; i++)
    {
        group_worker_t *group = &groups->groups[i];
        spin_lock_init(&group->lock);
        group->task_total_count = 0;
        group->groups = groups;
        group->event = os_event_create(NULL);
        UT_LIST_INIT(group->running_tasks);
        UT_LIST_INIT(group->high_running_tasks);
        UT_LIST_INIT(group->free_tasks);
        for (uint32 j = 0; j < groups->thread_count_per_group; j++) {
            os_thread_create(&thread_group_routine, group, &thread_id);
        }
    }

    char *tasks_ptr = (char *)groups + ut_align8(sizeof(thread_groups_t));
    for (uint32 i = 0; i < task_count; i++)
    {
        group_task_t *task = (group_task_t *)(tasks_ptr + ut_align8(sizeof(group_task_t)) * i);
        UT_LIST_ADD_LAST(list_node, groups->free_tasks, task);
    }

    return groups;
}

static bool32 groups_extend_free_tasks(thread_groups_t* groups)
{
    memory_page_t *page;

    page = mpool_alloc_page(groups->mem_pool);
    if (page == NULL) {
        return FALSE;
    }

    uint32 task_count = groups->mem_pool->page_size / ut_align8(sizeof(group_task_t));
    char *tasks_ptr = (char *)MEM_PAGE_DATA_PTR(page);
    for (uint32 i = 0; i < task_count; i++) {
        group_task_t *task = (group_task_t *)(tasks_ptr + ut_align8(sizeof(group_task_t)) * i);
        UT_LIST_ADD_LAST(list_node, groups->free_tasks, task);
    }

    return TRUE;
}

static bool32 get_free_tasks_from_groups(group_worker_t *group)
{
    uint32 task_count = FREE_TASK_COUNT_PRE_GROUP / 2;
    group_task_t *task;

    spin_lock(&group->groups->lock, NULL);
    if (UT_LIST_GET_LEN(group->groups->free_tasks) < task_count &&
        !groups_extend_free_tasks(group->groups)) {
        spin_unlock(&group->groups->lock);
        return FALSE;
    }

    task = UT_LIST_GET_FIRST(group->groups->free_tasks);
    while (task && task_count > 0) {
        UT_LIST_REMOVE(list_node, group->groups->free_tasks, task);
        UT_LIST_ADD_LAST(list_node, group->free_tasks, task);
        task = UT_LIST_GET_FIRST(group->groups->free_tasks);
        if (task_count > 0) {
            task_count--;
        }
    }
    spin_unlock(&group->groups->lock);

    return TRUE;
}

static group_task_t* group_alloc_free_task(group_worker_t *group)
{
    group_task_t   *task;

    spin_lock(&group->lock, NULL);
    for (;;) {
        task = UT_LIST_GET_FIRST(group->free_tasks);
        if (task != NULL) {
            UT_LIST_REMOVE(list_node, group->free_tasks, task);
            break;
        }

        if (!get_free_tasks_from_groups(group)) {
            break;
        }
    }
    spin_unlock(&group->lock);

    return task;
}

static void group_free_free_task(group_worker_t *group, group_task_t *task)
{
    spin_lock(&group->lock, NULL);
    if (UT_LIST_GET_LEN(group->free_tasks) > FREE_TASK_COUNT_PRE_GROUP) {
        spin_lock(&group->groups->lock, NULL);
        UT_LIST_ADD_LAST(list_node, group->groups->free_tasks, task);
        spin_unlock(&group->groups->lock);
    } else {
        UT_LIST_ADD_LAST(list_node, group->free_tasks, task);
    }
    spin_unlock(&group->lock);
}

bool32 append_to_thread_group(thread_groups_t* groups, uint8 group_idx, callback_data_t *data, callback_func func, bool32 is_high_priority)
{
    uint32           size;
    group_task_t    *task;
    group_worker_t  *group;

    if (group_idx >= groups->group_count) {
        return FALSE;
    }
    group = &groups->groups[group_idx];

    task = group_alloc_free_task(group);
    if (task) {
        task->data = *data;
        task->func = func;

        spin_lock(&group->lock, NULL);
        if (is_high_priority) {
            UT_LIST_ADD_LAST(list_node, group->high_running_tasks, task);
        } else {
            UT_LIST_ADD_LAST(list_node, group->running_tasks, task);
        }
        size = UT_LIST_GET_LEN(group->high_running_tasks) + UT_LIST_GET_LEN(group->running_tasks);
        spin_unlock(&group->lock);

        if (1 == size) {
            os_event_set_signal(group->event);
        }
    }

    return task ? TRUE : FALSE;
}

void* thread_group_routine(void *arg)
{
    group_worker_t *group = (group_worker_t *)arg;
    group_task_t   *task;

    while (1)
    {
        spin_lock(&group->lock, NULL);
        task = UT_LIST_GET_FIRST(group->high_running_tasks);
        if (task != NULL) {
            UT_LIST_REMOVE(list_node, group->high_running_tasks, task);
        } else {
            task = UT_LIST_GET_FIRST(group->running_tasks);
            if (task != NULL) {
                UT_LIST_REMOVE(list_node, group->running_tasks, task);
            }
        }
        spin_unlock(&group->lock);

        if (task == NULL) {
            os_event_wait_time(group->event, 100000);
            os_event_reset(group->event);
            continue;
        }

        (*(task->func)) (&task->data);

        group_free_free_task(group, task);
    }

    return NULL;
} 

#ifdef __cplusplus
}
#endif
