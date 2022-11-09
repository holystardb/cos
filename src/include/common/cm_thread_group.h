#ifndef _CM_THREAD_GROUP_H
#define _CM_THREAD_GROUP_H

#include "cm_type.h"
#include "cm_thread.h"
#include "cm_mutex.h"
#include "cm_list.h"
#include "cm_queue.h"
#include "cm_memory.h"


#ifdef __cplusplus
extern "C" {
#endif

typedef struct st_group_task group_task_t;
typedef struct st_thread_groups thread_groups_t;

struct st_group_task
{
    callback_func         func; 
    callback_data_t       data;
    UT_LIST_NODE_T(group_task_t) list_node;
};

typedef struct st_group_worker
{
    os_event_t          event;
    spinlock_t          lock;
    uint32              task_total_count;
    thread_groups_t    *groups;
    UT_LIST_BASE_NODE_T(group_task_t) high_running_tasks;
    UT_LIST_BASE_NODE_T(group_task_t) running_tasks;
    UT_LIST_BASE_NODE_T(group_task_t) free_tasks;
} group_worker_t; 

#define THREAD_GROUPS_COUNT             256

struct st_thread_groups
{
    memory_pool_t      *mem_pool;
    group_worker_t      groups[THREAD_GROUPS_COUNT];
    spinlock_t          lock;
    uint8               group_count;
    uint8               thread_count_per_group;
    UT_LIST_BASE_NODE_T(group_task_t) free_tasks;
}; 


thread_groups_t* thread_groups_create(memory_pool_t      * mem_pool, uint8 group_count, uint8 thread_count_per_group);

bool32 append_to_thread_group(thread_groups_t* groups, uint8 group_idx, callback_data_t *data, callback_func func, bool32 is_high_priority);


#ifdef __cplusplus
}
#endif

#endif  /* _CM_THREAD_GROUP_H */

