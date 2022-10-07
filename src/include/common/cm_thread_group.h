#ifndef _CM_THREAD_GROUP_H
#define _CM_THREAD_GROUP_H

#include "cm_type.h"
#include "cm_thread.h"
#include "cm_mutex.h"
#include "cm_list.h"
#include "cm_queue.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct st_group_task group_task_t;
struct st_group_task
{
    os_callback_func      func; 
    callback_data_t       data;
    UT_LIST_NODE_T(group_task_t) list_node;
};

typedef struct st_group_worker
{
    os_event_t          event;
    spinlock_t          lock;
    uint32              task_total_count;
    void               *thread_pool;
    UT_LIST_BASE_NODE_T(group_task_t) high_task_list;
    UT_LIST_BASE_NODE_T(group_task_t) task_list;
    UT_LIST_BASE_NODE_T(group_task_t) free_list;
} group_worker_t; 

typedef struct
{ 
    group_worker_t      workers[256];
    uint8               group_count;
    uint8               thread_count_per_group;
} thread_groups_t; 


thread_groups_t* thread_groups_create(uint8 group_count, uint8 thread_count_per_group);

bool32 append_to_thread_group(thread_groups_t* groups, uint8 group_idx,
                                       callback_data_t *data, os_callback_func func, bool32 is_high_priority);


#ifdef __cplusplus
}
#endif

#endif  /* _CM_THREAD_GROUP_H */

