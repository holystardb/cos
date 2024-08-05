#ifndef _CM_BI_QUEUE_H
#define _CM_BI_QUEUE_H

#include  <stddef.h>
#include "cm_type.h"
#include "cm_mutex.h"
#include "cm_list.h"
#include "cm_memory.h"


#define BIQUEUE_MAGIC       123461526

#define OBJECT_OF_QUEUE_NODE(type, node, member) ((type *)((char *)(&node->node_id) - VAR_OFFSET(type, member)))
#define QUEUE_NODE_OF_OBJECT(node, member)       ((biqueue_node_t *)&node->member)

#ifdef __cplusplus
extern "C" {
#endif

typedef struct st_biqueue_node
{
    uint32      node_id;
    uint32      magic;
    UT_LIST_NODE_T(struct st_biqueue_node) list_node;
} biqueue_node_t;

typedef struct
{
    uint32            node_size;
    uint32            slot_count;
    char            **slot;
    uint32            slot_index;
    spinlock_t        lock;
    memory_pool_t    *mpool;
    uint32            slot_size;
    UT_LIST_BASE_NODE_T(biqueue_node_t) free_list;
    UT_LIST_BASE_NODE_T(memory_page_t) used_pages;
} biqueue_t;

biqueue_t* biqueue_init(uint32 node_size, uint32 count, memory_pool_t *mpool);
biqueue_node_t* biqueue_alloc(biqueue_t *queue);
biqueue_node_t* biqueue_get_node(biqueue_t *queue, uint32 node_id);
bool32 biqueue_free(biqueue_t *queue, biqueue_node_t* node);
bool32 biqueue_free_to_tail(biqueue_t *queue, biqueue_node_t* node);
void biqueue_destroy(biqueue_t *queue);

#ifdef __cplusplus
}
#endif

#endif  /* _CM_QUEUE_H */
