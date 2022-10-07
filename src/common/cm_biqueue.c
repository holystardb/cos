#include "cm_biqueue.h"
#include "cm_log.h"


#define M_SLOT_SIZE                     256

static void biqueue_lock_create(biqueue_t *queue)
{
    queue->lock = 0;
}

static void biqueue_lock_destroy(biqueue_t *queue)
{
    queue->lock = 0;
}

static void biqueue_lock(biqueue_t *queue)
{
    spin_lock(&queue->lock, NULL);
}

static void biqueue_unlock(biqueue_t *queue)
{
    spin_unlock(&queue->lock);
}

biqueue_t* biqueue_init(uint32 node_size, uint32 count)
{
    uint32      i;
    if (sizeof(biqueue_node_t) >= node_size) {
        LOG_PRINT(LOG_NOTICE, "biqueue_init: invalid node_size(%d)", node_size);
        return NULL;
    }

    biqueue_t *queue = (biqueue_t *)malloc(sizeof(biqueue_t));
    biqueue_lock_create(queue);
    queue->node_size = node_size;
    queue->slot_count = (count >> 8) + 1;
    queue->slot = (char **)malloc(sizeof(char *) * queue->slot_count);
    queue->slot[0] = (char *)malloc(queue->node_size * M_SLOT_SIZE);
    memset(queue->slot[0], 0, queue->node_size * M_SLOT_SIZE);
    queue->slot_index = 1;
    
    UT_LIST_INIT(queue->free_list);
    for(i = 0; i < M_SLOT_SIZE; i++) {
        biqueue_node_t* node = (biqueue_node_t *)(queue->slot[0] + i * queue->node_size);
        node->node_id = i;
        node->magic = 0;
        UT_LIST_ADD_LAST(list_node, queue->free_list, node);
    }

    return queue;
}

biqueue_node_t* biqueue_alloc(biqueue_t *queue)
{
    uint32          i;
    biqueue_node_t* node;

    biqueue_lock(queue);

retry_get_node:

    node = UT_LIST_GET_FIRST(queue->free_list);
    if(node != NULL) {
        if (node->magic != 0) {
            LOG_PRINT(LOG_WARN, "biqueue_alloc: queue %p node %p node_id %d invalid magic", queue, node, node->node_id);
        }
        node->magic = BIQUEUE_MAGIC;
        UT_LIST_REMOVE(list_node, queue->free_list, node);
        biqueue_unlock(queue);
        return node;
    }

    if (queue->slot_index >= queue->slot_count) {
        biqueue_unlock(queue);
        return NULL;
    }

    // extend slot
    queue->slot[queue->slot_index] = (char *)malloc(queue->node_size * M_SLOT_SIZE);
    memset(queue->slot[queue->slot_index], 0, queue->node_size * M_SLOT_SIZE);
    for(i = 0; i < M_SLOT_SIZE; i++) {
        biqueue_node_t* node = (biqueue_node_t *)(queue->slot[queue->slot_index] + i * queue->node_size);
        node->node_id = queue->slot_index * M_SLOT_SIZE + i;
        UT_LIST_ADD_LAST(list_node, queue->free_list, node);
    }
    queue->slot_index++;

    goto retry_get_node;

    biqueue_unlock(queue);

    return NULL;
}

biqueue_node_t* biqueue_get_node(biqueue_t *queue, uint32 node_id)
{
    biqueue_node_t* node;
    uint32 index, slot_index;

    index = (node_id & 0xFF);
    slot_index = (node_id >> 8);
    if (slot_index >= queue->slot_index) {
        LOG_PRINT(LOG_NOTICE, "biqueue_get_node: invalid node_id(%d), slot_index %d", node_id, queue->slot_index);
        return NULL;
    }
    node = (biqueue_node_t *)(queue->slot[slot_index] + index * queue->node_size);
    if (node->magic != BIQUEUE_MAGIC) {
        LOG_PRINT(LOG_WARN, "biqueue_get_node: queue %p node %p node_id %d invalid magic", queue, node, node->node_id);
    }
    return node;
}

bool32 biqueue_free(biqueue_t *queue, biqueue_node_t* node)
{
    biqueue_lock(queue);
    if (node->magic != BIQUEUE_MAGIC) {
        LOG_PRINT(LOG_WARN, "biqueue_free: queue %p node %p node_id %d invalid magic", queue, node, node->node_id);
    }
    node->magic = 0;
    UT_LIST_ADD_FIRST(list_node, queue->free_list, node);
    biqueue_unlock(queue);
    
    return TRUE;;
}

bool32 biqueue_free_to_tail(biqueue_t *queue, biqueue_node_t* node)
{
    biqueue_lock(queue);
    if (node->magic != BIQUEUE_MAGIC) {
        LOG_PRINT(LOG_WARN, "biqueue_free_to_tail: node %p node_id %d invalid magic", node, node->node_id);
    }
    node->magic = 0;
    UT_LIST_ADD_LAST(list_node, queue->free_list, node);
    biqueue_unlock(queue);
    
    return TRUE;;
}

void biqueue_destroy(biqueue_t *queue)
{
    uint32      i;
    
    biqueue_lock_destroy(queue);
    for(i = 0; i < queue->slot_index; i++) {
        free(queue->slot[i]);
    }
    free(queue->slot);
    free(queue);
}

