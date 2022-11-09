#include "cm_queue.h"

queue_t* queue_init(uint32 count)
{
    uint32   index;
    queue_t* queue;
    
    if (0 == count || QUEUE_MAX_SIZE < count)
        return NULL;

    queue = (queue_t *)malloc(sizeof(queue_t) + sizeof(uint32) * count);
    
    queue->head = 0;
    queue->tail = count - 1;
    queue->free = count;
    
    queue->addr = (uint32 *)((char *)queue + sizeof(queue_t));
    for(index = 0; index < count; index++) {
        queue->addr[index] = index + 1;
    }
    queue->addr[count - 1] = 0;
    spin_lock_init(&queue->lock);
    queue->count = count;

    return queue;
}

uint32 queue_push(queue_t *queue)
{
    uint32  index;

    spin_lock(&queue->lock, NULL);
    
    if(queue->free == 0) {
        spin_unlock(&queue->lock);
        return INVALID_QUEUE_INDEX;
    }
    
    index = queue->head;
    queue->free--;
    if(queue->free != 0) {
        uint32 temp = queue->head;
        queue->head = queue->addr[temp];
        queue->addr[temp] = INVALID_QUEUE_INDEX;
        queue->addr[queue->tail] = queue->head;
    } else {
        queue->addr[queue->head] = INVALID_QUEUE_INDEX;
    }
    
    spin_unlock(&queue->lock);

    return index;    
}

void queue_pop(queue_t *queue, uint32 index)
{
    if (index >= queue->count)
        return;

    spin_lock(&queue->lock, NULL);

    if( queue->addr[index] != INVALID_QUEUE_INDEX) {
        spin_unlock(&queue->lock);
        return; 
    }
    
    if(queue->free != 0) {
        queue->addr[queue->tail] = index;
        queue->addr[index] = queue->head;
        queue->tail = index;
        queue->free++;
    } else {
        queue->head = queue->tail = index;
        queue->addr[index] = index;
        queue->free++;
    }

    spin_unlock(&queue->lock);
}

void queue_free(queue_t *queue)
{
    if (NULL != queue) {
        free(queue);
    }
}

