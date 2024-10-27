#include "cm_queue.h"
#include "cm_memory.h"

/*******************************************************************
 *                            number queue                         *
 ******************************************************************/

queue_t* queue_init(uint32 count)
{
    uint32   index;
    queue_t* queue;
    
    if (0 == count || QUEUE_MAX_SIZE < count)
        return NULL;

    queue = (queue_t *)ut_malloc_zero(sizeof(queue_t) + sizeof(uint32) * count);

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

uint32 queue_push(queue_t* queue)
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

void queue_pop(queue_t* queue, uint32 index)
{
    if (index >= queue->count) {
        ut_ad(index < queue->count);
        return;
    }

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

void queue_free(queue_t* queue)
{
    if (NULL != queue) {
        ut_free(queue);
    }
}


/*******************************************************************
 *                          dynamic queue                          *
 ******************************************************************/

void dyn_queue::append(void* first)
{
    if (m_first == NULL) {
        return;
    }

    mutex_enter(&m_lock, NULL);

    *m_last = first;

    /*
      Go to the last instance of the list. We expect lists to be
      moderately short. If they are not, we need to track the end of
      the queue as well.
    */
    while (m_next_node_func(first)) {
        first = m_next_node_func(first);
    }
    m_last = m_next_node_address_func(first);

    mutex_exit(&m_lock);
}

void* dyn_queue::fetch_and_empty()
{
    mutex_enter(&m_lock, NULL);

    void* result = m_first;
    m_first = NULL;
    m_last = &m_first;

    ut_a(m_first || m_last == &m_first);

    mutex_exit(&m_lock);

    return result;
}

void* dyn_queue::pop_front()
{
    mutex_enter(&m_lock, NULL);

    void* result = m_first;
    if (result) {
        m_first = m_next_node_func(result);
    }

    if (m_first == NULL) {
        m_last = &m_first;
    }

    ut_a(m_first || m_last == &m_first);

    mutex_exit(&m_lock);

    return result;
}

