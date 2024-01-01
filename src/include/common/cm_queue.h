#ifndef _CM_QUEUE_H
#define _CM_QUEUE_H

#include "cm_type.h"
#include "cm_mutex.h"

#ifdef __cplusplus
extern "C" {
#endif


/*******************************************************************
 *                            number queue                         *
 ******************************************************************/

#define QUEUE_MAX_SIZE                  0xFFFFFF
#define INVALID_QUEUE_INDEX             0xFFFFFFFF


typedef struct
{
    uint32            head;
    uint32            tail;
    uint32            free;
    uint32           *addr;
    spinlock_t        lock;
    uint32            count;
} queue_t;

queue_t* queue_init(uint32 count);
uint32 queue_push(queue_t *queue);
void queue_pop(queue_t *queue, uint32 index);
void queue_free(queue_t *queue);




/*******************************************************************
 *                            dynamic queue                        *
 ******************************************************************/

typedef void* (*dyn_queue_next_node_func) (void *current);
typedef void** (*dyn_queue_next_node_address_func) (void *current);

class dyn_queue 
{
public:
    dyn_queue(dyn_queue_next_node_func next_node_func,
                  dyn_queue_next_node_address_func next_node_address_func)
        : m_first(NULL), m_last(&m_first) {
        m_next_node_func = next_node_func;
        m_next_node_address_func = next_node_address_func;
    }

    void init() {
      mutex_create(&m_lock);
    }

    void deinit() {
        mutex_destroy(&m_lock);
    }

    bool is_empty() const {
        return m_first == NULL;
    }

    /** Append a linked list of threads to the queue */
    bool32 append(void *first);

    // Fetch the entire queue for a stage.
    // This will fetch the entire queue in one go.
    void *fetch_and_empty();

    void* pop_front();

private:

    //Pointer to the first thread in the queue, or NULL if the queue is empty.
    void *m_first;

    /**
       Pointer to the location holding the end of the queue.

       This is either @c &first, or a pointer to the @c next_to_node of
       the last thread that is enqueued.
    */
    void **m_last;

    /** Lock for protecting the queue. */
    mutex_t m_lock;

    dyn_queue_next_node_func m_next_node_func;
    dyn_queue_next_node_address_func m_next_node_address_func;
};


#ifdef __cplusplus
}
#endif

#endif  /* _CM_QUEUE_H */

