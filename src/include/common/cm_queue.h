#ifndef _CM_QUEUE_H
#define _CM_QUEUE_H

#include "cm_type.h"
#include "cm_mutex.h"

#ifdef __cplusplus
extern "C" {
#endif

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

#ifdef __cplusplus
}
#endif

#endif  /* _CM_QUEUE_H */

