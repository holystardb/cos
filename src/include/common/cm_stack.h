#ifndef _CM_STACK_H
#define _CM_STACK_H

#include "cm_type.h"
#include "securec.h"
#include "cm_error.h"

#ifndef UNIV_MEMORY_DEBUG

// reserved size to save the last push_offset
// 8 bytes align
#define CM_PUSH_RESERVE_SIZE    8
#define CM_PUSH_OFFSET_POS      4
#define STACK_MAGIC_NUM         (uint32)0x12345678

typedef struct st_stack {
    uint8 *buf;
    uint32 size;
    uint32 push_offset; // top postion of the stack, begin from max_stack_size to 0
    uint32 heap_offset; // bottom postion of the stack, begin from 0 to max_stack_size
} cm_stack_t;

static inline void cm_stack_reset(cm_stack_t *stack)
{
    stack->push_offset = stack->size;
    stack->heap_offset = 0;
}

static inline void *cm_push(cm_stack_t *stack, uint32 size)
{
    uint32 last_offset;
    uint32 actual_size = CM_ALIGN8(size) + CM_PUSH_RESERVE_SIZE;
    uint8 *ptr = stack->buf + stack->push_offset - actual_size + CM_PUSH_RESERVE_SIZE;

    if (stack->push_offset < (uint64)stack->heap_offset + actual_size) {
        return NULL;
    }

    last_offset = stack->push_offset;
    stack->push_offset -= actual_size;
    *(uint32 *)(stack->buf + stack->push_offset + CM_PUSH_OFFSET_POS) = last_offset;

#if defined(UNIV_DEBUG)
    /* set magic number */
    *(uint32 *)(stack->buf + stack->push_offset) = STACK_MAGIC_NUM;
#endif

    return ptr;
}

static inline void cm_pop(cm_stack_t *stack)
{
    if (stack->push_offset == stack->size) {
        return;
    }

    stack->push_offset = *(uint32 *)(stack->buf + stack->push_offset + CM_PUSH_OFFSET_POS);

#if defined(UNIV_DEBUG)
    /* check magic number */
    if (stack->push_offset != stack->size) {
        CM_ASSERT(*(uint32 *)(stack->buf + stack->push_offset) == STACK_MAGIC_NUM);
    }
#endif
}

static inline void cm_pop_to(cm_stack_t *stack, uint32 push_offset)
{
    if (stack->push_offset >= push_offset) {
        return;
    }

    stack->push_offset = push_offset;

#if defined(UNIV_DEBUG)
    /* check magic number */
    if (stack->push_offset != stack->size) {
        CM_ASSERT(*(uint32 *)(stack->buf + stack->push_offset) == STACK_MAGIC_NUM);
    }
#endif
}

#define STACK_ALLOC_ADDR(stack) ((stack)->buf + (stack)->heap_offset)

static inline status_t cm_stack_alloc(void *owner, uint32 size, void **ptr)
{
    uint32 actual_size;
    cm_stack_t *stack;

    stack = (cm_stack_t *)owner;
    actual_size = CM_ALIGN8(size);
    if ((uint64)stack->heap_offset + actual_size + CM_MIN_KERNEL_RESERVE_SIZE >= stack->push_offset) {
        CM_THROW_ERROR(ERR_STACK_OVERFLOW);
        return CM_ERROR;
    }

    *ptr = STACK_ALLOC_ADDR(stack);
    stack->heap_offset += actual_size;
    return CM_SUCCESS;
}

static inline void *cm_stack_heap_head(cm_stack_t *stack)
{
    return STACK_ALLOC_ADDR(stack);
}

static inline void cm_stack_heap_reset(cm_stack_t *stack, void *to)
{
    stack->heap_offset = (uint32)((uint8 *)to - stack->buf);
}

static inline void cm_stack_init(cm_stack_t *stack, char *buf, uint32 buf_size)
{
    CM_ASSERT(stack != NULL);
    MEMS_RETVOID_IFERR(memset_s(stack, sizeof(cm_stack_t), 0, sizeof(cm_stack_t)));

    stack->buf = (uint8 *)buf;
    stack->size = buf_size;
    cm_stack_reset(stack);
}

#define CM_SAVE_STACK(stack)                       \
    uint32 __heap_offset__ = (stack)->heap_offset; \
    uint32 __push_offset__ = (stack)->push_offset;

#define CM_RESTORE_STACK(stack)                 \
    do {                                        \
        cm_pop_to(stack, __push_offset__);      \
        (stack)->heap_offset = __heap_offset__; \
    } while (0)

static inline void cm_keep_stack_variant(cm_stack_t *stack, char *buf)
{
    if (buf == NULL) {
        return;
    }
    if (buf < (char *)(stack->buf + stack->heap_offset) ||
        buf >= (char *)stack->buf + stack->push_offset + CM_PUSH_RESERVE_SIZE) {
        return;
    }

    stack->push_offset = (uint32)(buf - (char *)stack->buf - CM_PUSH_RESERVE_SIZE);

#if defined(UNIV_DEBUG)
    /* check magic number when keep variant, because the buff must be pushed */
    if (stack->push_offset != stack->size) {
        CM_ASSERT(*(uint32 *)(stack->buf + stack->push_offset) == STACK_MAGIC_NUM);
    }
#endif
}

#else // UNIV_MEMORY_DEBUG

#define CM_MAX_TEST_HEAP_DEPTH      (uint32)256
#define CM_MAX_TEST_STACK_DEPTH     (uint32)1024

typedef struct st_stack {
    uint8* buf;
    uint32 push_offset;
    uint32 heap_offset;
    void*  heap_addr[CM_MAX_TEST_HEAP_DEPTH];
    uint32 push_depth;
    void*  stack_addr[CM_MAX_TEST_STACK_DEPTH];
} cm_stack_t;

inline void cm_stack_reset(cm_stack_t *stack)
{
    uint32 i;
    for (i = 0; i < CM_MAX_TEST_STACK_DEPTH && stack->stack_addr[i] != NULL; ++i) {
        free(stack->stack_addr[i]);
        stack->stack_addr[i] = NULL;
    }
    stack->push_depth = 0;

    for (i = 0; i < stack->heap_offset; ++i) {
        free(stack->heap_addr[i]);
        stack->heap_addr[i] = NULL;
    }
    stack->heap_offset = 0;
}

static inline void *cm_push(cm_stack_t *stack, uint32 size)
{
    errno_t rc_memzero;

    if (stack->push_depth + 1 > CM_MAX_TEST_STACK_DEPTH) {
        return NULL;
    }
    if (size == 0) {
        return NULL;
    }
    void *ptr = malloc(size);
    if (ptr == NULL) {
        return NULL;
    }

    if (stack->stack_addr[stack->push_depth] != NULL) {
        free(stack->stack_addr[stack->push_depth]);
        stack->stack_addr[stack->push_depth] = NULL;
    }

    stack->stack_addr[stack->push_depth] = ptr;
    stack->push_depth++;

    rc_memzero = memset_s(ptr, size, 0, size);
    if (rc_memzero != EOK) {
        free(ptr);
        //CM_THROW_ERROR(ERR_SYSTEM_CALL, rc_memzero);
        return NULL;
    }
    return ptr;
}

static inline void cm_pop(cm_stack_t *stack)
{
    if (stack->push_depth == 0) {
        return;
    }

    stack->push_depth--;
}

static inline void cm_pop_to(cm_stack_t *stack, uint32 to, uint32 push_offset)
{
    if (stack->push_depth <= to) {
        return;
    }

    stack->push_depth = to;
}

#define STACK_ALLOC_ADDR(stack) ((stack)->heap_offset == 0 ? NULL : (stack)->heap_addr[(stack)->heap_offset - 1])

static inline status_t cm_stack_alloc(void *owner, uint32 size, void **ptr)
{
    cm_stack_t *stack;
    errno_t errcode;

    stack = (cm_stack_t *)owner;

    if (stack->heap_offset + 1 > CM_MAX_TEST_HEAP_DEPTH) {
        *ptr = NULL;
        return CM_ERROR;
    }
    if (size == 0) {
        *ptr = NULL;
        return CM_ERROR;
    }
    *ptr = malloc(size);
    if (*ptr == NULL) {
        return CM_ERROR;
    }

    stack->heap_addr[stack->heap_offset] = *ptr;
    stack->heap_offset++;

    errcode = memset_s(*ptr, size, 0, size);
    if (errcode != EOK) {
        free(*ptr);
        //CM_THROW_ERROR(ERR_SYSTEM_CALL, errcode);
        return CM_ERROR;
    }

    return CM_SUCCESS;
}

static inline void *cm_stack_heap_head(cm_stack_t *stack)
{
    return STACK_ALLOC_ADDR(stack);
}

static inline void cm_stack_heap_reset(cm_stack_t *stack, void *to)
{
    while (stack->heap_offset > 0 && stack->heap_addr[stack->heap_offset - 1] != to) {
        free(stack->heap_addr[stack->heap_offset - 1]);
        stack->heap_addr[stack->heap_offset - 1] = NULL;
        stack->heap_offset--;
    }
}

static inline void cm_stack_init(cm_stack_t *stack, char *buf, uint32 buf_size)
{
    MEMS_RETVOID_IFERR(memset_s(stack, sizeof(cm_stack_t), 0, sizeof(cm_stack_t)));
    stack->buf = (uint8 *)buf;
}

#define CM_SAVE_STACK(stack)                       \
    uint32 __stack_depth__ = (stack)->push_depth;  \
    uint32 __heap_offset__ = (stack)->heap_offset; \
    uint32 __push_offset__ = (stack)->push_offset;

#define CM_RESTORE_STACK(stack)                             \
    do {                                                    \
        cm_pop_to(stack, __stack_depth__, __push_offset__); \
        (stack)->heap_offset = __heap_offset__;             \
    } while (0)

static inline void cm_keep_stack_variant(cm_stack_t *stack, char *buf)
{
    if (buf == NULL) {
        return;
    }

    for (uint32 i = 0; stack->stack_addr[i] != NULL && i < CM_MAX_TEST_STACK_DEPTH; i++) {
        if (buf == stack->stack_addr[i]) {
            if (stack->push_depth < i + 1) {
                stack->push_depth = i + 1;
            }
        }
    }
}

#endif  // UNIV_MEMORY_DEBUG

#define CM_PUSH_UPDATE_INFO(session, update_info)                                                                     \
    do {                                                                                                              \
        (update_info).columns = (uint16 *)cm_push((session)->stack,                                                   \
            (session)->kernel->attr.max_column_count * sizeof(uint16));                                               \
        (update_info).offsets = (uint16 *)cm_push((session)->stack,                                                   \
            (session)->kernel->attr.max_column_count * sizeof(uint16));                                               \
        (update_info).lens = (uint16 *)cm_push((session)->stack,                                                      \
            (session)->kernel->attr.max_column_count * sizeof(uint16));                                               \
        CM_ASSERT((update_info).columns != NULL);                                                                     \
        CM_ASSERT((update_info).offsets != NULL);                                                                     \
        CM_ASSERT((update_info).lens != NULL);                                                                        \
    } while (0)

#endif

