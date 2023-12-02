#ifndef _CM_MEMORY_H
#define _CM_MEMORY_H

#include "cm_type.h"
#include "cm_list.h"
#include "cm_mutex.h"
#include "cm_util.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct st_memory_page {
    UT_LIST_NODE_T(struct st_memory_page) list_node;
} memory_page_t;

#define MEM_AREA_PAGE_MAX_SIZE             (32*1024*1024)
#define MEM_AREA_PAGE_ARRAY_SIZE           16

typedef struct st_memory_area {
    char                *buf;
    uint64               offset;
    uint64               size;
    bool32               is_extend;
    spinlock_t           lock;
    //  1k  2k  4k  8k  16k  32k  64k  128k  256k  512k  1m  2m  4m  8m  16m  32m
    //  0   1   2   3   4    5    6    7     8     9     10  11  12  13  14   15
    UT_LIST_BASE_NODE_T(memory_page_t) free_pages[MEM_AREA_PAGE_ARRAY_SIZE];
} memory_area_t;

typedef struct st_memory_context memory_context_t;

typedef struct st_memory_pool {
    memory_area_t   *area;
    spinlock_t       lock;
    uint32           page_size;
    uint32           local_page_count;
    uint32           max_page_count;
    uint32           page_alloc_count;
    UT_LIST_BASE_NODE_T(memory_page_t) free_pages;

    // for context
    UT_LIST_BASE_NODE_T(memory_context_t) free_contexts;
    UT_LIST_BASE_NODE_T(memory_page_t) context_used_pages;
} memory_pool_t;

#define MEM_POOL_PAGE_UNLIMITED        0xFFFFFFFF

#define MEM_BLOCK_FREE_LIST_SIZE       8
#define MEM_BLOCK_MIN_SIZE             64
#define MEM_BLOCK_MAX_SIZE             8192

typedef struct st_mem_block {
    uint32              size : 31;
    uint32              is_free : 1;
    uint32              reserved;
    memory_page_t      *page;
    UT_LIST_NODE_T(struct st_mem_block) list_node;
} mem_block_t;

typedef struct st_mem_buf {
    char               *buf;
    uint32              offset;
} mem_buf_t;

struct st_memory_context {
    UT_LIST_NODE_T(struct st_memory_context) list_node;
    memory_pool_t   *pool;
    spinlock_t       lock;
    UT_LIST_BASE_NODE_T(memory_page_t) used_buf_pages;

    // for alloc and free
    //  64  128  256  512  1k  2k  4k  8k
    //  0   1    2    3    4   5   6   7
    UT_LIST_BASE_NODE_T(mem_block_t) free_mem_blocks[MEM_BLOCK_FREE_LIST_SIZE];
    UT_LIST_BASE_NODE_T(memory_page_t) used_block_pages;
};

extern memory_area_t* marea_create(uint64 mem_size, bool32 is_extend);
extern void marea_destroy(memory_area_t* area);
extern memory_page_t* marea_alloc_page(memory_area_t *area, uint32 page_size);
extern void marea_free_page(memory_area_t *area, memory_page_t *page, uint32 page_size);

extern memory_pool_t* mpool_create(memory_area_t *area, uint32 local_page_count, uint32 max_page_count, uint32 page_size);
extern void mpool_destroy(memory_pool_t *pool);
extern memory_page_t* mpool_alloc_page(memory_pool_t *pool);
extern void mpool_free_page(memory_pool_t *pool, memory_page_t *page);

extern memory_context_t* mcontext_create(memory_pool_t *pool);
extern void mcontext_destroy(memory_context_t *context);
extern bool32 mcontext_clean(memory_context_t *context);

extern void* mcontext_stack_push(memory_context_t *context, uint32 size);
extern void mcontext_stack_pop(memory_context_t *context, void *ptr, uint32 size);
extern void mcontext_stack_pop2(memory_context_t *context, void *ptr);

extern void* mcontext_alloc(memory_context_t *context, uint32 size);
extern void* mcontext_realloc(memory_context_t *context, void *old_ptr, uint32 size);
extern void mcontext_free(memory_context_t *context, void *ptr);

extern void *os_mem_alloc_large(uint64 *n);
extern void os_mem_free_large(void *ptr, uint64 size);

/* Advices the OS that this chunk should (not) be dumped to a core file. */
extern bool32 madvise_dump(char *mem_ptr, uint64 mem_size);
extern bool32 madvise_dont_dump(char *mem_ptr, uint64 mem_size);



//-------------------------------------------------------------------

#define MEM_PAGE_DATA_PTR(page)  ((char *)page + MemoryPageHeaderSize)

extern uint32 MemoryContextHeaderSize;
extern uint32 MemoryPageHeaderSize;
extern uint32 MemoryBlockHeaderSize;
extern uint32 MemoryBufHeaderSize;

#define ut_malloc(size)         malloc(size)
#define ut_free(size)           free(size)

#ifdef MEMORY_DEBUG
#define my_malloc(size)         malloc(size)
#define my_free(size)           free(size)
#define my_realloc(ptr, size)   realloc(ptr, size)
#else
#define my_malloc(size)         (current_memory_context ? mcontext_alloc(current_memory_context, size) : malloc(size))
#define my_free(ptr)            (current_memory_context ? mcontext_free(current_memory_context, ptr) : free(ptr))
#define my_realloc(ptr, size)   (current_memory_context ? mcontext_realloc(current_memory_context, ptr, size) : realloc(ptr, size))
#endif
#define my_alloca(size)         alloca((size_t)(size))

/*-------------------------------------------------------------------*/

extern THREAD_LOCAL memory_context_t  *current_memory_context;

inline memory_context_t* memory_context_switch_to(memory_context_t *context)
{
    memory_context_t *old = current_memory_context;
    current_memory_context = context;
    return old;
}

#ifdef __cplusplus
}
#endif

#endif  /* _CM_MEMORY_H */

