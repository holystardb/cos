#ifndef _CM_MEMORY_H
#define _CM_MEMORY_H

#include "cm_type.h"
#include "cm_list.h"
#include "cm_mutex.h"
#include "cm_util.h"
#include "securec.h"


#ifdef __cplusplus
extern "C" {
#endif



/******************************************************************************
 *                             memory context                                 *
 *****************************************************************************/

typedef struct st_memory_page {
    UT_LIST_NODE_T(struct st_memory_page) list_node;
    void*  context;
    uint32 owner_list_id;
    uint32 id;
} memory_page_t;

#define MEM_AREA_PAGE_MAX_SIZE             SIZE_M(32)
#define MEM_AREA_PAGE_ARRAY_SIZE           16

typedef struct st_memory_pool memory_pool_t;

typedef struct st_memory_area {
    char                *buf;
    uint64               offset;
    uint64               size;
    mutex_t              mutex;
    bool32               is_extend;

    mutex_t              free_pages_mutex[MEM_AREA_PAGE_ARRAY_SIZE];
    //  1k  2k  4k  8k  16k  32k  64k  128k  256k  512k  1m  2m  4m  8m  16m  32m
    //  0   1   2   3   4    5    6    7     8     9     10  11  12  13  14   15
    UT_LIST_BASE_NODE_T(memory_page_t) free_pages[MEM_AREA_PAGE_ARRAY_SIZE];

    mutex_t              free_pools_mutex;
    UT_LIST_BASE_NODE_T(memory_pool_t) free_pools;
} memory_area_t;

typedef struct st_memory_context memory_context_t;
typedef struct st_memory_stack_context memory_stack_context_t;


#define MPOOL_FREE_PAGE_LIST_COUNT      64
#define MPOOL_FREE_CONTEXT_LIST_COUNT   64

typedef struct st_pool_free_page_list {
    atomic32_t       count;
    mutex_t          mutex;
    UT_LIST_BASE_NODE_T(memory_page_t) free_pages;
} pool_free_page_list_t;

typedef struct st_pool_free_context_list {
    mutex_t          mutex;
    volatile bool32  alloc_in_progress;
    UT_LIST_BASE_NODE_T(memory_context_t) free_contexts;
} pool_free_context_list_t;

typedef struct st_pool_free_stack_context_list {
    mutex_t          mutex;
    volatile bool32  alloc_in_progress;
    UT_LIST_BASE_NODE_T(memory_stack_context_t) free_contexts;
} pool_free_stack_context_list_t;

struct st_memory_pool {
    UT_LIST_NODE_T(struct st_memory_pool) list_node;
    memory_area_t   *area;

    uint32           page_size;
    uint32           lower_page_count;
    atomic32_t       page_alloc_count;  // current page count
    atomic64_t       alloc_memory_size;
    uint64           total_memory_size;

    // free page
    //mutex_stats_t         page_list_mutex_stats;
    pool_free_page_list_t page_list[MPOOL_FREE_PAGE_LIST_COUNT];
    // for free context
    //mutex_stats_t            context_list_mutex_stats;
    pool_free_context_list_t context_list[MPOOL_FREE_CONTEXT_LIST_COUNT];
    pool_free_stack_context_list_t stack_context_list[MPOOL_FREE_CONTEXT_LIST_COUNT];

    mutex_t                            context_used_pages_mutex;
    UT_LIST_BASE_NODE_T(memory_page_t) context_used_pages;

    bool32 increase_check_memory_size(uint32 alloc_size);
    void decrease_memory_size(uint32 alloc_size);
};

#define MEM_POOL_PAGE_UNLIMITED        0xFFFFFFFF

#define MEM_BLOCK_FREE_LIST_SIZE       12
#define MEM_BLOCK_MIN_SIZE             64
#define MEM_BLOCK_MAX_SIZE             SIZE_K(128)

#define MEM_BLOCK_MAGIC                569278163

// total 32 Bytes
typedef struct st_mem_block {
    uint32              size; // block size = MemoryBlockHeaderSize + alloc_size + reserved_size
    uint32              alloc_size;
    uint32              is_free : 1;
    uint32              is_ext : 1;  // alloced from os iff is_exit == 1
    uint32              align : 30;
    uint32              magic;
    union {
        memory_context_t*  mctx;  // mctx is null iff is_exit == 0
        memory_page_t*     page;  // page is null iff is_exit == 1
    };
    UT_LIST_NODE_T(struct st_mem_block) list_node;
} mem_block_t;

typedef struct st_mem_buf {
    char*  buf;
    uint32 offset;
    uint32 reserved;
} mem_buf_t;

struct st_memory_context {
    UT_LIST_NODE_T(memory_context_t) list_node;
    memory_pool_t   *pool;
    mutex_t          mutex;
    uint16           is_free;
    uint16           owner_list_id;
    UT_LIST_BASE_NODE_T(memory_page_t) used_block_pages;
    // for alloc and free
    //  64  128  256  512  1k  2k  4k  8k  16k  32k  64k  128k  256k  512k  1m  2m
    //  0   1    2    3    4   5   6   7    8    9   10    11    12    13   14  15
    UT_LIST_BASE_NODE_T(mem_block_t) free_mem_blocks[MEM_BLOCK_FREE_LIST_SIZE];
    UT_LIST_BASE_NODE_T(mem_block_t) alloced_ext_blocks;
};

struct st_memory_stack_context {
    UT_LIST_NODE_T(memory_stack_context_t) list_node;
    memory_pool_t   *pool;
    mutex_t          mutex;
    uint16           is_free;
    uint16           owner_list_id;
    UT_LIST_BASE_NODE_T(memory_page_t) used_buf_pages;
};


//------------------------------------------------------------------------------

extern memory_area_t* marea_create(uint64 mem_size, bool32 is_extend);
extern void marea_destroy(memory_area_t* area);
extern memory_page_t* marea_alloc_page(memory_area_t *area, uint32 page_size);
extern void marea_free_page(memory_area_t *area, memory_page_t *page, uint32 page_size);

extern inline memory_pool_t* mpool_create(memory_area_t *area, char* name, uint64 memory_size,
    uint32 page_size, uint32 initial_page_count, uint32 lower_page_count);
extern inline void mpool_destroy(memory_pool_t *pool);
extern inline memory_page_t* mpool_alloc_page(memory_pool_t *pool);
extern inline void mpool_free_page(memory_pool_t *pool, memory_page_t *page);


//------------------------------------------------------------------------------

extern inline memory_context_t* mcontext_create(memory_pool_t* pool);
extern inline void mcontext_destroy(memory_context_t* context);
extern inline void mcontext_clean(memory_context_t* context);
extern inline void* mcontext_alloc(memory_context_t* context, uint32 alloc_size, const char* file, int32 line, bool32 is_alloc_zero = FALSE);
extern inline void* mcontext_realloc(void* ptr, uint32 alloc_size, const char* file, int32 line, bool32 is_alloc_zero = FALSE);
extern inline void mcontext_free(void* ptr, memory_context_t* context = NULL);
extern inline uint64 mcontext_get_size(memory_context_t* context);


//------------------------------------------------------------------------------

extern inline memory_stack_context_t* mcontext_stack_create(memory_pool_t* pool);
extern inline void mcontext_stack_destroy(memory_stack_context_t* context);
extern inline void mcontext_stack_clean(memory_stack_context_t* context);
extern inline uint64 mcontext_stack_get_size(memory_stack_context_t* context);

extern inline void* mcontext_stack_push(memory_stack_context_t* context, uint32 size, bool32 is_alloc_zero = FALSE);
extern inline status_t mcontext_stack_pop(memory_stack_context_t* context, void* ptr, uint32 size);
extern inline void* mcontext_stack_save(memory_stack_context_t *context);
extern inline status_t mcontext_stack_restore(memory_stack_context_t* context, void* ptr);


//------------------------------------------------------------------------------

extern void *os_mem_alloc_large(uint64* n);
extern void os_mem_free_large(void* ptr, uint64 size);

/* Advices the OS that this chunk should (not) be dumped to a core file. */
extern bool32 madvise_dump(char* mem_ptr, uint64 mem_size);
extern bool32 madvise_dont_dump(char* mem_ptr, uint64 mem_size);



//-------------------------------------------------------------------

#define MEM_PAGE_DATA_PTR(page)  ((char *)page + MemoryPageHeaderSize)

extern uint32 MemoryContextHeaderSize;
extern uint32 MemoryPageHeaderSize;
extern uint32 MemoryBlockHeaderSize;
extern uint32 MemoryBufHeaderSize;

#define ut_malloc(size)         malloc(size)
#define ut_free(size)           free(size)

extern inline void* ut_malloc_zero(size_t size)
{
    void* tmp = malloc(size);
    memset_s(tmp, size, '\0', size);
    return tmp;
}


#ifdef UNIV_MEMROY_VALGRIND
#define my_malloc(mctx, size)          malloc(size)
#define my_free(size)                  free(size)
#define my_realloc(ptr, size)          realloc(ptr, size)
#else
#define my_malloc(mctx, size)          mcontext_alloc(mctx, size, __FILE__, __LINE__, FALSE)
#define my_malloc_zero(mctx, size)     mcontext_alloc(mctx, size, __FILE__, __LINE__, TRUE)
#define my_free(ptr)                   mcontext_free(ptr)
#define my_realloc(ptr, size)          mcontext_realloc(ptr, size, __FILE__, __LINE__, FALSE)
#define my_realloc_zero(ptr, size)     mcontext_realloc(ptr, size, __FILE__, __LINE__, TRUE)
#endif
#define my_alloca(size)                alloca((size_t)(size))  // malloc from stack


#define mem_alloc(mem_ctx, size)       mcontext_alloc(mem_ctx, size, __FILE__, __LINE__, FALSE)
#define mem_alloc_zero(mem_ctx, size)  mcontext_alloc(mem_ctx, size, __FILE__, __LINE__, TRUE)
#define mem_realloc(ptr, size)         mcontext_realloc(ptr, size, __FILE__, __LINE__, FALSE)
#define mem_realloc_zero(ptr, size)    mcontext_realloc(ptr, size, __FILE__, __LINE__, TRUE)
#define mem_free(ptr)                  mcontext_free(ptr)


/* alloc by current_memory_context */
#define palloc(size)                   my_malloc(current_memory_context, size)
#define palloc0(size)                  my_malloc_zero(current_memory_context, size)
#define pfree(ptr)                     my_free(ptr)
#define repalloc(ptr, size)            my_realloc(ptr, size)
#define repalloc0(ptr, size)           my_realloc_zero(ptr, size)



/******************************************************************************
 *                             memory BaseObject                              *
 *****************************************************************************/

// BaseObject is a basic class
// All other class should inherit from BaseObject class which
// override operator new/delete.
class BaseObject {
public:
    ~BaseObject()     {}

    void* operator new(size_t size, memory_context_t* mctx, const char* file, int line)
    {
        return mcontext_alloc(mctx, (uint32)size, file, line, FALSE);
    }

    void* operator new[](size_t size, memory_context_t* mctx, const char* file, int line)
    {
        return mcontext_alloc(mctx, (uint32)size, file, line, FALSE);
    }

    void operator delete(void* ptr)
    {
        mcontext_free(ptr);
    }

    void operator delete[](void* ptr)
    {
        mcontext_free(ptr);
    }
};


#define New(mctx) new(mctx, __FILE__, __LINE__)




/*-------------------------------------------------------------------*/

extern THREAD_LOCAL memory_context_t* current_memory_context;

inline memory_context_t* memory_context_switch_to(memory_context_t* mctx)
{
    memory_context_t* old = current_memory_context;
    current_memory_context = mctx;
    return old;
}

#ifdef __cplusplus
}
#endif

#endif  /* _CM_MEMORY_H */

