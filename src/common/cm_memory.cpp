#include "cm_memory.h"
#include "cm_dbug.h"
#include "cm_util.h"
#include "cm_log.h"
#include "cm_error.h"

#ifndef __WIN__
#include <sys/mman.h>
#endif

THREAD_LOCAL memory_context_t  *current_memory_context = NULL;


#define M_CONTEXT_PAGE_SIZE       8192 // 8KB
#define M_POOL_PAGE_SIZE          8192 // 8KB


uint32 MemoryContextHeaderSize = ut_align8(sizeof(memory_context_t));
uint32 MemoryStackContextHeaderSize = ut_align8(sizeof(memory_stack_context_t));
uint32 MemoryPageHeaderSize = ut_align8(sizeof(memory_page_t));
uint32 MemoryBlockHeaderSize = ut_align8(sizeof(mem_block_t));
uint32 MemoryBufHeaderSize = ut_align8(sizeof(mem_buf_t));

memory_area_t* marea_create(uint64 mem_size, bool32 is_extend)
{
    memory_area_t *area = (memory_area_t *)ut_malloc_zero(sizeof(memory_area_t));
    if (area == NULL) {
        return NULL;
    }
    mutex_create(&area->mutex);
    area->is_extend = is_extend;
    area->offset = 0;

    mutex_create(&area->free_pools_mutex);
    UT_LIST_INIT(area->free_pools);

    for (uint32 i = 0; i < MEM_AREA_PAGE_ARRAY_SIZE; i++) {
        mutex_create(&area->free_pages_mutex[i]);
        UT_LIST_INIT(area->free_pages[i]);
    }

    area->size = ut_2pow_round(mem_size, M_CONTEXT_PAGE_SIZE);
    area->buf = NULL;
    if (area->size > 0) {
        area->buf = (char *)os_mem_alloc_large(&area->size);
        if (area->buf == NULL) {
            ut_free(area);
            area = NULL;
        }
    }

    return area;
}

void marea_destroy(memory_area_t* area)
{
    memory_page_t *page;

    if (area == NULL) {
        return;
    }

    if (area->is_extend) {
        for (uint32 i = 0; i < MEM_AREA_PAGE_ARRAY_SIZE; i++) {
            page = UT_LIST_GET_FIRST(area->free_pages[i]);
            while (page) {
                UT_LIST_REMOVE(list_node, area->free_pages[i], page);
                if ((char *)page < (char *)area->buf || (char *)page > (char *)area->buf + area->size) {
                    free(page);
                }
                page = UT_LIST_GET_FIRST(area->free_pages[i]);
            }
        }
    }

    os_mem_free_large(area->buf, area->size);
    free(area);
}

memory_page_t* marea_alloc_page(memory_area_t *area, uint32 page_size)
{
    memory_page_t *page;

    if (page_size % 1024 != 0) {
        return NULL;
    }

    uint32 n = ut_2_log(page_size / 1024);
    if (n >= MEM_AREA_PAGE_ARRAY_SIZE) {
        return NULL;
    }

    mutex_enter(&area->free_pages_mutex[n], NULL);
    page = UT_LIST_GET_FIRST(area->free_pages[n]);
    if (page) {
        UT_LIST_REMOVE(list_node, area->free_pages[n], page);
    }
    mutex_exit(&area->free_pages_mutex[n]);

    if (page == NULL) {
        mutex_enter(&area->mutex, NULL);
        if (area->offset + page_size + MemoryPageHeaderSize <= area->size) {
            page = (memory_page_t *)(area->buf + area->offset);
            area->offset += page_size + MemoryPageHeaderSize;
        }
        mutex_exit(&area->mutex);
    }

    if (page == NULL && area->is_extend) {
        page = (memory_page_t *)ut_malloc(page_size + MemoryPageHeaderSize);
    }

    return page;
}

void marea_free_page(memory_area_t *area, memory_page_t *page, uint32 page_size)
{
    uint32 n = ut_2_log(page_size / 1024);

    ut_a(n < MEM_AREA_PAGE_ARRAY_SIZE);

    mutex_enter(&area->free_pages_mutex[n], NULL);
    UT_LIST_ADD_LAST(list_node, area->free_pages[n], page);
    mutex_exit(&area->free_pages_mutex[n]);
}

inline memory_pool_t* mpool_create(memory_area_t *area, char* name, uint64 memory_size,
    uint32 page_size, uint32 initial_page_count, uint32 lower_page_count)
{
    if (page_size == 0 || page_size % SIZE_K(1) != 0 || page_size > MEM_AREA_PAGE_MAX_SIZE) {
        CM_SET_ERROR(ERR_CREATE_MEMORY_POOL, "can not create memory pool", name);
        return NULL;
    }
    if (memory_size > (page_size + MemoryPageHeaderSize) * UINT_MAX32) {
        CM_SET_ERROR(ERR_CREATE_MEMORY_POOL, "can not create memory pool", name);
        return NULL;
    }
    initial_page_count = initial_page_count > lower_page_count ? lower_page_count : initial_page_count;
    if (memory_size < initial_page_count * (page_size + MemoryPageHeaderSize)) {
        CM_SET_ERROR(ERR_CREATE_MEMORY_POOL, "can not create memory pool", name);
        return NULL;
    }

    memory_page_t *page;
    memory_pool_t *pool = NULL;

    mutex_enter(&area->free_pools_mutex, NULL);

retry:

    pool = UT_LIST_GET_FIRST(area->free_pools);
    if (pool) {
        UT_LIST_REMOVE(list_node, area->free_pools, pool);
    } else {
        page = marea_alloc_page(area, M_POOL_PAGE_SIZE);
        if (page == NULL) {
            mutex_exit(&area->free_pools_mutex);
            return NULL;
        }

        char *buf = MEM_PAGE_DATA_PTR(page);
        uint32 num = M_POOL_PAGE_SIZE / sizeof(memory_pool_t);
        if (num == 0) {
            mutex_exit(&area->free_pools_mutex);
            return NULL;
        }
        for (uint32 i = 0; i < num; i++) {
            pool = (memory_pool_t *)(buf + i * sizeof(memory_pool_t));
            UT_LIST_ADD_FIRST(list_node, area->free_pools, pool);
        }

        goto retry;
    }

    mutex_exit(&area->free_pools_mutex);

    //
    memset(pool, 0x00, sizeof(memory_pool_t));
    pool->lower_page_count = lower_page_count;
    pool->page_alloc_count = 0;
    pool->alloc_memory_size = 0;
    pool->total_memory_size = memory_size;
    pool->page_size = page_size;
    pool->area = area;
    mutex_create(&pool->context_used_pages_mutex);
    UT_LIST_INIT(pool->context_used_pages);

    //memset(&pool->context_list_mutex_stats, 0x00, sizeof(mutex_stats_t));
    for (uint32 i = 0; i < MPOOL_FREE_CONTEXT_LIST_COUNT; i++) {
        pool_free_context_list_t* contexts = &pool->context_list[i];
        mutex_create(&contexts->mutex);
        UT_LIST_INIT(contexts->free_contexts);
        contexts->alloc_in_progress = FALSE;
    }
    for (uint32 i = 0; i < MPOOL_FREE_CONTEXT_LIST_COUNT; i++) {
        pool_free_stack_context_list_t* contexts = &pool->stack_context_list[i];
        mutex_create(&contexts->mutex);
        UT_LIST_INIT(contexts->free_contexts);
        contexts->alloc_in_progress = FALSE;
    }

    for (uint32 i = 0; i < MPOOL_FREE_PAGE_LIST_COUNT; i++) {
        pool_free_page_list_t* page_list = &pool->page_list[i];
        mutex_create(&page_list->mutex);
        UT_LIST_INIT(page_list->free_pages);
        page_list->count = 0;
    }
    //memset(&pool->page_list_mutex_stats, 0x00, sizeof(mutex_stats_t));

    // initialize stack memory context
    uint32 owner_list_id = 0;
    uint32 page_count = 8;
    for (uint32 page_num = 0; page_num < page_count; page_num++) {
        page = marea_alloc_page(pool->area, M_CONTEXT_PAGE_SIZE);
        if (page) {
            UT_LIST_ADD_LAST(list_node, pool->context_used_pages, page);

            memory_stack_context_t* ctx;
            pool_free_stack_context_list_t* context_list;
            uint32 num = M_CONTEXT_PAGE_SIZE / MemoryStackContextHeaderSize;
            char* buf = MEM_PAGE_DATA_PTR(page);
            for (uint32 i = 0; i < num; i++) {
                ctx = (memory_stack_context_t *)(buf + i * MemoryStackContextHeaderSize);
                ctx->owner_list_id = owner_list_id & (MPOOL_FREE_CONTEXT_LIST_COUNT - 1);
                ctx->is_free = TRUE;
                owner_list_id++;
                ctx->pool = pool;

                context_list = &pool->stack_context_list[ctx->owner_list_id];
                UT_LIST_ADD_LAST(list_node, context_list->free_contexts, ctx);
            }
        }
    }

    // initialize memory context
    owner_list_id = 0;
    page_count = 32;
    for (uint32 page_num = 0; page_num < page_count; page_num++) {
        page = marea_alloc_page(pool->area, M_CONTEXT_PAGE_SIZE);
        if (page) {
            UT_LIST_ADD_LAST(list_node, pool->context_used_pages, page);

            memory_context_t* ctx;
            pool_free_context_list_t* context_list;
            uint32 num = M_CONTEXT_PAGE_SIZE / MemoryContextHeaderSize;
            char* buf = MEM_PAGE_DATA_PTR(page);
            for (uint32 i = 0; i < num; i++) {
                ctx = (memory_context_t *)(buf + i * MemoryContextHeaderSize);
                ctx->owner_list_id = owner_list_id & (MPOOL_FREE_CONTEXT_LIST_COUNT - 1);
                ctx->is_free = TRUE;
                owner_list_id++;
                ctx->pool = pool;

                context_list = &pool->context_list[ctx->owner_list_id];
                UT_LIST_ADD_LAST(list_node, context_list->free_contexts, ctx);
            }
        }
    }

    // intialize free page
    for (uint32 i = 0; i < initial_page_count; i++) {
        page = marea_alloc_page(pool->area, pool->page_size);
        if (page) {
            page->owner_list_id = i % MPOOL_FREE_PAGE_LIST_COUNT;
            page->context = NULL;
            pool_free_page_list_t* page_list = &pool->page_list[page->owner_list_id];
            mutex_enter(&page_list->mutex, NULL);
            UT_LIST_ADD_LAST(list_node, page_list->free_pages, page);
            mutex_exit(&page_list->mutex);
            atomic32_inc(&page_list->count);
            page->id = atomic32_inc(&pool->page_alloc_count);
            pool->increase_check_memory_size(pool->page_size + MemoryPageHeaderSize);
        }
    }

    return pool;
}

inline void mpool_destroy(memory_pool_t *pool)
{
    memory_page_t *page;

    for (uint32 i = 0; i < MPOOL_FREE_CONTEXT_LIST_COUNT; i++) {
        pool_free_context_list_t* contexts = &pool->context_list[i];
        mutex_enter(&contexts->mutex);
        UT_LIST_INIT(contexts->free_contexts);
        mutex_exit(&contexts->mutex);
        mutex_destroy(&contexts->mutex);
    }
    for (uint32 i = 0; i < MPOOL_FREE_CONTEXT_LIST_COUNT; i++) {
        pool_free_stack_context_list_t* contexts = &pool->stack_context_list[i];
        mutex_enter(&contexts->mutex);
        UT_LIST_INIT(contexts->free_contexts);
        mutex_exit(&contexts->mutex);
        mutex_destroy(&contexts->mutex);
    }

    for (uint32 i = 0; i < MPOOL_FREE_PAGE_LIST_COUNT; i++) {
        pool_free_page_list_t* page_list = &pool->page_list[i];
        mutex_enter(&page_list->mutex);
        page = UT_LIST_GET_FIRST(page_list->free_pages);
        while (page) {
            UT_LIST_REMOVE(list_node, page_list->free_pages, page);
            marea_free_page(pool->area, page, pool->page_size);
            page = UT_LIST_GET_FIRST(page_list->free_pages);
        }
        page_list->count = 0;
        mutex_exit(&page_list->mutex);
        mutex_destroy(&page_list->mutex);
    }

    mutex_enter(&pool->context_used_pages_mutex);
    page = UT_LIST_GET_FIRST(pool->context_used_pages);
    while (page) {
        UT_LIST_REMOVE(list_node, pool->context_used_pages, page);
        marea_free_page(pool->area, page, M_CONTEXT_PAGE_SIZE);
        page = UT_LIST_GET_FIRST(pool->context_used_pages);
    }
    mutex_exit(&pool->context_used_pages_mutex);
    mutex_destroy(&pool->context_used_pages_mutex);

    mutex_enter(&pool->area->free_pools_mutex, NULL);
    UT_LIST_ADD_LAST(list_node, pool->area->free_pools, pool);
    mutex_exit(&pool->area->free_pools_mutex);
}

bool32 memory_pool_t::increase_check_memory_size(uint32 alloc_size)
{
    uint64 size = atomic64_add(&alloc_memory_size, alloc_size);
    if (unlikely(size > total_memory_size)) {
        decrease_memory_size(alloc_size);
        return FALSE;
    }
    return TRUE;
}

void memory_pool_t::decrease_memory_size(uint32 alloc_size)
{
    bool32 ret;
    int64 old_val, new_val;

    do {
        old_val = atomic64_get(&alloc_memory_size);
        if (old_val < alloc_size) {
            return;
        }
        new_val = old_val - alloc_size;
        ret = atomic64_compare_and_swap(&alloc_memory_size, old_val, new_val);
    } while (ret == FALSE);
}

static inline memory_page_t* mpool_alloc_page_low(memory_pool_t *pool, uint64 index)
{
    memory_page_t *page = NULL;
    uint32         owner_list_id, tmp_list_id;
    uint32         owner_list_count = UINT_MAX32;
    uint32         page_count, loop_count = 8;
    pool_free_page_list_t* pages;

    for (uint32 i = 0; i < loop_count; i++) {
        tmp_list_id = (index + i) & (MPOOL_FREE_PAGE_LIST_COUNT - 1);
        pages = &pool->page_list[tmp_list_id];

        mutex_enter(&pages->mutex, NULL);
        page = UT_LIST_GET_FIRST(pages->free_pages);
        if (page != NULL) {
            UT_LIST_REMOVE(list_node, pages->free_pages, page);
            mutex_exit(&pages->mutex);
            break;
        }
        page_count = atomic32_get(&pages->count);
        mutex_exit(&pages->mutex);

        /* try to borrow page from another freelist */

        if (owner_list_count > page_count) {
            owner_list_count = page_count;
            owner_list_id = tmp_list_id;
        }
    }

    if (page) {
        return page;
    }

    ut_ad(owner_list_id < MPOOL_FREE_PAGE_LIST_COUNT);
    pages = &pool->page_list[owner_list_id];
    atomic32_inc(&pages->count);

    if (!pool->increase_check_memory_size(pool->page_size + MemoryPageHeaderSize)) {
        atomic32_dec(&pages->count);
        return NULL;
    }

    page = marea_alloc_page(pool->area, pool->page_size);
    if (page == NULL) {
        atomic32_dec(&pages->count);
        pool->decrease_memory_size(pool->page_size + MemoryPageHeaderSize);
        return NULL;
    }
    page->id = atomic32_inc(&pool->page_alloc_count);
    page->owner_list_id = owner_list_id;

    return page;
}

inline memory_page_t* mpool_alloc_page(memory_pool_t* pool)
{
    uint64 owner_list_id = os_thread_get_internal_id();
    return mpool_alloc_page_low(pool, owner_list_id);
}

inline void mpool_free_page(memory_pool_t *pool, memory_page_t *page)
{
    uint32 owner_list_id = page->owner_list_id;
    ut_ad(owner_list_id < MPOOL_FREE_PAGE_LIST_COUNT);
    pool_free_page_list_t* pages  = &pool->page_list[owner_list_id];

    if ((uint32)pool->page_alloc_count > pool->lower_page_count) {
        ut_ad(atomic32_get(&pool->page_alloc_count) > 0);
        atomic32_dec(&pool->page_alloc_count);

        ut_ad(atomic32_get(&pages->count) > 0);
        atomic32_dec(&pages->count);

        pool->decrease_memory_size(pool->page_size + MemoryPageHeaderSize);
        marea_free_page(pool->area, page, pool->page_size);
        return;
    }

    mutex_enter(&pages->mutex, NULL);
    ut_ad(UT_LIST_GET_LEN(pages->free_pages) <= (uint32)atomic32_get(&pages->count));
    UT_LIST_ADD_LAST(list_node, pages->free_pages, page);
    mutex_exit(&pages->mutex);
}

static inline void mpool_fill_mcontext(memory_pool_t *pool,
    pool_free_context_list_t* context_list, memory_page_t* page, uint32 owner_list_id)
{
    memory_context_t *ctx = NULL;
    uint32 num = M_CONTEXT_PAGE_SIZE / MemoryContextHeaderSize;
    char *buf = MEM_PAGE_DATA_PTR(page);

    ut_ad(mutex_own(&context_list->mutex));

    for (uint32 i = 0; i < num; i++) {
        ctx = (memory_context_t *)(buf + i * MemoryContextHeaderSize);
        ctx->owner_list_id = owner_list_id;
        ctx->pool = pool;
        UT_LIST_ADD_FIRST(list_node, context_list->free_contexts, ctx);
    }
}

static inline memory_context_t* mpool_alloc_mcontext_low(memory_pool_t *pool, uint64 index)
{
    memory_context_t *ctx = NULL;
    uint32 loop_count = 8;
    uint32 owner_list_id;
    pool_free_context_list_t* contexts;

retry_loop:

    for (uint32 i = 0; i < loop_count; i++) {
        owner_list_id = (index + i) & (MPOOL_FREE_CONTEXT_LIST_COUNT - 1);
        contexts = &pool->context_list[owner_list_id];

        mutex_enter(&contexts->mutex, NULL);
        ctx = UT_LIST_GET_FIRST(contexts->free_contexts);
        if  (ctx != NULL) {
            UT_LIST_REMOVE(list_node, contexts->free_contexts, ctx);
            mutex_exit(&contexts->mutex);
            break;
        }
        mutex_exit(&contexts->mutex);
    }

    if (ctx) {
        ctx->is_free = FALSE;
        return ctx;
    }

    owner_list_id = index & (MPOOL_FREE_CONTEXT_LIST_COUNT - 1);
    contexts = &pool->context_list[owner_list_id];

    mutex_enter(&contexts->mutex, NULL);
    if (contexts->alloc_in_progress) {
        mutex_exit(&contexts->mutex);
        os_thread_sleep(50);
        goto retry_loop;
    }
    contexts->alloc_in_progress = TRUE;
    mutex_exit(&contexts->mutex);

    //
    memory_page_t* page = marea_alloc_page(pool->area, M_CONTEXT_PAGE_SIZE);
    if (page == NULL) {
        mutex_enter(&contexts->mutex, NULL);
        contexts->alloc_in_progress = FALSE;
        mutex_exit(&contexts->mutex);
        return NULL;
    }

    mutex_enter(&contexts->mutex, NULL);
    mpool_fill_mcontext(pool, contexts, page, owner_list_id);
    contexts->alloc_in_progress = FALSE;
    mutex_exit(&contexts->mutex);

    mutex_enter(&pool->context_used_pages_mutex, NULL);
    UT_LIST_ADD_LAST(list_node, pool->context_used_pages, page);
    mutex_exit(&pool->context_used_pages_mutex);

    goto retry_loop;
    return NULL;
}

static inline memory_context_t* mpool_alloc_mcontext(memory_pool_t *pool)
{
    uint64 owner_list_id = os_thread_get_internal_id();
    return mpool_alloc_mcontext_low(pool, owner_list_id);
}

static inline void mpool_free_mcontext(memory_context_t *context)
{
    pool_free_context_list_t* context_list;

    ut_ad(context->owner_list_id < MPOOL_FREE_CONTEXT_LIST_COUNT);
    context->is_free = TRUE;
    context_list= &context->pool->context_list[context->owner_list_id];
    mutex_enter(&context_list->mutex, NULL);
    UT_LIST_ADD_LAST(list_node, context_list->free_contexts, context);
    mutex_exit(&context_list->mutex);
}

static inline void mpool_fill_stack_mcontext(memory_pool_t *pool,
    pool_free_stack_context_list_t* context_list, memory_page_t* page, uint32 owner_list_id)
{
    memory_stack_context_t *ctx = NULL;
    uint32 num = M_CONTEXT_PAGE_SIZE / MemoryStackContextHeaderSize;
    char *buf = MEM_PAGE_DATA_PTR(page);

    ut_ad(mutex_own(&context_list->mutex));

    for (uint32 i = 0; i < num; i++) {
        ctx = (memory_stack_context_t *)(buf + i * MemoryStackContextHeaderSize);
        ctx->owner_list_id = owner_list_id;
        ctx->pool = pool;
        UT_LIST_ADD_FIRST(list_node, context_list->free_contexts, ctx);
    }
}

static inline memory_stack_context_t* mpool_alloc_stack_mcontext_low(memory_pool_t *pool, uint64 index)
{
    memory_stack_context_t *ctx = NULL;
    uint32 loop_count = 8;
    uint32 owner_list_id;
    pool_free_stack_context_list_t* contexts;

retry_loop:

    for (uint32 i = 0; i < loop_count; i++) {
        owner_list_id = (index + i) & (MPOOL_FREE_CONTEXT_LIST_COUNT - 1);
        contexts = &pool->stack_context_list[owner_list_id];

        mutex_enter(&contexts->mutex, NULL);
        ctx = UT_LIST_GET_FIRST(contexts->free_contexts);
        if  (ctx != NULL) {
            UT_LIST_REMOVE(list_node, contexts->free_contexts, ctx);
            mutex_exit(&contexts->mutex);
            break;
        }
        mutex_exit(&contexts->mutex);
    }

    if (ctx) {
        ctx->is_free = FALSE;
        return ctx;
    }

    owner_list_id = index & (MPOOL_FREE_CONTEXT_LIST_COUNT - 1);
    contexts = &pool->stack_context_list[owner_list_id];

    mutex_enter(&contexts->mutex, NULL);
    if (contexts->alloc_in_progress) {
        mutex_exit(&contexts->mutex);
        os_thread_sleep(50);
        goto retry_loop;
    }
    contexts->alloc_in_progress = TRUE;
    mutex_exit(&contexts->mutex);

    //
    memory_page_t* page = marea_alloc_page(pool->area, M_CONTEXT_PAGE_SIZE);
    if (page == NULL) {
        mutex_enter(&contexts->mutex, NULL);
        contexts->alloc_in_progress = FALSE;
        mutex_exit(&contexts->mutex);
        return NULL;
    }

    mutex_enter(&contexts->mutex, NULL);
    mpool_fill_stack_mcontext(pool, contexts, page, owner_list_id);
    contexts->alloc_in_progress = FALSE;
    mutex_exit(&contexts->mutex);

    mutex_enter(&pool->context_used_pages_mutex, NULL);
    UT_LIST_ADD_LAST(list_node, pool->context_used_pages, page);
    mutex_exit(&pool->context_used_pages_mutex);

    goto retry_loop;
    return NULL;
}

static inline memory_stack_context_t* mpool_alloc_stack_mcontext(memory_pool_t *pool)
{
    uint64 owner_list_id = os_thread_get_internal_id();
    return mpool_alloc_stack_mcontext_low(pool, owner_list_id);
}

static inline void mpool_free_stack_mcontext(memory_stack_context_t *context)
{
    pool_free_stack_context_list_t* context_list;

    ut_ad(context->owner_list_id < MPOOL_FREE_CONTEXT_LIST_COUNT);
    context->is_free = TRUE;
    context_list= &context->pool->stack_context_list[context->owner_list_id];
    mutex_enter(&context_list->mutex, NULL);
    UT_LIST_ADD_LAST(list_node, context_list->free_contexts, context);
    mutex_exit(&context_list->mutex);
}


/******************************************************************************
 *                             stack context                                  *
 *****************************************************************************/

inline memory_stack_context_t* mcontext_stack_create(memory_pool_t *pool)
{
    memory_stack_context_t *context = mpool_alloc_stack_mcontext(pool);
    if (context != NULL) {
        mutex_create(&context->mutex);
        UT_LIST_INIT(context->used_buf_pages);
    }

    return context;
}

inline void mcontext_stack_destroy(memory_stack_context_t *context)
{
    mcontext_stack_clean(context);
    mpool_free_stack_mcontext(context);
}

static inline bool32 mcontext_stack_page_extend(memory_stack_context_t *context)
{
    memory_page_t *page;

    page = mpool_alloc_page_low(context->pool, (uint64)context);
    if (page == NULL) {
        return FALSE;
    }
    page->context = context;

    mutex_enter(&context->mutex, NULL);
    mem_buf_t *buf = (mem_buf_t *)MEM_PAGE_DATA_PTR(page);
    buf->offset = MemoryBufHeaderSize;
    buf->buf = (char *)buf + MemoryBufHeaderSize;
    UT_LIST_ADD_LAST(list_node, context->used_buf_pages, page);
    mutex_exit(&context->mutex);

    return TRUE;
}

inline void* mcontext_stack_push(memory_stack_context_t *context, uint32 size, bool32 is_alloc_zero)
{
    void *ptr = NULL;
    memory_page_t *page;
    uint32 align_size = ut_align8(size);

    if (unlikely(size == 0 || align_size > context->pool->page_size - MemoryBufHeaderSize)) {
        return NULL;
    }

    for (;;) {
        mutex_enter(&context->mutex, NULL);
        page = UT_LIST_GET_LAST(context->used_buf_pages);
        if (page) {
            mem_buf_t *mem_buf = (mem_buf_t *)MEM_PAGE_DATA_PTR(page);
            if (context->pool->page_size >= mem_buf->offset + align_size) {
                ptr = mem_buf->buf + mem_buf->offset;
                mem_buf->offset += align_size;
                mutex_exit(&context->mutex);
                break;
            } else {
                page = NULL;
            }
        }
        mutex_exit(&context->mutex);

        if (page == NULL && !mcontext_stack_page_extend(context)) {
            break;
        }
    }

    if (is_alloc_zero) {
        memset(ptr, 0x00, align_size);
    }

    return ptr;
}

inline status_t mcontext_stack_pop(memory_stack_context_t *context, void *ptr, uint32 size)
{
    bool32 is_need_free = FALSE, is_popped = FALSE;
    memory_page_t *page;
    uint32 align_size = ut_align8(size);

    ut_ad(ptr);

    mutex_enter(&context->mutex, NULL);
    page = UT_LIST_GET_LAST(context->used_buf_pages);
    if (page) {
        mem_buf_t *buf = (mem_buf_t *)MEM_PAGE_DATA_PTR(page);
        if (buf->offset >= align_size && ((char *)ptr + align_size == buf->buf + buf->offset)) {
            buf->offset -= align_size;
            is_popped = TRUE;
            if (buf->offset == 0) {
                UT_LIST_REMOVE(list_node, context->used_buf_pages, page);
                is_need_free = TRUE;
            }
        }
    }
    mutex_exit(&context->mutex);

    if (!is_popped) {
        return CM_ERROR;
    }

    if (is_need_free) {
        mpool_free_page(context->pool, page);
    }

    return CM_SUCCESS;
}

inline void* mcontext_stack_save(memory_stack_context_t* context)
{
    void* ptr = NULL;
    memory_page_t *page;

    if (context == NULL) {
        return NULL;
    }

    mutex_enter(&context->mutex, NULL);
    page = UT_LIST_GET_LAST(context->used_buf_pages);
    if (page) {
        mem_buf_t *mem_buf = (mem_buf_t *)MEM_PAGE_DATA_PTR(page);
        ptr = mem_buf->buf + mem_buf->offset;
    }
    mutex_exit(&context->mutex);

    return ptr;
}

inline status_t mcontext_stack_restore(memory_stack_context_t* context, void* ptr)
{
    bool32 is_restored = FALSE;
    memory_page_t *page, *tmp;
    UT_LIST_BASE_NODE_T(memory_page_t) used_buf_pages;

    UT_LIST_INIT(used_buf_pages);

    mutex_enter(&context->mutex, NULL);

    if (ptr == NULL) {
        page = UT_LIST_GET_FIRST(context->used_buf_pages);
        is_restored = TRUE;
    } else {
        page = UT_LIST_GET_LAST(context->used_buf_pages);
        while (page) {
            mem_buf_t *mem_buf = (mem_buf_t *)MEM_PAGE_DATA_PTR(page);
            if (mem_buf->buf <= (char *)ptr &&
                (char *)ptr < (char *)mem_buf + context->pool->page_size) {
                is_restored = TRUE;
                if ((char *)ptr > mem_buf->buf) {
                    mem_buf->offset = (uint32)((char *)ptr - mem_buf->buf);
                    page = UT_LIST_GET_NEXT(list_node, page);
                }
                break;
            }
            page = UT_LIST_GET_PREV(list_node, page);
        }
    }

    while (page) {
        tmp = page;
        page = UT_LIST_GET_NEXT(list_node, page);
        UT_LIST_REMOVE(list_node, context->used_buf_pages, tmp);
        UT_LIST_ADD_LAST(list_node, used_buf_pages, tmp);
    }

    mutex_exit(&context->mutex);

    page = UT_LIST_GET_FIRST(used_buf_pages);
    while (page) {
        UT_LIST_REMOVE(list_node, used_buf_pages, page);
        mpool_free_page(context->pool, page);
        page = UT_LIST_GET_FIRST(used_buf_pages);
    }

    if (!is_restored) {
        return CM_ERROR;
    }

    return CM_SUCCESS;
}

inline void mcontext_stack_clean(memory_stack_context_t *context)
{
    memory_page_t *page;
    UT_LIST_BASE_NODE_T(memory_page_t) used_buf_pages;

    mutex_enter(&context->mutex, NULL);
    used_buf_pages.count = context->used_buf_pages.count;
    used_buf_pages.start = context->used_buf_pages.start;
    used_buf_pages.end = context->used_buf_pages.end;
    UT_LIST_INIT(context->used_buf_pages);
    mutex_exit(&context->mutex);

    page = UT_LIST_GET_FIRST(used_buf_pages);
    while (page) {
        UT_LIST_REMOVE(list_node, used_buf_pages, page);
        mpool_free_page(context->pool, page);
        page = UT_LIST_GET_FIRST(used_buf_pages);
    }
}

inline uint64 mcontext_stack_get_size(memory_stack_context_t* context)
{
    uint64 size;

    mutex_enter(&context->mutex, NULL);
    size = UT_LIST_GET_LEN(context->used_buf_pages) * context->pool->page_size;
    mutex_exit(&context->mutex);

    return size;
}


/******************************************************************************
 *                             normal context                                 *
 *****************************************************************************/


inline memory_context_t* mcontext_create(memory_pool_t *pool)
{
    memory_context_t *context = mpool_alloc_mcontext(pool);
    if (context != NULL) {
        mutex_create(&context->mutex);
        UT_LIST_INIT(context->used_block_pages);
        for (uint32 i = 0; i < MEM_BLOCK_FREE_LIST_SIZE; i++) {
            UT_LIST_INIT(context->free_mem_blocks[i]);
        }
        UT_LIST_INIT(context->alloced_ext_blocks);
    }

    return context;
}

inline void mcontext_destroy(memory_context_t *context)
{
    mcontext_clean(context);
    mpool_free_mcontext(context);
}

inline uint64 mcontext_get_size(memory_context_t* context)
{
    uint64 size;

    mutex_enter(&context->mutex, NULL);
    size = UT_LIST_GET_LEN(context->used_block_pages) * context->pool->page_size;
    mutex_exit(&context->mutex);

    return size;
}

static inline uint32 mcontext_get_free_blocks_index(uint32 align_size)
{
    uint32 n;

    if (align_size < MEM_BLOCK_MIN_SIZE) {
        n = 0;
    } else {
        n = ut_2_log(align_size) - 6;
    }

    return n;
}

static inline uint32 mcontext_get_size_by_blocks_index(uint32 n)
{
    return ut_2_exp(n + 6);
}

static inline bool32 mcontext_block_fill_free_list(memory_context_t *context, uint32 n)
{
    mem_block_t* block;
    mem_block_t* block2;

    if (n + 1 >= MEM_BLOCK_FREE_LIST_SIZE) {
        /* We come here when we have run out of space in the memory pool: */
        return FALSE;
    }

    block = UT_LIST_GET_FIRST(context->free_mem_blocks[n + 1]);
    if (block == NULL) {
        if (!mcontext_block_fill_free_list(context, n + 1)) {
            return FALSE;
        }
        block = UT_LIST_GET_FIRST(context->free_mem_blocks[n + 1]);
    }
    UT_LIST_REMOVE(list_node, context->free_mem_blocks[n + 1], block);

    block2 = (mem_block_t *)((char *)block + mcontext_get_size_by_blocks_index(n));
    block2->page = block->page;
    block2->size = mcontext_get_size_by_blocks_index(n);
    block2->is_free = 1;
    block2->is_ext = 0;
    block2->magic = MEM_BLOCK_MAGIC;
    UT_LIST_ADD_LAST(list_node, context->free_mem_blocks[n], block2);

    block->size = mcontext_get_size_by_blocks_index(n);
    UT_LIST_ADD_LAST(list_node, context->free_mem_blocks[n], block);

    return TRUE;
}

static inline bool32 mcontext_block_alloc_page(memory_context_t *context)
{
    memory_page_t *page = mpool_alloc_page_low(context->pool, (uint64)context);
    if (!page) {
        return FALSE;
    }

    mutex_enter(&context->mutex, NULL);
    //
    UT_LIST_ADD_LAST(list_node, context->used_block_pages, page);
    page->context = context;
    //
    char *curr_ptr = MEM_PAGE_DATA_PTR(page);
    uint32 used = 0;
    uint32 mem_block_min_size = 2 * MemoryBlockHeaderSize;
    while (context->pool->page_size - used >= MEM_BLOCK_MIN_SIZE) {
        uint32 i = mcontext_get_free_blocks_index(context->pool->page_size - used);
        if (mcontext_get_size_by_blocks_index(i) > context->pool->page_size - used) {
            i--;  /* ut_2_log rounds upward */
        }

        mem_block_t *block = (mem_block_t *)(curr_ptr + used);
        block->page = page;
        block->size = mcontext_get_size_by_blocks_index(i);
        block->is_free = 1;
        block->is_ext = 0;
        block->magic = MEM_BLOCK_MAGIC;
        UT_LIST_ADD_LAST(list_node, context->free_mem_blocks[i], block);

        used = used + mcontext_get_size_by_blocks_index(i);
    }
    mutex_exit(&context->mutex);

    return TRUE;
}

static inline mem_block_t* mcontext_block_get_buddy(memory_context_t *context, mem_block_t* block)
{
    char *data_ptr = MEM_PAGE_DATA_PTR(block->page);
    mem_block_t *buddy;

    if ((((char *)block - data_ptr) % (2 * block->size)) == 0) {
        /* The buddy is in a higher address */
        buddy = (mem_block_t *)((char *)block + block->size);
        if ((((char *)buddy) - data_ptr) + block->size > context->pool->page_size) {
            /* The buddy is not wholly contained in the pool: there is no buddy */
            buddy = NULL;
        }
    } else {
        /* The buddy is in a lower address; NOTE that area cannot be at the pool lower end,
           because then we would end up to the upper branch in this if-clause: the remainder would be 0 */
        buddy = (mem_block_t *)((char *)block - block->size);
    }

    return buddy;
}

static inline void* mcontext_alloc_ext(memory_context_t* context, uint32 alloc_size, bool32 is_alloc_zero)
{
    uint32 total_size = alloc_size + MemoryBlockHeaderSize;

    if (!context->pool->increase_check_memory_size(total_size)) {
        return NULL;
    }

    mem_block_t* block = (mem_block_t *)malloc(total_size);
    if (unlikely(block == NULL)) {
        context->pool->decrease_memory_size(total_size);
        return NULL;
    }

    block->is_free = 0;
    block->is_ext = 1;
    block->magic = MEM_BLOCK_MAGIC;
    block->size = total_size;
    block->alloc_size = alloc_size;
    block->mctx = context;
    if (is_alloc_zero) {
        memset((char *)block + MemoryBlockHeaderSize, 0x00, alloc_size);
    }

    mutex_enter(&context->mutex, NULL);
    UT_LIST_ADD_LAST(list_node, context->alloced_ext_blocks, block);
    mutex_exit(&context->mutex);

    return (char *)block + MemoryBlockHeaderSize;
}

static inline void mcontext_free_ext(mem_block_t* block)
{
    memory_context_t* context = block->mctx;

    ut_a(block->is_ext);

    context->pool->decrease_memory_size(block->size);

    ut_ad(mutex_own(&context->mutex));
    UT_LIST_REMOVE(list_node, context->alloced_ext_blocks, block);

    free(block);
}

inline void* mcontext_alloc(memory_context_t* context, uint32 alloc_size, const char* file, int32 line, bool32 is_alloc_zero)
{
    if (unlikely(context == NULL || alloc_size == 0)) {
        return NULL;
    }

    // 1 alloc from os

    uint32 total_size = alloc_size + MemoryBlockHeaderSize;
    if (total_size > MEM_BLOCK_MAX_SIZE || total_size > context->pool->page_size) {
        return mcontext_alloc_ext(context, alloc_size, is_alloc_zero);
    }

    // 2 alloc from memory page

    mem_block_t *block = NULL;
    uint32 n = mcontext_get_free_blocks_index(total_size);

retry_loop:

    mutex_enter(&context->mutex, NULL);
    while (block == NULL) {
        block = UT_LIST_GET_FIRST(context->free_mem_blocks[n]);
        if (block) {
            UT_LIST_REMOVE(list_node, context->free_mem_blocks[n], block);
            block->is_free = 0;
        } else if (!mcontext_block_fill_free_list(context, n)) {
            // fail to allocate memory from a higher level of free_mem_bufs
            break;
        }
    }
    mutex_exit(&context->mutex);

    if (unlikely(block == NULL && mcontext_block_alloc_page(context))) {
        goto retry_loop;
    }

    if (unlikely(block == NULL)) {
        return NULL;
    }

    block->alloc_size = alloc_size;
    char* ptr = (char *)block + MemoryBlockHeaderSize;
    if (is_alloc_zero) {
        memset(ptr, 0x00, alloc_size);
    }

    return ptr;
}

inline void* mcontext_realloc(void* ptr, uint32 alloc_size, const char* file, int32 line, bool32 is_alloc_zero)
{
    if (ptr == NULL) {
        return NULL;
    }

    mem_block_t *block = (mem_block_t *)((char *)ptr - MemoryBlockHeaderSize);
    ut_a(!block->is_free);
    ut_a(block->magic == MEM_BLOCK_MAGIC);
    ut_a(block->alloc_size + MemoryBlockHeaderSize <= block->size);

    // 
    if (block->size >= alloc_size + MemoryBlockHeaderSize) {
        if (is_alloc_zero && alloc_size > block->alloc_size) {
            memset((char *)ptr + block->alloc_size, 0x00, alloc_size - block->alloc_size);
        }
        block->alloc_size = alloc_size;
        return ptr;
    }

    void* new_ptr;
    memory_context_t* context = block->is_ext ? block->mctx : (memory_context_t *)block->page->context;
    uint32 total_size = alloc_size + MemoryBlockHeaderSize;
    if (total_size > MEM_BLOCK_MAX_SIZE || total_size > context->pool->page_size) {
        // realloc from os
        if (block->is_ext) {
            ut_a(block->size > context->pool->page_size);
            if (!context->pool->increase_check_memory_size(alloc_size - block->alloc_size)) {
                return NULL;
            }

            new_ptr = realloc(ptr, alloc_size + MemoryBlockHeaderSize);
            if (new_ptr == NULL) {
                context->pool->decrease_memory_size(alloc_size - block->alloc_size);
                return NULL;
            }

            if (is_alloc_zero) {
                memset((char *)ptr + block->alloc_size, 0x00, alloc_size - block->alloc_size);
            }
            block->alloc_size = alloc_size;
            return new_ptr;
        }

        // alloc from memory page
        new_ptr = mcontext_alloc_ext(context, alloc_size, is_alloc_zero);
    } else {
        // alloc from memory page
        new_ptr = mcontext_alloc(context, alloc_size, file, line, is_alloc_zero);
    }

    if (likely(new_ptr != NULL)) {
        errno_t err = memcpy_s(new_ptr, alloc_size, ptr, block->alloc_size);
        securec_check(err);
        mcontext_free(ptr, context);
    }

    return new_ptr;
}

inline void mcontext_free(void* ptr, memory_context_t* context)
{
    mem_block_t *block = (mem_block_t *)((char *)ptr - MemoryBlockHeaderSize);
    ut_a(!block->is_free);
    ut_a(block->magic == MEM_BLOCK_MAGIC);
    ut_a(block->alloc_size + MemoryBlockHeaderSize <= block->size);

    // 1 free to os

    if (block->is_ext) {
        ut_a(block->size > block->mctx->pool->page_size);
        ut_a(context == NULL || context == block->mctx);

        memory_context_t *mctx = block->mctx;
        mutex_enter(&mctx->mutex, NULL);
        mcontext_free_ext(block);
        mutex_exit(&mctx->mutex);
        return;
    }

    // 2 free to memory page

    memory_context_t *mctx = (memory_context_t *)block->page->context;
    ut_a(context == NULL || mctx == context);
    uint32 n = mcontext_get_free_blocks_index(block->size);
    mem_block_t *buddy = mcontext_block_get_buddy(mctx, block);

    mutex_enter(&mctx->mutex, NULL);
    if (buddy && buddy->is_free && buddy->size == block->size) {
        UT_LIST_REMOVE(list_node, mctx->free_mem_blocks[n], buddy);
        buddy->is_free = 0;
        if ((char *)buddy < (char *)block) {
            buddy->size = block->size * 2;
        } else {
            block->size = block->size * 2;
            buddy = block;
        }
        mutex_exit(&mctx->mutex);

        mcontext_free((char *)buddy + MemoryBlockHeaderSize, mctx);
        return;
    } else {
        block->is_free = 1;
        UT_LIST_ADD_LAST(list_node, mctx->free_mem_blocks[n], block);
    }
    mutex_exit(&mctx->mutex);
}

inline void mcontext_clean(memory_context_t *context)
{
    memory_page_t *page;
    UT_LIST_BASE_NODE_T(memory_page_t) used_block_pages;

    mutex_enter(&context->mutex, NULL);
    for (uint32 i = 0; i < MEM_BLOCK_FREE_LIST_SIZE; i++) {
        UT_LIST_INIT(context->free_mem_blocks[i]);
    }
    used_block_pages.count = context->used_block_pages.count;
    used_block_pages.start = context->used_block_pages.start;
    used_block_pages.end = context->used_block_pages.end;
    UT_LIST_INIT(context->used_block_pages);
    mutex_exit(&context->mutex);

    page = UT_LIST_GET_FIRST(used_block_pages);
    while (page) {
        UT_LIST_REMOVE(list_node, used_block_pages, page);
        mpool_free_page(context->pool, page);
        page = UT_LIST_GET_FIRST(used_block_pages);
    }

    mutex_enter(&context->mutex, NULL);
    mem_block_t* block = UT_LIST_GET_FIRST(context->alloced_ext_blocks);
    while (block) {
        UT_LIST_REMOVE(list_node, context->alloced_ext_blocks, block);
        mcontext_free_ext(block);
        block = UT_LIST_GET_FIRST(context->alloced_ext_blocks);
    }
    mutex_exit(&context->mutex);
}



/******************************************************************************
 *                             large memory                                   *
 *****************************************************************************/

void* os_mem_alloc_large(uint64* n)
{
    void *ptr;
    uint64 size;

#ifdef __WIN__
    SYSTEM_INFO system_info;
    GetSystemInfo(&system_info);

    /* Align block size to system page size */
    ut_ad(ut_is_2pow(system_info.dwPageSize));
    /* system_info.dwPageSize is only 32-bit. Casting to ulint is required on 64-bit Windows. */
    size = *n = ut_2pow_round(*n + (system_info.dwPageSize - 1), (uint64)system_info.dwPageSize);
    ptr = VirtualAlloc(NULL, size, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
    if (!ptr) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_MEMORY, "VirtualAlloc(%lld bytes) failed; Windows error %d", size, GetLastError());
    }
#else
    size = getpagesize();
    /* Align block size to system page size */
    ut_ad(ut_is_2pow(size));
    size = *n = ut_2pow_round(*n + (size - 1), size);
    ptr = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (unlikely(ptr == (void *)-1)) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_MEMORY, "mmap(%lld bytes) failed; errno %d", size, errno);
        ptr = NULL;
    }
#endif

    return (ptr);
}

void os_mem_free_large(void* ptr, uint64 size)
{
#ifdef __WIN__
    /* When RELEASE memory, the size parameter must be 0. Do not use MEM_RELEASE with MEM_DECOMMIT. */
    if (!VirtualFree(ptr, 0, MEM_RELEASE)) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_MEMORY, "VirtualFree(%p %lld bytes) failed; Windows error %d", ptr, size, GetLastError());
    }
#else
    if (munmap(ptr, size)) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_MEMORY, "munmap(%p %lld bytes) failed; errno %d", ptr, size, errno);
    }
#endif
}

bool32 madvise_dont_dump(char* mem_ptr, uint64 mem_size)
{
#ifdef __WIN__

#else
    if (madvise(mem_ptr, mem_size, MADV_DONTDUMP)) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_MEMORY, "MADV_DONTDUMP(%p %lld) error %s",
            mem_ptr, mem_size, strerror(errno));
        return FALSE;
    }
#endif
    return TRUE;
}

bool32 madvise_dump(char* mem_ptr, uint64 mem_size)
{
#ifdef __WIN__
    
#else
    if (madvise(mem_ptr, mem_size, MADV_DODUMP)) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_MEMORY, "MADV_DODUMP(%p %lld) error %s",
            mem_ptr, mem_size, strerror(errno));
        return FALSE;
    }
#endif
    return TRUE;
}

