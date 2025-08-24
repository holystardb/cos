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

    area->memory_size = ut_2pow_round(mem_size, M_CONTEXT_PAGE_SIZE);
    area->buffer = NULL;
    if (area->memory_size > 0) {
        area->buffer = (char *)os_mem_alloc_large(&area->memory_size);
        if (area->buffer == NULL) {
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
                if ((char *)page < (char *)area->buffer ||
                    (char *)page > (char *)area->buffer + area->memory_size) {
                    free(page);
                }
                page = UT_LIST_GET_FIRST(area->free_pages[i]);
            }
        }
    }

    os_mem_free_large(area->buffer, area->memory_size);
    ut_free(area);
}

memory_page_t* memory_area_t::alloc_page(uint32 page_size)
{
    memory_page_t *page;

    if (page_size % 1024 != 0) {
        return NULL;
    }

    uint32 n = ut_2_log(page_size / 1024);
    if (n >= MEM_AREA_PAGE_ARRAY_SIZE) {
        return NULL;
    }

    mutex_enter(&free_pages_mutex[n], NULL);
    page = UT_LIST_GET_FIRST(free_pages[n]);
    if (page) {
        UT_LIST_REMOVE(list_node, free_pages[n], page);
    }
    mutex_exit(&free_pages_mutex[n]);

    if (page == NULL) {
        mutex_enter(&mutex, NULL);
        if (offset + page_size + MemoryPageHeaderSize <= memory_size) {
            page = (memory_page_t *)(buffer + offset);
            offset += page_size + MemoryPageHeaderSize;
        }
        mutex_exit(&mutex);
    }

    if (page == NULL && is_extend) {
        page = (memory_page_t *)ut_malloc(page_size + MemoryPageHeaderSize);
    }

    return page;
}

void memory_area_t::free_page(memory_page_t *page, uint32 page_size)
{
    uint32 n = ut_2_log(page_size / 1024);
    ut_a(n < MEM_AREA_PAGE_ARRAY_SIZE);

    mutex_enter(&free_pages_mutex[n], NULL);
    UT_LIST_ADD_LAST(list_node, free_pages[n], page);
    mutex_exit(&free_pages_mutex[n]);
}

memory_pool_t* mpool_create(memory_area_t *marea, char* name, uint64 memory_size,
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

    mutex_enter(&marea->free_pools_mutex, NULL);

retry:

    pool = UT_LIST_GET_FIRST(marea->free_pools);
    if (pool) {
        UT_LIST_REMOVE(list_node, marea->free_pools, pool);
    } else {
        page = marea->alloc_page(M_POOL_PAGE_SIZE);
        if (page == NULL) {
            mutex_exit(&marea->free_pools_mutex);
            return NULL;
        }

        char *buf = MEM_PAGE_DATA_PTR(page);
        uint32 num = M_POOL_PAGE_SIZE / sizeof(memory_pool_t);
        if (num == 0) {
            mutex_exit(&marea->free_pools_mutex);
            return NULL;
        }
        for (uint32 i = 0; i < num; i++) {
            pool = (memory_pool_t *)(buf + i * sizeof(memory_pool_t));
            UT_LIST_ADD_FIRST(list_node, marea->free_pools, pool);
        }

        goto retry;
    }

    mutex_exit(&marea->free_pools_mutex);

    //
    memset(pool, 0x00, sizeof(memory_pool_t));
    pool->lower_page_count = lower_page_count;
    pool->page_alloc_count = 0;
    pool->alloc_memory_size = 0;
    pool->total_memory_size = memory_size;
    pool->page_size = page_size;
    pool->marea = marea;
    mutex_create(&pool->context_used_pages_mutex);
    UT_LIST_INIT(pool->context_used_pages);

    //memset(&pool->context_list_mutex_stats, 0x00, sizeof(mutex_stats_t));
    for (uint32 i = 0; i < MPOOL_FREE_CONTEXT_LIST_COUNT; i++) {
        pool_free_context_list_t* contexts = &pool->free_context_list[i];
        mutex_create(&contexts->mutex);
        UT_LIST_INIT(contexts->free_contexts);
        contexts->alloc_in_progress = FALSE;
    }
    for (uint32 i = 0; i < MPOOL_FREE_CONTEXT_LIST_COUNT; i++) {
        pool_free_stack_context_list_t* contexts = &pool->free_stack_context_list[i];
        mutex_create(&contexts->mutex);
        UT_LIST_INIT(contexts->free_contexts);
        contexts->alloc_in_progress = FALSE;
    }

    for (uint32 i = 0; i < MPOOL_FREE_PAGE_LIST_COUNT; i++) {
        pool_free_page_list_t* page_list = &pool->free_page_list[i];
        mutex_create(&page_list->mutex);
        UT_LIST_INIT(page_list->free_pages);
        page_list->count = 0;
    }
    //memset(&pool->page_list_mutex_stats, 0x00, sizeof(mutex_stats_t));

    // initialize stack memory context
    uint32 owner_list_id = 0;
    uint32 page_count = 8;
    for (uint32 page_num = 0; page_num < page_count; page_num++) {
        page = marea->alloc_page(M_CONTEXT_PAGE_SIZE);
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

                context_list = &pool->free_stack_context_list[ctx->owner_list_id];
                UT_LIST_ADD_LAST(list_node, context_list->free_contexts, ctx);
            }
        }
    }

    // initialize memory context
    owner_list_id = 0;
    page_count = 32;
    for (uint32 page_num = 0; page_num < page_count; page_num++) {
        page = marea->alloc_page(M_CONTEXT_PAGE_SIZE);
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

                context_list = &pool->free_context_list[ctx->owner_list_id];
                UT_LIST_ADD_LAST(list_node, context_list->free_contexts, ctx);
            }
        }
    }

    // intialize free page
    for (uint32 i = 0; i < initial_page_count; i++) {
        page = marea->alloc_page(pool->page_size);
        if (page) {
            page->owner_list_id = i % MPOOL_FREE_PAGE_LIST_COUNT;
            page->context = NULL;
            pool_free_page_list_t* page_list = &pool->free_page_list[page->owner_list_id];
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

void mpool_destroy(memory_pool_t *pool)
{
    memory_page_t *page;

    for (uint32 i = 0; i < MPOOL_FREE_CONTEXT_LIST_COUNT; i++) {
        pool_free_context_list_t* contexts = &pool->free_context_list[i];
        mutex_enter(&contexts->mutex);
        UT_LIST_INIT(contexts->free_contexts);
        mutex_exit(&contexts->mutex);
        mutex_destroy(&contexts->mutex);
    }
    for (uint32 i = 0; i < MPOOL_FREE_CONTEXT_LIST_COUNT; i++) {
        pool_free_stack_context_list_t* contexts = &pool->free_stack_context_list[i];
        mutex_enter(&contexts->mutex);
        UT_LIST_INIT(contexts->free_contexts);
        mutex_exit(&contexts->mutex);
        mutex_destroy(&contexts->mutex);
    }

    for (uint32 i = 0; i < MPOOL_FREE_PAGE_LIST_COUNT; i++) {
        pool_free_page_list_t* page_list = &pool->free_page_list[i];
        mutex_enter(&page_list->mutex);
        page = UT_LIST_GET_FIRST(page_list->free_pages);
        while (page) {
            UT_LIST_REMOVE(list_node, page_list->free_pages, page);
            pool->marea->free_page(page, pool->page_size);
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
        pool->marea->free_page(page, M_CONTEXT_PAGE_SIZE);
        page = UT_LIST_GET_FIRST(pool->context_used_pages);
    }
    mutex_exit(&pool->context_used_pages_mutex);
    mutex_destroy(&pool->context_used_pages_mutex);

    mutex_enter(&pool->marea->free_pools_mutex, NULL);
    UT_LIST_ADD_LAST(list_node, pool->marea->free_pools, pool);
    mutex_exit(&pool->marea->free_pools_mutex);
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

memory_page_t* memory_pool_t::alloc_page()
{
    memory_page_t *page = NULL;
    uint32         owner_list_id, tmp_list_id;
    uint32         owner_list_count = UINT_MAX32;
    uint32         page_count, loop_count = 8;
    pool_free_page_list_t* page_list;
    uint64 thread_index = os_thread_get_internal_id();

    for (uint32 i = 0; i < loop_count; i++) {
        tmp_list_id = (thread_index + i) & (MPOOL_FREE_PAGE_LIST_COUNT - 1);
        page_list = &free_page_list[tmp_list_id];

        mutex_enter(&page_list->mutex, NULL);
        page = UT_LIST_GET_FIRST(page_list->free_pages);
        if (page != NULL) {
            UT_LIST_REMOVE(list_node, page_list->free_pages, page);
            mutex_exit(&page_list->mutex);
            break;
        }
        page_count = atomic32_get(&page_list->count);
        mutex_exit(&page_list->mutex);

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
    page_list = &free_page_list[owner_list_id];
    atomic32_inc(&page_list->count);

    if (!increase_check_memory_size(page_size + MemoryPageHeaderSize)) {
        atomic32_dec(&page_list->count);
        return NULL;
    }

    page = marea->alloc_page(page_size);
    if (page == NULL) {
        atomic32_dec(&page_list->count);
        decrease_memory_size(page_size + MemoryPageHeaderSize);
        return NULL;
    }
    page->id = atomic32_inc(&page_alloc_count);
    page->owner_list_id = owner_list_id;

    return page;
}

void memory_pool_t::free_page(memory_page_t *page)
{
    uint32 owner_list_id = page->owner_list_id;
    ut_ad(owner_list_id < MPOOL_FREE_PAGE_LIST_COUNT);
    pool_free_page_list_t* page_list  = &free_page_list[owner_list_id];

    if ((uint32)page_alloc_count > lower_page_count) {
        ut_ad(atomic32_get(&page_alloc_count) > 0);
        atomic32_dec(&page_alloc_count);

        ut_ad(atomic32_get(&page_list->count) > 0);
        atomic32_dec(&page_list->count);

        decrease_memory_size(page_size + MemoryPageHeaderSize);
        marea->free_page(page, page_size);
        return;
    }

    mutex_enter(&page_list->mutex, NULL);
    ut_ad(UT_LIST_GET_LEN(page_list->free_pages) <= (uint32)atomic32_get(&page_list->count));
    UT_LIST_ADD_LAST(list_node, page_list->free_pages, page);
    mutex_exit(&page_list->mutex);
}

void memory_pool_t::fill_mcontext(pool_free_context_list_t* context_list,
    memory_page_t* page, uint32 owner_list_id)
{
    memory_context_t *ctx = NULL;
    uint32 num = M_CONTEXT_PAGE_SIZE / MemoryContextHeaderSize;
    char *buf = MEM_PAGE_DATA_PTR(page);

    ut_ad(mutex_own(&context_list->mutex));

    for (uint32 i = 0; i < num; i++) {
        ctx = (memory_context_t *)(buf + i * MemoryContextHeaderSize);
        ctx->owner_list_id = owner_list_id;
        ctx->pool = this;
        UT_LIST_ADD_FIRST(list_node, context_list->free_contexts, ctx);
    }
}

memory_context_t* memory_pool_t::alloc_mcontext()
{
    memory_context_t *ctx = NULL;
    uint32 loop_count = 8;
    uint32 owner_list_id;
    pool_free_context_list_t* context_list;
    uint64 thread_index = os_thread_get_internal_id();

retry_loop:

    for (uint32 i = 0; i < loop_count; i++) {
        owner_list_id = (thread_index + i) & (MPOOL_FREE_CONTEXT_LIST_COUNT - 1);
        context_list = &free_context_list[owner_list_id];

        mutex_enter(&context_list->mutex, NULL);
        ctx = UT_LIST_GET_FIRST(context_list->free_contexts);
        if  (ctx != NULL) {
            UT_LIST_REMOVE(list_node, context_list->free_contexts, ctx);
            mutex_exit(&context_list->mutex);
            break;
        }
        mutex_exit(&context_list->mutex);
    }

    if (ctx) {
        ctx->is_free = FALSE;
        mutex_create(&ctx->mutex);
        UT_LIST_INIT(ctx->used_block_pages);
        for (uint32 i = 0; i < MEM_BLOCK_FREE_LIST_SIZE; i++) {
            UT_LIST_INIT(ctx->free_mem_blocks[i]);
        }
        UT_LIST_INIT(ctx->alloced_ext_blocks);
        return ctx;
    }

    owner_list_id = thread_index & (MPOOL_FREE_CONTEXT_LIST_COUNT - 1);
    context_list = &free_context_list[owner_list_id];

    mutex_enter(&context_list->mutex, NULL);
    if (context_list->alloc_in_progress) {
        mutex_exit(&context_list->mutex);
        os_thread_sleep(50);
        goto retry_loop;
    }
    context_list->alloc_in_progress = TRUE;
    mutex_exit(&context_list->mutex);

    //
    memory_page_t* page = marea->alloc_page(M_CONTEXT_PAGE_SIZE);
    if (page == NULL) {
        mutex_enter(&context_list->mutex, NULL);
        context_list->alloc_in_progress = FALSE;
        mutex_exit(&context_list->mutex);
        return NULL;
    }

    mutex_enter(&context_list->mutex, NULL);
    fill_mcontext(context_list, page, owner_list_id);
    context_list->alloc_in_progress = FALSE;
    mutex_exit(&context_list->mutex);

    mutex_enter(&context_used_pages_mutex, NULL);
    UT_LIST_ADD_LAST(list_node, context_used_pages, page);
    mutex_exit(&context_used_pages_mutex);

    goto retry_loop;
    return NULL;
}

void memory_pool_t::free_mcontext(memory_context_t *context)
{
    pool_free_context_list_t* context_list;

    ut_ad(context->owner_list_id < MPOOL_FREE_CONTEXT_LIST_COUNT);
    context->clean();

    context_list= &free_context_list[context->owner_list_id];
    mutex_enter(&context_list->mutex, NULL);
    context->is_free = TRUE;
    mutex_destroy(&context->mutex);
    UT_LIST_ADD_LAST(list_node, context_list->free_contexts, context);
    mutex_exit(&context_list->mutex);
}

void memory_pool_t::fill_stack_mcontext(    pool_free_stack_context_list_t* context_list,
    memory_page_t* page, uint32 owner_list_id)
{
    memory_stack_context_t *ctx = NULL;
    uint32 num = M_CONTEXT_PAGE_SIZE / MemoryStackContextHeaderSize;
    char *buf = MEM_PAGE_DATA_PTR(page);

    ut_ad(mutex_own(&context_list->mutex));

    for (uint32 i = 0; i < num; i++) {
        ctx = (memory_stack_context_t *)(buf + i * MemoryStackContextHeaderSize);
        ctx->owner_list_id = owner_list_id;
        ctx->pool = this;
        UT_LIST_ADD_FIRST(list_node, context_list->free_contexts, ctx);
    }
}

memory_stack_context_t* memory_pool_t::alloc_stack_mcontext()
{
    memory_stack_context_t *ctx = NULL;
    uint32 loop_count = 8;
    uint32 owner_list_id;
    pool_free_stack_context_list_t* context_list;
    uint64 thread_index = os_thread_get_internal_id();

retry_loop:

    for (uint32 i = 0; i < loop_count; i++) {
        owner_list_id = (thread_index + i) & (MPOOL_FREE_CONTEXT_LIST_COUNT - 1);
        context_list = &free_stack_context_list[owner_list_id];

        mutex_enter(&context_list->mutex, NULL);
        ctx = UT_LIST_GET_FIRST(context_list->free_contexts);
        if  (ctx != NULL) {
            UT_LIST_REMOVE(list_node, context_list->free_contexts, ctx);
            mutex_exit(&context_list->mutex);
            break;
        }
        mutex_exit(&context_list->mutex);
    }

    if (ctx) {
        ctx->is_free = FALSE;
        mutex_create(&ctx->mutex);
        UT_LIST_INIT(ctx->used_buf_pages);
        return ctx;
    }

    owner_list_id = thread_index & (MPOOL_FREE_CONTEXT_LIST_COUNT - 1);
    context_list = &free_stack_context_list[owner_list_id];

    mutex_enter(&context_list->mutex, NULL);
    if (context_list->alloc_in_progress) {
        mutex_exit(&context_list->mutex);
        os_thread_sleep(50);
        goto retry_loop;
    }
    context_list->alloc_in_progress = TRUE;
    mutex_exit(&context_list->mutex);

    //
    memory_page_t* page = marea->alloc_page(M_CONTEXT_PAGE_SIZE);
    if (page == NULL) {
        mutex_enter(&context_list->mutex, NULL);
        context_list->alloc_in_progress = FALSE;
        mutex_exit(&context_list->mutex);
        return NULL;
    }

    mutex_enter(&context_list->mutex, NULL);
    fill_stack_mcontext(context_list, page, owner_list_id);
    context_list->alloc_in_progress = FALSE;
    mutex_exit(&context_list->mutex);

    mutex_enter(&context_used_pages_mutex, NULL);
    UT_LIST_ADD_LAST(list_node, context_used_pages, page);
    mutex_exit(&context_used_pages_mutex);

    goto retry_loop;
    return NULL;
}

void memory_pool_t::free_stack_mcontext(memory_stack_context_t *context)
{
    pool_free_stack_context_list_t* context_list;

    ut_ad(context->owner_list_id < MPOOL_FREE_CONTEXT_LIST_COUNT);
    context->clean();

    context_list= &free_stack_context_list[context->owner_list_id];
    mutex_enter(&context_list->mutex, NULL);
    context->is_free = TRUE;
    mutex_destroy(&context->mutex);
    UT_LIST_ADD_LAST(list_node, context_list->free_contexts, context);
    mutex_exit(&context_list->mutex);
}

bool32 memory_stack_context_t::stack_page_extend()
{
    memory_page_t *page;

    page = pool->alloc_page();
    if (page == NULL) {
        return FALSE;
    }
    page->context = this;

    mutex_enter(&mutex, NULL);
    mem_buf_t *buf = (mem_buf_t *)MEM_PAGE_DATA_PTR(page);
    buf->offset = MemoryBufHeaderSize;
    buf->buf = (char *)buf + MemoryBufHeaderSize;
    UT_LIST_ADD_LAST(list_node, used_buf_pages, page);
    mutex_exit(&mutex);

    return TRUE;
}

void* memory_stack_context_t::stack_push(uint32 size, bool32 is_alloc_zero)
{
    void *ptr = NULL;
    memory_page_t *page;
    uint32 align_size = ut_align8(size);

    if (unlikely(size == 0 || align_size > pool->page_size - MemoryBufHeaderSize)) {
        return NULL;
    }

    for (;;) {
        mutex_enter(&mutex, NULL);
        page = UT_LIST_GET_LAST(used_buf_pages);
        if (page) {
            mem_buf_t *mem_buf = (mem_buf_t *)MEM_PAGE_DATA_PTR(page);
            if (pool->page_size >= mem_buf->offset + align_size) {
                ptr = mem_buf->buf + mem_buf->offset;
                mem_buf->offset += align_size;
                mutex_exit(&mutex);
                break;
            } else {
                page = NULL;
            }
        }
        mutex_exit(&mutex);

        if (page == NULL && !stack_page_extend()) {
            break;
        }
    }

    if (ptr && is_alloc_zero) {
        memset(ptr, 0x00, align_size);
    }

    return ptr;
}

status_t memory_stack_context_t::stack_pop(void *ptr, uint32 size)
{
    bool32 is_need_free = FALSE, is_popped = FALSE;
    memory_page_t *page;
    uint32 align_size = ut_align8(size);

    ut_ad(ptr);

    mutex_enter(&mutex, NULL);
    page = UT_LIST_GET_LAST(used_buf_pages);
    if (page) {
        mem_buf_t *buf = (mem_buf_t *)MEM_PAGE_DATA_PTR(page);
        if (buf->offset >= align_size && ((char *)ptr + align_size == buf->buf + buf->offset)) {
            buf->offset -= align_size;
            is_popped = TRUE;
            if (buf->offset == 0) {
                UT_LIST_REMOVE(list_node, used_buf_pages, page);
                is_need_free = TRUE;
            }
        }
    }
    mutex_exit(&mutex);

    if (!is_popped) {
        return CM_ERROR;
    }

    if (is_need_free) {
        pool->free_page(page);
    }

    return CM_SUCCESS;
}

void* memory_stack_context_t::stack_save()
{
    void* ptr = NULL;
    memory_page_t *page;

    mutex_enter(&mutex, NULL);
    page = UT_LIST_GET_LAST(used_buf_pages);
    if (page) {
        mem_buf_t *mem_buf = (mem_buf_t *)MEM_PAGE_DATA_PTR(page);
        ptr = mem_buf->buf + mem_buf->offset;
    }
    mutex_exit(&mutex);

    return ptr;
}

status_t memory_stack_context_t::stack_restore(void* ptr)
{
    bool32 is_restored = FALSE;
    memory_page_t *page, *tmp;
    UT_LIST_BASE_NODE_T(memory_page_t) tmp_pages;

    UT_LIST_INIT(tmp_pages);

    mutex_enter(&mutex, NULL);

    if (ptr == NULL) {
        page = UT_LIST_GET_FIRST(used_buf_pages);
        is_restored = TRUE;
    } else {
        page = UT_LIST_GET_LAST(used_buf_pages);
        while (page) {
            mem_buf_t *mem_buf = (mem_buf_t *)MEM_PAGE_DATA_PTR(page);
            if (mem_buf->buf <= (char *)ptr &&
                (char *)ptr < (char *)mem_buf + pool->page_size) {
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
        UT_LIST_REMOVE(list_node, used_buf_pages, tmp);
        UT_LIST_ADD_LAST(list_node, tmp_pages, tmp);
    }

    mutex_exit(&mutex);

    page = UT_LIST_GET_FIRST(tmp_pages);
    while (page) {
        UT_LIST_REMOVE(list_node, tmp_pages, page);
        pool->free_page(page);
        page = UT_LIST_GET_FIRST(tmp_pages);
    }

    if (!is_restored) {
        return CM_ERROR;
    }

    return CM_SUCCESS;
}

void memory_stack_context_t::clean()
{
    memory_page_t *page;
    UT_LIST_BASE_NODE_T(memory_page_t) pages;

    mutex_enter(&mutex, NULL);
    pages.count = used_buf_pages.count;
    pages.start = used_buf_pages.start;
    pages.end = used_buf_pages.end;
    UT_LIST_INIT(used_buf_pages);
    mutex_exit(&mutex);

    page = UT_LIST_GET_FIRST(pages);
    while (page) {
        UT_LIST_REMOVE(list_node, pages, page);
        mpool_free_page(pool, page);
        page = UT_LIST_GET_FIRST(pages);
    }
}

uint64 memory_stack_context_t::get_memory_size()
{
    uint64 size;

    mutex_enter(&mutex, NULL);
    size = UT_LIST_GET_LEN(used_buf_pages) * pool->page_size;
    mutex_exit(&mutex);

    return size;
}

uint64 memory_context_t::get_memory_size()
{
    uint64 size;

    mutex_enter(&mutex, NULL);
    size = UT_LIST_GET_LEN(used_block_pages) * pool->page_size;
    mutex_exit(&mutex);

    return size;
}

uint32 memory_context_t::get_free_blocks_index(uint32 align_size)
{
    uint32 n;

    if (align_size < MEM_BLOCK_MIN_SIZE) {
        n = 0;
    } else {
        n = ut_2_log(align_size) - 6;
    }

    return n;
}

uint32 memory_context_t::get_size_by_blocks_index(uint32 n)
{
    return ut_2_exp(n + 6);
}

bool32 memory_context_t::block_fill_free_list(uint32 n)
{
    mem_block_t* block;
    mem_block_t* block2;

    if (n + 1 >= MEM_BLOCK_FREE_LIST_SIZE) {
        /* We come here when we have run out of space in the memory pool: */
        return FALSE;
    }

    block = UT_LIST_GET_FIRST(free_mem_blocks[n + 1]);
    if (block == NULL) {
        if (!block_fill_free_list(n + 1)) {
            return FALSE;
        }
        block = UT_LIST_GET_FIRST(free_mem_blocks[n + 1]);
    }
    UT_LIST_REMOVE(list_node, free_mem_blocks[n + 1], block);

    block2 = (mem_block_t *)((char *)block + get_size_by_blocks_index(n));
    block2->page = block->page;
    block2->size = get_size_by_blocks_index(n);
    block2->is_free = 1;
    block2->is_large_mem = 0;
    block2->magic = MEM_BLOCK_MAGIC;
    UT_LIST_ADD_LAST(list_node, free_mem_blocks[n], block2);

    block->size = get_size_by_blocks_index(n);
    UT_LIST_ADD_LAST(list_node, free_mem_blocks[n], block);

    return TRUE;
}

bool32 memory_context_t::block_alloc_page()
{
    memory_page_t *page = pool->alloc_page();
    if (!page) {
        return FALSE;
    }

    mutex_enter(&mutex, NULL);

    UT_LIST_ADD_LAST(list_node, used_block_pages, page);
    page->context = this;
    //
    char *curr_ptr = MEM_PAGE_DATA_PTR(page);
    uint32 used = 0;
    while (pool->page_size - used >= MEM_BLOCK_MIN_SIZE) {
        uint32 i = get_free_blocks_index(pool->page_size - used);
        if (get_size_by_blocks_index(i) > pool->page_size - used) {
            i--;  /* ut_2_log rounds upward */
        }

        mem_block_t *block = (mem_block_t *)(curr_ptr + used);
        block->page = page;
        block->size = get_size_by_blocks_index(i);
        block->is_free = 1;
        block->is_large_mem = 0;
        block->magic = MEM_BLOCK_MAGIC;
        UT_LIST_ADD_LAST(list_node, free_mem_blocks[i], block);

        used = used + get_size_by_blocks_index(i);
    }
    mutex_exit(&mutex);

    return TRUE;
}

mem_block_t* memory_context_t::block_get_buddy(mem_block_t* block)
{
    char *data_ptr = MEM_PAGE_DATA_PTR(block->page);
    mem_block_t *buddy;

    if ((((char *)block - data_ptr) % (2 * block->size)) == 0) {
        /* The buddy is in a higher address */
        buddy = (mem_block_t *)((char *)block + block->size);
        if ((((char *)buddy) - data_ptr) + block->size > pool->page_size) {
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

void* memory_context_t::alloc_large_mem(uint32 alloc_size, bool32 is_alloc_zero)
{
    uint32 total_size = alloc_size + MemoryBlockHeaderSize;

    if (!pool->increase_check_memory_size(total_size)) {
        return NULL;
    }

    mem_block_t* block = (mem_block_t *)ut_malloc(total_size);
    if (unlikely(block == NULL)) {
        pool->decrease_memory_size(total_size);
        return NULL;
    }

    block->is_free = 0;
    block->is_large_mem = 1;
    block->magic = MEM_BLOCK_MAGIC;
    block->size = total_size;
    block->alloc_size = alloc_size;
    block->mctx = this;
    if (is_alloc_zero) {
        memset((char *)block + MemoryBlockHeaderSize, 0x00, alloc_size);
    }

    mutex_enter(&mutex, NULL);
    UT_LIST_ADD_LAST(list_node, alloced_ext_blocks, block);
    mutex_exit(&mutex);

    return (char *)block + MemoryBlockHeaderSize;
}

void memory_context_t::free_large_mem(mem_block_t* block)
{
    ut_ad(mutex_own(&mutex));
    ut_ad(block->mctx == this);
    ut_ad(block->is_large_mem);

    pool->decrease_memory_size(block->size);
    UT_LIST_REMOVE(list_node, alloced_ext_blocks, block);

    ut_free(block);
}

void* memory_context_t::alloc_mem(uint32 alloc_size, const char* file, int32 line, bool32 is_alloc_zero)
{
    mem_block_t *block = NULL;

    if (unlikely(alloc_size == 0)) {
        return NULL;
    }

    // 1 alloc from os

    uint32 total_size = alloc_size + MemoryBlockHeaderSize;
    if (total_size > MEM_BLOCK_MAX_SIZE || total_size > pool->page_size) {
        char* ptr = (char *)alloc_large_mem(alloc_size, is_alloc_zero);
        if (unlikely(ptr == NULL)) {
            return NULL;
        }
        block = (mem_block_t *)(ptr - MemoryBlockHeaderSize);
        //block->file = file;
        //block->line = line;
        return ptr;
    }

    // 2 alloc from memory page

    uint32 n = get_free_blocks_index(total_size);

retry_loop:

    mutex_enter(&mutex, NULL);
    while (block == NULL) {
        block = UT_LIST_GET_FIRST(free_mem_blocks[n]);
        if (block) {
            UT_LIST_REMOVE(list_node, free_mem_blocks[n], block);
            block->is_free = 0;
        } else if (!block_fill_free_list(n)) {
            // fail to allocate memory from a higher level of free_mem_bufs
            break;
        }
    }
    mutex_exit(&mutex);

    if (unlikely(block == NULL && block_alloc_page())) {
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

void* memory_context_t::realloc_mem(void* ptr, uint32 alloc_size, bool32 is_alloc_zero)
{
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

    memory_context_t* context = block->is_large_mem ? block->mctx : (memory_context_t *)block->page->context;
    ut_a(context == this);

    void* new_ptr;
    uint32 total_size = alloc_size + MemoryBlockHeaderSize;
    if (total_size > MEM_BLOCK_MAX_SIZE || total_size > pool->page_size) {
        // realloc from os
        if (block->is_large_mem) {
            ut_a(block->size > pool->page_size);
            if (!pool->increase_check_memory_size(alloc_size - block->alloc_size)) {
                return NULL;
            }

            new_ptr = realloc(ptr, alloc_size + MemoryBlockHeaderSize);
            if (new_ptr == NULL) {
                pool->decrease_memory_size(alloc_size - block->alloc_size);
                return NULL;
            }

            if (is_alloc_zero) {
                memset((char *)ptr + block->alloc_size, 0x00, alloc_size - block->alloc_size);
            }
            block->alloc_size = alloc_size;
            return new_ptr;
        }

        // alloc from memory page
        new_ptr = alloc_large_mem(alloc_size, is_alloc_zero);
    } else {
        // alloc from memory page
        new_ptr = alloc_mem(alloc_size, NULL, 0, is_alloc_zero);
    }

    if (likely(new_ptr != NULL)) {
        errno_t err = memcpy_s(new_ptr, alloc_size, ptr, block->alloc_size);
        securec_check(err);
        free_mem(ptr);
    }

    return new_ptr;
}

void memory_context_t::free_mem(void* ptr)
{
    mem_block_t *block = (mem_block_t *)((char *)ptr - MemoryBlockHeaderSize);
    ut_a(!block->is_free);
    ut_a(block->magic == MEM_BLOCK_MAGIC);
    ut_a(block->alloc_size + MemoryBlockHeaderSize <= block->size);

    // 1 free to os

    if (block->is_large_mem) {
        ut_a(block->size > block->mctx->pool->page_size);
        ut_ad(this == block->mctx);

        memory_context_t *mctx = block->mctx;
        mutex_enter(&mctx->mutex, NULL);
        free_large_mem(block);
        mutex_exit(&mctx->mutex);
        return;
    }

    // 2 free to memory page

    ut_ad((memory_context_t *)block->page->context == this);
    uint32 n = get_free_blocks_index(block->size);
    mem_block_t *buddy = block_get_buddy(block);

    mutex_enter(&mutex, NULL);
    if (buddy && buddy->is_free && buddy->size == block->size) {
        UT_LIST_REMOVE(list_node, free_mem_blocks[n], buddy);
        buddy->is_free = 0;
        if ((char *)buddy < (char *)block) {
            buddy->size = block->size * 2;
        } else {
            block->size = block->size * 2;
            buddy = block;
        }
        mutex_exit(&mutex);

        free_mem((char *)buddy + MemoryBlockHeaderSize);
        return;
    } else {
        block->is_free = 1;
        UT_LIST_ADD_LAST(list_node, free_mem_blocks[n], block);
    }
    mutex_exit(&mutex);
}

void memory_context_t::clean()
{
    memory_page_t *page;
    UT_LIST_BASE_NODE_T(memory_page_t) pages;

    mutex_enter(&mutex, NULL);
    for (uint32 i = 0; i < MEM_BLOCK_FREE_LIST_SIZE; i++) {
        UT_LIST_INIT(free_mem_blocks[i]);
    }
    pages.count = used_block_pages.count;
    pages.start = used_block_pages.start;
    pages.end = used_block_pages.end;
    UT_LIST_INIT(used_block_pages);
    mutex_exit(&mutex);

    page = UT_LIST_GET_FIRST(pages);
    while (page) {
        UT_LIST_REMOVE(list_node, pages, page);
        mpool_free_page(pool, page);
        page = UT_LIST_GET_FIRST(pages);
    }

    mutex_enter(&mutex, NULL);
    mem_block_t* block = UT_LIST_GET_FIRST(alloced_ext_blocks);
    while (block) {
        UT_LIST_REMOVE(list_node, alloced_ext_blocks, block);
        free_large_mem(block);
        block = UT_LIST_GET_FIRST(alloced_ext_blocks);
    }
    mutex_exit(&mutex);
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

