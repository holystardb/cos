#include "cm_memory.h"
#include "cm_dbug.h"
#include "cm_util.h"
#include "cm_log.h"

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
    memory_area_t *area = (memory_area_t *)malloc(sizeof(memory_area_t));
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
            free(area);
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
        page = (memory_page_t *)malloc(page_size + MemoryPageHeaderSize);
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

memory_pool_t* mpool_create(memory_area_t *area, uint32 initial_page_count,
    uint32 local_page_count, uint32 max_page_count, uint32 page_size)
{
    memory_page_t *page;
    memory_pool_t *pool = NULL;

    if (page_size == 0 || page_size % 1024 != 0 || page_size > MEM_AREA_PAGE_MAX_SIZE ||
        initial_page_count > max_page_count || initial_page_count > local_page_count ||
        local_page_count > max_page_count) {
        return NULL;
    }

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
        for (uint32 i = 0; i < num; i++) {
            pool = (memory_pool_t *)(buf + i * sizeof(memory_pool_t));
            UT_LIST_ADD_FIRST(list_node, area->free_pools, pool);
        }

        goto retry;
    }

    mutex_exit(&area->free_pools_mutex);

    //
    mutex_create(&pool->mutex);
    mutex_create(&pool->context_mutex);
    mutex_create(&pool->stack_context_mutex);
    pool->initial_page_count = initial_page_count;
    pool->local_page_count = local_page_count;
    pool->max_page_count = max_page_count;
    pool->page_alloc_count = 0;
    pool->page_size = page_size;
    pool->area = area;
    pool->next_alloc_free_page_list_id = 0;

    UT_LIST_INIT(pool->free_contexts);
    UT_LIST_INIT(pool->context_used_pages);
    UT_LIST_INIT(pool->free_stack_contexts);
    UT_LIST_INIT(pool->stack_context_used_pages);

    for (uint32 i = 0; i < MPOOL_FREE_PAGE_LIST_COUNT; i++) {
        mutex_create(&pool->free_pages_mutex[i]);
        UT_LIST_INIT(pool->free_pages[i].pages);
        pool->free_pages[i].count = 0;
    }

    for (uint32 i = 0; i < pool->initial_page_count; i++) {
        page = marea_alloc_page(pool->area, pool->page_size);
        if (page) {
            uint32 owner_list_id = i % MPOOL_FREE_PAGE_LIST_COUNT;

            mutex_enter(&pool->free_pages_mutex[owner_list_id], NULL);
            UT_LIST_ADD_LAST(list_node, pool->free_pages[owner_list_id].pages, page);
            mutex_exit(&pool->free_pages_mutex[owner_list_id]);

            atomic32_dec(&pool->page_alloc_count);
        }
    }

    return pool;
}

void mpool_destroy(memory_pool_t *pool)
{
    memory_page_t *page;

    page = UT_LIST_GET_FIRST(pool->context_used_pages);
    while (page) {
        UT_LIST_REMOVE(list_node, pool->context_used_pages, page);
        marea_free_page(pool->area, page, M_CONTEXT_PAGE_SIZE);
        page = UT_LIST_GET_FIRST(pool->context_used_pages);
    }
    UT_LIST_INIT(pool->context_used_pages);

    page = UT_LIST_GET_FIRST(pool->stack_context_used_pages);
    while (page) {
        UT_LIST_REMOVE(list_node, pool->stack_context_used_pages, page);
        marea_free_page(pool->area, page, M_CONTEXT_PAGE_SIZE);
        page = UT_LIST_GET_FIRST(pool->stack_context_used_pages);
    }
    UT_LIST_INIT(pool->stack_context_used_pages);

    for (uint32 i = 0; i < MPOOL_FREE_PAGE_LIST_COUNT; i++) {
        page = UT_LIST_GET_FIRST(pool->free_pages[i].pages);
        while (page) {
            UT_LIST_REMOVE(list_node, pool->free_pages[i].pages, page);
            marea_free_page(pool->area, page, pool->page_size);
            page = UT_LIST_GET_FIRST(pool->free_pages[i].pages);
        }
        UT_LIST_INIT(pool->free_pages[i].pages);
        pool->free_pages[i].count = 0;
    }

    mutex_enter(&pool->area->free_pools_mutex, NULL);
    UT_LIST_ADD_LAST(list_node, pool->area->free_pools, pool);
    mutex_exit(&pool->area->free_pools_mutex);
}

static memory_page_t* mpool_alloc_page_low(memory_pool_t *pool, uint64 index)
{
    memory_page_t *page;
    bool32         need_alloc = FALSE;
    uint32         owner_list_id;
    uint32         owner_list_count = UINT_MAX32;

    for (uint32 i = 0; i < MPOOL_FREE_PAGE_LIST_COUNT; i++) {
        index = (i + index) % MPOOL_FREE_PAGE_LIST_COUNT;

        mutex_enter(&pool->free_pages_mutex[index], NULL);
        page = UT_LIST_GET_FIRST(pool->free_pages[index].pages);
        if (page != NULL) {
            UT_LIST_REMOVE(list_node, pool->free_pages[index].pages, page);
            mutex_exit(&pool->free_pages_mutex[index]);
            break;
        }
        mutex_exit(&pool->free_pages_mutex[index]);

        uint32 count = pool->free_pages[index].count;
        if (owner_list_count > count) {
            owner_list_count = count;
            owner_list_id = i;
        }
    }

    if (page) {
        return page;
    }

    uint32 page_alloc_count = atomic32_inc(&pool->page_alloc_count);
    if (page_alloc_count < pool->max_page_count) {
        page = marea_alloc_page(pool->area, pool->page_size);
        if (page) {
            page->owner_list_id = owner_list_id;
            atomic32_inc(&pool->free_pages[owner_list_id].count);
            return page;
        }
    }
    atomic32_dec(&pool->page_alloc_count);

    return NULL;
}

memory_page_t* mpool_alloc_page(memory_pool_t *pool)
{
    uint32 list_id = pool->next_alloc_free_page_list_id++;
    return mpool_alloc_page_low(pool, list_id);
}

void mpool_free_page(memory_pool_t *pool, memory_page_t *page)
{
    uint32 owner_list_id = page->owner_list_id;
    ut_a(owner_list_id < MPOOL_FREE_PAGE_LIST_COUNT);

    if ((uint32)pool->page_alloc_count > pool->local_page_count) {
        ut_a(pool->page_alloc_count >= 0);
        atomic32_dec(&pool->page_alloc_count);

        ut_a(pool->free_pages[owner_list_id].count >= 0);
        atomic32_dec(&pool->free_pages[owner_list_id].count);

        marea_free_page(pool->area, page, pool->page_size);
        return;
    }

    mutex_enter(&pool->free_pages_mutex[owner_list_id], NULL);
    UT_LIST_ADD_LAST(list_node, pool->free_pages[owner_list_id].pages, page);
    mutex_exit(&pool->free_pages_mutex[owner_list_id]);
}

static void mpool_fill_mcontext(memory_pool_t *pool, memory_page_t *page)
{
    memory_context_t *ctx = NULL;
    uint32 num = pool->page_size / MemoryContextHeaderSize;
    char *buf = MEM_PAGE_DATA_PTR(page);

    ut_ad(mutex_own(&pool->context_mutex));

    for (uint32 i = 0; i < num; i++) {
        ctx = (memory_context_t *)(buf + i * MemoryContextHeaderSize);
        UT_LIST_ADD_FIRST(list_node, pool->free_contexts, ctx);
    }
}

static memory_context_t* mpool_alloc_mcontext(memory_pool_t *pool)
{
    memory_page_t *page;
    memory_context_t *ctx;

    mutex_enter(&pool->context_mutex, NULL);
    for (;;) {
        ctx = UT_LIST_GET_FIRST(pool->free_contexts);
        if  (ctx != NULL) {
            UT_LIST_REMOVE(list_node, pool->free_contexts, ctx);
            ctx->pool = pool;
            break;
        }
        //
        page = marea_alloc_page(pool->area, M_CONTEXT_PAGE_SIZE);
        if (page == NULL) {
            break;
        }
        UT_LIST_ADD_LAST(list_node, pool->context_used_pages, page);
        mpool_fill_mcontext(pool, page);
    }
    mutex_exit(&pool->context_mutex);

    return ctx;
}

static void mpool_free_mcontext(memory_context_t *ctx)
{
    mutex_enter(&ctx->pool->context_mutex, NULL);
    UT_LIST_ADD_LAST(list_node, ctx->pool->free_contexts, ctx);
    mutex_exit(&ctx->pool->context_mutex);
}

static void mpool_fill_stack_mcontext(memory_pool_t *pool, memory_page_t *page)
{
    memory_stack_context_t *ctx = NULL;
    uint32 num = pool->page_size / MemoryStackContextHeaderSize;
    char *buf = MEM_PAGE_DATA_PTR(page);

    ut_ad(mutex_own(&pool->stack_context_mutex));

    for (uint32 i = 0; i < num; i++) {
        ctx = (memory_stack_context_t *)(buf + i * MemoryStackContextHeaderSize);
        UT_LIST_ADD_FIRST(list_node, pool->free_stack_contexts, ctx);
    }
}

static memory_stack_context_t* mpool_alloc_stack_mcontext(memory_pool_t *pool)
{
    memory_page_t *page;
    memory_stack_context_t *ctx;

    mutex_enter(&pool->stack_context_mutex, NULL);
    for (;;) {
        ctx = UT_LIST_GET_FIRST(pool->free_stack_contexts);
        if  (ctx != NULL) {
            UT_LIST_REMOVE(list_node, pool->free_stack_contexts, ctx);
            ctx->pool = pool;
            break;
        }
        //
        page = marea_alloc_page(pool->area, M_CONTEXT_PAGE_SIZE);
        if (page == NULL) {
            break;
        }
        UT_LIST_ADD_LAST(list_node, pool->stack_context_used_pages, page);
        mpool_fill_stack_mcontext(pool, page);
    }
    mutex_exit(&pool->stack_context_mutex);

    return ctx;
}

static void mpool_free_stack_mcontext(memory_stack_context_t *ctx)
{
    mutex_enter(&ctx->pool->stack_context_mutex, NULL);
    UT_LIST_ADD_LAST(list_node, ctx->pool->free_stack_contexts, ctx);
    mutex_exit(&ctx->pool->stack_context_mutex);
}


/******************************************************************************
 *                             stack context                                  *
 *****************************************************************************/

memory_stack_context_t* mcontext_stack_create(memory_pool_t *pool)
{
    memory_stack_context_t *context = mpool_alloc_stack_mcontext(pool);
    if (context != NULL) {
        mutex_create(&context->mutex);
        UT_LIST_INIT(context->used_buf_pages);
    }

    return context;
}

void mcontext_stack_destroy(memory_stack_context_t *context)
{
    mcontext_stack_clean(context);
    mpool_free_stack_mcontext(context);
}

static bool32 mcontext_stack_page_extend(memory_stack_context_t *context)
{
    memory_page_t *page;

    page = mpool_alloc_page_low(context->pool, (uint64)context);
    if (page == NULL) {
        return FALSE;
    }
    page->context = context;

    mutex_enter(&context->mutex, NULL);
    mem_buf_t *buf = (mem_buf_t *)MEM_PAGE_DATA_PTR(page);
    buf->offset = 0;
    buf->buf = (char *)buf + MemoryBufHeaderSize;
    UT_LIST_ADD_LAST(list_node, context->used_buf_pages, page);
    mutex_exit(&context->mutex);

    return TRUE;
}

void* mcontext_stack_push(memory_stack_context_t *context, uint32 size)
{
    void *ptr = NULL;
    memory_page_t *page;
    uint32 align_size = ut_align8(size);

    if (align_size > context->pool->page_size - MemoryBufHeaderSize) {
        return NULL;
    }

    for (;;) {
        mutex_enter(&context->mutex, NULL);
        page = UT_LIST_GET_LAST(context->used_buf_pages);
        if (page) {
            mem_buf_t *mem_buf = (mem_buf_t *)MEM_PAGE_DATA_PTR(page);
            if (context->pool->page_size - mem_buf->offset >= align_size) {
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

    return ptr;
}

void mcontext_stack_pop(memory_stack_context_t *context, void *ptr, uint32 size)
{
    bool32 is_need_free = FALSE;
    memory_page_t *page;
    uint32 align_size = ut_align8(size);

    mutex_enter(&context->mutex, NULL);
    page = UT_LIST_GET_LAST(context->used_buf_pages);
    if (page) {
        mem_buf_t *buf = (mem_buf_t *)MEM_PAGE_DATA_PTR(page);
        if ((buf->offset >= align_size) &&
            (ptr == NULL || ptr == buf->buf + buf->offset - align_size)) {
            buf->offset -= align_size;
            if (buf->offset == 0) {
                UT_LIST_REMOVE(list_node, context->used_buf_pages, page);
                is_need_free = TRUE;
            }
        }
    }
    mutex_exit(&context->mutex);

    if (is_need_free) {
        mpool_free_page(context->pool, page);
    }
}

void mcontext_stack_pop2(memory_stack_context_t *context, void *ptr)
{
    memory_page_t *page, *tmp;
    UT_LIST_BASE_NODE_T(memory_page_t) used_buf_pages;

    UT_LIST_INIT(used_buf_pages);

    mutex_enter(&context->mutex, NULL);
    page = UT_LIST_GET_LAST(context->used_buf_pages);
    while (page) {
        mem_buf_t *mem_buf = (mem_buf_t *)MEM_PAGE_DATA_PTR(page);
        if ((char *)mem_buf < (char *)ptr &&
             ((uint32)((char *)ptr - mem_buf->buf) < context->pool->page_size)) {
            if ((char *)ptr != mem_buf->buf) {
                page = UT_LIST_GET_NEXT(list_node, page);
            }
            break;
        }
        page = UT_LIST_GET_PREV(list_node, page);
    }

    ut_a(page);

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
        mpool_free_page(context->pool, page;
        page = UT_LIST_GET_FIRST(used_buf_pages);
    }
}

bool32 mcontext_stack_clean(memory_stack_context_t *context)
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
        mpool_free_page(context->pool, page;
        page = UT_LIST_GET_FIRST(used_buf_pages);
    }

    return TRUE;
}


/******************************************************************************
 *                             normal context                                 *
 *****************************************************************************/


memory_context_t* mcontext_create(memory_pool_t *pool)
{
    memory_context_t *context = mpool_alloc_mcontext(pool);
    if (context != NULL) {
        mutex_create(&context->mutex);
        UT_LIST_INIT(context->used_block_pages);
        for (uint32 i = 0; i < MEM_BLOCK_FREE_LIST_SIZE; i++) {
            UT_LIST_INIT(context->free_mem_blocks[i]);
        }
    }

    return context;
}

void mcontext_destroy(memory_context_t *context)
{
    mcontext_clean(context);
    mpool_free_mcontext(context);
}

bool32 mcontext_clean(memory_context_t *context)
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
        mpool_free_page(context->pool, page;
        page = UT_LIST_GET_FIRST(used_block_pages);
    }

    return TRUE;
}

static uint32 mcontext_get_free_blocks_index(uint32 align_size)
{
    uint32 n;

    if (align_size < MEM_BLOCK_MIN_SIZE) {
        n = 0;
    } else {
        n = ut_2_log(align_size) - 6;
    }

    return n;
}

static uint32 mcontext_get_size_by_blocks_index(uint32 n)
{
    return ut_2_exp(n + 6);
}

static bool32 mcontext_block_fill_free_list(memory_context_t *context, uint32 n)
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
    UT_LIST_ADD_LAST(list_node, context->free_mem_blocks[n], block2);

    block->size = mcontext_get_size_by_blocks_index(n);
    UT_LIST_ADD_LAST(list_node, context->free_mem_blocks[n], block);

    return TRUE;
}

static bool32 mcontext_block_alloc_page(memory_context_t *context)
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
        UT_LIST_ADD_LAST(list_node, context->free_mem_blocks[i], block);

        used = used + mcontext_get_size_by_blocks_index(i);
    }
    mutex_exit(&context->mutex);

    return TRUE;
}

static mem_block_t* mcontext_block_get_buddy(memory_context_t *context, mem_block_t* block)
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

void* mcontext_alloc(memory_context_t *context, uint32 size, const char* file, int line)
{
    uint32 n;
    mem_block_t *block = NULL;
    uint32 align_size = size + MemoryBlockHeaderSize;

    if (context == NULL || align_size > MEM_BLOCK_MAX_SIZE) {
        return NULL;
    }

    n = mcontext_get_free_blocks_index(align_size);

retry:

    mutex_enter(&context->mutex, NULL);
    while (block == NULL) {
        block = UT_LIST_GET_FIRST(context->free_mem_blocks[n]);
        if (block) {
            UT_LIST_REMOVE(list_node, context->free_mem_blocks[n], block);
            block->is_free = 0;
            block->reserved = 0;
        } else if (!mcontext_block_fill_free_list(context, n)) { // Allocate memory from a higher level of free_mem_bufs
            break;
        }
    }
    mutex_exit(&context->mutex);

    if (block == NULL && mcontext_block_alloc_page(context)) {
        goto retry;
    }

    return block ? (char *)block + MemoryBlockHeaderSize : NULL;
}

void* mcontext_realloc(void *ptr, uint32 size, const char* file, int line)
{
    if (ptr == NULL) {
        return NULL;
    }

    mem_block_t *block = (mem_block_t *)((char *)ptr - MemoryBlockHeaderSize);
    ut_a(!block->is_free);

    if (block->size == size) {
        return ptr;
    }

    memory_context_t *context = (memory_context_t *)block->page->context;
    void *new_ptr = mcontext_alloc(context, size, file, line);
    if (likely(new_ptr != NULL)) {
        uint32 min_size = (block->size < size) ? block->size : size;
        memcpy(new_ptr, ptr, min_size);
        mcontext_free(ptr, context);
    }

    return new_ptr;
}

void mcontext_free(void *ptr, memory_context_t *context)
{
    mem_block_t *block = (mem_block_t *)((char *)ptr - MemoryBlockHeaderSize);
    memory_context_t *mctx = (memory_context_t *)block->page->context;

    if (context != NULL) {
        ut_a(context != mctx);
    }

    ut_a(!block->is_free);

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


/******************************************************************************
 *                             large memory                                   *
 *****************************************************************************/

void *os_mem_alloc_large(uint64 *n)
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
        LOGGER_ERROR(LOGGER, "VirtualAlloc(%lld bytes) failed; Windows error %d", size, GetLastError());
    }
#else
    size = getpagesize();
    /* Align block size to system page size */
    ut_ad(ut_is_2pow(size));
    size = *n = ut_2pow_round(*n + (size - 1), size);
    ptr = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | OS_MAP_ANON, -1, 0);
    if (unlikely(ptr == (void *)-1)) {
        log_stderr(LOG_ERROR, "mmap(%lld bytes) failed; errno %d", size, errno);
        ptr = NULL;
    }
#endif

    return (ptr);
}

void os_mem_free_large(void *ptr, uint64 size)
{
#ifdef __WIN__
    /* When RELEASE memory, the size parameter must be 0. Do not use MEM_RELEASE with MEM_DECOMMIT. */
    if (!VirtualFree(ptr, 0, MEM_RELEASE)) {
        LOGGER_ERROR(LOGGER, "VirtualFree(%p %lld bytes) failed; Windows error %d", ptr, size, GetLastError());
    }
#else
    if (munmap(ptr, size)) {
        log_stderr(LOG_ERROR, "munmap(%p %lld bytes) failed; errno %d", ptr, size, errno);
    }
#endif
}

bool32 madvise_dont_dump(char *mem_ptr, uint64 mem_size)
{
#ifdef __WIN__

#else
    if (madvise(mem_ptr, mem_size, MADV_DONTDUMP)) {
        log_stderr(LOG_ERROR, "MADV_DONTDUMP(%p %lld) error %s", mem, mem_size, strerror(errno));
        return FALSE;
    }
#endif
    return TRUE;
}

bool32 madvise_dump(char *mem_ptr, uint64 mem_size)
{
#ifdef __WIN__
    
#else
    if (madvise(mem_ptr, mem_size, MADV_DODUMP)) {
        log_stderr(LOG_ERROR, "MADV_DODUMP(%p %lld) error %s", mem, mem_size, strerror(errno));
        return FALSE;
    }
#endif
    return TRUE;
}

