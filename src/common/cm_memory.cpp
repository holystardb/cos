#include "cm_memory.h"
#include "cm_dbug.h"
#include "cm_util.h"
#include "cm_log.h"

THREAD_LOCAL memory_pool_t     *current_memory_pool = NULL;
THREAD_LOCAL memory_context_t  *current_memory_context = NULL;


#define M_CONTEXT_PAGE_SIZE       8192 // 8KB
#define PageData(page)            ((char *)page + MemoryPageHeaderSize)

uint32 MemoryContextHeaderSize = ut_align8(sizeof(memory_context_t));
uint32 MemoryPageHeaderSize = ut_align8(sizeof(memory_page_t));
uint32 MemoryBlockHeaderSize = ut_align8(sizeof(mem_block_t));
uint32 MemoryBufHeaderSize = ut_align8(sizeof(mem_buf_t));

memory_area_t* marea_create(uint64 mem_size, bool32 is_extend)
{
    memory_area_t *area = (memory_area_t *)malloc(sizeof(memory_area_t));
    if (area) {
        area->is_extend = is_extend;
        spin_lock_init(&area->lock);
        area->offset = 0;
        for (uint32 i = 0; i < MEM_AREA_PAGE_ARRAY_SIZE; i++) {
            UT_LIST_INIT(area->free_pages[i]);
        }
        area->size = ut_2pow_round(mem_size, M_CONTEXT_PAGE_SIZE);
        area->buf = (char *)os_mem_alloc_large(&area->size);
        if (!area->buf) {
            free(area);
            area = NULL;
        }
    }

    return area;
}

void marea_destroy(memory_area_t* area)
{
    memory_page_t *page;

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

    uint32 n = ut_2_log(page_size / 1024);
    if (n >= MEM_AREA_PAGE_ARRAY_SIZE) {
        return NULL;
    }

    spin_lock(&area->lock, NULL);
    page = UT_LIST_GET_FIRST(area->free_pages[n]);
    if (page) {
        UT_LIST_REMOVE(list_node, area->free_pages[n], page);
    } else if (area->offset + page_size + MemoryPageHeaderSize <= area->size) {
        page = (memory_page_t *)(area->buf + area->offset);
        area->offset += page_size + MemoryPageHeaderSize;
    }
    spin_unlock(&area->lock);

    if (page == NULL && area->is_extend) {
        page = (memory_page_t *)malloc(page_size + ut_align8(sizeof(memory_page_t)));
    }

    return page;
}

void marea_free_page(memory_area_t *area, memory_page_t *page, uint32 page_size)
{
    uint32 n = ut_2_log(page_size / 1024);

    ut_a(n < MEM_AREA_PAGE_ARRAY_SIZE);

    spin_lock(&area->lock, NULL);
    UT_LIST_ADD_LAST(list_node, area->free_pages[n], page);
    spin_unlock(&area->lock);
}

memory_pool_t* mpool_create(memory_area_t *area, uint32 local_page_count, uint32 max_page_count, uint32 page_size)
{
    if (page_size == 0 || page_size % 1024 != 0 || page_size > MEM_AREA_PAGE_MAX_SIZE ||
        local_page_count == 0 || max_page_count < local_page_count) {
        return NULL;
    }
    memory_pool_t *pool = (memory_pool_t *)malloc(sizeof(memory_pool_t));
    if (pool != NULL) {
        spin_lock_init(&pool->lock);
        pool->local_page_count = local_page_count;
        pool->max_page_count = max_page_count;
        pool->page_alloc_count = 0;
        pool->page_size = page_size;
        pool->area = area;
        UT_LIST_INIT(pool->free_pages);
        UT_LIST_INIT(pool->free_contexts);
        UT_LIST_INIT(pool->context_used_pages);
    }

    return pool;
}

void mpool_destroy(memory_pool_t *pool)
{
    memory_page_t *page;
    UT_LIST_BASE_NODE_T(memory_page_t) free_pages, context_pages;

    spin_lock(&pool->lock, NULL);
    free_pages.count = pool->free_pages.count;
    free_pages.start = pool->free_pages.start;
    free_pages.end = pool->free_pages.end;
    UT_LIST_INIT(pool->free_pages);
    context_pages.count = pool->context_used_pages.count;
    context_pages.start = pool->context_used_pages.start;
    context_pages.end = pool->context_used_pages.end;
    UT_LIST_INIT(pool->context_used_pages);
    spin_unlock(&pool->lock);

    page = UT_LIST_GET_FIRST(free_pages);
    while (page) {
        UT_LIST_REMOVE(list_node, free_pages, page);
        marea_free_page(pool->area, page, pool->page_size);
        page = UT_LIST_GET_FIRST(free_pages);
    }

    page = UT_LIST_GET_FIRST(context_pages);
    while (page) {
        UT_LIST_REMOVE(list_node, context_pages, page);
        marea_free_page(pool->area, page, M_CONTEXT_PAGE_SIZE);
        page = UT_LIST_GET_FIRST(context_pages);
    }

    free(pool);
}

memory_page_t* mpool_alloc_page(memory_pool_t *pool)
{
    memory_page_t *page;
    bool32 need_alloc = FALSE;

    spin_lock(&pool->lock, NULL);
    page = UT_LIST_GET_FIRST(pool->free_pages);
    if (page != NULL) {
        UT_LIST_REMOVE(list_node, pool->free_pages, page);
    } else if (pool->page_alloc_count < pool->max_page_count) {
        page = marea_alloc_page(pool->area, pool->page_size);
        if (page) {
            pool->page_alloc_count++;
        }
    }
    spin_unlock(&pool->lock);

    return page;
}

void mpool_free_page(memory_pool_t *pool, memory_page_t *page)
{
    bool32 is_need_free = FALSE;

    spin_lock(&pool->lock, NULL);
    if (pool->local_page_count >= pool->page_alloc_count) {
        UT_LIST_ADD_LAST(list_node, pool->free_pages, page);
    } else {
        if (pool->page_alloc_count > 0) {
            pool->page_alloc_count--;
        }
        is_need_free = TRUE;
    }
    spin_unlock(&pool->lock);

    if (is_need_free) {
        marea_free_page(pool->area, page, pool->page_size);
    }
}

static void mpool_fill_mcontext(memory_pool_t *pool, memory_page_t *page)
{
    memory_context_t *ctx = NULL;
    uint32 num = pool->page_size / MemoryContextHeaderSize;
    char *buf = PageData(page);

    for (uint32 i = 0; i < num; i++) {
        ctx = (memory_context_t *)(buf + i * MemoryContextHeaderSize);
        UT_LIST_ADD_FIRST(list_node, pool->free_contexts, ctx);
    }
}

static memory_context_t* mpool_alloc_mcontext(memory_pool_t *pool)
{
    memory_page_t *page;
    memory_context_t *ctx;

    spin_lock(&pool->lock, NULL);
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
    spin_unlock(&pool->lock);

    return ctx;
}

static void mpool_free_mcontext(memory_context_t *ctx)
{
    spin_lock(&ctx->pool->lock, NULL);
    UT_LIST_ADD_LAST(list_node, ctx->pool->free_contexts, ctx);
    spin_unlock(&ctx->pool->lock);
}

memory_context_t* mcontext_create(memory_pool_t *pool)
{
    memory_context_t *context = mpool_alloc_mcontext(pool);
    if (context != NULL) {
        spin_lock_init(&context->lock);
        UT_LIST_INIT(context->used_buf_pages);
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

static bool32 mcontext_page_extend(memory_context_t *context)
{
    memory_page_t *page = mpool_alloc_page(context->pool);

    if (page == NULL) {
        return FALSE;
    }

    spin_lock(&context->lock, NULL);
    mem_buf_t *buf = (mem_buf_t *)PageData(page);
    buf->offset = 0;
    buf->buf = (char *)buf + MemoryBufHeaderSize;
    UT_LIST_ADD_LAST(list_node, context->used_buf_pages, page);
    spin_unlock(&context->lock);

    return TRUE;
}

void* mcontext_push(memory_context_t *context, uint32 size)
{
    void *ptr = NULL;
    memory_page_t *page;
    uint32 align_size = ut_align8(size);

    if (align_size > context->pool->page_size - MemoryBufHeaderSize) {
        return NULL;
    }

    for (;;) {
        spin_lock(&context->lock, NULL);
        page = UT_LIST_GET_LAST(context->used_buf_pages);
        if (page) {
            mem_buf_t *mem_buf = (mem_buf_t *)PageData(page);
            if (context->pool->page_size - mem_buf->offset >= align_size) {
                ptr = mem_buf->buf + mem_buf->offset;
                mem_buf->offset += align_size;
                spin_unlock(&context->lock);
                break;
            } else {
                page = NULL;
            }
        }
        spin_unlock(&context->lock);

        if (page == NULL && !mcontext_page_extend(context)) {
            break;
        }
    }

    return ptr;
}

void mcontext_pop(memory_context_t *context, void *ptr, uint32 size)
{
    bool32 is_need_free = FALSE;
    memory_page_t *page;
    uint32 align_size = ut_align8(size);

    spin_lock(&context->lock, NULL);
    page = UT_LIST_GET_LAST(context->used_buf_pages);
    if (page) {
        mem_buf_t *buf = (mem_buf_t *)PageData(page);
        if (buf->offset >= align_size && (ptr == NULL || ptr == buf->buf + buf->offset - align_size)) {
            buf->offset -= align_size;
            if (buf->offset == 0) {
                UT_LIST_REMOVE(list_node, context->used_buf_pages, page);
                is_need_free = TRUE;
            }
        }
    }
    spin_unlock(&context->lock);

    if (is_need_free) {
        mpool_free_page(context->pool, page);
    }
}

void mcontext_pop2(memory_context_t *context, void *ptr)
{
    memory_page_t *page, *tmp;
    UT_LIST_BASE_NODE_T(memory_page_t) used_buf_pages;

    UT_LIST_INIT(used_buf_pages);

    spin_lock(&context->lock, NULL);
    page = UT_LIST_GET_LAST(context->used_buf_pages);
    while (page) {
        mem_buf_t *mem_buf = (mem_buf_t *)PageData(page);
        if ((char *)mem_buf < (char *)ptr && ((uint32)((char *)ptr - mem_buf->buf) < context->pool->page_size)) {
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
    spin_unlock(&context->lock);

    page = UT_LIST_GET_FIRST(used_buf_pages);
    while (page) {
        UT_LIST_REMOVE(list_node, used_buf_pages, page);
        mpool_free_page(context->pool, page);
        page = UT_LIST_GET_FIRST(used_buf_pages);
    }
}

bool32 mcontext_clean(memory_context_t *context)
{
    memory_page_t *page;
    UT_LIST_BASE_NODE_T(memory_page_t) used_buf_pages;
    UT_LIST_BASE_NODE_T(memory_page_t) used_block_pages;

    spin_lock(&context->lock, NULL);
    for (uint32 i = 0; i < MEM_BLOCK_FREE_LIST_SIZE; i++) {
        UT_LIST_INIT(context->free_mem_blocks[i]);
    }

    used_buf_pages.count = context->used_buf_pages.count;
    used_buf_pages.start = context->used_buf_pages.start;
    used_buf_pages.end = context->used_buf_pages.end;
    UT_LIST_INIT(context->used_buf_pages);

    used_block_pages.count = context->used_block_pages.count;
    used_block_pages.start = context->used_block_pages.start;
    used_block_pages.end = context->used_block_pages.end;
    UT_LIST_INIT(context->used_block_pages);
    spin_unlock(&context->lock);

    page = UT_LIST_GET_FIRST(used_buf_pages);
    while (page) {
        UT_LIST_REMOVE(list_node, used_buf_pages, page);
        mpool_free_page(context->pool, page);
        page = UT_LIST_GET_FIRST(used_buf_pages);
    }

    page = UT_LIST_GET_FIRST(used_block_pages);
    while (page) {
        UT_LIST_REMOVE(list_node, used_block_pages, page);
        mpool_free_page(context->pool, page);
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

    if (n >= MEM_BLOCK_FREE_LIST_SIZE) {
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
    memory_page_t *page = mpool_alloc_page(context->pool);
    if (!page) {
        return FALSE;
    }

    spin_lock(&context->lock, NULL);
    //
    UT_LIST_ADD_LAST(list_node, context->used_block_pages, page);

    //
    char *curr_ptr = PageData(page);
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
    spin_unlock(&context->lock);

    return TRUE;
}

static mem_block_t* mcontext_block_get_buddy(memory_context_t *context, mem_block_t* block)
{
    char *data_ptr = PageData(block->page);
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

void* mcontext_alloc(memory_context_t *context, uint32 size)
{
    uint32 n;
    mem_block_t *block = NULL;
    uint32 align_size = size + MemoryBlockHeaderSize;

    if (align_size > MEM_BLOCK_MAX_SIZE) {
        return NULL;
    }

    n = mcontext_get_free_blocks_index(align_size);

retry:

    spin_lock(&context->lock, NULL);
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
    spin_unlock(&context->lock);

    if (block == NULL && mcontext_block_alloc_page(context)) {
        goto retry;
    }

    return block ? (char *)block + MemoryBlockHeaderSize : NULL;
}

void* mcontext_realloc(memory_context_t *context, void *old_ptr, uint32 size)
{
    if (old_ptr == NULL) {
        return mcontext_alloc(context, size);
    }

    mem_block_t *block = (mem_block_t *)((char *)old_ptr - MemoryBlockHeaderSize);
    ut_a(!block->is_free);

    if (block->size == size) {
        return old_ptr;
    }

    void *new_ptr = mcontext_alloc(context, size);
    if (likely(new_ptr != NULL)) {
        uint32 min_size = (block->size < size) ? block->size : size;
        memcpy(new_ptr, old_ptr, min_size);
        mcontext_free(context, old_ptr);
    }

    return new_ptr;
}

void mcontext_free(memory_context_t *context, void *ptr)
{
    mem_block_t *block = (mem_block_t *)((char *)ptr - MemoryBlockHeaderSize);

    ut_a(!block->is_free);

    uint32 n = mcontext_get_free_blocks_index(block->size);
    mem_block_t *buddy = mcontext_block_get_buddy(context, block);

    spin_lock(&context->lock, NULL);
    if (buddy && buddy->is_free && buddy->size == block->size) {
        UT_LIST_REMOVE(list_node, context->free_mem_blocks[n], buddy);
        buddy->is_free = 0;

        if ((char *)buddy < (char *)block) {
            buddy->size = block->size * 2;
        } else {
            block->size = block->size * 2;
            buddy = block;
        }
        spin_unlock(&context->lock);

        mcontext_free(context, (char *)buddy + MemoryBlockHeaderSize);
        return;
    } else {
        block->is_free = 1;
        UT_LIST_ADD_LAST(list_node, context->free_mem_blocks[n], block);
    }
    spin_unlock(&context->lock);
}

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
        LOG_PRINT_ERROR("VirtualAlloc(%lld bytes) failed; Windows error %d", size, GetLastError());
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
        LOG_PRINT_ERROR("VirtualFree(%p %lld bytes) failed; Windows error %d", ptr, size, GetLastError());
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

