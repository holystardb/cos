#include "knl_buf_lru.h"
#include "cm_list.h"
#include "cm_dbug.h"
#include "cm_log.h"
#include "knl_buf.h"
#include "knl_buf_flush.h"
#include "knl_checkpoint.h"


// We scan these many blocks when looking for a clean page to evict during LRU eviction
static const uint32 BUF_LRU_SEARCH_EVICTION_THRESHOLD = 32;

inline void buf_LRU_insert_block_to_lru_list(buf_pool_t* buf_pool, buf_page_t* bpage)
{
    ut_ad(buf_pool == buf_pool_from_bpage(bpage));
    ut_ad(mutex_own(&buf_pool->LRU_list_mutex));
    ut_ad(buf_page_in_file(bpage));
    ut_ad(!bpage->in_LRU_list);

    UT_LIST_ADD_FIRST(LRU_list_node, buf_pool->LRU, bpage);
    bpage->in_LRU_list = TRUE;

    buf_pool->stat.LRU_bytes += bpage->size.physical();
}

// Puts a block back to the free list, block must not contain a file page
inline void buf_LRU_insert_block_to_free_list(buf_pool_t* buf_pool, buf_block_t* block)
{
    ut_ad(buf_pool == buf_pool_from_block(block));
    ut_ad(mutex_own(&block->mutex));

    switch (buf_block_get_state(block)) {
    case BUF_BLOCK_MEMORY:
    case BUF_BLOCK_READY_FOR_USE:
        break;
    default:
        ut_error;
    }

    ut_ad(!block->page.in_free_list);
    ut_ad(!block->page.in_flush_list);
    ut_ad(!block->page.in_LRU_list);

    buf_block_set_state(block, BUF_BLOCK_NOT_USED);
    // Wipe page_no and space_id
    memset(block->frame + FIL_PAGE_OFFSET, 0xfe, 4);
    memset(block->frame + FIL_PAGE_SPACE, 0xfe, 4);

    mutex_enter(&buf_pool->free_list_mutex, NULL);
    ut_ad(block->page.in_free_list == FALSE);
    UT_LIST_ADD_FIRST(list_node, buf_pool->free_pages, block);
    block->page.in_free_list = TRUE;
    mutex_exit(&buf_pool->free_list_mutex);
}

// Removes a block from the LRU list
inline void buf_LRU_remove_block_from_lru_list(buf_pool_t* buf_pool, buf_page_t* bpage)
{
    ut_ad(buf_pool == buf_pool_from_bpage(bpage));
    ut_ad(mutex_own(&buf_pool->LRU_list_mutex));
    ut_ad(buf_page_in_file(bpage));
    ut_ad(bpage->in_LRU_list);

    // Remove the block from the LRU list
    UT_LIST_REMOVE(LRU_list_node, buf_pool->LRU, bpage);
    bpage->in_LRU_list = FALSE;

    buf_pool->stat.LRU_bytes -= bpage->size.physical();
}

// Remove one page from LRU list and put it to free list
// block must contain a file page and be in a state where it can be freed;
// there may or may not be a hash index to the page.
inline void buf_LRU_free_one_page(buf_page_t* bpage)
{
    buf_pool_t* buf_pool = buf_pool_from_bpage(bpage);
    rw_lock_t* hash_lock = buf_page_hash_lock_get(buf_pool, bpage->id);
    mutex_t* block_mutex = buf_page_get_mutex(bpage);

    rw_lock_x_lock(hash_lock);

    // remove page from page_hash
    ut_ad(bpage->in_page_hash);
    bpage->in_page_hash = FALSE;
    HASH_DELETE(buf_page_t, hash, buf_pool->page_hash, bpage->id.fold(), bpage);

    mutex_enter(block_mutex);
    rw_lock_x_unlock(hash_lock);

    //
    buf_block_set_state((buf_block_t*)bpage, BUF_BLOCK_MEMORY);

    // Puts a block back to the free list
    buf_LRU_insert_block_to_free_list(buf_pool, (buf_block_t*)bpage);

    mutex_exit(block_mutex);
}


// Try to free a block.
static inline bool32 buf_LRU_free_page(buf_pool_t* buf_pool, buf_page_t* bpage)
{
    ut_ad(buf_pool == buf_pool_from_bpage(bpage));
    ut_ad(mutex_own(&buf_pool->LRU_list_mutex));
    ut_ad(buf_page_in_file(bpage));

    // 1.
    mutex_t* block_mutex = buf_page_get_mutex(bpage);
    mutex_enter(block_mutex, NULL);
    if (!buf_page_can_relocate(bpage)) {
        // Do not free buffer fixed and I/O-fixed blocks
        mutex_exit(block_mutex);
        return FALSE;
    }
    mutex_exit(block_mutex);

    // 2.
    rw_lock_t* hash_lock = buf_page_hash_lock_get(buf_pool, bpage->id);
    rw_lock_x_lock(hash_lock);
    mutex_enter(block_mutex, NULL);

    // check
    if (!buf_page_can_relocate(bpage)) {
        mutex_exit(block_mutex);
        rw_lock_x_unlock(hash_lock);
        return FALSE;
    }

    //
    ut_ad(!bpage->in_flush_list);

    // remove page from LRU list
    buf_LRU_remove_block_from_lru_list(buf_pool, bpage);

    // remove page from page_hash
    ut_ad(bpage->in_page_hash);
    bpage->in_page_hash = FALSE;
    HASH_DELETE(buf_page_t, hash, buf_pool->page_hash, bpage->id.fold(), bpage);

    // Puts a block back to the free list
    buf_LRU_insert_block_to_free_list(buf_pool, (buf_block_t*)bpage);

    mutex_exit(block_mutex);
    rw_lock_x_unlock(hash_lock);

    return TRUE;
}

// Removes a block to the begin from the LRU list
static inline void buf_LRU_remove_block_to_lru_list_first(buf_pool_t* buf_pool, buf_page_t* bpage)
{
    ut_ad(buf_pool == buf_pool_from_bpage(bpage));
    ut_ad(mutex_own(&buf_pool->LRU_list_mutex));
    ut_ad(buf_page_in_file(bpage));
    ut_ad(bpage->in_LRU_list);

    // Remove the block from the LRU list
    UT_LIST_REMOVE(LRU_list_node, buf_pool->LRU, bpage);
    UT_LIST_ADD_FIRST(LRU_list_node, buf_pool->LRU, bpage);
}

inline bool32 buf_LRU_scan_and_free_block(buf_pool_t* buf_pool)
{
    uint32 scanned = 0, scan_count, evicted = 0;
    buf_page_t *bpage, *prev_bpage;

    mutex_enter(&buf_pool->LRU_list_mutex, NULL);

    scan_count = UT_LIST_GET_LEN(buf_pool->LRU);
    bpage = UT_LIST_GET_LAST(buf_pool->LRU);
    while (bpage && scanned < scan_count && evicted < BUF_LRU_SEARCH_EVICTION_THRESHOLD) {
        prev_bpage = UT_LIST_GET_PREV(LRU_list_node, bpage);

        if (buf_LRU_free_page(buf_pool, bpage)) {
            evicted++;
        } else {
            // move block to the begin of the LRU list
            buf_LRU_remove_block_to_lru_list_first(buf_pool, bpage);
        }

        bpage = prev_bpage;
        scanned++;
    }

    mutex_exit(&buf_pool->LRU_list_mutex);

    buf_pool->stat.n_ra_pages_evicted += evicted;

    return evicted;
}


// Returns a free block from the buf_pool.  The block is taken off the free list.
// If it is empty, returns NULL. */
static inline buf_block_t *buf_LRU_get_free_only(buf_pool_t* buf_pool)
{
    buf_block_t* block;

    mutex_enter(&buf_pool->free_list_mutex, NULL);

    block = (buf_block_t *)(UT_LIST_GET_FIRST(buf_pool->free_pages));
    if (block != NULL) {
        ut_ad(!block->page.in_flush_list);
        ut_ad(!block->page.in_LRU_list);
        ut_ad(!buf_page_in_file(&block->page));

        ut_ad(block->page.in_free_list);
        block->page.in_free_list = FALSE;

        UT_LIST_REMOVE(list_node, buf_pool->free_pages, block);

        mutex_exit(&buf_pool->free_list_mutex);

        mutex_enter(&block->mutex);
        buf_block_set_state(block, BUF_BLOCK_READY_FOR_USE);
        ut_ad(buf_pool_from_block(block) == buf_pool);
        mutex_exit(&block->mutex);

        return block;
    }

    mutex_exit(&buf_pool->free_list_mutex);

    return NULL;
}

// Returns a free block from the buf_pool.
// The block is taken off the free list.
// If free list is empty, blocks are moved from the end of the LRU list to the free list.
buf_block_t* buf_LRU_get_free_block(buf_pool_t* buf_pool)
{
    buf_block_t* block = NULL;
    bool32       freed = false;
    uint32       n_iterations = 0;
    uint32       flush_failures = 0;

loop:

    // If there is a block in the free list, take it
    block = buf_LRU_get_free_only(buf_pool);
    if (block != NULL) {
        ut_ad(buf_pool_from_block(block) == buf_pool);
        return block;
    }

    // If no block was in the free list,
    // search from the end of the LRU list and try to free a block there.
    mutex_enter(&buf_pool->mutex, NULL);
    if (buf_pool->try_LRU_scan) {
        mutex_exit(&buf_pool->mutex);
        os_thread_sleep(20); // 20us
        goto loop;
    }
    buf_pool->try_LRU_scan = TRUE;
    mutex_exit(&buf_pool->mutex);

    freed = buf_LRU_scan_and_free_block(buf_pool);

    mutex_enter(&buf_pool->mutex, NULL);
    buf_pool->try_LRU_scan = FALSE;
    mutex_exit(&buf_pool->mutex);

    if (freed) {
        goto loop;
    }

    if (n_iterations > 20) {
        LOGGER_WARN(LOGGER, LOG_MODULE_BUFFERPOOL,
            "Difficult to find free blocks in the buffer pool(%lu search iterations), "
            "%lu failed attempts to flush a page! "
            "It is also possible that in your fsync is very slow.",
            n_iterations, flush_failures);
    }

    /* If we have scanned the whole LRU and still are unable to find a free block
       then we should sleep here to let the page_cleaner do an LRU batch for us. */

    if (!srv_read_only_mode) {
        checkpoint_wake_up_thread();
    }

    if (n_iterations > 3) {
        os_thread_sleep(10000); // 10ms
    }

    srv_stats.buf_pool_wait_free.add(n_iterations, 1);

    n_iterations++;

    goto loop;
}


