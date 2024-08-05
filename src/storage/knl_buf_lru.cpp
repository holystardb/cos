#include "knl_buf_lru.h"
#include "cm_list.h"
#include "cm_dbug.h"
#include "cm_log.h"
#include "knl_buf.h"
#include "knl_buf_flush.h"

/******************************************************************//**
Returns a free block from the buf_pool.  The block is taken off the free list.
If it is empty, returns NULL. */
buf_block_t *buf_LRU_get_free_only(buf_pool_t *buf_pool)
{
    buf_block_t *block;

    mutex_enter(&buf_pool->free_list_mutex, NULL);

    block = (buf_block_t *)(UT_LIST_GET_FIRST(buf_pool->free_pages));
    if (block != NULL) {
        ut_ad(block->page.in_free_list);
        ut_ad(!block->page.in_flush_list);
        ut_ad(!block->page.in_LRU_list);
        ut_a(!buf_page_in_file(&block->page));

        UT_LIST_REMOVE(list_node, buf_pool->free_pages, &block->page);

        mutex_exit(&buf_pool->free_list_mutex);

        buf_block_set_state(block, BUF_BLOCK_READY_FOR_USE);

        return (block);
    }

    mutex_exit(&buf_pool->free_list_mutex);

    return block;
}

/** We scan these many blocks when looking for a clean page to evict during LRU eviction. */
static const uint32 BUF_LRU_SEARCH_SCAN_THRESHOLD = 100;

bool32 buf_flush_ready_for_replace(buf_page_t *bpage)
{
    ut_ad(spin_lock_own(buf_page_get_mutex(bpage)));
    ut_ad(bpage->in_LRU_list);

    if (buf_page_in_file(bpage)) {
        return (bpage->recovery_lsn == 0 &&
                bpage->buf_fix_count == 0 &&
                buf_page_get_io_fix(bpage) == BUF_IO_NONE);
    }

    LOGGER_ERROR(LOGGER,
        "Buffer block %lu state %lu in the LRU list!",
        bpage, bpage->state);

    return (FALSE);
}

/** Takes a block out of the LRU list and page hash table.
If the block is compressed-only (BUF_BLOCK_ZIP_PAGE), the object will be freed.

The caller must hold buf_pool->LRU_list_mutex, the buf_page_get_mutex() mutex and the appropriate hash_lock.
This function will release the buf_page_get_mutex() and the hash_lock. */
static bool32 buf_LRU_block_remove_hashed(buf_page_t *bpage, bool zip, bool ignore_content)
{
    const buf_page_t *hashed_bpage;
    buf_pool_t *buf_pool = buf_pool_from_bpage(bpage);
    rw_lock_t *hash_lock;

    ut_ad(mutex_own(&buf_pool->LRU_list_mutex));
    ut_ad(mutex_own(buf_page_get_mutex(bpage)));

    hash_lock = buf_page_hash_lock_get(buf_pool, bpage->id);

    ut_ad(rw_lock_own(hash_lock, RW_LOCK_EXCLUSIVE));

    ut_a(buf_page_get_io_fix(bpage) == BUF_IO_NONE);
    ut_a(bpage->buf_fix_count == 0);

    buf_LRU_remove_block(bpage);
    buf_pool->freed_page_clock += 1;

    switch (buf_page_get_state(bpage)) {
    case BUF_BLOCK_FILE_PAGE: {
        ((buf_block_t *)bpage)->modify_clock++;
        if (!ignore_content) {
            /* Account the eviction of index leaf pages from the buffer pool(s). */
            //const byte *frame = bpage->frame;
            //const uint32 type = fil_page_get_type(frame);
            //if ((type == FIL_PAGE_INDEX || type == FIL_PAGE_RTREE) && page_is_leaf(frame)) {
            //    uint32 space_id = bpage->id.space();
            //    space_index_t idx_id = btr_page_get_index_id(frame);
            //    buf_stat_per_index->dec(index_id_t(space_id, idx_id));
            //}
        }
        break;
    }
    case BUF_BLOCK_POOL_WATCH:
    case BUF_BLOCK_ZIP_DIRTY:
    case BUF_BLOCK_NOT_USED:
    case BUF_BLOCK_READY_FOR_USE:
    case BUF_BLOCK_MEMORY:
    case BUF_BLOCK_REMOVE_HASH:
        ut_error;
        break;
    }

    hashed_bpage = buf_page_hash_get_low(buf_pool, bpage->id);
    if (bpage != hashed_bpage) {
        LOGGER_ERROR(LOGGER,
            "Page(space %lu page %lu) not found in the hash table",
            bpage->id.space(), bpage->id.page_no());

        if (hashed_bpage) {
            LOGGER_ERROR(LOGGER,
                "In hash table we find block %p of space %lu page %lu which is not %p",
                hashed_bpage, bpage->id.space(), bpage->id.page_no(), bpage);
        }

        ut_d(mutex_exit(buf_page_get_mutex(bpage)));

        ut_d(rw_lock_x_unlock(hash_lock));

        ut_d(mutex_exit(&buf_pool->LRU_list_mutex));
        ut_d(ut_error);
    }

    ut_ad(bpage->in_page_hash);
    ut_d(bpage->in_page_hash = false);

    HASH_DELETE(buf_page_t, hash, buf_pool->page_hash, bpage->id.fold(), bpage);

    switch (buf_page_get_state(bpage)) {
    case BUF_BLOCK_FILE_PAGE: {
        memset(((buf_block_t *)bpage)->frame + FIL_PAGE_OFFSET, 0xff, 4);
        memset(((buf_block_t *)bpage)->frame + FIL_PAGE_SPACE,  0xff, 4);
        //UNIV_MEM_INVALID(((buf_block_t *)bpage)->frame, UNIV_PAGE_SIZE);
        buf_page_set_state(bpage, BUF_BLOCK_REMOVE_HASH);

        ut_ad(mutex_own(&buf_pool->LRU_list_mutex));

        rw_lock_x_unlock(hash_lock);

        mutex_exit(buf_page_get_mutex(bpage));

        return TRUE;
    }
    case BUF_BLOCK_POOL_WATCH:
    case BUF_BLOCK_ZIP_DIRTY:
    case BUF_BLOCK_NOT_USED:
    case BUF_BLOCK_READY_FOR_USE:
    case BUF_BLOCK_MEMORY:
    case BUF_BLOCK_REMOVE_HASH:
        break;
    }

    ut_error;
}


/*******************************************************************//**
Initializes the old blocks pointer in the LRU list. This function should be
called when the LRU list grows to BUF_LRU_OLD_MIN_LEN length. */
static void buf_LRU_old_init(buf_pool_t *buf_pool)
{
	buf_page_t*	bpage;

	ut_ad(mutex_own(&buf_pool->LRU_list_mutex));
	ut_a(UT_LIST_GET_LEN(buf_pool->LRU) == BUF_LRU_OLD_MIN_LEN);

	/* We first initialize all blocks in the LRU list as old and then use
	the adjust function to move the LRU_old pointer to the right
	position */

	for (bpage = UT_LIST_GET_LAST(buf_pool->LRU); bpage != NULL;
	     bpage = UT_LIST_GET_PREV(LRU, bpage)) {
		ut_ad(bpage->in_LRU_list);
		ut_ad(buf_page_in_file(bpage));
		/* This loop temporarily violates the
		assertions of buf_page_set_old(). */
		bpage->old = TRUE;
	}

	buf_pool->LRU_old = UT_LIST_GET_FIRST(buf_pool->LRU);
	buf_pool->LRU_old_len = UT_LIST_GET_LEN(buf_pool->LRU);

	buf_LRU_old_adjust_len(buf_pool);
}

/******************************************************************//**
Increases LRU size in bytes with zip_size for compressed page,
UNIV_PAGE_SIZE for uncompressed page in inline function */
static inline void incr_LRU_size_in_bytes(
    buf_page_t *bpage,     /*!< in: control block */
    buf_pool_t *buf_pool)  /*!< in: buffer pool instance */
{
    ut_ad(mutex_own(&buf_pool->LRU_list_mutex));

    buf_pool->stat.LRU_bytes += UNIV_PAGE_SIZE;
    ut_ad(buf_pool->stat.LRU_bytes <= buf_pool->curr_pool_size);
}

// Removes a block from the LRU list. */
void buf_LRU_remove_block(buf_page_t *bpage)
{
    buf_pool_t *buf_pool = buf_pool_from_bpage(bpage);

    ut_ad(buf_pool);
    ut_ad(mutex_own(&buf_pool->LRU_list_mutex));
    ut_a(buf_page_in_file(bpage));
    ut_ad(bpage->in_LRU_list);

    /* If the LRU_old pointer is defined and points to just this block,
       move it backward one step */

    if (UNLIKELY(bpage == buf_pool->LRU_old)) {
        /* Below: the previous block is guaranteed to exist,
           because the LRU_old pointer is only allowed to differ
           by BUF_LRU_OLD_TOLERANCE from strict
           buf_pool->LRU_old_ratio/BUF_LRU_OLD_RATIO_DIV of the LRU list length. */
        buf_page_t *prev_bpage = UT_LIST_GET_PREV(LRU, bpage);
        ut_a(!prev_bpage->old);
        buf_pool->LRU_old = prev_bpage;
        buf_page_set_old(prev_bpage, TRUE);
        buf_pool->LRU_old_len++;
    }

    /* Remove the block from the LRU list */
    UT_LIST_REMOVE(LRU, buf_pool->LRU, bpage);
    ut_d(bpage->in_LRU_list = FALSE);

    buf_pool->stat.LRU_bytes -= UNIV_PAGE_SIZE;

    /* If the LRU list is so short that LRU_old is not defined,
       clear the "old" flags and return */
    if (UT_LIST_GET_LEN(buf_pool->LRU) < BUF_LRU_OLD_MIN_LEN) {
        for (bpage = UT_LIST_GET_FIRST(buf_pool->LRU); bpage != NULL;
             bpage = UT_LIST_GET_NEXT(LRU, bpage)) {
            /* This loop temporarily violates the assertions of buf_page_set_old(). */
            bpage->old = FALSE;
        }
        buf_pool->LRU_old = NULL;
        buf_pool->LRU_old_len = 0;
        return;
    }

    ut_ad(buf_pool->LRU_old);

    /* Update the LRU_old_len field if necessary */
    if (buf_page_is_old(bpage)) {
        buf_pool->LRU_old_len--;
    }

    /* Adjust the length of the old block list if necessary */
    buf_LRU_old_adjust_len(buf_pool);
}


// Adds a block to the LRU list.
// Please make sure that the zip_size is
// already set into the page zip when invoking the function,
// so that we can get correct zip_size from the buffer page when adding a block into LRU
void buf_LRU_add_block(
    buf_page_t *bpage,  /*!< in: control block */
    bool32      old)    /*!< in: TRUE if should be put to the old blocks
                         in the LRU list, else put to the start; if the
                         LRU list is very short, the block is added to
                         the start, regardless of this parameter */
{
    buf_pool_t *buf_pool = buf_pool_from_bpage(bpage);

    ut_ad(buf_pool);
    ut_ad(mutex_own(&buf_pool->LRU_list_mutex));

    ut_a(buf_page_in_file(bpage));
    ut_ad(!bpage->in_LRU_list);

    if (!old || (UT_LIST_GET_LEN(buf_pool->LRU) < BUF_LRU_OLD_MIN_LEN)) {
        UT_LIST_ADD_FIRST(LRU, buf_pool->LRU, bpage);
        bpage->freed_page_clock = buf_pool->freed_page_clock;
    } else {
#ifdef UNIV_LRU_DEBUG
        /* buf_pool->LRU_old must be the first item in the LRU list whose "old" flag is set. */
        ut_a(buf_pool->LRU_old->old);
        ut_a(!UT_LIST_GET_PREV(LRU, buf_pool->LRU_old) || !UT_LIST_GET_PREV(LRU, buf_pool->LRU_old)->old);
        ut_a(!UT_LIST_GET_NEXT(LRU, buf_pool->LRU_old) || UT_LIST_GET_NEXT(LRU, buf_pool->LRU_old)->old);
#endif /* UNIV_LRU_DEBUG */
        UT_LIST_ADD_AFTER(LRU, buf_pool->LRU, buf_pool->LRU_old, bpage);
        buf_pool->LRU_old_len++;
    }

    ut_d(bpage->in_LRU_list = TRUE);

    incr_LRU_size_in_bytes(bpage, buf_pool);

    if (UT_LIST_GET_LEN(buf_pool->LRU) > BUF_LRU_OLD_MIN_LEN) {
        ut_ad(buf_pool->LRU_old);
        /* Adjust the length of the old block list if necessary */
        buf_page_set_old(bpage, old);
        buf_LRU_old_adjust_len(buf_pool);
    } else if (UT_LIST_GET_LEN(buf_pool->LRU) == BUF_LRU_OLD_MIN_LEN) {
        /* The LRU list is now long enough for LRU_old to become defined: init it */
        buf_LRU_old_init(buf_pool);
    } else {
        buf_page_set_old(bpage, buf_pool->LRU_old != NULL);
    }
}

// Try to free a block.
bool32 buf_LRU_free_page(buf_page_t *bpage)
{
    buf_page_t *b = NULL;
    buf_pool_t *buf_pool = buf_pool_from_bpage(bpage);
    rw_lock_t *hash_lock = buf_page_hash_lock_get(buf_pool, bpage->id);
    mutex_t *block_lock = buf_page_get_mutex(bpage);

    ut_ad(mutex_own(&buf_pool->LRU_list_mutex));
    ut_ad(mutex_own(block_lock));
    ut_ad(buf_page_in_file(bpage));
    ut_ad(bpage->in_LRU_list);

    if (!buf_page_can_relocate(bpage)) {
        /* Do not free buffer fixed and I/O-fixed blocks. */
        return FALSE;
    }

    return TRUE;
}

static void buf_LRU_block_free_hashed_page(buf_block_t *block)
{
    buf_block_set_state(block, BUF_BLOCK_MEMORY);

    buf_LRU_block_free_non_file_page(block);
}

void buf_LRU_free_one_page(buf_page_t *bpage, bool32 ignore_content)
{
#ifdef UNIV_DEBUG
    buf_pool_t *buf_pool = buf_pool_from_bpage(bpage);
    mutex_t *block_mutex = buf_page_get_mutex(bpage);
    rw_lock_t *hash_lock = buf_page_hash_lock_get(buf_pool, bpage->id);

    ut_ad(mutex_own(&buf_pool->LRU_list_mutex));
    ut_ad(mutex_own(block_mutex));
    ut_ad(rw_lock_own(hash_lock, RW_LOCK_EXCLUSIVE));
#endif /* UNIV_DEBUG */

    if (buf_LRU_block_remove_hashed(bpage, true, ignore_content)) {
        buf_LRU_block_free_hashed_page((buf_block_t *)bpage);
    }

    /* buf_LRU_block_remove_hashed() releases hash_lock and block_mutex */
    ut_ad(!rw_lock_own(hash_lock, RW_LOCK_EXCLUSIVE) &&
          !rw_lock_own(hash_lock, RW_LOCK_SHARED));

#ifdef UNIV_DEBUG
    ut_ad(!mutex_own(block_mutex));
#endif /* UNIV_DEBUG */
}

// Puts a block back to the free list
// block block must not contain a file page
void buf_LRU_block_free_non_file_page(buf_block_t *block)
{
    void*       data;
    buf_pool_t* buf_pool = buf_pool_from_block(block);

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
    /* Wipe page_no and space_id */
    memset(block->frame + FIL_PAGE_OFFSET, 0xfe, 4);
    memset(block->frame + FIL_PAGE_SPACE, 0xfe, 4);

    mutex_enter(&buf_pool->free_list_mutex, NULL);
    UT_LIST_ADD_FIRST(list_node, buf_pool->free_pages, (&block->page));
    ut_d(block->page.in_free_list = TRUE);
    mutex_exit(&buf_pool->free_list_mutex);
}

/** Scan depth for LRU flush batch i.e.: number of blocks scanned*/
ulong srv_LRU_scan_depth = 1024;

//Try to free a clean page from the common LRU list.
//scan_all scan whole LRU list if true, otherwise scan only up to BUF_LRU_SEARCH_SCAN_THRESHOLD
static bool32 buf_LRU_free_from_common_LRU_list(buf_pool_t *buf_pool, bool32 scan_all)
{
    ut_ad(mutex_own(&buf_pool->LRU_list_mutex));

    uint32      scanned = 0;
    bool32      freed = false;
    buf_page_t *bpage;

    for (bpage = UT_LIST_GET_LAST(buf_pool->LRU), scanned = 1, freed = FALSE;
         bpage != NULL && !freed && (scan_all || scanned < srv_LRU_scan_depth);
         ++scanned) {

        uint32      accessed = 0;
        buf_page_t* prev_bpage = UT_LIST_GET_PREV(LRU, bpage);

        ut_ad(buf_page_in_file(bpage));
        ut_ad(bpage->in_LRU_list);

        accessed = buf_page_is_accessed(bpage);
        freed = buf_LRU_free_page(bpage);
        if (freed && !accessed) {
            /* Keep track of pages that are evicted without
               ever being accessed. This gives us a measure of
               the effectiveness of readahead */
            ++buf_pool->stat.n_ra_pages_evicted;
        }

        bpage = prev_bpage;
    }

    return freed;
}

/** Try to free a replaceable block.
@param[in,out] buf_pool buffer pool instance
@param[in]     scan_all scan whole LRU list if ture, otherwise scan only BUF_LRU_SEARCH_SCAN_THRESHOLD blocks
@return true if found and freed */
bool32 buf_LRU_scan_and_free_block(buf_pool_t *buf_pool, bool scan_all)
{
    bool32 freed;

    mutex_enter(&buf_pool->LRU_list_mutex, NULL);
    freed = buf_LRU_free_from_common_LRU_list(buf_pool, scan_all);
    if (!freed) {
        mutex_exit(&buf_pool->LRU_list_mutex);
    }

    return (freed);
}

#define BUF_LRU_OLD_TOLERANCE 20  // #define since it is used in #if below
#define BUF_LRU_OLD_RATIO_DIV 1024 // The denominator of buf_pool->LRU_old_ratio


/** Maximum value of buf_pool->LRU_old_ratio.
@see buf_LRU_old_adjust_len
@see buf_pool->LRU_old_ratio_update */
#define BUF_LRU_OLD_RATIO_MAX BUF_LRU_OLD_RATIO_DIV
/** Minimum value of buf_pool->LRU_old_ratio.
@see buf_LRU_old_adjust_len
@see buf_pool->LRU_old_ratio_update
The minimum must exceed
(BUF_LRU_OLD_TOLERANCE + 5) * BUF_LRU_OLD_RATIO_DIV / BUF_LRU_OLD_MIN_LEN. */
#define BUF_LRU_OLD_RATIO_MIN 51

/** The minimum amount of non-old blocks when the LRU_old list exists
(that is, when there are more than BUF_LRU_OLD_MIN_LEN blocks).
@see buf_LRU_old_adjust_len */
#define BUF_LRU_NON_OLD_MIN_LEN 5


// Moves the LRU_old pointer so that the length of the old blocks list
// is inside the allowed limits.
void buf_LRU_old_adjust_len(buf_pool_t* buf_pool)
{
    uint32 old_len;
    uint32 new_len = 0;

    ut_a(buf_pool->LRU_old);
    ut_ad(mutex_own(&buf_pool->LRU_list_mutex));
    ut_ad(buf_pool->LRU_old_ratio >= BUF_LRU_OLD_RATIO_MIN);
    ut_ad(buf_pool->LRU_old_ratio <= BUF_LRU_OLD_RATIO_MAX);

    old_len = buf_pool->LRU_old_len;
    new_len = ut_min(UT_LIST_GET_LEN(buf_pool->LRU) * buf_pool->LRU_old_ratio / BUF_LRU_OLD_RATIO_DIV,
        UT_LIST_GET_LEN(buf_pool->LRU) - (BUF_LRU_OLD_TOLERANCE + BUF_LRU_NON_OLD_MIN_LEN));

    for (;;) {
        buf_page_t* LRU_old = buf_pool->LRU_old;

        ut_a(LRU_old);
        ut_ad(LRU_old->in_LRU_list);

        // Update the LRU_old pointer if necessary
        if (old_len + BUF_LRU_OLD_TOLERANCE < new_len) {
            buf_pool->LRU_old = LRU_old = UT_LIST_GET_PREV(LRU, LRU_old);
            old_len = ++buf_pool->LRU_old_len;
            buf_page_set_old(LRU_old, TRUE);
        } else if (old_len > new_len + BUF_LRU_OLD_TOLERANCE) {
            buf_pool->LRU_old = UT_LIST_GET_NEXT(LRU, LRU_old);
            old_len = --buf_pool->LRU_old_len;
            buf_page_set_old(LRU_old, FALSE);
        } else {
            return;
        }
    }
}

// Returns a free block from the buf_pool.
// The block is taken off the free list.
// If free list is empty, blocks are moved from the end of the LRU list to the free list.
buf_block_t* buf_LRU_get_free_block(buf_pool_t *buf_pool)
{
    buf_block_t *block = NULL;
    bool         freed = false;
    uint32       n_iterations = 0;
    uint32       flush_failures = 0;

loop:

    /* If there is a block in the free list, take it */
    block = buf_LRU_get_free_only(buf_pool);
    if (block != NULL) {
        ut_ad(buf_pool_from_block(block) == buf_pool);
        //block->skip_flush_check = false;
        //block->page.flush_observer = NULL;
        return(block);
    }

    freed = false;
    os_rmb;
    if (buf_pool->try_LRU_scan || n_iterations > 0) {
        /* If no block was in the free list,
         * search from the end of the LRU list and try to free a block there.
         * If we are doing for the first time we'll scan only tail of the LRU list
         * otherwise we scan the whole LRU list.
         */
        freed = buf_LRU_scan_and_free_block(buf_pool, n_iterations > 0);
        if (!freed && n_iterations == 0) {
            /* Tell other threads that there is no point in scanning the LRU list.
               This flag is set to TRUE again when we flush a batch from this buffer pool. */
            buf_pool->try_LRU_scan = FALSE;
            os_rmb;
        }
    }

    if (freed) {
        goto loop;
    }

    if (n_iterations > 20) {
        LOGGER_WARN(LOGGER,
            "Difficult to find free blocks in the buffer pool(%lu search iterations), "
            "%lu failed attempts to flush a page! "
            "It is also possible that in your fsync is very slow.",
            n_iterations, flush_failures);
        //os_event_set(lock_sys->timeout_event);
    }

    /* If we have scanned the whole LRU and still are unable to find a free block
       then we should sleep here to let the page_cleaner do an LRU batch for us. */

    if (!srv_read_only_mode) {
        //os_event_set(buf_flush_event);
    }

    if (n_iterations > 1) {
        os_thread_sleep(10000);
    }

    /* No free block was found: try to flush the LRU list.
     * This call will flush one page from the LRU and put it on the free list.
     * That means that the free block is up for grabs for all user threads.
     */

    if (!buf_flush_single_page_from_LRU(buf_pool)) {
        ++flush_failures;
    }

    srv_stats.buf_pool_wait_free.add(n_iterations, 1);

    n_iterations++;

    goto loop;
}

// Moves a block to the start of the LRU list.
void buf_LRU_make_block_young(buf_page_t *bpage)
{
    buf_pool_t *buf_pool = buf_pool_from_bpage(bpage);

    ut_ad(mutex_own(&buf_pool->LRU_list_mutex));

    if (bpage->old) {
        buf_pool->stat.n_pages_made_young++;
    }

    buf_LRU_remove_block(bpage);
    buf_LRU_add_block(bpage, false);
}

