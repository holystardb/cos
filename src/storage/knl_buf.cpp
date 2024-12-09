#include "knl_buf.h"
#include "cm_thread.h"
#include "cm_log.h"
#include "cm_memory.h"
#include "cm_timer.h"
#include "knl_dblwrite.h"
#include "knl_hash_table.h"
#include "knl_mtr.h"
#include "knl_buf_lru.h"
#include "knl_page.h"
#include "knl_buf_lru.h"

/** The buffer pools of the database */
buf_pool_t *buf_pool_ptr;
uint32      buf_pool_instances;


static void buf_block_init_low(buf_block_t *block);

// Inits a page to the buffer buf_pool
static void buf_page_init(buf_pool_t* buf_pool, const page_id_t& page_id,
    const page_size_t& page_size, buf_block_t* block);

static inline void buf_block_lock(buf_block_t* block, rw_lock_type_t lock_type, mtr_t* mtr);


// Returns the control block of a file page, NULL if not found.
// return block, NULL if not found */
inline buf_page_t *buf_page_hash_get_low(buf_pool_t* buf_pool, const page_id_t& page_id)
{
    buf_page_t* bpage;

#ifdef UNIV_DEBUG
    rw_lock_t *hash_lock = hash_get_lock(buf_pool->page_hash, page_id.fold());
    ut_ad(rw_lock_own(hash_lock, RW_LOCK_EXCLUSIVE) || rw_lock_own(hash_lock, RW_LOCK_SHARED));
#endif /* UNIV_DEBUG */

    /* Look for the page in the hash table */
    HASH_SEARCH(hash, buf_pool->page_hash, page_id.fold(), buf_page_t*, bpage,
                ut_ad(bpage->in_page_hash && buf_page_in_file(bpage)),
                page_id.equals_to(bpage->id));
    if (bpage) {
        ut_a(buf_page_in_file(bpage));
        ut_ad(bpage->in_page_hash);
        ut_ad(buf_pool_from_bpage(bpage) == buf_pool);
    }

    return (bpage);
}


buf_page_t* buf_page_hash_get_locked(
    buf_pool_t* buf_pool,
    const page_id_t& page_id,
    rw_lock_t** lock, /*!< in/out: lock of the page hash acquired if bpage is found. NULL otherwise.
                           If NULL is passed then the hash_lock is released by this function */
    uint32 lock_mode) /*!< in: RW_LOCK_EXCLUSIVE or RW_LOCK_SHARED. Ignored if lock == NULL */
{
    buf_page_t* bpage = NULL;
    rw_lock_t*  hash_lock;
    uint32      mode = RW_LOCK_SHARED;

    if (lock != NULL) {
        ut_ad(lock_mode == RW_LOCK_EXCLUSIVE || lock_mode == RW_LOCK_SHARED);
        *lock = NULL;
        mode = lock_mode;
    }

    hash_lock = hash_get_lock(buf_pool->page_hash, page_id.fold());

#ifdef UNIV_SYNC_DEBUG
    ut_ad(!rw_lock_own(hash_lock, RW_LOCK_EXCLUSIVE) && !rw_lock_own(hash_lock, RW_LOCK_SHARED));
#endif /* UNIV_SYNC_DEBUG */

    if (mode == RW_LOCK_SHARED) {
        rw_lock_s_lock(hash_lock);
    } else {
        rw_lock_x_lock(hash_lock);
    }

    bpage = buf_page_hash_get_low(buf_pool, page_id);
    if (bpage == NULL) {
        goto unlock_and_exit;
    }

    ut_ad(buf_page_in_file(bpage));
    ut_ad(bpage->id.equals_to(page_id));

    if (lock == NULL) {
        // The caller wants us to release the page_hash lock
        goto unlock_and_exit;
    } else {
        // To be released by the caller
        *lock = hash_lock;
        goto exit;
    }

unlock_and_exit:

    if (mode == RW_LOCK_SHARED) {
        rw_lock_s_unlock(hash_lock);
    } else {
        rw_lock_x_unlock(hash_lock);
    }

exit:

    return bpage;
}



inline bool32 buf_page_in_file(const buf_page_t *bpage) /*!< in: pointer to control block */
{
    switch (buf_page_get_state(bpage)) {
    case BUF_BLOCK_POOL_WATCH:
        ut_error;
        break;
    case BUF_BLOCK_ZIP_PAGE:
    case BUF_BLOCK_ZIP_DIRTY:
    case BUF_BLOCK_FILE_PAGE:
        return (TRUE);
    case BUF_BLOCK_NOT_USED:
    case BUF_BLOCK_READY_FOR_USE:
    case BUF_BLOCK_MEMORY:
    case BUF_BLOCK_REMOVE_HASH:
        break;
    }

    return (FALSE);
}

buf_io_fix_t buf_page_get_io_fix_unlocked(const buf_page_t *bpage)
{
    ut_ad(bpage != NULL);

    buf_io_fix_t io_fix = bpage->io_fix;

#ifdef UNIV_DEBUG
    switch (io_fix) {
    case BUF_IO_NONE:
    case BUF_IO_READ:
    case BUF_IO_WRITE:
    case BUF_IO_PIN:
      return (io_fix);
    }
    ut_error;
#endif  /* UNIV_DEBUG */

    return (io_fix);
}

// Determine if a buffer block can be relocated in memory.
// The block can be dirty, but it must not be I/O-fixed or bufferfixed.
inline bool32 buf_page_can_relocate(buf_page_t* bpage)
{
    ut_ad(mutex_own(buf_page_get_mutex(bpage)));
    ut_ad(buf_page_in_file(bpage));
    ut_ad(bpage->in_LRU_list);

    bpage->touch_number = bpage->touch_number / BUF_PAGE_AGE_DEC_FACTOR;

    if (buf_page_get_io_fix(bpage) != BUF_IO_NONE ||
        bpage->buf_fix_count > 0 ||
        bpage->touch_number > 0) {
        return FALSE;
    }

    return TRUE;
}

inline buf_io_fix_t buf_page_get_io_fix(const buf_page_t *bpage) /*!< in: pointer to the control block */
{
    ut_ad(mutex_own(buf_page_get_mutex(bpage)));
    return buf_page_get_io_fix_unlocked(bpage);
}

// Gets the mutex of a block
inline mutex_t* buf_page_get_mutex(const buf_page_t *bpage) /*!< in: pointer to control block */
{
    buf_pool_t *buf_pool = buf_pool_from_bpage(bpage);

    switch (buf_page_get_state(bpage)) {
    case BUF_BLOCK_POOL_WATCH:
      ut_error;
    case BUF_BLOCK_ZIP_PAGE:
    case BUF_BLOCK_ZIP_DIRTY:
      //return (&buf_pool->zip_mutex);
    default:
      return (&((buf_block_t *)bpage)->mutex);
    }
}

// Calculates a folded value of a file page address to use in the page hash table
uint32 buf_page_address_fold(const page_id_t *page_id)
{
    return (page_id->space_id() << 20) + page_id->space_id() + page_id->page_no();
}


/*
  Function which inits a page for read to the buffer buf_pool.
  If the page is
     (1) already in buf_pool, or
     (2) if we specify to read only ibuf pages and the page is not an ibuf page, or
     (3) if the space is deleted or being deleted,
  then this function does nothing.
  Sets the io_fix flag to BUF_IO_READ and sets a non-recursive exclusive lock
  on the buffer frame. The io-handler must take care that the flag is cleared
  and the lock released later.
  return pointer to the block or NULL 
*/
static inline buf_page_t* buf_page_init_for_read(
    buf_pool_t* buf_pool,
    status_t*   err, // out: CM_SUCCESS or DB_TABLESPACE_DELETED
    const page_id_t& page_id, const page_size_t& page_size)
{
    buf_block_t* block;
    buf_page_t*  bpage;
    rw_lock_t*   hash_lock;

    *err = CM_SUCCESS;

    ut_ad(buf_pool == buf_pool_from_page_id(page_id));

    block = buf_LRU_get_free_block(buf_pool);
    ut_ad(block);
    ut_ad(buf_pool_from_block(block) == buf_pool);

    hash_lock = buf_page_hash_lock_get(buf_pool, page_id);

    rw_lock_x_lock(hash_lock);

    bpage = buf_page_hash_get_low(buf_pool, page_id);
    if (bpage) {
        // The page is already in the buffer pool
        rw_lock_x_unlock(hash_lock);
        //
        if (block) {
            mutex_enter(&block->mutex);
            buf_LRU_insert_block_to_free_list(buf_pool, block);
            mutex_exit(&block->mutex);
        }
        return NULL;
    }
    bpage = &block->page;

    mutex_enter(&block->mutex);

    buf_page_init(buf_pool, page_id, page_size, block);

    // We are using the hash_lock for protection.
    // This is safe because no other thread can lookup the block from the page hashtable yet.
    buf_page_set_io_fix(bpage, BUF_IO_READ);

    rw_lock_x_unlock(hash_lock);

    /* We set a pass-type x-lock on the frame because then the same thread
       which called for the read operation (and is running now at this point of code) can wait
       for the read to complete by waiting for the x-lock on the frame;
       if the x-lock were recursive, the same thread would illegally get the x-lock before the page
       read is completed.  The x-lock is cleared by the io-handler thread. */
    rw_lock_x_lock_gen(&block->rw_lock, BUF_IO_READ);

    mutex_exit(&block->mutex);

    buf_pool->n_pend_reads++;

    return bpage;
}

inline bool32 buf_page_io_complete(buf_page_t* bpage, buf_io_fix_t io_type, bool32 evict)
{
    mutex_t *block_mutex;
    buf_pool_t *buf_pool = buf_pool_from_bpage(bpage);

    ut_a(buf_page_in_file(bpage));

    switch (io_type) {
    case BUF_IO_READ: {
        byte* frame = ((buf_block_t*)bpage)->frame;
        uint32 read_page_no = mach_read_from_4(frame + FIL_PAGE_OFFSET);
        uint32 read_space_id = mach_read_from_4(frame + FIL_PAGE_SPACE);
        bpage->is_resident = (mach_read_from_2(frame + FIL_PAGE_TYPE) & FIL_PAGE_TYPE_RESIDENT_FLAG);

        block_mutex = buf_page_get_mutex(bpage);
        mutex_enter(block_mutex);
        buf_page_set_io_fix(bpage, BUF_IO_NONE);
        rw_lock_x_unlock_gen(&((buf_block_t *)bpage)->rw_lock, BUF_IO_READ);
        mutex_exit(block_mutex);

        // The block must be put to the LRU list, to the begining of LRU list
        if (UNLIKELY(bpage->is_resident)) {
            mutex_enter(&buf_pool->LRU_list_mutex);
            buf_LRU_insert_block_to_lru_list(buf_pool, bpage);
            mutex_exit(&buf_pool->LRU_list_mutex);
        }

        ut_ad(buf_pool->n_pend_reads > 0);
        //buf_pool->n_pend_reads.fetch_sub(1);
        //buf_pool->stat.n_pages_read.fetch_add(1);

        break;
    }
    case BUF_IO_WRITE: {
        /* Write means a flush operation: call the completion routine in the flush system */
        //buf_flush_write_complete(bpage);

        block_mutex = buf_page_get_mutex(bpage);
        mutex_enter(block_mutex);
        rw_lock_s_unlock_gen(&((buf_block_t *)bpage)->rw_lock, BUF_IO_WRITE);
        mutex_exit(block_mutex);
        //buf_pool->stat.n_pages_written.fetch_add(1);

        break;
    }
    default:
        ut_error;
    }

    return TRUE;
}


// Unfixes the page, unlatches the page, removes it from page_hash and removes it from LRU.
static void buf_read_page_handle_error(buf_pool_t* buf_pool, buf_page_t* bpage)
{
    ut_ad(buf_pool == buf_pool_from_bpage(bpage));

    // 1 remove from LRU list
    mutex_enter(&buf_pool->LRU_list_mutex);
    buf_LRU_remove_block_from_lru_list(buf_pool, bpage);
    mutex_exit(&buf_pool->LRU_list_mutex);

    // 2 unfix and release lock on the bpage

    mutex_enter(buf_page_get_mutex(bpage));
    ut_ad(buf_page_get_io_fix(bpage) == BUF_IO_READ);
    ut_ad(bpage->buf_fix_count == 0);
    // Set BUF_IO_NONE before we remove the block from LRU list
    buf_page_set_io_fix(bpage, BUF_IO_NONE);
    //
    rw_lock_x_unlock_gen(&((buf_block_t *)bpage)->rw_lock, BUF_IO_READ);
    mutex_exit(buf_page_get_mutex(bpage));

    // 3 remove page from page_hash and insert into free list
    buf_LRU_free_one_page(bpage);

    ut_ad(buf_pool->n_pend_reads > 0);
    //buf_pool->n_pend_reads.fetch_sub(1);
}

static status_t buf_read_page_callback(int32 code, os_aio_slot_t* slot)
{
    buf_page_t* page = (buf_page_t *)slot->message1;

    if (code != OS_FILE_IO_COMPLETION) {
        char err_info[CM_ERR_MSG_MAX_LEN];
        os_file_get_error_desc_by_err(code, err_info, CM_ERR_MSG_MAX_LEN);
        LOGGER_FATAL(LOGGER, LOG_MODULE_BUFFERPOOL,
            "buf_read_page: fatal error occurred, error = %d err desc = %s, service exited",
            slot->ret, err_info);
        ut_error;
    }

    if (page) {
        buf_page_io_complete(page, BUF_IO_READ, FALSE);
    }

    return CM_SUCCESS;
}

// Low-level function which reads a page asynchronously
// from a file to the buffer buf_pool if it is not already there, in which case does nothing.
// Sets the io_fix flag and sets an exclusive lock on the buffer frame.
// The flag is cleared and the x-lock released by an i/o-handler thread.
// return:
//   1 if a read request was queued,
//   0 if the page already resided in buf_pool, or if the tablespace does not exist or is being dropped
static inline bool32 buf_read_page_low(
    buf_pool_t* buf_pool,
    status_t* err, // out: CM_SUCCESS or DB_TABLESPACE_DELETED if we are trying
                   //      to read from a non-existent tablespace,
                   //      or a tablespace which is just now being dropped
    bool32 sync, // in: true if synchronous aio is desired
    const page_id_t& page_id, const page_size_t& page_size)
{
    buf_page_t* bpage;

    *err = CM_SUCCESS;

    /* The following call will also check if the tablespace does not exist or is being dropped;
       if we succeed in initing the page in the buffer pool for read,
       then DISCARD cannot proceed until the read has completed */
    bpage = buf_page_init_for_read(buf_pool, err, page_id, page_size);
    if (bpage == NULL) {
        // The page is already in the buffer pool
        return TRUE;
    }

    ut_ad(buf_page_in_file(bpage));
    ut_a(buf_page_get_state(bpage) == BUF_BLOCK_FILE_PAGE);

    *err = fil_read(sync, page_id, page_size, page_size.physical(),
        (void *)((buf_block_t*)bpage)->frame, buf_read_page_callback, bpage);
    if (*err != CM_SUCCESS) {
        if (*err == ERR_TABLESPACE_DELETED) {
            buf_read_page_handle_error(buf_pool, bpage);
            return FALSE;
        }
        ut_error;
    }

    if (sync) {
        // The i/o is already completed when we arrive from fil_read
        buf_page_io_complete(bpage, BUF_IO_READ, FALSE);
    }

    return TRUE;
}

// High-level function which reads a page asynchronously
// from a file to the buffer buf_pool if it is not already there.
// Sets the io_fix flag and sets an exclusive lock on the buffer frame.
// The flag is cleared and the x-lock released by the i/o-handler thread.
// return TRUE if page has been read in, FALSE in case of failure */
static inline bool32 buf_read_page(buf_pool_t* buf_pool, const page_id_t& page_id, const page_size_t& page_size)
{
    bool32    ret;
    status_t  err;

    // We do the i/o in the synchronous aio mode to save thread switches: hence TRUE
    ret = buf_read_page_low(buf_pool, &err, TRUE, page_id, page_size);
    srv_stats.buf_pool_reads.add(1);
    if (err == ERR_TABLESPACE_DELETED) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_BUFFERPOOL,
            "Error: trying to access tablespace %lu page no. %lu,\n"
            "but the tablespace does not exist or is just being dropped.\n",
            page_id.space_id(), page_id.page_no());
    }

    return ret;
}

static bool32 buf_block_wait_complete_io(buf_block_t* block, buf_io_fix_t io_fix)
{
    buf_io_fix_t tmp_io_fix;
    mutex_t* block_mutex = buf_page_get_mutex(&block->page);

    for (;;) {
        mutex_enter(block_mutex);
        tmp_io_fix = buf_page_get_io_fix(&block->page);
        mutex_exit(block_mutex);

        if (tmp_io_fix == io_fix) {
            // wait by temporaly s-latch
            rw_lock_s_lock(&(block->rw_lock));
            rw_lock_s_unlock(&(block->rw_lock));
        } else {
            break;
        }
    }

    return TRUE;
}

static inline void buf_page_update_touch_number(buf_page_t* bpage)
{
    date_t now_us = g_timer()->now_us;
    if (now_us < buf_page_is_accessed(bpage) + BUF_PAGE_ACCESS_WINDOW) {
        return;
    }

    switch (((buf_block_t *)bpage)->get_page_type()) {
    case FIL_PAGE_TYPE_FSP_HDR:
    case FIL_PAGE_TYPE_XDES:
    case FIL_PAGE_TYPE_INODE:
    case FIL_PAGE_TYPE_TRX_SYS:
    case FIL_PAGE_TYPE_SYSAUX:
    case FIL_PAGE_TYPE_UNDO_LOG:
    case FIL_PAGE_TYPE_HEAP_FSM:
    case FIL_PAGE_TYPE_BTREE_NONLEAF:
        bpage->touch_number += BUF_HOT_PAGE_TCH;
        break;
    default:
        bpage->touch_number++;
        break;
    }

    buf_page_set_accessed(bpage, now_us);
}

inline buf_block_t *buf_page_get_gen(const page_id_t& page_id, const page_size_t& page_size,
    rw_lock_type_t rw_latch, buf_block_t* guess, Page_fetch mode, mtr_t* mtr)
{
    buf_block_t* block;
    bool32       must_read;
    rw_lock_t*   hash_lock;
    uint32       retries = 0;
    buf_pool_t*  buf_pool;

    ut_ad((rw_latch == RW_S_LATCH) || (rw_latch == RW_X_LATCH) || (rw_latch == RW_NO_LATCH));

    buf_pool = buf_pool_from_page_id(page_id);
    buf_pool->stat.n_page_gets++;

    hash_lock = buf_page_hash_lock_get(buf_pool, page_id);

retry:

    block = guess;

    // see if the block is in the buffer pool already
    rw_lock_s_lock(hash_lock);

    if (block) {
        if (!page_id.equals_to(block->page.id) || buf_block_get_state(block) != BUF_BLOCK_FILE_PAGE) {
            // Our guess was bogus or things have changed since.
            block = guess = NULL;
        }
    }

    if (block == NULL) {
        block = (buf_block_t*)buf_page_hash_get_low(buf_pool, page_id);
    }

    if (block == NULL) {
        // Didn't find it in the buffer pool:
        // needs to be read from file We'll have to initialize a new buffer.
        rw_lock_s_unlock(hash_lock);

        if (mode == Page_fetch::IF_IN_POOL || mode == Page_fetch::PEEK_IF_IN_POOL) {
            return NULL;
        }

        if (!buf_read_page(buf_pool, page_id, page_size)) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_BUFFERPOOL,
                "Error: Unable to read tablespace %lu page no %lu into the buffer pool\n"
                "The most probable cause of this error may be that the"
                " table has been corrupted. Aborting...\n",
                page_id.space_id(), page_id.page_no());
            ut_error;
        }
        retries++;
        goto retry;
    }

    /* We can release hash_lock after we acquire block_mutex to
       make sure that no state change takes place. */

    mutex_enter(&block->mutex, NULL);

    // Now safe to release page_hash mutex
    rw_lock_s_unlock(hash_lock);

    // pin buffer
    buf_page_fix(&block->page);
    must_read = (buf_page_get_io_fix(&block->page) == BUF_IO_READ);

    mutex_exit(&block->mutex);

    if (must_read) {
        // The page is being read to buffer pool, Let us wait until the read operation completes
        buf_block_wait_complete_io(block, BUF_IO_READ);
    }

    if (!block->page.is_resident && mode != Page_fetch::PEEK_IF_IN_POOL) {
        buf_page_update_touch_number(&block->page);
    }

    buf_block_lock(block, rw_latch, mtr);

    // prefetch
    //if (!block->page.is_resident && mode != BUF_PEEK_IF_IN_POOL && !access_time) {
        // In the case of a first access, try to apply linear read-ahead
        //buf_read_ahead_linear(page_id, page_size, ibuf_inside(mtr));
    //}

    return block;
}

void buf_page_init_low(buf_page_t* bpage) /*!< in: block to init */
{
    bpage->flush_type = BUF_FLUSH_LRU;
    bpage->io_fix = BUF_IO_NONE;
    bpage->buf_fix_count = 0;
    bpage->is_resident = FALSE;
    bpage->touch_number = 0;
    bpage->access_time = 0;
    bpage->newest_modification = 0;
    bpage->recovery_lsn = 0;
    HASH_INVALIDATE(bpage, hash);

    ut_d(bpage->file_page_was_freed = FALSE);
}


// Inits a page to the buffer buf_pool.
// The block pointer must be private to the calling thread at the start of this function.
static void buf_page_init(buf_pool_t* buf_pool, const page_id_t& page_id,
                          const page_size_t& page_size, buf_block_t* block)
{
    buf_page_t *hash_page;

    ut_ad(buf_pool == buf_pool_from_page_id(page_id));
    ut_ad(mutex_own(buf_page_get_mutex(&block->page)));
    ut_a(buf_block_get_state(block) != BUF_BLOCK_FILE_PAGE);
    ut_ad(rw_lock_own(buf_page_hash_lock_get(buf_pool, page_id), RW_LOCK_EXCLUSIVE));

    // Set the state of the block
    buf_block_set_state(block, BUF_BLOCK_FILE_PAGE);
    block->page.id.copy_from(page_id);

    buf_block_init_low(block);
    buf_page_init_low(&block->page);

    // Insert into the hash table of file pages
    hash_page = buf_page_hash_get_low(buf_pool, page_id);
    if (hash_page == NULL) {
        // Block not found in hash table
    } else {
        LOGGER_ERROR(LOGGER, LOG_MODULE_BUFFERPOOL,
            "buf_page_init: Page (space %lu, page %lu) already found in the hash table: %p, %p",
            page_id.space_id(), page_id.page_no(), (const void*)hash_page, (const void*)block);
        ut_error;
    }

    ut_ad(!block->page.in_page_hash);
    ut_d(block->page.in_page_hash = TRUE);

    block->page.id.copy_from(page_id);
    block->page.size.copy_from(page_size);
    HASH_INSERT(buf_page_t, hash, buf_pool->page_hash, page_id.fold(), &block->page);
}

inline buf_block_t* buf_page_create(const page_id_t& page_id, const page_size_t& page_size,
    rw_lock_type_t rw_latch, Page_fetch mode, mtr_t* mtr)
{
    buf_block_t* block;
    buf_block_t* free_block = NULL;
    buf_pool_t* buf_pool = buf_pool_from_page_id(page_id);
    rw_lock_t* hash_lock;

    ut_ad(mtr->is_active());

    free_block = buf_LRU_get_free_block(buf_pool);

    hash_lock = buf_page_hash_lock_get(buf_pool, page_id);
    rw_lock_x_lock(hash_lock);

    block = (buf_block_t *)buf_page_hash_get_low(buf_pool, page_id);
    if (block && buf_page_in_file(&block->page)) {
        ut_d(block->page.file_page_was_freed = FALSE);

        // Page can be found in buf_pool
        rw_lock_x_unlock(hash_lock);

        buf_block_free(buf_pool, free_block);
        return buf_page_get_gen(page_id, page_size, rw_latch, NULL, mode, mtr);
    }

    // If we get here, the page was not in buf_pool: init it there

    LOGGER_DEBUG(LOGGER, LOG_MODULE_BUFFERPOOL,
        "buf_page_create: create page %lu : %lu", page_id.space_id(), page_id.page_no());

    block = free_block;

    mutex_enter(&block->mutex, NULL);

    // init and insert page to buf_pool->page_hash
    buf_page_init(buf_pool, page_id, page_size, block);

    rw_lock_x_unlock(hash_lock);

    if (UNLIKELY(mode == Page_fetch::RESIDENT)) {
        block->page.is_resident = TRUE;
    }

    buf_page_fix(&block->page);
    buf_page_set_accessed(&block->page, g_timer()->now_us);

    mutex_exit(&block->mutex);

    // rw_lock
    buf_block_lock(block, rw_latch, mtr);

    // The block must be put to the LRU list
    if (UNLIKELY(!block->page.is_resident)) {
        mutex_enter(&buf_pool->LRU_list_mutex, NULL);
        buf_LRU_insert_block_to_lru_list(buf_pool, &block->page);
        mutex_exit(&buf_pool->LRU_list_mutex);
    }

    atomic32_inc(&buf_pool->stat.n_pages_created);
    //buf_pool->stat.n_pages_created.fetch_add(1);

    buf_frame_t* frame = buf_block_get_frame(block);
    memset(frame + FIL_PAGE_PREV, FIL_NULL, 4);
    memset(frame + FIL_PAGE_NEXT, FIL_NULL, 4);
    if (UNLIKELY(mode == Page_fetch::RESIDENT)) {
        mach_write_to_2(frame + FIL_PAGE_TYPE, FIL_PAGE_TYPE_ALLOCATED | FIL_PAGE_TYPE_RESIDENT_FLAG);
    } else {
        mach_write_to_2(frame + FIL_PAGE_TYPE, FIL_PAGE_TYPE_ALLOCATED);
    }
    mach_write_to_4(frame + FIL_PAGE_SPACE, page_id.space_id());
    mach_write_to_4(frame + FIL_PAGE_OFFSET, page_id.page_no());
    memset(frame + FIL_PAGE_FILE_FLUSH_LSN, 0, 8);

    return block;
}

inline date_t buf_page_is_accessed(const buf_page_t* bpage)
{
    ut_ad(buf_page_in_file(bpage));
    return bpage->access_time;
}

inline void buf_page_set_accessed(buf_page_t* bpage, date_t access_time)
{
    ut_ad(buf_page_in_file(bpage));

    bpage->access_time = access_time;
}

// Sets the io_fix state of a block. */
inline void buf_page_set_io_fix(buf_page_t *bpage, buf_io_fix_t io_fix)
{
#ifdef UNIV_DEBUG
    //buf_pool_t *buf_pool = buf_pool_from_bpage(bpage);
    //ut_ad(mutex_own(&buf_pool->mutex));
#endif
    ut_ad(mutex_own(buf_page_get_mutex(bpage)));

    bpage->io_fix = io_fix;
    ut_ad(buf_page_get_io_fix(bpage) == io_fix);
}


inline buf_page_state_t buf_page_get_state(const buf_page_t *bpage)
{
    buf_page_state_t state = bpage->state;

#ifdef UNIV_DEBUG
    switch (state) {
    case BUF_BLOCK_POOL_WATCH:
    case BUF_BLOCK_ZIP_PAGE:
    case BUF_BLOCK_ZIP_DIRTY:
    case BUF_BLOCK_NOT_USED:
    case BUF_BLOCK_READY_FOR_USE:
    case BUF_BLOCK_FILE_PAGE:
    case BUF_BLOCK_MEMORY:
    case BUF_BLOCK_REMOVE_HASH:
        break;
    default:
        ut_error;
    }
#endif /* UNIV_DEBUG */

    return(state);
}

inline void buf_page_set_state(buf_page_t *bpage, buf_page_state_t state)
{
#ifdef UNIV_DEBUG
    buf_page_state_t old_state	= buf_page_get_state(bpage);

    switch (old_state) {
    case BUF_BLOCK_POOL_WATCH:
        ut_error;
        break;
    case BUF_BLOCK_ZIP_PAGE:
        ut_a(state == BUF_BLOCK_ZIP_DIRTY);
        break;
    case BUF_BLOCK_ZIP_DIRTY:
        ut_a(state == BUF_BLOCK_ZIP_PAGE);
        break;
    case BUF_BLOCK_NOT_USED:
        ut_a(state == BUF_BLOCK_READY_FOR_USE);
        break;
    case BUF_BLOCK_READY_FOR_USE:
        ut_a(state == BUF_BLOCK_MEMORY
             || state == BUF_BLOCK_FILE_PAGE
             || state == BUF_BLOCK_NOT_USED);
        break;
    case BUF_BLOCK_MEMORY:
        ut_a(state == BUF_BLOCK_NOT_USED);
        break;
    case BUF_BLOCK_FILE_PAGE:
        ut_a(state == BUF_BLOCK_NOT_USED
             || state == BUF_BLOCK_REMOVE_HASH);
        break;
    case BUF_BLOCK_REMOVE_HASH:
        ut_a(state == BUF_BLOCK_MEMORY);
        break;
    }
#endif /* UNIV_DEBUG */

    bpage->state = state;
    ut_ad(buf_page_get_state(bpage) == state);
}

inline void buf_block_set_state(buf_block_t *block, buf_page_state_t state)
{
    buf_page_set_state(&block->page, state);
}


// Gets the youngest modification log sequence number for a frame.
// Returns zero if not file page or no modification occurred yet.
inline lsn_t buf_page_get_newest_modification(const buf_page_t* bpage)
{
    lsn_t lsn;
    mutex_t* block_mutex = buf_page_get_mutex(bpage);

    mutex_enter(block_mutex);

    if (buf_page_in_file(bpage)) {
        lsn = bpage->newest_modification;
    } else {
        lsn = 0;
    }

    mutex_exit(block_mutex);

    return(lsn);
}

// Increments the bufferfix count
inline uint32 buf_page_fix(buf_page_t* bpage)
{
    uint32 count = atomic32_inc(&bpage->buf_fix_count);
    return count;
}

inline void buf_page_unfix(buf_page_t* bpage)
{
    ut_a(bpage->buf_fix_count > 0);
    atomic32_dec(&bpage->buf_fix_count);
}

inline void buf_block_lock(buf_block_t* block, rw_lock_type_t lock_type, mtr_t* mtr)
{
    if (lock_type == RW_S_LATCH) {
        rw_lock_s_lock(&(block->rw_lock));
        mtr_memo_push(mtr, block, MTR_MEMO_PAGE_S_FIX);
    } else if (lock_type == RW_X_LATCH) {
        rw_lock_x_lock(&(block->rw_lock));
        mtr_memo_push(mtr, block, MTR_MEMO_PAGE_X_FIX);
    } else {
        mtr_memo_push(mtr, block, MTR_MEMO_BUF_FIX);
    }
}

inline void buf_block_lock_and_fix(buf_block_t* block, rw_lock_type_t lock_type, mtr_t* mtr)
{
    buf_page_fix(&block->page);
    buf_block_lock(block, lock_type, mtr);
}

inline void buf_block_unlock(buf_block_t* block, rw_lock_type_t lock_type, mtr_t* mtr)
{
    if (lock_type == RW_S_LATCH) {
        mtr_memo_release(mtr, block, MTR_MEMO_PAGE_S_FIX);
    } else if (lock_type == RW_X_LATCH) {
        mtr_memo_release(mtr, block, MTR_MEMO_PAGE_X_FIX);
    } else {
        mtr_memo_release(mtr, block, MTR_MEMO_BUF_FIX);
    }
}

inline buf_block_t *buf_block_alloc(buf_pool_t *buf_pool)
{
    buf_block_t *block;
    uint32 index;
    static uint32 buf_pool_index;

    if (buf_pool == NULL) {
        /* We are allocating memory from any buffer pool,
           ensure we spread the grace on all buffer pool instances. */
        index = buf_pool_index++ % buf_pool_instances;
        buf_pool = &buf_pool_ptr[index];
    }

    block = buf_LRU_get_free_block(buf_pool);

    buf_block_set_state(block, BUF_BLOCK_MEMORY);

    return (block);
}

inline void buf_block_free(buf_pool_t* buf_pool, buf_block_t* block) // in, own: block to be freed
{
    ut_a(buf_block_get_state(block) != BUF_BLOCK_FILE_PAGE);
    buf_LRU_insert_block_to_free_list(buf_pool, block);
}

inline buf_page_state_t buf_block_get_state(const buf_block_t *block)
{
    return (buf_page_get_state(&block->page));
}

// Gets the block to whose frame the pointer is pointing to if found in this buffer pool instance.
buf_block_t* buf_block_align_instance(buf_pool_t* buf_pool, /*!< in: buffer in which the block resides */
    const byte* ptr) /*!< in: pointer to a frame */
{
    uint32 offs;
    if (ptr < buf_pool->blocks->frame) {
        return NULL;
    }

    offs = (uint32)((ptr - buf_pool->blocks->frame) >> UNIV_PAGE_SIZE_SHIFT_DEF);
    if (offs < buf_pool->size) {
        buf_block_t* block = &buf_pool->blocks[offs];

        /* The function buf_chunk_init() invokes
        buf_block_init() so that block[n].frame == block->frame + n * UNIV_PAGE_SIZE.  Check it. */
        ut_ad((char *)block->frame == (char *)page_align((void *)ptr));

        return(block);
    }

    return NULL;
}


// Gets the block to whose frame the pointer is pointing to.
buf_block_t* buf_block_align(const byte* ptr) /*!< in: pointer to a frame */
{
    uint32 i;

    for (i = 0; i < buf_pool_instances; i++) {
        buf_block_t* block = buf_block_align_instance(&buf_pool_ptr[i], ptr);
        if (block) {
            return(block);
        }
    }

    /* The block should always be found. */
    ut_error;
    return(NULL);
}


static void buf_block_init_low(buf_block_t *block)
{
    //block->index = NULL;
    //block->made_dirty_with_no_latch = false;

    block->n_hash_helps = 0;
    block->n_fields = 1;
    block->n_bytes = 0;
    block->left_side = TRUE;

}

/** Initializes a buffer control block when the buf_pool is created. */
static void buf_block_init(
    buf_pool_t *buf_pool, /*!< in: buffer pool instance */
    buf_block_t *block,   /*!< in: pointer to control block */
    byte *frame)          /*!< in: pointer to buffer frame */
{
    block->frame = frame;

    block->page.buf_pool_index = buf_pool - buf_pool_ptr;
    block->page.state = BUF_BLOCK_NOT_USED;
    block->page.buf_fix_count = 0;
    block->page.io_fix = BUF_IO_NONE;
    //block->page.flush_observer = NULL;

    block->modify_clock = 0;

    //ut_d(block->page.file_page_was_freed = FALSE);

    //block->index = NULL;
    //block->made_dirty_with_no_latch = false;

    ut_d(block->page.in_page_hash = FALSE);
    ut_d(block->page.in_flush_list = FALSE);
    ut_d(block->page.in_free_list = FALSE);
    ut_d(block->page.in_LRU_list = FALSE);
    //ut_d(block->in_unzip_LRU_list = FALSE);
    //ut_d(block->in_withdraw_list = FALSE);

    mutex_create(&block->mutex);

    rw_lock_create(&block->rw_lock);

    //ut_d(rw_lock_create(buf_block_debug_latch_key, &block->debug_latch, SYNC_NO_ORDER_CHECK));

    block->rw_lock.is_block_lock = 1;

    ut_ad(rw_lock_validate(&(block->rw_lock)));
}

static bool32 buf_pool_fill_block(buf_pool_t *buf_pool, uint64 mem_size)
{
    buf_block_t *block;
    byte *frame;
    uint64 i;

    /* Round down to a multiple of page size, although it already should be. */
    mem_size = ut_2pow_round(mem_size, UNIV_PAGE_SIZE);
    /* Reserve space for the block descriptors. */
    buf_pool->mem_size = mem_size +
        ut_2pow_round((mem_size / UNIV_PAGE_SIZE) * sizeof(buf_block_t) + (UNIV_PAGE_SIZE - 1), UNIV_PAGE_SIZE);
    buf_pool->mem = (uchar *)os_mem_alloc_large(&buf_pool->mem_size);
    if (buf_pool->mem == NULL) {
        LOGGER_FATAL(LOGGER, LOG_MODULE_BUFFERPOOL, "can not alloc memory, size = %lu", buf_pool->mem_size);
        return FALSE;
    }

    /* Dump core without large memory buffers */
    if (buf_pool_should_madvise) {
        madvise_dont_dump((char *)buf_pool->mem, buf_pool->mem_size);
    }

    /* Allocate the block descriptors from the start of the memory block. */
    buf_pool->blocks = (buf_block_t *)buf_pool->mem;

    /* Align a pointer to the first frame.
     * Note that when os_large_page_size is smaller than UNIV_PAGE_SIZE,
     * we may allocate one fewer block than requested.
     * When it is bigger, we may allocate more blocks than requested.
     */
    frame = (byte *)ut_align_up(buf_pool->mem, UNIV_PAGE_SIZE);
    buf_pool->size = (uint32)(buf_pool->mem_size / UNIV_PAGE_SIZE - (frame != buf_pool->mem));

    /* Subtract the space needed for block descriptors. */
    {
        uint32 size = buf_pool->size;
        while (frame < (byte *)(buf_pool->blocks + size)) {
            frame += UNIV_PAGE_SIZE;
            size--;
        }
        buf_pool->size = size;
    }

    /* Init block structs and assign frames for them.
     * Then we assign the frames to the first blocks (we already mapped the memory above).
     */
    block = buf_pool->blocks;
    for (i = buf_pool->size; i--;) {
        buf_block_init(buf_pool, block, frame);
        //UNIV_MEM_INVALID(block->frame, UNIV_PAGE_SIZE);

        /* Add the block to the free list */
        UT_LIST_ADD_LAST(list_node, buf_pool->free_pages, block);

        ut_d(block->page.in_free_list = TRUE);
        ut_ad(buf_pool_from_block(block) == buf_pool);

        block++;
        frame += UNIV_PAGE_SIZE;
    }

    return TRUE;
}


// page_hash_lock_count: number of locks to protect buf_pool->page_hash
static void buf_pool_create_instance(buf_pool_t* buf_pool,
    uint64 buf_pool_size, uint32 instance_no, uint32 page_hash_lock_count, status_t* err)
{
    uint32 i;

    /* 1. Initialize general fields */
    mutex_create(&buf_pool->LRU_list_mutex);
    mutex_create(&buf_pool->free_list_mutex);
    mutex_create(&buf_pool->flush_state_mutex);

    UT_LIST_INIT(buf_pool->LRU);
    UT_LIST_INIT(buf_pool->free_pages);
    UT_LIST_INIT(buf_pool->flush_list);
    //UT_LIST_INIT(buf_pool->withdraw, &buf_page_t::list);
    buf_pool->withdraw_target = 0;

    if (!buf_pool_fill_block(buf_pool, buf_pool_size)) {
        *err = CM_ERROR;
        return;
    }

    buf_pool->instance_no = instance_no;
    buf_pool->curr_pool_size = buf_pool->size * UNIV_PAGE_SIZE;

    /* Number of locks protecting page_hash must be a power of two */
    ut_a(page_hash_lock_count != 0);
    ut_a(page_hash_lock_count <= MAX_PAGE_HASH_LOCKS);

    buf_pool->page_hash = HASH_TABLE_CREATE(2 * buf_pool->size, HASH_TABLE_SYNC_RW_LOCK, page_hash_lock_count);

    //buf_pool->page_hash->heap = ;
    //buf_pool->page_hash->heaps = ;
    buf_pool->page_hash_old = NULL;

    buf_pool->last_printout_time = current_monotonic_time();

    /* 2. Initialize flushing fields */
    mutex_create(&buf_pool->flush_list_mutex);
    for (i = BUF_FLUSH_LRU; i < BUF_FLUSH_N_TYPES; i++) {
      buf_pool->no_flush[i] = os_event_create(0);
    }

    //buf_pool->watch = (buf_page_t *)my_malloc(sizeof(*buf_pool->watch) * BUF_POOL_WATCH_SIZE);
    //for (i = 0; i < BUF_POOL_WATCH_SIZE; i++) {
    //    buf_pool->watch[i].buf_pool_index = buf_pool->instance_no;
    //}

    /* All fields are initialized by ut_zalloc_nokey(). */
    buf_pool->try_LRU_scan = TRUE;
    /* Dirty Page Tracking is disabled by default. */
    //buf_pool->track_page_lsn = LSN_MAX;
    buf_pool->max_lsn_io = 0;

    memset(&buf_pool->stat, 0x00, sizeof(buf_pool->stat));


    *err = CM_SUCCESS;
}

static void buf_pool_free_instance(buf_pool_t *buf_pool)
{
    buf_page_t *bpage;
    buf_page_t *prev_bpage = 0;

    mutex_destroy(&buf_pool->LRU_list_mutex);
    mutex_destroy(&buf_pool->free_list_mutex);
    mutex_destroy(&buf_pool->flush_state_mutex);
    mutex_destroy(&buf_pool->flush_list_mutex);

    for (bpage = UT_LIST_GET_LAST(buf_pool->LRU); bpage != NULL; bpage = prev_bpage) {
        prev_bpage = UT_LIST_GET_PREV(LRU_list_node, bpage);
        //buf_page_state state = buf_page_get_state(bpage);

        //ut_ad(buf_page_in_file(bpage));
        //ut_ad(bpage->in_LRU_list);

        //if (state != BUF_BLOCK_FILE_PAGE) {
        //  /* We must not have any dirty block except
        //  when doing a fast shutdown. */
        //  ut_ad(state == BUF_BLOCK_ZIP_PAGE || srv_fast_shutdown == 2);
        //  buf_page_free_descriptor(bpage);
        //}
    }

    buf_block_t *block = buf_pool->blocks;

    for (uint64 i = buf_pool->size; i--; block++) {
        //os_mutex_free(&block->mutex);
        //rw_lock_free(&block->lock);
        //ut_d(rw_lock_free(&block->debug_latch));
    }

    if (buf_pool_should_madvise) {
        madvise_dump((char *)buf_pool->mem, buf_pool->mem_size);
    }
    os_mem_free_large(buf_pool->mem, buf_pool->mem_size);


    //for (ulint i = BUF_FLUSH_LRU; i < BUF_FLUSH_N_TYPES; ++i) {
    //  os_event_destroy(buf_pool->no_flush[i]);
    //}

    //ha_clear(buf_pool->page_hash);
    HASH_TABLE_FREE(buf_pool->page_hash);
}

// Frees the buffer pool global data structures
static void buf_pool_free()
{
    my_free(buf_pool_ptr);
    buf_pool_ptr = nullptr;
}

uint32 buf_pool_get_instances()
{
    return buf_pool_instances;
}

status_t buf_pool_init(uint64 total_size, uint32 n_instances, uint32 page_hash_lock_count)
{
    const uint64 size = total_size / n_instances;

    ut_ad(n_instances > 0);
    ut_ad(n_instances <= MAX_BUFFER_POOLS);

    buf_pool_instances = n_instances;
    buf_pool_ptr = (buf_pool_t *)ut_malloc_zero(buf_pool_instances * sizeof(buf_pool_t));

    /* Magic number 8 is from empirical testing on a 4 socket x 10 Cores x 2 HT host.
       128G / 16 instances takes about 4 secs, compared to 10 secs without this optimisation.. */
    uint32 n_cores = 8;
    status_t errs[8], err = CM_SUCCESS;
    os_thread_t threads[8];

    for (uint32 i = 0; i < buf_pool_instances; /* no op */) {
        uint32 n = i + n_cores;

        if (n > buf_pool_instances) {
            n = buf_pool_instances;
        }

        for (uint32 id = i; id < n; ++id) {
            threads[id - i] = thread_start(buf_pool_create_instance,
                &buf_pool_ptr[id], size, id, page_hash_lock_count, &errs[id - i]);
        }
        for (uint32 id = i; id < n; ++id) {
            if (!os_thread_join(threads[id - i]) || errs[id] != CM_SUCCESS) {
                err = CM_ERROR;
            }
        }

        if (err != CM_SUCCESS) {
            for (uint32 id = 0; id < n; ++id) {
                buf_pool_free_instance(&buf_pool_ptr[id]);
            }
            buf_pool_free();
            return err;
        }

        /* Do the next block of instances */
        i = n;
    }

    //buf_LRU_old_ratio_update(100 * 3 / 8, FALSE);

    return CM_SUCCESS;
}

inline buf_pool_t* buf_pool_from_page_id(const page_id_t& page_id)
{
    /* 2log of BUF_READ_AHEAD_AREA (64) */
    page_no_t ignored_page_no = page_id.page_no() >> 6;
    page_id_t id(page_id.space_id(), ignored_page_no);

    uint32 i = id.fold() % buf_pool_instances;

    return (&buf_pool_ptr[i]);
}

inline buf_pool_t* buf_pool_from_bpage(const buf_page_t *bpage) /*!< in: buffer pool page */
{
    uint32 i = bpage->buf_pool_index;

    ut_ad(i < buf_pool_instances);

    return (&buf_pool_ptr[i]);
}

inline buf_pool_t* buf_pool_from_block(const buf_block_t *block) /*!< in: block */
{
    return(buf_pool_from_bpage(&block->page));
}

inline buf_pool_t* buf_pool_get(uint32 id)
{
    if (id >= buf_pool_instances) {
        return NULL;
    }

    return (&buf_pool_ptr[id]);
}


// Gets recovery lsn for any page in the pool.
// Returns zero if all modified pages have been flushed to disk.
lsn_t buf_pool_get_recovery_lsn(void)
{
    buf_block_t* block;
    lsn_t       lsn = 0;
    lsn_t       recovery_lsn = 0;
    buf_pool_t* buf_pool;

    // When we traverse all the flush lists
    // we don't want another thread to add a dirty page to any flush list.
    //log_flush_order_mutex_enter();

    for (uint32 i = 0; i < buf_pool_instances; i++) {
        buf_pool = buf_pool_get(i);

        mutex_enter(&buf_pool->flush_list_mutex);

        block = UT_LIST_GET_LAST(buf_pool->flush_list);
        if (block != NULL) {
            ut_ad(block->page.in_flush_list);
            lsn = block->page.recovery_lsn;
        }

        mutex_exit(&buf_pool->flush_list_mutex);

        if (recovery_lsn == 0 || recovery_lsn > lsn) {
            recovery_lsn = lsn;
        }
    }

    //log_flush_order_mutex_exit();

    return recovery_lsn;
}

// Invalidates file pages in one buffer pool instance
static void buf_pool_invalidate_instance(buf_pool_t *buf_pool)
{
    uint32  i;

    mutex_enter(&buf_pool->mutex);

    for (i = BUF_FLUSH_LRU; i < BUF_FLUSH_N_TYPES; i++) {

        /* As this function is called during startup and
        during redo application phase during recovery, InnoDB
        is single threaded (apart from IO helper threads) at
        this stage. No new write batch can be in intialization
        stage at this point. */
        ut_ad(buf_pool->init_flush[i] == FALSE);

        /* However, it is possible that a write batch that has
        been posted earlier is still not complete. For buffer
        pool invalidation to proceed we must ensure there is NO
        write activity happening. */
        if (buf_pool->n_flush[i] > 0) {
            buf_flush_t type = static_cast<buf_flush_t>(i);

            mutex_exit(&buf_pool->mutex);
            //buf_flush_wait_batch_end(buf_pool, type);
            mutex_enter(&buf_pool->mutex);
        }
    }

    mutex_exit(&buf_pool->mutex);

    //ut_ad(buf_all_freed_instance(buf_pool));

    mutex_enter(&buf_pool->mutex);

    while (buf_LRU_scan_and_free_block(buf_pool)) {
    }

    ut_ad(UT_LIST_GET_LEN(buf_pool->LRU) == 0);

    buf_pool->freed_page_clock = 0;
    buf_pool->LRU_old = NULL;
    buf_pool->LRU_old_len = 0;

    memset(&buf_pool->stat, 0x00, sizeof(buf_pool->stat));
    buf_pool->last_printout_time = get_time_ms();
    buf_pool->old_stat = buf_pool->stat;

    mutex_exit(&buf_pool->mutex);
}


/*********************************************************************//**
Invalidates the file pages in the buffer pool when an archive recovery is
completed. All the file pages buffered must be in a replaceable state when
this function is called: not latched and not modified. */
void buf_pool_invalidate(void)
{
    uint32 i;

    for (i = 0; i < buf_pool_instances; i++) {
        buf_pool_invalidate_instance(&buf_pool_ptr[i]);
    }
}

