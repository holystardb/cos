#include "knl_buf_lru.h"
#include "knl_buf.h"
#include "cm_list.h"
#include "cm_dbug.h"
#include "cm_log.h"

/******************************************************************//**
Returns a free block from the buf_pool.  The block is taken off the free list.
If it is empty, returns NULL. */
buf_block_t *buf_LRU_get_free_only(buf_pool_t *buf_pool)
{
    buf_block_t *block;

    spin_lock(&buf_pool->free_list_mutex, NULL);

    block = (buf_block_t *)(UT_LIST_GET_FIRST(buf_pool->free));
    if (block != NULL) {
        ut_ad(block->page.in_free_list);
        ut_d(block->page.in_free_list = FALSE);
        ut_ad(!block->page.in_flush_list);
        ut_ad(!block->page.in_LRU_list);
        ut_a(!buf_page_in_file(&block->page));

        UT_LIST_REMOVE(list_node, buf_pool->free, &block->page);

        spin_unlock(&buf_pool->free_list_mutex);

        buf_block_set_state(block, BUF_BLOCK_READY_FOR_USE);

        return (block);
    }

    spin_lock(&buf_pool->free_list_mutex, NULL);

    return block;
}

/** We scan these many blocks when looking for a clean page to evict during LRU eviction. */
static const uint32 BUF_LRU_SEARCH_SCAN_THRESHOLD = 100;


bool32 buf_flush_ready_for_replace(buf_page_t *bpage)
{
  ut_ad(spin_lock_own(buf_page_get_mutex(bpage)));
  ut_ad(bpage->in_LRU_list);

  if (buf_page_in_file(bpage)) {
    return (bpage->oldest_modification == 0 && bpage->buf_fix_count == 0 && buf_page_get_io_fix(bpage) == BUF_IO_NONE);
  }

  LOG_PRINT_ERROR("Buffer block %lu state %lu in the LRU list!", bpage, bpage->state);

  return (FALSE);
}

/** Takes a block out of the LRU list and page hash table.
If the block is compressed-only (BUF_BLOCK_ZIP_PAGE), the object will be freed.

The caller must hold buf_pool->LRU_list_mutex, the buf_page_get_mutex() mutex and the appropriate hash_lock.
This function will release the buf_page_get_mutex() and the hash_lock. */
static bool32 buf_LRU_block_remove_hashed(buf_page_t *bpage, bool zip, bool ignore_content)
{
    return FALSE;
}


/** Try to free a block.  If bpage is a descriptor of a compressed-only
page, the descriptor object will be freed as well.
NOTE: this function may temporarily release and relock the
buf_page_get_get_mutex(). Furthermore, the page frame will no longer be
accessible via bpage. If this function returns true, it will also release
the LRU list mutex.
The caller must hold the LRU list and buf_page_get_mutex() mutexes.
@param[in]	bpage	block to be freed
@param[in]	zip	true if should remove also the compressed page of
                        an uncompressed page
@return true if freed, false otherwise. */
bool buf_LRU_free_page(buf_page_t *bpage)
{
    buf_page_t *b = NULL;
    buf_pool_t *buf_pool = buf_pool_from_bpage(bpage);

    rw_lock_t *hash_lock = buf_page_hash_lock_get(buf_pool, bpage->id);

    spinlock_t *block_lock = buf_page_get_mutex(bpage);

    ut_ad(spin_lock_own(&buf_pool->LRU_list_mutex));
    ut_ad(spin_lock_own(block_lock));
    ut_ad(buf_page_in_file(bpage));
    ut_ad(bpage->in_LRU_list);

    if (!buf_page_can_relocate(bpage)) {
        /* Do not free buffer fixed and I/O-fixed blocks. */
        return (false);
    }

    if (bpage->oldest_modification > 0 && buf_page_get_state(bpage) != BUF_BLOCK_FILE_PAGE) {
        ut_ad(buf_page_get_state(bpage) == BUF_BLOCK_ZIP_DIRTY);
        return (false);
    } else if (buf_page_get_state(bpage) == BUF_BLOCK_FILE_PAGE) {
        b = buf_page_alloc_descriptor();
        ut_a(b);
    }

    ut_ad(buf_page_in_file(bpage));
    ut_ad(bpage->in_LRU_list);
    ut_ad(!bpage->in_flush_list == !bpage->oldest_modification);

    LOG_PRINT_DEBUG("free page %lu : %lu", bpage->id.space(), bpage->id.page_no());

    spin_unlock(block_lock);

    rw_lock_x_lock(hash_lock);

    spin_lock(block_lock, NULL);

    if (!buf_page_can_relocate(bpage)) {

not_freed:

        rw_lock_x_unlock(hash_lock);

        if (b != NULL) {
            buf_page_free_descriptor(b);
        }
        return (false);

    } else if (bpage->oldest_modification > 0 && buf_page_get_state(bpage) != BUF_BLOCK_FILE_PAGE) {

        ut_ad(buf_page_get_state(bpage) == BUF_BLOCK_ZIP_DIRTY);
        goto not_freed;

    } else if (b != NULL) {
        new (b) buf_page_t(*bpage);
    }

    ut_ad(rw_lock_own(hash_lock, RW_LOCK_EXCLUSIVE));
    ut_ad(buf_page_can_relocate(bpage));

  /* buf_LRU_block_remove_hashed() releases the hash_lock */
  ut_ad(!rw_lock_own(hash_lock, RW_LOCK_EXCLUSIVE) && !rw_lock_own(hash_lock, RW_LOCK_SHARED));

  /* We have just freed a BUF_BLOCK_FILE_PAGE. If b != NULL
  then it was a compressed page with an uncompressed frame and
  we are interested in freeing only the uncompressed frame.
  Therefore we have to reinsert the compressed page descriptor
  into the LRU and page_hash (and possibly flush_list).
  if b == NULL then it was a regular page that has been freed */

  if (b != NULL) {
    buf_page_t *prev_b = UT_LIST_GET_PREV(LRU, b);

    rw_lock_x_lock(hash_lock);

    spin_lock(block_lock, NULL);

    ut_a(!buf_page_hash_get_low(buf_pool, b->id));

    b->state = b->oldest_modification ? BUF_BLOCK_ZIP_DIRTY : BUF_BLOCK_ZIP_PAGE;

    ut_ad(b->size.is_compressed());

    //UNIV_MEM_DESC(b->zip.data, b->size.physical());

    /* The fields in_page_hash and in_LRU_list of
    the to-be-freed block descriptor should have
    been cleared in
    buf_LRU_block_remove_hashed(), which
    invokes buf_LRU_remove_block(). */
    ut_ad(!bpage->in_page_hash);
    ut_ad(!bpage->in_LRU_list);

    /* bpage->state was BUF_BLOCK_FILE_PAGE because
    b != NULL. The type cast below is thus valid. */
    //ut_ad(!((buf_block_t *)bpage)->in_unzip_LRU_list);

    /* The fields of bpage were copied to b before
    buf_LRU_block_remove_hashed() was invoked. */
    //ut_ad(!b->in_zip_hash);
    ut_ad(b->in_page_hash);
    ut_ad(b->in_LRU_list);

    HASH_INSERT(buf_page_t, hash, buf_pool->page_hash, b->id.fold(), b);

    /* Insert b where bpage was in the LRU list. */
    if (prev_b != NULL) {
      uint32 lru_len;

      ut_ad(prev_b->in_LRU_list);
      ut_ad(buf_page_in_file(prev_b));

      UT_LIST_ADD_AFTER(LRU, buf_pool->LRU, prev_b, b);

      //incr_LRU_size_in_bytes(b, buf_pool);
      buf_pool->stat.LRU_bytes += UNIV_PAGE_SIZE;

      if (b->old) {
        buf_pool->LRU_old_len++;
        if (buf_pool->LRU_old == UT_LIST_GET_NEXT(LRU, b)) {
          buf_pool->LRU_old = b;
        }
      }

      lru_len = UT_LIST_GET_LEN(buf_pool->LRU);

      if (lru_len > BUF_LRU_OLD_MIN_LEN) {
        ut_ad(buf_pool->LRU_old);
        /* Adjust the length of the old block list if necessary */
        //buf_LRU_old_adjust_len(buf_pool);
      } else if (lru_len == BUF_LRU_OLD_MIN_LEN) {
        /* The LRU list is now long enough for LRU_old to become defined: init it */
        //buf_LRU_old_init(buf_pool);
      }
#ifdef UNIV_LRU_DEBUG
      /* Check that the "old" flag is consistent in the block and its neighbours. */
      buf_page_set_old(b, buf_page_is_old(b));
#endif /* UNIV_LRU_DEBUG */
    } else {
      ut_d(b->in_LRU_list = FALSE);
      //buf_LRU_add_block_low(b, buf_page_is_old(b));
    }

    //mutex_exit(block_mutex);
  }

  mutex_exit(&buf_pool->LRU_list_mutex);

  /* Remove possible adaptive hash index on the page.
  The page was declared uninitialized by
  buf_LRU_block_remove_hashed().  We need to flag
  the contents of the page valid (which it still is) in
  order to avoid bogus Valgrind warnings.*/

  //UNIV_MEM_VALID(((buf_block_t *)bpage)->frame, UNIV_PAGE_SIZE);
  //btr_search_drop_page_hash_index((buf_block_t *)bpage);
  //UNIV_MEM_INVALID(((buf_block_t *)bpage)->frame, UNIV_PAGE_SIZE);

  //buf_LRU_block_free_hashed_page((buf_block_t *)bpage);

  return (true);
}


/** Puts a block back to the free list.
@param[in] block block must not contain a file page */
void buf_LRU_block_free_non_file_page(buf_block_t *block)
{
}

/** Scan depth for LRU flush batch i.e.: number of blocks scanned*/
ulong srv_LRU_scan_depth = 1024;

/** Try to free a clean page from the common LRU list.
@param[in,out] buf_pool buffer pool instance
@param[in]     scan_all scan whole LRU list if true, otherwise scan only up to BUF_LRU_SEARCH_SCAN_THRESHOLD
@return true if freed */
static bool buf_LRU_free_from_common_LRU_list(buf_pool_t *buf_pool, bool scan_all)
{
  ut_ad(spin_lock_own(&buf_pool->LRU_list_mutex));

  uint32 scanned = 0;
  bool freed = false;
  buf_page_t* bpage;

  for (bpage = UT_LIST_GET_LAST(buf_pool->LRU), scanned = 1, freed = FALSE;
       bpage != NULL && !freed && (scan_all || scanned < srv_LRU_scan_depth);
       ++scanned) {
  
      unsigned    accessed = 0;
      buf_page_t* prev_bpage = UT_LIST_GET_PREV(LRU, bpage);
  
      ut_ad(buf_page_in_file(bpage));
      ut_ad(bpage->in_LRU_list);
  
      //accessed = buf_page_is_accessed(bpage);
      //freed = buf_LRU_free_page(bpage, true);
      if (freed && !accessed) {
          /* Keep track of pages that are evicted without
          ever being accessed. This gives us a measure of
          the effectiveness of readahead */
          ++buf_pool->stat.n_ra_pages_evicted;
      }
  
      bpage = prev_bpage;
  }


  if (scanned) {
    //MONITOR_INC_VALUE_CUMULATIVE(MONITOR_LRU_SEARCH_SCANNED,
    //                             MONITOR_LRU_SEARCH_SCANNED_NUM_CALL,
    //                             MONITOR_LRU_SEARCH_SCANNED_PER_CALL, scanned);
  }

  return (freed);
}

/** Try to free a replaceable block.
@param[in,out] buf_pool buffer pool instance
@param[in]     scan_all scan whole LRU list if ture, otherwise scan only BUF_LRU_SEARCH_SCAN_THRESHOLD blocks
@return true if found and freed */
bool buf_LRU_scan_and_free_block(buf_pool_t *buf_pool, bool scan_all)
{
    bool freed;

    spin_lock(&buf_pool->LRU_list_mutex);

    freed = buf_LRU_free_from_common_LRU_list(buf_pool, scan_all);

    if (!freed) {
        spin_unlock(&buf_pool->LRU_list_mutex);
    }

    return (freed);
}

#define BUF_LRU_OLD_TOLERANCE 20  // #define since it is used in #if below

/*******************************************************************//**
Moves the LRU_old pointer so that the length of the old blocks list
is inside the allowed limits. */
void buf_LRU_old_adjust_len(buf_pool_t* buf_pool)
{
	uint32	old_len;
	uint32	new_len = 0;

	ut_a(buf_pool->LRU_old);
	//ut_ad(buf_pool_mutex_own(buf_pool));
	//ut_ad(buf_pool->LRU_old_ratio >= BUF_LRU_OLD_RATIO_MIN);
	//ut_ad(buf_pool->LRU_old_ratio <= BUF_LRU_OLD_RATIO_MAX);

	old_len = buf_pool->LRU_old_len;
	//new_len = ut_min(UT_LIST_GET_LEN(buf_pool->LRU) * buf_pool->LRU_old_ratio / BUF_LRU_OLD_RATIO_DIV,
	//		 UT_LIST_GET_LEN(buf_pool->LRU) - (BUF_LRU_OLD_TOLERANCE + BUF_LRU_NON_OLD_MIN_LEN));

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


/******************************************************************//**
Returns a free block from the buf_pool. The block is taken off the free list.
If free list is empty, blocks are moved from the end of the LRU list to the free list. */
buf_block_t* buf_LRU_get_free_block(buf_pool_t *buf_pool)
{
    buf_block_t *block = NULL;
    bool         freed = false;
    uint32       n_iterations = 0;
    uint32       flush_failures = 0;

    //MONITOR_INC(MONITOR_LRU_GET_FREE_SEARCH);

loop:

    /* If there is a block in the free list, take it */
    block = buf_LRU_get_free_only(buf_pool);
    if (block != NULL) {
        ut_ad(buf_pool_from_block(block) == buf_pool);
        //block->skip_flush_check = false;
        //block->page.flush_observer = NULL;
        return(block);
    }

    //MONITOR_INC(MONITOR_LRU_GET_FREE_LOOPS );

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
    /*
    if (n_iterations > 20 && srv_buf_pool_old_size == srv_buf_pool_size) {
        LOG_PRINT_WARNING("Difficult to find free blocks in the buffer pool(%lu search iterations)! "
            "%lu failed attempts to flush a page! Consider increasing the buffer pool size."
            "It is also possible that in your fsync is very slow."
            "Look at the number of fsyncs in diagnostic info below."
            " Pending flushes (fsync) log: %lu "
            " buffer pool: %lu "
            " OS file reads: %lu "
            " OS file writes: %lu "
            " OS fsyncs: %lu",
            n_iterations, flush_failures,
            fil_n_pending_log_flushes, fil_n_pending_tablespace_flushes,
            os_n_file_reads, os_n_file_writes, os_n_fsyncs);

        os_event_set(lock_sys->timeout_event);
    }
    */
    /* If we have scanned the whole LRU and still are unable to find a free block
       then we should sleep here to let the page_cleaner do an LRU batch for us. */

    //if (!srv_read_only_mode) {
    //    os_event_set(buf_flush_event);
    //}

    if (n_iterations > 1) {
        //MONITOR_INC( MONITOR_LRU_GET_FREE_WAITS );
        os_thread_sleep(10000);
    }

    /* No free block was found: try to flush the LRU list.
     * This call will flush one page from the LRU and put it on the free list.
     * That means that the free block is up for grabs for all user threads.
     */

    //if (!buf_flush_single_page_from_LRU(buf_pool)) {
        //MONITOR_INC(MONITOR_LRU_SINGLE_FLUSH_FAILURE_COUNT);
    //    ++flush_failures;
    //}

    srv_stats.buf_pool_wait_free.add(n_iterations, 1);

    n_iterations++;

    goto loop;
}

