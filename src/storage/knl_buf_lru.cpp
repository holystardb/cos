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

    bool32 zip = FALSE;
    if (!buf_LRU_block_remove_hashed(bpage, zip, false)) {
        spin_unlock(&buf_pool->LRU_list_mutex);

        if (b != NULL) {
            buf_page_free_descriptor(b);
        }

        return (true);
    }

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

    UNIV_MEM_DESC(b->zip.data, b->size.physical());

    /* The fields in_page_hash and in_LRU_list of
    the to-be-freed block descriptor should have
    been cleared in
    buf_LRU_block_remove_hashed(), which
    invokes buf_LRU_remove_block(). */
    ut_ad(!bpage->in_page_hash);
    ut_ad(!bpage->in_LRU_list);

    /* bpage->state was BUF_BLOCK_FILE_PAGE because
    b != NULL. The type cast below is thus valid. */
    ut_ad(!((buf_block_t *)bpage)->in_unzip_LRU_list);

    /* The fields of bpage were copied to b before
    buf_LRU_block_remove_hashed() was invoked. */
    ut_ad(!b->in_zip_hash);
    ut_ad(b->in_page_hash);
    ut_ad(b->in_LRU_list);

    HASH_INSERT(buf_page_t, hash, buf_pool->page_hash, b->id.fold(), b);

    /* Insert b where bpage was in the LRU list. */
    if (prev_b != NULL) {
      ulint lru_len;

      ut_ad(prev_b->in_LRU_list);
      ut_ad(buf_page_in_file(prev_b));

      UT_LIST_INSERT_AFTER(buf_pool->LRU, prev_b, b);

      incr_LRU_size_in_bytes(b, buf_pool);

      if (buf_page_is_old(b)) {
        buf_pool->LRU_old_len++;
        if (buf_pool->LRU_old == UT_LIST_GET_NEXT(LRU, b)) {
          buf_pool->LRU_old = b;
        }
      }

      lru_len = UT_LIST_GET_LEN(buf_pool->LRU);

      if (lru_len > BUF_LRU_OLD_MIN_LEN) {
        ut_ad(buf_pool->LRU_old);
        /* Adjust the length of the
        old block list if necessary */
        buf_LRU_old_adjust_len(buf_pool);
      } else if (lru_len == BUF_LRU_OLD_MIN_LEN) {
        /* The LRU list is now long
        enough for LRU_old to become
        defined: init it */
        buf_LRU_old_init(buf_pool);
      }
#ifdef UNIV_LRU_DEBUG
      /* Check that the "old" flag is consistent
      in the block and its neighbours. */
      buf_page_set_old(b, buf_page_is_old(b));
#endif /* UNIV_LRU_DEBUG */
    } else {
      ut_d(b->in_LRU_list = FALSE);
      buf_LRU_add_block_low(b, buf_page_is_old(b));
    }

    mutex_enter(&buf_pool->zip_mutex);
    rw_lock_x_unlock(hash_lock);
    if (b->state == BUF_BLOCK_ZIP_PAGE) {
#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
      buf_LRU_insert_zip_clean(b);
#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */
    } else {
      /* Relocate on buf_pool->flush_list. */
      buf_flush_relocate_on_flush_list(bpage, b);
    }

    bpage->zip.data = NULL;

    page_zip_set_size(&bpage->zip, 0);

    bpage->size.copy_from(
        page_size_t(bpage->size.logical(), bpage->size.logical(), false));

    /* Prevent buf_page_get_gen() from
    decompressing the block while we release block_mutex. */

    buf_page_set_sticky(b);

    mutex_exit(&buf_pool->zip_mutex);

    mutex_exit(block_mutex);
  }

  mutex_exit(&buf_pool->LRU_list_mutex);

  /* Remove possible adaptive hash index on the page.
  The page was declared uninitialized by
  buf_LRU_block_remove_hashed().  We need to flag
  the contents of the page valid (which it still is) in
  order to avoid bogus Valgrind warnings.*/

  UNIV_MEM_VALID(((buf_block_t *)bpage)->frame, UNIV_PAGE_SIZE);
  btr_search_drop_page_hash_index((buf_block_t *)bpage);
  UNIV_MEM_INVALID(((buf_block_t *)bpage)->frame, UNIV_PAGE_SIZE);

  if (b != NULL) {
    /* Compute and stamp the compressed page
    checksum while not holding any mutex.  The
    block is already half-freed
    (BUF_BLOCK_REMOVE_HASH) and removed from
    buf_pool->page_hash, thus inaccessible by any
    other thread. */

    ut_ad(b->size.is_compressed());

    BlockReporter reporter = BlockReporter(false, b->zip.data, b->size, false);

    const uint32_t checksum = reporter.calc_zip_checksum(
        static_cast<srv_checksum_algorithm_t>(srv_checksum_algorithm));

    mach_write_to_4(b->zip.data + FIL_PAGE_SPACE_OR_CHKSUM, checksum);
  }

  if (b != NULL) {
    mutex_enter(&buf_pool->zip_mutex);

    buf_page_unset_sticky(b);

    mutex_exit(&buf_pool->zip_mutex);
  }

  buf_LRU_block_free_hashed_page((buf_block_t *)bpage);

  return (true);
}


/** Puts a block back to the free list.
@param[in] block block must not contain a file page */
void buf_LRU_block_free_non_file_page(buf_block_t *block)
{
}


/** Try to free a clean page from the common LRU list.
@param[in,out] buf_pool buffer pool instance
@param[in]     scan_all scan whole LRU list if true, otherwise scan only up to BUF_LRU_SEARCH_SCAN_THRESHOLD
@return true if freed */
static bool buf_LRU_free_from_common_LRU_list(buf_pool_t *buf_pool, bool scan_all)
{
  ut_ad(spin_lock_own(&buf_pool->LRU_list_mutex));

  uint32 scanned = 0;
  bool freed = false;

  for (buf_page_t *bpage = buf_pool->lru_scan_itr.start();
       bpage != NULL && !freed && (scan_all || scanned < BUF_LRU_SEARCH_SCAN_THRESHOLD);
       ++scanned, bpage = buf_pool->lru_scan_itr.get()) {

    buf_page_t *prev = UT_LIST_GET_PREV(LRU, bpage);
    spinlock_t *lock = buf_page_get_mutex(bpage);

    buf_pool->lru_scan_itr.set(prev);

    ut_ad(buf_page_in_file(bpage));
    ut_ad(bpage->in_LRU_list);

    spin_lock(lock, NULL);

    if (buf_flush_ready_for_replace(bpage)) {
      freed = buf_LRU_free_page(bpage, true);
    }

    if (!freed) spin_unlock(lock);

    if (freed && !bpage->access_time) {
      /* Keep track of pages that are evicted without ever being accessed.
         This gives us a measure of the effectiveness of readahead */
      ++buf_pool->stat.n_ra_pages_evicted;
    }

    ut_ad(!spin_lock_own(lock));

    if (freed) break;
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

