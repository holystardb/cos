#include "knl_buf_flush.h"
#include "cm_log.h"

/** This function picks up a single page from the tail of the LRU
list, flushes it (if it is dirty), removes it from page_hash and LRU
list and puts it on the free list. It is called from user threads when
they are unable to find a replaceable page at the tail of the LRU
list i.e.: when the background LRU flushing in the page_cleaner thread
is not fast enough to keep pace with the workload.
@param[in,out]  buf_pool        buffer pool instance
@return true if success. */
bool32 buf_flush_single_page_from_LRU(buf_pool_t *buf_pool)
{
#if 0
  bool32 freed;
  uint32 scanned;
  buf_page_t *bpage;

  mutex_enter(&buf_pool->LRU_list_mutex);

  for (bpage = buf_pool->single_scan_itr.start(), scanned = 0, freed = false;
       bpage != nullptr; ++scanned, bpage = buf_pool->single_scan_itr.get()) {
    ut_ad(mutex_own(&buf_pool->LRU_list_mutex));

    auto prev = UT_LIST_GET_PREV(LRU, bpage);

    buf_pool->single_scan_itr.set(prev);

    if (bpage->was_stale()) {
      freed = buf_page_free_stale(buf_pool, bpage);
      if (freed) {
        break;
      }
    } else {
      auto block_mutex = buf_page_get_mutex(bpage);

      mutex_enter(block_mutex);

      if (buf_flush_ready_for_replace(bpage)) {
        /* block is ready for eviction i.e., it is
        clean and is not IO-fixed or buffer fixed. */

        if (buf_LRU_free_page(bpage, true)) {
          freed = true;
          break;
        }

        mutex_exit(block_mutex);

      } else if (buf_flush_ready_for_flush(bpage, BUF_FLUSH_SINGLE_PAGE)) {
        /* Block is ready for flush. Try and dispatch an IO
        request. We'll put it on free list in IO completion
        routine if it is not buffer fixed. The following call
        will release the buffer pool and block mutex.

        Note: There is no guarantee that this page has actually
        been freed, only that it has been flushed to disk */

        freed = buf_flush_page(buf_pool, bpage, BUF_FLUSH_SINGLE_PAGE, true);

        if (freed) {
          break;
        }

        mutex_exit(block_mutex);
      } else {
        mutex_exit(block_mutex);
      }
      ut_ad(!mutex_own(block_mutex));
    }
  }

  if (!freed) {
    /* Can't find a single flushable page. */
    ut_ad(bpage == nullptr);
    mutex_exit(&buf_pool->LRU_list_mutex);
  }

  if (scanned) {
    MONITOR_INC_VALUE_CUMULATIVE(MONITOR_LRU_SINGLE_FLUSH_SCANNED,
                                 MONITOR_LRU_SINGLE_FLUSH_SCANNED_NUM_CALL,
                                 MONITOR_LRU_SINGLE_FLUSH_SCANNED_PER_CALL,
                                 scanned);
  }

  ut_ad(!mutex_own(&buf_pool->LRU_list_mutex));

  return freed;
#endif

    return TRUE;
}


// Inserts a modified block into the flush list
static inline void buf_flush_insert_into_flush_list(buf_pool_t* buf_pool, buf_block_t* block)
{
    ut_ad(mutex_own(&block->mutex));
    ut_ad(block->is_resident() || rw_lock_own(&block->rw_lock, RW_LOCK_EXCLUSIVE));

    mutex_enter(&buf_pool->flush_list_mutex, &buf_pool->stat.flush_list_mutex_stat);

    ut_ad(!block->page.in_flush_list);
    block->page.in_flush_list = TRUE;
    block->page.recovery_lsn = log_get_flushed_to_disk_lsn() + 1;
    UT_LIST_ADD_FIRST(list_node_flush, buf_pool->flush_list, block);

    buf_pool->stat.flush_list_bytes += UNIV_PAGE_SIZE;

    mutex_exit(&buf_pool->flush_list_mutex);

    LOGGER_DEBUG(LOGGER,
        "buf_flush_insert_into_flush_list: block (space id = %lu, page no = %lu) newest_modification = %llu recovery_lsn = %llu",
        block->page.id.space_id(), block->page.id.page_no(), block->page.newest_modification, block->page.recovery_lsn);
}

// Puts the block to the list of modified blocks, if it is not already in it.
inline void buf_flush_note_modification(buf_block_t* block, mtr_t* mtr)
{
    buf_pool_t* buf_pool = buf_pool_from_block(block);

    ut_ad(!srv_read_only_mode);
    ut_ad(block->is_resident() || rw_lock_own(&block->rw_lock, RW_LOCK_EXCLUSIVE));
    ut_ad(buf_block_get_state(block) == BUF_BLOCK_FILE_PAGE);
    ut_ad((block->is_resident() && block->get_fix_count() == 0) ||
        (!block->is_resident() && block->get_fix_count() > 0));
    ut_ad(mtr->modifications);

    mutex_enter(&block->mutex);

    // if it is block of transaction slot, because block does not have a rw_lock
    // block->page.newest_modification is not assigned in order
    if (block->page.newest_modification < mtr->end_lsn) {
        block->page.newest_modification = mtr->end_lsn;
    }

    if (block->page.recovery_lsn == 0) {
        buf_flush_insert_into_flush_list(buf_pool, block);
    } else {
        // In the following two situations, may be block->page.recovery_lsn > mtr->start_buf_lsn.val.lsn
        //    1) block of transaction slot, because block does not have a rw_lock
        //    2) A block added multiple times to mtr
        //ut_ad(block->page.recovery_lsn <= mtr->start_buf_lsn.val.lsn);
    }

    mutex_exit(&block->mutex);

    srv_stats.buf_pool_write_requests.inc();
}





