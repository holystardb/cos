#include "knl_buf_flush.h"


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
static inline void buf_flush_insert_into_flush_list(buf_pool_t* buf_pool, buf_block_t* block, lsn_t flushed_lsn)
{
    ut_ad(mutex_own(&block->mutex));

    mutex_enter(&buf_pool->flush_list_mutex, NULL);

    ut_ad((UT_LIST_GET_FIRST(buf_pool->flush_list) == NULL)
          || (UT_LIST_GET_FIRST(buf_pool->flush_list)->recovery_lsn <= flushed_lsn));

    // If we are in the recovery then we need to update the flush red-black tree as well.
    //if (UNIV_LIKELY_NULL(buf_pool->flush_rbt)) {
    //    mutex_exit(&buf_pool->flush_list_mutex);
    //    buf_flush_insert_sorted_into_flush_list(buf_pool, block, lsn);
    //    return;
    //}

    ut_ad(!block->page.in_flush_list);
    block->page.in_flush_list = TRUE;
    block->page.recovery_lsn = flushed_lsn;
    UT_LIST_ADD_FIRST(list_node, buf_pool->flush_list, &block->page);

    buf_pool->stat.flush_list_bytes += UNIV_PAGE_SIZE;

    mutex_exit(&buf_pool->flush_list_mutex);
}

// Puts the block to the list of modified blocks, if it is not already in it.
inline void buf_flush_note_modification(buf_block_t* block, mtr_t* mtr, lsn_t flushed_lsn)
{
    buf_pool_t* buf_pool = buf_pool_from_block(block);

    ut_ad(!srv_read_only_mode);
    ut_ad(buf_block_get_state(block) == BUF_BLOCK_FILE_PAGE);
    ut_ad(block->page.buf_fix_count > 0);
    ut_ad(mtr->modifications);
    ut_ad(!mtr->made_dirty || log_flush_order_mutex_own());

    mutex_enter(&block->mutex);

    ut_ad(block->page.newest_modification <= mtr->end_lsn);
    block->page.newest_modification = mtr->end_lsn;

    if (block->page.recovery_lsn == 0) {
        buf_flush_insert_into_flush_list(buf_pool, block, flushed_lsn);
    } else {
        ut_ad(block->page.recovery_lsn <= mtr->start_buf_lsn.val.lsn);
    }

    mutex_exit(&block->mutex);

    srv_stats.buf_pool_write_requests.inc();
}





