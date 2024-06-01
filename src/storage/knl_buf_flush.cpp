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


