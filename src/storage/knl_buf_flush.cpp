#include "knl_buf_flush.h"

/** This function picks up a single page from the tail of the LRU list,
flushes it (if it is dirty), removes it from page_hash and LRU list and puts it on the free list.
It is called from user threads when they are unable to find a replaceable page at the tail of the LRU list
i.e.: when the background LRU flushing in the page_cleaner thread is not fast enough to keep pace with the workload.
@param[in,out] buf_pool	buffer pool instance
@return true if success. */
bool buf_flush_single_page_from_LRU(buf_pool_t *buf_pool);

