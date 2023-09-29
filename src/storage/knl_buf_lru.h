#ifndef _KNL_BUF_LRU_H
#define _KNL_BUF_LRU_H

#include "cm_type.h"
#include "knl_buf.h"

/** Minimum LRU list length for which the LRU_old pointer is defined */
#define BUF_LRU_OLD_MIN_LEN	512	/* 8 megabytes of 16k pages */


buf_block_t* buf_LRU_get_free_only(buf_pool_t *buf_pool);



#endif  /* _KNL_BUF_LRU_H */
