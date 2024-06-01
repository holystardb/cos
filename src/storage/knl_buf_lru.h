#ifndef _KNL_BUF_LRU_H
#define _KNL_BUF_LRU_H

#include "cm_type.h"
#include "knl_buf.h"

/** Minimum LRU list length for which the LRU_old pointer is defined */
#define BUF_LRU_OLD_MIN_LEN	512	/* 8 megabytes of 16k pages */


buf_block_t* buf_LRU_get_free_only(buf_pool_t *buf_pool);
buf_block_t* buf_LRU_get_free_block(buf_pool_t *buf_pool);

bool32 buf_LRU_scan_and_free_block(buf_pool_t *buf_pool, bool scan_all);

void buf_LRU_old_adjust_len(buf_pool_t* buf_pool);
void buf_LRU_add_block(buf_page_t *bpage, bool32 old);

void buf_LRU_block_free_non_file_page(buf_block_t *block);
void buf_LRU_free_one_page(buf_page_t *bpage, bool32 ignore_content);
void buf_LRU_make_block_young(buf_page_t *bpage);
void buf_LRU_remove_block(buf_page_t *bpage);


#endif  /* _KNL_BUF_LRU_H */
