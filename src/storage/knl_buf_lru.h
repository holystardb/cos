#ifndef _KNL_BUF_LRU_H
#define _KNL_BUF_LRU_H

#include "cm_type.h"
#include "knl_buf.h"

extern inline void buf_LRU_insert_block_to_free_list(buf_pool_t* buf_pool, buf_block_t* block);
extern inline void buf_LRU_insert_block_to_lru_list(buf_pool_t* buf_pool, buf_page_t* bpage);
extern inline void buf_LRU_remove_block_from_lru_list(buf_pool_t* buf_pool, buf_page_t* bpage);
extern inline buf_block_t* buf_LRU_get_free_block(buf_pool_t* buf_pool);
extern inline bool32 buf_LRU_scan_and_free_block(buf_pool_t* buf_pool);
extern inline void buf_LRU_free_one_page(buf_page_t* bpage);


#endif  /* _KNL_BUF_LRU_H */
