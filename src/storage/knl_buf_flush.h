#ifndef _KNL_BUF_FLUSH_H
#define _KNL_BUF_FLUSH_H

#include "cm_type.h"
#include "knl_buf.h"


extern bool32 buf_flush_single_page_from_LRU(buf_pool_t *buf_pool);

extern inline void buf_flush_note_modification(buf_block_t* block, mtr_t* mtr);
extern inline void buf_flush_recv_note_modification(buf_block_t* block, lsn_t start_lsn, lsn_t end_lsn);
#endif  /* _KNL_BUF_FLUSH_H */
