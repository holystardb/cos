#include "knl_trx_undo.h"
#include "knl_trx.h"
#include "knl_mtr.h"
#include "knl_trx_rseg.h"
#include "knl_undo_fsm.h"

const page_size_t undo_log_page_size(DB_UNDO_START_SPACE_ID);

const uint16 UNDO_PAGE_REUSE_LIMIT = (undo_log_page_size.physical() / 5);
const uint16 undo_rec_max_size = undo_log_page_size.physical() - FIL_PAGE_DATA - FIL_PAGE_DATA_END
    - TRX_UNDO_PAGE_HDR_SIZE - TRX_UNDO_SEG_HDR_SIZE - TRX_UNDO_LOG_HDR_SIZE;
const uint16 undo_page_reserved_size = 10 + FIL_PAGE_DATA_END;
#define UNDO_PAGE_LEFT(undo_page)  (undo_log_page_size.physical() - undo_page->page_offset - undo_page_reserved_size)




// -----------------------------------------------------------------------


// Initializes the fields in an undo log segment page
static inline void trx_undo_page_reuse(trx_undo_page_t* undo_page, uint32 trx_undo_op)
{
    ut_ad(undo_page->guess_block->get_fix_count() > 0);
    page_t* page = buf_block_get_frame(undo_page->guess_block);
    undo_page->guess_block->page.touch_number += BUF_HOT_PAGE_TCH;

    // 1. undo log page header

    trx_undo_page_hdr_t* page_hdr = page + TRX_UNDO_PAGE_HDR;
    mach_write_to_2(page_hdr + TRX_UNDO_PAGE_TYPE, trx_undo_op);
    mach_write_to_2(page_hdr + TRX_UNDO_PAGE_START, TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_HDR_SIZE + TRX_UNDO_SEG_HDR_SIZE);
    mach_write_to_2(page_hdr + TRX_UNDO_PAGE_FREE, TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_HDR_SIZE + TRX_UNDO_SEG_HDR_SIZE);
    // Because it is reusing PAGE, there is no need to set TRX_UNDO_PAGE_NODE_ADDR

    // 2. undo segment header

    trx_undo_seg_hdr_t* seg_hdr = page + TRX_UNDO_SEG_HDR;
    mach_write_to_2(seg_hdr + TRX_UNDO_STATE, TRX_UNDO_PAGE_STATE_ACTIVE);
    mach_write_to_2(seg_hdr + TRX_UNDO_LAST_LOG, 0xFFFF);
    flst_init(seg_hdr + TRX_UNDO_PAGE_LIST, NULL);

    // 3 redo log

    mtr_t mtr;
    mtr_start(&mtr);
    buf_block_lock_and_fix(undo_page->guess_block, RW_X_LATCH, &mtr);
    mlog_write_log(MLOG_UNDO_PAGE_REUSE, undo_page->guess_block->get_space_id(),
        undo_page->guess_block->get_page_no(), page_hdr + TRX_UNDO_PAGE_TYPE, MLOG_2BYTES, &mtr);
    mtr_commit(&mtr);

    // 4
    undo_page->scn_timestamp = 0;
    undo_page->page_offset = TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_HDR_SIZE + TRX_UNDO_SEG_HDR_SIZE;
}

// Reuses a cached undo log
// if TRX_UNDO_MODIFY_OP and size == 0, need a new page
static inline trx_undo_page_t* trx_undo_reuse_cache_page(
    trx_rseg_t* rseg, uint32 trx_undo_op, uint16 size, uint64 min_scn)
{
    trx_undo_page_t* undo_page;

    if (trx_undo_op == UNDO_INSERT_OP) {
        mutex_enter(&(rseg->insert_undo_cache_mutex), NULL);
        SLIST_GET_AND_REMOVE_FIRST(list_node, rseg->insert_undo_cache, undo_page);
        mutex_exit(&(rseg->insert_undo_cache_mutex));
        if (undo_page) {
            trx_undo_page_reuse(undo_page, UNDO_INSERT_OP);
        }
    } else {
        trx_undo_page_t* prev = NULL;

        // get from update_undo_cache
        mutex_enter(&(rseg->update_undo_cache_mutex), NULL);
        undo_page = SLIST_GET_FIRST(rseg->update_undo_cache);
        while (undo_page && (UNDO_PAGE_LEFT(undo_page) < size || undo_page->scn_timestamp > min_scn)) {
            prev = undo_page;
            undo_page = SLIST_GET_NEXT(list_node, undo_page);
        }
        if (undo_page) {
            SLIST_REMOVE(list_node, rseg->update_undo_cache, prev, undo_page);
        }
        mutex_exit(&(rseg->update_undo_cache_mutex));

        // get from insert_undo_cache
        if (undo_page == NULL) {
            mutex_enter(&(rseg->insert_undo_cache_mutex), NULL);
            SLIST_GET_AND_REMOVE_FIRST(list_node, rseg->insert_undo_cache, undo_page);
            mutex_exit(&(rseg->insert_undo_cache_mutex));
            if (undo_page) {
                trx_undo_page_reuse(undo_page, UNDO_INSERT_OP);
            }
        }
    }

    return undo_page;
}

// Initializes the fields in an undo log segment page
static inline buf_block_t* trx_undo_page_init(uint32 undo_space_id, uint32 undo_page_no, uint32 trx_undo_op)
{
    mtr_t mtr;
    const page_id_t page_id(undo_space_id, undo_page_no);

    mtr_start(&mtr);

    buf_block_t* block = buf_page_create(page_id, undo_log_page_size, RW_X_LATCH, Page_fetch::NORMAL, &mtr);
    page_t* page = buf_block_get_frame(block);
    mach_write_to_2(page + FIL_PAGE_TYPE, FIL_PAGE_TYPE_UNDO_LOG);
    block->page.touch_number += BUF_HOT_PAGE_TCH;

    // 1. undo log page header

    trx_undo_page_hdr_t* page_hdr = page + TRX_UNDO_PAGE_HDR;
    mach_write_to_2(page_hdr + TRX_UNDO_PAGE_TYPE, trx_undo_op);
    mach_write_to_2(page_hdr + TRX_UNDO_PAGE_START, TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_HDR_SIZE + TRX_UNDO_SEG_HDR_SIZE);
    mach_write_to_2(page_hdr + TRX_UNDO_PAGE_FREE, TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_HDR_SIZE + TRX_UNDO_SEG_HDR_SIZE);

    // 2. undo segment header

    trx_undo_seg_hdr_t* seg_hdr = page + TRX_UNDO_SEG_HDR;
    mach_write_to_2(seg_hdr + TRX_UNDO_STATE, TRX_UNDO_PAGE_STATE_ACTIVE);
    mach_write_to_2(seg_hdr + TRX_UNDO_LAST_LOG, 0xFFFF);
    flst_init(seg_hdr + TRX_UNDO_PAGE_LIST, NULL);

    // 3 redo log
    mlog_write_log(MLOG_UNDO_PAGE_INIT, undo_space_id, undo_page_no,
        page_hdr + TRX_UNDO_PAGE_TYPE, MLOG_2BYTES, &mtr);

    // 4 page fix, prevent page release for LRU
    buf_page_fix(&block->page);

    mtr_commit(&mtr);

    return block;
}

static inline trx_undo_page_t* trx_undo_create_page(trx_rseg_t* rseg, uint32 trx_undo_op, uint64 min_scn)
{
    // get undo_page from rseg free_cache
    trx_undo_page_t* undo_page = trx_rseg_alloc_free_undo_page(rseg);
    if (undo_page == NULL) {
        // exceeding memory limit
        return NULL;
    }

    // get undo log page from undo fsm
    undo_fsm_page_t fsm_page = undo_fsm_alloc_page(rseg->undo_space_id, min_scn);
    if (fsm_page.page_no == FIL_NULL) {
        trx_rseg_free_free_undo_page(rseg, undo_page);
        return NULL;
    }
    ut_ad(!fil_addr_is_null(fsm_page.node_addr));
    ut_ad(fsm_page.space_id == rseg->undo_space_id);

    // recreate undo_log page,
    // the purpose is to save one read IO for undo_log page.
    undo_page->scn_timestamp = 0;
    undo_page->page_offset = TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_HDR_SIZE + TRX_UNDO_SEG_HDR_SIZE;
    undo_page->page_no = fsm_page.page_no;
    undo_page->node_page_no = fsm_page.node_addr.page;
    undo_page->node_page_offset = fsm_page.node_addr.boffset;
    undo_page->guess_block = trx_undo_page_init(rseg->undo_space_id, fsm_page.page_no, trx_undo_op);

    return undo_page;
}

static inline void trx_undo_release_page(trx_rseg_t* rseg,
    trx_undo_page_base undo_page_base, uint32 undo_page_type, uint64 scn_timestamp)
{
    trx_undo_page_t* undo_page;
    undo_fsm_page_t fsm_page;

    undo_page = SLIST_GET_FIRST(undo_page_base);
    while (undo_page) {
        SLIST_REMOVE_FIRST(list_node, undo_page_base);

        //
        ut_ad(undo_page->guess_block->get_fix_count() == 1);
        buf_page_unfix(&undo_page->guess_block->page);

        //
        fsm_page.page_no = undo_page->page_no;
        fsm_page.node_addr.page = undo_page->node_page_no;
        fsm_page.node_addr.boffset = undo_page->node_page_offset;
        undo_fsm_free_page(rseg->undo_space_id, fsm_page, undo_page_type, scn_timestamp);

        //
        trx_rseg_free_free_undo_page(rseg, undo_page);

        undo_page = SLIST_GET_FIRST(undo_page_base);
    }
}

// Tries to add a page to the undo log segment where the undo log is placed.
// return X-latched block if success, else NULL
static inline trx_undo_page_t* trx_undo_add_undo_page(
    trx_t* trx, uint32 trx_undo_op, uint16 size, uint64 min_scn, mtr_t* mtr)
{
    trx_rseg_t* rseg = TRX_GET_RSEG(trx->trx_slot_id.rseg_id);

    ut_ad(trx_undo_op == UNDO_INSERT_OP || trx_undo_op == UNDO_MODIFY_OP);

    // 1 get undo page

    //only get undo page from insert_cache of current rseg
    trx_undo_page_t* undo_page = trx_undo_reuse_cache_page(rseg, UNDO_INSERT_OP, size, min_scn);
    if (undo_page == NULL) {
        undo_page = trx_undo_create_page(rseg, trx_undo_op, min_scn);
    }
    if (undo_page == NULL) {
        return NULL;
    }

    // 2 add undo_page to insert_undo/update_undo list of trx_slot

    trx_undo_page_t* seg_undo_page;
    if (trx_undo_op == UNDO_INSERT_OP) {
        seg_undo_page = SLIST_GET_LAST(trx->insert_undo);
        SLIST_ADD_LAST(list_node, trx->insert_undo, undo_page);
    } else {
        ut_ad(trx_undo_op == UNDO_MODIFY_OP);
        seg_undo_page = SLIST_GET_LAST(trx->update_undo);
        SLIST_ADD_LAST(list_node, trx->update_undo, undo_page);
    }
    ut_ad(seg_undo_page);

    trx_undo_page_hdr_t* page_hdr = buf_block_get_frame(undo_page->guess_block) + TRX_UNDO_PAGE_HDR;
    trx_undo_seg_hdr_t* seg_hdr = buf_block_get_frame(seg_undo_page->guess_block) + TRX_UNDO_SEG_HDR;
    flst_add_last(seg_hdr + TRX_UNDO_PAGE_LIST, page_hdr + TRX_UNDO_PAGE_FLST_NODE, mtr);

    return undo_page;
}

static inline void trx_undo_write_log_header(trx_t* trx, trx_undo_page_t* undo_page, mtr_t* mtr)
{
    trx_rseg_t* rseg = TRX_GET_RSEG(trx->trx_slot_id.rseg_id);

    page_id_t page_id(rseg->id, undo_page->page_no);
    buf_block_t* block = buf_page_get_gen(page_id, undo_log_page_size, RW_X_LATCH, undo_page->guess_block, Page_fetch::NORMAL, mtr);
    page_t* page = buf_block_get_frame(block);

    trx_undo_page_hdr_t* page_hdr = page + TRX_UNDO_PAGE_HDR;
    uint32 free = mach_read_from_2(page_hdr + TRX_UNDO_PAGE_FREE);

    trx_undo_log_hdr_t* log_hdr = page + free;
    mach_write_to_8(log_hdr + TRX_UNDO_TRX_ID, trx->trx_slot_id.id);
    mach_write_to_2(log_hdr + TRX_UNDO_LOG_START, free + TRX_UNDO_LOG_HDR_SIZE);
    mach_write_to_1(log_hdr + TRX_UNDO_XAID_EXISTS, FALSE);
    mach_write_to_1(log_hdr + TRX_UNDO_DICT_TRANS, FALSE);
    mach_write_to_2(log_hdr + TRX_UNDO_NEXT_LOG, 0xFFFF);

    trx_undo_seg_hdr_t* seg_hdr = page + TRX_UNDO_SEG_HDR;
    uint32 prev_log = mach_read_from_2(seg_hdr + TRX_UNDO_LAST_LOG);
    mach_write_to_2(log_hdr + TRX_UNDO_PREV_LOG, prev_log);

    mach_write_to_2(seg_hdr + TRX_UNDO_LAST_LOG, free);

    //
    mach_write_to_2(page_hdr + TRX_UNDO_PAGE_FREE, free + TRX_UNDO_LOG_HDR_SIZE);
    undo_page->page_offset = free + TRX_UNDO_LOG_HDR_SIZE;

    // redo
    mlog_write_log(MLOG_UNDO_LOG_HDR_CREATE, block->get_space_id(), block->get_page_no(), NULL, 0, mtr);
    mlog_catenate_string(mtr, log_hdr, TRX_UNDO_LOG_HDR_SIZE);
    mlog_catenate_string(mtr, seg_hdr + TRX_UNDO_LAST_LOG, MLOG_2BYTES);
    mlog_catenate_string(mtr, page_hdr + TRX_UNDO_PAGE_FREE, MLOG_2BYTES);
}

static inline trx_undo_page_t* trx_undo_assign_undo_page(
    trx_t* trx, uint32 trx_undo_op, uint16 size, uint64 min_scn, mtr_t* mtr)
{
    trx_undo_page_t* undo_page;
    trx_rseg_t* rseg = TRX_GET_RSEG(trx->trx_slot_id.rseg_id);

    // 1 get undo page from cache of current rseg

    undo_page = trx_undo_reuse_cache_page(rseg, trx_undo_op, size, min_scn);
    if (undo_page == NULL) {
        undo_page = trx_undo_create_page(rseg, trx_undo_op, min_scn);
    }
    if (undo_page == NULL) {
        return NULL;
    }

    // 2 add undo_page to insert_undo/update_undo list of trx_slot

    trx_slot_t* trx_slot = (trx_slot_t *)TRX_GET_RSEG_TRX_SLOT(trx->trx_slot_id);
    buf_block_t* trx_slot_block = TRX_GET_RSEG_TRX_SLOT_BLOCK(trx->trx_slot_id);
#ifdef UNIV_DEBUG
    page_id_t page_id(TRX_SYS_SPACE, TRX_GET_RSEG_TRX_SLOT_PAGE_NO(trx->trx_slot_id));
    buf_block_t* block = buf_page_get_gen(page_id, undo_log_page_size, RW_X_LATCH, trx_slot_block, Page_fetch::NORMAL, mtr);
    ut_ad(block == trx_slot_block);
    ut_ad((byte*)trx_slot != buf_block_get_frame(block) + trx->trx_slot_id.slot * sizeof(trx_slot_t));
#endif /* UNIV_DEBUG */

    // lock block
    buf_block_lock_and_fix(trx_slot_block, RW_X_LATCH, mtr);
    if (trx_undo_op == UNDO_INSERT_OP) {
        mlog_write_uint32((byte *)trx_slot + TRX_RSEG_SLOT_INSERT_LOG_PAGE, undo_page->page_no, MLOG_4BYTES, mtr);
        SLIST_ADD_LAST(list_node, trx->insert_undo, undo_page);
    } else {
        mlog_write_uint32((byte *)trx_slot + TRX_RSEG_SLOT_UPDATE_LOG_PAGE, undo_page->page_no, MLOG_4BYTES, mtr);
        SLIST_ADD_LAST(list_node, trx->update_undo, undo_page);
    }

    // 3 write undo log header
    trx_undo_write_log_header(trx, undo_page, mtr);

    return undo_page;
}

inline status_t trx_undo_prepare(que_sess_t* sess, undo_data_t* undo_data, mtr_t* mtr)
{
    trx_undo_page_t* undo_page;
    trx_t* trx = (trx_t *)sess->trx;
    trx_rseg_t* rseg = TRX_GET_RSEG(trx->trx_slot_id.rseg_id);

    if (undo_rec_max_size < undo_data->rec.data_size + TRX_UNDO_LOG_HDR_SIZE) {
        CM_SET_ERROR(ERR_UNDO_RECORD_TOO_BIG, undo_data->rec.data_size + TRX_UNDO_LOG_HDR_SIZE);
        return CM_ERROR;
    }

    if (undo_data->undo_op == UNDO_INSERT_OP) {
        if (SLIST_GET_LEN(trx->insert_undo) == 0) {
            trx_undo_assign_undo_page(trx, undo_data->undo_op,
                undo_data->rec.data_size + TRX_UNDO_LOG_HDR_SIZE, undo_data->query_min_scn, mtr);
        }
        undo_page = SLIST_GET_LAST(trx->insert_undo);
    } else {
        ut_ad(undo_data->undo_op == UNDO_MODIFY_OP);
        if (SLIST_GET_LEN(trx->update_undo) == 0) {
            trx_undo_assign_undo_page(trx, undo_data->undo_op,
                undo_data->rec.data_size + TRX_UNDO_LOG_HDR_SIZE, undo_data->query_min_scn, mtr);
        }
        undo_page = SLIST_GET_LAST(trx->update_undo);
    }

    if (undo_page && UNDO_PAGE_LEFT(undo_page) <= undo_data->rec.data_size) {
        // We have to extend the undo log by one page, add a page to an undo log
        undo_page = trx_undo_add_undo_page(trx, undo_data->undo_op,
            undo_data->rec.data_size, undo_data->query_min_scn, mtr);
    }

    if (undo_page == NULL) {
        CM_SET_ERROR(ERR_NO_FREE_UNDO_PAGE);
        return CM_ERROR;
    }

    undo_data->undo_space_index = rseg->undo_space_id - DB_UNDO_START_SPACE_ID;
    undo_data->undo_page_no = undo_page->page_no;
    undo_data->undo_page_offset = undo_page->page_offset;

    return CM_SUCCESS;
}

static inline page_t* trx_undo_get_page(uint32 space_id, trx_undo_page_t* undo_page, undo_op_type undo_op, mtr_t* mtr)
{
    page_id_t page_id(space_id, undo_page->page_no);
    undo_page->guess_block = buf_page_get_gen(page_id, undo_log_page_size,
        RW_X_LATCH, undo_page->guess_block, Page_fetch::NORMAL, mtr);
    ut_ad(mach_read_from_2(buf_block_get_frame(undo_page->guess_block) + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE) == undo_op);

    return buf_block_get_frame(undo_page->guess_block);
}

inline status_t trx_undo_write_log_rec(    que_sess_t* sess, undo_data_t* undo_data, mtr_t* mtr)
{
    trx_undo_page_t* undo_page;
    if (undo_data->undo_op == UNDO_INSERT_OP) {
        undo_page = SLIST_GET_LAST(sess->trx->insert_undo);
    } else {
        ut_ad(undo_data->undo_op == UNDO_MODIFY_OP);
        undo_page = SLIST_GET_LAST(sess->trx->update_undo);
    }
    ut_ad(undo_page);

    // check enough space for writing
    if (UNDO_PAGE_LEFT(undo_page) < TRX_UNDO_REC_HDR_SIZE + UNDO_REC_HEADER_SIZE + undo_data->rec.data_size) {
        CM_SET_ERROR(ERR_NO_FREE_UNDO_PAGE);
        return CM_ERROR;
    }

    //
    trx_rseg_t* rseg = TRX_GET_RSEG(sess->trx->trx_slot_id.rseg_id);
    page_t* page = trx_undo_get_page(rseg->undo_space_id, undo_page, undo_data->undo_op, mtr);

    uint32 first_free = mach_read_from_2(page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE);
    ut_ad(first_free <= UNIV_PAGE_SIZE);

    trx_undo_rec_hdr_t* rec_ptr = page + first_free;

    // 1. undo log record

    // Reserve 2 bytes for offset to the next undo log record
    rec_ptr += 2;
    // undo type
    mach_write_to_1(rec_ptr, undo_data->rec.type);
    rec_ptr += 1;
    // table id
    //mach_write_to_8(rec_ptr, table->id);
    //rec_ptr += 8;
    // undo data
    memcpy(rec_ptr, (byte *)&undo_data->rec, UNDO_REC_HEADER_SIZE);
    rec_ptr += UNDO_REC_HEADER_SIZE;
    if (undo_data->rec.data_size > 0) {
        memcpy(rec_ptr, undo_data->rec.data, undo_data->rec.data_size);
        rec_ptr += undo_data->rec.data_size;
    }
    // start offset of current undo log record
    mach_write_to_2(rec_ptr, first_free);
    rec_ptr += 2;

    // Write offset of the next undo log record
    uint16 next_rec_offset = (uint16)(rec_ptr - page);
    mach_write_to_2(page + first_free, next_rec_offset);

    // 2. Update the offset of the first free byte on the page
    undo_page->page_offset = next_rec_offset;
    mach_write_to_2(page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE, next_rec_offset);

    // 3. Write to the REDO log about this change in the UNDO log

    mlog_write_log(MLOG_UNDO_INSERT, rseg->undo_space_id, undo_page->page_no,
        page + first_free, (uint16)(rec_ptr - (page + first_free)), mtr);
    mlog_catenate_string(mtr, page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE, MLOG_2BYTES);

    return CM_SUCCESS;
}

static inline void trx_insert_undo_cleanup(trx_rseg_t* rseg, trx_t* trx)
{
    if (SLIST_GET_LEN(trx->insert_undo) == 0) {
        return;
    }

    bool32 is_reused = FALSE;

    // return insert_undo_page to rseg->insert_undo_cache
    mutex_enter(&(rseg->insert_undo_cache_mutex), NULL);
    if (SLIST_GET_LEN(trx->insert_undo) + SLIST_GET_LEN(rseg->insert_undo_cache) <= rseg->undo_cached_max_count) {
        SLIST_APPEND_SLIST(list_node, trx->insert_undo, rseg->insert_undo_cache);
        is_reused = TRUE;
    }
    mutex_exit(&(rseg->insert_undo_cache_mutex));

    // return insert_undo_page to undo table_space
    if (!is_reused) {
        trx_undo_release_page(rseg, trx->insert_undo, UNDO_PAGE_TYPE_INSERT, 0);
    }
}

static inline void trx_update_undo_cleanup(trx_rseg_t* rseg, trx_t* trx, bool32 is_commited, scn_t scn)
{
    if (SLIST_GET_LEN(trx->update_undo) == 0) {
        return;
    }

    uint64 scn_timestamp = scn >> 32;

    if (!is_commited) {
        mtr_t mtr;
        mtr_start(&mtr);
        // reset first undo_page
        trx_undo_page_t* undo_page = SLIST_GET_FIRST(trx->update_undo);
        page_t* page = trx_undo_get_page(rseg->undo_space_id, undo_page, UNDO_MODIFY_OP, &mtr);
        trx_undo_page_hdr_t* page_hdr = page + TRX_UNDO_PAGE_HDR;
        trx_undo_seg_hdr_t* seg_hdr = page + TRX_UNDO_SEG_HDR;
        trx_undo_log_hdr_t* undo_log_hdr = page + mach_read_from_2(seg_hdr + TRX_UNDO_LAST_LOG);
        uint32 prev_log_offset = mach_read_from_2(undo_log_hdr + TRX_UNDO_PREV_LOG);
        mlog_write_uint32(seg_hdr + TRX_UNDO_LAST_LOG, prev_log_offset, MLOG_2BYTES, &mtr);
        mlog_write_uint32(page_hdr + TRX_UNDO_PAGE_FREE, prev_log_offset, MLOG_2BYTES, &mtr);
        undo_page->page_offset = prev_log_offset;
        mtr_commit(&mtr);

        // first undo_page contains useful data,
        // return to rseg->update_undo_cache or tablespace UNDO_FSM_UPDATE_LIST
        if (mach_read_from_2(page_hdr + TRX_UNDO_PAGE_START) != prev_log_offset) {
            SLIST_REMOVE_FIRST(list_node, rseg->update_undo_cache);

            mutex_enter(&(rseg->update_undo_cache_mutex), NULL);
            if (1 + SLIST_GET_LEN(rseg->update_undo_cache) <= rseg->undo_cached_max_count) {
                // rseg->update_undo_cache is a traversal scan, does not require updating timestamps for undo_page
                SLIST_ADD_FIRST(list_node, rseg->update_undo_cache, undo_page);
                undo_page = NULL;
            }
            mutex_exit(&(rseg->update_undo_cache_mutex));
            if (undo_page) {
                trx_undo_page_base undo_page_base;
                SLIST_INIT(undo_page_base);
                SLIST_ADD_FIRST(list_node, undo_page_base, undo_page);

                // Due to not knowing the sorting position, use the latest timestamp
                trx_undo_release_page(rseg, undo_page_base, UNDO_PAGE_TYPE_UPDATE, scn_timestamp);
            }
        }

        // other undo_page can be fully reused,
        // return directly to rseg->insert_undo_cache or tablespace UNDO_FSM_INSERT_LIST
        bool32 is_reused = FALSE;
        mutex_enter(&(rseg->insert_undo_cache_mutex), NULL);
        if (SLIST_GET_LEN(trx->update_undo) + SLIST_GET_LEN(rseg->insert_undo_cache) <= rseg->undo_cached_max_count) {
            SLIST_APPEND_SLIST(list_node, trx->update_undo, rseg->insert_undo_cache);
            is_reused = TRUE;
        }
        mutex_exit(&(rseg->insert_undo_cache_mutex));
        if (!is_reused) {
            trx_undo_release_page(rseg, trx->update_undo, UNDO_PAGE_TYPE_INSERT, scn_timestamp);
        }
    } else { // committed
        trx_undo_page_t* undo_page = SLIST_GET_FIRST(trx->update_undo);
        if (SLIST_GET_LEN(trx->update_undo) == 1 &&
            UNDO_PAGE_LEFT(undo_page) >= UNDO_PAGE_REUSE_LIMIT &&
            SLIST_GET_LEN(rseg->update_undo_cache) + 1 <= rseg->undo_cached_max_count) {
            undo_page->scn_timestamp = scn_timestamp;
            mutex_enter(&(rseg->update_undo_cache_mutex), NULL);
            SLIST_ADD_LAST(list_node, rseg->update_undo_cache, undo_page);
            mutex_exit(&(rseg->update_undo_cache_mutex));
        } else {
            trx_undo_release_page(rseg, trx->update_undo, UNDO_PAGE_TYPE_UPDATE, scn_timestamp);
        }
    }
}

inline void trx_undo_cleanup(trx_t* trx, bool32 is_commited, scn_t scn)
{
    trx_rseg_t* rseg = TRX_GET_RSEG(trx->trx_slot_id.rseg_id);

    trx_insert_undo_cleanup(rseg, trx);

    trx_update_undo_cleanup(rseg, trx, is_commited, scn);
}


