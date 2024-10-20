#include "knl_trx_rseg.h"

#include "cm_log.h"

#include "knl_trx.h"
#include "knl_buf.h"
#include "knl_heap.h"




static const uint32  trx_slot_count_per_page =
    (UNIV_PAGE_SIZE - FIL_PAGE_DATA - FIL_PAGE_DATA_END - TRX_RSEG_SLOT_HEADER) / sizeof(trx_slot_t);
static uint32        trx_undo_page_no = 0;

static uint32        trx_undo_page_count_per_rseg = 0;



trx_rsegf_t* trx_rsegf_get(
    uint32              space_id,
    uint32              page_no,
    const page_size_t&  page_size,
    mtr_t*              mtr)
{
    buf_block_t*    block;
    trx_rsegf_t*    header;
    page_id_t       page_id(space_id, page_no);

    block = buf_page_get(page_id, page_size, RW_X_LATCH, mtr);

    //buf_block_dbg_add_level(block, SYNC_RSEG_HEADER_NEW);

    header = TRX_RSEG + buf_block_get_frame(block);

    return(header);
}


// Gets a pointer to the transaction system header and x-latches its page
inline trx_sysf_t* trx_sysf_get(mtr_t* mtr)
{
    buf_block_t* block;
    trx_sysf_t* header;
    page_id_t page_id(TRX_SYS_SPACE, TRX_SYS_PAGE_NO);
    const page_size_t page_size(0);

    block = buf_page_get(page_id, page_size, RW_X_LATCH, mtr);
    //buf_block_dbg_add_level(block, SYNC_TRX_SYS_HEADER);

    header = TRX_SYS + buf_block_get_frame(block);

    return header;
}


// Creates the file page for the transaction system.
// This function is called only at the database creation, before trx_sys_init.
static status_t trx_sysf_create()
{
    mtr_t mtr;

    mtr_start(&mtr);

    page_id_t page_id(TRX_SYS_SPACE, TRX_SYS_PAGE_NO);
    const page_size_t page_size(0);
    buf_block_t* block = buf_page_get(page_id, page_size, RW_X_LATCH, &mtr);
    ut_a(block->get_page_no() == TRX_SYS_PAGE_NO);

    page_t* page = buf_block_get_frame(block);
    mlog_write_uint32(page + FIL_PAGE_TYPE, FIL_PAGE_TRX_SYS, MLOG_2BYTES, &mtr);

    // 1. Transaction system header

    trx_sysf_t* sys_header = TRX_SYS + page;
    /* Start counting transaction ids from number 1 up */
    mach_write_to_8(sys_header + TRX_SYS_TRX_ID_STORE, 1);

    fseg_inode_t* inode = TRX_SYS_INODE + page;
    flst_init(inode + FSEG_FREE, &mtr);
    flst_init(inode + FSEG_NOT_FULL, &mtr);
    flst_init(inode + FSEG_FULL, &mtr);

    // 2. doublewrite

    //mlog_write_uint32(page + TRX_SYS_DOUBLEWRITE + TRX_SYS_DOUBLEWRITE_MAGIC, 0, MLOG_4BYTES, mtr);

    // 3. replication

    mtr_commit(&mtr);

    return CM_SUCCESS;
}

static status_t trx_rseg_undo_page_init(trx_rseg_t* rseg)
{
    trx_undo_page_t* undo_page;

    mutex_create(&rseg->undo_cache_mutex);

    SLIST_INIT(rseg->history_undo_list);
    SLIST_INIT(rseg->insert_undo_cache);
    SLIST_INIT(rseg->update_undo_cache);

    while (trx_undo_page_count_per_rseg > SLIST_GET_LEN(rseg->insert_undo_cache)) {
        undo_page = (trx_undo_page_t*)mcontext_stack_push(trx_sys->context, sizeof(trx_undo_page_t));
        if (undo_page == NULL) {
            return CM_ERROR;
        }
        undo_page->type = TRX_UNDO_INSERT;
        undo_page->scn = 0;
        undo_page->free_size = UNIV_PAGE_SIZE - FSEG_PAGE_DATA - FIL_PAGE_DATA_END
            - TRX_UNDO_PAGE_HDR_SIZE - TRX_UNDO_SEG_HDR_SIZE;
        undo_page->page_no = trx_undo_page_no;
        undo_page->block = NULL;
        trx_undo_page_no++;

        SLIST_ADD_LAST(list_node, rseg->insert_undo_cache, undo_page);
    }

    return CM_SUCCESS;
}

static status_t trx_rseg_trx_init(trx_rseg_t* rseg)
{
    mutex_create(&rseg->trx_mutex);
    SLIST_INIT(rseg->trx_base);

    uint32 trx_count = TRX_SLOT_PAGE_COUNT_PER_RSEG * trx_slot_count_per_page;
    rseg->trxs = (trx_t**)ut_malloc_zero(sizeof(void *) * trx_count);
    if (rseg->trxs == NULL) {
        return CM_ERROR;
    }

    trx_t* trx;
    uint32 slot = 0;
    while (trx_count > slot) {
        trx = (trx_t*)mcontext_stack_push(trx_sys->context, sizeof(trx_t));
        if (trx == NULL) {
            return CM_ERROR;
        }
        trx->trx_slot_id.rseg_id = rseg->id;
        trx->trx_slot_id.slot = slot;
        trx->trx_slot_id.xnum = 0;
        trx->is_active = FALSE;
        rseg->trxs[slot] = trx;
        SLIST_ADD_LAST(list_node, rseg->trx_base, trx);
        slot++;
    }

    return CM_SUCCESS;
}

static status_t trx_rseg_open_trx_slot(trx_rseg_t* rseg)
{
    mtr_t init_mtr, *mtr = &init_mtr;
    const page_size_t page_size(0);

    rseg->trx_slots = (void **)ut_malloc_zero(sizeof(void *) * trx_slot_count_per_page * TRX_SLOT_PAGE_COUNT_PER_RSEG);
    if (rseg->trx_slots == NULL) {
        return CM_ERROR;
    }

    mtr_start(mtr);

    for (uint32 i = 0; i < TRX_SLOT_PAGE_COUNT_PER_RSEG; i++) {
        //
        uint32 page_no = FSP_FIRST_RSEG_PAGE_NO + rseg->id * TRX_SLOT_PAGE_COUNT_PER_RSEG + i;
        page_id_t page_id(TRX_SYS_SPACE, page_no);
        rseg->trx_slot_blocks[i] = buf_page_get_gen(page_id, page_size, RW_X_LATCH, Page_fetch::RESIDENT, mtr);
        ut_a(rseg->trx_slot_blocks[i]->get_page_no() == page_no);
        //buf_block_dbg_add_level(rseg->trx_slot_blocks[i], SYNC_RSEG_HEADER_NEW);
        page_t* page = buf_block_get_frame(rseg->trx_slot_blocks[i]);

        for (uint32 j = 0; j < trx_slot_count_per_page; j++) {
            trx_slot_t* slot = (trx_slot_t *)(page + TRX_RSEG + TRX_RSEG_SLOT_HEADER + j * sizeof(trx_slot_t));
            rseg->trx_slots[i * trx_slot_count_per_page + j] = slot;
        }
    }

    mtr_commit(mtr);

    return CM_SUCCESS;
}

static status_t trx_rseg_create_trx_slot(trx_rseg_t* rseg)
{
    mtr_t init_mtr, *mtr = &init_mtr;
    const page_size_t page_size(0);

    mtr_start(mtr);

    rseg->trx_slots = (void **)ut_malloc_zero(sizeof(void*) * trx_slot_count_per_page * TRX_SLOT_PAGE_COUNT_PER_RSEG);

    for (uint32 i = 0; i < TRX_SLOT_PAGE_COUNT_PER_RSEG; i++) {
        //
        uint32 page_no = FSP_FIRST_RSEG_PAGE_NO + rseg->id * TRX_SLOT_PAGE_COUNT_PER_RSEG + i;
        page_id_t page_id(TRX_SYS_SPACE, page_no);
        rseg->trx_slot_blocks[i] = buf_page_get_gen(page_id, page_size, RW_X_LATCH, Page_fetch::RESIDENT, mtr);
        ut_a(rseg->trx_slot_blocks[i]->get_page_no() == page_no);
        //buf_block_dbg_add_level(rseg->trx_slot_blocks[i], SYNC_RSEG_HEADER_NEW);

        //
        page_t* page = buf_block_get_frame(rseg->trx_slot_blocks[i]);
        mlog_write_uint32(page + FIL_PAGE_TYPE, FIL_PAGE_TRX_SLOT, MLOG_2BYTES, NULL);

        trx_rsegf_t* rsegf = page + TRX_RSEG;
        /* Initialize the history list */
        mlog_write_uint32(rsegf + TRX_RSEG_HISTORY_SIZE, 0, MLOG_4BYTES, NULL);
        flst_init(rsegf + TRX_RSEG_HISTORY, NULL);

        fseg_inode_t* inode = TRX_RSEG_FSEG_HEADER + TRX_RSEG + page;
        flst_init(inode + FSEG_FREE, NULL);
        flst_init(inode + FSEG_NOT_FULL, NULL);
        flst_init(inode + FSEG_FULL, NULL);

        for (uint32 j = 0; j < trx_slot_count_per_page; j++) {
            trx_slot_t* slot = (trx_slot_t *)(page + TRX_RSEG + TRX_RSEG_SLOT_HEADER + j * sizeof(trx_slot_t));
            slot->scn = 0;
            slot->insert_page_no = FIL_NULL;
            slot->update_page_no = FIL_NULL;
            slot->status = XACT_END;
            slot->reserved = 0;
            slot->xnum = 0;
            rseg->trx_slots[i * trx_slot_count_per_page + j] = slot;
        }

        // redo
        mlog_write_initial_log_record(page, MLOG_TRX_RSEG_PAGE_INIT, mtr);
        mlog_catenate_uint32(mtr, rseg->id, MLOG_1BYTE);
        mlog_catenate_uint32(mtr, page_no, MLOG_4BYTES);
    }

    mtr_commit(mtr);

    return CM_SUCCESS;
}

static status_t trx_sys_calc_page_count_per_rseg(uint64 undo_cache_size)
{
    if (trx_sys->rseg_count < TRX_RSEG_MIN_COUNT || trx_sys->rseg_count > TRX_RSEG_MAX_COUNT) {
        LOGGER_INFO(LOGGER, "Error, invalid undo segment, count = %d", trx_sys->rseg_count);
        return CM_ERROR;
    }

    trx_undo_page_count_per_rseg = undo_cache_size / UNIV_PAGE_SIZE / trx_sys->rseg_count;
    if (trx_undo_page_count_per_rseg == 0) {
        LOGGER_INFO(LOGGER, "Error, invalid undo segment, size = %llu", undo_cache_size);
        return CM_ERROR;
    }

    return CM_SUCCESS;
}

// Creates and initializes the transaction system at the database creation.
status_t trx_sys_create_undo_segments(uint64 undo_cache_size)
{
    status_t err;

    LOGGER_INFO(LOGGER, "transaction rollback segment initialize");

    err = trx_sys_calc_page_count_per_rseg(undo_cache_size);
    CM_RETURN_IF_ERROR(err);

    err = trx_sysf_create();
    CM_RETURN_IF_ERROR(err);

    // Create all rollback segment in the SYSTEM tablespace
    trx_rseg_t* rseg;
    for (uint32 rseg_id = 0;
         rseg_id < TRX_RSEG_MAX_COUNT && rseg_id < trx_sys->rseg_count;
         rseg_id++) {

        rseg = TRX_GET_RSEG(rseg_id);
        ut_ad(rseg->id == 0xFFFFFFFF);
        rseg->id = rseg_id;

        err = trx_rseg_undo_page_init(rseg);
        CM_RETURN_IF_ERROR(err);

        err = trx_rseg_trx_init(rseg);
        CM_RETURN_IF_ERROR(err);

        err = trx_rseg_create_trx_slot(rseg);
        CM_RETURN_IF_ERROR(err);
    }

    return CM_SUCCESS;
}

status_t trx_sys_open_undo_segments(uint64 undo_cache_size)
{
    status_t err;

    err = trx_sys_calc_page_count_per_rseg(undo_cache_size);
    CM_RETURN_IF_ERROR(err);

    // Create all rollback segment in the SYSTEM tablespace
    trx_rseg_t* rseg;
    for (uint32 rseg_id = 0;
         rseg_id < TRX_RSEG_MAX_COUNT && rseg_id < trx_sys->rseg_count;
         rseg_id++) {

        rseg = TRX_GET_RSEG(rseg_id);
        ut_ad(rseg->id == 0xFFFFFFFF);
        rseg->id = rseg_id;

        err = trx_rseg_undo_page_init(rseg);
        CM_RETURN_IF_ERROR(err);

        err = trx_rseg_trx_init(rseg);
        CM_RETURN_IF_ERROR(err);

        err = trx_rseg_open_trx_slot(rseg);
        CM_RETURN_IF_ERROR(err);
    }

    return CM_SUCCESS;
}


static inline bool32 trx_rset_check_trx_slot(trx_t* trx)
{
    bool32 ret = TRUE;
    mtr_t mtr;
    page_t* page;
    buf_block_t* block;
    const page_size_t page_size(0);
    page_id_t page_id(TRX_SYS_SPACE, TRX_GET_RSEG_TRX_SLOT_PAGE_NO(trx->trx_slot_id));
    trx_rseg_t* rseg = TRX_GET_RSEG(trx->trx_slot_id.rseg_id);
    trx_slot_t* slot = (trx_slot_t *)TRX_GET_RSEG_TRX_SLOT(trx->trx_slot_id);

    mtr_start(&mtr);

    block = buf_page_get_gen(page_id, page_size, RW_NO_LATCH, Page_fetch::RESIDENT, &mtr);
    if (block != TRX_GET_RSEG_TRX_SLOT_BLOCK(trx->trx_slot_id)) {
        ret = FALSE;
    }

    page = buf_block_get_frame(block);
    if ((byte *)slot != page + trx->trx_slot_id.slot * sizeof(trx_slot_t)) {
        ret = FALSE;
    }

    mtr_commit(&mtr);

    return ret;
}

static inline trx_t* trx_rseg_alloc_trx(trx_rseg_t* rseg, mtr_t* mtr)
{
    trx_t* trx;

    mutex_enter(&rseg->trx_mutex);
    SLIST_GET_AND_REMOVE_FIRST(list_node, rseg->trx_base, trx);
    mutex_exit(&rseg->trx_mutex);

    if (LIKELY(trx)) {
        trx_slot_t* slot = (trx_slot_t *)TRX_GET_RSEG_TRX_SLOT(trx->trx_slot_id);

        ut_ad(trx_rset_check_trx_slot(trx));

        ut_a(slot->status == XACT_END);
        slot->status = XACT_BEGIN;
        slot->xnum++;

        ut_a(trx->is_active == FALSE);
        trx->is_active = TRUE;
        trx->trx_slot_id.xnum = slot->xnum;

        // redo log
        mlog_write_initial_log_record((byte *)slot, MLOG_TRX_RSEG_SLOT_BEGIN, mtr);
        mlog_catenate_uint64(mtr, trx->trx_slot_id.id);

        buf_block_mark_dirty(TRX_GET_RSEG_TRX_SLOT_BLOCK(trx->trx_slot_id), mtr);
    }

    return trx;
}

inline trx_t* trx_rseg_assign_and_alloc_trx(mtr_t* mtr)
{
    trx_t* trx = NULL;
    uint64 index = ut_rnd_gen_uint64();

    for (uint32 i = 0; i < trx_sys->rseg_count; i++) {
        trx_rseg_t* rseg = &trx_sys->rseg_array[(index + i) % trx_sys->rseg_count];
        trx = trx_rseg_alloc_trx(rseg, mtr);
        if (trx) {
            break;
        }
    }

    return trx;
}


static inline uint64 trx_inc_scn(time_t init_time, struct timeval* now, uint64 seq, atomic64_t* scn)
{
    uint64 curr_scn;

    if (now->tv_sec < init_time) {
        return atomic64_inc(scn);
    }

    curr_scn = TRX_TIMESEQ_TO_SCN(now, init_time, seq);
    if (curr_scn <= TRX_GET_SCN(scn)) {
        return TRX_INC_SCN(scn);
    }

    while (TRX_GET_SCN(scn) < curr_scn && !atomic64_compare_and_swap(scn, TRX_GET_SCN(scn), curr_scn)) {
        // nothing to do
    }

    return curr_scn;
}

inline uint64 trx_get_next_scn()
{
    uint64 seq = 1;
    uint64 scn;
    struct timeval now;

    get_time_of_day(&now);

    scn = trx_inc_scn(trx_sys->init_time, &now, seq, &trx_sys->scn);

    return scn;
}

inline void trx_rseg_set_end(trx_t* trx, mtr_t* mtr)
{
    trx_slot_t* slot = (trx_slot_t *)TRX_GET_RSEG_TRX_SLOT(trx->trx_slot_id);
    uint64 scn = trx_get_next_scn();

    ut_ad(trx_rset_check_trx_slot(trx));

    slot->scn = scn;
    slot->status = XACT_END;

    // redo log
    mlog_write_initial_log_record((byte *)slot, MLOG_TRX_RSEG_SLOT_END, mtr);
    mlog_catenate_uint64(mtr, trx->trx_slot_id.id);
    mlog_catenate_uint64(mtr, scn);

    buf_block_mark_dirty(TRX_GET_RSEG_TRX_SLOT_BLOCK(trx->trx_slot_id), mtr);
}

inline void trx_rseg_release_trx(trx_t* trx)
{
    trx_rseg_t* rseg = TRX_GET_RSEG(trx->trx_slot_id.rseg_id);

    ut_a(trx->is_active);
    trx->is_active = FALSE;

    mutex_enter(&rseg->trx_mutex);
    SLIST_ADD_LAST(list_node, rseg->trx_base, trx);
    mutex_exit(&rseg->trx_mutex);
}

inline void trx_get_status_by_itl(trx_slot_id_t trx_slot_id, trx_status_t* trx_status)
{
    trx_rseg_t* rseg = TRX_GET_RSEG(trx_slot_id.rseg_id);
    trx_slot_t* slot = (trx_slot_t *)TRX_GET_RSEG_TRX_SLOT(trx_slot_id);
    trx_t* trx = TRX_GET_RSEG_TRX(trx_slot_id);

    if (slot->xnum == trx_slot_id.xnum) {
        trx_status->is_overwrite_scn = FALSE;
        if (slot->status == XACT_XA_PREPARE || slot->status == XACT_XA_ROLLBACK) {
            trx_status->scn = slot->scn;
            trx_status->status = slot->status;
        } else if (slot->status != XACT_END || trx->is_active) {
            trx_status->scn = 0;
            trx_status->status = XACT_BEGIN;
        } else {
            trx_status->scn = slot->scn;
            trx_status->status = XACT_END;
        }
    } else {
        trx_status->scn = slot->scn;
        trx_status->status = XACT_END;
    }
}

