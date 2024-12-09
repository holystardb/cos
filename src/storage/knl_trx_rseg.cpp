#include "knl_trx_rseg.h"

#include "cm_log.h"

#include "knl_trx.h"
#include "knl_buf.h"
#include "knl_heap.h"



const page_size_t systrans_page_size(DB_SYSTRANS_SPACE_ID);

static const uint32  trx_slot_count_per_page = TRX_SLOT_COUNT_PER_PAGE;


// Gets a pointer to the transaction system header and x-latches its page
inline trx_sysf_t* trx_sysf_get(mtr_t* mtr)
{
    buf_block_t* block;
    trx_sysf_t* header;
    page_id_t page_id(DB_SYSTEM_SPACE_ID, TRX_SYS_PAGE_NO);
    const page_size_t page_size(DB_SYSTEM_SPACE_ID);

    block = buf_page_get(page_id, page_size, RW_X_LATCH, mtr);
    //buf_block_dbg_add_level(block, SYNC_TRX_SYS_HEADER);

    header = TRX_SYS + buf_block_get_frame(block);

    return header;
}


// Creates the file page for the transaction system.
// This function is called only at the database creation, before trx_sys_init.
status_t trx_sysf_create()
{
    mtr_t mtr;

    mtr_start(&mtr);

    page_id_t page_id(DB_SYSTEM_SPACE_ID, TRX_SYS_PAGE_NO);
    const page_size_t page_size(DB_SYSTEM_SPACE_ID);
    buf_block_t* block = buf_page_create(page_id, page_size, RW_X_LATCH, Page_fetch::NORMAL, &mtr);
    ut_a(block->get_page_no() == TRX_SYS_PAGE_NO);

    page_t* page = buf_block_get_frame(block);
    mlog_write_uint32(page + FIL_PAGE_TYPE, FIL_PAGE_TYPE_TRX_SYS, MLOG_2BYTES, &mtr);

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

static status_t trx_sys_extend_free_undo_page()
{
    mutex_enter(&trx_sys->used_pages_mutex, NULL);
    if (SLIST_GET_LEN(trx_sys->free_undo_page_list) > 0) {
        mutex_exit(&trx_sys->used_pages_mutex);
        return CM_SUCCESS;
    }

    if (trx_sys->extend_in_process) {
        mutex_exit(&trx_sys->used_pages_mutex);
        os_thread_sleep(20);
        return CM_SUCCESS;
    }

    trx_sys->extend_in_process = TRUE;
    mutex_exit(&trx_sys->used_pages_mutex);

    memory_page_t* page = mpool_alloc_page(trx_sys->mem_pool);
    if (page) {
        mutex_enter(&trx_sys->used_pages_mutex, NULL);
        trx_sys->extend_in_process = FALSE;
        mutex_exit(&trx_sys->used_pages_mutex);
        return CM_ERROR;
    }

    //
    uint32 count = trx_sys->mem_pool->page_size / sizeof(trx_undo_page_t);
    trx_undo_page_t* undo_page;

    mutex_enter(&trx_sys->used_pages_mutex, NULL);
    UT_LIST_ADD_LAST(list_node, trx_sys->used_pages, page);

    for (uint32 i = 0; i < count; i++) {
        undo_page = (trx_undo_page_t*)(MEM_PAGE_DATA_PTR(page) + i * sizeof(trx_undo_page_t));
        SLIST_ADD_LAST(list_node, trx_sys->free_undo_page_list, undo_page);
    }

    trx_sys->extend_in_process = FALSE;
    mutex_exit(&trx_sys->used_pages_mutex);

    return CM_SUCCESS;
}

static status_t trx_rseg_extend_free_undo_page(trx_rseg_t* rseg)
{
    trx_undo_page_t* undo_page;
    const uint32 fetch_count_per = 32;
    SLIST_BASE_NODE_T(trx_undo_page_t) free_undo_list;
    SLIST_INIT(free_undo_list);

retry:

    mutex_enter(&trx_sys->used_pages_mutex, NULL);
    undo_page = SLIST_GET_FIRST(trx_sys->free_undo_page_list);
    for (uint32 i  = 0; i < fetch_count_per && undo_page; i++) {
        SLIST_REMOVE_FIRST(list_node, trx_sys->free_undo_page_list);
        SLIST_ADD_LAST(list_node, free_undo_list, undo_page);
        undo_page = SLIST_GET_FIRST(trx_sys->free_undo_page_list);
    }
    mutex_exit(&trx_sys->used_pages_mutex);

    // extend
    if (SLIST_GET_LEN(free_undo_list) == 0) {
        CM_RETURN_IF_ERROR(trx_sys_extend_free_undo_page());
        goto retry;
    }

    //
    mutex_enter(&rseg->free_undo_list_mutex, NULL);
    SLIST_APPEND_SLIST(list_node, free_undo_list, rseg->free_undo_list);
    mutex_exit(&rseg->free_undo_list_mutex);

    return CM_SUCCESS;
}

status_t trx_rseg_undo_page_init(trx_rseg_t* rseg)
{
    mutex_create(&rseg->insert_undo_cache_mutex);
    mutex_create(&rseg->update_undo_cache_mutex);
    mutex_create(&rseg->free_undo_list_mutex);

    SLIST_INIT(rseg->free_undo_list);
    SLIST_INIT(rseg->insert_undo_cache);
    SLIST_INIT(rseg->update_undo_cache);

    //
    if (!trx_rseg_extend_free_undo_page(rseg)) {
        return CM_ERROR;
    }

    return CM_SUCCESS;
}

inline trx_undo_page_t* trx_rseg_alloc_free_undo_page(trx_rseg_t* rseg)
{
    trx_undo_page_t* undo_page;

retry:

    mutex_enter(&rseg->free_undo_list_mutex, NULL);
    undo_page = SLIST_GET_FIRST(rseg->free_undo_list);
    if (undo_page) {
        SLIST_REMOVE_FIRST(list_node, rseg->free_undo_list)
    }
    mutex_exit(&rseg->free_undo_list_mutex);
    if (undo_page) {
        return undo_page;
    }

    // extend free_undo_page

    mutex_enter(&rseg->free_undo_list_mutex, NULL);
    if (rseg->extend_undo_in_process) {
        mutex_exit(&rseg->free_undo_list_mutex);
        os_thread_sleep(20);
        goto retry;
    }
    rseg->extend_undo_in_process = TRUE;
    mutex_exit(&rseg->free_undo_list_mutex);

    //
    status_t is_success = trx_rseg_extend_free_undo_page(rseg);

    mutex_enter(&rseg->free_undo_list_mutex, NULL);
    rseg->extend_undo_in_process = FALSE;
    mutex_exit(&rseg->free_undo_list_mutex);

    if (is_success == CM_SUCCESS) {
        goto retry;
    }

    return NULL;
}

inline void trx_rseg_free_free_undo_page(trx_rseg_t* rseg, trx_undo_page_t* undo_page)
{
    ut_ad(rseg);
    ut_ad(undo_page);

    mutex_enter(&rseg->free_undo_list_mutex, NULL);
    if (SLIST_GET_LEN(rseg->free_undo_list) < rseg->undo_cached_max_count) {
        SLIST_ADD_LAST(list_node, rseg->free_undo_list, undo_page);
        undo_page = NULL;
    }
    mutex_exit(&rseg->free_undo_list_mutex);

    // return undo_page to trx_sys->free_undo_page_list
    if (undo_page) {
        mutex_enter(&trx_sys->used_pages_mutex, NULL);
        SLIST_ADD_LAST(list_node, trx_sys->free_undo_page_list, undo_page);
        mutex_exit(&trx_sys->used_pages_mutex);
    }
}

status_t trx_rseg_open_trx_slots(trx_rseg_t* rseg)
{
    mtr_t init_mtr, *mtr = &init_mtr;

    mtr_start(mtr);

    for (uint32 i = 0; i < TRX_SLOT_PAGE_COUNT_PER_RSEG; i++) {
        // get block
        uint32 page_no = rseg->id * TRX_SLOT_PAGE_COUNT_PER_RSEG + i;
        page_id_t page_id(DB_SYSTRANS_SPACE_ID, page_no);
        rseg->trx_slot_blocks[i] = buf_page_get(page_id, systrans_page_size, RW_X_LATCH, mtr);
        ut_a(rseg->trx_slot_blocks[i]->get_page_no() == page_no);
        //buf_block_dbg_add_level(rseg->trx_slot_blocks[i], SYNC_RSEG_HEADER_NEW);

        //
        page_t* page = buf_block_get_frame(rseg->trx_slot_blocks[i]);
        trx_slot_page_hdr_t* header = page + TRX_SLOT_PAGE_HDR;

        //
        for (uint32 j = 0; j < TRX_SLOT_COUNT_PER_PAGE; j++) {
            trx_slot_t* slot = (trx_slot_t *)(header + TRX_SLOT_PAGE_HEADER_SIZE + j * sizeof(trx_slot_t));

            uint32 slot_idx = i * TRX_SLOT_COUNT_PER_PAGE + j;
            rseg->trx_slot_list[slot_idx] = slot;

            trx_t* trx = &rseg->trx_list[slot_idx];
            trx->trx_slot_id.rseg_id = rseg->id;
            trx->trx_slot_id.slot = slot_idx;
            trx->trx_slot_id.xnum = slot->xnum;
            if (slot->status == XACT_END) {
                trx->is_active = FALSE;
            } else {
                trx->is_active = TRUE;
            }
        }
    }

    mtr_commit(mtr);

    return CM_SUCCESS;
}


status_t trx_rseg_create_trx_slots(trx_rseg_t* rseg)
{
    mtr_t init_mtr, *mtr = &init_mtr;

    mtr_start(mtr);

    for (uint32 i = 0; i < TRX_SLOT_PAGE_COUNT_PER_RSEG; i++) {
        // get block
        uint32 page_no = rseg->id * TRX_SLOT_PAGE_COUNT_PER_RSEG + i;
        page_id_t page_id(DB_SYSTRANS_SPACE_ID, page_no);
        rseg->trx_slot_blocks[i] = buf_page_create(page_id, systrans_page_size, RW_X_LATCH, Page_fetch::RESIDENT, mtr);
        ut_a(rseg->trx_slot_blocks[i]->get_page_no() == page_no);
        //buf_block_dbg_add_level(rseg->trx_slot_blocks[i], SYNC_RSEG_HEADER_NEW);

        //
        page_t* page = buf_block_get_frame(rseg->trx_slot_blocks[i]);
        mach_write_to_2(page + FIL_PAGE_TYPE, FIL_PAGE_TYPE_TRX_SYS | FIL_PAGE_TYPE_RESIDENT_FLAG);

        //
        trx_slot_page_hdr_t* header = page + TRX_SLOT_PAGE_HDR;
        mach_write_to_8(header + TRX_SLOT_PAGE_OWSCN, INVALID_SCN);
        rseg->page_ow_scn[i] = INVALID_SCN;

        //
        for (uint32 j = 0; j < TRX_SLOT_COUNT_PER_PAGE; j++) {
            trx_slot_t* slot = (trx_slot_t *)(header + TRX_SLOT_PAGE_HEADER_SIZE + j * sizeof(trx_slot_t));
            slot->scn = INVALID_SCN;
            slot->insert_page_no = FIL_NULL;
            slot->update_page_no = FIL_NULL;
            slot->status = XACT_END;
            slot->reserved = 0;
            slot->xnum = 0;

            uint32 slot_idx = i * TRX_SLOT_COUNT_PER_PAGE + j;
            rseg->trx_slot_list[slot_idx] = slot;

            trx_t* trx = &rseg->trx_list[slot_idx];
            trx->trx_slot_id.rseg_id = rseg->id;
            trx->trx_slot_id.slot = slot_idx;
            trx->trx_slot_id.xnum = 0;
            trx->is_active = FALSE;
        }

        // redo
        mlog_write_initial_log_record(page, MLOG_TRX_RSEG_PAGE_INIT, mtr);
        mlog_catenate_uint32(mtr, rseg->id, MLOG_1BYTE);
        mlog_catenate_uint32(mtr, page_no, MLOG_4BYTES);
    }

    mtr_commit(mtr);

    return CM_SUCCESS;
}

// Creates and initializes the transaction system at the database creation.
status_t trx_sys_create_or_open_undo_rsegs(bool32 is_create_database)
{
    status_t err;

    LOGGER_INFO(LOGGER, LOG_MODULE_ROLLSEGMENT, "trx_sys_create_or_open_undo_rsegs: initializing ...");

    // Create all rollback segment in the SYSTRANS tablespace
    for (uint32 rseg_id = 0; rseg_id < TRX_RSEG_COUNT; rseg_id++) {
        trx_rseg_t* rseg = TRX_GET_RSEG(rseg_id);
        ut_ad(rseg->id == rseg_id);

        err = is_create_database ? trx_rseg_create_trx_slots(rseg) : trx_rseg_open_trx_slots(rseg);
        if (err != CM_SUCCESS) {
            LOGGER_INFO(LOGGER, LOG_MODULE_ROLLSEGMENT,
                "trx_sys_create_or_open_undo_rsegs: Failed to initialize rseg, rseg_id = %u", rseg_id);
            return CM_ERROR;
        }
    }

    LOGGER_INFO(LOGGER, LOG_MODULE_ROLLSEGMENT, "trx_sys_create_or_open_undo_rsegs: initialized");

    return CM_SUCCESS;
}

static inline buf_block_t* trx_rseg_xlock_slot_page(trx_slot_id_t trx_slot_id, mtr_t* mtr)
{
    page_id_t page_id(TRX_SYS_SPACE, TRX_GET_RSEG_TRX_SLOT_PAGE_NO(trx_slot_id));
    buf_block_t* guess_block = TRX_GET_RSEG_TRX_SLOT_BLOCK(trx_slot_id);
    buf_block_t* block = buf_page_get_gen(page_id, systrans_page_size, RW_X_LATCH, guess_block, Page_fetch::NORMAL, mtr);
    ut_ad(block == TRX_GET_RSEG_TRX_SLOT_BLOCK(trx_slot_id));

    return block;
}

static inline trx_t* trx_rseg_alloc_trx(trx_rseg_t* rseg)
{
    trx_t* trx;

    // get trx from trx_free_list
    mutex_enter(&rseg->trx_mutex);
    trx = SLIST_GET_FIRST(rseg->trx_free_list);
    if (trx) {
        SLIST_REMOVE_FIRST(list_node, rseg->trx_free_list);
    }
    mutex_exit(&rseg->trx_mutex);

    if (UNLIKELY(trx)) {
        return NULL;
    }

    mtr_t mtr;
    mtr_start(&mtr);

    // get slot
    ut_ad(trx->trx_slot_id.rseg_id < TRX_RSEG_COUNT);
    ut_ad(trx->trx_slot_id.slot < TRX_SLOT_COUNT_PER_PAGE * TRX_SLOT_PAGE_COUNT_PER_RSEG);
    trx_slot_t* slot = (trx_slot_t *)TRX_GET_RSEG_TRX_SLOT(trx->trx_slot_id);
    ut_a(slot->status == XACT_END);

    // get block without rw_lock
    buf_block_t* guess_block = TRX_GET_RSEG_TRX_SLOT_BLOCK(trx->trx_slot_id);
#ifdef UNIV_DEBUG
    page_id_t page_id(TRX_SYS_SPACE, TRX_GET_RSEG_TRX_SLOT_PAGE_NO(trx->trx_slot_id));
    buf_block_t* block = buf_page_get_gen(page_id, systrans_page_size, RW_X_LATCH, guess_block, Page_fetch::PEEK_IF_IN_POOL, &mtr);
    ut_ad(block == guess_block);
#endif /* UNIV_DEBUG */

    // overwrite scn
    uint64 ow_scn = INVALID_SCN;
    trx_slot_page_hdr_t* header = NULL;
    if (slot->status == XACT_END && slot->scn != INVALID_SCN) {
        ow_scn = slot->scn;
        header = buf_block_get_frame(guess_block) + TRX_SLOT_PAGE_HDR;
    }

    //
    mutex_enter(&trx->mutex, NULL);
    ut_ad(trx->is_active == FALSE);
    trx->is_active = TRUE;
    slot->status = XACT_BEGIN;
    slot->xnum++;
    trx->trx_slot_id.xnum = slot->xnum;
    mutex_exit(&trx->mutex);
    SLIST_INIT(trx->insert_undo);
    SLIST_INIT(trx->update_undo);

    // lock block
    buf_block_lock_and_fix(guess_block, RW_X_LATCH, &mtr);

    // overwrite scn
    if (ow_scn != INVALID_SCN) {
        if (ow_scn > mach_read_from_8(header + TRX_SLOT_PAGE_OWSCN)) {
            mlog_write_uint64(header + TRX_SLOT_PAGE_OWSCN, ow_scn, &mtr);
            rseg->page_ow_scn[trx->trx_slot_id.slot / trx_slot_count_per_page] = ow_scn;
        }
    }

    // redo log
    mlog_write_initial_log_record((byte *)slot, MLOG_TRX_RSEG_SLOT_BEGIN, &mtr);
    mlog_catenate_uint64(&mtr, trx->trx_slot_id.id);

    mtr_commit(&mtr);

    return trx;
}

inline trx_t* trx_rseg_assign_and_alloc_trx()
{
    trx_t* trx = NULL;
    uint64 index = ut_rnd_gen_uint64();

    for (uint32 i = 0; i < TRX_RSEG_COUNT; i++) {
        trx_rseg_t* rseg = &trx_sys->rseg_array[(index + i) & (TRX_RSEG_COUNT - 1)];
        trx = trx_rseg_alloc_trx(rseg);
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
    if (curr_scn <= TRX_SCN_GET(scn)) {
        return TRX_SCN_INC(scn);
    }

    while (TRX_SCN_GET(scn) < curr_scn && !atomic64_compare_and_swap(scn, TRX_SCN_GET(scn), curr_scn)) {
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

inline scn_t trx_rseg_set_end(trx_t* trx, bool32 is_commit)
{
    mtr_t mtr;
    trx_slot_t* slot;
    uint64 scn = trx_get_next_scn();

    mtr_start(&mtr);

    // get slot
    ut_ad(trx->trx_slot_id.rseg_id < TRX_RSEG_COUNT);
    ut_ad(trx->trx_slot_id.slot < trx_slot_count_per_page * TRX_SLOT_PAGE_COUNT_PER_RSEG);
    slot = (trx_slot_t *)TRX_GET_RSEG_TRX_SLOT(trx->trx_slot_id);

    // get block without rw_lock
    buf_block_t* guess_block = TRX_GET_RSEG_TRX_SLOT_BLOCK(trx->trx_slot_id);
#ifdef UNIV_DEBUG
    page_id_t page_id(TRX_SYS_SPACE, TRX_GET_RSEG_TRX_SLOT_PAGE_NO(trx->trx_slot_id));
    buf_block_t* block = buf_page_get_gen(page_id, systrans_page_size, RW_X_LATCH, guess_block, Page_fetch::PEEK_IF_IN_POOL, &mtr);
    ut_ad(block == guess_block);
#endif /* UNIV_DEBUG */

    //
    mutex_enter(&trx->mutex, NULL);
    slot->scn = scn;
    slot->status = XACT_END;
    trx->is_active = FALSE;
    mutex_exit(&trx->mutex);

    // lock block
    buf_block_lock_and_fix(guess_block, RW_X_LATCH, &mtr);

    // redo log
    mlog_write_initial_log_record((byte *)slot, MLOG_TRX_RSEG_SLOT_END, &mtr);
    mlog_catenate_uint64(&mtr, trx->trx_slot_id.id);
    mlog_catenate_uint64(&mtr, scn);
    mlog_catenate_uint32(&mtr, is_commit, MLOG_1BYTE);

    mtr_commit(&mtr);

    return scn;
}

inline void trx_rseg_release_trx(trx_t* trx)
{
    trx_rseg_t* rseg = TRX_GET_RSEG(trx->trx_slot_id.rseg_id);

    ut_ad(trx->is_active);

    mutex_enter(&rseg->trx_mutex);
    SLIST_ADD_LAST(list_node, rseg->trx_free_list, trx);
    mutex_exit(&rseg->trx_mutex);
}

inline void trx_get_status_by_itl(trx_slot_id_t trx_slot_id, trx_status_t* trx_status)
{
    trx_slot_t snapshot;
    bool32 is_active;
    trx_slot_t* slot = (trx_slot_t *)TRX_GET_RSEG_TRX_SLOT(trx_slot_id);
    trx_t* trx = &TRX_GET_RSEG_TRX(trx_slot_id);

    mutex_enter(&trx->mutex, NULL);
    snapshot.xnum = slot->xnum;
    snapshot.status = slot->status;
    snapshot.scn = slot->scn;
    is_active = trx->is_active;
    mutex_exit(&trx->mutex);

    if (snapshot.xnum == trx_slot_id.xnum) {
        trx_status->is_ow_scn = FALSE;
        if (snapshot.status == XACT_XA_PREPARE || snapshot.status == XACT_XA_ROLLBACK) {
            trx_status->scn = snapshot.scn;
            trx_status->status = snapshot.status;
        } else if (snapshot.status != XACT_END || is_active) {
            trx_status->scn = 0;// (session);
            trx_status->status = XACT_BEGIN;
        } else {
            trx_status->scn = snapshot.scn;
            trx_status->status = XACT_END;
        }
    } else if (snapshot.xnum + 1 == trx_slot_id.xnum && snapshot.status == (uint8)XACT_BEGIN) {
        /*
         * To increase transaction info retention time, we would not overwrite
         * transaction scn when we are reusing a committed transaction. So, we
         * can get commit version from current transaction directly.
         */
        trx_status->scn = snapshot.scn;
        trx_status->is_ow_scn = FALSE;
        trx_status->status = (uint8)XACT_END;

    } else {
        trx_rseg_t* rseg = TRX_GET_RSEG(trx_slot_id.rseg_id);
        trx_status->scn = rseg->page_ow_scn[trx_slot_id.slot / trx_slot_count_per_page];
        trx_status->is_ow_scn = TRUE;
        trx_status->status = XACT_END;
    }
}


// ---------------------------------------------------------

byte* trx_rseg_replay_init_page(uint32 type, byte* log_rec_ptr, byte* log_end_ptr, void* block)
{
    ut_ad(type == MLOG_TRX_RSEG_PAGE_INIT);
    ut_a(log_end_ptr - log_rec_ptr >= 5);

    uint32 rseg_id = mach_read_from_1(log_rec_ptr);
    log_rec_ptr += 1;
    ut_ad(rseg_id < TRX_RSEG_COUNT);

    uint32 page_no = mach_read_from_4(log_rec_ptr);
    log_rec_ptr += 4;
    ut_ad(page_no >= FSP_FIRST_RSEG_PAGE_NO + rseg_id * TRX_SLOT_PAGE_COUNT_PER_RSEG);
    ut_ad(page_no < FSP_FIRST_RSEG_PAGE_NO + (rseg_id + 1) * TRX_SLOT_PAGE_COUNT_PER_RSEG);
    ut_ad(((buf_block_t*)block)->get_page_no() == page_no);

    LOGGER_TRACE(LOGGER, LOG_MODULE_RECOVERY,
        "trx_rseg_replay_init_page: type %lu block (%p space_id %lu page_no %lu) rseg_id %lu",
        type, block, block ? ((buf_block_t *)block)->get_space_id() : INVALID_SPACE_ID,
        block ? ((buf_block_t *)block)->get_page_no() : INVALID_PAGE_NO, rseg_id);

    //trx_rseg_t* rseg = &trx_sys->rseg_array[rseg_id];

    return log_rec_ptr;
}

byte* trx_rseg_replay_begin_slot(uint32 type, byte* log_rec_ptr, byte* log_end_ptr, void* block)
{
    ut_ad(type == MLOG_TRX_RSEG_SLOT_BEGIN);
    ut_a(log_end_ptr - log_rec_ptr >= 8);

    trx_slot_t* slot;
    trx_slot_id_t trx_slot_id;

    trx_slot_id.id = mach_read_from_8(log_rec_ptr);
    log_rec_ptr += 8;
    ut_ad(trx_slot_id.rseg_id < TRX_RSEG_COUNT);
    ut_ad(trx_slot_id.slot < trx_slot_count_per_page * TRX_SLOT_PAGE_COUNT_PER_RSEG);

    slot = (trx_slot_t *)TRX_GET_RSEG_TRX_SLOT(trx_slot_id);
    slot->status = XACT_BEGIN;
    slot->xnum = trx_slot_id.xnum;

    LOGGER_TRACE(LOGGER, LOG_MODULE_RECOVERY,
        "trx_rseg_replay_begin_slot: type %lu block (%p space_id %lu page_no %lu) rseg_id %lu slot %lu xnum %llu",
        type, block, block ? ((buf_block_t *)block)->get_space_id() : INVALID_SPACE_ID,
        block ? ((buf_block_t *)block)->get_page_no() : INVALID_PAGE_NO,
        trx_slot_id.rseg_id, trx_slot_id.slot, trx_slot_id.xnum);

    return log_rec_ptr;
}

byte* trx_rseg_replay_end_slot(uint32 type, byte* log_rec_ptr, byte* log_end_ptr, void* block)
{
    ut_ad(type == MLOG_TRX_RSEG_SLOT_END);
    ut_a(log_end_ptr - log_rec_ptr >= 16);

    trx_slot_t* slot;
    trx_slot_id_t trx_slot_id;
    lsn_t scn;

    trx_slot_id.id = mach_read_from_8(log_rec_ptr);
    log_rec_ptr += 8;
    ut_ad(trx_slot_id.rseg_id < TRX_RSEG_COUNT);
    ut_ad(trx_slot_id.slot < trx_slot_count_per_page * TRX_SLOT_PAGE_COUNT_PER_RSEG);

    scn = mach_read_from_8(log_rec_ptr);
    log_rec_ptr += 8;
    ut_ad(scn > 0 && scn != INVALID_SCN);

    slot = (trx_slot_t *)TRX_GET_RSEG_TRX_SLOT(trx_slot_id);
    slot->scn = scn;
    slot->status = XACT_END;

    LOGGER_TRACE(LOGGER, LOG_MODULE_RECOVERY,
        "trx_rseg_replay_end_slot: type %lu block (%p space_id %lu page_no %lu) rseg_id %lu slot %lu xnum %llu scn %llu",
        type, block, block ? ((buf_block_t *)block)->get_space_id() : INVALID_SPACE_ID,
        block ? ((buf_block_t *)block)->get_page_no() : INVALID_PAGE_NO,
        trx_slot_id.rseg_id, trx_slot_id.slot, trx_slot_id.xnum, scn);

    return log_rec_ptr;
}

