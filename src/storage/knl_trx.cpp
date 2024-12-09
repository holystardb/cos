#include "knl_trx.h"

#include "cm_timer.h"
#include "knl_buf.h"
#include "knl_fsp.h"
#include "knl_trx_rseg.h"
#include "knl_undo_fsm.h"

// The transaction system
trx_sys_t    g_trx_sys, *trx_sys = &g_trx_sys;



// Creates the trx_sys instance and initializes ib_bh and mutex.
status_t trx_sys_init_at_db_start(memory_pool_t* mem_pool, uint32 undo_space_count, bool32 is_create_database)
{
    status_t err;
    trx_rseg_t* rseg;
    trx_t* trx;

    mutex_create(&trx_sys->mutex);
    mutex_create(&trx_sys->used_pages_mutex);
    UT_LIST_INIT(trx_sys->used_pages);
    SLIST_INIT(trx_sys->free_undo_page_list);
    trx_sys->extend_in_process = FALSE;
    trx_sys->mem_pool = mem_pool;
    trx_sys->context = mcontext_stack_create(trx_sys->mem_pool);
    if (trx_sys->context == NULL) {
        return CM_ERROR;
    }

    if (is_create_database) {
        err = trx_sysf_create();
        CM_RETURN_IF_ERROR(err);
    }

    for (uint32 rseg_id = 0; rseg_id < TRX_RSEG_COUNT; rseg_id++) {
        rseg = TRX_GET_RSEG(rseg_id);

        rseg->id = rseg_id;
        rseg->undo_space_id = rseg_id % undo_space_count;
        rseg->undo_cached_max_count = 1024;
        rseg->extend_undo_in_process = FALSE;
        mutex_create(&rseg->trx_mutex);
        SLIST_INIT(rseg->trx_free_list);
        SLIST_INIT(rseg->trx_need_recovery_list);

        err = trx_rseg_undo_page_init(rseg);
        CM_RETURN_IF_ERROR(err);

        if (is_create_database) {
            err = trx_rseg_create_trx_slots(rseg);
            CM_RETURN_IF_ERROR(err);
        } else {
            err = trx_rseg_open_trx_slots(rseg);
            CM_RETURN_IF_ERROR(err);
        }
    }

    // add trx to rseg trx_list
    for (uint32 slot_idx = 0; slot_idx < TRX_SLOT_COUNT_PER_RSEG; slot_idx++) {
        for (uint32 rseg_id = 0; rseg_id < TRX_RSEG_COUNT; rseg_id++) {
            rseg = TRX_GET_RSEG(rseg_id);
            trx = &rseg->trx_list[slot_idx];
            if (trx->is_active) {
                SLIST_ADD_LAST(list_node, rseg->trx_need_recovery_list, trx);
            } else {
                SLIST_ADD_LAST(list_node, rseg->trx_free_list, trx);
            }
        }
    }

    // rollback transaction
    trx = SLIST_GET_FIRST(rseg->trx_need_recovery_list);
    while (trx) {
        SLIST_REMOVE_FIRST(list_node, rseg->trx_need_recovery_list);

        //
        

        trx = SLIST_GET_FIRST(rseg->trx_need_recovery_list);
    }

    // undo tablespace
    for (uint32 i = 0; i < undo_space_count; i++) {
        err = undo_fsm_recovery_fsp_pages(FIL_UNDO_START_SPACE_ID + i);
        CM_RETURN_IF_ERROR(err);
    }

    return CM_SUCCESS;
}

void trx_undo_insert_cleanup(trx_t* trx)
{
}

#define TRX_SYS_TRX_ID_WRITE_MARGIN         256

// Writes the value of max_trx_id to the file based trx system header
static void trx_sys_flush_max_trx_id(void)
{
    mtr_t       mtr;
    trx_sysf_t* sys_header;

    ut_ad(mutex_own(&trx_sys->mutex));

    if (!srv_read_only_mode) {
        mtr_start(&mtr);
        sys_header = trx_sysf_get(&mtr);
        mlog_write_uint64(sys_header + TRX_SYS_TRX_ID_STORE, trx_sys->max_trx_id, &mtr);
        mtr_commit(&mtr);
    }
}

/*
static uint64 trx_sys_get_new_trx_id()
{
    ut_ad(mutex_own(&trx_sys->mutex));

    if (!(trx_sys->max_trx_id % TRX_SYS_TRX_ID_WRITE_MARGIN)) {
        trx_sys_flush_max_trx_id();
    }

    return (trx_sys->max_trx_id++);
}
*/


uint64 trx_scn_inc(que_sess_t* sess, time_t init_time)
{
    uint64 seq, next_scn;
    timeval_t time_val, old_time_val;

    mutex_enter(&sess->scn_mutex, NULL);

    TRX_SCN_TO_TIMESEQ(sess->current_scn, &old_time_val, seq, init_time);

    //(void)cm_gettimeofday(&time_val);
    date_t now = g_timer()->monotonic_now_us;
    time_val.tv_sec = now / MICROSECS_PER_SECOND;
    time_val.tv_usec = now % MICROSECS_PER_SECOND;
    if (time_val.tv_sec <= old_time_val.tv_sec) {
        next_scn = TRX_SCN_INC(&sess->current_scn);
    } else {
        next_scn = TRX_TIMESEQ_TO_SCN(&time_val, init_time, 1);
    }
    atomic64_test_and_set(&sess->current_scn, next_scn);

    mutex_exit(&sess->scn_mutex);

    return next_scn;
}



inline trx_t* trx_begin(que_sess_t* sess)
{
    return trx_rseg_assign_and_alloc_trx();
}

static inline void trx_commit_in_memory(que_sess_t* sess, trx_t* trx, scn_t scn)
{
    // undo page
    if (UT_LIST_GET_LEN(trx->insert_undo) > 0) {
        trx_undo_insert_cleanup(trx);
    }

    // set itl status of data page
    if (sess->fast_clean_pages) {
        mtr_t mtr;
        mtr_start(&mtr);
        fast_clean_page_hdr_t* clean_page_hdr = (fast_clean_page_hdr_t *)MEM_PAGE_DATA_PTR(sess->fast_clean_pages);
        uint16 page_count = mach_read_from_2(clean_page_hdr);
        for (uint32 i = 0; i < page_count; i++) {
            fast_clean_page_t* clean_page = clean_page_hdr + FAST_CLEAN_PAGE_HEADER_SIZE + i * FAST_CLEAN_PAGE_SIZE;
            uint32 space_id = mach_read_from_4(clean_page_hdr + FAST_CLEAN_PAGE_SPACE_ID);
            uint32 page_no = mach_read_from_4(clean_page_hdr + FAST_CLEAN_PAGE_PAGE_NO);
            uint8  itl_id = mach_read_from_4(clean_page_hdr + FAST_CLEAN_PAGE_ITL_ID);
            buf_block_t* guess = (buf_block_t*)DatumGetPointer(mach_read_from_8(clean_page_hdr + FAST_CLEAN_PAGE_BLOCK));

            const page_id_t page_id(space_id, page_no);
            const page_size_t page_size(space_id);
            buf_block_t* block = buf_page_get_gen(page_id, page_size,
                RW_NO_LATCH, guess, Page_fetch::PEEK_IF_IN_POOL, &mtr);
            if (block) {
                heap_set_itl_trx_end(block, trx->trx_slot_id, itl_id, scn, &mtr);
            }
        }
        mach_write_to_2(clean_page_hdr, 0);
        mtr_commit(&mtr);
    }
}

inline void trx_commit(que_sess_t* sess, trx_t* trx)
{
    //
    scn_t scn = trx_rseg_set_end(trx, TRUE);

    trx_commit_in_memory(sess, trx, scn);

    trx_rseg_release_trx(trx);
    srv_stats.trx_commits.inc();
}

inline void trx_rollback(que_sess_t* sess, trx_t* trx, trx_savepoint_t* savepoint)
{
    //
    trx_rseg_set_end(trx, FALSE);

    trx_rseg_release_trx(trx);
    srv_stats.trx_rollbacks.inc();
}

inline void trx_savepoint(que_sess_t* sess, trx_t* trx, trx_savepoint_t* savepoint)
{
    trx_undo_page_t* undo_page = SLIST_GET_LAST(trx->insert_undo);
    if (undo_page) {
        savepoint->insert_undo_page_no = undo_page->page_no;
        savepoint->insert_undo_page_offset = undo_page->page_offset;
    }
    undo_page = SLIST_GET_LAST(trx->update_undo);
    if (undo_page) {
        savepoint->update_undo_page_no = undo_page->page_no;
        savepoint->update_undo_page_offset = undo_page->page_offset;
    }
}

inline void trx_start_if_not_started(que_sess_t* sess)
{
    if (sess->trx) {
        return;
    }

    sess->trx = trx_rseg_assign_and_alloc_trx();
    ut_a(sess->trx);
}

