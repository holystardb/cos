#include "knl_trx.h"

#include "cm_timer.h"
#include "knl_buf.h"
#include "knl_fsp.h"
#include "knl_trx_rseg.h"
#include "knl_undo_fsm.h"

// The transaction system
trx_sys_t    g_trx_sys, *trx_sys = &g_trx_sys;


// Creates the trx_sys instance
status_t trx_sys_create(memory_pool_t* mem_pool, uint32 rseg_count, uint32 undo_space_count)
{
    ut_a(rseg_count >= TRX_RSEG_MIN_COUNT && rseg_count <= TRX_RSEG_MAX_COUNT);

    mutex_create(&trx_sys->mutex);
    mutex_create(&trx_sys->used_pages_mutex);
    UT_LIST_INIT(trx_sys->used_pages);
    SLIST_INIT(trx_sys->free_undo_page_list);
    trx_sys->extend_in_process = FALSE;
    trx_sys->mem_pool = mem_pool;
    trx_sys->rseg_count = rseg_count;
    trx_sys->undo_space_count = undo_space_count;
    trx_sys->context = mcontext_stack_create(trx_sys->mem_pool);
    if (trx_sys->context == NULL) {
        return CM_ERROR;
    }

    return CM_SUCCESS;
}

// Creates and initializes transaction slots
status_t trx_sys_init_at_db_start(bool32 is_create_database)
{
    status_t err;
    trx_rseg_t* rseg;
    trx_t* trx;

    if (is_create_database) {
        err = trx_sysf_create();
        CM_RETURN_IF_ERROR(err);

        err = trx_sys_reserve_systrans_space();
        CM_RETURN_IF_ERROR(err);
    }

    for (uint32 rseg_id = 0; rseg_id < trx_sys->rseg_count; rseg_id++) {
        rseg = TRX_GET_RSEG(rseg_id);

        rseg->id = rseg_id;
        rseg->undo_space_id = rseg_id % trx_sys->undo_space_count;
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
        for (uint32 rseg_id = 0; rseg_id < trx_sys->rseg_count; rseg_id++) {
            rseg = TRX_GET_RSEG(rseg_id);
            trx = &rseg->trx_list[slot_idx];
            if (trx->is_active) {
                SLIST_ADD_LAST(list_node, rseg->trx_need_recovery_list, trx);
            } else {
                SLIST_ADD_LAST(list_node, rseg->trx_free_list, trx);
            }
        }
    }

    return CM_SUCCESS;
}

status_t trx_sys_recovery_at_db_start()
{
    status_t err;
    trx_rseg_t* rseg;
    trx_t* trx;

    // rollback transaction
    for (uint32 rseg_id = 0; rseg_id < trx_sys->rseg_count; rseg_id++) {
        rseg = TRX_GET_RSEG(rseg_id);
        trx = SLIST_GET_FIRST(rseg->trx_need_recovery_list);
        while (trx) {
            SLIST_REMOVE_FIRST(list_node, rseg->trx_need_recovery_list);

            //


            trx = SLIST_GET_FIRST(rseg->trx_need_recovery_list);
        }
    }

    // undo tablespace
    for (uint32 i = 0; i < trx_sys->undo_space_count; i++) {
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
    time_val.tv_sec = (uint32)(now / MICROSECS_PER_SECOND);
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


static inline void trx_commit_fast_clean(que_sess_t* sess, trx_t* trx, scn_t scn)
{
    uint32 clean_block_count = sess->fast_clean_mgr.get_clean_block_count();
    mtr_t mtr;

    for (uint32 i = 0; i < clean_block_count; i++) {
        mtr_start(&mtr);

        fast_clean_block_t* clean_block = sess->fast_clean_mgr.find_clean_block(i);
        const page_id_t page_id(clean_block->space_id, clean_block->page_no);
        const page_size_t page_size(clean_block->space_id);
        buf_block_t* block = buf_page_get_gen(page_id, page_size,
            RW_X_LATCH, clean_block->block, Page_fetch::PEEK_IF_IN_POOL, &mtr);
        if (block == NULL) {
            continue;
        }

        // set itl status and fsc of data page
        heap_set_itl_trx_end(block, trx->trx_slot_id, clean_block->itl_id, scn, &mtr);

        mtr_commit(&mtr);
    }
    sess->fast_clean_mgr.clean();
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

    // set itl status and fsc of data page
    trx_commit_fast_clean(sess,trx, scn);
}

inline void trx_commit(que_sess_t* sess, trx_t* trx)
{
    //
    scn_t scn = trx_rseg_set_end(trx, TRUE);

    trx_commit_in_memory(sess, trx, scn);

    trx_rseg_release_trx(trx);
    srv_stats.trx_commits.inc();
}

static void trx_rollback_pcr_pages(que_sess_t* sess, trx_t* trx, trx_savepoint_t* savepoint)
{
}

static void trx_rollback_one_row(que_sess_t* sess, trx_t* trx, trx_undo_rec_hdr_t* undo_rec, uint32 undo_rec_size)
{
    uint16 type = mach_read_from_2(undo_rec + TRX_UNDO_REC_TYPE);
    switch (type) {
    case UNDO_HEAP_INSERT:
        heap_undo_insert(sess, trx, undo_rec, undo_rec_size);
        break;
    case UNDO_HEAP_DELETE:
        break;
    case UNDO_HEAP_UPDATE:
        break;

    default:
        ut_error;
        break;
    }
}

static void trx_rollback_rcr_rows(que_sess_t* sess, trx_t* trx, trx_savepoint_t* savepoint)
{
    mtr_t mtr;
    trx_rollback_undo_mgr_t undo_mgr(trx);

    mtr_start(&mtr);
    undo_mgr.init(&mtr);

    uint32 undo_rec_size;
    trx_undo_rec_hdr_t* undo_rec = undo_mgr.pop_top_undo_rec(undo_rec_size, mtr);
    while (undo_rec) {
        trx_rollback_one_row(sess, trx, undo_rec, undo_rec_size);
        undo_rec = undo_mgr.pop_top_undo_rec(undo_rec_size, mtr);
    }

    //for (i = 0; i < heap_assist.rows; i++) {
    //    session->change_list = heap_assist.change_list[i];
    //    heap_try_change_map(session, heap_assist.heap, heap_assist.page_id[i]);
    //}

    // itl status

    mtr_commit(&mtr);

    // clean fast_clean_pages
    sess->fast_clean_mgr.reset();
}

inline void trx_rollback(que_sess_t* sess, trx_t* trx, trx_savepoint_t* savepoint)
{
    //
    trx_rseg_set_end(trx, FALSE);

    trx_rseg_release_trx(trx);
    srv_stats.trx_rollbacks.inc();

    trx_rollback_pages(sess, trx, savepoint);



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

