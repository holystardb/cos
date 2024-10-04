#include "knl_trx.h"

#include "knl_buf.h"
#include "knl_fsp.h"
#include "knl_trx_rseg.h"


// The transaction system
trx_sys_t*    trx_sys = NULL;


status_t trx_sys_init_at_db_start()
{
    return CM_SUCCESS;
}

// Creates the trx_sys instance and initializes ib_bh and mutex.
status_t trx_sys_create(memory_pool_t* mem_pool, uint32 rseg_count)
{
    ut_ad(trx_sys == NULL);
    ut_a(rseg_count >= TRX_RSEG_MIN_COUNT && rseg_count <= TRX_RSEG_MAX_COUNT);

    trx_sys = (trx_sys_t *)ut_malloc_zero(sizeof(trx_sys_t));
    if (trx_sys == NULL) {
        return CM_ERROR;
    }

    mutex_create(&trx_sys->mutex);

    trx_sys->mem_pool = mem_pool;
    trx_sys->context = mcontext_stack_create(trx_sys->mem_pool);
    if (trx_sys->context == NULL) {
        return CM_ERROR;
    }

    trx_sys->rseg_count = rseg_count;
    for (uint32 i = 0; i < TRX_RSEG_MAX_COUNT; i++) {
        trx_sys->rseg_array[i].id = 0xFFFFFFFF;
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

inline trx_t* trx_begin(mtr_t* mtr)
{
    return trx_rseg_assign_and_alloc_trx(mtr);
}

static inline void trx_commit_in_memory(trx_t* trx, lsn_t lsn)
{
    if (lsn) {
        if (UT_LIST_GET_LEN(trx->insert_undo) > 0) {
            trx_undo_insert_cleanup(trx);
        }
    }
}

inline void trx_commit(trx_t* trx, mtr_t* mtr)
{
    //
    trx_rseg_set_end(trx, mtr);

    mtr_commit(mtr);
    trx_commit_in_memory(trx, mtr->end_lsn);

    trx_rseg_release_trx(trx);
    srv_stats.trx_commits.inc();
}

inline void trx_rollback(trx_t* trx, trx_savept_t* savept, mtr_t* mtr)
{
    //
    trx_rseg_set_end(trx, mtr);

    trx_rseg_release_trx(trx);
    srv_stats.trx_rollbacks.inc();
}

inline void trx_start_if_not_started(que_sess_t* sess, mtr_t* mtr)
{
    if (sess->trx) {
        return;
    }

    sess->trx = trx_rseg_assign_and_alloc_trx(mtr);
    ut_a(sess->trx);
}

