#ifndef _KNL_TRX_H
#define _KNL_TRX_H

#include "cm_mutex.h"
#include "cm_type.h"
#include "knl_mtr.h"
#include "knl_server.h"
#include "knl_fsp.h"
#include "knl_trx_types.h"
#include "knl_trx_undo.h"
#include "knl_session.h"

/** Space id of the transaction system page (the system tablespace) */
static const uint32 TRX_SYS_SPACE = DB_SYSTRANS_SPACE_ID;

/** The automatically created system rollback segment has this id */
#define TRX_SYS_SYSTEM_RSEG_ID      0

/** The offset of the transaction system header on the page */
#define TRX_SYS                     FSEG_PAGE_DATA
#define TRX_SYS_INODE               (FSEG_PAGE_DATA + 8)

/** Transaction system header */
#define TRX_SYS_TRX_ID_STORE        0
#define TRX_SYS_FSEG_HEADER         8
#define TRX_SYS_RSEGS               (8 + FSEG_HEADER_SIZE)

/** Page number of the transaction system page */
#define TRX_SYS_PAGE_NO             FSP_TRX_SYS_PAGE_NO



/** Transaction execution states when trx->state == TRX_STATE_ACTIVE */
enum trx_que_t {
	TRX_QUE_RUNNING,		/*!< transaction is running */
	TRX_QUE_LOCK_WAIT,		/*!< transaction is waiting for a lock */
	TRX_QUE_ROLLING_BACK,		/*!< transaction is rolling back */
	TRX_QUE_COMMITTING		/*!< transaction is committing */
};

/** Transaction states (trx_t::state) */
enum trx_state_t {
    TRX_STATE_NOT_STARTED,
    TRX_STATE_ACTIVE,
    TRX_STATE_PREPARED, // Support for 2PC/XA
    TRX_STATE_COMMITTED_IN_MEMORY
};


/** Transaction savepoint */
typedef struct st_trx_savepoint {
    uint32           insert_undo_page_no;
    uint32           insert_undo_page_offset;
    uint32           update_undo_page_no;
    uint32           update_undo_page_offset;
} trx_savepoint_t;




// seconds: '2019-01-01 00:00:00'UTC since Epoch ('1970-01-01 00:00:00'UTC)
#define TRX_GTS_BASE_TIME    1546300800







/* X/Open XA distributed transaction identifier */
#define XIDDATASIZE              128
typedef struct st_xa_id {
    int32 formatID;          /* format identifier; -1 means that the XID is null */
    int32 gtrid_length;      /* value from 1 through 64 */
    int32 bqual_length;      /* value from 1 through 64 */
    char  data[XIDDATASIZE]; /* distributed transaction identifier */
} xa_id_t;


//-----------------------------------------------------------------

#define TRX_TIMESEQ_TO_SCN1(time_val, init_time, seq)                           \
    ((uint64)((time_val)->tv_sec - (init_time)) << 28 | (uint64)((time_val)->tv_usec / 1000) << 16 | (seq))


#define TRX_TIMESEQ_TO_SCN(time_val, init_time, seq)                           \
    ((uint64)((time_val)->tv_sec - (init_time)) << 32 | (uint64)(time_val)->tv_usec << 12 | (seq))

#define TRX_SCN_TO_TIMESEQ(scn, time_val, seq, init_time)                                     \
    do {                                                                                      \
        (time_val)->tv_sec = (uint64)(((scn) >> 32) & 0x00000000FFFFFFFFULL + (init_time));   \
        (time_val)->tv_usec = (uint64)(((scn) >> 12) & 0x00000000000FFFFFULL);                 \
        seq = (uint64)((scn) & 0x0000000000000FFFULL);                                        \
    } while (0)

#define TRX_SCN_TO_TIME(scn, time_val, init_time)                                                \
        do {                                                                                     \
            (time_val)->tv_sec = (uint64)(((scn) >> 32) & 0x00000000FFFFFFFFULL + (init_time));  \
            (time_val)->tv_usec = (uint64)((scn) >> 12) & 0x00000000000FFFFFULL);                \
        } while (0)

#define TRX_SCN_INC(scn)           ((uint64)atomic64_inc(scn))
#define TRX_SCN_GET(scn)           ((uint64)atomic64_get(scn))
#define TRX_SCN_IS_INVALID(scn)    ((scn) == 0 || (scn) == UINT_MAX64)



//-----------------------------------------------------------------

extern status_t trx_sys_create(memory_pool_t* mem_pool, uint32 rseg_count, uint32 undo_space_count);
extern status_t trx_sys_init_at_db_start(bool32 is_create_database);
extern status_t trx_sys_recovery_at_db_start();
extern status_t trx_sys_create(memory_pool_t* mem_pool);

extern inline trx_t* trx_begin(que_sess_t* sess);
extern inline void trx_commit(que_sess_t* sess, trx_t* trx);
extern inline void trx_rollback(que_sess_t* sess, trx_t* trx, trx_savepoint_t* savepoint = NULL);
extern inline void trx_savepoint(que_sess_t* sess, trx_t* trx, trx_savepoint_t* savepoint);

extern inline void trx_start_if_not_started(que_sess_t* sess);

//-----------------------------------------------------------------


extern trx_sys_t*     trx_sys;


#endif  /* _KNL_TRX_H */
