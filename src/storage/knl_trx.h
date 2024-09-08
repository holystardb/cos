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
static const uint32 TRX_SYS_SPACE = 0;

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

/** Doublewrite buffer */

/** The offset of the doublewrite buffer header on the trx system header page */
#define TRX_SYS_DOUBLEWRITE		(UNIV_PAGE_SIZE - 200)
/*-------------------------------------------------------------*/
#define TRX_SYS_DOUBLEWRITE_FSEG	0	/*!< fseg header of the fseg containing the doublewrite buffer */
#define TRX_SYS_DOUBLEWRITE_MAGIC	FSEG_HEADER_SIZE
/*!< 4-byte magic number which shows if we already have created the doublewrite buffer */
#define TRX_SYS_DOUBLEWRITE_BLOCK1	(4 + FSEG_HEADER_SIZE)
/*!< page number of the first page in the first sequence of 64 (= FSP_EXTENT_SIZE) consecutive pages in the doublewrite buffer */
#define TRX_SYS_DOUBLEWRITE_BLOCK2	(8 + FSEG_HEADER_SIZE)
/*!< page number of the first page in the second sequence of 64 consecutive pages in the doublewrite buffer */
#define TRX_SYS_DOUBLEWRITE_REPEAT	12	/*!< we repeat
TRX_SYS_DOUBLEWRITE_MAGIC,
TRX_SYS_DOUBLEWRITE_BLOCK1,
TRX_SYS_DOUBLEWRITE_BLOCK2
so that if the trx sys header is half-written to disk, we still may be able to recover the information */
/** If this is not yet set to TRX_SYS_DOUBLEWRITE_SPACE_ID_STORED_N,
we must reset the doublewrite buffer, because starting from 4.1.x the
space id of a data page is stored into FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID. */
#define TRX_SYS_DOUBLEWRITE_SPACE_ID_STORED (24 + FSEG_HEADER_SIZE)


/*-------------------------------------------------------------*/
/* Size of a rollback segment specification slot */
#define TRX_SYS_RSEG_SLOT_SIZE	8

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
typedef struct st_trx_savept {
    uint32    least_undo_no;	/*!< least undo number to undo */
} trx_savept_t;


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


#define TRX_TIMESEQ_TO_SCN(time_val, init_time, seq)                           \
    ((uint64)((time_val)->tv_sec - (init_time)) << 32 | (uint64)(time_val)->tv_usec << 12 | (seq))

#define TRX_SCN_TO_TIMESEQ(scn, time_val, seq, init_time)                                     \
    do {                                                                                      \
        (time_val)->tv_sec = (uint64)(((scn) >> 32) & 0x00000000FFFFFFFFULL + (init_time));  \
        (time_val)->tv_usec = (uint64)((scn) >> 12) & 0x00000000000FFFFFULL);                 \
        seq = (uint64)((scn) & 0x0000000000000FFFULL);                                        \
    } while (0)

#define TRX_SCN_TO_TIME(scn, time_val, init_time)                                                 \
        do {                                                                                      \
            (time_val)->tv_sec = (uint64)(((scn) >> 32) & 0x00000000FFFFFFFFULL + (init_time));  \
            (time_val)->tv_usec = (uint64)((scn) >> 12) & 0x00000000000FFFFFULL);                 \
        } while (0)

#define TRX_INC_SCN(scn)              ((uint64)atomic64_inc(scn))
#define TRX_GET_SCN(scn)              ((uint64)atomic64_get(scn))


//-----------------------------------------------------------------

extern void trx_sys_init_at_db_start();
extern void trx_sys_create(memory_pool_t* mem_pool, uint32 rseg_count);

extern inline trx_t* trx_begin(mtr_t* mtr);
extern inline void trx_commit(trx_t* trx, mtr_t* mtr);
extern inline void trx_rollback(trx_t* trx, trx_savept_t* savept, mtr_t* mtr);

extern inline void trx_start_if_not_started(que_sess_t* sess, mtr_t* mtr);

//-----------------------------------------------------------------


extern trx_sys_t*     trx_sys;


#endif  /* _KNL_TRX_H */
