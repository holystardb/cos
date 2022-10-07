#ifndef _MTR_MTR_H_
#define _MTR_MTR_H_

#include "cm_type.h"
#include "cm_list.h"
#include "cm_memory.h"

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/* Initial 'payload' size in bytes in a dynamic array block */
#define DYN_ARRAY_DATA_SIZE             512
#define DYN_ARRAY_BLOCK_SIZE            (DYN_ARRAY_DATA_SIZE + ut_align8(sizeof(dyn_block_t)))

typedef struct st_dyn_block{
    uint32      used; /* number of data bytes used in this block */
    byte        data[DYN_ARRAY_DATA_SIZE]; /* storage for array elements */	
    UT_LIST_NODE_T(struct st_dyn_block) list_node; /* linear list node: used in all blocks */
} dyn_block_t;

typedef struct st_dyn_array{
    memory_pool_t           *pool;
    uint32                   current_page_used; // number of data bytes used in last memory_page_t
    UT_LIST_BASE_NODE_T(memory_page_t) pages;
    UT_LIST_BASE_NODE_T(dyn_block_t) blocks;
} dyn_array_t;


/* Logging modes for a mini-transaction */
#define MTR_LOG_ALL         21 /* default mode: log all operations modifying disk-based data */
#define MTR_LOG_NONE        22 /* log no operations */

/* Types for the mlock objects to store in the mtr memo; NOTE that the
first 3 values must be RW_S_LATCH, RW_X_LATCH, RW_NO_LATCH */
#define	MTR_MEMO_PAGE_S_FIX	RW_S_LATCH
#define	MTR_MEMO_PAGE_X_FIX	RW_X_LATCH
#define	MTR_MEMO_BUF_FIX	RW_NO_LATCH
#define MTR_MEMO_MODIFY		54
#define	MTR_MEMO_S_LOCK		55
#define	MTR_MEMO_X_LOCK		56

/* Log item types: we have made them to be of the type 'byte'
for the compiler to warn if val and type parameters are switched
in a call to mlog_write_ulint. NOTE! For 1 - 8 bytes, the
flag value must give the length also! */
#define	MLOG_SINGLE_REC_FLAG	128		/* if the mtr contains only
						one log record for one page,
						i.e., write_initial_log_record
						has been called only once,
						this flag is ORed to the type
						of that first log record */
#define	MLOG_1BYTE		((byte)1) 	/* one byte is written */
#define	MLOG_2BYTES		((byte)2)	/* 2 bytes ... */
#define	MLOG_4BYTES		((byte)4)	/* 4 bytes ... */
#define	MLOG_8BYTES		((byte)8)	/* 8 bytes ... */
#define	MLOG_REC_INSERT		((byte)9)	/* record insert */
#define	MLOG_REC_CLUST_DELETE_MARK ((byte)10) 	/* mark clustered index record
						deleted */
#define	MLOG_REC_SEC_DELETE_MARK ((byte)11) 	/* mark secondary index record
						deleted */
#define MLOG_REC_UPDATE_IN_PLACE ((byte)13)	/* update of a record,
						preserves record field sizes */
#define MLOG_REC_DELETE		((byte)14)	/* delete a record from a
						page */
#define	MLOG_LIST_END_DELETE 	((byte)15)	/* delete record list end on
						index page */
#define	MLOG_LIST_START_DELETE 	((byte)16) 	/* delete record list start on
						index page */
#define	MLOG_LIST_END_COPY_CREATED ((byte)17) 	/* copy record list end to a
						new created index page */
#define	MLOG_PAGE_REORGANIZE 	((byte)18)	/* reorganize an index page */
#define MLOG_PAGE_CREATE 	((byte)19)	/* create an index page */
#define	MLOG_UNDO_INSERT 	((byte)20)	/* insert entry in an undo
						log */
#define MLOG_UNDO_ERASE_END	((byte)21)	/* erase an undo log page end */
#define	MLOG_UNDO_INIT 		((byte)22)	/* initialize a page in an
						undo log */
#define MLOG_UNDO_HDR_DISCARD	((byte)23)	/* discard an update undo log
						header */
#define	MLOG_UNDO_HDR_REUSE	((byte)24)	/* reuse an insert undo log
						header */
#define MLOG_UNDO_HDR_CREATE	((byte)25)	/* create an undo log header */
#define MLOG_REC_MIN_MARK	((byte)26)	/* mark an index record as the
						predefined minimum record */
#define MLOG_IBUF_BITMAP_INIT	((byte)27)	/* initialize an ibuf bitmap
						page */
#define	MLOG_FULL_PAGE		((byte)28)	/* full contents of a page */
#define MLOG_INIT_FILE_PAGE	((byte)29)	/* this means that a file page
						is taken into use and the prior
						contents of the page should be
						ignored: in recovery we must
						not trust the lsn values stored
						to the file page */
#define MLOG_WRITE_STRING	((byte)30)	/* write a string to a page */
#define	MLOG_MULTI_REC_END	((byte)31)	/* if a single mtr writes
						log records for several pages,
						this log record ends the
						sequence of these records */
#define MLOG_DUMMY_RECORD	((byte)32)	/* dummy log record used to
						pad a log block full */
#define MLOG_BIGGEST_TYPE	((byte)32) 	/* biggest value (used in
						asserts) */
					


/* Mini-transaction handle and buffer */
typedef struct st_mtr{
    uint32 state; /* MTR_ACTIVE, MTR_COMMITTING, MTR_COMMITTED */
    dyn_array_t memo; /* memo stack for locks etc. */
    dyn_array_t log; /* mini-transaction log */
    bool32 modifications; /* TRUE if the mtr made modifications to buffer pool pages */
    uint32 n_log_recs; /* count of how many page initial log records have been written to the mtr log */
    uint32 log_mode; /* specifies which operations should be logged; default value MTR_LOG_ALL */
    uint64 start_lsn; /* start lsn of the possible log entry for this mtr */
    uint64 end_lsn; /* end lsn of the possible log entry for this mtr */
    uint32 magic_n;
} mtr_t;

mtr_t* mtr_start(mtr_t *mtr);
void mtr_commit(mtr_t *mtr);

byte* mlog_open(mtr_t *mtr, uint32 size);
void mlog_close(mtr_t *mtr, byte *ptr);


#ifdef __cplusplus
}
#endif // __cplusplus

#endif // _MTR_MTR_H_