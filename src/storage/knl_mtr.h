#ifndef _KNL_MTR_H_
#define _KNL_MTR_H_

#include "cm_type.h"
#include "cm_list.h"
#include "cm_memory.h"
#include "cm_rwlock.h"

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
//#define MTR_MEMO_PAGE_S_FIX     RW_S_LATCH
//#define MTR_MEMO_PAGE_X_FIX     RW_X_LATCH
//#define MTR_MEMO_PAGE_SX_FIX    RW_SX_LATCH
//#define MTR_MEMO_BUF_FIX        RW_NO_LATCH
//#define MTR_MEMO_MODIFY         32
//#define MTR_MEMO_S_LOCK         64
//#define MTR_MEMO_X_LOCK         128

enum mtr_memo_type_t {
  MTR_MEMO_PAGE_S_FIX = RW_S_LATCH,

  MTR_MEMO_PAGE_X_FIX = RW_X_LATCH,

  MTR_MEMO_PAGE_SX_FIX = RW_SX_LATCH,

  MTR_MEMO_BUF_FIX = RW_NO_LATCH,

#ifdef UNIV_DEBUG
  MTR_MEMO_MODIFY = 32,
#endif /* UNIV_DEBUG */

  MTR_MEMO_S_LOCK = 64,

  MTR_MEMO_X_LOCK = 128,

  MTR_MEMO_SX_LOCK = 256
};




/** Log item types
The log items are declared 'byte' so that the compiler can warn if val
and type parameters are switched in a call to mlog_write_ulint. NOTE!
For 1 - 8 bytes, the flag value must give the length also! */
enum mlog_id_t {
	/** if the mtr contains only one log record for one page,
	i.e., write_initial_log_record has been called only once,
	this flag is ORed to the type of that first log record */
	MLOG_SINGLE_REC_FLAG = 128,

	/** one byte is written */
	MLOG_1BYTE = 1,

	/** 2 bytes ... */
	MLOG_2BYTES = 2,

	/** 4 bytes ... */
	MLOG_4BYTES = 4,

	/** 8 bytes ... */
	MLOG_8BYTES = 8,

	/** Record insert */
	MLOG_REC_INSERT = 9,

	/** Mark clustered index record deleted */
	MLOG_REC_CLUST_DELETE_MARK = 10,

	/** Mark secondary index record deleted */
	MLOG_REC_SEC_DELETE_MARK = 11,

	/** update of a record, preserves record field sizes */
	MLOG_REC_UPDATE_IN_PLACE = 13,

	/*!< Delete a record from a page */
	MLOG_REC_DELETE = 14,

	/** Delete record list end on index page */
	MLOG_LIST_END_DELETE = 15,

	/** Delete record list start on index page */
	MLOG_LIST_START_DELETE = 16,

	/** Copy record list end to a new created index page */
	MLOG_LIST_END_COPY_CREATED = 17,

	/** Reorganize an index page in ROW_FORMAT=REDUNDANT */
	MLOG_PAGE_REORGANIZE = 18,

	/** Create an index page */
	MLOG_PAGE_CREATE = 19,

	/** Insert entry in an undo log */
	MLOG_UNDO_INSERT = 20,

	/** erase an undo log page end */
	MLOG_UNDO_ERASE_END = 21,

	/** initialize a page in an undo log */
	MLOG_UNDO_INIT = 22,

	/** discard an update undo log header */
	MLOG_UNDO_HDR_DISCARD = 23,

	/** reuse an insert undo log header */
	MLOG_UNDO_HDR_REUSE = 24,

	/** create an undo log header */
	MLOG_UNDO_HDR_CREATE = 25,

	/** mark an index record as the predefined minimum record */
	MLOG_REC_MIN_MARK = 26,

	/** initialize an ibuf bitmap page */
	MLOG_IBUF_BITMAP_INIT = 27,

#ifdef UNIV_LOG_LSN_DEBUG
	/** Current LSN */
	MLOG_LSN = 28,
#endif /* UNIV_LOG_LSN_DEBUG */

	/** this means that a file page is taken into use and the prior
	contents of the page should be ignored: in recovery we must not
	trust the lsn values stored to the file page.
	Note: it's deprecated because it causes crash recovery problem
	in bulk create index, and actually we don't need to reset page
	lsn in recv_recover_page_func() now. */
	MLOG_INIT_FILE_PAGE = 29,

	/** write a string to a page */
	MLOG_WRITE_STRING = 30,

	/** If a single mtr writes several log records, this log
	record ends the sequence of these records */
	MLOG_MULTI_REC_END = 31,

	/** dummy log record used to pad a log block full */
	MLOG_DUMMY_RECORD = 32,

	/** log record about an .ibd file creation */
	//MLOG_FILE_CREATE = 33,

	/** rename databasename/tablename (no .ibd file name suffix) */
	//MLOG_FILE_RENAME = 34,

	/** delete a tablespace file that starts with (space_id,page_no) */
	MLOG_FILE_DELETE = 35,

	/** mark a compact index record as the predefined minimum record */
	MLOG_COMP_REC_MIN_MARK = 36,

	/** create a compact index page */
	MLOG_COMP_PAGE_CREATE = 37,

	/** compact record insert */
	MLOG_COMP_REC_INSERT = 38,

	/** mark compact clustered index record deleted */
	MLOG_COMP_REC_CLUST_DELETE_MARK = 39,

	/** mark compact secondary index record deleted; this log
	record type is redundant, as MLOG_REC_SEC_DELETE_MARK is
	independent of the record format. */
	MLOG_COMP_REC_SEC_DELETE_MARK = 40,

	/** update of a compact record, preserves record field sizes */
	MLOG_COMP_REC_UPDATE_IN_PLACE = 41,

	/** delete a compact record from a page */
	MLOG_COMP_REC_DELETE = 42,

	/** delete compact record list end on index page */
	MLOG_COMP_LIST_END_DELETE = 43,

	/*** delete compact record list start on index page */
	MLOG_COMP_LIST_START_DELETE = 44,

	/** copy compact record list end to a new created index page */
	MLOG_COMP_LIST_END_COPY_CREATED = 45,

	/** reorganize an index page */
	MLOG_COMP_PAGE_REORGANIZE = 46,

	/** log record about creating an .ibd file, with format */
	MLOG_FILE_CREATE2 = 47,

	/** write the node pointer of a record on a compressed
	non-leaf B-tree page */
	MLOG_ZIP_WRITE_NODE_PTR = 48,

	/** write the BLOB pointer of an externally stored column
	on a compressed page */
	MLOG_ZIP_WRITE_BLOB_PTR = 49,

	/** write to compressed page header */
	MLOG_ZIP_WRITE_HEADER = 50,

	/** compress an index page */
	MLOG_ZIP_PAGE_COMPRESS = 51,

	/** compress an index page without logging it's image */
	MLOG_ZIP_PAGE_COMPRESS_NO_DATA = 52,

	/** reorganize a compressed page */
	MLOG_ZIP_PAGE_REORGANIZE = 53,

	/** rename a tablespace file that starts with (space_id,page_no) */
	MLOG_FILE_RENAME2 = 54,

	/** note the first use of a tablespace file since checkpoint */
	MLOG_FILE_NAME = 55,

	/** note that all buffered log was written since a checkpoint */
	MLOG_CHECKPOINT = 56,

	/** Create a R-Tree index page */
	MLOG_PAGE_CREATE_RTREE = 57,

	/** create a R-tree compact page */
	MLOG_COMP_PAGE_CREATE_RTREE = 58,

	/** this means that a file page is taken into use.
	We use it to replace MLOG_INIT_FILE_PAGE. */
	MLOG_INIT_FILE_PAGE2 = 59,

	/** Table is being truncated. (Marked only for file-per-table) */
	MLOG_TRUNCATE = 60,

	/** notify that an index tree is being loaded without writing
	redo log about individual pages */
	MLOG_INDEX_LOAD = 61,

	/** biggest value (used in assertions) */
	MLOG_BIGGEST_TYPE = MLOG_INDEX_LOAD
};

enum mtr_state_t {
    MTR_STATE_INIT = 0,
    MTR_STATE_ACTIVE = 12231,
    MTR_STATE_COMMITTING = 56456,
    MTR_STATE_COMMITTED = 34676
};

/* Mini-transaction handle and buffer */
typedef struct st_mtr{

    dyn_array_t memo; /* memo stack for locks etc. */
    dyn_array_t log; /* mini-transaction log */
    unsigned inside_ibuf:1; /*!< TRUE if inside ibuf changes */
    unsigned modifications:1; /*!< TRUE if the mini-transaction modified buffer pool pages */
    unsigned made_dirty:1; /*!< TRUE if mtr has made at least one buffer pool page dirty */
    uint32 n_log_recs; /* count of how many page initial log records have been written to the mtr log */
    uint32 log_mode; /* specifies which operations should be logged; default value MTR_LOG_ALL */
    uint64 start_lsn; /* start lsn of the possible log entry for this mtr */
    uint64 end_lsn; /* end lsn of the possible log entry for this mtr */
    uint32 magic_n;
    uint32 state; /* MTR_ACTIVE, MTR_COMMITTING, MTR_COMMITTED */

    // return true if the mini-transaction is active */
    bool32 is_active()
    {
        return (state == MTR_STATE_ACTIVE);
    }

    void memo_push(void* object, uint32 type);

    void s_lock(rw_lock_t* lock, const char* file, uint32 line);
    void x_lock(rw_lock_t* lock, const char* file, uint32 line);

} mtr_t;

#define MTR_MAGIC_N      54551
#define MTR_ACTIVE       12231

/** Mini-transaction memo stack slot. */
typedef struct mtr_memo_slot_t{
    uint32 type;  /*!< type of the stored object (MTR_MEMO_S_LOCK, ...) */
    void* object;  /*!< pointer to the object */
} mtr_memo_slot_t;




mtr_t* mtr_start(mtr_t *mtr);
void mtr_commit(mtr_t *mtr);

byte* mlog_open(mtr_t *mtr, uint32 size);
void mlog_close(mtr_t *mtr, byte *ptr);

void mlog_write_uint32(
    byte*       ptr,    /*!< in: pointer where to write */
    uint32      val,    /*!< in: value to write */
    mlog_id_t   type,   /*!< in: MLOG_1BYTE, MLOG_2BYTES, MLOG_4BYTES */
    mtr_t*      mtr);    /*!< in: mini-transaction handle */

void mlog_write_uint64(
    byte*       ptr,    /*!< in: pointer where to write */
    uint64      val,    /*!< in: value to write */
    mtr_t*      mtr);    /*!< in: mini-transaction handle */

void mlog_write_string(
    byte*       ptr,    /*!< in: pointer where to write */
    const byte* str,    /*!< in: string to write */
    uint32      len,    /*!< in: string length */
    mtr_t*      mtr);    /*!< in: mini-transaction handle */

void mlog_log_string(
    byte*   ptr,    /*!< in: pointer written to */
    uint32   len,    /*!< in: string length */
    mtr_t*  mtr);    /*!< in: mini-transaction handle */

uint32 mlog_read_uint32(const byte* ptr, mlog_id_t type);

/********************************************************//**
Writes initial part of a log record consisting of one-byte item
type and four-byte space and page numbers. */
void mlog_write_initial_log_record(
	const byte*	ptr,	/*!< in: pointer to (inside) a buffer
				frame holding the file page where
				modification is made */
	mlog_id_t	type,	/*!< in: log item type: MLOG_1BYTE, ... */
	mtr_t*		mtr);	/*!< in: mini-transaction handle */

uint32 mtr_read_uint(
	const byte*	ptr,	/*!< in: pointer from where to read */
	mlog_id_t	type,	/*!< in: MLOG_1BYTE, MLOG_2BYTES, MLOG_4BYTES */
	mtr_t*		mtr);
				/*!< in: mini-transaction handle */


/** Check if memo contains the given page.
@return TRUE if contains */
#define mtr_memo_contains_page(m, p, t)     FALSE
//        (m)->memo_contains_page_flagged((p), (t))


#ifdef __cplusplus
}
#endif // __cplusplus

#endif // _KNL_MTR_H_