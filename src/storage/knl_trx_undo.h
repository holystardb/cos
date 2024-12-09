#ifndef _KNL_TRX_UNDO_H
#define _KNL_TRX_UNDO_H

#include "cm_type.h"
#include "knl_flst.h"
#include "knl_buf.h"
#include "knl_heap.h"
#include "knl_dict.h"
#include "knl_session.h"

/* States of an undo log segment */
#define TRX_UNDO_PAGE_STATE_ACTIVE         1 /* contains an undo log of an active transaction */
#define TRX_UNDO_PAGE_STATE_CACHED         2 /* update cached for quick reuse */
#define TRX_UNDO_PAGE_STATE_FREE           3 /* insert undo segment can be freed */
#define TRX_UNDO_PAGE_STATE_HISTORY_LIST   4 /* update undo segment will not be reused */
#define TRX_UNDO_PAGE_STATE_PREPARED       5 /* contains an undo log of an prepared transaction */


/* Operation type flags used in trx_undo_write */
enum undo_op_type {
    UNDO_INSERT_OP = 1,
    UNDO_MODIFY_OP = 2
};


//-----------------------------------------------------------------

typedef byte    trx_undo_page_hdr_t;

// The offset of the undo log page header on pages of the undo log
#define TRX_UNDO_PAGE_HDR       FSEG_PAGE_DATA

#define TRX_UNDO_PAGE_TYPE          0 // TRX_UNDO_INSERT_OP or TRX_UNDO_MODIFY_OP
// Byte offset where the undo log header for the FIRST transaction start on this page
// (remember that in an update undo log, the first page can contain several undo logs)
#define TRX_UNDO_PAGE_START         2
// byte offset of the first free byte on the page
#define TRX_UNDO_PAGE_FREE          4
// The file list node in the chain of undo log pages
#define TRX_UNDO_PAGE_FLST_NODE     6  // for base node TRX_UNDO_PAGE_LIST

/* Size of the transaction undo log page header, in bytes */
#define TRX_UNDO_PAGE_HDR_SIZE      (6 + FLST_NODE_SIZE)


//-----------------------------------------------------------------

// The offset of the undo log segment header on the first page of the undo log segment
typedef byte    trx_undo_seg_hdr_t;

/* Undo log segment header */
#define TRX_UNDO_SEG_HDR        (TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_HDR_SIZE)

#define TRX_UNDO_STATE          0   // TRX_UNDO_ACTIVE, ...
// Offset of the last undo log header on the segment header page, 0 if none
#define TRX_UNDO_LAST_LOG       2
// Base node for the list of pages in the undo log segment;
// defined only on the undo log segment's first page
#define TRX_UNDO_PAGE_LIST      4

/* Size of the undo log segment header */
#define TRX_UNDO_SEG_HDR_SIZE   (4 + FLST_BASE_NODE_SIZE)

//-----------------------------------------------------------------

// The undo log header for transaction.
// There can be several undo log headers on the first page of an update undo log segment.
typedef byte    trx_undo_log_hdr_t;

#define TRX_UNDO_LOG_HDR        (TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE)

#define TRX_UNDO_TRX_ID         0 // Transaction id
// Transaction number of the transaction;
// defined only if the log is in a history list
#define TRX_UNDO_TRX_SCN        8
// Offset of the first undo log record of this log on the header page
#define TRX_UNDO_LOG_START      16
// TRUE if undo log header includes X/Open XA transaction identification XID
#define TRX_UNDO_XAID_EXISTS    18
// TRUE if the transaction is a table create, index create, or drop transaction,
// in recovery the transaction cannot be rolled back in the usual way,
// a 'rollback' rather means dropping the created or dropped table, if it still exists
#define TRX_UNDO_DICT_TRANS     19
// Id of the table if the preceding field is TRUE
#define TRX_UNDO_TABLE_ID       20
// Offset of the next undo log header on this page, 0 if none
#define TRX_UNDO_NEXT_LOG       28
// Offset of the previous undo log header on this page, 0 if none
#define TRX_UNDO_PREV_LOG       30

// Size of the undo log header without XID information
#define TRX_UNDO_LOG_HDR_SIZE   32

//-----------------------------------------------------------------

// trx undo log record
typedef byte    trx_undo_rec_hdr_t;

#define TRX_UNDO_NEXT_REC       0
#define TRX_UNDO_REC_TYPE       2 // TRX_UNDO_INSERT_REC
#define TRX_UNDO_REC_TABLE_ID   3
#define TRX_UNDO_REC_DATA       11
#define TRX_UNDO_REC_START      13

#define TRX_UNDO_REC_HDR_SIZE   13


//-----------------------------------------------------------------



/* X/Open XA Transaction Identification (XID) */

/** Size of the undo log header without XID information */
#define TRX_UNDO_LOG_OLD_HDR_SIZE (34 + FLST_NODE_SIZE)


/** xid_t::formatID */
#define TRX_UNDO_XA_FORMAT       (TRX_UNDO_LOG_OLD_HDR_SIZE)
/** xid_t::gtrid_length */
#define TRX_UNDO_XA_TRID_LEN     (TRX_UNDO_XA_FORMAT + 4)
/** xid_t::bqual_length */
#define TRX_UNDO_XA_BQUAL_LEN    (TRX_UNDO_XA_TRID_LEN + 4)
/** Distributed transaction identifier data */
#define TRX_UNDO_XA_XID          (TRX_UNDO_XA_BQUAL_LEN + 4)

/* Total size of the undo log header with the XA XID */
#define TRX_UNDO_LOG_XA_HDR_SIZE (TRX_UNDO_XA_XID + XIDDATASIZE)


//-----------------------------------------------------------------

typedef enum en_undo_type {
    /* heap */
    UNDO_HEAP_INSERT = 1,      /* < heap insert */
    UNDO_HEAP_DELETE = 2,      /* < heap delete */
    UNDO_HEAP_UPDATE = 3,      /* < heap update */
    UNDO_HEAP_UPDATE_FULL = 4, /* < heap update full */

    /* btree */
    UNDO_BTREE_INSERT = 5,  /* < btree insert */
    UNDO_BTREE_DELETE = 6,  /* < btree delete */
    UNDO_LOCK_SNAPSHOT = 7, /* < not used */
    UNDO_CREATE_INDEX = 8,  /* < fill index */

    UNDO_LOB_INSERT = 9, /* < lob insert */
    UNDO_LOB_DELETE_COMMIT = 10,

    /* temp table */
    UNDO_TEMP_HEAP_INSERT = 11,
    UNDO_TEMP_HEAP_DELETE = 12,
    UNDO_TEMP_HEAP_UPDATE = 13,
    UNDO_TEMP_HEAP_UPDATE_FULL = 14,
    UNDO_TEMP_BTREE_INSERT = 15,
    UNDO_TEMP_BTREE_DELETE = 16,

    UNDO_LOB_DELETE = 17,

    /* heap chain */
    UNDO_HEAP_INSERT_MIGR = 18,
    UNDO_HEAP_UPDATE_LINKRID = 19,
    UNDO_HEAP_DELETE_MIGR = 20,
    UNDO_HEAP_DELETE_ORG = 21,
    UNDO_HEAP_COMPACT_DELETE = 22,
    UNDO_HEAP_COMPACT_DELETE_ORG = 23,

    /* temp table batch insert */
    UNDO_TEMP_HEAP_BINSERT = 24,
    UNDO_TEMP_BTREE_BINSERT = 25,

    /* PCR heap */
    UNDO_PCRH_ITL = 30,
    UNDO_PCRH_INSERT = 31,
    UNDO_PCRH_DELETE = 32,
    UNDO_PCRH_UPDATE = 33,
    UNDO_PCRH_UPDATE_FULL = 34,
    UNDO_PCRH_UPDATE_LINK_SSN = 35,
    UNDO_PCRH_UPDATE_NEXT_RID = 36,
    UNDO_PCRH_BATCH_INSERT = 37,
    UNDO_PCRH_COMPACT_DELETE = 38,

    /* PCR btree */
    UNDO_PCRB_ITL = 40,
    UNDO_PCRB_INSERT = 41,
    UNDO_PCRB_DELETE = 42,
    UNDO_PCRB_BATCH_INSERT = 43,

    /* lob new delete commit */
    UNDO_LOB_DELETE_COMMIT_RECYCLE = 50,
    UNDO_LOB_ALLOC_PAGE = 51,
    UNDO_CREATE_HEAP = 52, /* < add hash partition */
    UNDO_CREATE_LOB = 53, /* < add hash partition */
    UNDO_LOB_TEMP_ALLOC_PAGE = 54,
    UNDO_LOB_TEMP_DELETE = 55,
} undo_type_t;

#define UNDO_REC_HEADER_SIZE         OFFSET_OF(undo_rec_t, data)

typedef struct st_undo_snapshot {
    uint64  scn; // commit scn or command id
    uint32  undo_page_no;
    uint32  offsets;  // heap page offset and undo page offset
} undo_snapshot_t;

typedef struct st_undo_rec {
    undo_type_t     type;
    uint32          ssn;  // ssn generate current undo
    undo_snapshot_t snapshot;  // old dir value

    union {
        uint64 row_id; // rowid to locate row or itl
        struct {
            uint32 space_id;
            uint32 page_no;
            uint16 dir_slot;
            uint64 seg_file : 10; /* < btree segment entry file_id */
            uint64 seg_page : 30; /* < btree segment entry page_id */
            uint64 user_id : 14;  /* < user id */
            uint64 index_id : 6;  /* < index id */
            uint64 unused : 4;
        };
    };

    uint32  data_size;
    char*   data;
} undo_rec_t;

typedef struct st_undo_data {
    uint64       query_min_scn;
    undo_op_type undo_op;
    uint32       undo_space_index;  // start from 0 (DB_UNDO_START_SPACE_ID)
    uint32       undo_page_no;
    uint32       undo_page_offset;
    undo_rec_t   rec;
} undo_data_t;

//-----------------------------------------------------------------

extern inline status_t trx_undo_prepare(que_sess_t* sess, undo_data_t* undo_data, mtr_t* mtr);
extern inline status_t trx_undo_write_log_rec(que_sess_t* sess, undo_data_t* undo_data, mtr_t* mtr);


#endif  /* _KNL_TRX_UNDO_H */
