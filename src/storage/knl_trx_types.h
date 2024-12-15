#ifndef _KNL_TRX_TYPES_H
#define _KNL_TRX_TYPES_H

#include "cm_type.h"
#include "cm_list.h"
#include "knl_server.h"
#include "knl_buf.h"

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

// File objects
typedef byte    trx_sysf_t;          // Transaction system header
typedef byte    trx_rsegf_t;         // Rollback segment header
typedef byte    trx_undo_seg_hdr_t;  // Undo segment header
typedef byte    trx_undo_log_hdr_t;  // Undo log header
typedef byte    trx_undo_page_hdr_t; // Undo log page header
typedef byte    trx_undo_rec_t;      //Undo log record




// --------------------------------------------------------------------------------------------

#pragma pack(4)

/* Transaction slot header */
#define TRX_RSEG_SLOT_SCN               0
#define TRX_RSEG_SLOT_INSERT_LOG_PAGE   8
#define TRX_RSEG_SLOT_UPDATE_LOG_PAGE   12
#define TRX_RSEG_SLOT_STATUS            16
#define TRX_RSEG_SLOT_RESERVED          17
#define TRX_RSEG_SLOT_XNUM              19

typedef struct st_trx_slot       trx_slot_t;

// 24 bytes: txn slot in txn page
struct st_trx_slot {
    uint64            scn;
    uint32            insert_page_no;
    uint32            update_page_no;
    // XACT_BEGIN / XACT_END / XACT_XA_PREPARE / XACT_XA_ROLLBACK
    uint64            status : 8;
    uint64            reserved : 16;
    uint64            xnum : 40;
};

typedef union st_undo_page_id {
    uint32  value;
    struct {
        uint32 file : 10;
        uint32 page : 22;
    };
} undo_page_id_t;

typedef struct st_trx_pcr_itl {
    union {
        uint64        scn;  // commit scn
        struct {
            uint32    ssn; // txn ssn, command id
            uint16    fsc; // free space credit (bytes)
            uint16    unused;  // for aligned
        };
    };

    uint64            xid;
    undo_page_id_t    undo_page; // undo page for current transaction

    union {
        struct {
            uint16    undo_slot;
            uint16    flags;
        };
        struct {
            uint64    aligned1;
            uint16    is_active : 1;  // committed or not
            uint16    is_overwrite_scn : 1;   // txn scn overwrite or not
            uint16    is_copied : 1;  // itl is copied or not
            uint16    is_historical : 1;  // itl is historical or not (used in CR rollback)
            uint16    is_fast : 1;   // itl is fast committed or not
            uint16    unused : 11;
        };
    };
} trx_pcr_itl_t;

typedef struct st_trx_info {
    uint64    scn;
    bool8     is_overwrite_scn;
    uint8     status;  // XACT_BEGIN / XACT_END / XACT_XA_PREPARE / XACT_XA_ROLLBACK
    uint8     unused[2];
} trx_info_t;

#pragma pack()


// --------------------------------------------------------------

typedef byte    trx_slot_page_hdr_t; // trx_slot page header

#define TRX_SLOT_PAGE_HDR              FIL_PAGE_DATA
#define TRX_SLOT_PAGE_OWSCN            0
#define TRX_SLOT_PAGE_HEADER_SIZE      8

//
typedef struct st_trx_rseg       trx_rseg_t;
typedef struct st_trx_undo_page  trx_undo_page_t;

union trx_slot_id_t {
    uint64 id;
    struct {
        uint64  rseg_id : 8;
        uint64  slot : 16;    // trx_slot index of rseg
        uint64  xnum : 40;
    };
};

// 32bytes, Transaction undo log page memory object;
struct st_trx_undo_page {
    uint32           node_page_no : 16;
    uint32           node_page_offset : 16;
    uint32           page_no;
    uint64           page_offset : 16;
    uint64           scn_timestamp : 48;
    buf_block_t*     guess_block;
    SLIST_NODE_T(trx_undo_page_t) list_node;
};

typedef struct st_undo_rowid {
    roll_ptr_t    uba;
    union {
        page_no_t page_no;
        uint16    offset;
        uint16    reserved;
    };
} undo_rowid_t;

typedef SLIST_BASE_NODE_T(trx_undo_page_t) trx_undo_page_base;

typedef struct st_trx {
    trx_slot_id_t     trx_slot_id;
    mutex_t           mutex;
    bool32            is_active;

    //SLIST_BASE_NODE_T(trx_undo_page_t) insert_undo;
    //SLIST_BASE_NODE_T(trx_undo_page_t) update_undo;
    trx_undo_page_base insert_undo;
    trx_undo_page_base update_undo;

    // listnode for rseg->trxs
    SLIST_NODE_T(struct st_trx) list_node;
} trx_t;

// trx_status_t.status
#define XACT_END            0
#define XACT_BEGIN          1
#define XACT_XA_PREPARE     2
#define XACT_XA_ROLLBACK    3

typedef struct st_trx_status {
    uint64    scn;
    bool32    is_ow_scn;  // overwrite scn
    uint32    status;
} trx_status_t;

#define TRX_SLOT_PAGE_COUNT_PER_RSEG     8  // total 4KB * 8 = 32KB
#define TRX_SLOT_COUNT_PER_PAGE          ((UNIV_SYSTRANS_PAGE_SIZE - TRX_SLOT_PAGE_HDR - TRX_SLOT_PAGE_HEADER_SIZE - FIL_PAGE_DATA_END) / sizeof(trx_slot_t))
#define TRX_SLOT_COUNT_PER_RSEG          (TRX_SLOT_PAGE_COUNT_PER_RSEG * TRX_SLOT_COUNT_PER_PAGE)


/* The rollback segment memory object */
struct st_trx_rseg {
    uint32            id;       // rollback segment id
    uint32            undo_space_id;

    uint32            max_size; /* maximum allowed size in pages */
    uint32            curr_size;/* current size in pages */

    // trx_list
    trx_t             trx_list[TRX_SLOT_COUNT_PER_RSEG];
    mutex_t           trx_mutex;
    SLIST_BASE_NODE_T(trx_t) trx_free_list;
    SLIST_BASE_NODE_T(trx_t) trx_need_recovery_list;

    // trx_slot list
    void*             trx_slot_list[TRX_SLOT_COUNT_PER_RSEG];
    buf_block_t*      trx_slot_blocks[TRX_SLOT_PAGE_COUNT_PER_RSEG];
    uint64            page_ow_scn[TRX_SLOT_PAGE_COUNT_PER_RSEG];
    // undo page
    mutex_t           insert_undo_cache_mutex;
    mutex_t           update_undo_cache_mutex;
    mutex_t           free_undo_list_mutex;
    bool32            extend_undo_in_process;
    uint32            undo_cached_max_count; //1024 * 16KB * 2 = 32MB

    SLIST_BASE_NODE_T(trx_undo_page_t) insert_undo_cache;
    SLIST_BASE_NODE_T(trx_undo_page_t) update_undo_cache;
    SLIST_BASE_NODE_T(trx_undo_page_t) free_undo_list;

    uint32      last_page_no;   /* Page number of the last not yet purged log header in the history list;
                                FIL_NULL if all list purged */
    uint32      last_offset;    /* Byte offset of the last not yet purged log header */
                                //trx_id_t    last_trx_no;    /* Transaction number of the last not yet purged log */
    bool32      last_del_marks; /* TRUE if the last not yet purged log needs purging */
};


#define TRX_RSEG_MAX_COUNT                    256
#define TRX_RSEG_MIN_COUNT                    32


// The transaction system central memory data structure
typedef struct st_trx_sys {
    mutex_t        mutex;
    mutex_t        used_pages_mutex;

    uint64         max_trx_id;
    uint32         undo_space_count;
    uint32         rseg_count;
    trx_rseg_t     rseg_array[TRX_RSEG_MAX_COUNT];

    bool32         extend_in_process;
    UT_LIST_BASE_NODE_T(memory_page_t) used_pages;
    SLIST_BASE_NODE_T(trx_undo_page_t) free_undo_page_list;

    time_t         init_time;
    atomic64_t     scn;
    memory_pool_t* mem_pool;
    memory_stack_context_t* context;
} trx_sys_t;


//-----------------------------------------------------------------

#define TRX_GET_RSEG(rseg_id)           &trx_sys->rseg_array[rseg_id]

#define TRX_GET_RSEG_TRX(slot_id)       trx_sys->rseg_array[slot_id.rseg_id].trx_list[slot_id.slot]

#define TRX_GET_RSEG_TRX_SLOT(slot_id)  trx_sys->rseg_array[slot_id.rseg_id].trx_slot_list[slot_id.slot]

#define TRX_GET_RSEG_TRX_SLOT_PAGE_NO(slot_id)                          \
    (uint32)(FSP_FIRST_RSEG_PAGE_NO + slot_id.rseg_id * TRX_SLOT_PAGE_COUNT_PER_RSEG + slot_id.slot / TRX_SLOT_COUNT_PER_PAGE)

#define TRX_GET_RSEG_TRX_SLOT_BLOCK(slot_id)                            \
    trx_sys->rseg_array[slot_id.rseg_id].trx_slot_blocks[slot_id.slot / TRX_SLOT_COUNT_PER_PAGE]




#ifdef __cplusplus
}
#endif // __cplusplus

#endif  /* _KNL_TRX_TYPES_H */
