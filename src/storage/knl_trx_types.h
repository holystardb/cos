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

// Transaction undo log page memory object;
struct st_trx_undo_page {
    uint64           scn;
    uint16           type : 1; // TRX_UNDO_INSERT or TRX_UNDO_UPDATE
    uint16           offset : 15;
    uint16           free_size;
    uint32           page_no;
    buf_block_t*     block;

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

typedef struct st_trx {
    trx_slot_id_t     trx_slot_id;
    bool32            is_active;
    time_t            start_time;

    SLIST_BASE_NODE_T(trx_undo_page_t) insert_undo;
    SLIST_BASE_NODE_T(trx_undo_page_t) update_undo;

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
    bool32    is_overwrite_scn;
    uint32    status;
} trx_status_t;

#define TRX_SLOT_PAGE_COUNT_PER_RSEG     4

/* The rollback segment memory object */
struct st_trx_rseg {
    uint32            id;       /* rollback segment id */
    uint32            max_size; /* maximum allowed size in pages */
    uint32            curr_size;/* current size in pages */

                                // trx_list
    trx_t**           trxs;
    mutex_t           trx_mutex;
    SLIST_BASE_NODE_T(trx_t) trx_base;

    // trx_slot list
    void**            trx_slots;
    buf_block_t*      trx_slot_blocks[TRX_SLOT_PAGE_COUNT_PER_RSEG];
    // undo page
    mutex_t           undo_cache_mutex;

    SLIST_BASE_NODE_T(trx_undo_page_t) insert_undo_cache;
    SLIST_BASE_NODE_T(trx_undo_page_t) update_undo_cache;
    SLIST_BASE_NODE_T(trx_undo_page_t) history_undo_list;


    uint32      last_page_no;   /* Page number of the last not yet purged log header in the history list;
                                FIL_NULL if all list purged */
    uint32      last_offset;    /* Byte offset of the last not yet purged log header */
                                //trx_id_t    last_trx_no;    /* Transaction number of the last not yet purged log */
    bool32      last_del_marks; /* TRUE if the last not yet purged log needs purging */
};


#define TRX_RSEG_MAX_COUNT                64    // 64 * 4 pages = 4MB
#define TRX_RSEG_MIN_COUNT                4     // 4  * 4 pages = 256KB
#define TRX_RSEG_DEFAULT_COUNT            32    // 32 * 4 pages = 2MB

#define TRX_RSEG_UNDO_PAGE_MAX_COUNT      16384 // 256GB

// The transaction system central memory data structure
typedef struct st_trx_sys {
    mutex_t        mutex;
    uint64         max_trx_id;
    uint32         rseg_count;
    trx_rseg_t     rseg_array[TRX_RSEG_MAX_COUNT];

    time_t         init_time;
    atomic64_t     scn;
    memory_pool_t* mem_pool;
    memory_stack_context_t* context;
} trx_sys_t;


//-----------------------------------------------------------------

#define TRX_GET_RSEG(rseg_id)           &trx_sys->rseg_array[rseg_id]

#define TRX_GET_RSEG_TRX(slot_id)       trx_sys->rseg_array[slot_id.rseg_id].trxs[slot_id.slot]

#define TRX_GET_RSEG_TRX_SLOT(slot_id)  trx_sys->rseg_array[slot_id.rseg_id].trx_slots[slot_id.slot]

#define TRX_GET_RSEG_TRX_SLOT_PAGE_NO(slot_id)                          \
    (FSP_FIRST_RSEG_PAGE_NO + slot_id.rseg_id * TRX_SLOT_PAGE_COUNT_PER_RSEG + slot_id.slot / trx_slot_count_per_page)

#define TRX_GET_RSEG_TRX_SLOT_BLOCK(slot_id)                            \
    trx_sys->rseg_array[slot_id.rseg_id].trx_slot_blocks[slot_id.slot / trx_slot_count_per_page]




#ifdef __cplusplus
}
#endif // __cplusplus

#endif  /* _KNL_TRX_TYPES_H */