#ifndef _KNL_TRX_RSEG_H
#define _KNL_TRX_RSEG_H

#include "cm_type.h"
#include "cm_list.h"
#include "knl_page_size.h"
#include "knl_flst.h"
#include "knl_mtr.h"
#include "knl_trx_undo.h"
#include "knl_trx_types.h"

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

#define TRX_UNDO_INVALID_PAGENO FIL_NULL

/* The offset of the rollback segment header on its page */
#define TRX_RSEG                FSEG_PAGE_DATA

/* Transaction rollback segment header */
#define TRX_RSEG_MAX_SIZE       0   /* Maximum allowed size for rollback segment in pages */
#define TRX_RSEG_HISTORY_SIZE   4   /* Number of file pages occupied by the logs in the history list */
#define TRX_RSEG_HISTORY        8   /* The update undo logs for committed transactions */
#define TRX_RSEG_FSEG_HEADER    (8 + FLST_BASE_NODE_SIZE)  /* Header for the file segment where this page is placed */
#define TRX_RSEG_RESERVED       (8 + 4 * FLST_BASE_NODE_SIZE) // RESERVED 266 BYTES
#define TRX_RSEG_SLOT_HEADER    (8 + 4 * FLST_BASE_NODE_SIZE + 266) // start from 376




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

// txn slot in txn page
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




extern uint32 trx_sysf_rseg_get_page_no(
    trx_sysf_t*	sys_header,	/*!< in: trx system header */
    uint32		i,		/*!< in: slot index == rseg id */
    mtr_t*		mtr); /*!< in: mtr */

extern void trx_sys_create_sys_pages(void);

extern inline trx_sysf_t* trx_sysf_get(mtr_t* mtr);

extern inline trx_t* trx_rseg_assign_and_alloc_trx(mtr_t* mtr);
extern inline void trx_rseg_release_trx(trx_t* trx);
extern inline void trx_rseg_set_end(trx_t* trx, mtr_t* mtr);
extern inline uint64 trx_get_next_scn();
extern inline void trx_get_status_by_itl(trx_slot_id_t trx_slot_id, trx_status_t* trx_status);


//-----------------------------------------------------------------






#ifdef __cplusplus
}
#endif // __cplusplus

#endif  /* _KNL_TRX_RSEG_H */
