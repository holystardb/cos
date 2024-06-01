#ifndef _KNL_TRX_RSEG_H
#define _KNL_TRX_RSEG_H

#include "cm_type.h"
#include "knl_dict.h"
#include "knl_page_size.h"
#include "knl_flst.h"
#include "knl_mtr.h"


/* Undo log segment slot in a rollback segment header */
#define	TRX_RSEG_SLOT_PAGE_NO	0	/* Page number of the header page of an undo log segment */
/* Slot size */
#define TRX_RSEG_SLOT_SIZE	4

/* The offset of the rollback segment header on its page */
#define	TRX_RSEG		FSEG_PAGE_DATA

/* Transaction rollback segment header */
#define	TRX_RSEG_MAX_SIZE	0	/* Maximum allowed size for rollback segment in pages */
#define	TRX_RSEG_HISTORY_SIZE	4	/* Number of file pages occupied by the logs in the history list */
#define	TRX_RSEG_HISTORY	8	/* The update undo logs for committed transactions */
#define	TRX_RSEG_FSEG_HEADER	(8 + FLST_BASE_NODE_SIZE)
/* Header for the file segment where this page is placed */
#define TRX_RSEG_UNDO_SLOTS	(8 + FLST_BASE_NODE_SIZE + FSEG_HEADER_SIZE)
/* Undo log segment slots */

/* The rollback segment memory object */
struct trx_rseg_t {
	/*--------------------------------------------------------*/
	uint32		id;	/*!< rollback segment id == the index of
				its slot in the trx system file copy */
	mutex_t		mutex;	/*!< mutex protecting the fields in this struct except id, which is constant */
	uint32		space;	/*!< space where the rollback segment is header is placed */
	uint32		page_no;/* page number of the rollback segment
				header */
	uint32		max_size;/* maximum allowed size in pages */
	uint32		curr_size;/* current size in pages */
	/*--------------------------------------------------------*/
	/* Fields for update undo logs */
	UT_LIST_BASE_NODE_T(trx_undo_t) update_undo_list;
					/* List of update undo logs */
	UT_LIST_BASE_NODE_T(trx_undo_t) update_undo_cached;
					/* List of update undo log segments cached for fast reuse */
	/*--------------------------------------------------------*/
	/* Fields for insert undo logs */
	UT_LIST_BASE_NODE_T(trx_undo_t) insert_undo_list;
					/* List of insert undo logs */
	UT_LIST_BASE_NODE_T(trx_undo_t) insert_undo_cached;
					/* List of insert undo log segments cached for fast reuse */
	/*--------------------------------------------------------*/
	uint32		last_page_no;	/*!< Page number of the last not yet
					purged log header in the history list;
					FIL_NULL if all list purged */
	uint32		last_offset;	/*!< Byte offset of the last not yet purged log header */
	trx_id_t	last_trx_no;	/*!< Transaction number of the last not yet purged log */
	bool32		last_del_marks;	/*!< TRUE if the last not yet purged log needs purging */
};




//-----------------------------------------------------------------

extern uint32 trx_sysf_rseg_get_page_no(
    trx_sysf_t*	sys_header,	/*!< in: trx system header */
    uint32		i,		/*!< in: slot index == rseg id */
    mtr_t*		mtr); /*!< in: mtr */

extern uint32 trx_rseg_header_create(
    uint32			space,
    const page_size_t&	page_size,
    uint32			max_size,
    uint32			rseg_slot_no,
    mtr_t*			mtr);

//-----------------------------------------------------------------

extern trx_sys_t       *trx_sys;


#endif  /* _KNL_TRX_RSEG_H */
