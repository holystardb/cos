#include "knl_trx_rseg.h"
#include "knl_trx.h"
#include "knl_buf.h"



/* Rollback segment specification slot offsets */
/*-------------------------------------------------------------*/
#define	TRX_SYS_RSEG_SPACE	0	/* space where the segment
					header is placed; starting with
					MySQL/InnoDB 5.1.7, this is
					UNIV_UNDEFINED if the slot is unused */
#define	TRX_SYS_RSEG_PAGE_NO	4	/*  page number where the segment
					header is placed; this is FIL_NULL
					if the slot is unused */
/*-------------------------------------------------------------*/
/* Size of a rollback segment specification slot */
#define TRX_SYS_RSEG_SLOT_SIZE	8


//Gets the page number of the nth rollback segment slot in the trx system header.
//return page number, FIL_NULL if slot unused
uint32 trx_sysf_rseg_get_page_no(
	trx_sysf_t*	sys_header,	/*!< in: trx system header */
	uint32		i,		/*!< in: slot index == rseg id */
	mtr_t*		mtr)		/*!< in: mtr */
{
	ut_ad(sys_header);
	ut_ad(i < TRX_SYS_N_RSEGS);

	return(mtr_read_uint32(sys_header + TRX_SYS_RSEGS
			      + i * TRX_SYS_RSEG_SLOT_SIZE
			      + TRX_SYS_RSEG_PAGE_NO, MLOG_4BYTES, mtr));
}

trx_rsegf_t* trx_rsegf_get_new(
    uint32			space,
    uint32			page_no,
    const page_size_t&	page_size,
    mtr_t*			mtr)
{
    buf_block_t*	block;
    trx_rsegf_t*	header;
    page_id_t       page_id(space, page_no);

    block = buf_page_get(&page_id, page_size, RW_X_LATCH, mtr);

    //buf_block_dbg_add_level(block, SYNC_RSEG_HEADER_NEW);

    header = TRX_RSEG + buf_block_get_frame(block);

    return(header);
}

uint32 trx_rsegf_get_nth_undo(
    trx_rsegf_t*	rsegf,	/*!< in: rollback segment header */
    uint32		n,	/*!< in: index of slot */
    mtr_t*		mtr)	/*!< in: mtr */
{
    //ut_a(n < TRX_RSEG_N_SLOTS);

    return(mtr_read_uint32(rsegf + TRX_RSEG_UNDO_SLOTS
        + n * TRX_RSEG_SLOT_SIZE, MLOG_4BYTES, mtr));
}

void trx_rsegf_set_nth_undo(
    trx_rsegf_t*	rsegf,	/*!< in: rollback segment header */
    uint32		n,	/*!< in: index of slot */
    uint32		page_no,/*!< in: page number of the undo log segment */
    mtr_t*		mtr)	/*!< in: mtr */
{
    //ut_a(n < TRX_RSEG_N_SLOTS);

    mlog_write_uint32(rsegf + TRX_RSEG_UNDO_SLOTS + n * TRX_RSEG_SLOT_SIZE,
        page_no, MLOG_4BYTES, mtr);
}

//Sets the space id of the nth rollback segment slot in the trx system file copy
void trx_sysf_rseg_set_space(
    trx_sysf_t*	sys_header,	/*!< in: trx sys file copy */
    uint32		i,		/*!< in: slot index == rseg id */
    uint32		space,		/*!< in: space id */
    mtr_t*		mtr)		/*!< in: mtr */
{
    ut_ad(sys_header);
    ut_ad(i < TRX_SYS_N_RSEGS);

    mlog_write_uint32(sys_header + TRX_SYS_RSEGS
        + i * TRX_SYS_RSEG_SLOT_SIZE
        + TRX_SYS_RSEG_SPACE,
        space,
        MLOG_4BYTES, mtr);
}

// Sets the page number of the nth rollback segment slot in the trx system header
void trx_sysf_rseg_set_page_no(
    trx_sysf_t*	sys_header,	/*!< in: trx sys header */
    uint32		i,		/*!< in: slot index == rseg id */
    uint32		page_no,	/*!< in: page number, FIL_NULL if the slot is reset to unused */
    mtr_t*		mtr)		/*!< in: mtr */
{
    ut_ad(sys_header);
    ut_ad(i < TRX_SYS_N_RSEGS);

    mlog_write_uint32(sys_header + TRX_SYS_RSEGS
        + i * TRX_SYS_RSEG_SLOT_SIZE
        + TRX_SYS_RSEG_PAGE_NO,
        page_no,
        MLOG_4BYTES, mtr);
}

/** Creates a rollback segment header.
This function is called only when a new rollback segment is created in
the database.
@param[in]	space		space id
@param[in]	page_size	page size
@param[in]	max_size	max size in pages
@param[in]	rseg_slot_no	rseg id == slot number in trx sys
@param[in,out]	mtr		mini-transaction
@return page number of the created segment, FIL_NULL if fail */
uint32 trx_rseg_header_create(
	uint32			space,
	const page_size_t&	page_size,
	uint32			max_size,
	uint32			rseg_slot_no,
	mtr_t*			mtr)
{
	uint32		page_no;
	trx_rsegf_t*	rsegf;
	trx_sysf_t*	sys_header;
	uint32		i;
	buf_block_t*	block;

	ut_ad(mtr);
	ut_ad(mtr_memo_contains(mtr, fil_space_get_latch(space, NULL), MTR_MEMO_X_LOCK));

	/* Allocate a new file segment for the rollback segment */
	block = fseg_create_general(space, 0, TRX_RSEG + TRX_RSEG_FSEG_HEADER, FALSE, mtr);
	if (block == NULL) {
		/* No space left */
		return(FIL_NULL);
	}

	//buf_block_dbg_add_level(block, SYNC_RSEG_HEADER_NEW);

	page_no = block->page.id.page_no();

	/* Get the rollback segment file page */
	rsegf = trx_rsegf_get_new(space, page_no, page_size, mtr);

	/* Initialize max size field */
	mlog_write_uint32(rsegf + TRX_RSEG_MAX_SIZE, max_size,
			 MLOG_4BYTES, mtr);

	/* Initialize the history list */

	mlog_write_uint32(rsegf + TRX_RSEG_HISTORY_SIZE, 0, MLOG_4BYTES, mtr);
	flst_init(rsegf + TRX_RSEG_HISTORY, mtr);

	/* Reset the undo log slots */
	for (i = 0; i < TRX_RSEG_UNDO_SLOTS; i++) {
		trx_rsegf_set_nth_undo(rsegf, i, FIL_NULL, mtr);
	}

	//if (!trx_sys_is_noredo_rseg_slot(rseg_slot_no)) {
		/* Non-redo rseg are re-created on restart and so no need
		to persist this information in sys-header. Anyway, on restart
		this information is not valid too as there is no space with
		persisted space-id on restart. */

		/* Add the rollback segment info to the free slot in the trx system header */

		sys_header = trx_sysf_get(mtr);
		trx_sysf_rseg_set_space(sys_header, rseg_slot_no, space, mtr);
		trx_sysf_rseg_set_page_no(sys_header, rseg_slot_no, page_no, mtr);
	//}

	return(page_no);
}

