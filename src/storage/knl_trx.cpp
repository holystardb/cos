#include "knl_trx.h"

#include "knl_buf.h"
#include "knl_fsp.h"
#include "knl_trx_rseg.h"


/** The transaction system */
trx_sys_t       *trx_sys = NULL;


// Creates the trx_sys instance and initializes ib_bh and mutex.
void trx_sys_create(void)
{
    ut_ad(trx_sys == NULL);

    trx_sys = (trx_sys_t *)malloc(sizeof(trx_sys_t));
    memset(trx_sys, 0x00, sizeof(trx_sys_t));
    mutex_create(&trx_sys->mutex);
}


//Gets a pointer to the transaction system header and x-latches its page
trx_sysf_t* trx_sysf_get(mtr_t*	mtr)	/*!< in: mtr */
{
    buf_block_t*	block;
    trx_sysf_t*	header;
    page_id_t page_id(TRX_SYS_SPACE, TRX_SYS_PAGE_NO);
    const page_size_t page_size(0);

    ut_ad(mtr);

    block = buf_page_get(&page_id, page_size, RW_X_LATCH, mtr);
    //buf_block_dbg_add_level(block, SYNC_TRX_SYS_HEADER);

    header = TRX_SYS + buf_block_get_frame(block);

    return(header);
}


//Looks for a free slot for a rollback segment in the trx system file copy
uint32 trx_sysf_rseg_find_free(
        mtr_t*	mtr,			/*!< in/out: mtr */
    bool	include_tmp_slots,	/*!< in: if true, report slots reserved
                                for temp-tablespace as free slots. */
    uint32	nth_free_slots)		/*!< in: allocate nth free slot.
                                0 means next free slot. */
{
    uint32		i;
    trx_sysf_t*	sys_header;

    sys_header = trx_sysf_get(mtr);

    uint32	found_free_slots = 0;
    for (i = 0; i < TRX_SYS_N_RSEGS; i++) {
        uint32	page_no;

        page_no = trx_sysf_rseg_get_page_no(sys_header, i, mtr);

        if (page_no == FIL_NULL) {
            if (found_free_slots++ >= nth_free_slots) {
                return(i);
            }
        }
    }

    return(UINT32_UNDEFINED);
}


// Creates the file page for the transaction system.
// This function is called only at the database creation, before trx_sys_init.
static void trx_sysf_create(mtr_t *mtr)
{
    trx_sysf_t  *sys_header;
    uint32       slot_no;
    buf_block_t *block;
    page_t      *page;
    uint32       page_no;
    byte*        ptr;
    uint32      len;

    ut_ad(mtr);

    /* Note that below we first reserve the file space x-latch,
       and then enter the kernel:
       we must do it in this order to conform to the latching order rules. */

	mtr_x_lock(fil_space_get_latch(SRV_SYSTEM_SPACE_ID, NULL), mtr);

	/* Create the trx sys file block in a new allocated file segment */
	block = fseg_create_general(TRX_SYS_SPACE, FSP_TRX_SYS_PAGE_NO, TRX_SYS + TRX_SYS_FSEG_HEADER, FALSE, mtr);
	//buf_block_dbg_add_level(block, SYNC_TRX_SYS_HEADER);

	ut_a(block->get_page_no() == TRX_SYS_PAGE_NO);

	page = buf_block_get_frame(block);

	mlog_write_uint32(page + FIL_PAGE_TYPE, FIL_PAGE_TYPE_TRX_SYS, MLOG_2BYTES, mtr);

	/* Reset the doublewrite buffer magic number to zero so that we
	know that the doublewrite buffer has not yet been created (this
	suppresses a Valgrind warning) */

	mlog_write_uint32(page + TRX_SYS_DOUBLEWRITE
			 + TRX_SYS_DOUBLEWRITE_MAGIC, 0, MLOG_4BYTES, mtr);

	sys_header = trx_sysf_get(mtr);

	/* Start counting transaction ids from number 1 up */
	mach_write_to_8(sys_header + TRX_SYS_TRX_ID_STORE, 1);

	/* Reset the rollback segment slots.  Old versions of InnoDB
	define TRX_SYS_N_RSEGS as 256 (TRX_SYS_OLD_N_RSEGS) and expect
	that the whole array is initialized. */
	ptr = TRX_SYS_RSEGS + sys_header;
	len = ut_max(TRX_SYS_OLD_N_RSEGS, TRX_SYS_N_RSEGS) * TRX_SYS_RSEG_SLOT_SIZE;
	memset(ptr, 0xff, len);
	ptr += len;
	ut_a(ptr <= page + (UNIV_PAGE_SIZE - FIL_PAGE_DATA_END));

	/* Initialize all of the page.  This part used to be uninitialized. */
	memset(ptr, 0, UNIV_PAGE_SIZE - FIL_PAGE_DATA_END + page - ptr);

    mlog_log_string(sys_header, UNIV_PAGE_SIZE - FIL_PAGE_DATA_END + page - sys_header, mtr);

    // Create all rollback segment in the SYSTEM tablespace
    for (uint32 i = 0; i < TRX_SYS_N_RSEGS; i++) {
        const page_size_t page_size(0);
        page_no = trx_rseg_header_create(TRX_SYS_SPACE, page_size, UINT32_MAX, i, mtr);
        mach_write_to_4(sys_header + TRX_SYS_RSEGS, 1);
    }
}

// Creates and initializes the transaction system at the database creation.
void trx_sys_create_sys_pages(void)
{
    mtr_t mtr;
    mtr_start(&mtr);
    trx_sysf_create(&mtr);
    mtr_commit(&mtr);
}

