#include "knl_fsp.h"
#include "cm_util.h"
#include "cm_log.h"
#include "knl_buf.h"
#include "knl_mtr.h"
#include "knl_flst.h"
#include "knl_page.h"
#include "knl_dict.h"
#include "knl_file_system.h"

static uint32 xdes_calc_descriptor_page(const page_size_t& page_size, uint32 offset);
//static bool32 fil_node_prepare_for_io(fil_node_t *node);
static void fsp_space_modify_check(uint32 id, const mtr_t* mtr);
static void fsp_init_file_page(buf_block_t* block, mtr_t* mtr);
//static void fil_node_complete_io(fil_node_t* node, uint32 type);

// Gets the page number from the nth fragment page slot.
// return page number, FIL_NULL if not in use
static uint32 fseg_get_nth_frag_page_no(
    fseg_inode_t *inode,	/*!< in: segment inode */
    uint32        n,	/*!< in: slot index */
    mtr_t        *mtr);

static void fseg_set_nth_frag_page_no(
    fseg_inode_t *inode,  /*!< in: segment inode */
    uint32        n,      /*!< in: slot index */
    uint32        page_no,/*!< in: page number to set */
    mtr_t        *mtr);    /*!< in/out: mini-transaction */

static fseg_inode_t* fsp_seg_inode_page_get_nth_inode(
    page_t*	page,	/*!< in: segment inode page */
    uint32	i,	/*!< in: inode index on page */
    uint32	zip_size,   /*!< in: compressed page size, or 0 */
    mtr_t*	mtr);

static uint32 fsp_seg_inode_page_find_free(
    page_t*	page,	/*!< in: segment inode page */
    uint32	i,	/*!< in: search forward starting from this index */
    uint32	zip_size,/*!< in: compressed page size, or 0 */
    mtr_t*	mtr);	/*!< in/out: mini-transaction */



byte *fut_get_ptr(space_id_t space, const page_size_t &page_size,
                  fil_addr_t addr, rw_lock_type_t rw_latch, mtr_t *mtr,
                  buf_block_t **ptr_block)
{
    buf_block_t *block;
    byte *ptr;
    const page_id_t page_id(space, addr.page);

    ut_ad(addr.boffset < UNIV_PAGE_SIZE);
    ut_ad((rw_latch == RW_S_LATCH) || (rw_latch == RW_X_LATCH));

    block = buf_page_get(page_id, page_size, rw_latch, mtr);
    ptr = buf_block_get_frame(block) + addr.boffset;

    if (ptr_block != NULL) {
      *ptr_block = block;
    }

    return(ptr);
}






// Sets a descriptor bit of a page.
inline void xdes_set_bit(
    xdes_t  *descr,  /*!< in: descriptor */
    uint32   bit,    /*!< in: XDES_FREE_BIT or XDES_CLEAN_BIT */
    uint32   offset, /*!< in: page offset within extent: 0 ... FSP_EXTENT_SIZE - 1 */
    bool32   val,    /*!< in: bit value */
    mtr_t   *mtr)    /*!< in/out: mini-transaction */
{
    uint32 index;
    uint32 byte_index;
    uint32 bit_index;
    uint32 descr_byte;

    ut_ad(mtr_memo_contains_page(mtr, descr, MTR_MEMO_PAGE_X_FIX));
    ut_ad((bit == XDES_FREE_BIT) || (bit == XDES_CLEAN_BIT));
    ut_ad(offset < FSP_EXTENT_SIZE);

    index = bit + XDES_BITS_PER_PAGE * offset;

    byte_index = index / 8;
    bit_index = index % 8;

    descr_byte = mach_read_from_1(descr + XDES_BITMAP + byte_index);
    descr_byte = ut_bit32_set_nth(descr_byte, bit_index, val);

    mlog_write_uint32(descr + XDES_BITMAP + byte_index, descr_byte, MLOG_1BYTE, mtr);
}

/**********************************************************************//**
Gets a descriptor bit of a page.
@return TRUE if free */
bool32 xdes_get_bit(
	const xdes_t*	descr,	/*!< in: descriptor */
	uint32		bit,	/*!< in: XDES_FREE_BIT or XDES_CLEAN_BIT */
	uint32		offset)	/*!< in: page offset within extent:
				0 ... FSP_EXTENT_SIZE - 1 */
{
	ut_ad(offset < FSP_EXTENT_SIZE);
	ut_ad(bit == XDES_FREE_BIT || bit == XDES_CLEAN_BIT);

	uint32	index = bit + XDES_BITS_PER_PAGE * offset;

	uint32	bit_index = index % 8;
	uint32	byte_index = index / 8;

	return(ut_bit32_get_nth(
			mlog_read_uint32(descr + XDES_BITMAP + byte_index,
					MLOG_1BYTE),
			bit_index));
}

/**********************************************************************//**
Gets a descriptor bit of a page.
@return TRUE if free */
bool32 xdes_mtr_get_bit(
    const xdes_t*   descr,  /*!< in: descriptor */
    uint32          bit,    /*!< in: XDES_FREE_BIT or XDES_CLEAN_BIT */
    uint32          offset, /*!< in: page offset within extent: 0 ... FSP_EXTENT_SIZE - 1 */
    mtr_t*          mtr)    /*!< in: mini-transaction */
{
    ut_ad(mtr->is_active());
    ut_ad(mtr_memo_contains_page(mtr, descr, MTR_MEMO_PAGE_X_FIX));

    return(xdes_get_bit(descr, bit, offset));
}

// Looks for a descriptor bit having the desired value.
// Starts from hint and scans upward;
// at the end of the extent the search is wrapped to the start of the extent.
// return bit index of the bit, ULINT_UNDEFINED if not found
uint32 xdes_find_bit(
    xdes_t* descr,  /*!< in: descriptor */
    uint32  bit,    /*!< in: XDES_FREE_BIT or XDES_CLEAN_BIT */
    bool32  val,    /*!< in: desired bit value */
    uint32  hint,   /*!< in: hint of which bit position would be desirable */
    mtr_t*  mtr)    /*!< in/out: mini-transaction */
{
    uint32 i;

    ut_ad(descr && mtr);
    ut_ad(val <= TRUE);
    ut_ad(hint < FSP_EXTENT_SIZE);
    ut_ad(mtr_memo_contains_page(mtr, descr, MTR_MEMO_PAGE_X_FIX));

    for (i = hint; i < FSP_EXTENT_SIZE; i++) {
        if (val == xdes_mtr_get_bit(descr, bit, i, mtr)) {
            return(i);
        }
    }

    for (i = 0; i < hint; i++) {
        if (val == xdes_mtr_get_bit(descr, bit, i, mtr)) {
            return(i);
        }
    }

    return(UINT32_UNDEFINED);
}

// Returns the number of used pages in a descriptor
uint32 xdes_get_n_used(const xdes_t *descr, mtr_t *mtr)
{
    uint32 count = 0;

    ut_ad(descr && mtr);
    ut_ad(mtr_memo_contains_page(mtr, descr, MTR_MEMO_PAGE_X_FIX));

    for (uint32 i = 0; i < FSP_EXTENT_SIZE; ++i) {
        if (FALSE == xdes_mtr_get_bit(descr, XDES_FREE_BIT, i, mtr)) {
            count++;
        }
    }

    return(count);
}

// Returns true if extent contains no used pages(totally free)
bool32 xdes_is_free(const xdes_t *descr, mtr_t *mtr)
{
    if (0 == xdes_get_n_used(descr, mtr)) {
        return(TRUE);
    }

    return(FALSE);
}

// Returns true if extent contains no free pages
bool32 xdes_is_full(const xdes_t  *descr, mtr_t *mtr)
{
    if (FSP_EXTENT_SIZE == xdes_get_n_used(descr, mtr)) {
        return TRUE;
    }

    return FALSE;
}

// Sets the state of an xdes.
void xdes_set_state(
    xdes_t  *descr,  /*!< in/out: descriptor */
    uint32   state,  /*!< in: state to set */
    mtr_t   *mtr)    /*!< in/out: mini-transaction */
{
    ut_ad(descr && mtr);
    ut_ad(state >= XDES_FREE);
    ut_ad(state <= XDES_FSEG);
    ut_ad(mtr_memo_contains_page(mtr, descr, MTR_MEMO_PAGE_X_FIX));

    mlog_write_uint32(descr + XDES_STATE, state, MLOG_4BYTES, mtr);
}

// Gets the state of an xdes.
uint32 xdes_get_state(const xdes_t *descr, mtr_t *mtr)
{
    uint32  state;

    ut_ad(descr && mtr);
    ut_ad(mtr_memo_contains_page(mtr, descr, MTR_MEMO_PAGE_X_FIX));

    state = mach_read_from_4(descr + XDES_STATE);
    ut_ad(state - 1 < XDES_FSEG);
    return state;
}

// Inits an extent descriptor to the free and clean state.
void xdes_init(xdes_t *descr, mtr_t  *mtr)
{
    uint32  i;

    ut_ad(descr && mtr);
    ut_ad(mtr_memo_contains_page(mtr, descr, MTR_MEMO_PAGE_X_FIX));
    ut_ad((XDES_SIZE - XDES_BITMAP) % 4 == 0);

    for (i = XDES_BITMAP; i < XDES_SIZE; i += 4) {
        mlog_write_uint32(descr + i, 0xFFFFFFFFUL, MLOG_4BYTES, mtr);
    }

    xdes_set_state(descr, XDES_FREE, mtr);
}

uint32 xdes_calc_descriptor_index(
	const page_size_t&	page_size,
	uint32			offset)
{
	return(ut_2pow_remainder(offset, page_size.physical())
	       / FSP_EXTENT_SIZE);
}

// Get pointer to a the extent descriptor of a page
xdes_t* xdes_get_descriptor_with_space_hdr(
    fsp_header_t*   sp_header,
    uint32          space,
    uint32          offset,
    mtr_t*          mtr)
{
    uint32  limit;
    uint32  size;
    uint32  descr_page_no;
    uint32  flags;
    page_t* descr_page;

    ut_ad(mtr_memo_contains_page(mtr, sp_header, MTR_MEMO_PAGE_X_FIX));
    ut_ad(page_offset(sp_header) == FSP_HEADER_OFFSET);
    /* Read free limit and space size */
    limit = mach_read_from_4(sp_header + FSP_FREE_LIMIT);
    size  = mach_read_from_4(sp_header + FSP_SIZE);
    flags = mach_read_from_4(sp_header + FSP_SPACE_FLAGS);

    if ((offset >= size) || (offset >= limit)) {
        return(NULL);
    }

    buf_block_t   *block;
    const page_size_t page_size(flags);
    descr_page_no = xdes_calc_descriptor_page(page_size, offset);
    if (descr_page_no == 0) {
        /* It is on the space header page */
        descr_page = page_align(sp_header);
        block = NULL;
    } else {
        const page_id_t page_id(space, descr_page_no);
        block = buf_page_get(page_id, page_size, RW_X_LATCH, mtr);
        descr_page = buf_block_get_frame(block);
    }

    return(descr_page + XDES_ARR_OFFSET
       + XDES_SIZE * xdes_calc_descriptor_index(page_size, offset));
}

// Gets pointer to a the extent descriptor of a page.
// The page where the extent descriptor resides is x-locked.
xdes_t* xdes_get_descriptor(
    uint32  space_id,
    uint32  offset,
    mtr_t*  mtr)
{
    buf_block_t*        block;
    fsp_header_t*       sp_header;
    const page_id_t     page_id(space_id, 0);
    const page_size_t   page_size(0);

    block = buf_page_get(page_id, page_size, RW_X_LATCH, mtr);

    sp_header = FSP_HEADER_OFFSET + buf_block_get_frame(block);
    return(xdes_get_descriptor_with_space_hdr(sp_header, space_id, offset, mtr));
}

// Gets pointer to a the extent descriptor
// if the file address of the descriptor list node is known.
// The page where the extent descriptor resides is x-locked.
//return pointer to the extent descriptor
xdes_t* xdes_lst_get_descriptor(
    uint32              space,  /*!< in: space id */
    const page_size_t&  page_size,
    fil_addr_t          lst_node,/*!< in: file address of the list node contained in the descriptor */
    mtr_t*              mtr)    /*!< in/out: mini-transaction */
{
    xdes_t*	descr;

    ut_ad(mtr);
    //ut_ad(mtr_memo_contains(mtr, fil_space_get_latch(space, NULL), MTR_MEMO_X_LOCK));
    descr = fut_get_ptr(space, page_size, lst_node, RW_X_LATCH, mtr, NULL) - XDES_FLST_NODE;

    return(descr);
}

/********************************************************************//**
Returns page offset of the first page in extent described by a descriptor.
@return offset of the first page in extent */
inline uint32 xdes_get_offset(const xdes_t* descr)
{
	ut_ad(descr);

	return(page_get_page_no(page_align((void *)descr))
	       + ((page_offset((void *)descr) - XDES_ARR_OFFSET) / XDES_SIZE)
	       * FSP_EXTENT_SIZE);
}

/** Calculates the page where the descriptor of a page resides.
@param[in]	page_size	page size
@param[in]	offset		page offset
@return descriptor page offset */
uint32 xdes_calc_descriptor_page(const page_size_t& page_size, uint32 offset)
{
	ut_ad(UNIV_PAGE_SIZE > XDES_ARR_OFFSET + (UNIV_PAGE_SIZE / FSP_EXTENT_SIZE) * XDES_SIZE);
	//ut_ad(UNIV_ZIP_SIZE_MIN > XDES_ARR_OFFSET + (UNIV_ZIP_SIZE_MIN / FSP_EXTENT_SIZE) * XDES_SIZE);

#ifdef UNIV_DEBUG
	if (page_size.is_compressed()) {
		ut_a(page_size.physical() > XDES_ARR_OFFSET  + (page_size.physical() / FSP_EXTENT_SIZE) * XDES_SIZE);
	}
#endif /* UNIV_DEBUG */

	return(ut_2pow_round(offset, page_size.physical()));
}


static uint32 fsp_get_autoextend_increment(fil_space_t* space)
{
    if (space->id >= FIL_USER_SPACE_ID) {
        
    } else if (space->id == FIL_SYSTEM_SPACE_ID) {
    } else {
    }
    return FSP_EXTENT_SIZE * FSP_FREE_ADD;
}

// Tries to extend the last data file of a tablespace if it is auto-extending.
static bool32 fsp_try_extend_data_file(
    uint32       *actual_increase,/*!< out: actual increase in pages */
    fil_space_t  *space,  /*!< in: space */
    fsp_header_t *header,  /*!< in/out: space header */
    mtr_t        *mtr)
{
    uint32    size;  /* current number of pages in the datafile */
    uint32    size_increase;  /* number of pages to extend this file */
    uint32    actual_size;

    ut_d(fsp_space_modify_check(space->id, mtr));

    size = mach_read_from_4(header + FSP_SIZE);
    ut_ad(size == space->size_in_header);

    const page_size_t page_size(mach_read_from_4(header + FSP_SPACE_FLAGS));

    size_increase = fsp_get_autoextend_increment(space);
    if (size_increase == 0) {
        return FALSE;
    }

    if (!fil_space_extend(space, size + size_increase, &actual_size)) {
        return FALSE;
    }

    space->size_in_header += actual_size;
    mlog_write_uint32(header + FSP_SIZE, space->size_in_header, MLOG_4BYTES, mtr);

    return TRUE;
}


static void fsp_space_modify_check(uint32 id, const mtr_t* mtr)
{
	switch (mtr_get_log_mode((mtr_t*)mtr)) {
	case MTR_LOG_SHORT_INSERTS:
	case MTR_LOG_NONE:
		/* These modes are only allowed within a non-bitmap page
		when there is a higher-level redo log record written. */
		break;
	case MTR_LOG_NO_REDO:
#ifdef UNIV_DEBUG
		{
			//const fil_type_t	type = fil_space_get_type(id);
			//ut_a(id == srv_tmp_space.space_id()
			//     || srv_is_tablespace_truncated(id)
			//     || fil_space_is_being_truncated(id)
			//     || fil_space_get_flags(id) == ULINT_UNDEFINED
			//     || type == FIL_TYPE_TEMPORARY
			//     || type == FIL_TYPE_IMPORT
			//     || fil_space_is_redo_skipped(id));
		}
#endif /* UNIV_DEBUG */
		return;
	case MTR_LOG_ALL:
		/* We must not write redo log for the shared temporary tablespace. */
		//ut_ad(id != srv_tmp_space.space_id());
		/* If we write redo log, the tablespace must exist. */
		//ut_ad(fil_space_get_type(id) == FIL_TYPE_TABLESPACE);
		//ut_ad(mtr->is_named_space(id));
		return;
	}

	ut_ad(0);
}


static void fsp_init_file_page_low(buf_block_t* block)
{
    page_t* page = buf_block_get_frame(block);

    if (!fsp_is_system_temporary(block->page.id.space_id())) {
        memset(page, 0, UNIV_PAGE_SIZE);
    }

    mach_write_to_4(page + FIL_PAGE_OFFSET, block->get_page_no());
    mach_write_to_4(page + FIL_PAGE_SPACE, block->get_space_id());
}

// Initialize a file page.
static void fsp_init_file_page(buf_block_t* block, mtr_t* mtr)
{
    //fsp_init_file_page_low(block);

    //ut_d(fsp_space_modify_check(block->page.id.space(), mtr));
    mlog_write_initial_log_record(buf_block_get_frame(block), MLOG_INIT_FILE_PAGE2, mtr);
}



// Gets a buffer block for an allocated page.
// NOTE:
//    If init_mtr != mtr, the block will only be initialized
//    if it was not previously x-latched.
//    It is assumed that the block has been x-latched only by mtr,
//    and freed in mtr in that case.
// return block, initialized if init_mtr==mtr or rw_lock_x_lock_count(&block->lock) == 1
buf_block_t* fsp_page_create(
    uint32  space_id,      /*!< in: space id of the allocated page */
    uint32  page_no,    /*!< in: page number of the allocated page */
    mtr_t*  mtr,        /*!< in: mini-transaction of the allocation */
    mtr_t*  init_mtr)   /*!< in: mini-transaction for initializing the page */
{
    const page_id_t page_id(space_id, page_no);
    const page_size_t page_size(0);
    buf_block_t *block;

    block = buf_page_create(page_id, page_size, RW_X_LATCH, Page_fetch::NORMAL, init_mtr);

    if (init_mtr == mtr || rw_lock_get_x_lock_count(&block->rw_lock) == 1) {
        /* Initialize the page, unless it was already X-latched in mtr.
           (In this case, we would want to allocate another page that has not been freed in mtr.) */
        ut_ad(init_mtr == mtr || !mtr_memo_contains(mtr, block, MTR_MEMO_PAGE_X_FIX));

        fsp_init_file_page(block, init_mtr);
    }

    return(block);
}





// Gets a pointer to the space header and x-locks its page
fsp_header_t* fsp_get_space_header(
    uint32              space_id,
    const page_size_t&  page_size,
    mtr_t*              mtr)
{
    buf_block_t*    block;
    fsp_header_t*   header;
    const page_id_t page_id(space_id, 0);
    ut_ad(space_id != 0 || !page_size.is_compressed());

    block = buf_page_get(page_id, page_size, RW_X_LATCH, mtr);
    header = FSP_HEADER_OFFSET + buf_block_get_frame(block);
    //buf_block_dbg_add_level(block, SYNC_FSP_PAGE);

    ut_ad(space_id == mach_read_from_4(FSP_SPACE_ID + header));
#ifdef UNIV_DEBUG
    const uint32 flags = mach_read_from_4(FSP_SPACE_FLAGS + header);
    //ut_ad(page_size_t(flags).equals_to(page_size));
#endif /* UNIV_DEBUG */
    return(header);
}









// Looks for a used segment inode on a segment inode page.
// return segment inode index, or ULINT_UNDEFINED if not found
static uint32 fsp_seg_inode_page_find_used(
	page_t*	page,	/*!< in: segment inode page */
	uint32	zip_size,/*!< in: compressed page size, or 0 */
	mtr_t*	mtr)	/*!< in/out: mini-transaction */
{
	uint32		i;
	fseg_inode_t*	inode;

	for (i = 0; i < FSP_SEG_INODES_PER_PAGE; i++) {
		inode = fsp_seg_inode_page_get_nth_inode(page, i, zip_size, mtr);
		if (mach_read_from_8(inode + FSEG_ID)) {
			/* This is used */
			ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);
			return(i);
		}
	}

	return(UINT32_UNDEFINED);
}

// Frees a file segment inode
static void fsp_free_seg_inode(
	uint32 space_id, /*!< in: space id */
	uint32		zip_size,/*!< in: compressed page size in bytes
				or 0 for uncompressed pages */
	fseg_inode_t* inode,	/*!< in: segment inode */
	mtr_t* mtr)	/*!< in/out: mini-transaction */
{
    page_t* page;
    fsp_header_t* space_header;
    const page_size_t page_size(0);

    page = page_align(inode);
    space_header = fsp_get_space_header(space_id, page_size, mtr);

    ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

    if (UINT32_UNDEFINED == fsp_seg_inode_page_find_free(page, 0, zip_size, mtr)) {
        // Move the page to another list
        flst_remove(space_header + FSP_SEG_INODES_FULL, page + FSEG_INODE_PAGE_NODE, mtr);
        flst_add_last(space_header + FSP_SEG_INODES_FREE, page + FSEG_INODE_PAGE_NODE, mtr);
        LOGGER_TRACE(LOGGER,
            "fsp_free_seg_inode: page_id (space id %u, page no %u) move inode %p from fsp SEG_INODES_FULL list %p "
            "to fsp SEG_INODES_FREE list %p",
            space_id, page_get_page_no(page), page + FSEG_INODE_PAGE_NODE,
            space_header + FSP_SEG_INODES_FULL, space_header + FSP_SEG_INODES_FREE);
    }

    mlog_write_uint64(inode + FSEG_ID, 0, mtr);
    mlog_write_uint32(inode + FSEG_MAGIC_N, 0xfa051ce3, MLOG_4BYTES, mtr);

    if (UINT32_UNDEFINED == fsp_seg_inode_page_find_used(page, zip_size, mtr)) {
        // There are no other used headers left on the page: free it
        flst_remove(space_header + FSP_SEG_INODES_FREE, page + FSEG_INODE_PAGE_NODE, mtr);
        LOGGER_TRACE(LOGGER,
            "fsp_free_seg_inode: page_id (space id %u, page no %u) remove inode %p from fsp SEG_INODES_FREE list %p",
            space_id, page_get_page_no(page), page + FSEG_INODE_PAGE_NODE, space_header + FSP_SEG_INODES_FREE);

        page_id_t page_id(space_id, page_get_page_no(page));
        const page_size_t page_size(0);
        fsp_free_page(page_id, page_size, mtr);
    }
}

// Looks for an unused segment inode on a segment inode page.
// return segment inode index, or ULINT_UNDEFINED if not found
static uint32 fsp_seg_inode_page_find_free(
	page_t*	page,	/*!< in: segment inode page */
	uint32	i,	/*!< in: search forward starting from this index */
	uint32	zip_size,/*!< in: compressed page size, or 0 */
	mtr_t*	mtr)	/*!< in/out: mini-transaction */
{
	for (; i < FSP_SEG_INODES_PER_PAGE; i++) {
		fseg_inode_t *inode;
		inode = fsp_seg_inode_page_get_nth_inode(page, i, zip_size, mtr);

		if (!mach_read_from_8(inode + FSEG_ID)) {
			/* This is unused */
			return(i);
		}

		ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);
	}

	return(UINT32_UNDEFINED);
}


// Returns the nth inode slot on an inode page.
// return segment inode
fseg_inode_t* fsp_seg_inode_page_get_nth_inode(
	page_t*	page,	/*!< in: segment inode page */
	uint32	i,	/*!< in: inode index on page */
	uint32	zip_size,
			/*!< in: compressed page size, or 0 */
	mtr_t*	mtr)
{
    ut_ad(i < FSP_SEG_INODES_PER_PAGE);
    ut_ad(mtr_memo_contains_page(mtr, page, MTR_MEMO_PAGE_X_FIX));

    return(page + FSEG_ARR_OFFSET + FSEG_INODE_SIZE * i);
}

// Allocates a new file segment inode page
// return TRUE if could be allocated
static buf_block_t* fsp_alloc_seg_inode_page(
    fsp_header_t *space_header, /*!< in: space header */
    mtr_t        *mtr)          /*!< in/out: mini-transaction */
{
    fseg_inode_t*   inode;
    buf_block_t*    block;
    page_t*         page;
    uint32          space;

    ut_ad(page_offset(space_header) == FSP_HEADER_OFFSET);

    space = page_get_space_id(page_align(space_header));
    const page_size_t page_size(0);
    block = fsp_alloc_free_page(space, page_size, mtr);
    if (block == NULL) {
        return NULL;
    }

    //buf_block_dbg_add_level(block, SYNC_FSP_PAGE);
    ut_ad(rw_lock_get_x_lock_count(&block->rw_lock) == 1);
    //block->check_index_page_at_flush = FALSE;

    page = buf_block_get_frame(block);

    mlog_write_uint32(page + FIL_PAGE_TYPE, FIL_PAGE_INODE, MLOG_2BYTES, mtr);

    for (uint32 i = 0; i < FSP_SEG_INODES_PER_PAGE; i++) {
        inode = fsp_seg_inode_page_get_nth_inode(page, i, page_size.physical(), mtr);
        mlog_write_uint64(inode + FSEG_ID, 0, mtr);
    }

    flst_add_last(space_header + FSP_SEG_INODES_FREE, page + FSEG_INODE_PAGE_NODE, mtr);

    LOGGER_TRACE(LOGGER,
        "fsp_alloc_seg_inode_page: page_id (space id %u, page no %u) add inode %p to fsp SEG_INODES_FREE list %p",
        block->page.id.space_id(), block->page.id.page_no(), page + FSEG_INODE_PAGE_NODE,
        space_header + FSP_SEG_INODES_FREE);

    return block;
}

// Allocates a new file segment inode.
// return segment inode, or NULL if not enough space
static fseg_inode_t* fsp_alloc_seg_inode(
    fsp_header_t *space_header, /*!< in: space header */
    mtr_t        *mtr)          /*!< in/out: mini-transaction */
{
    uint32          page_no;
    buf_block_t*    block;
    page_t*         page;
    fseg_inode_t*   inode;
    bool32          success;
    uint32          n;

    ut_ad(page_offset(space_header) == FSP_HEADER_OFFSET);

    if (flst_get_len(space_header + FSP_SEG_INODES_FREE) == 0) {
        /* Allocate a new segment inode page */
        block = fsp_alloc_seg_inode_page(space_header, mtr);
        if (block == NULL) {
            return NULL;
        }
    }

    page_no = flst_get_first(space_header + FSP_SEG_INODES_FREE, mtr).page;
    const page_id_t page_id(page_get_space_id(page_align(space_header)), page_no);
    const page_size_t page_size(0);
    block = buf_page_get(page_id, page_size, RW_X_LATCH, mtr);
    //buf_block_dbg_add_level(block, SYNC_FSP_PAGE);
    page = buf_block_get_frame(block);

    n = fsp_seg_inode_page_find_free(page, 0, page_size.physical(), mtr);
    ut_a(n != UINT32_UNDEFINED);

    inode = fsp_seg_inode_page_get_nth_inode(page, n, page_size.physical(), mtr);
    if (UINT32_UNDEFINED == fsp_seg_inode_page_find_free(page, n + 1, page_size.physical(), mtr)) {
        // There are no other unused headers left on the page: move it to another list
        flst_remove(space_header + FSP_SEG_INODES_FREE, page + FSEG_INODE_PAGE_NODE, mtr);
        flst_add_last(space_header + FSP_SEG_INODES_FULL, page + FSEG_INODE_PAGE_NODE, mtr);
        LOGGER_TRACE(LOGGER,
            "fsp_alloc_seg_inode: page_id (space id %u, page no %u) move inode %p from fsp SEG_INODES_FREE list %p to "
            "fsp SEG_INODES_FULL list %p",
            page_id.space_id(), page_id.page_no(), page + FSEG_INODE_PAGE_NODE,
            space_header + FSP_SEG_INODES_FREE, space_header + FSP_SEG_INODES_FULL);
    }

    ut_ad(!mach_read_from_8(inode + FSEG_ID) || mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

    return inode;
}

// Calculates reserved fragment page slots.
// return number of fragment pages
static uint32 fseg_get_n_frag_pages(fseg_inode_t *inode, mtr_t *mtr)
{
    uint32 i;
    uint32 count = 0;

    ut_ad(inode && mtr);

    for (i = 0; i < FSEG_FRAG_ARR_N_SLOTS; i++) {
        if (FIL_NULL != fseg_get_nth_frag_page_no(inode, i, mtr)) {
            count++;
        }
    }

    return(count);
}

// Calculates the number of pages reserved by a segment,
// and how many pages are currently used.
// return number of reserved pages.
inline uint32 fseg_get_reserved_pages(
    fseg_inode_t* inode, /*!< in: segment inode */
    uint32* used,  /*!< out: number of pages used (not more than reserved) */
    mtr_t* mtr)   /*!< in/out: mini-transaction */
{
    uint32 ret;

    ut_ad(mtr_memo_contains_page(mtr, inode, MTR_MEMO_PAGE_X_FIX));

    if (used) {
        *used = mtr_read_uint32(inode + FSEG_NOT_FULL_N_USED, MLOG_4BYTES, mtr)
            + FSP_EXTENT_SIZE * flst_get_len(inode + FSEG_FULL)
            + fseg_get_n_frag_pages(inode, mtr);
    }

    ret = fseg_get_n_frag_pages(inode, mtr)
        + FSP_EXTENT_SIZE * flst_get_len(inode + FSEG_FREE)
        + FSP_EXTENT_SIZE * flst_get_len(inode + FSEG_NOT_FULL)
        + FSP_EXTENT_SIZE * flst_get_len(inode + FSEG_FULL);

    return ret;
}

// Sets the page number in the nth fragment page slot
static void fseg_set_nth_frag_page_no(
    fseg_inode_t* inode,  /*!< in: segment inode */
    uint32        n,      /*!< in: slot index */
    uint32        page_no,/*!< in: page number to set */
    mtr_t*        mtr)    /*!< in/out: mini-transaction */
{
    ut_ad(inode && mtr);
    ut_ad(n < FSEG_FRAG_ARR_N_SLOTS);
    ut_ad(mtr_memo_contains_page(mtr, inode, MTR_MEMO_PAGE_X_FIX));
    ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

    mlog_write_uint32(inode + FSEG_FRAG_ARR + n * FSEG_FRAG_SLOT_SIZE, page_no, MLOG_4BYTES, mtr);
}

// Gets the page number from the nth fragment page slot.
// return page number, FIL_NULL if not in use
static uint32 fseg_get_nth_frag_page_no(
    fseg_inode_t *inode, /*!< in: segment inode */
    uint32        n,     /*!< in: slot index */
    mtr_t        *mtr)
{
    ut_ad(inode && mtr);
    ut_ad(n < FSEG_FRAG_ARR_N_SLOTS);
    ut_ad(mtr_memo_contains_page(mtr, inode, MTR_MEMO_PAGE_X_FIX));
    ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);
    return(mach_read_from_4(inode + FSEG_FRAG_ARR + n * FSEG_FRAG_SLOT_SIZE));
}

// Marks a page used.
// The page must reside within the extents of the given segment
static void fseg_mark_page_used(
    uint32          space_id,
    fseg_inode_t*   seg_inode,  /*!< in: segment inode */
    uint32          page_no,    /*!< in: page offset */
    xdes_t*         descr,      /*!< in: extent descriptor */
    mtr_t*          mtr)        /*!< in/out: mini-transaction */
{
    uint32 not_full_n_used;

    ut_ad(!((page_offset(seg_inode) - FSEG_ARR_OFFSET) % FSEG_INODE_SIZE));
    ut_ad(mach_read_from_4(seg_inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

    ut_ad(mtr_read_uint32(seg_inode + FSEG_ID, MLOG_4BYTES, mtr)
            == mtr_read_uint32(descr + XDES_ID, MLOG_4BYTES, mtr));

    if (xdes_is_free(descr, mtr)) {
        /* We move the extent from the free list to the NOT_FULL list */
        flst_remove(seg_inode + FSEG_FREE, descr + XDES_FLST_NODE, mtr);
        flst_add_last(seg_inode + FSEG_NOT_FULL, descr + XDES_FLST_NODE, mtr);
        LOGGER_TRACE(LOGGER,
            "fseg_mark_page_used: page_id (space id %u, page no %u) move xdes %p from fseg FREE list "
            "to fseg NOT_FULL list %p",
            space_id, page_no, descr, seg_inode + FSEG_FREE, seg_inode + FSEG_NOT_FULL);
    }

    ut_ad(xdes_mtr_get_bit(descr, XDES_FREE_BIT, page_no % FSP_EXTENT_SIZE, mtr));

    // We mark the page as used
    xdes_set_bit(descr, XDES_FREE_BIT, page_no % FSP_EXTENT_SIZE, FALSE, mtr);

    not_full_n_used = mtr_read_uint32(seg_inode + FSEG_NOT_FULL_N_USED, MLOG_4BYTES, mtr);
    not_full_n_used++;
    mlog_write_uint32(seg_inode + FSEG_NOT_FULL_N_USED, not_full_n_used, MLOG_4BYTES, mtr);
    if (xdes_is_full(descr, mtr)) {
        // We move the extent from the NOT_FULL list to the FULL list
        flst_remove(seg_inode + FSEG_NOT_FULL, descr + XDES_FLST_NODE, mtr);
        flst_add_last(seg_inode + FSEG_FULL, descr + XDES_FLST_NODE, mtr);
        mlog_write_uint32(seg_inode + FSEG_NOT_FULL_N_USED, not_full_n_used - FSP_EXTENT_SIZE, MLOG_4BYTES, mtr);
        LOGGER_TRACE(LOGGER,
            "fseg_mark_page_used: page_id (space id %u, page no %u) move xdes %p from fseg NOT_FULL list to "
            "fseg FULL list %p, reset fseg_not_null_n_used %u",
            space_id, page_no, descr, seg_inode + FSEG_NOT_FULL,
            seg_inode + FSEG_FULL, not_full_n_used - FSP_EXTENT_SIZE);
    }
}

// Finds a fragment page slot which is free
// return slot index; ULINT_UNDEFINED if none found
static uint32 fseg_find_free_frag_page_slot(
    fseg_inode_t*   inode,  /*!< in: segment inode */
    mtr_t*          mtr)    /*!< in/out: mini-transaction */
{
    uint32  i;
    uint32  page_no;

    for (i = 0; i < FSEG_FRAG_ARR_N_SLOTS; i++) {
        page_no = fseg_get_nth_frag_page_no(inode, i, mtr);
        if (page_no == FIL_NULL) {
            return(i);
        }
    }

    return UINT32_UNDEFINED;
}

// Frees a single page of a segment
static void fseg_free_page(uint32 space_id, fseg_inode_t* inode, uint32 page_no, mtr_t* mtr)
{
    xdes_t* descr;
    uint32  not_full_n_used;
    uint32  state;
    uint64  descr_id;
    uint64  seg_id;
    uint32  i;
    const page_size_t page_size(0);
    page_id_t page_id(space_id, page_no);

    ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);
    ut_ad(!((page_offset(inode) - FSEG_ARR_OFFSET) % FSEG_INODE_SIZE));

    descr = xdes_get_descriptor(space_id, page_no, mtr);
    if (xdes_mtr_get_bit(descr, XDES_FREE_BIT, page_no % FSP_EXTENT_SIZE, mtr)) {
        LOGGER_FATAL(LOGGER,
            "Serious error! Trying to free page_id (space id %lu, page no %lu) "
            "though it is already marked as free in the tablespace!\n"
            "The tablespace free space info is corrupt.\n", space_id, page_no);
        ut_error;
    }

    state = xdes_get_state(descr, mtr);
    if (state != XDES_FSEG) {
        // The page is in the fragment pages of the segment
        for (i = 0;; i++) {
            if (fseg_get_nth_frag_page_no(inode, i, mtr) == page_no) {
                fseg_set_nth_frag_page_no(inode, i, FIL_NULL, mtr);
                break;
            }
        }
        fsp_free_page(page_id, page_size, mtr);
        return;
    }

    // If we get here, the page is in some extent of the segment

    descr_id = mach_read_from_8(descr + XDES_ID);
    seg_id = mach_read_from_8(inode + FSEG_ID);

    if (UNLIKELY(descr_id != seg_id)) {
        LOGGER_FATAL(LOGGER,
            "Serious error! Trying to free page_id (space id %lu, page no %lu),\n"
            "which does not belong to segment %llu but belongs to segment %llu.\n",
            space_id, page_no, descr_id, seg_id);
        ut_error;
    }

    not_full_n_used = mtr_read_uint32(inode + FSEG_NOT_FULL_N_USED, MLOG_4BYTES, mtr);
    if (xdes_is_full(descr, mtr)) {
        // The fragment is full: move it to another list
        flst_remove(inode + FSEG_FULL, descr + XDES_FLST_NODE, mtr);
        flst_add_last(inode + FSEG_NOT_FULL, descr + XDES_FLST_NODE, mtr);
        mlog_write_uint32(inode + FSEG_NOT_FULL_N_USED, not_full_n_used + FSP_EXTENT_SIZE - 1, MLOG_4BYTES, mtr);
        LOGGER_TRACE(LOGGER,
            "fseg_free_page: page_id (space id %u, page no %u) move xdes %p from fseg FULL list to "
            "fseg NOT_FULL list %p, reset not_full_n_used %u",
            space_id, page_no, descr, inode + FSEG_FULL, inode + FSEG_NOT_FULL, not_full_n_used + FSP_EXTENT_SIZE - 1);
    } else {
        ut_a(not_full_n_used > 0);
        mlog_write_uint32(inode + FSEG_NOT_FULL_N_USED, not_full_n_used - 1, MLOG_4BYTES, mtr);
        LOGGER_TRACE(LOGGER,
            "fseg_free_page: page_id (space id %u, page no %u) xdes %p fseg NOT_FULL list %p, reset not_full_n_used %u",
            space_id, page_no, descr, inode + FSEG_NOT_FULL, not_full_n_used - 1);
    }

    xdes_set_bit(descr, XDES_FREE_BIT, page_no % FSP_EXTENT_SIZE, TRUE, mtr);
    xdes_set_bit(descr, XDES_CLEAN_BIT, page_no % FSP_EXTENT_SIZE, TRUE, mtr);

    if (xdes_is_free(descr, mtr)) {
        /* The extent has become free: free it to space */
        flst_remove(inode + FSEG_NOT_FULL, descr + XDES_FLST_NODE, mtr);
        fsp_free_extent(space_id, descr, mtr);
        LOGGER_TRACE(LOGGER,
            "fseg_free_page: page_id (space id %u, page no %u) remove xdes %p from fseg NOT_FULL list %p",
            space_id, page_no, descr, inode + FSEG_NOT_FULL);
    }
}

// Allocates a single free page from a segment.
buf_block_t* fseg_alloc_free_page(
    uint32          space_id,  /*!< in: space */
    fseg_inode_t*   inode, /*!< in/out: segment inode */
    mtr_t*          mtr,        /*!< in/out: mini-transaction */
    mtr_t*          init_mtr)   /*!< in/out: mtr or another mini-transaction
                        in which the page should be initialized.
                        If init_mtr!=mtr, but the page is already
                        latched in mtr, do not initialize the page. */
{
    fsp_header_t*   space_header;
    fil_addr_t      first;
    uint64          seg_id;
    uint32          ret_page_no;// the allocated page offset, FIL_NULL if could not be allocated
    xdes_t*         ret_descr;  // the extent of the allocated page
    const page_size_t page_size(0);

    //ut_ad((direction >= FSP_UP) && (direction <= FSP_NO_DIR));
    ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);
    ut_ad(!((page_offset(inode) - FSEG_ARR_OFFSET) % FSEG_INODE_SIZE));

    seg_id = mach_read_from_8(inode + FSEG_ID);
    ut_ad(seg_id);

    if (flst_get_len(inode + FSEG_NOT_FULL) > 0) {
        first = flst_get_first(inode + FSEG_NOT_FULL, mtr);
    } else if (flst_get_len(inode + FSEG_FREE) > 0) {
        first = flst_get_first(inode + FSEG_FREE, mtr);
    } else if (flst_get_len(inode + FSEG_FULL) > 0 ||
        fseg_get_n_frag_pages(inode, mtr) >= FSEG_FRAG_LIMIT) {
        // We allocate the free extent from space
        ret_descr = fsp_alloc_free_extent(space_id, page_size, mtr);
        if (ret_descr == NULL) {
            // No free space left
            return NULL;
        }

        xdes_set_state(ret_descr, XDES_FSEG, mtr);
        mlog_write_uint64(ret_descr + XDES_ID, seg_id, mtr);
        flst_add_last(inode + FSEG_FREE, ret_descr + XDES_FLST_NODE, mtr);

        LOGGER_TRACE(LOGGER,
            "fseg_alloc_free_page: space id %u add xdes %p to fseg (segid %llu) FREE list %p",
            space_id, ret_descr, seg_id, inode + FSEG_FREE);

        first = flst_get_first(inode + FSEG_FREE, mtr);
    } else {
        // We allocate an individual page from the space
        buf_block_t* block = fsp_alloc_free_page(space_id, page_size, mtr);
        if (block != NULL) {
            // Put the page in the fragment page array of the segment
            uint32 n = fseg_find_free_frag_page_slot(inode, mtr);
            ut_a(n != UINT32_UNDEFINED);
            fseg_set_nth_frag_page_no(inode, n, block->get_page_no(), mtr);
        } else {
            // No free space left
            // nothing to do
        }

        // fsp_alloc_free_page() invoked fsp_init_file_page() already.
        return block;
    }

    ret_descr = xdes_lst_get_descriptor(space_id, page_size, first, mtr);
    ut_ad(ret_descr != NULL);
    ret_page_no = xdes_get_offset(ret_descr) + xdes_find_bit(ret_descr, XDES_FREE_BIT, TRUE, 0, mtr);
    ut_ad(ret_page_no != FIL_NULL);

    /* At this point we know the extent and the page offset.
       The extent is still in the appropriate list (FSEG_NOT_FULL or FSEG_FREE),
       and the page is not yet marked as used. */

    ut_ad(xdes_get_descriptor(space_id, ret_page_no, mtr) == ret_descr);
    ut_ad(xdes_mtr_get_bit(ret_descr, XDES_FREE_BIT, ret_page_no % FSP_EXTENT_SIZE, mtr));

    fseg_mark_page_used(space_id, inode, ret_page_no, ret_descr, mtr);

    return fsp_page_create(space_id, ret_page_no, mtr, init_mtr);
}

// Allocates a free extent for the segment
xdes_t* fseg_alloc_free_extent(uint32 space_id, fseg_inode_t* inode, mtr_t* mtr)
{
    xdes_t* descr;
    const page_size_t page_size(0);

    ut_ad(!((page_offset(inode) - FSEG_ARR_OFFSET) % FSEG_INODE_SIZE));
    ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

    if (flst_get_len(inode + FSEG_FREE) > 0) {
        // Segment free list is not empty, allocate from it
        fil_addr_t first = flst_get_first(inode + FSEG_FREE, mtr);
        descr = xdes_lst_get_descriptor(space_id, page_size, first, mtr);
    } else {
        // Segment free list was empty, allocate from space
        descr = fsp_alloc_free_extent(space_id, page_size, mtr);
        if (descr == NULL) {
            // No free space left
            return(NULL);
        }

        uint64 seg_id = mach_read_from_8(inode + FSEG_ID);
        xdes_set_state(descr, XDES_FSEG, mtr);
        mlog_write_uint64(descr + XDES_ID, seg_id, mtr);
        flst_add_last(inode + FSEG_FREE, descr + XDES_FLST_NODE, mtr);

        LOGGER_TRACE(LOGGER,
            "fseg_alloc_free_extent: space id %u add xdes %p to fseg (segid %llu) FREE list %p",
            space_id, descr, seg_id, inode + FSEG_FREE);
    }

    return(descr);
}

// Allocates free extents from table space
xdes_t* fseg_reserve_free_extents(
    uint32 space_id,
    fseg_inode_t* inode,
    uint32 count, // number of extents to reserve
    mtr_t* mtr)
{
    xdes_t* descr = NULL;
    const page_size_t page_size(0);

    ut_ad(!((page_offset(inode) - FSEG_ARR_OFFSET) % FSEG_INODE_SIZE));
    ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

    for (uint32 i = 0; i < count; i++) {
        // allocate from space
        descr = fsp_alloc_free_extent(space_id, page_size, mtr);
        if (descr == NULL) {
            /* No free space left */
            return NULL;
        }

        uint64 seg_id = mach_read_from_8(inode + FSEG_ID);
        xdes_set_state(descr, XDES_FSEG, mtr);
        mlog_write_uint64(descr + XDES_ID, seg_id, mtr);
        flst_add_last(inode + FSEG_FREE, descr + XDES_FLST_NODE, mtr);

        LOGGER_TRACE(LOGGER,
            "fseg_reserve_free_extents: space id %u add xdes %p to fseg (segid %llu) FREE list %p",
            space_id, descr, seg_id, inode + FSEG_FREE);
    }

    return descr;
}


void fseg_init(fseg_inode_t* inode, uint64 seg_id, mtr_t* mtr)
{
    mlog_write_uint64(inode + FSEG_ID, seg_id, mtr);
    mlog_write_uint32(inode + FSEG_NOT_FULL_N_USED, 0, MLOG_4BYTES, mtr);

    flst_init(inode + FSEG_FREE, mtr);
    flst_init(inode + FSEG_NOT_FULL, mtr);
    flst_init(inode + FSEG_FULL, mtr);

    mlog_write_uint32(inode + FSEG_MAGIC_N, FSEG_MAGIC_N_VALUE, MLOG_4BYTES, mtr);
    for (uint32 i = 0; i < FSEG_FRAG_ARR_N_SLOTS; i++) {
        fseg_set_nth_frag_page_no(inode, i, FIL_NULL, mtr);
    }
}



// Put new extents to the free list
static void fsp_fill_extent_free_list(fil_space_t* space, fsp_header_t* header, mtr_t* mtr)
{
    ut_ad(page_offset(header) == FSP_HEADER_OFFSET);
    ut_d(fsp_space_modify_check(space->id, mtr));

    /* Check if we can fill free list from above the free list limit */
    uint32 size = mach_read_from_4(header + FSP_SIZE);
    uint32 limit = mach_read_from_4(header + FSP_FREE_LIMIT);
    uint32 flags = mach_read_from_4(header + FSP_SPACE_FLAGS);

    ut_ad(size == space->size_in_header);
    ut_ad(limit == space->free_limit);
    ut_ad(flags == space->flags);

    const page_size_t page_size(flags);

    uint32 i = limit;
    while (i + FSP_EXTENT_SIZE <= size) {

        bool32 init_xdes = (ut_2pow_remainder(i, page_size.physical()) == 0);
        space->free_limit = i + FSP_EXTENT_SIZE;
        mlog_write_uint32(header + FSP_FREE_LIMIT, i + FSP_EXTENT_SIZE, MLOG_4BYTES, mtr);

        if (init_xdes) {
            buf_block_t* block;
            /* We are going to initialize a new descriptor page */
            if (i > 0) {
                const page_id_t page_id(space->id, i);
                block = buf_page_create(page_id, page_size, RW_X_LATCH, Page_fetch::NORMAL, mtr);
                fsp_init_file_page(block, mtr);
                mlog_write_uint32(buf_block_get_frame(block) + FIL_PAGE_TYPE, FIL_PAGE_XDES, MLOG_2BYTES, mtr);
            }
        }

        xdes_t* descr = xdes_get_descriptor_with_space_hdr(header, space->id, i, mtr);
        xdes_init(descr, mtr);

        if (UNLIKELY(init_xdes)) {
            xdes_set_bit(descr, XDES_FREE_BIT, 0, FALSE, mtr);
            xdes_set_state(descr, XDES_FREE_FRAG, mtr);
            flst_add_last(header + FSP_FREE_FRAG, descr + XDES_FLST_NODE, mtr);
            uint32 frag_n_used = mach_read_from_4(header + FSP_FRAG_N_USED);
            mlog_write_uint32(header + FSP_FRAG_N_USED, frag_n_used + 1, MLOG_4BYTES, mtr);
            LOGGER_TRACE(LOGGER,
                "fsp_fill_extent_free_list: space id %u add xdes %p to FREE_FRAG list %p, reset frag_n_used %u",
                space->id, descr, header + FSP_FREE_FRAG, frag_n_used + 1);
        } else {
            flst_add_last(header + FSP_FREE, descr + XDES_FLST_NODE, mtr);
            LOGGER_TRACE(LOGGER,
                "fsp_fill_extent_free_list: space id %u add xdes %p to FREE list %p",
                space->id, descr, header + FSP_FREE);
        }

        i += FSP_EXTENT_SIZE;
    }
}

static bool32 fsp_extend_space(fil_space_t *space, fsp_header_t* header, mtr_t* mtr)
{
    uint32 size = mtr_read_uint32(header + FSP_SIZE, MLOG_4BYTES, mtr);
    uint32 hwm_page_no = size + fsp_get_autoextend_increment(space);
    uint32 actual_size = 0;

    if (!fil_space_extend(space, hwm_page_no, &actual_size)) {
        return FALSE;
    }

    space->size_in_header += actual_size;
    mlog_write_uint32(header + FSP_SIZE, space->size_in_header, MLOG_4BYTES, mtr);

    fsp_fill_extent_free_list(space, header, mtr);

    return TRUE;
}

static bool32 fsp_try_extend_space(uint32 space_id, mtr_t* mtr)
{
    fil_space_t *space;
    bool32       ret = TRUE;
    uint32       hwm_page_no;

    space = fil_system_get_space_by_id(space_id);
    if (space == NULL) {
        return FALSE;
    }

retry:

    mutex_enter(&space->mutex);
    if (space->io_in_progress) {
        mutex_exit(&space->mutex);
        os_thread_sleep(1000);
        goto retry;
    }
    space->io_in_progress = TRUE;
    mutex_exit(&space->mutex);

    const page_size_t page_size(0);
    fsp_header_t* header = fsp_get_space_header(space_id, page_size, mtr);
    if (flst_get_len(header + FSP_FREE) <= 0) {
        ret = fsp_extend_space(space, header, mtr);
    }

    mutex_enter(&space->mutex);
    space->io_in_progress = FALSE;
    mutex_exit(&space->mutex);

    fil_system_unpin_space(space);

    return ret;
}

// Returns an extent to the free list of a space
void fsp_free_extent(uint32 space_id, xdes_t* xdes, mtr_t* mtr)
{
    const page_size_t page_size(0);
    fsp_header_t* header;

    header = fsp_get_space_header(space_id, page_size, mtr);
    ut_a(xdes_get_state(xdes, mtr) != XDES_FREE);
    xdes_init(xdes, mtr);
    flst_add_last(header + FSP_FREE, xdes + XDES_FLST_NODE, mtr);

    LOGGER_TRACE(LOGGER,
        "fsp_free_extent: space id %u add xdes %p to fsp FREE list %p",
        space_id, xdes, header + FSP_FREE);
}

// Allocates a new free extent
// out: extent descriptor, NULL if cannot be allocated
xdes_t* fsp_alloc_free_extent(
    uint32              space_id,
    const page_size_t&  page_size,
    mtr_t*              mtr)
{
    fsp_header_t* header;
    fil_addr_t first;
    xdes_t* descr;

    header = fsp_get_space_header(space_id, page_size, mtr);

    first = flst_get_first(header + FSP_FREE, mtr);
    if (fil_addr_is_null(first)) {
        if (!fsp_try_extend_space(space_id, mtr)) {
            return NULL;
        }
        first = flst_get_first(header + FSP_FREE, mtr);
    }

    descr = xdes_lst_get_descriptor(space_id, page_size, first, mtr);
    flst_remove(header + FSP_FREE, descr + XDES_FLST_NODE, mtr);

    LOGGER_TRACE(LOGGER,
        "fsp_alloc_free_extent: space id %u remove xdes %p from fsp FREE list %p",
        space_id, descr, header + FSP_FREE);

    return descr;
}

// Frees a single page of a space
// The page is marked as free and clean
void fsp_free_page(
    const page_id_t& page_id,
    const page_size_t& page_size,
    mtr_t* mtr)
{
    fsp_header_t* header;
    xdes_t* descr;
    uint32 state;
    uint32 frag_n_used;

    ut_ad(mtr);

    /* printf("Freeing page %lu in space %lu\n", page, space); */

    header = fsp_get_space_header(page_id.space_id(), page_size, mtr);
    descr = xdes_get_descriptor_with_space_hdr(header, page_id.space_id(), page_id.page_no(), mtr);
    state = xdes_get_state(descr, mtr);

    ut_a((state == XDES_FREE_FRAG) || (state == XDES_FULL_FRAG));
    ut_a(xdes_get_bit(descr, XDES_FREE_BIT, page_id.page_no() % FSP_EXTENT_SIZE) == FALSE);

    xdes_set_bit(descr, XDES_FREE_BIT, page_id.page_no() % FSP_EXTENT_SIZE, TRUE, mtr);
    xdes_set_bit(descr, XDES_CLEAN_BIT, page_id.page_no() % FSP_EXTENT_SIZE, TRUE, mtr);

    frag_n_used = mach_read_from_4(header + FSP_FRAG_N_USED);
    if (state == XDES_FULL_FRAG) {
        /* The fragment was full: move it to another list */
        flst_remove(header + FSP_FULL_FRAG, descr + XDES_FLST_NODE, mtr);
        xdes_set_state(descr, XDES_FREE_FRAG, mtr);
        flst_add_last(header + FSP_FREE_FRAG, descr + XDES_FLST_NODE, mtr);
        mlog_write_uint32(header + FSP_FRAG_N_USED, frag_n_used + FSP_EXTENT_SIZE - 1, MLOG_4BYTES, mtr);
        LOGGER_TRACE(LOGGER,
            "fsp_free_page: page_id (space id %u, page no %u) move xdes %p from fsp FULL_FRAG list %p "
            "to fsp FREE_FRAG list %p, reset frag_n_used %u",
            page_id.space_id(), page_id.page_no(), descr, header + FSP_FULL_FRAG,
            header + FSP_FREE_FRAG, frag_n_used + FSP_EXTENT_SIZE - 1);
    } else {
        ut_a(frag_n_used > 0);
        mlog_write_uint32(header + FSP_FRAG_N_USED, frag_n_used - 1, MLOG_4BYTES, mtr);
        LOGGER_TRACE(LOGGER,
            "fsp_free_page: page_id (space id %u, page no %u) xdes %p, fsp FREE_FRAG list %p reset frag_n_used %u",
            page_id.space_id(), page_id.page_no(), descr, header + FSP_FREE_FRAG, frag_n_used - 1);
    }

    if (UNLIKELY(xdes_is_free(descr, mtr))) {
        // The extent has become free: move it to another list
        flst_remove(header + FSP_FREE_FRAG, descr + XDES_FLST_NODE, mtr);
        fsp_free_extent(page_id.space_id(), descr, mtr);
        LOGGER_TRACE(LOGGER,
            "fsp_free_page: page_id (space id %u, page no %u) move xdes %p from fsp FREE_FRAG list %p to fsp FREE list",
            page_id.space_id(), page_id.page_no(), descr, header + FSP_FREE_FRAG);
    }
}

// Allocates a single free page from a space
// The page is marked as used
// out: the page offset, FIL_NULL if no page could be allocated
buf_block_t* fsp_alloc_free_page(
    uint32              space_id,
    const page_size_t&  page_size,
    mtr_t*              mtr)
{
    fsp_header_t* header;
    fil_addr_t first;
    xdes_t* descr;
    page_t* page;
    uint32 free;
    uint32 frag_n_used;
    uint32 page_no;

    ut_ad(mtr);

    header = fsp_get_space_header(space_id, page_size, mtr);

    /* take the first extent in free_frag list */
    first = flst_get_first(header + FSP_FREE_FRAG, mtr);
    if (UNLIKELY(fil_addr_is_null(first))) {
        first = flst_get_first(header + FSP_FREE, mtr);
        if (fil_addr_is_null(first)) {
            if (!fsp_try_extend_space(space_id, mtr)) {
                return NULL;
            }
            first = flst_get_first(header + FSP_FREE, mtr);
        }
        ut_ad(!fil_addr_is_null(first));

        descr = xdes_lst_get_descriptor(space_id, page_size, first, mtr);
        ut_ad(xdes_is_free(descr, mtr));
        xdes_set_state(descr, XDES_FREE_FRAG, mtr);
        flst_remove(header + FSP_FREE, descr + XDES_FLST_NODE, mtr);
        flst_add_last(header + FSP_FREE_FRAG, descr + XDES_FLST_NODE, mtr);
        LOGGER_TRACE(LOGGER,
            "fsp_alloc_free_page: space id %u move xdes %p from fsp FREE list %p to fsp FREE_FRAG list %p",
            space_id, descr, header + FSP_FREE, header + FSP_FREE_FRAG);
    } else {
        descr = xdes_lst_get_descriptor(space_id, page_size, first, mtr);
    }

    // Now we have in descr an extent with at least one free page. Look for a free page in the extent
    free = xdes_find_bit(descr, XDES_FREE_BIT, TRUE, 0, mtr);
    ut_ad(free != UINT32_UNDEFINED);
    xdes_set_bit(descr, XDES_FREE_BIT, free, FALSE, mtr);

    // Update the FRAG_N_USED field
    frag_n_used = mach_read_from_4(header + FSP_FRAG_N_USED);
    frag_n_used++;
    mlog_write_uint32(header + FSP_FRAG_N_USED, frag_n_used, MLOG_4BYTES, mtr);
    if (UNLIKELY(xdes_is_full(descr, mtr))) {
        // The fragment is full: move it to another list
        flst_remove(header + FSP_FREE_FRAG, descr + XDES_FLST_NODE, mtr);
        xdes_set_state(descr, XDES_FULL_FRAG, mtr);
        flst_add_last(header + FSP_FULL_FRAG, descr + XDES_FLST_NODE, mtr);
        mlog_write_uint32(header + FSP_FRAG_N_USED, frag_n_used - FSP_EXTENT_SIZE, MLOG_4BYTES, mtr);
        LOGGER_TRACE(LOGGER,
            "fsp_alloc_free_page: page_id (space id %u, page no %u) move xdes %p from fsp FREE_FRAG list %p "
            "to fsp FULL_FRAG list %p, reset frag_n_used %u",
            space_id, xdes_get_offset(descr) + free, descr, header + FSP_FREE_FRAG,
            header + FSP_FULL_FRAG, frag_n_used - FSP_EXTENT_SIZE);
    } else {
        LOGGER_TRACE(LOGGER,
            "fsp_alloc_free_page: page_id (space id %u, page no %u) xdes %p fsp FREE_FRAG list %p reset frag_n_used %u",
            space_id, xdes_get_offset(descr) + free, descr, header + FSP_FREE_FRAG, frag_n_used);
    }

    page_no = xdes_get_offset(descr) + free;

    // Initialize the allocated page to the buffer pool,
    // so that it can be obtained immediately with buf_page_get without need for a disk read.
    const page_id_t page_id(space_id, page_no);
    buf_block_t* block = buf_page_create(page_id, page_size, RW_X_LATCH, Page_fetch::NORMAL, mtr);

    /* Prior contents of the page should be ignored */
    fsp_init_file_page(block, mtr);

    return block;
}

// Initializes the space header of a new created space
static fsp_header_t* fsp_header_init(fil_space_t* space, mtr_t* mtr)
{
    const page_id_t page_id(space->id, 0);
    const page_size_t page_size(0);
    buf_block_t* block = buf_page_create(page_id, page_size, RW_X_LATCH, Page_fetch::NORMAL, mtr);
    fsp_init_file_page(block, mtr);

    page_t* page = buf_block_get_frame(block);
    mlog_write_uint32(page + FIL_PAGE_TYPE, FIL_PAGE_FSP_HDR, MLOG_2BYTES, mtr);

    fsp_header_t* header = FSP_HEADER_OFFSET + page;

    space->size_in_header = 0;
    mlog_write_uint32(header + FSP_SIZE, 0, MLOG_4BYTES, mtr);
    mlog_write_uint32(header + FSP_FREE_LIMIT, 0, MLOG_4BYTES, mtr);
    //mlog_write_uint32(header + FSP_LOWEST_NO_WRITE, 0, MLOG_4BYTES, mtr);
    mlog_write_uint32(header + FSP_FRAG_N_USED, 0, MLOG_4BYTES, mtr);

    flst_init(header + FSP_FREE, mtr);
    flst_init(header + FSP_FREE_FRAG, mtr);
    flst_init(header + FSP_FULL_FRAG, mtr);
    flst_init(header + FSP_SEG_INODES_FULL, mtr);
    flst_init(header + FSP_SEG_INODES_FREE, mtr);

    mlog_write_uint64(header + FSP_SEG_ID, 1, mtr);

    return header;
}

static status_t fsp_reserve_system_space(mtr_t* mtr)
{
    buf_block_t* block;
    const page_size_t page_size(0);

    for (uint32 i = 1; i < FSP_DYNAMIC_FIRST_PAGE_NO; i++) {
        if (i == 63 || i == 126) {
            block = NULL;
        }
        block = fsp_alloc_free_page(FIL_SYSTEM_SPACE_ID, page_size, mtr);
        if (block == NULL) {
            return CM_ERROR;
        }
    }

    return CM_SUCCESS;
}

// Initializes the space header of a new created space
// and creates also the insert buffer tree root. */
status_t fsp_init_space(uint32 space_id, uint32 size)
{
    status_t ret = CM_SUCCESS;
    fil_space_t *space;

    LOGGER_INFO(LOGGER, "fsp space initialize");

    space = fil_system_get_space_by_id(space_id);
    if (space == NULL) {
        LOGGER_ERROR(LOGGER, "fsp_init_space: Failed to find space by space_id %lu", space_id);
        return CM_ERROR;
    }

    mtr_t mtr;
    mtr_start(&mtr);

    // fsp header
    fsp_header_t* header = fsp_header_init(space, &mtr);
    if (header == NULL) {
        LOGGER_ERROR(LOGGER, "fsp_init_space: error for init fsp_header");
        ret = CM_ERROR;
        goto err_exit;
    }

    uint32 size_in_header = 0;
    while (size_in_header < size) {
        if (!fsp_extend_space(space, header, &mtr)) {
            LOGGER_ERROR(LOGGER, "fsp_init_space: error for extend space, increase size %u", size);
            ret = CM_ERROR;
            goto err_exit;
        }
        size_in_header = mtr_read_uint32(header + FSP_SIZE, MLOG_4BYTES, &mtr);
    }

    // set used for page0
    xdes_t* descr = xdes_get_descriptor_with_space_hdr(header, FIL_SYSTEM_SPACE_ID, 0, &mtr);
    ut_ad(descr);
    ut_ad(xdes_get_bit(descr, XDES_FREE_BIT, 0) == FALSE);
    ut_ad(xdes_get_state(descr, &mtr) == XDES_FREE_FRAG);

    //xdes_set_bit(descr, XDES_FREE_BIT, 0, FALSE, &mtr);
    //xdes_set_state(descr, XDES_FREE_FRAG, &mtr);
    //flst_remove(header + FSP_FREE, descr + XDES_FLST_NODE, &mtr);
    //flst_add_last(header + FSP_FREE_FRAG, descr + XDES_FLST_NODE, &mtr);
    //mlog_write_uint32(header + FSP_FRAG_N_USED, 1, MLOG_4BYTES, &mtr);

    // page no in [1, FSP_DYNAMIC_FIRST_PAGE_NO)
    if (fsp_reserve_system_space(&mtr) != CM_SUCCESS) {
        ret = CM_ERROR;
        goto err_exit;
    }

    // inode
    //buf_block_t* block = fsp_alloc_seg_inode_page(header, &mtr);
    //if (block == NULL) {
    //    LOGGER_ERROR(LOGGER, "fsp_init_space: error for alloc inode page");
    //    ret = CM_ERROR;
    //    goto err_exit;
    //}
    //ut_a(block->get_page_no() == FSP_FIRST_INODE_PAGE_NO);

err_exit:

    mtr_commit(&mtr);

    fil_system_unpin_space(space);

    return ret;
}

status_t fsp_system_space_reserve_pages(uint32 reserved_max_page_no)
{
    mtr_t mtr;

    LOGGER_INFO(LOGGER, "system space extend");

    mtr_start(&mtr);

    const page_size_t page_size(0);
    fsp_header_t* header = fsp_get_space_header(FIL_SYSTEM_SPACE_ID, page_size, &mtr);

    for (uint32 i = 0; i <= reserved_max_page_no; i++) {
        xdes_t* descr = xdes_get_descriptor_with_space_hdr(header, FIL_SYSTEM_SPACE_ID, i, &mtr);
        ut_ad (descr);

        if (xdes_get_bit(descr, XDES_FREE_BIT, i % FSP_EXTENT_SIZE) == FALSE) {
            continue;
        }

        xdes_set_bit(descr, XDES_FREE_BIT, i % FSP_EXTENT_SIZE, FALSE, &mtr);
        if (xdes_get_state(descr, &mtr) == XDES_FREE) {
            flst_remove(header + FSP_FREE, descr + XDES_FLST_NODE, &mtr);
            //
            xdes_set_state(descr, XDES_FREE_FRAG, &mtr);
            flst_add_last(header + FSP_FREE_FRAG, descr + XDES_FLST_NODE, &mtr);
            uint32 frag_n_used = mach_read_from_4(header + FSP_FRAG_N_USED);
            mlog_write_uint32(header + FSP_FRAG_N_USED, frag_n_used + 1, MLOG_4BYTES, &mtr);
        } else {
            uint32 frag_n_used = mach_read_from_4(header + FSP_FRAG_N_USED);
            frag_n_used++;
            mlog_write_uint32(header + FSP_FRAG_N_USED, frag_n_used, MLOG_4BYTES, &mtr);
            if (xdes_is_full(descr, &mtr)) {
                /* The fragment is full: move it to another list */
                flst_remove(header + FSP_FREE_FRAG, descr + XDES_FLST_NODE, &mtr);
                xdes_set_state(descr, XDES_FULL_FRAG, &mtr);
                flst_add_last(header + FSP_FULL_FRAG, descr + XDES_FLST_NODE, &mtr);
                mlog_write_uint32(header + FSP_FRAG_N_USED, frag_n_used - FSP_EXTENT_SIZE, MLOG_4BYTES, &mtr);
            }
        }
    }

    mtr_commit(&mtr);

    return CM_SUCCESS;
}

