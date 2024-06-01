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
static bool32 fil_node_prepare_for_io(fil_node_t *node);
static void fsp_space_modify_check(uint32 id, const mtr_t* mtr);
static void fsp_init_file_page(buf_block_t* block, mtr_t* mtr);
static void fil_node_complete_io(fil_node_t* node, uint32 type);

// Gets the page number from the nth fragment page slot.
// return page number, FIL_NULL if not in use
static uint32 fseg_get_nth_frag_page_no(
    fseg_inode_t *inode,	/*!< in: segment inode */
    uint32        n,	/*!< in: slot index */
    mtr_t        *mtr);

static void fseg_fill_free_list(
    fseg_inode_t*	inode,	/*!< in: segment inode */
    uint32		space,	/*!< in: space id */
    uint32		zip_size,/*!< in: compressed page size in bytes
                         or 0 for uncompressed pages */
    uint32		hint,	/*!< in: hint which extent would be good as
                        the first extent */
    mtr_t*		mtr);	/*!< in/out: mini-transaction */

static uint32 fseg_n_reserved_pages_low(
    fseg_inode_t *inode, /*!< in: segment inode */
    uint32       *used,  /*!< out: number of pages used (not more than reserved) */
    mtr_t        *mtr);   /*!< in/out: mini-transaction */

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
    page_id_t page_id(space, addr.page);

    ut_ad(addr.boffset < UNIV_PAGE_SIZE);
    ut_ad((rw_latch == RW_S_LATCH) || (rw_latch == RW_X_LATCH));
    
    block = buf_page_get(&page_id, page_size, rw_latch, mtr);
    ptr = buf_block_get_frame(block) + addr.boffset;

    if (ptr_block != NULL) {
      *ptr_block = block;
    }

	return(ptr);
}






// Sets a descriptor bit of a page.
void xdes_set_bit(
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
    const xdes_t*	descr,	/*!< in: descriptor */
    uint32		bit,	/*!< in: XDES_FREE_BIT or XDES_CLEAN_BIT */
    uint32		offset,	/*!< in: page offset within extent: 0 ... FSP_EXTENT_SIZE - 1 */
    mtr_t*		mtr)	/*!< in: mini-transaction */
{
    ut_ad(mtr->is_active());
    ut_ad(mtr_memo_contains_page(mtr, descr, MTR_MEMO_PAGE_X_FIX));

    return(xdes_get_bit(descr, bit, offset));
}

/**********************************************************************//**
Looks for a descriptor bit having the desired value. Starts from hint
and scans upward; at the end of the extent the search is wrapped to
the start of the extent.
@return bit index of the bit, ULINT_UNDEFINED if not found */
uint32 xdes_find_bit(
	xdes_t*	descr,	/*!< in: descriptor */
	uint32	bit,	/*!< in: XDES_FREE_BIT or XDES_CLEAN_BIT */
	bool32	val,	/*!< in: desired bit value */
	uint32	hint,	/*!< in: hint of which bit position would be desirable */
	mtr_t*	mtr)	/*!< in/out: mini-transaction */
{
	uint32	i;

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

/** Get pointer to a the extent descriptor of a page.
@param[in,out]	sp_header	tablespace header page, x-latched
@param[in]	space		tablespace identifier
@param[in]	offset		page offset
@param[in,out]	mtr		mini-transaction
@param[in]	init_space	whether the tablespace is being initialized
@param[out]	desc_block	descriptor block, or NULL if it is the same as the tablespace header
@return pointer to the extent descriptor, NULL if the page does not exist in the space or if the offset exceeds free limit */
xdes_t* xdes_get_descriptor_with_space_hdr(
	fsp_header_t*	sp_header,
	uint32		space,
	uint32		offset,
	mtr_t*		mtr,
	bool32		init_space = FALSE,
	buf_block_t**	desc_block = NULL)
{
	uint32	limit;
	uint32	size;
	uint32	descr_page_no;
	uint32	flags;
	page_t*	descr_page;

	ut_ad(mtr_memo_contains_page(mtr, sp_header, MTR_MEMO_PAGE_X_FIX));
	ut_ad(page_offset(sp_header) == FSP_HEADER_OFFSET);
	/* Read free limit and space size */
	limit = mach_read_from_4(sp_header + FSP_FREE_LIMIT);
	size  = mach_read_from_4(sp_header + FSP_SIZE);
	flags = mach_read_from_4(sp_header + FSP_SPACE_FLAGS);

	if ((offset >= size) || (offset >= limit)) {
		return(NULL);
	}

	const page_size_t	page_size(flags);

	descr_page_no = xdes_calc_descriptor_page(page_size, offset);

	buf_block_t   *block;
    
	if (descr_page_no == 0) {
		/* It is on the space header page */
		descr_page = page_align(sp_header);
		block = NULL;
	} else {
        page_id_t page_id(space, descr_page_no);
		block = buf_page_get(&page_id, page_size, RW_X_LATCH, mtr);
		descr_page = buf_block_get_frame(block);
	}

	if (desc_block != NULL) {
		*desc_block = block;
	}

	return(descr_page + XDES_ARR_OFFSET
	       + XDES_SIZE * xdes_calc_descriptor_index(page_size, offset));
}

/** Gets pointer to a the extent descriptor of a page.
The page where the extent descriptor resides is x-locked. If the page offset
is equal to the free limit of the space, adds new extents from above the free
limit to the space free list, if not free limit == space size. This adding
is necessary to make the descriptor defined, as they are uninitialized above the free limit.
@param[in]	space_id	space id
@param[in]	offset		page offset; if equal to the free limit, we try to add new extents to the space free list
@param[in]	page_size	page size
@param[in,out]	mtr		mini-transaction
@return pointer to the extent descriptor, NULL if the page does not exist in the space or if the offset exceeds the free limit */
xdes_t* xdes_get_descriptor(
	uint32			space_id,
	uint32			offset,
	const page_size_t&	page_size,
	mtr_t*			mtr)
{
	buf_block_t*	block;
	fsp_header_t*	sp_header;
    page_id_t       page_id(space_id, 0);
	block = buf_page_get(&page_id, page_size, RW_X_LATCH, mtr);

	sp_header = FSP_HEADER_OFFSET + buf_block_get_frame(block);
	return(xdes_get_descriptor_with_space_hdr(sp_header, space_id, offset, mtr));
}

/********************************************************************//**
Gets pointer to a the extent descriptor if the file address
of the descriptor list node is known. The page where the extent descriptor resides is x-locked.
@return pointer to the extent descriptor */
xdes_t* xdes_lst_get_descriptor(
	uint32		space,	/*!< in: space id */
	const page_size_t&	page_size,
	fil_addr_t	lst_node,/*!< in: file address of the list node
				contained in the descriptor */
	mtr_t*		mtr)	/*!< in/out: mini-transaction */
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
uint32 xdes_get_offset(
	const xdes_t*	descr)	/*!< in: extent descriptor */
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

static bool32 fsp_try_extend_data_file_with_pages(
	fil_space_t*  space,
	uint32        page_no,
	fsp_header_t* header,
	mtr_t*        mtr)
{
	bool32  success;
	uint32  size, actual_size = 0;

	ut_a(!is_system_tablespace(space->id));
	ut_d(fsp_space_modify_check(space->id, mtr));

	size = mach_read_from_4(header + FSP_SIZE);
	ut_ad(size == space->size_in_header);

	ut_a(page_no >= size);

	success = fil_space_extend(space, page_no + 1, &actual_size);
	/* The size may be less than we wanted if we ran out of disk space. */
	mlog_write_uint32(header + FSP_SIZE, actual_size, MLOG_4BYTES, mtr);
	space->size_in_header = actual_size;

	return(success);
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


/*
 * Put new extents to the free list if there are free extents above the free limit.
 * If an extent happens to contain an extent descriptor page,
 * the extent is put to the FSP_FREE_FRAG list with the page marked as used.
 */
static void fsp_fill_free_list(bool32 init_space, fil_space_t* space, fsp_header_t* header, mtr_t* mtr)
{
    uint32 limit;
    uint32 size;
    uint32 flags;
    xdes_t* descr;
    uint32 count = 0;
    uint32 frag_n_used;
    uint32 actual_increase = 0;
    uint32 i;

    ut_ad(page_offset(header) == FSP_HEADER_OFFSET);
    ut_d(fsp_space_modify_check(space->id, mtr));

    /* Check if we can fill free list from above the free list limit */
    size = mach_read_from_4(header + FSP_SIZE);
    limit = mach_read_from_4(header + FSP_FREE_LIMIT);
    flags = mach_read_from_4(header + FSP_SPACE_FLAGS);

    ut_ad(size == space->size_in_header);
    ut_ad(limit == space->free_limit);
    ut_ad(flags == space->flags);

    const page_size_t page_size(flags);

    if (space->id == FIL_SYSTEM_SPACE_ID && size < limit + FSP_EXTENT_SIZE * FSP_FREE_ADD) {
        /* Try to increase the last data file size */
        fsp_try_extend_data_file(&actual_increase, space, header, mtr);
        size = mtr_read_uint32(header + FSP_SIZE, MLOG_4BYTES, mtr);
    }

    if (space != FIL_SYSTEM_SPACE_ID && !init_space && size < limit + FSP_EXTENT_SIZE * FSP_FREE_ADD) {
        /* Try to increase the .ibd file size */
        fsp_try_extend_data_file(&actual_increase, space, header, mtr);
        size = mtr_read_uint32(header + FSP_SIZE, MLOG_4BYTES, mtr);
    }

    i = limit;
    while ((i + FSP_EXTENT_SIZE <= size) && (count < FSP_FREE_ADD)) {

        bool32 init_xdes = (ut_2pow_remainder(i, page_size.physical()) == 0);
        space->free_limit = i + FSP_EXTENT_SIZE;
        mlog_write_uint32(header + FSP_FREE_LIMIT, i + FSP_EXTENT_SIZE, MLOG_4BYTES, mtr);

        if (init_xdes) {
            buf_block_t* block;

            /* We are going to initialize a new descriptor page 
               and a new ibuf bitmap page:
               the prior contents of the pages should be ignored. */

            if (i > 0) {
                page_id_t page_id(space->id, i);
                block = buf_page_create(&page_id, page_size, RW_X_LATCH, mtr);
                //buf_page_get(page_id, page_size, RW_SX_LATCH, mtr);
                fsp_init_file_page(block, mtr);
                mlog_write_uint32(buf_block_get_frame(block) + FIL_PAGE_TYPE, FIL_PAGE_TYPE_XDES, MLOG_2BYTES, mtr);
            }
        }

        buf_block_t* desc_block = NULL;
        descr = xdes_get_descriptor_with_space_hdr(header, space->id, i, mtr, init_space, &desc_block);
        if (desc_block != NULL) {
            //fil_block_check_type(desc_block, FIL_PAGE_TYPE_XDES, mtr);
        }
        xdes_init(descr, mtr);

        if (UNLIKELY(init_xdes)) {
            /* The first page in the extent is a descriptor page
               and the second is an ibuf bitmap page: mark them used */
            xdes_set_bit(descr, XDES_FREE_BIT, 0, FALSE, mtr);
            xdes_set_bit(descr, XDES_FREE_BIT, FSP_IBUF_BITMAP_OFFSET, FALSE, mtr);
            xdes_set_state(descr, XDES_FREE_FRAG, mtr);

            flst_add_last(header + FSP_FREE_FRAG, descr + XDES_FLST_NODE, mtr);
            frag_n_used = mach_read_from_4(header + FSP_FRAG_N_USED);
            mlog_write_uint32(header + FSP_FRAG_N_USED, frag_n_used + 2, MLOG_4BYTES, mtr);
        } else {
            flst_add_last(header + FSP_FREE, descr + XDES_FLST_NODE, mtr);
            count++;
        }

        i += FSP_EXTENT_SIZE;
    }
}


static void fsp_init_file_page_low(buf_block_t* block)	/*!< in: pointer to a page */
{
    page_t* page = buf_block_get_frame(block);

    if (!fsp_is_system_temporary(block->page.id.space())) {
        memset(page, 0, UNIV_PAGE_SIZE);
    }

    mach_write_to_4(page + FIL_PAGE_OFFSET, block->page.id.page_no());
    mach_write_to_4(page + FIL_PAGE_SPACE_ID, block->page.id.space());
}


// Initialize a file page.
static void fsp_init_file_page(buf_block_t* block, mtr_t* mtr)
{
    fsp_init_file_page_low(block);

    //ut_d(fsp_space_modify_check(block->page.id.space(), mtr));
    mlog_write_initial_log_record(buf_block_get_frame(block), MLOG_INIT_FILE_PAGE2, mtr);
}


/* Allocates a new free extent. */
/* out: extent descriptor, NULL if cannot be allocated */
static xdes_t* fsp_alloc_free_extent(
    uint32 space_id,   /* in: space id */
    const page_size_t&  page_size,
    uint32 hint,    /* in: hint of which extent would be desirable: 
                           any page offset in the extent goes; the hint must not be > FSP_FREE_LIMIT */
    mtr_t* mtr)     /* in: mtr */
{
    fsp_header_t* header;
    fil_addr_t first;
    xdes_t* descr;

    ut_ad(mtr);

    header = fsp_get_space_header(space_id, page_size, mtr);
    descr = xdes_get_descriptor_with_space_hdr(header, space_id, hint, mtr);

    if (descr && (xdes_get_state(descr, mtr) == XDES_FREE)) {
        /* Ok, we can take this extent */
    } else {
        /* Take the first extent in the free list */
        first = flst_get_first(header + FSP_FREE, mtr);
        if (fil_addr_is_null(first)) {
            fil_space_t *space = fil_space_get_by_id(space_id);
            ut_a(space != NULL);
            fsp_fill_free_list(false, space, header, mtr);
            first = flst_get_first(header + FSP_FREE, mtr);
        }
        if (fil_addr_is_null(first)) {
            return(NULL); /* No free extents left */
        }
        descr = xdes_lst_get_descriptor(space_id, page_size, first, mtr);
    }
    flst_remove(header + FSP_FREE, descr + XDES_FLST_NODE, mtr);

    return(descr);
}


/**********************************************************************//**
Gets a buffer block for an allocated page.

NOTE: If init_mtr != mtr, the block will only be initialized if it was
not previously x-latched. It is assumed that the block has been
x-latched only by mtr, and freed in mtr in that case.

@return block, initialized if init_mtr==mtr
or rw_lock_x_lock_count(&block->lock) == 1 */
static
buf_block_t*
fsp_page_create(
/*============*/
	uint32	space,		/*!< in: space id of the allocated page */
	uint32	zip_size,	/*!< in: compressed page size in bytes
				or 0 for uncompressed pages */
	uint32	page_no,	/*!< in: page number of the allocated page */
	mtr_t*	mtr,		/*!< in: mini-transaction of the allocation */
	mtr_t*	init_mtr)	/*!< in: mini-transaction for initializing
				the page */
{
    page_id_t page_id(space, 0);
    const page_size_t page_size(0);
	buf_block_t *block = buf_page_create(&page_id, page_size, RW_X_LATCH, init_mtr);
#ifdef UNIV_SYNC_DEBUG
	ut_ad(mtr_memo_contains(mtr, block, MTR_MEMO_PAGE_X_FIX)
	      == rw_lock_own(&block->lock, RW_LOCK_EX));
#endif /* UNIV_SYNC_DEBUG */

	/* Mimic buf_page_get(), but avoid the buf_pool->page_hash lookup. */
	rw_lock_x_lock(&block->rw_lock);
	mutex_enter(&block->mutex);
    buf_block_fix(block);
	mutex_exit(&block->mutex);
	mtr_memo_push(init_mtr, block, MTR_MEMO_PAGE_X_FIX);

	if (init_mtr == mtr
	    || rw_lock_get_x_lock_count(&block->rw_lock) == 1) {

		/* Initialize the page, unless it was already
		X-latched in mtr. (In this case, we would want to
		allocate another page that has not been freed in mtr.) */
		ut_ad(init_mtr == mtr
		      || !mtr_memo_contains(mtr, block, MTR_MEMO_PAGE_X_FIX));

		fsp_init_file_page(block, init_mtr);
	}

	return(block);
}


/**************************************************************************
Allocates a single free page from a space. The page is marked as used. */
/* out: the page offset, FIL_NULL if no page could be allocated */
static buf_block_t* fsp_alloc_free_page(
    uint32 space,   /* in: space id */
    const page_size_t&  page_size,
    uint32 hint,    /* in: hint of which page would be desirable */
    mtr_t *mtr)     /* in: mtr handle */
{
    fsp_header_t* header;
    fil_addr_t first;
    xdes_t* descr;
    page_t* page;
    uint32 free;
    uint32 frag_n_used;
    uint32 page_no;

    ut_ad(mtr);

    header = fsp_get_space_header(space, page_size, mtr);

    /* Get the hinted descriptor */
    descr = xdes_get_descriptor_with_space_hdr(header, space, hint, mtr);

    if (descr && (xdes_get_state(descr, mtr) == XDES_FREE_FRAG)) {
        /* Ok, we can take this extent */
    } else {
        /* Else take the first extent in free_frag list */
        first = flst_get_first(header + FSP_FREE_FRAG, mtr);

        if (fil_addr_is_null(first)) {
            /* There are no partially full fragments: allocate a free extent and add it to the FREE_FRAG list.
               NOTE that the allocation may have as a side-effect
               that an extent containing a descriptor page is added to the FREE_FRAG list. 
               But we will allocate our page from the the free extent anyway. */
            descr = fsp_alloc_free_extent(space, page_size, hint, mtr);
            if (descr == NULL) {
                /* No free space left */
                return NULL;
            }
            xdes_set_state(descr, XDES_FREE_FRAG, mtr);
            flst_add_last(header + FSP_FREE_FRAG, descr + XDES_FLST_NODE, mtr);
        } else {
            descr = xdes_lst_get_descriptor(space, page_size, first, mtr);
        }

        /* Reset the hint */
        hint = 0;
    }

    /* Now we have in descr an extent with at least one free page. Look for a free page in the extent. */
    free = xdes_find_bit(descr, XDES_FREE_BIT, TRUE, hint % FSP_EXTENT_SIZE, mtr);
    //ut_a(free != uint32_UNDEFINED);
    xdes_set_bit(descr, XDES_FREE_BIT, free, FALSE, mtr);

    /* Update the FRAG_N_USED field */
    frag_n_used = mach_read_from_4(header + FSP_FRAG_N_USED);
    frag_n_used++;
    mlog_write_uint32(header + FSP_FRAG_N_USED, frag_n_used, MLOG_4BYTES, mtr);
    if (xdes_is_full(descr, mtr)) {
        /* The fragment is full: move it to another list */
        flst_remove(header + FSP_FREE_FRAG, descr + XDES_FLST_NODE, mtr);
        xdes_set_state(descr, XDES_FULL_FRAG, mtr);
        flst_add_last(header + FSP_FULL_FRAG, descr + XDES_FLST_NODE, mtr);
        mlog_write_uint32(header + FSP_FRAG_N_USED, frag_n_used - FSP_EXTENT_SIZE, MLOG_4BYTES, mtr);
    }

    page_no = xdes_get_offset(descr) + free;

    /* Initialize the allocated page to the buffer pool,
       so that it can be obtained immediately with buf_page_get without need for a disk read. */
    page_id_t page_id1(space, page_no);
    buf_page_create(&page_id1, page_size, RW_X_LATCH, mtr);
    buf_block_t  *block;
    page_id_t     page_id(space, page_no);
    block = buf_page_get(&page_id, page_size, RW_X_LATCH, mtr);
    //page = buf_block_get_frame(block);

    /* Prior contents of the page should be ignored */
    fsp_init_file_page(block, mtr);

    return block;
}


/**************************************************************************
Returns an extent to the free list of a space. */
static void fsp_free_extent(
    const page_id_t &page_id,
    const page_size_t &page_size,
    mtr_t* mtr)    /* in: mtr */
{
    fsp_header_t* header;
    xdes_t* descr;

    ut_ad(mtr);

    header = fsp_get_space_header(page_id.space(), page_size, mtr);
    descr = xdes_get_descriptor_with_space_hdr(header, page_id.space(), page_id.page_no(), mtr);
    ut_a(xdes_get_state(descr, mtr) != XDES_FREE);
    xdes_init(descr, mtr);
    flst_add_last(header + FSP_FREE, descr + XDES_FLST_NODE, mtr);
}

/**************************************************************************
Frees a single page of a space. The page is marked as free and clean. */
static void fsp_free_page(
    const page_id_t& page_id,
    const page_size_t& page_size,
    mtr_t* mtr)     /* in: mtr handle */
{
    fsp_header_t* header;
    xdes_t* descr;
    uint32 state;
    uint32 frag_n_used;

    ut_ad(mtr);

    /* printf("Freeing page %lu in space %lu\n", page, space); */

    header = fsp_get_space_header(page_id.space(), page_size, mtr);
    descr = xdes_get_descriptor_with_space_hdr(header, page_id.space(), page_id.page_no(), mtr);
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
    } else {
        ut_a(frag_n_used > 0);
        mlog_write_uint32(header + FSP_FRAG_N_USED, frag_n_used - 1, MLOG_4BYTES, mtr);
    }

    if (xdes_is_free(descr, mtr)) {
        /* The extent has become free: move it to another list */
        flst_remove(header + FSP_FREE_FRAG, descr + XDES_FLST_NODE, mtr);
        fsp_free_extent(page_id, page_size, mtr);
    }
}



/** Gets a pointer to the space header and x-locks its page.
@param[in]  id          space id
@param[in]  page_size   page size
@param[in,out]  mtr     mini-transaction
@return pointer to the space header, page x-locked */
fsp_header_t* fsp_get_space_header(
    uint32              id,
    const page_size_t&  page_size,
    mtr_t*              mtr)
{
    buf_block_t*    block;
    fsp_header_t*   header;
    page_id_t       page_id(id, 0);
    ut_ad(id != 0 || !page_size.is_compressed());

    block = buf_page_get(&page_id, page_size, RW_X_LATCH, mtr);
    header = FSP_HEADER_OFFSET + buf_block_get_frame(block);
    //buf_block_dbg_add_level(block, SYNC_FSP_PAGE);

    ut_ad(id == mach_read_from_4(FSP_SPACE_ID + header));
#ifdef UNIV_DEBUG
    const uint32	flags = mach_read_from_4(FSP_SPACE_FLAGS + header);
    //ut_ad(page_size_t(flags).equals_to(page_size));
#endif /* UNIV_DEBUG */
    return(header);
}

//Initializes the space header of a new created space and creates also the insert buffer tree root. */
void fsp_header_init(fil_space_t *space, uint32 size, mtr_t *mtr)
{
    fsp_header_t* header;
    buf_block_t *block;
    page_t* page;

    ut_ad(mtr);
    ut_a(space != NULL);

    mtr_x_lock(&space->latch, mtr);

    page_id_t page_id(space->id, 0);
    const page_size_t page_size(0);
    block = buf_page_create(&page_id, page_size, RW_X_LATCH, mtr);

    fsp_init_file_page(block, mtr);
    page = buf_block_get_frame(block);

    header = FSP_HEADER_OFFSET + page;

    space->size_in_header = size;
    mlog_write_uint32(header + FSP_SIZE, size, MLOG_4BYTES, mtr);
    mlog_write_uint32(header + FSP_FREE_LIMIT, 0, MLOG_4BYTES, mtr);
    //mlog_write_uint32(header + FSP_LOWEST_NO_WRITE, 0, MLOG_4BYTES, mtr);
    mlog_write_uint32(header + FSP_FRAG_N_USED, 0, MLOG_4BYTES, mtr);

    flst_init(header + FSP_FREE, mtr);
    flst_init(header + FSP_FREE_FRAG, mtr);
    flst_init(header + FSP_FULL_FRAG, mtr);
    flst_init(header + FSP_SEG_INODES_FULL, mtr);
    flst_init(header + FSP_SEG_INODES_FREE, mtr);

    mlog_write_uint64(header + FSP_SEG_ID, ut_ull_create(0, 1), mtr);

    mutex_enter(&fil_system->mutex);
    fsp_fill_free_list(false, space, header, mtr);
    mutex_exit(&fil_system->mutex);

    //btr_create(DICT_CLUSTERED | DICT_UNIVERSAL | DICT_IBUF, space,
    //			ut_dulint_add(DICT_IBUF_ID_MIN, space), mtr);
}




/********************************************************************//**
Marks a page used. The page must reside within the extents of the given
segment. */
static void fseg_mark_page_used(
	fseg_inode_t*	seg_inode,/*!< in: segment inode */
	uint32		page,	/*!< in: page offset */
	xdes_t*		descr,  /*!< in: extent descriptor */
	mtr_t*		mtr)	/*!< in/out: mini-transaction */
{
	uint32	not_full_n_used;

	ut_ad(!((page_offset(seg_inode) - FSEG_ARR_OFFSET) % FSEG_INODE_SIZE));
	ut_ad(mach_read_from_4(seg_inode + FSEG_MAGIC_N)
	      == FSEG_MAGIC_N_VALUE);

	ut_ad(mtr_read_uint32(seg_inode + FSEG_ID, MLOG_4BYTES, mtr)
	      == mtr_read_uint32(descr + XDES_ID, MLOG_4BYTES, mtr));

	if (xdes_is_free(descr, mtr)) {
		/* We move the extent from the free list to the
		NOT_FULL list */
		flst_remove(seg_inode + FSEG_FREE, descr + XDES_FLST_NODE,
			    mtr);
		flst_add_last(seg_inode + FSEG_NOT_FULL,
			      descr + XDES_FLST_NODE, mtr);
	}

	ut_ad(xdes_mtr_get_bit(
			descr, XDES_FREE_BIT, page % FSP_EXTENT_SIZE, mtr));

	/* We mark the page as used */
	xdes_set_bit(descr, XDES_FREE_BIT, page % FSP_EXTENT_SIZE, FALSE, mtr);

	not_full_n_used = mtr_read_uint32(seg_inode + FSEG_NOT_FULL_N_USED, MLOG_4BYTES, mtr);
	not_full_n_used++;
    mlog_write_uint32(seg_inode + FSEG_NOT_FULL_N_USED, not_full_n_used, MLOG_4BYTES, mtr);
	if (xdes_is_full(descr, mtr)) {
		/* We move the extent from the NOT_FULL list to the FULL list */
		flst_remove(seg_inode + FSEG_NOT_FULL, descr + XDES_FLST_NODE, mtr);
		flst_add_last(seg_inode + FSEG_FULL, descr + XDES_FLST_NODE, mtr);

        mlog_write_uint32(seg_inode + FSEG_NOT_FULL_N_USED,
				 not_full_n_used - FSP_EXTENT_SIZE, MLOG_4BYTES, mtr);
	}
}


/**********************************************************************//**
Finds a fragment page slot which is free.
@return	slot index; ULINT_UNDEFINED if none found */
static
uint32 fseg_find_free_frag_page_slot(
	fseg_inode_t*	inode,	/*!< in: segment inode */
	mtr_t*		mtr)	/*!< in/out: mini-transaction */
{
	uint32	i;
	uint32	page_no;

	ut_ad(inode && mtr);

	for (i = 0; i < FSEG_FRAG_ARR_N_SLOTS; i++) {
		page_no = fseg_get_nth_frag_page_no(inode, i, mtr);
		if (page_no == FIL_NULL) {
			return(i);
		}
	}

	return UINT32_UNDEFINED;
}


/*********************************************************************//**
Allocates a free extent for the segment: looks first in the free list of the
segment, then tries to allocate from the space free list. NOTE that the extent
returned still resides in the segment free list, it is not yet taken off it!
@retval NULL if no page could be allocated
@retval block, rw_lock_x_lock_count(&block->lock) == 1 if allocation succeeded
(init_mtr == mtr, or the page was not previously freed in mtr)
@retval block (not allocated or initialized) otherwise */
static xdes_t* fseg_alloc_free_extent(
	fseg_inode_t*	inode,	/*!< in: segment inode */
	uint32		space,	/*!< in: space id */
	uint32		zip_size,/*!< in: compressed page size in bytes or 0 for uncompressed pages */
	mtr_t*		mtr)	/*!< in/out: mini-transaction */
{
	xdes_t*		descr;
	ib_id_t		seg_id;
	fil_addr_t	first;
    const page_size_t page_size(0);

	ut_ad(!((page_offset(inode) - FSEG_ARR_OFFSET) % FSEG_INODE_SIZE));
	ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

	if (flst_get_len(inode + FSEG_FREE) > 0) {
		/* Segment free list is not empty, allocate from it */
		first = flst_get_first(inode + FSEG_FREE, mtr);
		descr = xdes_lst_get_descriptor(space, page_size, first, mtr);
	} else {
		/* Segment free list was empty, allocate from space */
		descr = fsp_alloc_free_extent(space, page_size, 0, mtr);
		if (descr == NULL) {
			return(NULL);
		}

		seg_id = mach_read_from_8(inode + FSEG_ID);

		xdes_set_state(descr, XDES_FSEG, mtr);
		mlog_write_uint64(descr + XDES_ID, seg_id, mtr);
		flst_add_last(inode + FSEG_FREE, descr + XDES_FLST_NODE, mtr);

		/* Try to fill the segment free list */
		fseg_fill_free_list(inode, space, zip_size,
				    xdes_get_offset(descr) + FSP_EXTENT_SIZE,
				    mtr);
	}

	return(descr);
}


/*********************************************************************//**
Tries to fill the free list of a segment with consecutive free extents.
This happens if the segment is big enough to allow extents in the free list,
the free list is empty, and the extents can be allocated consecutively from
the hint onward. */
static
void
fseg_fill_free_list(
	fseg_inode_t*	inode,	/*!< in: segment inode */
	uint32		space,	/*!< in: space id */
	uint32		zip_size,/*!< in: compressed page size in bytes
				or 0 for uncompressed pages */
	uint32		hint,	/*!< in: hint which extent would be good as
				the first extent */
	mtr_t*		mtr)	/*!< in/out: mini-transaction */
{
	xdes_t*	descr;
	uint32	i;
	ib_id_t	seg_id;
	uint32	reserved;
	uint32	used;
    const page_size_t page_size(0);

	ut_ad(inode && mtr);
	ut_ad(!((page_offset(inode) - FSEG_ARR_OFFSET) % FSEG_INODE_SIZE));

	reserved = fseg_n_reserved_pages_low(inode, &used, mtr);

	if (reserved < FSEG_FREE_LIST_LIMIT * FSP_EXTENT_SIZE) {

		/* The segment is too small to allow extents in free list */

		return;
	}

	if (flst_get_len(inode + FSEG_FREE) > 0) {
		/* Free list is not empty */

		return;
	}

	for (i = 0; i < FSEG_FREE_LIST_MAX_LEN; i++) {
		descr = xdes_get_descriptor(space, hint, page_size, mtr);

		if ((descr == NULL)
		    || (XDES_FREE != xdes_get_state(descr, mtr))) {

			/* We cannot allocate the desired extent: stop */

			return;
		}

		descr = fsp_alloc_free_extent(space, page_size, hint, mtr);

		xdes_set_state(descr, XDES_FSEG, mtr);

		seg_id = mach_read_from_8(inode + FSEG_ID);
		ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N)
		      == FSEG_MAGIC_N_VALUE);
		mlog_write_uint64(descr + XDES_ID, seg_id, mtr);

		flst_add_last(inode + FSEG_FREE, descr + XDES_FLST_NODE, mtr);
		hint += FSP_EXTENT_SIZE;
	}
}


/**********************************************************************//**
Allocates a single free page from a segment.
 This function implements the intelligent allocation strategy which tries to minimize file space fragmentation.
@retval NULL if no page could be allocated
@retval block, rw_lock_x_lock_count(&block->lock) == 1 if allocation succeeded
(init_mtr == mtr, or the page was not previously freed in mtr)
@retval block (not allocated or initialized) otherwise */
static
buf_block_t*
fseg_alloc_free_page_low(
	uint32		space,	/*!< in: space */
	uint32		zip_size,/*!< in: compressed page size in bytes
				or 0 for uncompressed pages */
	fseg_inode_t*	seg_inode, /*!< in/out: segment inode */
	uint32		hint,	/*!< in: hint of which page would be
				desirable */
	byte		direction, /*!< in: if the new page is needed because
				of an index page split, and records are
				inserted there in order, into which
				direction they go alphabetically: FSP_DOWN,
				FSP_UP, FSP_NO_DIR */
	mtr_t*		mtr,	/*!< in/out: mini-transaction */
	mtr_t*		init_mtr)/*!< in/out: mtr or another mini-transaction
				in which the page should be initialized.
				If init_mtr!=mtr, but the page is already
				latched in mtr, do not initialize the page. */
{
	fsp_header_t*	space_header;
	uint32		space_size;
	ib_id_t		seg_id;
	uint32		used;
	uint32		reserved;
	xdes_t*		descr;		/*!< extent of the hinted page */
	uint32		ret_page;	/*!< the allocated page offset, FIL_NULL if could not be allocated */
	xdes_t*		ret_descr;	/*!< the extent of the allocated page */
	bool32		success;
	uint32		n;

    const page_size_t page_size(0);

	ut_ad(mtr);
	ut_ad((direction >= FSP_UP) && (direction <= FSP_NO_DIR));
	ut_ad(mach_read_from_4(seg_inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);
	ut_ad(!((page_offset(seg_inode) - FSEG_ARR_OFFSET) % FSEG_INODE_SIZE));
	seg_id = mach_read_from_8(seg_inode + FSEG_ID);

	ut_ad(seg_id);

	reserved = fseg_n_reserved_pages_low(seg_inode, &used, mtr);
	space_header = fsp_get_space_header(space, page_size, mtr);

	descr = xdes_get_descriptor_with_space_hdr(space_header, space, hint, mtr);
	if (descr == NULL) {
		/* Hint outside space or too high above free limit: reset hint */
		/* The file space header page is always allocated. */
		hint = 0;
		descr = xdes_get_descriptor(space, zip_size, page_size, mtr);
	}

	/* In the big if-else below we look for ret_page and ret_descr */
	/*-------------------------------------------------------------*/
	if ((xdes_get_state(descr, mtr) == XDES_FSEG)
	    && mach_read_from_8(descr + XDES_ID) == seg_id
	    && (xdes_mtr_get_bit(descr, XDES_FREE_BIT,
				 hint % FSP_EXTENT_SIZE, mtr) == TRUE)) {
take_hinted_page:
		/* 1. We can take the hinted page
		=================================*/
		ret_descr = descr;
		ret_page = hint;
		/* Skip the check for extending the tablespace. If the
		page hint were not within the size of the tablespace,
		we would have got (descr == NULL) above and reset the hint. */
		goto got_hinted_page;
		/*-----------------------------------------------------------*/
	} else if (xdes_get_state(descr, mtr) == XDES_FREE
		   && reserved - used < reserved / FSEG_FILLFACTOR
		   && used >= FSEG_FRAG_LIMIT) {

		/* 2. We allocate the free extent from space and can take
		=========================================================
		the hinted page
		===============*/
		ret_descr = fsp_alloc_free_extent(space, page_size, hint, mtr);

		ut_a(ret_descr == descr);

		xdes_set_state(ret_descr, XDES_FSEG, mtr);
		mlog_write_uint64(ret_descr + XDES_ID, seg_id, mtr);
		flst_add_last(seg_inode + FSEG_FREE, ret_descr + XDES_FLST_NODE, mtr);

		/* Try to fill the segment free list */
		fseg_fill_free_list(seg_inode, space, page_size.physical(), hint + FSP_EXTENT_SIZE, mtr);
		goto take_hinted_page;
		/*-----------------------------------------------------------*/
	} else if ((direction != FSP_NO_DIR)
		   && ((reserved - used) < reserved / FSEG_FILLFACTOR)
		   && (used >= FSEG_FRAG_LIMIT)
		   && (!!(ret_descr
			  = fseg_alloc_free_extent(seg_inode, space, zip_size, mtr)))) {

		/* 3. We take any free extent (which was already assigned above
		===============================================================
		in the if-condition to ret_descr) and take the lowest or
		========================================================
		highest page in it, depending on the direction
		==============================================*/
		ret_page = xdes_get_offset(ret_descr);

		if (direction == FSP_DOWN) {
			ret_page += FSP_EXTENT_SIZE - 1;
		}
		/*-----------------------------------------------------------*/
	} else if ((xdes_get_state(descr, mtr) == XDES_FSEG)
		   && mach_read_from_8(descr + XDES_ID) == seg_id
		   && (!xdes_is_full(descr, mtr))) {

		/* 4. We can take the page from the same extent as the
		======================================================
		hinted page (and the extent already belongs to the
		==================================================
		segment)
		========*/
		ret_descr = descr;
		ret_page = xdes_get_offset(ret_descr)
			+ xdes_find_bit(ret_descr, XDES_FREE_BIT, TRUE, hint % FSP_EXTENT_SIZE, mtr);
		/*-----------------------------------------------------------*/
	} else if (reserved - used > 0) {
		/* 5. We take any unused page from the segment
		==============================================*/
		fil_addr_t	first;

		if (flst_get_len(seg_inode + FSEG_NOT_FULL) > 0) {
			first = flst_get_first(seg_inode + FSEG_NOT_FULL, mtr);
		} else if (flst_get_len(seg_inode + FSEG_FREE) > 0) {
			first = flst_get_first(seg_inode + FSEG_FREE, mtr);
		} else {
			ut_error;
			return(NULL);
		}

		ret_descr = xdes_lst_get_descriptor(space, page_size, first, mtr);
		ret_page = xdes_get_offset(ret_descr)
			+ xdes_find_bit(ret_descr, XDES_FREE_BIT, TRUE, 0, mtr);
		/*-----------------------------------------------------------*/
	} else if (used < FSEG_FRAG_LIMIT) {
		/* 6. We allocate an individual page from the space
		===================================================*/
		buf_block_t* block = fsp_alloc_free_page(space, page_size, hint, mtr);

		if (block != NULL) {
			/* Put the page in the fragment page array of the segment */
			n = fseg_find_free_frag_page_slot(seg_inode, mtr);
			ut_a(n != UINT32_UNDEFINED);

			fseg_set_nth_frag_page_no(seg_inode, n, block->get_page_no(), mtr);
		}

		/* fsp_alloc_free_page() invoked fsp_init_file_page() already. */
		return(block);
		/*-----------------------------------------------------------*/
	} else {
		/* 7. We allocate a new extent and take its first page
		======================================================*/
		ret_descr = fseg_alloc_free_extent(seg_inode, space, zip_size, mtr);
		if (ret_descr == NULL) {
			ret_page = FIL_NULL;
		} else {
			ret_page = xdes_get_offset(ret_descr);
		}
	}

	if (ret_page == FIL_NULL) {
		/* Page could not be allocated */
		return(NULL);
	}

	if (space != 0) {
		space_size = fil_space_get_size(space);
		if (space_size <= ret_page) {
			/* It must be that we are extending a single-table
			tablespace whose size is still < 64 pages */

			if (ret_page >= FSP_EXTENT_SIZE) {
				fprintf(stderr,
					"InnoDB: Error (2): trying to extend"
					" a single-table tablespace %lu\n"
					"InnoDB: by single page(s) though"
					" the space size %lu. Page no %lu.\n",
					(ulong) space, (ulong) space_size,
					(ulong) ret_page);
				return(NULL);
			}

			success = fsp_try_extend_data_file_with_pages(
                fil_space_get_by_id(space), ret_page, space_header, mtr);
			if (!success) {
				/* No disk space left */
				return(NULL);
			}
		}
	}

got_hinted_page:
	/* ret_descr == NULL if the block was allocated from free_frag
	(XDES_FREE_FRAG) */
	if (ret_descr != NULL) {
		/* At this point we know the extent and the page offset.
		The extent is still in the appropriate list (FSEG_NOT_FULL
		or FSEG_FREE), and the page is not yet marked as used. */

		ut_ad(xdes_get_descriptor(space, ret_page, page_size, mtr) == ret_descr);
		ut_ad(xdes_mtr_get_bit(ret_descr, XDES_FREE_BIT, ret_page % FSP_EXTENT_SIZE, mtr));

		fseg_mark_page_used(seg_inode, ret_page, ret_descr, mtr);
	}

	return(fsp_page_create(
		       space,
               0,//fsp_flags_get_zip_size(mach_read_from_4(FSP_SPACE_FLAGS + space_header)),
		       ret_page, mtr, init_mtr));
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
	uint32		space,	/*!< in: space id */
	uint32		zip_size,/*!< in: compressed page size in bytes
				or 0 for uncompressed pages */
	fseg_inode_t*	inode,	/*!< in: segment inode */
	mtr_t*		mtr)	/*!< in/out: mini-transaction */
{
	page_t*		page;
	fsp_header_t*	space_header;
    const page_size_t page_size(0);

	page = page_align(inode);
	space_header = fsp_get_space_header(space, page_size, mtr);

	ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

	if (UINT32_UNDEFINED == fsp_seg_inode_page_find_free(page, 0, zip_size, mtr)) {
		/* Move the page to another list */
		flst_remove(space_header + FSP_SEG_INODES_FULL, page + FSEG_INODE_PAGE_NODE, mtr);
		flst_add_last(space_header + FSP_SEG_INODES_FREE, page + FSEG_INODE_PAGE_NODE, mtr);
	}

	mlog_write_uint64(inode + FSEG_ID, 0, mtr);
	mlog_write_uint32(inode + FSEG_MAGIC_N, 0xfa051ce3, MLOG_4BYTES, mtr);

	if (UINT32_UNDEFINED == fsp_seg_inode_page_find_used(page, zip_size, mtr)) {
		/* There are no other used headers left on the page: free it */
		flst_remove(space_header + FSP_SEG_INODES_FREE, page + FSEG_INODE_PAGE_NODE, mtr);
        page_id_t page_id(space, page_get_page_no(page));
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
static bool32 fsp_alloc_seg_inode_page(
    fsp_header_t *space_header, /*!< in: space header */
    mtr_t        *mtr)          /*!< in/out: mini-transaction */
{
	fseg_inode_t*	inode;
	buf_block_t*	block;
	page_t*		page;
	uint32		space;

	ut_ad(page_offset(space_header) == FSP_HEADER_OFFSET);

	space = page_get_space_id(page_align(space_header));

	//zip_size = fsp_flags_get_zip_size(mach_read_from_4(FSP_SPACE_FLAGS + space_header));
    const page_size_t page_size(0);
	block = fsp_alloc_free_page(space, page_size, 0, mtr);
	if (block == NULL) {
		return(FALSE);
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

	return(TRUE);
}

// Allocates a new file segment inode.
// return segment inode, or NULL if not enough space
static fseg_inode_t* fsp_alloc_seg_inode(
    fsp_header_t *space_header, /*!< in: space header */
    mtr_t        *mtr)          /*!< in/out: mini-transaction */
{
	uint32		page_no;
	buf_block_t*	block;
	page_t*		page;
	fseg_inode_t*	inode;
	bool32		success;
	uint32		n;

	ut_ad(page_offset(space_header) == FSP_HEADER_OFFSET);

	if (flst_get_len(space_header + FSP_SEG_INODES_FREE) == 0) {
		/* Allocate a new segment inode page */
		success = fsp_alloc_seg_inode_page(space_header, mtr);
		if (!success) {
			return(NULL);
		}
	}

	page_no = flst_get_first(space_header + FSP_SEG_INODES_FREE, mtr).page;

    page_id_t page_id(page_get_space_id(page_align(space_header)), page_no);
    const page_size_t page_size(0);
	block = buf_page_get(&page_id, page_size, RW_X_LATCH, mtr);
	//buf_block_dbg_add_level(block, SYNC_FSP_PAGE);

	page = buf_block_get_frame(block);

	n = fsp_seg_inode_page_find_free(page, 0, page_size.physical(), mtr);

	ut_a(n != UINT32_UNDEFINED);

	inode = fsp_seg_inode_page_get_nth_inode(page, n, page_size.physical(), mtr);
	if (UINT32_UNDEFINED == fsp_seg_inode_page_find_free(page, n + 1, page_size.physical(), mtr)) {
		/* There are no other unused headers left on the page:
           move it to another list */
		flst_remove(space_header + FSP_SEG_INODES_FREE, page + FSEG_INODE_PAGE_NODE, mtr);
		flst_add_last(space_header + FSP_SEG_INODES_FULL, page + FSEG_INODE_PAGE_NODE, mtr);
	}

	ut_ad(!mach_read_from_8(inode + FSEG_ID)
	      || mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);
	return(inode);
}


// Gets the page number from the nth fragment page slot.
// return page number, FIL_NULL if not in use
uint32 fseg_get_nth_frag_page_no(
	fseg_inode_t *inode,	/*!< in: segment inode */
	uint32        n,	/*!< in: slot index */
	mtr_t        *mtr)
{
	ut_ad(inode && mtr);
	ut_ad(n < FSEG_FRAG_ARR_N_SLOTS);
	ut_ad(mtr_memo_contains_page(mtr, inode, MTR_MEMO_PAGE_X_FIX));
	ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);
	return(mach_read_from_4(inode + FSEG_FRAG_ARR + n * FSEG_FRAG_SLOT_SIZE));
}

// Calculates reserved fragment page slots.
// return number of fragment pages
static uint32 fseg_get_n_frag_pages(fseg_inode_t *inode, mtr_t *mtr)
{
	uint32	i;
	uint32	count	= 0;

	ut_ad(inode && mtr);

	for (i = 0; i < FSEG_FRAG_ARR_N_SLOTS; i++) {
		if (FIL_NULL != fseg_get_nth_frag_page_no(inode, i, mtr)) {
			count++;
		}
	}

	return(count);
}

// Calculates the number of pages reserved by a segment, and how many pages are currently used.
// return number of reserved pages
static uint32 fseg_n_reserved_pages_low(
    fseg_inode_t *inode, /*!< in: segment inode */
    uint32       *used,  /*!< out: number of pages used (not more than reserved) */
    mtr_t        *mtr)   /*!< in/out: mini-transaction */
{
	uint32 ret;

	ut_ad(inode && used && mtr);
	ut_ad(mtr_memo_contains_page(mtr, inode, MTR_MEMO_PAGE_X_FIX));

	*used = mtr_read_uint32(inode + FSEG_NOT_FULL_N_USED, MLOG_4BYTES, mtr)
		+ FSP_EXTENT_SIZE * flst_get_len(inode + FSEG_FULL)
		+ fseg_get_n_frag_pages(inode, mtr);

	ret = fseg_get_n_frag_pages(inode, mtr)
		+ FSP_EXTENT_SIZE * flst_get_len(inode + FSEG_FREE)
		+ FSP_EXTENT_SIZE * flst_get_len(inode + FSEG_NOT_FULL)
		+ FSP_EXTENT_SIZE * flst_get_len(inode + FSEG_FULL);

	return(ret);
}


/**********************************************************************//**
Checks that we have at least 2 frag pages free in the first extent of a
single-table tablespace, and they are also physically initialized to the data
file. That is we have already extended the data file so that those pages are
inside the data file. If not, this function extends the tablespace with
pages.
@return	TRUE if there were >= 3 free pages, or we were able to extend */
static bool32 fsp_reserve_free_pages(
	uint32		space,		/*!< in: space id, must be != 0 */
	fsp_header_t*	space_header,	/*!< in: header of that space,
					x-latched */
	uint32		size,		/*!< in: size of the tablespace in
					pages, must be < FSP_EXTENT_SIZE/2 */
	mtr_t*		mtr)		/*!< in/out: mini-transaction */
{
	xdes_t*	descr;
	uint32	n_used;

	ut_a(space != 0);
	ut_a(size < FSP_EXTENT_SIZE / 2);

	descr = xdes_get_descriptor_with_space_hdr(space_header, space, 0, mtr);
	n_used = xdes_get_n_used(descr, mtr);

	ut_a(n_used <= size);

	if (size >= n_used + 2) {
		return(TRUE);
	}

    mutex_enter(&fil_system->mutex, NULL);
    fil_space_t *fil_space = fil_space_get_by_id(space);
    ut_a(fil_space->magic_n == M_FIL_SPACE_MAGIC_N);
    mutex_exit(&fil_system->mutex);
	return(fsp_try_extend_data_file_with_pages(fil_space, n_used + 1, space_header, mtr));
}


// Reserves free pages from a tablespace
bool32 fsp_reserve_free_extents(
	uint32*	n_reserved,/*!< out: number of extents actually reserved; if we
			return TRUE and the tablespace size is < 64 pages,
			then this can be 0, otherwise it is n_ext */
	uint32	space,	/*!< in: space id */
	uint32	n_ext,	/*!< in: number of extents to reserve */
	uint32	alloc_type,/*!< in: FSP_NORMAL, FSP_UNDO, or FSP_CLEANING */
	mtr_t*	mtr)	/*!< in/out: mini-transaction */
{
	fsp_header_t*	space_header;
	rw_lock_t*	latch;
	uint32		n_free_list_ext;
	uint32		free_limit;
	uint32		size;
	uint32		flags;
	uint32		n_free;
	uint32		n_free_up;
	uint32		reserve;
	bool32		success;
	uint32		n_pages_added;

	ut_ad(mtr);
	*n_reserved = n_ext;

	latch = fil_space_get_latch(space, &flags);
	//zip_size = fsp_flags_get_zip_size(flags);
    const page_size_t page_size(0);

	mtr_x_lock(latch, mtr);

	space_header = fsp_get_space_header(space, page_size, mtr);
try_again:
	size = mtr_read_uint32(space_header + FSP_SIZE, MLOG_4BYTES, mtr);

	if (size < FSP_EXTENT_SIZE / 2) {
		/* Use different rules for small single-table tablespaces */
		*n_reserved = 0;
		return(fsp_reserve_free_pages(space, space_header, size, mtr));
	}

	n_free_list_ext = flst_get_len(space_header + FSP_FREE);

	free_limit = mtr_read_uint32(space_header + FSP_FREE_LIMIT, MLOG_4BYTES, mtr);

	/* Below we play safe when counting free extents above the free limit:
	some of them will contain extent descriptor pages, and therefore
	will not be free extents */

	n_free_up = (size - free_limit) / FSP_EXTENT_SIZE;

	if (n_free_up > 0) {
		n_free_up--;
		//if (!zip_size) {
		//	n_free_up -= n_free_up / (UNIV_PAGE_SIZE / FSP_EXTENT_SIZE);
		//} else {
			n_free_up -= n_free_up / (page_size.physical() / FSP_EXTENT_SIZE);
		//}
	}

	n_free = n_free_list_ext + n_free_up;

	if (alloc_type == FSP_NORMAL) {
		/* We reserve 1 extent + 0.5 % of the space size to undo logs
		and 1 extent + 0.5 % to cleaning operations; NOTE: this source
		code is duplicated in the function below! */
		reserve = 2 + ((size / FSP_EXTENT_SIZE) * 2) / 200;
		if (n_free <= reserve + n_ext) {
			goto try_to_extend;
		}
	} else if (alloc_type == FSP_UNDO) {
		/* We reserve 0.5 % of the space size to cleaning operations */
		reserve = 1 + ((size / FSP_EXTENT_SIZE) * 1) / 200;
		if (n_free <= reserve + n_ext) {
			goto try_to_extend;
		}
	} else {
		ut_a(alloc_type == FSP_CLEANING);
	}

	success = fil_space_reserve_free_extents(space, n_free, n_ext);

	if (success) {
		return(TRUE);
	}

try_to_extend:

    fil_space_t *fil_space = fil_space_get_by_id(space);
	success = fsp_try_extend_data_file(&n_pages_added, fil_space, space_header, mtr);
	if (success && n_pages_added > 0) {
		goto try_again;
	}

	return(FALSE);
}

// Sets the page number in the nth fragment page slot
void fseg_set_nth_frag_page_no(
    fseg_inode_t *inode,  /*!< in: segment inode */
    uint32        n,      /*!< in: slot index */
    uint32        page_no,/*!< in: page number to set */
    mtr_t        *mtr)    /*!< in/out: mini-transaction */
{
    ut_ad(inode && mtr);
    ut_ad(n < FSEG_FRAG_ARR_N_SLOTS);
    ut_ad(mtr_memo_contains_page(mtr, inode, MTR_MEMO_PAGE_X_FIX));
    ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

    mlog_write_uint32(inode + FSEG_FRAG_ARR + n * FSEG_FRAG_SLOT_SIZE, page_no, MLOG_4BYTES, mtr);
}


// Creates a new segment.
// return the block where the segment header is placed, x-latched,
// return NULL if could not create segment because of lack of space
buf_block_t* fseg_create_general(
	uint32	space,	/*!< in: space id */
	uint32	page,	/*!< in: page where the segment header is placed: if
			this is != 0, the page must belong to another segment,
			if this is 0, a new page will be allocated and it
			will belong to the created segment */
	uint32	byte_offset, /*!< in: byte offset of the created segment header
			on the page */
	bool32	has_done_reservation, /*!< in: TRUE if the caller has already
			done the reservation for the pages with
			fsp_reserve_free_extents (at least 2 extents: one for
			the inode and the other for the segment) then there is
			no need to do the check for this individual
			operation */
	mtr_t*	mtr)	/*!< in/out: mini-transaction */
{
	uint32		flags;
	fsp_header_t*	space_header;
	fseg_inode_t*	inode;
	ib_id_t		seg_id;
	buf_block_t*	block	= 0; /* remove warning */
	fseg_header_t*	header	= 0; /* remove warning */
	rw_lock_t*	latch;
	bool32		success;
	uint32		n_reserved;
	uint32		i;

	ut_ad(mtr);
	ut_ad(byte_offset + FSEG_HEADER_SIZE <= UNIV_PAGE_SIZE - FIL_PAGE_DATA_END);

	latch = fil_space_get_latch(space, &flags);
	//zip_size = UNIV_PAGE_SIZE;
    page_id_t page_id(space, page);
    const page_size_t page_size(0);

	if (page != 0) {
		block = buf_page_get(&page_id, page_size, RW_X_LATCH, mtr);
		header = byte_offset + buf_block_get_frame(block);
	}

	mtr_x_lock(latch, mtr);

	if (rw_lock_get_x_lock_count(latch) == 1) {
		/* This thread did not own the latch before this call:
           free excess pages from the insert buffer free list */
		//if (space == IBUF_SPACE_ID) {
		//	ibuf_free_excess_pages();
		//}
	}

	if (!has_done_reservation) {
		success = fsp_reserve_free_extents(&n_reserved, space, 2, FSP_NORMAL, mtr);
		if (!success) {
			return(NULL);
		}
	}

	space_header = fsp_get_space_header(space, page_size, mtr);
	inode = fsp_alloc_seg_inode(space_header, mtr);
	if (inode == NULL) {
		goto funct_exit;
	}

	/* Read the next segment id from space header and increment the
	value in space header */

	seg_id = mach_read_from_8(space_header + FSP_SEG_ID);
	mlog_write_uint64(space_header + FSP_SEG_ID, seg_id + 1, mtr);
	mlog_write_uint64(inode + FSEG_ID, seg_id, mtr);
	mlog_write_uint32(inode + FSEG_NOT_FULL_N_USED, 0, MLOG_4BYTES, mtr);

	flst_init(inode + FSEG_FREE, mtr);
	flst_init(inode + FSEG_NOT_FULL, mtr);
	flst_init(inode + FSEG_FULL, mtr);

	mlog_write_uint32(inode + FSEG_MAGIC_N, FSEG_MAGIC_N_VALUE, MLOG_4BYTES, mtr);
	for (i = 0; i < FSEG_FRAG_ARR_N_SLOTS; i++) {
		fseg_set_nth_frag_page_no(inode, i, FIL_NULL, mtr);
	}

	if (page == 0) {
		block = fseg_alloc_free_page_low(space, page_size.physical(), inode, 0, FSP_UP, mtr, mtr);
		if (block == NULL) {
			fsp_free_seg_inode(space, page_size.physical(), inode, mtr);
			goto funct_exit;
		}

		ut_ad(rw_lock_get_x_lock_count(&block->rw_lock) == 1);

		header = byte_offset + buf_block_get_frame(block);
		mlog_write_uint32(buf_block_get_frame(block) + FIL_PAGE_TYPE, FIL_PAGE_TYPE_SYS, MLOG_2BYTES, mtr);
	}

	mlog_write_uint32(header + FSEG_HDR_OFFSET, page_offset(inode), MLOG_2BYTES, mtr);
	mlog_write_uint32(header + FSEG_HDR_PAGE_NO, page_get_page_no(page_align(inode)), MLOG_4BYTES, mtr);
	mlog_write_uint32(header + FSEG_HDR_SPACE, space, MLOG_4BYTES, mtr);

funct_exit:
	if (!has_done_reservation) {
		fil_space_release_free_extents(space, n_reserved);
	}

	return(block);
}

