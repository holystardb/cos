#include "knl_fsp.h"
#include "cm_util.h"
#include "knl_buf.h"
#include "knl_mtr.h"
#include "knl_flst.h"
#include "knl_page.h"
#include "knl_dict.h"

/* The file system. This variable is NULL before the module is initialized. */
fil_system_t* fil_system = NULL;

/** The null file address */
fil_addr_t	fil_addr_null = {FIL_NULL, 0};



uint32 xdes_calc_descriptor_page(const page_size_t& page_size, uint32 offset);

byte *fut_get_ptr(space_id_t space, const page_size_t &page_size,
                  fil_addr_t addr, rw_lock_type_t rw_latch, mtr_t *mtr,
                  buf_block_t **ptr_block)
{
    buf_block_t *block;
    byte *ptr;
    
    ut_ad(addr.boffset < UNIV_PAGE_SIZE);
    ut_ad((rw_latch == RW_S_LATCH) || (rw_latch == RW_X_LATCH) ||
          (rw_latch == RW_SX_LATCH));
    
    block = buf_page_get(page_id_t(space, addr.page), page_size, rw_latch, mtr);
    ptr = buf_block_get_frame(block) + addr.boffset;

    if (ptr_block != NULL) {
      *ptr_block = block;
    }

	return(ptr);
}





/**********************************************************************//**
Sets a descriptor bit of a page. */
void xdes_set_bit(
	xdes_t*	descr,	/*!< in: descriptor */
	uint32	bit,	/*!< in: XDES_FREE_BIT or XDES_CLEAN_BIT */
	uint32	offset,	/*!< in: page offset within extent: 0 ... FSP_EXTENT_SIZE - 1 */
	bool32	val,	/*!< in: bit value */
	mtr_t*	mtr)	/*!< in/out: mini-transaction */
{
	uint32	index;
	uint32	byte_index;
	uint32	bit_index;
	uint32	descr_byte;

	ut_ad(mtr_memo_contains_page(mtr, descr, MTR_MEMO_PAGE_SX_FIX));
	ut_ad((bit == XDES_FREE_BIT) || (bit == XDES_CLEAN_BIT));
	ut_ad(offset < FSP_EXTENT_SIZE);

	index = bit + XDES_BITS_PER_PAGE * offset;

	byte_index = index / 8;
	bit_index = index % 8;

	descr_byte = mach_read_from_1(descr + XDES_BITMAP + byte_index);
	descr_byte = ut_bit32_set_nth(descr_byte, bit_index, val);

	mlog_write_uint32(descr + XDES_BITMAP + byte_index, descr_byte,
			 MLOG_1BYTE, mtr);
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
    ut_ad(mtr_memo_contains_page(mtr, descr, MTR_MEMO_PAGE_SX_FIX));

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
	ut_ad(mtr_memo_contains_page(mtr, descr, MTR_MEMO_PAGE_SX_FIX));
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

/**********************************************************************//**
Returns the number of used pages in a descriptor.
@return number of pages used */
uint32 xdes_get_n_used(
	const xdes_t*	descr,	/*!< in: descriptor */
	mtr_t*		mtr)	/*!< in/out: mini-transaction */
{
	uint32	count	= 0;

	ut_ad(descr && mtr);
	ut_ad(mtr_memo_contains_page(mtr, descr, MTR_MEMO_PAGE_SX_FIX));
	for (uint32 i = 0; i < FSP_EXTENT_SIZE; ++i) {
		if (FALSE == xdes_mtr_get_bit(descr, XDES_FREE_BIT, i, mtr)) {
			count++;
		}
	}

	return(count);
}

/**********************************************************************//**
Returns true if extent contains no used pages.
@return TRUE if totally free */
bool32 xdes_is_free(
	const xdes_t*	descr,	/*!< in: descriptor */
	mtr_t*		mtr)	/*!< in/out: mini-transaction */
{
	if (0 == xdes_get_n_used(descr, mtr)) {

		return(TRUE);
	}

	return(FALSE);
}

/**********************************************************************//**
Returns true if extent contains no free pages.
@return TRUE if full */
bool32 xdes_is_full(
	const xdes_t*	descr,	/*!< in: descriptor */
	mtr_t*		mtr)	/*!< in/out: mini-transaction */
{
	if (FSP_EXTENT_SIZE == xdes_get_n_used(descr, mtr)) {

		return(TRUE);
	}

	return(FALSE);
}

/**********************************************************************//**
Sets the state of an xdes. */
void xdes_set_state(
	xdes_t*	descr,	/*!< in/out: descriptor */
	uint32	state,	/*!< in: state to set */
	mtr_t*	mtr)	/*!< in/out: mini-transaction */
{
	ut_ad(descr && mtr);
	ut_ad(state >= XDES_FREE);
	ut_ad(state <= XDES_FSEG);
	ut_ad(mtr_memo_contains_page(mtr, descr, MTR_MEMO_PAGE_SX_FIX));

	mlog_write_uint32(descr + XDES_STATE, state, MLOG_4BYTES, mtr);
}

/**********************************************************************//**
Gets the state of an xdes.
@return state */
uint32 xdes_get_state(
	const xdes_t*	descr,	/*!< in: descriptor */
	mtr_t*		mtr)	/*!< in/out: mini-transaction */
{
	uint32	state;

	ut_ad(descr && mtr);
	ut_ad(mtr_memo_contains_page(mtr, descr, MTR_MEMO_PAGE_SX_FIX));

	state = mach_read_from_4(descr + XDES_STATE);
	ut_ad(state - 1 < XDES_FSEG);
	return(state);
}

/**********************************************************************//**
Inits an extent descriptor to the free and clean state. */
void xdes_init(
	xdes_t*	descr,	/*!< in: descriptor */
	mtr_t*	mtr)	/*!< in/out: mini-transaction */
{
	uint32	i;

	ut_ad(descr && mtr);
	ut_ad(mtr_memo_contains_page(mtr, descr, MTR_MEMO_PAGE_SX_FIX));
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
	bool		init_space = false,
	buf_block_t**	desc_block = NULL)
{
	uint32	limit;
	uint32	size;
	uint32	descr_page_no;
	uint32	flags;
	page_t*	descr_page;

	ut_ad(mtr_memo_contains_page(mtr, sp_header, MTR_MEMO_PAGE_SX_FIX));
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

	buf_block_t*		block;

	if (descr_page_no == 0) {
		/* It is on the space header page */

		descr_page = page_align(sp_header);
		block = NULL;
	} else {
		block = buf_page_get(
			page_id_t(space, descr_page_no), page_size,
			RW_SX_LATCH, mtr);

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

	block = buf_page_get(page_id_t(space_id, 0), page_size, RW_SX_LATCH, mtr);

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
	descr = fut_get_ptr(space, page_size, lst_node, RW_SX_LATCH, mtr, NULL) - XDES_FLST_NODE;

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








bool32 fil_system_init(memory_pool_t *pool, uint32 max_n_open, uint32 space_max_count, uint32 fil_node_max_count)
{
    if (fil_node_max_count > 0xFFFFFF || space_max_count > 0xFFFFFF) {
        return FALSE;
    }
    fil_node_max_count = fil_node_max_count < 0xFFF ? 0xFFF : fil_node_max_count;
    space_max_count = space_max_count < 0xFFF ? 0xFFF : space_max_count;

    fil_system = (fil_system_t *)malloc(ut_align8(sizeof(fil_system_t)) +
                                        fil_node_max_count * ut_align8(sizeof(fil_node_t *)));
    if (fil_system == NULL) {
        return FALSE;
    }

    fil_system->mem_context = mcontext_create(pool);
    fil_system->open_pending_num = 0;
    fil_system->max_n_open = max_n_open;
    fil_system->space_max_count = space_max_count;
    fil_system->fil_node_num = 0;
    fil_system->fil_node_max_count = fil_node_max_count;
    fil_system->fil_nodes = (fil_node_t **)((char *)fil_system + ut_align8(sizeof(fil_system_t)));
    memset(fil_system->fil_nodes, 0x00, fil_node_max_count * ut_align8(sizeof(fil_node_t *)));
    spin_lock_init(&fil_system->lock);
    fil_system->spaces =  HASH_TABLE_CREATE(space_max_count);
    fil_system->name_hash = HASH_TABLE_CREATE(space_max_count);
    UT_LIST_INIT(fil_system->fil_spaces);
    UT_LIST_INIT(fil_system->fil_node_lru);

    return TRUE;
}

fil_space_t* fil_space_create(char *name, uint32 space_id, uint32 purpose)
{
    fil_space_t *space, *tmp;
    uint32 size = ut_align8(sizeof(fil_space_t)) + (uint32)strlen(name) + 1;
    space = (fil_space_t *)my_malloc(fil_system->mem_context, size);
    if (space) {
        space->name = (char *)space + ut_align8(sizeof(fil_space_t));
        strncpy_s(space->name, strlen(name) + 1, name, strlen(name));
        space->name[strlen(name) + 1] = 0;

        space->id = space_id;
        space->purpose = purpose;
        space->page_size = 0;
        space->n_reserved_extents = 0;
        space->magic_n = M_FIL_SPACE_MAGIC_N;
        spin_lock_init(&space->lock);
        rw_lock_create(&space->latch);
        space->io_in_progress = 0;
        UT_LIST_INIT(space->fil_nodes);
        UT_LIST_INIT(space->free_pages);

        spin_lock(&fil_system->lock, NULL);
        tmp = UT_LIST_GET_FIRST(fil_system->fil_spaces);
        while (tmp) {
            if (strcmp(tmp->name, name) == 0 || tmp->id == space_id) {
                break;
            }
            tmp = UT_LIST_GET_NEXT(list_node, tmp);
        }
        if (tmp == NULL) {
            UT_LIST_ADD_LAST(list_node, fil_system->fil_spaces, space);
        }
        HASH_INSERT(fil_space_t, hash, fil_system->spaces, space_id, space);
        //HASH_INSERT(fil_space_t, name_hash, fil_system->name_hash, ut_fold_string(name), space);

        spin_unlock(&fil_system->lock);

        if (tmp) { // name or spaceid already exists
            my_free((void *)space);
            space = NULL;
        }
    }

    return space;
}

void fil_space_destroy(uint32 space_id)
{
    fil_space_t *space;
    fil_node_t *fil_node;
    UT_LIST_BASE_NODE_T(fil_node_t) fil_node_list;

    mutex_enter(&fil_system->lock, NULL);
    space = fil_space_get_by_id(space_id);
    ut_a(space->magic_n == M_FIL_SPACE_MAGIC_N);
    mutex_exit(&fil_system->lock);

    spin_lock(&space->lock, NULL);
    fil_node_list.count = space->fil_nodes.count;
    fil_node_list.start = space->fil_nodes.start;
    fil_node_list.end = space->fil_nodes.end;
    UT_LIST_INIT(space->fil_nodes);
    spin_unlock(&space->lock);

    spin_lock(&fil_system->lock, NULL);
    fil_node = UT_LIST_GET_FIRST(fil_node_list);
    while (fil_node != NULL) {
        if (fil_node->is_open) { /* The node is in the LRU list, remove it */
            UT_LIST_REMOVE(lru_list_node, fil_system->fil_node_lru, fil_node);
            fil_node->is_open = FALSE;
        }
        fil_system->fil_nodes[fil_node->id] = NULL;
        fil_node = UT_LIST_GET_NEXT(chain_list_node, fil_node);
    }
    spin_unlock(&fil_system->lock);

    fil_node = UT_LIST_GET_FIRST(fil_node_list);
    while (fil_node != NULL) {
        os_close_file(fil_node->handle);

        UT_LIST_REMOVE(chain_list_node, fil_node_list, fil_node);
        my_free((void *)fil_node);

        fil_node = UT_LIST_GET_FIRST(fil_node_list);
    }

    my_free((void *)space);
}

fil_node_t* fil_node_create(fil_space_t *space, char *name, uint32 page_max_count, uint32 page_size, bool32 is_extend)
{
    fil_node_t *node, *tmp;

    spin_lock(&fil_system->lock, NULL);
    if (fil_system->fil_node_num >= fil_system->fil_node_max_count) {
        spin_unlock(&fil_system->lock);
        return NULL;
    }
    fil_system->fil_node_num++;
    spin_unlock(&fil_system->lock);

    node = (fil_node_t *)my_malloc(fil_system->mem_context, ut_align8(sizeof(fil_node_t)) + (uint32)strlen(name) + 1);
    if (node) {
        node->name = (char *)node + ut_align8(sizeof(fil_node_t));
        strcpy_s(node->name, strlen(name) + 1, name);
        node->name[strlen(name) + 1] = 0;

        node->page_max_count = page_max_count;
        node->page_size = page_size;
        node->page_hwm = 0;
        node->handle = OS_FILE_INVALID_HANDLE;
        node->magic_n = M_FIL_NODE_MAGIC_N;
        node->is_open = 0;
        node->is_extend = is_extend;
        node->n_pending = 0;

        spin_lock(&fil_system->lock, NULL);
        for (uint32 i = 0; i < fil_system->fil_node_max_count; i++) {
            if (fil_system->fil_nodes[i] == NULL) {
                node->id = i;
                fil_system->fil_nodes[i] = node;
                break;
            }
        }
        spin_unlock(&fil_system->lock);

        spin_lock(&space->lock, NULL);
        tmp = UT_LIST_GET_FIRST(space->fil_nodes);
        while (tmp) {
            if (strcmp(tmp->name, name) == 0) {
                break;
            }
            tmp = UT_LIST_GET_NEXT(chain_list_node, tmp);
        }
        if (tmp == NULL) { // name not already exists
            UT_LIST_ADD_LAST(chain_list_node, space->fil_nodes, node);
        }
        spin_unlock(&space->lock);

        if (tmp) {
            spin_lock(&fil_system->lock, NULL);
            fil_system->fil_nodes[node->id] = NULL;
            fil_system->fil_node_num--;
            spin_unlock(&fil_system->lock);
            my_free((void *)node);
            node = NULL;
        };
    }

    return node;
}

bool32 fil_node_destroy(fil_space_t *space, fil_node_t *node)
{
    fil_node_close(space, node);

    fil_page_t *page, *tmp;
    spin_lock(&space->lock, NULL);
    UT_LIST_ADD_LAST(chain_list_node, space->fil_nodes, node);
    //
    page = UT_LIST_GET_FIRST(space->free_pages);
    while (page) {
        tmp = UT_LIST_GET_NEXT(list_node, page);
        if (page->file == node->id) {
            UT_LIST_REMOVE(list_node, space->free_pages, page);
        }
        page = tmp;
    }
    spin_unlock(&space->lock);

    spin_lock(&fil_system->lock, NULL);
    fil_system->fil_nodes[node->id] = NULL;
    fil_system->fil_node_num--;
    spin_unlock(&fil_system->lock);

    os_del_file(node->name);

    my_free((void *)node);

    return TRUE;
}

bool32 fil_node_open(fil_space_t *space, fil_node_t *node)
{
    os_file_t handle;
    fil_node_t *last_node;

    if (node->is_open) {
        return TRUE;
    }

    /* File is closed */
    /* If too many files are open, close one */
    spin_lock(&fil_system->lock, NULL);
    while (fil_system->open_pending_num + UT_LIST_GET_LEN(fil_system->fil_node_lru) + 1 >= fil_system->max_n_open) {
        last_node = UT_LIST_GET_LAST(fil_system->fil_node_lru);
        if (last_node == NULL) {
            fprintf(stderr,
                    "Error: cannot close any file to open another for i/o\n"
                    "Pending i/o's on %lu files exist\n",
                    fil_system->open_pending_num);
            ut_a(0);
        }
        handle = last_node->handle;
        last_node->handle = OS_FILE_INVALID_HANDLE;
        UT_LIST_REMOVE(lru_list_node, fil_system->fil_node_lru, node);
        node->is_open = FALSE;
        spin_unlock(&fil_system->lock);

        if (!os_close_file(handle)) {
            fprintf(stderr,
                    "Error: cannot close any file to open another for i/o\n"
                    "Pending i/o's on %lu files exist\n",
                    fil_system->open_pending_num);
            ut_a(0);
        }

        spin_lock(&fil_system->lock, NULL);
    }
    spin_unlock(&fil_system->lock);

    if (!os_open_file(node->name, 0, 0, &node->handle)) {
        return FALSE;
    }

    spin_lock(&fil_system->lock, NULL);
    node->is_open = TRUE;
    UT_LIST_ADD_LAST(lru_list_node, fil_system->fil_node_lru, node);
    spin_unlock(&fil_system->lock);

    return TRUE;
}

bool32 fil_node_close(fil_space_t *space, fil_node_t *node)
{
    bool32 ret;

    ut_a(node->is_open);
    ut_a(node->n_pending == 0);

    ret = os_close_file(node->handle);
    ut_a(ret);

    /* The node is in the LRU list, remove it */
    spin_lock(&fil_system->lock, NULL);
    if (node->is_open) {
        UT_LIST_REMOVE(lru_list_node, fil_system->fil_node_lru, node);
        node->is_open = FALSE;
    }
    spin_unlock(&fil_system->lock);

    return TRUE;
}


bool32 fil_space_extend_datafile(fil_space_t *space, bool32 need_redo)
{
    fil_node_t *node;
    uint32 total_count = 100;
    uint32 count = 0;

    if (total_count == 0) {
        return TRUE;
    }

    spin_lock(&space->lock, NULL);
    node = UT_LIST_GET_FIRST(space->fil_nodes);
    while (node) {
        if (node->page_hwm < node->page_max_count) {
            if (node->page_max_count - node->page_hwm >= total_count) {
                node->page_hwm += total_count;
                count = total_count;
            } else {
                node->page_hwm = node->page_max_count;
                total_count -= node->page_max_count - node->page_hwm;
                count = node->page_max_count - node->page_hwm;
            }
            break;
        }
        node = UT_LIST_GET_NEXT(chain_list_node, node);
    }
    spin_unlock(&space->lock);

    if (count > 0 && node) {
        
    }

    return TRUE;
}

// Tries to extend the last data file of a tablespace if it is auto-extending.
static bool32 fsp_try_extend_data_file(
	uint32*		actual_increase,/*!< out: actual increase in pages, where
					we measure the tablespace size from
					what the header field says; it may be
					the actual file size rounded down to
					megabyte */
    fil_space_t* space,		/*!< in: space */
	fsp_header_t*	header,		/*!< in/out: space header */
	mtr_t*		mtr)
{
    return FALSE;
}

fil_space_t* fil_space_get_by_id(uint32 space_id)
{
    fil_space_t*	space;

    ut_ad(mutex_own(&fil_system->lock));

    HASH_SEARCH(hash, fil_system->spaces, space_id,
        fil_space_t*, space,
        ut_ad(space->magic_n == M_FIL_SPACE_MAGIC_N),
        space->id == space_id);

    return space;
}

rw_lock_t* fil_space_get_latch(uint32 space_id, uint32 *flags)
{
    fil_space_t *space;

    ut_ad(fil_system);

    mutex_enter(&fil_system->lock);

    space = fil_space_get_by_id(space_id);

    ut_a(space);

    if (flags) {
        *flags = space->flags;
    }

    mutex_exit(&fil_system->lock);

    return(&(space->latch));
}





bool fil_addr_is_null(fil_addr_t addr) /*!< in: address */
{
    return(addr.page == FIL_NULL);
}


/********************************************************************//**
NOTE: you must call fil_mutex_enter_and_prepare_for_io() first!

Prepares a file node for i/o. Opens the file if it is closed. Updates the
pending i/o's field in the node and the system appropriately. Takes the node
off the LRU list if it is in the LRU list. The caller must hold the fil_sys
mutex.
@return false if the file can't be opened, otherwise true */
static bool32 fil_node_prepare_for_io(
    fil_node_t* node, /*!< in: file node */
    fil_system_t* system, /*!< in: tablespace memory cache */
    fil_space_t* space) /*!< in: space */
{

}


/********************************************************************//**
Updates the data structures when an i/o operation finishes. Updates the
pending i/o's field in the node appropriately. */
static void fil_node_complete_io(
    fil_node_t* node, /*!< in: file node */
    fil_system_t* system, /*!< in: tablespace memory cache */
    uint32 type) /*!< in: OS_FILE_WRITE or OS_FILE_READ; marks the node as modified if type == OS_FILE_WRITE */
{
}

#if 0
/********************************************************************//**
Reads or writes data. This operation is asynchronous (aio).
@return DB_SUCCESS, or DB_TABLESPACE_DELETED if we are trying to do i/o on a tablespace which does not exist */
dberr_t fil_io(
	uint32	type,		/*!< in: OS_FILE_READ or OS_FILE_WRITE,
				ORed to OS_FILE_LOG, if a log i/o
				and ORed to OS_AIO_SIMULATED_WAKE_LATER
				if simulated aio and we want to post a
				batch of i/os; NOTE that a simulated batch
				may introduce hidden chances of deadlocks,
				because i/os are not actually handled until
				all have been posted: use with great
				caution! */
	bool	sync,		/*!< in: true if synchronous aio is desired */
	uint32	space_id,	/*!< in: space id */
	uint32	zip_size,	/*!< in: compressed page size in bytes;
				0 for uncompressed pages */
	uint32	block_offset,	/*!< in: offset in number of blocks */
	uint32	byte_offset,	/*!< in: remainder of offset in bytes; in
				aio this must be divisible by the OS block
				size */
	uint32	len,		/*!< in: how many bytes to read or write; this
				must not cross a file boundary; in aio this
				must be a block size multiple */
	void*	buf,		/*!< in/out: buffer where to store read data
				or from where to write; in aio this must be
				appropriately aligned */
	void*	message)	/*!< in: message for aio handler if non-sync
				aio used, else ignored */
{
	uint32		mode;
	fil_space_t*	space;
	fil_node_t*	node;
	bool32		ret;
	uint32		is_log;
	uint32		wake_later;
	uint64	offset;
	bool32		ignore_nonexistent_pages;

	is_log = type & OS_FILE_LOG;
	type = type & ~OS_FILE_LOG;

	wake_later = type & OS_AIO_SIMULATED_WAKE_LATER;
	type = type & ~OS_AIO_SIMULATED_WAKE_LATER;

	ignore_nonexistent_pages = type & BUF_READ_IGNORE_NONEXISTENT_PAGES;
	type &= ~BUF_READ_IGNORE_NONEXISTENT_PAGES;

	ut_ad(byte_offset < UNIV_PAGE_SIZE);
	ut_ad(!zip_size || !byte_offset);
	ut_ad(ut_is_2pow(zip_size));
	ut_ad(buf);
	ut_ad(len > 0);
	ut_ad(UNIV_PAGE_SIZE == (ulong)(1 << UNIV_PAGE_SIZE_SHIFT));

	//ut_ad(fil_validate_skip());
#ifndef UNIV_HOTBACKUP
# ifndef UNIV_LOG_DEBUG
	/* ibuf bitmap pages must be read in the sync aio mode: */
	ut_ad(recv_no_ibuf_operations
	      || type == OS_FILE_WRITE
	      || !ibuf_bitmap_page(zip_size, block_offset)
	      || sync
	      || is_log);
# endif /* UNIV_LOG_DEBUG */
	if (sync) {
		mode = OS_AIO_SYNC;
	} else if (is_log) {
		mode = OS_AIO_LOG;
	} else if (type == OS_FILE_READ
		   && !recv_no_ibuf_operations
		   && ibuf_page(space_id, zip_size, block_offset, NULL)) {
		mode = OS_AIO_IBUF;
	} else {
		mode = OS_AIO_NORMAL;
	}
#else /* !UNIV_HOTBACKUP */
	ut_a(sync);
	mode = OS_AIO_SYNC;
#endif /* !UNIV_HOTBACKUP */

	if (type == OS_FILE_READ) {
		srv_stats.data_read.add(len);
	} else if (type == OS_FILE_WRITE) {
		ut_ad(!srv_read_only_mode);
		srv_stats.data_written.add(len);
	}

	/* Reserve the fil_system mutex and make sure that we can open at
	least one file while holding it, if the file is not already open */

	fil_mutex_enter_and_prepare_for_io(space_id);

	space = fil_space_get_by_id(space_id);

	/* If we are deleting a tablespace we don't allow any read
	operations on that. However, we do allow write operations. */
	if (space == 0 || (type == OS_FILE_READ && space->stop_new_ops)) {
		mutex_exit(&fil_system->mutex);

		ib_logf(IB_LOG_LEVEL_ERROR,
			"Trying to do i/o to a tablespace which does "
			"not exist. i/o type %lu, space id %lu, "
			"page no. %lu, i/o length %lu bytes",
			(ulong) type, (ulong) space_id, (ulong) block_offset,
			(ulong) len);

		return(DB_TABLESPACE_DELETED);
	}

	ut_ad(mode != OS_AIO_IBUF || space->purpose == FIL_TABLESPACE);

	node = UT_LIST_GET_FIRST(space->chain);

	for (;;) {
		if (node == NULL) {
			if (ignore_nonexistent_pages) {
				mutex_exit(&fil_system->mutex);
				return(DB_ERROR);
			}

			fil_report_invalid_page_access(
				block_offset, space_id, space->name,
				byte_offset, len, type);

			ut_error;

		} else if (fil_is_user_tablespace_id(space->id)
			   && node->size == 0) {

			/* We do not know the size of a single-table tablespace
			before we open the file */
			break;
		} else if (node->size > block_offset) {
			/* Found! */
			break;
		} else {
			block_offset -= node->size;
			node = UT_LIST_GET_NEXT(chain, node);
		}
	}

	/* Open file if closed */
	if (!fil_node_prepare_for_io(node, fil_system, space)) {
		if (space->purpose == FIL_TABLESPACE
		    && fil_is_user_tablespace_id(space->id)) {
			mutex_exit(&fil_system->mutex);

			ib_logf(IB_LOG_LEVEL_ERROR,
				"Trying to do i/o to a tablespace which "
				"exists without .ibd data file. "
				"i/o type %lu, space id %lu, page no %lu, "
				"i/o length %lu bytes",
				(ulong) type, (ulong) space_id,
				(ulong) block_offset, (ulong) len);

			return(DB_TABLESPACE_DELETED);
		}

		/* The tablespace is for log. Currently, we just assert here
		to prevent handling errors along the way fil_io returns.
		Also, if the log files are missing, it would be hard to
		promise the server can continue running. */
		ut_a(0);
	}

	/* Check that at least the start offset is within the bounds of a
	single-table tablespace, including rollback tablespaces. */
	if (UNIV_UNLIKELY(node->size <= block_offset)
	    && space->id != 0 && space->purpose == FIL_TABLESPACE) {

		fil_report_invalid_page_access(
			block_offset, space_id, space->name, byte_offset,
			len, type);

		ut_error;
	}

	/* Now we have made the changes in the data structures of fil_system */
	mutex_exit(&fil_system->mutex);

	/* Calculate the low 32 bits and the high 32 bits of the file offset */
	offset = ((uint64) block_offset << UNIV_PAGE_SIZE_SHIFT) + byte_offset;

	ut_a(node->size - block_offset  >= ((byte_offset + len + (UNIV_PAGE_SIZE - 1)) / UNIV_PAGE_SIZE));


	/* Do aio */

	ut_a(byte_offset % OS_FILE_LOG_BLOCK_SIZE == 0);
	ut_a((len % OS_FILE_LOG_BLOCK_SIZE) == 0);

#ifdef UNIV_HOTBACKUP
	/* In ibbackup do normal i/o, not aio */
	if (type == OS_FILE_READ) {
		ret = os_file_read(node->handle, buf, offset, len);
	} else {
		ut_ad(!srv_read_only_mode);
		ret = os_file_write(node->name, node->handle, buf,
				    offset, len);
	}
#else
	/* Queue the aio request */
	ret = os_aio(type, mode | wake_later, node->name, node->handle, buf,
		     offset, len, node, message);
#endif /* UNIV_HOTBACKUP */
	ut_a(ret);

	if (mode == OS_AIO_SYNC) {
		/* The i/o operation is already completed when we return from
		os_aio: */

		mutex_enter(&fil_system->mutex);

		fil_node_complete_io(node, fil_system, type);

		mutex_exit(&fil_system->mutex);

		ut_ad(fil_validate_skip());
	}

	return(DB_SUCCESS);
}
#endif

dberr_t fil_read(
    bool32 sync, /*!< in: true if synchronous aio is desired */
    uint32 space_id, /*!< in: space id */
    uint32 zip_size, /*!< in: compressed page size in bytes; 0 for uncompressed pages */
    uint32 block_offset, /*!< in: offset in number of blocks */
    uint32 byte_offset, /*!< in: remainder of offset in bytes;
                                 in aio this must be divisible by the OS block size */
    uint32 len, /*!< in: how many bytes to read; this must not cross a file boundary;
                         in aio this must be a block size multiple */
    void*  buf, /*!< in/out: buffer where to store data read */
    void*  message) /*!< in: message for aio handler if non-sync aio used, else ignored */
{
    //return(fil_io(OS_FILE_READ, sync, space_id, zip_size, block_offset,
	//				  byte_offset, len, buf, message));
    return DB_SUCCESS;
}

dberr_t fil_write(
    bool32 sync, /*!< in: true if synchronous aio is desired */
    uint32 space_id, /*!< in: space id */
    uint32 zip_size, /*!< in: compressed page size in bytes; 0 for uncompressed pages */
    uint32 block_offset, /*!< in: offset in number of blocks */
    uint32 byte_offset, /*!< in: remainder of offset in bytes;
                                 in aio this must be divisible by the OS block size */
    uint32 len, /*!< in: how many bytes to read; this must not cross a file boundary;
                         in aio this must be a block size multiple */
    void*  buf, /*!< in/out: buffer where to store data read */
    void*  message) /*!< in: message for aio handler if non-sync aio used, else ignored */
{
    //return(fil_io(OS_FILE_WRITE, sync, space_id, zip_size, block_offset,
	//				  byte_offset, len, buf, message));
    return DB_SUCCESS;
}


/*============================ FILE I/O ================================*/


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


/** Put new extents to the free list if there are free extents above the free
limit. If an extent happens to contain an extent descriptor page, the extent
is put to the FSP_FREE_FRAG list with the page marked as used.
@param[in]	init_space	true if this is a single-table tablespace
and we are only initializing the first extent and the first bitmap pages;
then we will not allocate more extents
@param[in,out]	space		tablespace
@param[in,out]	header		tablespace header
@param[in,out]	mtr		mini-transaction */
static void fsp_fill_free_list(bool32 init_space, fil_space_t* space, fsp_header_t* header, mtr_t* mtr)
{
    uint32 limit;
    uint32 size;
    uint32 flags;
    xdes_t* descr;
    uint32 count = 0;
    uint32 frag_n_used;
    uint32 actual_increase;
    uint32 i;

    ut_ad(page_offset(header) == FSP_HEADER_OFFSET);
    ut_d(fsp_space_modify_check(space->id, mtr));

    /* Check if we can fill free list from above the free list limit */
    size = mach_read_from_4(header + FSP_SIZE);
    limit = mach_read_from_4(header + FSP_FREE_LIMIT);
    flags = mach_read_from_4(header + FSP_SPACE_FLAGS);

    //ut_ad(size == space->size_in_header);
    //ut_ad(limit == space->free_limit);
    //ut_ad(flags == space->flags);

    const page_size_t page_size(flags);

    if (space == 0 && srv_auto_extend_last_data_file && size < limit + FSP_EXTENT_SIZE * FSP_FREE_ADD) {
        /* Try to increase the last data file size */
        fsp_try_extend_data_file(&actual_increase, space, header, mtr);
        size = mtr_read_uint32(header + FSP_SIZE, MLOG_4BYTES, mtr);
    }

    if (space != 0 && !init_space && size < limit + FSP_EXTENT_SIZE * FSP_FREE_ADD) {
        /* Try to increase the .ibd file size */
        fsp_try_extend_data_file(&actual_increase, space, header, mtr);
        size = mtr_read_uint32(header + FSP_SIZE, MLOG_4BYTES, mtr);
    }

    i = limit;
    while ((init_space && i < 1) || ((i + FSP_EXTENT_SIZE <= size) && (count < FSP_FREE_ADD))) {
        bool32 init_xdes = (ut_2pow_remainder(i, page_size.physical()) == 0);
        space->free_limit = i + FSP_EXTENT_SIZE;
        mlog_write_uint32(header + FSP_FREE_LIMIT, i + FSP_EXTENT_SIZE, MLOG_4BYTES, mtr);

        if (init_xdes) {
            buf_block_t* block;

            /* We are going to initialize a new descriptor page 
            and a new ibuf bitmap page: the prior contents of the pages should be ignored. */

            if (i > 0) {
                const page_id_t page_id(space->id, i);
                //block = buf_page_create(page_id, page_size, mtr);
                buf_page_get(page_id, page_size, RW_SX_LATCH, mtr);
                //buf_block_dbg_add_level(block, SYNC_FSP_PAGE);
                //fsp_init_file_page(block, mtr);
                //mlog_write_uint32(buf_block_get_frame(block) + FIL_PAGE_TYPE, FIL_PAGE_TYPE_XDES, MLOG_2BYTES, mtr);
            }

            /* Initialize the ibuf bitmap page in a separate
            mini-transaction because it is low in the latching
            order, and we must be able to release its latch.
            Note: Insert-Buffering is disabled for tables that
            reside in the temp-tablespace. */
            /*
            if (space->id != srv_tmp_space.space_id()) {
                mtr_t ibuf_mtr;

                mtr_start(&ibuf_mtr);
                ibuf_mtr.set_named_space(space);
                // Avoid logging while truncate table fix-up is active.
                if (space->purpose == FIL_TYPE_TEMPORARY || srv_is_tablespace_truncated(space->id)) {
                    mtr_set_log_mode(&ibuf_mtr, MTR_LOG_NO_REDO);
                }
                const page_id_t page_id(space->id, i + FSP_IBUF_BITMAP_OFFSET);
                block = buf_page_create(page_id, page_size, &ibuf_mtr);
                buf_page_get(page_id, page_size, RW_SX_LATCH, &ibuf_mtr);
                buf_block_dbg_add_level(block, SYNC_FSP_PAGE);
                fsp_init_file_page(block, &ibuf_mtr);
                ibuf_bitmap_page_init(block, &ibuf_mtr);
                mtr_commit(&ibuf_mtr);
            }
            */
        }

        buf_block_t* desc_block = NULL;
        descr = xdes_get_descriptor_with_space_hdr(header, space->id, i, mtr, init_space, &desc_block);
        //if (desc_block != NULL) {
        //    fil_block_check_type(desc_block, FIL_PAGE_TYPE_XDES, mtr);
        //}
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

    //space->free_len += count;
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


/**************************************************************************
Allocates a single free page from a space. The page is marked as used. */
/* out: the page offset, FIL_NULL if no page could be allocated */
static uint32 fsp_alloc_free_page(
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
                return(FIL_NULL);
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
    buf_page_create(page_id_t(space, page_no), page_size, RW_X_LATCH, mtr);
    buf_block_t* block;
    block = buf_page_get(page_id_t(space, page_no), page_size, RW_X_LATCH, mtr);
    //page = buf_block_get_frame(block);

    /* Prior contents of the page should be ignored */
    fsp_init_file_page(block, mtr);

    return page_no;
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

    ut_ad(id != 0 || !page_size.is_compressed());

    block = buf_page_get(page_id_t(id, 0), page_size, RW_SX_LATCH, mtr);
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
void fsp_header_init(uint32 space_id, uint32 size, mtr_t *mtr)
{
    fsp_header_t* header;
    buf_block_t *block;
    page_t* page;

    ut_ad(mtr);

    mtr_x_lock(fil_space_get_latch(space_id), mtr);

    const page_id_t page_id(space_id, 0);
    const page_size_t page_size(0);
    block = buf_page_create(page_id, page_size, RW_X_LATCH, mtr);
    //buf_page_dbg_add_level(page, SYNC_FSP_PAGE);
    //const page_id_t page_id(space_id, 0);
    //page = buf_page_get(page_id, size, RW_X_LATCH, mtr);
    //buf_page_dbg_add_level(page, SYNC_FSP_PAGE);

    /* The prior contents of the file page should be ignored */

    fsp_init_file_page(block, mtr);
    page = buf_block_get_frame(block);

    header = FSP_HEADER_OFFSET + page;

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

    mutex_enter(&fil_system->lock);
    fil_space_t *space = fil_space_get_by_id(space_id);
    ut_a(space != NULL);
    //fsp_fill_free_list(false, space, header, mtr);
    mutex_exit(&fil_system->lock);

    //btr_create(DICT_CLUSTERED | DICT_UNIVERSAL | DICT_IBUF, space,
    //			ut_dulint_add(DICT_IBUF_ID_MIN, space), mtr);
}


