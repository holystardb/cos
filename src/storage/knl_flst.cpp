#include "knl_flst.h"
#include "knl_fsp.h"
#include "knl_buf.h"
#include "knl_mtr.h"
#include "knl_page.h"

/* We define the field offsets of a node for the list */
#define FLST_PREV   0   /* 6-byte address of the previous list element;
                the page part of address is FIL_NULL, if no previous element */
#define FLST_NEXT   FIL_ADDR_SIZE   /* 6-byte address of the next list element;
                the page part of address is FIL_NULL, if no next element */

/* We define the field offsets of a base node for the list */
#define FLST_LEN    0   /* 32-bit list length field */
#define	FLST_FIRST  4   /* 6-byte address of the first element of the list; undefined if empty list */
#define	FLST_LAST   (4 + FIL_ADDR_SIZE) /* 6-byte address of the last element of the list; undefined if empty list */

/********************************************************************//**
Writes a file address. */
void flst_write_addr(
    fil_faddr_t *faddr, /*!< in: pointer to file faddress */
    fil_addr_t   addr,  /*!< in: file address */
    mtr_t       *mtr)   /*!< in: mini-transaction handle */
{
    ut_ad(faddr && mtr);
    //ut_ad(mtr_memo_contains_page_flagged(mtr, faddr, MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));
    ut_a(addr.page == FIL_NULL || addr.boffset >= FIL_PAGE_DATA);
    ut_a(ut_align_offset(faddr, UNIV_PAGE_SIZE) >= FIL_PAGE_DATA);

    mlog_write_uint32(faddr + FIL_ADDR_PAGE, addr.page, MLOG_4BYTES, mtr);
    mlog_write_uint32(faddr + FIL_ADDR_BYTE, addr.boffset, MLOG_2BYTES, mtr);
}

/********************************************************************//**
Reads a file address.
@return file address */
fil_addr_t flst_read_addr(
    const fil_faddr_t   *faddr, /*!< in: pointer to file faddress */
    mtr_t               *mtr)   /*!< in: mini-transaction handle */
{
    fil_addr_t	addr;

    ut_ad(faddr && mtr);

    addr.page = mtr_read_uint32(faddr + FIL_ADDR_PAGE, MLOG_4BYTES, mtr);
    addr.boffset = mtr_read_uint32(faddr + FIL_ADDR_BYTE, MLOG_2BYTES, mtr);
    ut_a(addr.page == FIL_NULL || addr.boffset >= FIL_PAGE_DATA);
    ut_a(ut_align_offset(faddr, UNIV_PAGE_SIZE) >= FIL_PAGE_DATA);
    return(addr);
}

/********************************************************************//**
Initializes a list base node. */
void flst_init(
    flst_base_node_t    *base,  /*!< in: pointer to base node */
    mtr_t               *mtr)   /*!< in: mini-transaction handle */
{
    //ut_ad(mtr_memo_contains_page_flagged(mtr, base, MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));

    mlog_write_uint32(base + FLST_LEN, 0, MLOG_4BYTES, mtr);
    flst_write_addr(base + FLST_FIRST, fil_addr_null, mtr);
    flst_write_addr(base + FLST_LAST, fil_addr_null, mtr);
}

// Get the length of a list
uint32 flst_get_len(const flst_base_node_t *base)
{
    return(mach_read_from_4(base + FLST_LEN));
}

/********************************************************************//**
Gets list first node address.
@return file address */
fil_addr_t flst_get_first(
    const flst_base_node_t  *base, /*!< in: pointer to base node */
    mtr_t                   *mtr)  /*!< in: mini-transaction handle */
{
    return(flst_read_addr(base + FLST_FIRST, mtr));
}

/********************************************************************//**
Gets list last node address.
@return file address */
fil_addr_t flst_get_last(
	const flst_base_node_t*	base,	/*!< in: pointer to base node */
	mtr_t*			mtr)	/*!< in: mini-transaction handle */
{
	return(flst_read_addr(base + FLST_LAST, mtr));
}

/********************************************************************//**
Gets list next node address.
@return file address */
fil_addr_t flst_get_next_addr(
	const flst_node_t*	node,	/*!< in: pointer to node */
	mtr_t*			mtr)	/*!< in: mini-transaction handle */
{
	return(flst_read_addr(node + FLST_NEXT, mtr));
}

/********************************************************************//**
Gets list prev node address.
@return file address */
fil_addr_t flst_get_prev_addr(
	const flst_node_t*	node,	/*!< in: pointer to node */
	mtr_t*			mtr)	/*!< in: mini-transaction handle */
{
	return(flst_read_addr(node + FLST_PREV, mtr));
}


/********************************************************************//**
Adds a node to an empty list. */
static void flst_add_to_empty(
	flst_base_node_t*	base,	/*!< in: pointer to base node of empty list */
	flst_node_t*		node,	/*!< in: node to add */
	mtr_t*			mtr)	/*!< in: mini-transaction handle */
{
	uint32		space;
	fil_addr_t	node_addr;
	uint32		len;

	ut_ad(mtr && base && node);
	ut_ad(base != node);
	//ut_ad(mtr_memo_contains_page_flagged(mtr, base, MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));
	//ut_ad(mtr_memo_contains_page_flagged(mtr, node, MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));
	len = flst_get_len(base);
	ut_a(len == 0);

	buf_ptr_get_fsp_addr(node, &space, &node_addr);

	/* Update first and last fields of base node */
	flst_write_addr(base + FLST_FIRST, node_addr, mtr);
	flst_write_addr(base + FLST_LAST, node_addr, mtr);

	/* Set prev and next fields of node to add */
	flst_write_addr(node + FLST_PREV, fil_addr_null, mtr);
	flst_write_addr(node + FLST_NEXT, fil_addr_null, mtr);

	/* Update len of base node */
	mlog_write_uint32(base + FLST_LEN, len + 1, MLOG_4BYTES, mtr);
}

/********************************************************************//**
Adds a node as the last node in a list. */
void flst_add_last(
	flst_base_node_t*	base,	/*!< in: pointer to base node of list */
	flst_node_t*		node,	/*!< in: node to add */
	mtr_t*			mtr)	/*!< in: mini-transaction handle */
{
	uint32		space;
	fil_addr_t	node_addr;
	uint32		len;
	fil_addr_t	last_addr;

	ut_ad(mtr && base && node);
	ut_ad(base != node);
	//ut_ad(mtr_memo_contains_page_flagged(mtr, base, MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));
	//ut_ad(mtr_memo_contains_page_flagged(mtr, node, MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));
	len = flst_get_len(base);
	last_addr = flst_get_last(base, mtr);

	buf_ptr_get_fsp_addr(node, &space, &node_addr);

	/* If the list is not empty, call flst_insert_after */
	if (len != 0) {
		flst_node_t*	last_node;

		if (last_addr.page == node_addr.page) {
			last_node = page_align(node) + last_addr.boffset;
		} else {
			//bool			found;
			//const page_size_t&	page_size = fil_space_get_page_size(space, &found);
			//ut_ad(found);
            page_size_t page_size(0);
			last_node = fut_get_ptr(space, page_size, last_addr, RW_X_LATCH, mtr, NULL);
		}

		flst_insert_after(base, last_node, node, mtr);
	} else {
		/* else call flst_add_to_empty */
		flst_add_to_empty(base, node, mtr);
	}
}

/********************************************************************//**
Adds a node as the first node in a list. */
void flst_add_first(
	flst_base_node_t*	base,	/*!< in: pointer to base node of list */
	flst_node_t*		node,	/*!< in: node to add */
	mtr_t*			mtr)	/*!< in: mini-transaction handle */
{
	uint32		space;
	fil_addr_t	node_addr;
	uint32		len;
	fil_addr_t	first_addr;
	flst_node_t*	first_node;

	ut_ad(mtr && base && node);
	ut_ad(base != node);
	//ut_ad(mtr_memo_contains_page_flagged(mtr, base, MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));
	//ut_ad(mtr_memo_contains_page_flagged(mtr, node, MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));
	len = flst_get_len(base);
	first_addr = flst_get_first(base, mtr);

	buf_ptr_get_fsp_addr(node, &space, &node_addr);

	/* If the list is not empty, call flst_insert_before */
	if (len != 0) {
		if (first_addr.page == node_addr.page) {
			first_node = page_align(node) + first_addr.boffset;
		} else {
			//bool			found;
			//const page_size_t&	page_size = fil_space_get_page_size(space, &found);
			//ut_ad(found);
            page_size_t page_size(0);
			first_node = fut_get_ptr(space, page_size, first_addr, RW_X_LATCH, mtr, NULL);
		}

		flst_insert_before(base, node, first_node, mtr);
	} else {
		/* else call flst_add_to_empty */
		flst_add_to_empty(base, node, mtr);
	}
}

/********************************************************************//**
Inserts a node after another in a list. */
void flst_insert_after(
	flst_base_node_t*	base,	/*!< in: pointer to base node of list */
	flst_node_t*		node1,	/*!< in: node to insert after */
	flst_node_t*		node2,	/*!< in: node to add */
	mtr_t*			mtr)	/*!< in: mini-transaction handle */
{
	uint32		space;
	fil_addr_t	node1_addr;
	fil_addr_t	node2_addr;
	flst_node_t*	node3;
	fil_addr_t	node3_addr;
	uint32		len;

	ut_ad(mtr && node1 && node2 && base);
	ut_ad(base != node1);
	ut_ad(base != node2);
	ut_ad(node2 != node1);
	//ut_ad(mtr_memo_contains_page_flagged(mtr, base, MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));
	//ut_ad(mtr_memo_contains_page_flagged(mtr, node1, MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));
	//ut_ad(mtr_memo_contains_page_flagged(mtr, node2, MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));

	buf_ptr_get_fsp_addr(node1, &space, &node1_addr);
	buf_ptr_get_fsp_addr(node2, &space, &node2_addr);

	node3_addr = flst_get_next_addr(node1, mtr);

	/* Set prev and next fields of node2 */
	flst_write_addr(node2 + FLST_PREV, node1_addr, mtr);
	flst_write_addr(node2 + FLST_NEXT, node3_addr, mtr);

	if (!fil_addr_is_null(node3_addr)) {
		/* Update prev field of node3 */
		//bool			found;
		//const page_size_t&	page_size = fil_space_get_page_size(space, &found);
		//ut_ad(found);
        page_size_t page_size(0);
		node3 = fut_get_ptr(space, page_size, node3_addr, RW_X_LATCH, mtr, NULL);
		flst_write_addr(node3 + FLST_PREV, node2_addr, mtr);
	} else {
		/* node1 was last in list: update last field in base */
		flst_write_addr(base + FLST_LAST, node2_addr, mtr);
	}

	/* Set next field of node1 */
	flst_write_addr(node1 + FLST_NEXT, node2_addr, mtr);

	/* Update len of base node */
	len = flst_get_len(base);
	mlog_write_uint32(base + FLST_LEN, len + 1, MLOG_4BYTES, mtr);
}

/********************************************************************//**
Inserts a node before another in a list. */
void flst_insert_before(
	flst_base_node_t*	base,	/*!< in: pointer to base node of list */
	flst_node_t*		node2,	/*!< in: node to insert */
	flst_node_t*		node3,	/*!< in: node to insert before */
	mtr_t*			mtr)	/*!< in: mini-transaction handle */
{
	uint32		space;
	flst_node_t*	node1;
	fil_addr_t	node1_addr;
	fil_addr_t	node2_addr;
	fil_addr_t	node3_addr;
	uint32		len;

	ut_ad(mtr && node2 && node3 && base);
	ut_ad(base != node2);
	ut_ad(base != node3);
	ut_ad(node2 != node3);
	//ut_ad(mtr_memo_contains_page_flagged(mtr, base, MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));
	//ut_ad(mtr_memo_contains_page_flagged(mtr, node2, MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));
	//ut_ad(mtr_memo_contains_page_flagged(mtr, node3, MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));

	buf_ptr_get_fsp_addr(node2, &space, &node2_addr);
	buf_ptr_get_fsp_addr(node3, &space, &node3_addr);

	node1_addr = flst_get_prev_addr(node3, mtr);

	/* Set prev and next fields of node2 */
	flst_write_addr(node2 + FLST_PREV, node1_addr, mtr);
	flst_write_addr(node2 + FLST_NEXT, node3_addr, mtr);

	if (!fil_addr_is_null(node1_addr)) {
		//bool			found;
		//const page_size_t&	page_size = fil_space_get_page_size(space, &found);
		//ut_ad(found);
        page_size_t page_size(0);
		/* Update next field of node1 */
		node1 = fut_get_ptr(space, page_size, node1_addr, RW_X_LATCH, mtr, NULL);
		flst_write_addr(node1 + FLST_NEXT, node2_addr, mtr);
	} else {
		/* node3 was first in list: update first field in base */
		flst_write_addr(base + FLST_FIRST, node2_addr, mtr);
	}

	/* Set prev field of node3 */
	flst_write_addr(node3 + FLST_PREV, node2_addr, mtr);

	/* Update len of base node */
	len = flst_get_len(base);
	mlog_write_uint32(base + FLST_LEN, len + 1, MLOG_4BYTES, mtr);
}

/********************************************************************//**
Removes a node. */
void flst_remove(
	flst_base_node_t*	base,	/*!< in: pointer to base node of list */
	flst_node_t*		node2,	/*!< in: node to remove */
	mtr_t*			mtr)	/*!< in: mini-transaction handle */
{
	uint32		space;
	flst_node_t*	node1;
	fil_addr_t	node1_addr;
	fil_addr_t	node2_addr;
	flst_node_t*	node3;
	fil_addr_t	node3_addr;
	uint32		len;

	ut_ad(mtr && node2 && base);
	//ut_ad(mtr_memo_contains_page_flagged(mtr, base, MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));
	//ut_ad(mtr_memo_contains_page_flagged(mtr, node2, MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));

	buf_ptr_get_fsp_addr(node2, &space, &node2_addr);

	//bool			found;
	//const page_size_t&	page_size = fil_space_get_page_size(space, &found);
	//ut_ad(found);
    page_size_t	page_size(0);

	node1_addr = flst_get_prev_addr(node2, mtr);
	node3_addr = flst_get_next_addr(node2, mtr);

	if (!fil_addr_is_null(node1_addr)) {

		/* Update next field of node1 */

		if (node1_addr.page == node2_addr.page) {

			node1 = page_align(node2) + node1_addr.boffset;
		} else {
			node1 = fut_get_ptr(space, page_size, node1_addr, RW_X_LATCH, mtr, NULL);
		}

		ut_ad(node1 != node2);

		flst_write_addr(node1 + FLST_NEXT, node3_addr, mtr);
	} else {
		/* node2 was first in list: update first field in base */
		flst_write_addr(base + FLST_FIRST, node3_addr, mtr);
	}

	if (!fil_addr_is_null(node3_addr)) {
		/* Update prev field of node3 */

		if (node3_addr.page == node2_addr.page) {

			node3 = page_align(node2) + node3_addr.boffset;
		} else {
			node3 = fut_get_ptr(space, page_size, node3_addr, RW_X_LATCH, mtr, NULL);
		}

		ut_ad(node2 != node3);

		flst_write_addr(node3 + FLST_PREV, node1_addr, mtr);
	} else {
		/* node2 was last in list: update last field in base */
		flst_write_addr(base + FLST_LAST, node1_addr, mtr);
	}

	/* Update len of base node */
	len = flst_get_len(base);
	ut_ad(len > 0);

	mlog_write_uint32(base + FLST_LEN, len - 1, MLOG_4BYTES, mtr);
}

/********************************************************************//**
Validates a file-based list.
@return TRUE if ok */
bool32 flst_validate(
	const flst_base_node_t*	base,	/*!< in: pointer to base node of list */
	mtr_t*			mtr)	/*!< in: mtr */
{
	uint32			space;
	const flst_node_t*	node;
	fil_addr_t		node_addr;
	fil_addr_t		base_addr;
	uint32			len;
	uint32			i;
	mtr_t			mtr2;

	ut_ad(base);
	//ut_ad(mtr_memo_contains_page_flagged(mtr1, base, MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));

	/* We use two mini-transaction handles: the first is used to
	lock the base node, and prevent other threads from modifying the
	list. The second is used to traverse the list. We cannot run the
	second mtr without committing it at times, because if the list
	is long, then the x-locked pages could fill the buffer resulting
	in a deadlock. */

	/* Find out the space id */
	buf_ptr_get_fsp_addr(base, &space, &base_addr);

	//bool			found;
	//const page_size_t&	page_size = fil_space_get_page_size(space, &found);
	//ut_ad(found);
    page_size_t	page_size(0);

	len = flst_get_len(base);
	node_addr = flst_get_first(base, mtr);

	for (i = 0; i < len; i++) {
		mtr_start(&mtr2);

		node = fut_get_ptr(space, page_size, node_addr, RW_X_LATCH, &mtr2, NULL);
		node_addr = flst_get_next_addr(node, &mtr2);

		mtr_commit(&mtr2); /* Commit mtr2 each round to prevent buffer becoming full */
	}

	ut_a(fil_addr_is_null(node_addr));

	node_addr = flst_get_last(base, mtr);

	for (i = 0; i < len; i++) {
		mtr_start(&mtr2);

		node = fut_get_ptr(space, page_size, node_addr, RW_X_LATCH, &mtr2, NULL);
		node_addr = flst_get_prev_addr(node, &mtr2);

		mtr_commit(&mtr2); /* Commit mtr2 each round to prevent buffer becoming full */
	}

	ut_a(fil_addr_is_null(node_addr));

	return(TRUE);
}

/********************************************************************//**
Prints info of a file-based list. */
void flst_print(
	const flst_base_node_t*	base,	/*!< in: pointer to base node of list */
	mtr_t*			mtr)	/*!< in: mtr */
{
	const buf_frame_t*	frame;
	uint32			len;

	ut_ad(base && mtr);
	//ut_ad(mtr_memo_contains_page_flagged(mtr, base, MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));
	frame = page_align((byte*) base);

	len = flst_get_len(base);

	//ib::info() << "FILE-BASED LIST: Base node in space "
	//	<< page_get_space_id(frame)
	//	<< "; page " << page_get_page_no(frame)
	//	<< "; byte offset " << page_offset(base)
	//	<< "; len " << len;
}


