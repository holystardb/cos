#include "knl_btree.h"




// Create the root node for a new index tree.
uint32 btr_create(
    uint32              type,
    uint32              space,
    const page_size_t&  page_size,
    index_id_t          index_id,
    dict_index_t*       index,
    const btr_create_t* btr_redo_create_info,
    mtr_t*              mtr)
{
/*
    uint32       page_no;
    buf_block_t*    block;
    buf_frame_t*    frame;
    page_t*     page;
    page_zip_des_t* page_zip;

    // Create the two new segments for the index tree;
    //    the segment headers are put on the allocated root page

    block = fseg_create(space, 0, PAGE_HEADER + PAGE_BTR_SEG_TOP, mtr);
    if (block == NULL) {
        return(FIL_NULL);
    }
    //buf_block_dbg_add_level(block, SYNC_TREE_NODE_NEW);
    page_no = buf_block_get_page_no(block);
    frame = buf_block_get_frame(block);

    if (!fseg_create(space, page_no, PAGE_HEADER + PAGE_BTR_SEG_LEAF, mtr)) {
        // Not enough space for new segment, free root segment before return
        btr_free_root(space, zip_size, page_no, mtr);
        return(FIL_NULL);
    }

    // The fseg create acquires a second latch on the page, therefore we must declare it
    //buf_block_dbg_add_level(block, SYNC_TREE_NODE_NEW);

    // Create a new index page on the allocated segment page
    page = page_create(block, mtr, dict_table_is_comp(index->table));
    // Set the level of the new index page
    btr_page_set_level(page, NULL, 0, mtr);

    block->check_index_page_at_flush = TRUE;

    // Set the index id of the page
    mlog_write_uint64(page + (PAGE_HEADER + PAGE_INDEX_ID), id, mtr);

    // Set the next node and previous node fields
    btr_page_set_next(page, page_zip, FIL_NULL, mtr);
    btr_page_set_prev(page, page_zip, FIL_NULL, mtr);

    // We reset the free bits for the page to allow creation of several
    // trees in the same mtr, otherwise the latch on a bitmap page would
    // prevent it because of the latching order

    if (!(type & DICT_CLUSTERED)) {
        ibuf_reset_free_bits(block);
    }

    // In the following assertion we test that two records of maximum
    // allowed size fit on the root page: this fact is needed to ensure
    // correctness of split algorithms

    ut_ad(page_get_max_insert_size(page, 2) > 2 * BTR_PAGE_MAX_REC_SIZE);

    return page_no;
*/

    return 0;
}

