#include "knl_undo_fsm.h"
#include "cm_log.h"
#include "cm_timer.h"
#include "knl_buf.h"
#include "knl_flst.h"
#include "knl_page.h"
#include "knl_fsp.h"

#define UNDO_FSM_HEADER_PAGE_NO         1

#define UNDO_FSM_STATUS_FULL            1

const page_size_t undo_log_page_size(DB_UNDO_START_SPACE_ID);
const uint32 NODE_COUNT_PER_UNDO_FSM_PAGE =
    (undo_log_page_size.physical() - FIL_PAGE_DATA - FIL_PAGE_DATA_END) / UNDO_FSM_NODE_SIZE;


buf_block_t* undo_fsm_hdr_block[DB_UNDO_SPACE_MAX_COUNT] = { NULL };


// ================================================================================================

static inline undo_fsm_header_t* undo_fsm_get_fsm_header(uint32 space_id, mtr_t* mtr)
{
    ut_ad(space_id >= FIL_UNDO_START_SPACE_ID && space_id <= FIL_UNDO_END_SPACE_ID);

    // get block
    buf_block_t* guess_block = undo_fsm_hdr_block[space_id - FIL_UNDO_START_SPACE_ID];
    ut_ad(guess_block);
#ifdef UNIV_DEBUG
    page_id_t page_id(space_id, UNDO_FSM_HEADER_PAGE_NO);
    buf_block_t* block = buf_page_get_gen(page_id, undo_log_page_size,
        RW_X_LATCH, guess_block, Page_fetch::NORMAL, mtr);
    ut_a(block->get_page_no() == UNDO_FSM_HEADER_PAGE_NO);
    //buf_block_dbg_add_level(fsm_root_block, SYNC_RSEG_HEADER_NEW);
    ut_ad(block == guess_block);
#endif /* UNIV_DEBUG */

    // lock block
    buf_block_lock_and_fix(guess_block, RW_X_LATCH, mtr);

    return buf_block_get_frame(guess_block) + UNDO_FSM_HEADER;
}

static inline status_t undo_fsm_alloc_log_pages(uint32 space_id,
    undo_fsm_header_t* fsm_header, uint32 alloc_count, mtr_t* mtr)
{
    uint32 page_count = 0;

    ut_ad(mlog_read_uint32(fsm_header + UNDO_FSM_STATUS, MLOG_4BYTES) != UNDO_FSM_STATUS_FULL);
    ut_ad(mlog_read_uint32(fsm_header + UNDO_FSM_LOG_PAGE_MAX_COUNT, MLOG_4BYTES) >=
        mlog_read_uint32(fsm_header + UNDO_FSM_LOG_PAGE_COUNT, MLOG_4BYTES) + alloc_count);

    for (page_count = 0; page_count < alloc_count; page_count++) {
        // alloc undo_log page
        buf_block_t* log_block = fsp_alloc_free_page(space_id, undo_log_page_size, Page_fetch::NORMAL, mtr);
        if (log_block == NULL) {
            // set full
            mlog_write_uint32(fsm_header + UNDO_FSM_STATUS, UNDO_FSM_STATUS_FULL, MLOG_4BYTES, mtr);
            break;
        }
        page_t* page = buf_block_get_frame(log_block);
        mlog_write_uint32(page + FIL_PAGE_TYPE, FIL_PAGE_TYPE_UNDO_LOG, MLOG_2BYTES, mtr);

        // alloc a unused node
        undo_fsm_node_t* node;
        fil_addr_t node_addr = flst_get_first(fsm_header + UNDO_FSM_UNUSED_LIST, mtr);
        ut_ad(!fil_addr_is_null(node_addr));
        node = flst_get_buf_ptr(space_id, undo_log_page_size, node_addr, RW_X_LATCH, mtr, NULL) - UNDO_FSM_FLST_NODE;
        flst_remove(fsm_header + UNDO_FSM_UNUSED_LIST, node + UNDO_FSM_FLST_NODE, mtr);

        // add node to insert_list
        flst_add_first(fsm_header + UNDO_FSM_INSERT_LIST, node + UNDO_FSM_FLST_NODE, mtr);
    }

    // set current size of undo file
    if (page_count > 0) {
        uint32 size = mlog_read_uint32(fsm_header + UNDO_FSM_LOG_PAGE_COUNT, MLOG_4BYTES);
        mlog_write_uint32(fsm_header + UNDO_FSM_LOG_PAGE_COUNT, size + page_count, MLOG_4BYTES, mtr);
    }

    return CM_SUCCESS;
}

static status_t undo_fsm_init_log_pages(uint32 space_id, uint32 page_init_count)
{
    status_t err;
    mtr_t init_mtr, *mtr = &init_mtr;

    mtr_start(mtr);

    // get root fsm page
    undo_fsm_header_t* fsm_header = undo_fsm_get_fsm_header(space_id, mtr);

    // alloc undo log page
    err = undo_fsm_alloc_log_pages(space_id, fsm_header, page_init_count, mtr);

    mtr_commit(mtr);

    return err;
}


static status_t undo_fsm_init_fsm_page(undo_fsm_header_t* fsm_header, page_t* page, mtr_t* mtr)
{
    undo_fsm_node_t *node;

    // add node to unused_list

    for (uint32 i = 0; i < NODE_COUNT_PER_UNDO_FSM_PAGE; i++) {
        node = page + UNDO_FSM_NODE + i * UNDO_FSM_NODE_SIZE;
        flst_add_first(fsm_header + UNDO_FSM_UNUSED_LIST, node + UNDO_FSM_FLST_NODE, mtr);
    }

    return CM_SUCCESS;
}

static status_t undo_fsm_init_fsm_pages(uint32 space_id, uint32 fsm_page_count)
{
    status_t err = CM_SUCCESS;
    mtr_t init_mtr, *mtr = &init_mtr;

    // 1 first extent
    for (uint32 page_no = 2; page_no < FSP_EXTENT_SIZE; page_no++) {
        mtr_start(mtr);

        // 1.1 create page
        buf_block_t* block = fsp_alloc_free_page(space_id, undo_log_page_size, Page_fetch::RESIDENT, mtr);
        if (block == NULL) {
            goto err_exit;
        }
        ut_a(block->get_page_no() == page_no);
        page_t* page = buf_block_get_frame(block);
        mlog_write_uint32(page + FIL_PAGE_TYPE, FIL_PAGE_TYPE_UNDO_FSM | FIL_PAGE_TYPE_RESIDENT_FLAG, MLOG_2BYTES, mtr);

        // 1.2
        undo_fsm_header_t* fsm_header = undo_fsm_get_fsm_header(space_id, mtr);
        if (undo_fsm_init_fsm_page(fsm_header, page, mtr) != CM_SUCCESS) {
            goto err_exit;
        }

        mtr_commit(mtr);
    }

    // other extents
    uint32 remain_page_count = (fsm_page_count > (FSP_EXTENT_SIZE - 2) * NODE_COUNT_PER_UNDO_FSM_PAGE)
        ? (fsm_page_count - (FSP_EXTENT_SIZE - 2) * NODE_COUNT_PER_UNDO_FSM_PAGE) : 0;
    if (remain_page_count  > 0) {
        uint32 extent_count = (remain_page_count + NODE_COUNT_PER_UNDO_FSM_PAGE - 1) / NODE_COUNT_PER_UNDO_FSM_PAGE;
        for (uint32 i = 0; i < extent_count; i++) {
            mtr_start(mtr);
            // get free extents
            xdes_t* descr = fsp_alloc_free_extent(space_id, undo_log_page_size, mtr);
            if (descr == NULL) {
                goto err_exit;
            }
            xdes_set_state(descr, XDES_FSEG, mtr);
            mtr_commit(mtr);

            // create pages
            for (uint32 i = 0; i < FSP_EXTENT_SIZE; i++) {
                mtr_start(mtr);

                //
                uint32 page_no = xdes_get_offset(descr) + i;
                const page_id_t page_id(space_id, page_no);
                buf_block_t* block = fsp_page_create(page_id, undo_log_page_size, Page_fetch::RESIDENT, mtr, mtr);
                ut_a(block);
                page_t* page = buf_block_get_frame(block);
                mlog_write_uint32(page + FIL_PAGE_TYPE, FIL_PAGE_TYPE_UNDO_FSM | FIL_PAGE_TYPE_RESIDENT_FLAG, MLOG_2BYTES, mtr);

                //
                undo_fsm_header_t* fsm_header = undo_fsm_get_fsm_header(space_id, mtr);
                if (undo_fsm_init_fsm_page(fsm_header, page, mtr) != CM_SUCCESS) {
                    goto err_exit;
                }

                mtr_commit(mtr);
            }
        }

    }

    return CM_SUCCESS;

err_exit:

    mtr->modifications = FALSE;
    mtr_commit(mtr);

    return CM_ERROR;
}

static status_t undo_fsm_hdr_init(uint32 space_id, uint32 fsm_page_count, uint32 log_page_max_count)
{
    status_t err = CM_SUCCESS;
    mtr_t init_mtr, *mtr = &init_mtr;

    mtr_start(mtr);

    //
    buf_block_t* block = fsp_alloc_free_page(space_id, undo_log_page_size, Page_fetch::RESIDENT, mtr);
    if (block == NULL) {
        mtr->modifications = FALSE;
        err = CM_ERROR;
        goto err_exit;
    }
    ut_a(block->get_page_no() == UNDO_FSM_HEADER_PAGE_NO);
    page_t* page = buf_block_get_frame(block);
    mlog_write_uint32(page + FIL_PAGE_TYPE, FIL_PAGE_TYPE_UNDO_FSM | FIL_PAGE_TYPE_RESIDENT_FLAG, MLOG_2BYTES, mtr);

    // init undo fsm header
    undo_fsm_header_t* header = page + UNDO_FSM_HEADER;
    mlog_write_uint32(header + UNDO_FSM_STATUS, 0, MLOG_4BYTES, mtr);
    mlog_write_uint32(header + UNDO_FSM_FSM_PAGE_COUNT, fsm_page_count, MLOG_4BYTES, mtr);
    mlog_write_uint32(header + UNDO_FSM_LOG_PAGE_COUNT, 0, MLOG_4BYTES, mtr);
    mlog_write_uint32(header + UNDO_FSM_LOG_PAGE_MAX_COUNT, log_page_max_count, MLOG_4BYTES, mtr);
    flst_init(header + UNDO_FSM_UNUSED_LIST, mtr);
    flst_init(header + UNDO_FSM_INSERT_LIST, mtr);
    flst_init(header + UNDO_FSM_UPDATE_LIST, mtr);
    flst_init(header + UNDO_FSM_USED_LIST, mtr);

    //
    ut_a(undo_fsm_hdr_block[space_id - FIL_UNDO_START_SPACE_ID] == NULL);
    undo_fsm_hdr_block[space_id - FIL_UNDO_START_SPACE_ID] = block;

err_exit:

    mtr_commit(mtr);

    return err;
}


status_t undo_fsm_tablespace_init(uint32 space_id, uint64 init_size, uint64 max_size)
{
    status_t err;

    ut_a(init_size >= 1024 * 1024 * 4);
    ut_a(init_size <= max_size);
    ut_a(space_id >= FIL_UNDO_START_SPACE_ID && space_id <= FIL_UNDO_END_SPACE_ID);

    // undo tablespace: Filespace Header/Extent Descriptor
    err = fsp_init_space(space_id, init_size, max_size, 0);
    CM_RETURN_IF_ERROR(err);

    //
    uint32 page_max_count = (uint32)(max_size / undo_log_page_size.physical());
    uint32 page_count_per_xdes_page = 16384;
    uint32 xdes_page_count = (page_max_count + page_count_per_xdes_page - 1) / page_count_per_xdes_page;
    uint32 fsm_page_count = (page_max_count - xdes_page_count + NODE_COUNT_PER_UNDO_FSM_PAGE - 1) / NODE_COUNT_PER_UNDO_FSM_PAGE;
    uint32 fsm_page_align_count = (fsm_page_count <= FSP_EXTENT_SIZE - 2) ?
        FSP_EXTENT_SIZE - 2 : (FSP_EXTENT_SIZE - 2) +
        FSP_EXTENT_SIZE * ((fsm_page_count - (FSP_EXTENT_SIZE - 2) + FSP_EXTENT_SIZE - 1) / FSP_EXTENT_SIZE);
    uint32 log_page_max_count = page_max_count - xdes_page_count - fsm_page_align_count - 1 /*fsm header*/;

    err = undo_fsm_hdr_init(space_id, fsm_page_align_count, log_page_max_count);
    CM_RETURN_IF_ERROR(err);

    // alloc undo fsm pages
    err = undo_fsm_init_fsm_pages(space_id, fsm_page_align_count);
    CM_RETURN_IF_ERROR(err);

    // alloc undo log page
    uint32 log_page_init_count = (init_size == max_size) ? log_page_max_count : (uint32)(init_size / undo_log_page_size.physical());
    if (log_page_init_count > log_page_max_count) {
        log_page_init_count = log_page_max_count;
    }
    err = undo_fsm_init_log_pages(space_id, log_page_init_count);
    CM_RETURN_IF_ERROR(err);

    return CM_SUCCESS;
}

status_t undo_fsm_recovery_fsp_pages(uint32 space_id)
{
    mtr_t init_mtr, *mtr = &init_mtr;
    undo_fsm_page_t fsm_page = {space_id, FIL_NULL, FIL_NULL, 0};

    ut_a(space_id >= FIL_UNDO_START_SPACE_ID && space_id <= FIL_UNDO_END_SPACE_ID);

    mtr_start(mtr);

    // 1 load fsm_header page and get fsm_header
    page_id_t hdr_page_id(space_id, UNDO_FSM_HEADER_PAGE_NO);
    buf_block_t* block = buf_page_get(hdr_page_id, undo_log_page_size, RW_X_LATCH, mtr);
    ut_a(block);
    ut_a(block->is_resident());
    ut_ad(block->get_page_no() == UNDO_FSM_HEADER_PAGE_NO);

    ut_a(undo_fsm_hdr_block[space_id - FIL_UNDO_START_SPACE_ID] == NULL);
    undo_fsm_hdr_block[space_id - FIL_UNDO_START_SPACE_ID] = block;

    undo_fsm_header_t* fsm_header = undo_fsm_get_fsm_header(space_id, mtr);

    // 2 load fsm page for resident
    uint32 fsm_page_count = mlog_read_uint32(fsm_header + UNDO_FSM_FSM_PAGE_COUNT, MLOG_4BYTES);
    for (uint32 i = 0; i < fsm_page_count; i++) {
        page_id_t page_id(space_id, i + UNDO_FSM_HEADER_PAGE_NO + 1);
        block = buf_page_get(page_id, undo_log_page_size, RW_X_LATCH, mtr);
        ut_a(block);
        ut_ad(block->get_page_no() == i + UNDO_FSM_HEADER_PAGE_NO + 1);
        //buf_block_dbg_add_level(fsm_root_block, SYNC_RSEG_HEADER_NEW);
    }

    // 3 recovery
    fil_addr_t node_addr = flst_get_first(fsm_header + UNDO_FSM_USED_LIST, mtr);
    while (!fil_addr_is_null(node_addr)) {
        // get node by node_addr
        undo_fsm_node_t* node;
        node = flst_get_buf_ptr(space_id, undo_log_page_size, node_addr, RW_X_LATCH, mtr, NULL) - UNDO_FSM_FLST_NODE;
        // remove node from used_list
        flst_remove(fsm_header + UNDO_FSM_USED_LIST, node + UNDO_FSM_FLST_NODE, mtr);


        // get undo log page and check: insert or update
        
        // add node to insert_list
        flst_add_first(fsm_header + UNDO_FSM_INSERT_LIST, node + UNDO_FSM_FLST_NODE, mtr);

        //
        node_addr = flst_get_first(fsm_header + UNDO_FSM_USED_LIST, mtr);
    }

    mtr_commit(mtr);

    return CM_SUCCESS;
}


static inline bool32 undo_fsm_extend(uint32 space_id, undo_fsm_header_t* fsm_header, mtr_t* mtr)
{
    ut_ad(fsm_header);

    // check
    uint32 status = mlog_read_uint32(fsm_header + UNDO_FSM_STATUS, MLOG_4BYTES);
    if (status == UNDO_FSM_STATUS_FULL) {
        return FALSE;
    }

    // alloc undo log page

    uint32 size = mlog_read_uint32(fsm_header + UNDO_FSM_LOG_PAGE_COUNT, MLOG_4BYTES);
    uint32 max_size = mlog_read_uint32(fsm_header + UNDO_FSM_LOG_PAGE_MAX_COUNT, MLOG_4BYTES);
    uint32 page_count = max_size - size > FSP_EXTENT_SIZE ? FSP_EXTENT_SIZE : max_size - size;

    if (page_count == 0) {
        // set full
        mlog_write_uint32(fsm_header + UNDO_FSM_STATUS, UNDO_FSM_STATUS_FULL, MLOG_4BYTES, mtr);
        return FALSE;
    }

    if (undo_fsm_alloc_log_pages(space_id, fsm_header, page_count, mtr) != CM_SUCCESS) {
        return FALSE;
    }

    return TRUE;
}

static inline undo_fsm_page_t undo_fsm_alloc_page_from_insert_list(uint32 space_id,
    undo_fsm_header_t* fsm_header, mtr_t* mtr)
{
    undo_fsm_page_t fsm_page = {space_id, FIL_NULL, FIL_NULL, 0};

    // get frist node from insert_list
    fsm_page.node_addr = flst_get_first(fsm_header + UNDO_FSM_INSERT_LIST, mtr);
    if (fil_addr_is_null(fsm_page.node_addr)) {
        return fsm_page;
    }

    // get node by node_addr
    undo_fsm_node_t* node;
    node = flst_get_buf_ptr(space_id, undo_log_page_size, fsm_page.node_addr, RW_X_LATCH, mtr, NULL) - UNDO_FSM_FLST_NODE;

    // remove node from insert_list
    flst_remove(fsm_header + UNDO_FSM_INSERT_LIST, node + UNDO_FSM_FLST_NODE, mtr);

    // add node to used_list
    flst_add_first(fsm_header + UNDO_FSM_USED_LIST, node + UNDO_FSM_FLST_NODE, mtr);

    return fsm_page;
}

static inline undo_fsm_page_t undo_fsm_alloc_page_from_update_list(uint32 space_id,
    undo_fsm_header_t* fsm_header, bool32 is_forced, uint64 min_scn, mtr_t* mtr)
{
    undo_fsm_page_t fsm_page = {space_id, FIL_NULL, FIL_NULL, 0};

    // get frist node from insert_list
    fsm_page.node_addr = flst_get_first(fsm_header + UNDO_FSM_UPDATE_LIST, mtr);
    if (fil_addr_is_null(fsm_page.node_addr)) {
        return fsm_page;
    }

    // get node by node_addr
    undo_fsm_node_t* node;
    node = flst_get_buf_ptr(space_id, undo_log_page_size, fsm_page.node_addr, RW_X_LATCH, mtr, NULL) - UNDO_FSM_FLST_NODE;

    // check retention time and retention size
    uint32 scn_timestamp = mlog_read_uint32(node + UNDO_FSM_NODE_SCN_TIMESTAMP, MLOG_4BYTES);
    uint32 retention_time = (min_scn >> 32);
    if (g_timer()->now_us < retention_time + scn_timestamp && !is_forced) {
        fsm_page.node_addr.page = FIL_NULL;
        return fsm_page;
    }

    // remove node from update_list
    flst_remove(fsm_header + UNDO_FSM_UPDATE_LIST, node + UNDO_FSM_FLST_NODE, mtr);

    // add node to used_list
    flst_add_first(fsm_header + UNDO_FSM_USED_LIST, node + UNDO_FSM_FLST_NODE, mtr);

    return fsm_page;
}

inline undo_fsm_page_t undo_fsm_alloc_page(uint32 space_id, uint64 min_scn)
{
    mtr_t init_mtr, *mtr = &init_mtr;
    undo_fsm_page_t fsm_page = {space_id, FIL_NULL, FIL_NULL, 0};

    mtr_start(mtr);

    // get root fsm page
    undo_fsm_header_t* fsm_header = undo_fsm_get_fsm_header(space_id, mtr);

    //
    bool32 is_force_from_update_list = FALSE;
    while (fsm_page.page_no == FIL_NULL) {
        // alloc from insert_list
        fsm_page = undo_fsm_alloc_page_from_insert_list(space_id, fsm_header, mtr);
        if (fsm_page.page_no != FIL_NULL) {
            ut_ad(!fil_addr_is_null(fsm_page.node_addr));
            break;
        }

        // alloc from update_list
        fsm_page = undo_fsm_alloc_page_from_update_list(space_id, fsm_header, is_force_from_update_list, min_scn, mtr);
        if (fsm_page.page_no != FIL_NULL || is_force_from_update_list) {
            // found or no space left
            ut_ad(fsm_page.page_no == FIL_NULL || !fil_addr_is_null(fsm_page.node_addr));
            break;
        }
        is_force_from_update_list = TRUE;

        // extend undo file
        if (undo_fsm_extend(space_id, fsm_header, mtr) != CM_SUCCESS) {
            break;
        }
    }

    mtr_commit(mtr);

    return fsm_page;
}

inline void undo_fsm_free_page(uint32 space_id, undo_fsm_page_t fsm_page,
    uint32 undo_page_type, uint64 scn_timestamp)
{
    mtr_t init_mtr, *mtr = &init_mtr;
    undo_fsm_header_t* fsm_header;
    undo_fsm_node_t* node;

    ut_a(fsm_page.node_addr.page != FIL_NULL);

    mtr_start(mtr);

    // get root fsm page
    fsm_header = undo_fsm_get_fsm_header(space_id, mtr);
    // get node by node_addr
    node = flst_get_buf_ptr(space_id, undo_log_page_size, fsm_page.node_addr, RW_X_LATCH, mtr, NULL) - UNDO_FSM_FLST_NODE;

    // remove node from used_list
    flst_remove(fsm_header + UNDO_FSM_USED_LIST, node + UNDO_FSM_FLST_NODE, mtr);

    // add node to insert_list
    ut_ad(undo_page_type == UNDO_PAGE_TYPE_INSERT || undo_page_type == UNDO_PAGE_TYPE_UPDATE);
    if (undo_page_type == UNDO_PAGE_TYPE_INSERT) {
        flst_add_first(fsm_header + UNDO_FSM_INSERT_LIST, node + UNDO_FSM_FLST_NODE, mtr);
    } else {
        mlog_write_uint32(node + UNDO_FSM_NODE_SCN_TIMESTAMP, (uint32)scn_timestamp, MLOG_4BYTES, mtr);
        flst_add_last(fsm_header + UNDO_FSM_UPDATE_LIST, node + UNDO_FSM_FLST_NODE, mtr);
    }

    mtr_commit(mtr);
}


