#include "knl_heap_fsm.h"
#include "cm_log.h"
#include "knl_buf.h"
#include "knl_flst.h"
#include "knl_page.h"
#include "knl_fsp.h"
#include "knl_heap.h"

#define FSM_CATEGORIES       127

#define FSM_ROOT_LEVEL       3
#define FSM_BOTTOM_LEVEL     0

#define fsm_get_node_value(fsm_nodes, node_no)                        \
    mach_read_from_1(fsm_nodes + FSM_NODES_ARRAY + node_no)

#define fsm_set_node_value(fsm_nodes, node_no, value, mtr)            \
    mlog_write_uint32(fsm_nodes + FSM_NODES_ARRAY + node_no, value, MLOG_1BYTE, mtr)

#define fsm_get_next_slot(fsm_nodes)                                  \
    mach_read_from_4(fsm_nodes + FSM_NODES_NEXT_SLOT)

#define fsm_set_next_slot(fsm_nodes, value)                           \
    mach_write_to_4(fsm_nodes + FSM_NODES_NEXT_SLOT, value)

#define fsm_get_node_page_no(fsm_nodes, node_no)            \
    mach_read_from_4(fsm_nodes + FSM_NODES_PAGES + node_no * FSM_NODE_PAGE_NO_SIZE)

#define fsm_get_nodes(page)    (page + FSM_NODES)


/* Macros to navigate the tree within a page. Root has index zero. */
#define left_child(x)    (2 * (x) + 1)
#define right_child(x)   (2 * (x) + 2)
#define parent_of(x)     (((x) - 1) / 2)

/*
 * Find right neighbor of x, wrapping around within the level
 */
static int32 right_neighbor(int x)
{
    // Move right.
    // This might wrap around, stepping to the leftmost node at the next level.
    x++;

    /*
     * Check if we stepped to the leftmost node at next level, and correct if so.
     * The leftmost nodes at each level are numbered x = 2^level - 1,
     * so check if (x + 1) is a power of two, using a standard twos-complement-arithmetic trick.
     */
    if (((x + 1) & x) == 0) {
        x = parent_of(x);
    }

    return x;
}

// Sets the value of a slot on page.
// Returns true if the page was modified.
// The caller must hold an exclusive lock on the page.
static bool32 fsm_set_avail(buf_block_t* block, int32 slot, uint8 value, bool32* is_modify_root, mtr_t* mtr)
{
    uint8 old_value;
    int32 node_no = FSM_NON_LEAF_NODES_PER_PAGE + slot;
    heap_fsm_nodes_t* fsm_nodes = fsm_get_nodes(buf_block_get_frame(block));

    ut_ad(slot < FSM_LEAF_NODES_PER_PAGE);

    old_value = fsm_get_node_value(fsm_nodes, node_no);
    // If the value hasn't changed, we don't need to do anything
    if (old_value == value && value <= fsm_get_node_value(fsm_nodes, 0)) {
        return FALSE;
    }

    fsm_set_node_value(fsm_nodes, node_no, value, mtr);

    // Propagate up, until we hit the root or a node that doesn't need to be updated
    do {
        uint8   new_value = 0;
        int     lchild;
        int     rchild;

        node_no = parent_of(node_no);
        lchild = left_child(node_no);
        rchild = lchild + 1;

        new_value = fsm_get_node_value(fsm_nodes, lchild);
        if (rchild < FSM_NODES_PER_PAGE) {
            new_value = ut_max(new_value, fsm_get_node_value(fsm_nodes, rchild));
        }

        old_value = fsm_get_node_value(fsm_nodes, node_no);
        if (old_value == new_value) {
            break;
        }

        fsm_set_node_value(fsm_nodes, node_no, new_value, mtr);
    } while (node_no > 0);

    // sanity check:
    // if the new value is (still) higher than the value at the top, the tree is corrupt. If so, rebuild.
    ut_a(value <= fsm_get_node_value(fsm_nodes, 0));
    //if (value > fsm_get_node_value(fsm_nodes, 0)) {
    //    fsm_rebuild_page(page);
    //}

    if (is_modify_root) {
        *is_modify_root = (node_no == 0) ? TRUE : FALSE;
    }

    return TRUE;
}

// Returns the value of given slot on page.
// Since this is just a read-only access of a single byte, the page doesn't need to be locked.
static inline uint8 fsm_get_avail(page_t* page, int32 slot)
{
    heap_fsm_nodes_t* fsm_nodes = fsm_get_nodes(page);

    ut_ad(slot < FSM_LEAF_NODES_PER_PAGE);

    return fsm_get_node_value(fsm_nodes, FSM_NON_LEAF_NODES_PER_PAGE + slot);
}

// Returns the value at the root of a page.
// Since this is just a read-only access of a single byte, the page doesn't need to be locked.
static inline uint8 fsm_get_max_avail(page_t* page)
{
    heap_fsm_nodes_t* fsm_nodes = fsm_get_nodes(page);

    return fsm_get_node_value(fsm_nodes, 0);
}

// Searches for a slot with category at least minvalue.
// Returns slot number, or -1 if none found.
// The caller must hold at least a shared lock on the page.
static int32 fsm_search_avail(buf_block_t* block, uint8 min_value)
{
    heap_fsm_nodes_t* fsm_nodes;
    int32             node_no;
    int32             target;

    fsm_nodes = fsm_get_nodes(buf_block_get_frame(block));

    // Check the root first, and exit quickly if there's no leaf with enough free space
    if (fsm_get_node_value(fsm_nodes, 0) < min_value) {
        return FSM_INVALID_SLOT;
    }

    // Start search using fp_next_slot.
    // It's just a hint, so check that it's sane.
    // (This also handles wrapping around when the prior call returned the last slot on the page.)

    target = fsm_get_next_slot(fsm_nodes);
    if (target < 0 || target >= FSM_LEAF_NODES_PER_PAGE) {
        target = 0;
    }
    target += FSM_NON_LEAF_NODES_PER_PAGE;

    /*
     * Start the search from the target slot.
     * At every step, move one node to the right, then climb up to the parent.
     * Stop when we reach a node with enough free space (as we must, since the root has enough space).
     */
    node_no = target;
    while (node_no > 0)
    {
        if (fsm_get_node_value(fsm_nodes, node_no) >= min_value) {
            break;
        }

        // Move to the right, wrapping around on same level if necessary, then climb up.
        node_no = parent_of(right_neighbor(node_no));
    }

    // We're now at a node with enough free space, somewhere in the middle of the tree.
    // Descend to the bottom, following a path with enough free space, preferring to move left if there's a choice.

    while (node_no < FSM_NON_LEAF_NODES_PER_PAGE) {
        int child_node_no = left_child(node_no);

        if (child_node_no < FSM_NODES_PER_PAGE && fsm_get_node_value(fsm_nodes, child_node_no) >= min_value) {
            node_no = child_node_no;
            continue;
        }

        child_node_no++;  // point to right child
        if (child_node_no < FSM_NODES_PER_PAGE && fsm_get_node_value(fsm_nodes, child_node_no) >= min_value) {
            node_no = child_node_no;
            continue;
        }

        LOGGER_FATAL(LOGGER, LOG_MODULE_HEAP_FSM, "corrupt FSM block, space id %u page no %u",
            block->get_space_id(), block->get_page_no());
        ut_error;
    }

    if (unlikely(node_no < FSM_NON_LEAF_NODES_PER_PAGE)) {
        LOGGER_FATAL(LOGGER, LOG_MODULE_HEAP_FSM, "corrupt FSM block, space id %u page no %u",
            block->get_space_id(), block->get_page_no());
        ut_error;
    }

    // We're now at the bottom level, at a node with enough space.
    int32 slot = node_no - FSM_NON_LEAF_NODES_PER_PAGE;

    // Update the next-target pointer
    // Note that we do this even if we're only holding a shared lock
    fsm_set_next_slot(fsm_nodes, slot + 1);

    return slot;
}

// Sets the available space to zero for all slots numbered >= nslots.
// Returns true if the page was modified.
bool32 fsm_truncate_avail(page_t* page, int32 nslots)
{
    mtr_t mtr;
    bool32 changed = FALSE;
    heap_fsm_nodes_t* fsm_nodes = fsm_get_nodes(page);

    ut_ad(nslots >= 0 && nslots < FSM_LEAF_NODES_PER_PAGE);

    mtr_start(&mtr);

    // Clear all truncated leaf nodes
    for (int32 i = FSM_NON_LEAF_NODES_PER_PAGE + nslots; i < FSM_NODES_PER_PAGE; i++) {
        if (fsm_get_node_value(fsm_nodes, i) != 0) {
            fsm_set_node_value(fsm_nodes, i, 0, &mtr);
            changed = TRUE;
        }
    }

    // Fix upper nodes
    if (changed) {
        fsm_rebuild_page(page, &mtr);
    }

    mtr_commit(&mtr);

    return changed;
}

// Reconstructs the upper levels of a page.
// Returns true if the page was modified.
bool32 fsm_rebuild_page(page_t* page, mtr_t* mtr)
{
    heap_fsm_nodes_t* fsm_nodes = fsm_get_nodes(page);
    bool32 changed = FALSE;
    int32 node_no;

    // Start from the lowest non-leaf level,
    // at last node, working our way backwards, through all non-leaf nodes at all levels, up to the root.

    for (node_no = FSM_NON_LEAF_NODES_PER_PAGE - 1; node_no >= 0; node_no--) {
        int32 lchild = left_child(node_no);
        int32 rchild = lchild + 1;
        uint8 new_value = 0;

        // The first few nodes we examine might have zero or one child
        if (lchild < FSM_NODES_PER_PAGE) {
            new_value = fsm_get_node_value(fsm_nodes, lchild);
        }
        if (rchild < FSM_NODES_PER_PAGE) {
            new_value = ut_max(new_value, fsm_get_node_value(fsm_nodes, rchild));
        }

        if (fsm_get_node_value(fsm_nodes, node_no) != new_value) {
            fsm_set_node_value(fsm_nodes, node_no, new_value, mtr);
            changed = TRUE;
        }
    }

    return changed;
}

static inline page_no_t fsm_get_child(buf_block_t* block, int32 slot)
{
    ut_ad(slot >= 0);

    heap_fsm_nodes_t* nodes = fsm_get_nodes(buf_block_get_frame(block));
    return mach_read_from_4(nodes + FSM_NODES_PAGES + slot * FSM_NODE_PAGE_NO_SIZE);
}

// Return the heap block number corresponding to given location in the FSM.
static inline page_no_t fsm_get_heap_page_no(buf_block_t* block, int32 slot)
{
    ut_ad(slot >= 0);

    heap_fsm_nodes_t* nodes = fsm_get_nodes(buf_block_get_frame(block));
    return mach_read_from_4(nodes + FSM_NODES_PAGES + slot * FSM_NODE_PAGE_NO_SIZE);
}

inline uint8 fsm_get_needed_to_category(dict_table_t* table, uint32 size)
{
    int32 category;
    uint16 reserved_size = table->pctfree * UNIV_PAGE_SIZE_DEF / 100;
    uint16 step = (UNIV_PAGE_SIZE_DEF - reserved_size) / (FSM_CATEGORIES - 1);

    if (size == 0) {
        return 1;
    }

    category = 1 + (size + step - 1) / step;
    if (category > FSM_CATEGORIES) {
        category = FSM_CATEGORIES;
    }

    return (uint8)category;
}

// Return category corresponding x bytes of free space
inline uint8 fsm_space_avail_to_category(dict_table_t* table, uint32 avail)
{
    int32 cat;
    uint32 reserved_size = table->pctfree * UNIV_PAGE_SIZE_DEF / 100;
    uint32 step = (UNIV_PAGE_SIZE_DEF - reserved_size) / (FSM_CATEGORIES - 1);

    if (avail <= reserved_size) {
        return 0;
    }

    ut_ad(avail < UNIV_PAGE_SIZE_DEF);

    // 0: [0, pctfree]
    // 1: (pctfree, pctfree + step]
    // 2: (pctfree + step, pctfree + 2 * step]
    // 3: (pctfree + 2 * step, pctfree + 3 * step]

    cat = 1 + (avail - reserved_size) / step;
    if (cat > FSM_CATEGORIES) {
        cat = FSM_CATEGORIES;
    }

    return (uint8)cat;
}

// Set value in given FSM page and slot
void fsm_recursive_set_catagory(dict_table_t* table, fsm_search_path_t& search_path, uint8 value, mtr_t* mtr)
{
    page_id_t page_id(table->space_id, search_path.nodes[0].page_no);
    const page_size_t page_size(0);

    for (uint32 i  = 0; i < FSM_PATH_LEVEL_COUNT; i++) {
        page_id.set_page_no(search_path.nodes[i].page_no);
        buf_block_t* block = buf_page_get(page_id, page_size, RW_X_LATCH, mtr);
        ut_ad(block->get_page_no() == search_path.nodes[i].page_no);
        ut_ad(block->get_page_type() == FIL_PAGE_HEAP_FSM);
        //buf_block_dbg_add_level(block, SYNC_DICT_HEADER);

        bool32 is_modify_root = FALSE;
        if (!fsm_set_avail(block, search_path.nodes[i].page_slot_in_upper_level, value, &is_modify_root, mtr)) {
            break;
        }
        if (!is_modify_root) {
            break;
        }
    }
}

static void fsm_wait_extend_table_segment_done(dict_table_t* table)
{
    bool32 is_in_progress = TRUE;

    while (is_in_progress) {
        mutex_enter(&table->mutex);
        is_in_progress = table->heap_io_in_progress;
        mutex_exit(&table->mutex);

        if (is_in_progress) {
            os_thread_sleep(100);
        }
    }
}

static bool32 fsm_extend_table_segment(dict_table_t* table)
{
    mutex_enter(&table->mutex);
    if (table->heap_io_in_progress) {
        mutex_exit(&table->mutex);
        fsm_wait_extend_table_segment_done(table);
        return TRUE;
    }
    table->heap_io_in_progress = TRUE;
    mutex_exit(&table->mutex);

    mtr_t mtr;

    mtr_start(&mtr);

    const page_size_t page_size(0);
    const page_id_t page_id(table->space_id, table->entry_page_no);
    buf_block_t* block = buf_page_get(page_id, page_size, RW_NO_LATCH, &mtr);
    ut_ad(block->get_page_no() == table->entry_page_no);
    ut_ad(block->get_page_type() == FIL_PAGE_HEAP_FSM);
    //buf_block_dbg_add_level(block, SYNC_DICT_HEADER);

    page_t* page = buf_block_get_frame(block);
    heap_fsm_header_t* fsm_header = FSM_HEADER + page;

    uint32 used = mach_read_from_4(fsm_header + FSM_HEAP_PAGE_COUNT);
    uint32 page_count;
    if (used < FSP_EXTENT_SIZE) {
        page_count = 8;
    } else if (used < FSP_EXTENT_SIZE * 128) {
        page_count = FSP_EXTENT_SIZE;
    } else if (used < FSP_EXTENT_SIZE * 1024) {
        page_count = FSP_EXTENT_SIZE * 8;
    } else {
        page_count = FSP_EXTENT_SIZE * 64;
    }

    bool32 ret = fsm_alloc_heap_page(table, page_count, &mtr);

    mtr_commit(&mtr);

    if (!ret) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_HEAP_FSM,
            "failed to extend segment of table, space id %u table name %s extend page count %u",
            table->space_id, table->name, page_count);
    }

    mutex_enter(&table->mutex);
    ut_ad(table->heap_io_in_progress);
    table->heap_io_in_progress = FALSE;
    mutex_exit(&table->mutex);

    return ret;
}

// Search the tree for a heap page with at least min_cat of free space
page_no_t fsm_search_free_page(dict_table_t* table, uint8 min_category, fsm_search_path_t& search_path)
{
    int restarts = 0;
    page_id_t page_id(table->space_id, table->entry_page_no);
    const page_size_t page_size(0);
    mtr_t mtr;
    page_no_t page_no;
    uint32 level = FSM_PATH_MAX_LEVEL;

    mtr_start(&mtr);

    search_path.nodes[level].page_no= table->entry_page_no;

    for (;;) {
        // Read the FSM page
        page_id.set_page_no(search_path.nodes[level].page_no);
        buf_block_t* block = buf_page_get(page_id, page_size, RW_NO_LATCH, &mtr);
        ut_ad(block->get_page_no() == search_path.nodes[level].page_no);
        ut_ad(block->get_page_type() == FIL_PAGE_HEAP_FSM);
        //buf_block_dbg_add_level(block, SYNC_DICT_HEADER);

        buf_block_lock(block, RW_S_LATCH, &mtr);

        // Search within the page
        int32 slot = fsm_search_avail(block, min_category);
        search_path.nodes[level].page_slot_in_upper_level = slot;

        if (slot != FSM_INVALID_SLOT) {
            // Descend the tree, or return the found block if we're at the bottom.
            if (level == FSM_BOTTOM_LEVEL) {
                page_no = fsm_get_heap_page_no(block, slot);
                break;
            }

            level--;
            search_path.nodes[level].page_no = fsm_get_child(block, slot);

            buf_block_unlock(block, RW_S_LATCH, &mtr);
        } else if (level == FSM_PATH_MAX_LEVEL) {
            // At the root, there's no page with enough free space in the FSM.
            mtr_commit(&mtr);

            if (!fsm_extend_table_segment(table)) {
                return INVALID_PAGE_NO;
            }

            // Start search all over from the root
            mtr_start(&mtr);
        } else {
            mtr_commit(&mtr);

            // If child page has been updated and parent page has not been updated,
            // need to loop quite a few times
            if (restarts++ > 10000) {
                return INVALID_PAGE_NO;
            }

            // Start search all over from the root
            level = FSM_PATH_MAX_LEVEL;
            mtr_start(&mtr);
        }
    }

    mtr_commit(&mtr);

    return page_no;
}



static buf_block_t* fsm_get_right_page(uint32 space_id, page_t* page, mtr_t* mtr)
{
    uint32 page_no;
    buf_block_t* block = NULL;

    page_no = mach_read_from_4(page + FIL_PAGE_NEXT);

    const page_id_t page_id(space_id, page_no);
    const page_size_t page_size(0);

    if (page_no != FIL_NULL) {
        block = buf_page_get(page_id, page_size, RW_S_LATCH, mtr);
    }

    return NULL;
}


static buf_block_t* fsm_get_map_page_with_x_latch(uint32 space_id, uint32 page_no, mtr_t* mtr)
{
    const page_id_t page_id(space_id, page_no);
    const page_size_t page_size(0);
    buf_block_t* block = buf_page_get(page_id, page_size, RW_X_LATCH, mtr);
    ut_ad(block->get_page_no() == page_no);
    ut_ad(block->get_page_type() == FIL_PAGE_HEAP_FSM);
    //buf_block_dbg_add_level(block, SYNC_DICT_HEADER);

    return block;
}

static buf_block_t* fsm_get_last_leaf_map_page(fsm_search_path_t& search_path, mtr_t* mtr)
{
    const page_size_t page_size(0);
    buf_block_t* block = NULL;
    uint32 level = FSM_PATH_MAX_LEVEL;

    while (TRUE) {
        const page_id_t page_id(search_path.space_id, search_path.nodes[level].page_no);

        block = buf_page_get(page_id, page_size,
            FSM_IS_LEAF_PAGE(level) ? RW_X_LATCH : RW_S_LATCH, mtr);
        ut_ad(block->get_page_no() == search_path.nodes[level].page_no);
        ut_ad(block->get_page_type() == FIL_PAGE_HEAP_FSM);
        //buf_block_dbg_add_level(block, SYNC_DICT_HEADER);

        page_t* page = buf_block_get_frame(block);

        uint32 right_page_no = mach_read_from_4(page + FIL_PAGE_NEXT);
        if (right_page_no != FIL_NULL) {
            buf_block_unlock(block, FSM_IS_LEAF_PAGE(level) ? RW_X_LATCH : RW_S_LATCH, mtr);
            search_path.nodes[level].page_no = right_page_no;
            continue;
        }

        if (FSM_IS_LEAF_PAGE(level)) {
            break;
        }

        heap_fsm_nodes_t* fsm_nodes = FSM_NODES + page;

        level--;
        search_path.nodes[level].page_slot_in_upper_level = mach_read_from_4(fsm_nodes + FSM_NODES_PAGE_COUNT);
        ut_ad(search_path.nodes[level].page_slot_in_upper_level > 0);
        search_path.nodes[level].page_no =
            mach_read_from_4(fsm_nodes + FSM_NODES_PAGES + (search_path.nodes[level].page_slot_in_upper_level - 1) * FSM_NODE_PAGE_NO_SIZE);

        // non-leaf page, it must be S-LATCH
        buf_block_unlock(block, RW_S_LATCH, mtr);
    }

    return block;
}

static inline void fsm_add_free_page_to_map(buf_block_t* owner_block, uint32 free_page_no, mtr_t* mtr)
{
    page_t* page = buf_block_get_frame(owner_block);
    heap_fsm_nodes_t* nodes = FSM_NODES + page;
    uint32 page_count = mach_read_from_4(nodes + FSM_NODES_PAGE_COUNT);

    mlog_write_uint32(nodes + FSM_NODES_ARRAY_LEAF + page_count, FSM_HEAP_MAX_CATALOG, MLOG_4BYTES, mtr);
    mlog_write_uint32(nodes + FSM_NODES_PAGES + page_count * FSM_NODE_PAGE_NO_SIZE, free_page_no, MLOG_4BYTES, mtr);
    mlog_write_uint32(nodes + FSM_NODES_PAGE_COUNT, page_count + 1, MLOG_4BYTES, mtr);
}

static inline void fsm_add_free_heap_pages_to_map(buf_block_t* owner_block, xdes_t* xdes, mtr_t* mtr)
{
    page_t* page = buf_block_get_frame(owner_block);
    heap_fsm_nodes_t* nodes = FSM_NODES + page;
    uint32 page_count = mach_read_from_4(nodes + FSM_NODES_PAGE_COUNT);

    ut_a(page_count + FSP_EXTENT_SIZE <= FSM_LEAF_NODES_PER_PAGE);

    for (uint32 i = 0; i < FSP_EXTENT_SIZE; i++) {
        uint32 free_page_no = xdes_get_offset(xdes) + i;
        mlog_write_uint32(nodes + FSM_NODES_ARRAY_LEAF + page_count + i,
                          FSM_HEAP_MAX_CATALOG, MLOG_4BYTES, mtr);
        mlog_write_uint32(nodes + FSM_NODES_PAGES + (page_count + i) * FSM_NODE_PAGE_NO_SIZE,
                          free_page_no, MLOG_4BYTES, mtr);
    }
    mlog_write_uint32(nodes+ FSM_NODES_PAGE_COUNT, page_count + FSP_EXTENT_SIZE, MLOG_4BYTES, mtr);
}

static void fsm_add_free_heap_pages_to_fseg(    page_t*     root_map_page, xdes_t* descr, mtr_t* mtr)
{
    heap_fsm_header_t* fsm_header = FSM_HEADER + root_map_page;

    flst_add_last(fsm_header + FSM_FSEG_FULL, descr + XDES_FLST_NODE, mtr);

    for (uint32 i = 0; i < FSP_EXTENT_SIZE; i++) {
        // mark the page as used
        xdes_set_bit(descr, XDES_FREE_BIT, i, FALSE, mtr);
    }

    uint32 page_count = mach_read_from_4(root_map_page + FSM_HEAP_PAGE_COUNT);
    ut_a(page_count >= FSP_EXTENT_SIZE);

    mlog_write_uint32(fsm_header + FSM_HEAP_PAGE_COUNT, page_count + FSP_EXTENT_SIZE, MLOG_4BYTES, mtr);
}

static void fsm_add_free_heap_page_to_fseg(    page_t*     root_map_page, uint32 free_page_no, mtr_t* mtr)
{
    heap_fsm_header_t* fsm_header = FSM_HEADER + root_map_page;

    uint32 page_count = mach_read_from_4(fsm_header + FSM_HEAP_PAGE_COUNT);
    ut_a(page_count < FSP_EXTENT_SIZE);

    mlog_write_uint32(fsm_header + FSM_FSEG_FRAG_ARR + page_count * FSM_NODE_PAGE_NO_SIZE,
                      free_page_no, MLOG_4BYTES, mtr);
    mlog_write_uint32(fsm_header + FSM_HEAP_PAGE_COUNT, page_count + 1, MLOG_4BYTES, mtr);
}

static inline void fsm_append_last_map_page(buf_block_t* block, buf_block_t* new_block, mtr_t* mtr)
{
    mlog_write_uint32(buf_block_get_frame(block) + FIL_PAGE_NEXT, new_block->get_page_no(), MLOG_4BYTES, mtr);
    mlog_write_uint32(buf_block_get_frame(new_block) + FIL_PAGE_PREV, block->get_page_no(), MLOG_4BYTES, mtr);
}

static inline void fsm_page_init(buf_block_t* block, mtr_t* mtr)
{
    page_t* page = buf_block_get_frame(block);

    // 1.

    mlog_write_uint32(page + FIL_PAGE_TYPE, FIL_PAGE_HEAP_FSM, MLOG_2BYTES, mtr);

    // 2.

    heap_fsm_header_t* header = FSM_HEADER + page;

    //mlog_write_uint32(header + FSM_LEVEL0_FIRST_PAGE, leaf_map_block->get_page_no(), MLOG_4BYTES, &mtr);
    //mlog_write_uint32(header + FSM_LEVEL1_FIRST_PAGE, non_leaf_map_block->get_page_no(), MLOG_4BYTES, &mtr);
    //mlog_write_uint32(header + FSM_LEVEL2_FIRST_PAGE, root_map_block->get_page_no(), MLOG_4BYTES, &mtr);
    mlog_write_uint32(header + FSM_HEAP_FIRST_PAGE, FIL_NULL, MLOG_4BYTES, mtr);
    mlog_write_uint32(header + FSM_HEAP_LAST_PAGE, FIL_NULL, MLOG_4BYTES, mtr);
    mlog_write_uint32(header + FSM_HEAP_PAGE_COUNT, 0, MLOG_4BYTES, mtr);

    flst_init(header + FSM_FSEG_FREE, mtr);
    flst_init(header + FSM_FSEG_NOT_FULL, mtr);
    flst_init(header + FSM_FSEG_FULL, mtr);
    for (uint32 i = 0; i < FSP_EXTENT_SIZE; i++) {
        mlog_write_uint32(header + FSM_FSEG_FRAG_ARR, FIL_NULL, MLOG_4BYTES, mtr);
    }
}

static inline uint32 fsm_get_nodes_page_count(buf_block_t* block)
{
    page_t* page = buf_block_get_frame(block);

    ut_ad(mach_read_from_2(page + FIL_PAGE_TYPE) == FIL_PAGE_HEAP_FSM);

    return mach_read_from_4(page + FSM_NODES + FSM_NODES_PAGE_COUNT);
}

// 
bool32 fsm_add_free_extents(uint32 space_id, uint32 root_page_no, xdes_t* xdes, mtr_t* mtr)
{
    fsm_search_path_t search_path;
    search_path.space_id = space_id;
    search_path.nodes[FSM_PATH_MAX_LEVEL].page_no = root_page_no;

    // get leaf map page
    buf_block_t* block0 = fsm_get_last_leaf_map_page(search_path, mtr);
    uint32 page0_count = fsm_get_nodes_page_count(block0);
    ut_a(page0_count == FSP_EXTENT_SIZE || (page0_count % FSP_EXTENT_SIZE) == 0);

    if (page0_count >= FSM_LEAF_NODES_PER_PAGE) {

        // alloc a map page
        const page_size_t page_size(0);
        buf_block_t* new_block0 = fsp_alloc_free_page(space_id, page_size, mtr);
        if (new_block0 == NULL) {
            return FALSE;
        }
        fsm_page_init(new_block0, mtr);

        buf_block_t* block1 = fsm_get_map_page_with_x_latch(space_id, search_path.nodes[1].page_no, mtr);
        uint32 page1_count = fsm_get_nodes_page_count(block1);
        if (page1_count >= FSM_LEAF_NODES_PER_PAGE) {

            buf_block_t* new_block1 = fsp_alloc_free_page(space_id, page_size, mtr);
            if (new_block1 == NULL) {
                const page_id_t page_id(space_id, new_block0->get_page_no());
                fsp_free_page(page_id, page_size, mtr);
                return FALSE;
            }
            fsm_page_init(new_block1, mtr);

            fsm_append_last_map_page(block1, new_block1, mtr);

            // add new map page (level=1) to parent(level=2)
            buf_block_t* block2 = fsm_get_map_page_with_x_latch(space_id, search_path.nodes[2].page_no, mtr);
            fsm_add_free_page_to_map(block2, new_block1->get_page_no(), mtr);

            block1 = new_block1;
        }

        //
        fsm_append_last_map_page(block0, new_block0, mtr);

        // add new map page (level=0) to parent(level=1)
        fsm_add_free_page_to_map(block1, new_block0->get_page_no(), mtr);

        //
        block0 = new_block0;
    }

    // add free page to leaf map page
    fsm_add_free_heap_pages_to_map(block0, xdes, mtr);

    //
    fsm_rebuild_page(buf_block_get_frame(block0), mtr);
    for (int32 i = 1 ; i < FSM_PATH_LEVEL_COUNT; i++) {
        buf_block_t* block = fsm_get_map_page_with_x_latch(space_id, search_path.nodes[i].page_no, mtr);
        page_t* page = buf_block_get_frame(block);
        mlog_write_uint32(page + FSM_NODES + FSM_NODES_ARRAY_LEAF + search_path.nodes[i-1].page_slot_in_upper_level,
                          FSM_HEAP_MAX_CATALOG, MLOG_4BYTES, mtr);
        fsm_rebuild_page(page, mtr);

        if (i == FSM_PATH_MAX_LEVEL) { // ROOT MAP PAGE
            fsm_add_free_heap_pages_to_fseg(page, xdes, mtr);
        }
    }

    return TRUE;
}

// Only for first 64 pages
void fsm_add_free_page(uint32 space_id, uint32 root_page_no, uint32 free_page_no, mtr_t* mtr)
{
    fsm_search_path_t search_path;
    buf_block_t* block[FSM_PATH_LEVEL_COUNT];
    page_t* page;

    search_path.space_id = space_id;
    search_path.nodes[FSM_PATH_MAX_LEVEL].page_no = root_page_no;

    // get leaf map page block and lock
    block[0] = fsm_get_last_leaf_map_page(search_path, mtr);
    page = buf_block_get_frame(block[0]);

    // check: only for frag page
    uint32 page_count = fsm_get_nodes_page_count(block[0]);
    ut_a(page_count < FSP_EXTENT_SIZE);

    // rebuild map page
    fsm_add_free_page_to_map(block[0], free_page_no, mtr);
    fsm_rebuild_page(page, mtr);

    // rebuild non map page and root map page
    for (int32 i = 1; i < FSM_PATH_LEVEL_COUNT; i++) {
        block[i] = fsm_get_map_page_with_x_latch(space_id, search_path.nodes[i].page_no, mtr);
        page = buf_block_get_frame(block[i]);
        mlog_write_uint32(page + FSM_NODES + FSM_NODES_ARRAY_LEAF + search_path.nodes[i-1].page_slot_in_upper_level,
                          FSM_HEAP_MAX_CATALOG, MLOG_4BYTES, mtr);
        fsm_rebuild_page(page, mtr);
    }

    // add page to segement of root map page
    page = buf_block_get_frame(block[FSM_PATH_MAX_LEVEL]);
    fsm_add_free_heap_page_to_fseg(page, free_page_no, mtr);

    // release lock
    buf_block_unlock(block[FSM_PATH_MAX_LEVEL], RW_X_LATCH, mtr);
    buf_block_unlock(block[1], RW_X_LATCH, mtr);
    buf_block_unlock(block[0], RW_X_LATCH, mtr);
}

// Create the root node for a new index tree.
uint32 fsm_create(uint32 space_id)
{
    mtr_t mtr;

    mtr_start(&mtr);

    // 1. alloc pages

    const page_size_t page_size(0);
    buf_block_t* leaf_map_block = fsp_alloc_free_page(space_id, page_size, &mtr);
    buf_block_t* non_leaf_map_block = fsp_alloc_free_page(space_id, page_size, &mtr);
    buf_block_t* root_map_block = fsp_alloc_free_page(space_id, page_size, &mtr);
    if (leaf_map_block == NULL || non_leaf_map_block == NULL || root_map_block == NULL) {
        goto err_exit;
    }

    // 2. fsm page init

    fsm_page_init(leaf_map_block, &mtr);
    fsm_page_init(non_leaf_map_block, &mtr);
    fsm_page_init(root_map_block, &mtr);

    // 3. create map tree

    fsm_add_free_page_to_map(non_leaf_map_block, leaf_map_block->get_page_no(), &mtr);
    fsm_add_free_page_to_map(root_map_block, non_leaf_map_block->get_page_no(), &mtr);
    
    mtr_commit(&mtr);

    return root_map_block->get_page_no();

err_exit:

    if (leaf_map_block) {
        const page_id_t page_id(space_id, leaf_map_block->get_page_no());
        fsp_free_page(page_id, page_size, &mtr);
    }
    if (non_leaf_map_block) {
        const page_id_t page_id(space_id, non_leaf_map_block->get_page_no());
        fsp_free_page(page_id, page_size, &mtr);
    }
    if (root_map_block) {
        const page_id_t page_id(space_id, root_map_block->get_page_no());
        fsp_free_page(page_id, page_size, &mtr);
    }

    mtr_commit(&mtr);

    return FIL_NULL;
}

// Only for first 64 pages, expand 8 pages at a time,
// Protected by table->heap_io_in_progress.
static bool32 fsm_extend_heap_pages(dict_table_t* table, mtr_t* mtr)
{
    const page_size_t page_size(0);
    const uint32 alloc_page_count = 8;
    buf_block_t* block[alloc_page_count] = {NULL};

    for (uint32 i = 0; i < alloc_page_count; i++) {
        block[i] = fsp_alloc_free_page(table->space_id, page_size, mtr);
        if (block[i] == NULL) {
            goto err_exit;
        }
    }

    for (uint32 i = 0; i < alloc_page_count; i++) {
        heap_page_init(block[i], table, mtr);
        fsm_add_free_page(table->space_id, table->entry_page_no, block[i]->get_page_no(), mtr);
    }

    return TRUE;

err_exit:

    for (uint32 i = 0; i < alloc_page_count; i++) {
        if (block[i]) {
            const page_id_t page_id(table->space_id, block[i]->get_page_no());
            fsp_free_page(page_id, page_size, mtr);
        }
    }

    return FALSE;
}

// segment expanded by a maximum of 64MB at a time
static bool32 fsm_extend_heap_extents(dict_table_t* table, uint32 extent_count, mtr_t* mtr)
{
    const page_size_t page_size(0);
    const uint32 alloc_extent_count = 64;
    xdes_t* descr[alloc_extent_count] = {NULL};

    extent_count = extent_count > alloc_extent_count ? alloc_extent_count : extent_count;

    // get free extents
    for (uint32 i = 0; i < extent_count; i++) {
        descr[i] = fsp_alloc_free_extent(table->space_id, page_size, mtr);
        if (descr[i] == NULL) {
            goto err_exit;
        }
    }

    for (uint32 i = 0; i < extent_count; i++) {
        // create pages
        for (uint32 j = 0; j < FSP_EXTENT_SIZE; j++) {
            uint32 page_no = xdes_get_offset(descr[i]) + j;
            buf_block_t* block = fsp_page_create(table->space_id, page_no, mtr, mtr);
            heap_page_init(block, table, mtr);
        }

        // add pages to map page
        fsm_add_free_extents(table->space_id, table->entry_page_no, descr[i], mtr);
    }

    return TRUE;

err_exit:

    for (uint32 i = 0; i < extent_count; i++) {
        if (descr[i]) {
            fsp_free_extent(table->space_id, descr[i], mtr);
        }
    }

    return FALSE;
}

// Protected by table->heap_io_in_progress
bool32 fsm_alloc_heap_page(dict_table_t* table, uint32 page_count, mtr_t* mtr)
{
    if (page_count < FSP_EXTENT_SIZE) {
        return fsm_extend_heap_pages(table, mtr);
    }

    return fsm_extend_heap_extents(table, page_count / FSP_EXTENT_SIZE, mtr);
}

