#ifndef _KNL_HEAP_FSM_H
#define _KNL_HEAP_FSM_H

#include "cm_type.h"
#include "knl_dict.h"

typedef byte heap_fsm_header_t;

#define FSM_HEADER                  FIL_PAGE_DATA

#define FSM_LEVEL0_FIRST_PAGE       0
#define FSM_LEVEL1_FIRST_PAGE       4
#define FSM_LEVEL2_FIRST_PAGE       8
#define FSM_HEAP_FIRST_PAGE         12
#define FSM_HEAP_LAST_PAGE          16
#define FSM_HEAP_PAGE_COUNT         20
#define FSM_FSEG_FREE               24  // not used
#define FSM_FSEG_NOT_FULL           (24 + FLST_BASE_NODE_SIZE)  // not used
#define FSM_FSEG_FULL               (24 + 2 * FLST_BASE_NODE_SIZE)
#define FSM_FSEG_FRAG_ARR           (24 + 3 * FLST_BASE_NODE_SIZE)  /* array of individual pages */

#define FSM_FSEG_INODE_SIZE         (3 * FLST_BASE_NODE_SIZE + FSP_EXTENT_SIZE * FSM_NODE_PAGE_NO_SIZE)
#define FSM_HEADER_SIZE             (24 + FSM_FSEG_INODE_SIZE)

typedef byte      heap_fsm_nodes_t;

#define FSM_NODES                   (FIL_PAGE_DATA + FSM_HEADER_SIZE)

#define FSM_NODES_NEXT_SLOT         0
#define FSM_NODES_PAGE_COUNT        4
#define FSM_NODES_ARRAY             8
#define FSM_NODES_ARRAY_LEAF        (8 + FSM_NON_LEAF_NODES_PER_PAGE)
#define FSM_NODES_PAGES             (8 + FSM_NON_LEAF_NODES_PER_PAGE + FSM_LEAF_NODES_PER_PAGE)

#define FSM_NON_LEAF_NODES_PER_PAGE 2047
#define FSM_LEAF_NODES_PER_PAGE     2048
#define FSM_NODES_PER_PAGE          (FSM_NON_LEAF_NODES_PER_PAGE + FSM_LEAF_NODES_PER_PAGE)
#define FSM_NODE_PAGE_NO_SIZE       4


//----------------------------------------------------------------------

#define FSM_INVALID_SLOT            (-1)
#define FSM_HEAP_MAX_CATALOG        127
#define FSM_PATH_MAX_LEVEL          2
#define FSM_PATH_LEVEL_COUNT        3
#define FSM_IS_LEAF_PAGE(level)     (level == 0)

typedef struct st_fsm_path_node
{
    page_no_t page_no;
    int32     page_slot_in_upper_level;
} fsm_path_node_t;

typedef struct st_fsm_search_path
{
    int32 space_id;
    uint8 category;
    uint8 reserved[3];
    fsm_path_node_t nodes[FSM_PATH_LEVEL_COUNT];
} fsm_search_path_t;



//----------------------------------------------------------------------

extern bool32 fsm_truncate_avail(page_t* page, int32 nslots);
extern bool32 fsm_rebuild_page(page_t* page, mtr_t* mtr);

extern inline uint8 fsm_get_needed_to_category(dict_table_t* table, uint32 size);
extern inline uint8 fsm_space_avail_to_category(dict_table_t* table, uint32 avail);

extern void fsm_recursive_set_catagory(dict_table_t* table, fsm_search_path_t& addr, uint8 value, mtr_t* mtr);


extern uint32 fsm_create(uint32     space_id);
extern bool32 fsm_alloc_heap_page(dict_table_t* table, uint32 page_count, mtr_t* mtr);
extern void fsm_add_free_page(uint32 space_id, uint32 root_page_no, uint32 free_page_no, mtr_t* mtr);
extern page_no_t fsm_search_free_page(dict_table_t* table, uint8 min_category, fsm_search_path_t& search_path);

#endif  /* _KNL_HEAP_FSM_H */
