#ifndef _KNL_UNDO_FSM_H
#define _KNL_UNDO_FSM_H

#include "cm_type.h"
#include "knl_dict.h"

typedef byte undo_fsm_header_t;

#define UNDO_FSM_HEADER                 FIL_PAGE_DATA

#define UNDO_FSM_STATUS                 0  // 0 - normal, 1 - full
#define UNDO_FSM_FSM_PAGE_COUNT         4  // count for fsm page
#define UNDO_FSM_LOG_PAGE_COUNT         8  // current count for log page
#define UNDO_FSM_LOG_PAGE_MAX_COUNT     12 // max count for log page
#define UNDO_FSM_UNUSED_LIST            16
#define UNDO_FSM_INSERT_LIST            (16 + FLST_BASE_NODE_SIZE)
#define UNDO_FSM_UPDATE_LIST            (16 + 2 * FLST_BASE_NODE_SIZE)
#define UNDO_FSM_USED_LIST              (16 + 3 * FLST_BASE_NODE_SIZE)

#define UNDO_FSM_HEADER_SIZE            (16 + 4 * FLST_BASE_NODE_SIZE)



// ---------------------------------------------------------

#define UNDO_FSM_NODE                   FIL_PAGE_DATA


typedef byte undo_fsm_node_t;


#define UNDO_FSM_NODE_SCN_TIMESTAMP     0   // ONLY timestamp
#define UNDO_FSM_NODE_PAGE              4
#define UNDO_FSM_FLST_NODE              8   // FLST_NODE_SIZE

#define UNDO_FSM_NODE_SIZE              (8 + FLST_NODE_SIZE)


#define UNDO_PAGE_TYPE_INSERT           1
#define UNDO_PAGE_TYPE_UPDATE           2

typedef struct st_undo_fsm_page {
    uint32      space_id;
    uint32      page_no;
    fil_addr_t  node_addr;
} undo_fsm_page_t;


//----------------------------------------------------------------------

extern status_t undo_fsm_tablespace_init(uint32 space_id, uint64 init_size, uint64 max_size);
extern status_t undo_fsm_recovery_fsp_pages(uint32 space_id);

extern inline undo_fsm_page_t undo_fsm_alloc_page(uint32 space_id, uint64 min_scn);
extern inline void undo_fsm_free_page(uint32 space_id, undo_fsm_page_t fsm_page, uint32 undo_page_type, uint64 scn_timestamp);


#endif  /* _KNL_UNDO_FSM_H */
