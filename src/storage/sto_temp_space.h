#ifndef _STO_TEMP_SPACE_H
#define _STO_TEMP_SPACE_H

#include "cm_type.h"
#include "cm_list.h"
#include "cm_mutex.h"
#include "mtr_mtr.h"
#include "cm_memory.h"
#include "sto_space.h"

#define TEMP_SPACE_INVALID_PAGE_ID           (uint64)-1

#define TEMP_SPACE_XDESC_BITMAP_SIZE         8
#define TEMP_SPACE_PAGE_SIZE                 (128 * 1024)

typedef struct st_temp_space_xdesc {
    UT_LIST_NODE_T(struct st_temp_space_xdesc) list_node;
    uint64          id;
    union {
        byte        bits[TEMP_SPACE_XDESC_BITMAP_SIZE];
        uint64      value;
    } bitmap;
} temp_space_xdesc_t;

typedef struct st_temp_space_fil_node {
    uint32           id;
    spinlock_t       lock;
    uint32           page_max_count;
    uint32           page_hwm;
    uint64           extend_size;
    os_file_t        file;
    uint32           is_extend : 1;
    uint32           io_in_progress : 1;
    uint32           reserved : 30;
    UT_LIST_BASE_NODE_T(temp_space_xdesc_t) free_xdescs;
    UT_LIST_BASE_NODE_T(memory_page_t) xdesc_pages;
} temp_space_fil_node_t;

#define TEMP_FIL_NODE_COUNT        8

typedef struct st_temp_space {
    memory_pool_t         *pool;
    temp_space_fil_node_t  nodes[TEMP_FIL_NODE_COUNT];
    uint32                 node_count;
} temp_space_t;

void temp_space_init(memory_pool_t *pool);
bool32 open_or_create_temp_space_file(char *file_name, bool32 is_create, uint64 size, uint64 max_size, uint64 extend_size);

bool32 temp_space_swap_in(memory_page_t *page, uint64 swid);
bool32 temp_space_swap_out(memory_page_t *page, uint64 *swid);
bool32 temp_space_swap_clean(uint64 swid);

#endif  /* _STO_TEMP_SPACE_H */
