#ifndef _CM_VIRTUAL_MEM_H
#define _CM_VIRTUAL_MEM_H

#include "cm_type.h"
#include "cm_list.h"
#include "cm_mutex.h"
#include "cm_file.h"

typedef struct st_vm_page {
    UT_LIST_NODE_T(struct st_vm_page) list_node;
} vm_page_t;

typedef struct st_vm_ctrl {
#ifdef UNIV_MEMORY_DEBUG
    uint32          id;
#endif
    mutex_t         mutex;
    uint32          io_in_progress : 1;
    uint32          is_free : 1;
    uint32          is_in_closed_list : 1;
    uint32          ref_num : 29;
    uint64          swap_page_id;  // file no: high-order 32 bits, page no: low-order 32 bits
    union {
        vm_page_t  *page;
        char       *data;
    } val;
    UT_LIST_NODE_T(struct st_vm_ctrl) list_node;
} vm_ctrl_t;

typedef struct st_vm_page_slot {
#ifdef UNIV_MEMORY_DEBUG
    uint32        id;
#endif
    union {
        uint64    bitmaps;
        byte      byte_bitmap[8];
    } val;
    struct st_vm_page_slot *next;
} vm_page_slot_t;

#define VM_FILE_HANDLE_COUNT    0x0F
typedef struct st_vm_file {
    char               *name;
    uint32              id;
    os_file_t           handle[VM_FILE_HANDLE_COUNT + 1];
    uint32              page_max_count;
    mutex_t             mutex;
    vm_page_slot_t     *free_slots;
    UT_LIST_BASE_NODE_T(vm_ctrl_t) slot_pages;
} vm_file_t;

#define VM_FILE_COUNT           8
typedef struct st_vm_pool {
    mutex_t          mutex;
    char            *buf;
    uint64           memory_size;
    uint32           page_size;
    uint32           page_count;
    uint32           page_hwm;

    uint32           ctrl_page_count;
    uint32           ctrl_page_max_count;

    uint32           ctrl_count_per_page;
    uint32           slot_count_pre_page;
    uint32           page_count_pre_slot_page;
    uint32           page_count_pre_slot;
    bool32           io_in_progress_ctrl_page;

#ifdef UNIV_MEMORY_DEBUG
    uint32           ctrl_sequence;
    uint32           slot_sequence;
#endif

    atomic32_t       vm_file_index;
    vm_file_t        vm_files[VM_FILE_COUNT];

    os_aio_array_t  *aio_array;

    UT_LIST_BASE_NODE_T(vm_page_t) free_pages;
    UT_LIST_BASE_NODE_T(vm_ctrl_t) free_ctrls;
    UT_LIST_BASE_NODE_T(vm_ctrl_t) closed_page_ctrls;
} vm_pool_t;

vm_pool_t* vm_pool_create(uint64 memory_size, uint32 page_size);
void vm_pool_destroy(vm_pool_t *pool);
bool32 vm_pool_add_file(vm_pool_t *pool, char *name, uint64 size);

vm_ctrl_t* vm_alloc(vm_pool_t *pool);
bool32 vm_free(vm_pool_t *pool, vm_ctrl_t *ctrl);
bool32 vm_open(vm_pool_t *pool, vm_ctrl_t* ctrl);
bool32 vm_close(vm_pool_t *pool, vm_ctrl_t *ctrl);


#endif  /* _CM_VIRTUAL_MEM_H */
