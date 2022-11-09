#ifndef _CM_VIRTUAL_MEM_H
#define _CM_VIRTUAL_MEM_H

#include "cm_type.h"
#include "cm_list.h"
#include "cm_mutex.h"

#define VM_PAGE_SIZE              (128 * 1024)
#define VM_CTRLS_PER_PAGE         (uint32)(VM_PAGE_SIZE / ut_align8(sizeof(vm_ctrl_t)))
#define VM_CTRL_PAGE_MAX_COUNT    (32 * 1024)  //32K, max memory is 1TB
#define VM_MAX_CTRLS              (uint32)(VM_CTRLS_PER_PAGE * VM_CTRL_PAGE_MAX_COUNT)

typedef struct st_vm_page {
    UT_LIST_NODE_T(struct st_vm_page) list_node;
} vm_page_t;

typedef bool32 (*vm_swap_in)(vm_page_t *page, uint64 swid);
typedef bool32 (*vm_swap_out)(vm_page_t *page, uint64 *swid);
typedef void (*vm_swap_clean)(uint64 swid);

typedef struct st_vm_swapper {
    vm_swap_out       out;
    vm_swap_in        in;
    vm_swap_clean     clean;
} vm_swapper_t;

typedef struct st_vm_ctrl {
    spinlock_t      lock;
    uint64          swap_page_id;  // file no: high-order 32 bits, page no: low-order 32 bits
    vm_page_t      *page;
    uint32          io_in_progress : 1;
    uint32          is_free : 1;
    uint32          is_in_closed_list : 1;
    uint32          ref_num : 29;
    UT_LIST_NODE_T(struct st_vm_ctrl) list_node;
} vm_ctrl_t;

typedef struct st_vm_ctrl_pool {
    spinlock_t       lock;
    UT_LIST_BASE_NODE_T(vm_ctrl_t) free_ctrls;
} vm_ctrl_pool_t;

typedef struct st_vm_page_pool {
    spinlock_t       lock;
    UT_LIST_BASE_NODE_T(vm_page_t) free_pages;
} vm_page_pool_t;


#define VM_CTRL_POOL_COUNT            16
#define VM_PAGE_POOL_COUNT            16

typedef struct st_vm_pool {
    spinlock_t       lock;
    vm_swapper_t    *swapper;
    char            *buf;
    uint32           page_count;
    uint32           page_hwm;
    uint32           ctrl_page_count;

    //vm_file_t        files[10];
    uint8            ctrl_pool_index;
    uint8            page_pool_index;
    vm_ctrl_pool_t   free_ctrl_pool[VM_CTRL_POOL_COUNT];
    vm_page_pool_t   free_page_pool[VM_PAGE_POOL_COUNT];

    UT_LIST_BASE_NODE_T(vm_page_t) free_pages;
    UT_LIST_BASE_NODE_T(vm_ctrl_t) free_ctrls;
    UT_LIST_BASE_NODE_T(vm_ctrl_t) closed_page_ctrls;
} vm_pool_t;

vm_pool_t* vm_pool_create(uint64 memory_size, vm_swapper_t *swapper);
void vm_pool_destroy(vm_pool_t *pool);
vm_ctrl_t* vm_alloc(vm_pool_t *pool);
void vm_free(vm_pool_t *pool, vm_ctrl_t *ctrl);
bool32 vm_open(vm_pool_t *pool, vm_ctrl_t* ctrl);
void vm_close(vm_pool_t *pool, vm_ctrl_t *ctrl);



#endif  /* _CM_VIRTUAL_MEM_H */
