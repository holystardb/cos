#ifndef _CM_VM_POOL_H
#define _CM_VM_POOL_H

#include "cm_type.h"
#include "cm_list.h"
#include "cm_mutex.h"
#include "cm_file.h"

typedef struct st_vm_page {
    UT_LIST_NODE_T(struct st_vm_page) list_node;
} vm_page_t;

#define VM_CTRL_GET_DATA_PTR(ctrl)    (ctrl->val.data)
#define VM_CTRL_IS_ALLOC(ctrl)        (!ctrl->is_free)
#define VM_CTRL_IS_OPEN(ctrl)         (ctrl->ref_num)

#define VM_CTRL_MAX_OPEN_COUNT        0xFFFFFF

struct vm_pool_t;

struct vm_ctrl_t {
    uint32          id;
    uint32          owner_list_id;
    mutex_t         mutex;
    volatile uint32 io_in_progress : 1;
    volatile uint32 is_free : 1;
    volatile uint32 is_in_closed_list : 1; // protected by close_ctrls->mutex
    volatile uint32 ref_num : 29;
    uint64          swap_page_id;  // file no: high-order 32 bits, page no: low-order 32 bits
    union {
        vm_page_t*  page;
        char*       data;
    } val;
    UT_LIST_NODE_T(vm_ctrl_t) list_node;
};

struct vm_free_ctrls_t {
    uint32          id;
    mutex_t         mutex;
    volatile bool32 io_in_progress_ctrl_page;
    UT_LIST_BASE_NODE_T(vm_ctrl_t) free_ctrls;

    void init(uint32 index);
    vm_ctrl_t* alloc_ctrl();
    void free_ctrl(vm_ctrl_t* ctrl);
    void expand_ctrls_by_page(vm_pool_t *pool, vm_page_t *page);
};

struct vm_free_pages_t {
    uint32          id;
    mutex_t         mutex;
    UT_LIST_BASE_NODE_T(vm_page_t) free_pages;

    void init(uint32 index);
    vm_page_t* alloc_page();
    void free_page(vm_page_t* page);
};

struct vm_close_ctrls_t {
    uint32          id;
    mutex_t         mutex;
    UT_LIST_BASE_NODE_T(vm_ctrl_t) close_ctrls;

    void init(uint32 index);
    void add_ctrl(vm_ctrl_t* ctrl);
    void remove_ctrl(vm_ctrl_t* ctrl);
    vm_ctrl_t* get_swapout_ctrl();
};

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

#define VM_CLOSE_CTRL_LIST_COUNT    64
#define VM_FREE_CTRL_LIST_COUNT     64
#define VM_FREE_PAGE_LIST_COUNT     64
#define VM_FILE_COUNT               8
struct vm_pool_t {
    mutex_t          page_hwm_mutex;
    char            *buf;
    uint64           memory_size;
    uint32           page_size;
    uint32           page_count;
    volatile uint32  page_hwm;

    uint32           ctrl_count_per_page;
    uint32           slot_count_pre_page;
    uint32           page_count_pre_slot_page;
    uint32           page_count_pre_slot;
    volatile bool32  io_in_progress_ctrl_page;

    atomic32_t       ctrl_id_sequence;
    uint32           slot_sequence;

    mutex_t          vm_files_mutex;
    atomic32_t       vm_file_index;
    vm_file_t        vm_files[VM_FILE_COUNT];

    os_aio_array_t  *aio_array;

    vm_free_ctrls_t  free_ctrl_list[VM_FREE_CTRL_LIST_COUNT];
    vm_free_pages_t  free_page_list[VM_FREE_PAGE_LIST_COUNT];
    vm_close_ctrls_t close_ctrl_list[VM_CLOSE_CTRL_LIST_COUNT];

    void free_ctrl(vm_ctrl_t* ctrl);
    vm_page_t* alloc_page_from_buf();
    vm_page_t* alloc_page_from_free_page_list();
    void free_page_to_free_page_list(vm_page_t* page);

    void add_ctrl_to_close_list(vm_ctrl_t* ctrl);
    void remove_ctrl_from_close_list(vm_ctrl_t* ctrl);
    vm_ctrl_t* get_swapout_ctrl_from_close_list();

    void initialize_free_ctrl_list();
    void initialize_free_page_list(uint32 initialize_page_count);
};


// =====================================================================================


extern vm_pool_t* vm_pool_create(uint64 memory_size, uint32 page_size, uint32 initialize_page_count);
extern void vm_pool_destroy(vm_pool_t *pool);
extern status_t vm_pool_add_file(vm_pool_t *pool, char *name, uint64 size);

extern vm_ctrl_t* vm_alloc(vm_pool_t *pool);
extern bool32 vm_free(vm_pool_t *pool, vm_ctrl_t *ctrl);
extern bool32 vm_open(vm_pool_t *pool, vm_ctrl_t* ctrl);
extern bool32 vm_close(vm_pool_t *pool, vm_ctrl_t *ctrl);




// =====================================================================================

// Flag for vm_vardata_chunk_t::used that indicates a full chunk
#define VM_VARDATA_CHUNK_FULL_FLAG      0x80000000UL
#define VM_VARDATA_DATA_MAX_SIZE        (SIZE_G(4) - 1)


typedef struct st_vm_vardata        vm_vardata_t;
typedef struct st_vm_vardata_chunk  vm_vardata_chunk_t;

struct st_vm_vardata_chunk {
    uint32           chunk_seq;
    uint32           used;
    char*            data;
    vm_ctrl_t*       ctrl;
    vm_vardata_t*    var;
    UT_LIST_NODE_T(vm_vardata_chunk_t) list_node;
};

struct st_vm_vardata {
    uint64       chunk_id;
    vm_pool_t*   pool;
    vm_ctrl_t*   current_open_ctrl;
    uint32       size;
    bool32       is_resident_memory;
    uint32       chunk_size;
    uint32       current_page_used;

    UT_LIST_BASE_NODE_T(vm_ctrl_t) ctrls;
    UT_LIST_BASE_NODE_T(vm_vardata_chunk_t) used_chunks;
    UT_LIST_BASE_NODE_T(vm_vardata_chunk_t) free_chunks;

    // only in the debug version:
    //    if dyn array is opened, this is the buffer end offset,
    //    else this is 0
    uint32       buf_end;
};

#define vm_vardata_get_first_chunk(var)       UT_LIST_GET_FIRST((var)->used_chunks)
#define vm_vardata_get_last_chunk(var)        UT_LIST_GET_LAST((var)->used_chunks)
#define vm_vardata_get_next_chunk(var, chunk) UT_LIST_GET_NEXT(list_node, chunk)
#define vm_vardata_get_prev_chunk(var, chunk) UT_LIST_GET_PREV(list_node, chunk)

extern inline vm_vardata_t* vm_vardata_create(vm_vardata_t* var, uint64 chunk_id,
    uint32 chunk_size, bool32 is_resident_memory, vm_pool_t* pool);
extern inline void vm_vardata_destroy(vm_vardata_t* var);

extern inline char* vm_vardata_open(vm_vardata_t* var, uint32 size);
extern inline void vm_vardata_close(vm_vardata_t* var, char* ptr);

extern inline void* vm_vardata_push(vm_vardata_t* var, uint32 size);
extern inline bool32 vm_vardata_push_string(vm_vardata_t* var, char* str, uint32 len);

extern inline void* vm_vardata_get_element(vm_vardata_t* var, uint64 offset);
extern inline uint32 vm_vardata_get_data_size(vm_vardata_t* var);
extern inline char* vm_vardata_chunk_get_data(vm_vardata_chunk_t* chunk);

// -------------------------------------------------------------------------------




#endif  /* _CM_VM_POOL_H */
