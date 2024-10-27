#include "cm_virtual_mem.h"
#include "cm_memory.h"
#include "cm_dbug.h"
#include "cm_log.h"
#include "cm_util.h"
#include "cm_error.h"

#define INVALID_SWAP_PAGE_ID                 (uint64)-1
#define INVALID_PAGE_SLOT                    0xFFFF
#define VM_GET_OFFSET_BY_SWAP_PAGE_ID(id)    ((id & 0xFFFFFFFF) * pool->page_size)
#define VM_GET_FILE_BY_SWAP_PAGE_ID(id)      (id >> 32)
#define VM_GET_PAGE_NO_BY_SWAP_PAGE_ID(id)   (id & 0xFFFFFFFF)
#define VM_PAGE_SLOT_IS_FULL(bits)           (bits == 0xFFFFFFFFFFFFFFFF)

static inline vm_page_t* vm_alloc_page(vm_pool_t *pool);
static inline vm_page_t* vm_swap_out_page(vm_pool_t *pool);

static inline void vm_file_free_page(vm_pool_t *pool, vm_file_t *vm_file, uint64 swap_page_id);
static inline bool32 vm_file_swap_in(vm_pool_t *pool, vm_page_t *page, vm_ctrl_t *ctrl);
static inline bool32 vm_file_swap_out(vm_pool_t *pool, vm_ctrl_t *ctrl, uint64 *swap_page_id);

vm_pool_t* vm_pool_create(uint64 memory_size, uint32 page_size)
{
    vm_pool_t* pool;
    uint64     tmp_memory_size;
    uint32     ctrl_page_max_count;

    // 8KB page size: 32768 pages * 8KB = 256MB, total 50GB memory 
    // 16KB page size: 16384 pages * 16KB = 256MB, total 100GB memory 
    // 32KB page size: 8192 pages * 32KB = 256MB, total 200GB memory 
    // 64KB page size: 4096 pages * 64KB = 256MB, total 400GB memory 
    // 128KB page size: 2048 pages * 128KB = 256MB, total 800GB memory 
    // 256KB page size: 512 pages * 256KB = 128MB, total 800GB memory 
    // 512KB page size: 128 pages * 512KB = 64MB, total 800GB memory 

    switch (page_size) {
    case 1024 * 8:
        ctrl_page_max_count = 32768;
        break;
    case 1024 * 16:
        ctrl_page_max_count = 16384;
        break;
    case 1024 * 32:
        ctrl_page_max_count = 8192;
        break;
    case 1024 * 64:
        ctrl_page_max_count = 4096;
        break;
    case 1024 * 128:
        ctrl_page_max_count = 2048;
        break;
    case 1024 * 256:
        ctrl_page_max_count = 512;
        break;
    case 1024 * 512:
        ctrl_page_max_count = 128;
        break;
    default:
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIRTUAL_MEM, "vm_pool_create: error, invalid page size %u", page_size);
        return FALSE;
    }

    tmp_memory_size = (memory_size / page_size) * page_size;
    if (tmp_memory_size < 1024 * 1024 || (tmp_memory_size / page_size) > ctrl_page_max_count) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIRTUAL_MEM, "vm_pool_create: error, invalid memory size %lu", memory_size);
        return NULL;
    }

    pool = (vm_pool_t *)ut_malloc_zero(sizeof(vm_pool_t));
    if (pool == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIRTUAL_MEM, "vm_pool_create: error, can not alloc memory for vm_pool_t");
        return NULL;
    }

    pool->memory_size = tmp_memory_size;
    pool->buf = (char *)os_mem_alloc_large(&pool->memory_size);
    pool->page_hwm = 0;
    pool->page_size = page_size;
    pool->page_count = (uint32)(tmp_memory_size / page_size);
    pool->ctrl_page_count = 0;
    pool->ctrl_page_max_count = ctrl_page_max_count;
    pool->ctrl_count_per_page = page_size / sizeof(vm_ctrl_t);
    pool->slot_count_pre_page = pool->page_size / sizeof(vm_page_slot_t);
    pool->page_count_pre_slot = 64;
    pool->page_count_pre_slot_page = pool->slot_count_pre_page * pool->page_count_pre_slot;
    pool->io_in_progress_ctrl_page = FALSE;
#ifdef UNIV_MEMORY_DEBUG
    pool->ctrl_sequence = 0;
    pool->slot_sequence = 0;
#endif

    mutex_create(&pool->mutex);
    UT_LIST_INIT(pool->free_ctrls);
    UT_LIST_INIT(pool->free_pages);
    UT_LIST_INIT(pool->closed_page_ctrls);

    for (uint32 i = 0; i < VM_FILE_COUNT; i++) {
        pool->vm_files[i].id = i;
        pool->vm_files[i].name = NULL;
        pool->vm_files[i].page_max_count = 0;
        pool->vm_files[i].free_slots = NULL;
        UT_LIST_INIT(pool->vm_files[i].slot_pages);
        for (uint32 j = 0; j <= VM_FILE_HANDLE_COUNT; j++) {
            pool->vm_files[i].handle[j] = OS_FILE_INVALID_HANDLE;
        }
    }

    uint32 io_pending_count_per_context = 1;
    uint32 io_context_count = 100;
    pool->aio_array = os_aio_array_create(io_pending_count_per_context, io_context_count);
    if (pool->aio_array == NULL) {
        os_mem_free_large(pool->buf, pool->memory_size);
        ut_free(pool);
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIRTUAL_MEM, "vm_pool_create: error, can not create aio array");
        return NULL;
    }

    os_file_init();

    return pool;
}

void vm_pool_destroy(vm_pool_t *pool)
{
    //
    for (uint32 i = 0; i < VM_FILE_COUNT; i++) {
        for (uint32 j = 0; j <= VM_FILE_HANDLE_COUNT; j++) {
            if (pool->vm_files[i].handle[j] != OS_FILE_INVALID_HANDLE) {
                os_close_file(pool->vm_files[i].handle[j]);
                pool->vm_files[i].handle[j] = OS_FILE_INVALID_HANDLE;
            }
        }
        if (pool->vm_files[i].name) {
            ut_free(pool->vm_files[i].name);
        }
    }
    //
    os_mem_free_large(pool->buf, pool->memory_size);

    os_aio_array_free(pool->aio_array);

    ut_free(pool);
}

status_t vm_pool_add_file(vm_pool_t *pool, char *name, uint64 size)
{
    bool32       ret;
    uint64       max_size;
    vm_file_t   *vm_file = NULL;
    vm_ctrl_t   *ctrl = NULL;

    switch (pool->page_size) {
    case 1024 * 8:
        max_size = (uint64)1024 * 1024 * 1024 * 50;
        break;
    case 1024 * 16:
        max_size = (uint64)1024 * 1024 * 1024 * 100;
        break;
    case 1024 * 32:
        max_size = (uint64)1024 * 1024 * 1024 * 200;
        break;
    case 1024 * 64:
        max_size = (uint64)1024 * 1024 * 1024 * 400;
        break;
    case 1024 * 128:
        max_size = (uint64)1024 * 1024 * 1024 * 800;
        break;
    case 1024 * 256:
        max_size = (uint64)1024 * 1024 * 1024 * 800;
        break;
    case 1024 * 512:
    default:
        max_size = (uint64)1024 * 1024 * 1024 * 800;
        break;
    }

    if (size > max_size || size < 8 * 1024 * 1024) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIRTUAL_MEM, "vm_pool_add_file: error, invalid size %lu", size);
        return CM_ERROR;
    }
    size = size / (8 * 1024 * 1024) * (8 * 1024 * 1024);

    mutex_enter(&pool->mutex, NULL);
    for (uint32 i = 0; i < VM_FILE_COUNT; i++) {
        if (pool->vm_files[i].name == NULL) {
            vm_file = &pool->vm_files[i];
            vm_file->id = i;
            vm_file->name = (char *)ut_malloc(strlen(name) + 1);
            strcpy_s(vm_file->name, strlen(name) + 1, name);
            vm_file->name[strlen(name)] = '\0';
            vm_file->page_max_count = (uint32)(size / pool->page_size);
            vm_file->free_slots = NULL;
            mutex_create(&vm_file->mutex);
            UT_LIST_INIT(vm_file->slot_pages);
            break;
        }
    }
    mutex_exit(&pool->mutex);

    if (vm_file == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIRTUAL_MEM, "vm_pool_add_file: error, file array is full, %s", name);
        return CM_ERROR;
    }

#ifdef __WIN__
    for (uint32 i = 0; i <= VM_FILE_HANDLE_COUNT; i++) {
        ret = os_open_file(name, i == 0 ? OS_FILE_CREATE : OS_FILE_OPEN, OS_FILE_AIO, &vm_file->handle[i]);
        if (ret == FALSE) {
            char err_info[CM_ERR_MSG_MAX_LEN];
            os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
            LOGGER_ERROR(LOGGER, LOG_MODULE_VIRTUAL_MEM,
                "vm_pool_add_file: error, can not create file %s, error desc %s", name, err_info);
            goto err_exit;
        }
    }
#else
    ret = os_open_file(name, OS_FILE_CREATE, OS_FILE_AIO, &vm_file->handle[0]);
    if (ret == FALSE) {
        char err_info[CM_ERR_MSG_MAX_LEN];
        os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
        LOGGER_ERROR(LOGGER, LOG_MODULE_VIRTUAL_MEM,
            "vm_pool_add_file: error, can not create file %s, error desc %s", name, err_info)
        goto err_exit;
    }
#endif

    uint32 slot_page_count;
    uint32 slot_count_pre_page;

    slot_count_pre_page = pool->page_size / sizeof(vm_page_slot_t);
    slot_page_count = vm_file->page_max_count / slot_count_pre_page;
    if (slot_page_count * slot_count_pre_page != vm_file->page_max_count) {
        slot_page_count += 1;
    }

    vm_page_slot_t *cur_slot = NULL;
    for (uint32 page_index = 0; page_index < slot_page_count; page_index++) {
        ctrl = vm_alloc(pool);
        if (ctrl == NULL) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_VIRTUAL_MEM, "vm_pool_add_file: error for alloc vm_ctrl");
            goto err_exit;
        }
        if (!vm_open(pool, ctrl)) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_VIRTUAL_MEM, "vm_pool_add_file: error for vm_open");
            goto err_exit;
        }
        UT_LIST_ADD_FIRST(list_node, vm_file->slot_pages, ctrl);

        uint32 slot_count;
        if (page_index == slot_page_count - 1) {
            slot_count = vm_file->page_max_count % slot_count_pre_page / 64;
        } else {
            slot_count = slot_count_pre_page;
        }
        for (uint32 i = 0; i < slot_count; i++) {
            vm_page_slot_t *slot = (vm_page_slot_t *)(ctrl->val.data + sizeof(vm_page_slot_t) * i);
            slot->val.bitmaps = 0;
            slot->next = NULL;
#ifdef UNIV_MEMORY_DEBUG
            slot->id = pool->slot_sequence++;
#endif
            //
            if (vm_file->free_slots == NULL) {
                vm_file->free_slots = slot;
            } else {
                cur_slot->next = slot;
            }
            cur_slot = slot;
        }
    }

    return CM_SUCCESS;

err_exit:

    if (ctrl) {
        vm_free(pool, ctrl);
    }
    ctrl = UT_LIST_GET_FIRST(vm_file->slot_pages);
    while (ctrl) {
        UT_LIST_REMOVE(list_node, vm_file->slot_pages, ctrl);
        vm_free(pool, ctrl);
        ctrl = UT_LIST_GET_FIRST(vm_file->slot_pages);
    }

    for (uint32 i = 0; i < VM_FILE_HANDLE_COUNT; i++) {
        if (vm_file->handle[i] != OS_FILE_INVALID_HANDLE) {
            os_close_file(vm_file->handle[i]);
            vm_file->handle[i] = OS_FILE_INVALID_HANDLE;
        }
    }

    ut_free(vm_file->name);
    vm_file->name = NULL;

    return CM_ERROR;
}

static inline void vm_pool_fill_ctrls_by_page(vm_pool_t *pool, vm_page_t *page)
{
    vm_ctrl_t * ctrl;

    ut_ad(mutex_own(&pool->mutex));

    for (uint32 i = 0; i < pool->ctrl_count_per_page; i++) {
        ctrl = (vm_ctrl_t *)((char *)page + i * sizeof(vm_ctrl_t));
        mutex_create(&ctrl->mutex);
        ctrl->swap_page_id = INVALID_SWAP_PAGE_ID;
        ctrl->val.page = NULL;
        ctrl->is_free = TRUE;
        ctrl->io_in_progress = FALSE;
        ctrl->is_in_closed_list = FALSE;
        ctrl->ref_num = 0;
        UT_LIST_ADD_FIRST(list_node, pool->free_ctrls, ctrl);
#ifdef UNIV_MEMORY_DEBUG
        ctrl->id = pool->ctrl_sequence++;
#endif
    }
}

static inline vm_page_t* vm_alloc_ctrls_from_free_pages(vm_pool_t *pool)
{
    vm_page_t *page;

    ut_ad(mutex_own(&pool->mutex));

    page = UT_LIST_GET_FIRST(pool->free_pages);
    if (page != NULL) {
        UT_LIST_REMOVE(list_node, pool->free_pages, page);
    } else if (pool->page_hwm < pool->page_count) {
        page = (vm_page_t *)(pool->buf + pool->page_hwm * pool->page_size);
        pool->page_hwm++;
        pool->ctrl_page_count++;
    }

    if (page) {
        vm_pool_fill_ctrls_by_page(pool, page);
    }

    return page;
}

inline vm_ctrl_t* vm_alloc(vm_pool_t *pool)
{
    vm_ctrl_t *ctrl = NULL;
    vm_page_t *page;

    mutex_enter(&pool->mutex, NULL);
    while (ctrl == NULL) {
        ctrl = UT_LIST_GET_FIRST(pool->free_ctrls);
        if (ctrl != NULL) {
            UT_LIST_REMOVE(list_node, pool->free_ctrls, ctrl);
            break;
        }
        if (pool->ctrl_page_count >= pool->ctrl_page_max_count) {
            // full
            break;
        }

        if (vm_alloc_ctrls_from_free_pages(pool)) {
            continue;
        }

        if (pool->io_in_progress_ctrl_page) {
            mutex_exit(&pool->mutex);

            os_thread_sleep(100);

            mutex_enter(&pool->mutex, NULL);
            continue;
        }

        pool->io_in_progress_ctrl_page = TRUE;
        mutex_exit(&pool->mutex);

        // alloc page from swap out
        page = vm_swap_out_page(pool);
        if (page == NULL) {
            return NULL;
        }

        mutex_enter(&pool->mutex, NULL);

        vm_pool_fill_ctrls_by_page(pool, page);

        if (pool->io_in_progress_ctrl_page) {
            pool->io_in_progress_ctrl_page = FALSE;
        }
    }
    mutex_exit(&pool->mutex);

    if (ctrl != NULL) {
        ctrl->swap_page_id = INVALID_SWAP_PAGE_ID;
        ctrl->val.page = NULL;
        ctrl->io_in_progress = FALSE;
        ctrl->is_free = FALSE;
        ctrl->is_in_closed_list = FALSE;
        ctrl->ref_num = 0;
    }

    return ctrl;
}

static inline void vm_lock_ctrl_for_free(vm_ctrl_t* ctrl)
{
    for (;;) {
        mutex_enter(&ctrl->mutex, NULL);
        if (ctrl->io_in_progress == FALSE && ctrl->ref_num == 0) {
            break;
        }
        mutex_exit(&ctrl->mutex);

        os_thread_sleep(100);
    }
}

inline bool32 vm_free(vm_pool_t *pool, vm_ctrl_t *ctrl)
{
    uint64     swap_page_id;
    vm_page_t *page;

    DBUG_ENTER("vm_free");

    vm_lock_ctrl_for_free(ctrl);
    if (ctrl->is_free) {
        mutex_exit(&ctrl->mutex);
        DBUG_PRINT("error, ctrl %p is free", ctrl);
        DBUG_RETURN(FALSE);
    }

    ctrl->is_free = TRUE;
    swap_page_id = ctrl->swap_page_id;
    page = ctrl->val.page;
    ctrl->val.page = NULL;
    mutex_exit(&ctrl->mutex);

    mutex_enter(&pool->mutex, NULL);
    if (page) {
        UT_LIST_ADD_LAST(list_node, pool->free_pages, page);
    }
    if (ctrl->is_in_closed_list) {
        UT_LIST_REMOVE(list_node, pool->closed_page_ctrls, ctrl);
        ctrl->is_in_closed_list = FALSE;
    }
    UT_LIST_ADD_LAST(list_node, pool->free_ctrls, ctrl);
    mutex_exit(&pool->mutex);

    if (swap_page_id != INVALID_SWAP_PAGE_ID) {
        vm_file_t *vm_file = &pool->vm_files[VM_GET_FILE_BY_SWAP_PAGE_ID(swap_page_id)];
        vm_file_free_page(pool, vm_file, swap_page_id);
    }

    DBUG_RETURN(TRUE);
}

static inline vm_page_t* vm_swap_out_page(vm_pool_t *pool)
{
    bool32     is_found = FALSE;
    uint64     swap_page_id = INVALID_SWAP_PAGE_ID;
    vm_page_t *page = NULL;
    vm_ctrl_t *ctrl;

    DBUG_ENTER("vm_swap_out_page");

    mutex_enter(&pool->mutex, NULL);
    ctrl = UT_LIST_GET_FIRST(pool->closed_page_ctrls);
    while (ctrl) {
        ut_a(ctrl->is_in_closed_list);

        mutex_enter(&ctrl->mutex, NULL);
        if (ctrl->ref_num == 0 && ctrl->io_in_progress == FALSE && ctrl->val.page != NULL) {
            ctrl->io_in_progress = TRUE;
            is_found = TRUE;
        }
        mutex_exit(&ctrl->mutex);

        if (is_found) {
            ctrl->is_in_closed_list = FALSE;
            UT_LIST_REMOVE(list_node, pool->closed_page_ctrls, ctrl);
            break;
        }
        //
        ctrl = UT_LIST_GET_NEXT(list_node ,ctrl);
    }
    mutex_exit(&pool->mutex);

    if (ctrl && ctrl->io_in_progress) {
        if (vm_file_swap_out(pool, ctrl, &swap_page_id) == FALSE) {
            mutex_enter(&pool->mutex, NULL);
            ctrl->is_in_closed_list = TRUE;
            UT_LIST_ADD_FIRST(list_node, pool->closed_page_ctrls, ctrl);
            mutex_exit(&pool->mutex);

            mutex_enter(&ctrl->mutex, NULL);
            ctrl->io_in_progress = FALSE;
            mutex_exit(&ctrl->mutex);

            DBUG_RETURN(NULL);
        }

        mutex_enter(&ctrl->mutex, NULL);
        page = ctrl->val.page;
        ctrl->val.page = NULL;
        ctrl->io_in_progress = FALSE;
        ctrl->swap_page_id = swap_page_id;
        mutex_exit(&ctrl->mutex);

        DBUG_PRINT("ctrl %p swap out, file %u page no %u", ctrl,
            VM_GET_FILE_BY_SWAP_PAGE_ID(swap_page_id),
            VM_GET_PAGE_NO_BY_SWAP_PAGE_ID(swap_page_id));
    }

    DBUG_RETURN(page);
}

static inline vm_page_t* vm_alloc_page(vm_pool_t *pool)
{
    vm_page_t *page;

    DBUG_ENTER("vm_alloc_page");

    mutex_enter(&pool->mutex, NULL);
    page = UT_LIST_GET_FIRST(pool->free_pages);
    if (page != NULL) {
        UT_LIST_REMOVE(list_node, pool->free_pages, page);
    } else if (pool->page_hwm < pool->page_count) {
        page = (vm_page_t *)(pool->buf + pool->page_hwm * pool->page_size);
        pool->page_hwm++;
    }
    mutex_exit(&pool->mutex);

    if (page == NULL) {
        page = vm_swap_out_page(pool);
    }

    DBUG_RETURN(page);
}

static inline void vm_lock_ctrl_for_open_close(vm_ctrl_t* ctrl)
{
    for (;;) {
        mutex_enter(&ctrl->mutex, NULL);
        if (ctrl->io_in_progress == FALSE) {
            break;
        }
        mutex_exit(&ctrl->mutex);
        os_thread_sleep(100);
    }
}

inline bool32 vm_open(vm_pool_t *pool, vm_ctrl_t* ctrl)
{
    vm_page_t *page = NULL;

    DBUG_ENTER("vm_open");

    vm_lock_ctrl_for_open_close(ctrl);
    if (ctrl->is_free) {
        mutex_exit(&ctrl->mutex);
        DBUG_PRINT("error, ctrl %p is free", ctrl);
        DBUG_RETURN(FALSE);
    }

    if (ctrl->ref_num >= VM_CTRL_MAX_OPEN_COUNT) {
        mutex_exit(&ctrl->mutex);
        CM_SET_ERROR(ERR_VM_OPEN_LIMIT_EXCEED, VM_CTRL_MAX_OPEN_COUNT);
        DBUG_RETURN(FALSE);
    }

    if (ctrl->ref_num > 0) {
        ctrl->ref_num++;
        mutex_exit(&ctrl->mutex);
        DBUG_PRINT("ctrl %p : ref_num %u", ctrl, ctrl->ref_num);
        DBUG_RETURN(TRUE);
    }

    if (ctrl->val.page == NULL) { // first open or swap out
        ctrl->io_in_progress = TRUE;
    }

    ctrl->ref_num = 1;
    mutex_exit(&ctrl->mutex);

    if (ctrl->is_in_closed_list) {
        mutex_enter(&pool->mutex, NULL);
        if (ctrl->is_in_closed_list) {
            UT_LIST_REMOVE(list_node, pool->closed_page_ctrls, ctrl);
            ctrl->is_in_closed_list = FALSE;
        }
        mutex_exit(&pool->mutex);
    }

    if (ctrl->val.page) {
        DBUG_PRINT("ctrl %p : ref_num 1, page %p", ctrl, ctrl->val.page);
        DBUG_RETURN(TRUE);
    }

    page = vm_alloc_page(pool);
    if (page == NULL) {
        DBUG_PRINT("error, vm_alloc_page is NULL, ctrl %p", ctrl);
        mutex_enter(&ctrl->mutex, NULL);
        ctrl->ref_num--;
        ctrl->io_in_progress = FALSE;
        mutex_exit(&ctrl->mutex);
        DBUG_RETURN(FALSE);
    }

    if (ctrl->swap_page_id == INVALID_SWAP_PAGE_ID) {
        DBUG_PRINT("ctrl %p no need to swap_in", ctrl);
        mutex_enter(&ctrl->mutex, NULL);
        ctrl->io_in_progress = FALSE;
        ctrl->val.page = page;
        mutex_exit(&ctrl->mutex);
        DBUG_RETURN(TRUE);
    }

    if (vm_file_swap_in(pool, page, ctrl)) {
        DBUG_PRINT("ctrl %p swap_in: file %u page no %u", ctrl,
            VM_GET_FILE_BY_SWAP_PAGE_ID(ctrl->swap_page_id), VM_GET_PAGE_NO_BY_SWAP_PAGE_ID(ctrl->swap_page_id));
        mutex_enter(&ctrl->mutex, NULL);
        ctrl->swap_page_id = INVALID_SWAP_PAGE_ID;
        ctrl->val.page = page;
        ctrl->io_in_progress = FALSE;
        mutex_exit(&ctrl->mutex);
        DBUG_RETURN(TRUE);
    }

    DBUG_PRINT("ctrl %p fail to do swap_in: file %u page no %u", ctrl,
        VM_GET_FILE_BY_SWAP_PAGE_ID(ctrl->swap_page_id), VM_GET_PAGE_NO_BY_SWAP_PAGE_ID(ctrl->swap_page_id));

    mutex_enter(&ctrl->mutex, NULL);
    ctrl->ref_num--;
    ctrl->io_in_progress = FALSE;
    mutex_exit(&ctrl->mutex);

    mutex_enter(&pool->mutex, NULL);
    UT_LIST_ADD_LAST(list_node, pool->free_pages, page);
    mutex_exit(&pool->mutex);

    DBUG_RETURN(FALSE);
}

inline bool32 vm_close(vm_pool_t *pool, vm_ctrl_t *ctrl)
{
    bool32 is_need_add_closed_list = FALSE;

    vm_lock_ctrl_for_open_close(ctrl);
    if (ctrl->is_free) {
        mutex_exit(&ctrl->mutex);
        return FALSE;
    }

    if (ctrl->ref_num == 0) {
        mutex_exit(&ctrl->mutex);
        return FALSE;
    }
    ctrl->ref_num--;
    if (ctrl->ref_num == 0) {
        is_need_add_closed_list = TRUE;
    }
    mutex_exit(&ctrl->mutex);

    if (is_need_add_closed_list) {
        mutex_enter(&pool->mutex, NULL);
        mutex_enter(&ctrl->mutex, NULL);
        if (ctrl->is_in_closed_list == FALSE && ctrl->ref_num == 0) {
            UT_LIST_ADD_LAST(list_node, pool->closed_page_ctrls, ctrl);
            ctrl->is_in_closed_list = TRUE;
        }
        mutex_exit(&ctrl->mutex);
        mutex_exit(&pool->mutex);
    }

    return TRUE;
}

static inline void vm_file_free_page(vm_pool_t *pool, vm_file_t *vm_file, uint64 swap_page_id)
{
    vm_ctrl_t      *ctrl;
    vm_page_slot_t *slot;
    uint32          page_count, remain_page_count, page_index, slot_index, byte_index, bit_index;

    DBUG_ENTER("vm_file_free_page");

    ut_a(vm_file->id == VM_GET_FILE_BY_SWAP_PAGE_ID(swap_page_id));

    page_count = VM_GET_PAGE_NO_BY_SWAP_PAGE_ID(swap_page_id) + 1;
    page_index = (page_count - 1) / pool->page_count_pre_slot_page;
    remain_page_count = page_count % pool->page_count_pre_slot_page;
    if (remain_page_count == 0) {
        slot_index = pool->slot_count_pre_page;
        byte_index = 7;
        bit_index = 7;
    } else {
        if (remain_page_count % pool->page_count_pre_slot == 0) {
            slot_index = (remain_page_count - 1) / pool->page_count_pre_slot;
            byte_index = 7;
            bit_index = 7;
        } else {
            slot_index = remain_page_count / pool->page_count_pre_slot;
            remain_page_count = remain_page_count % pool->page_count_pre_slot;
            if (remain_page_count % 8 == 0) {
                byte_index = (remain_page_count - 1) / 8;
                bit_index = 7;
            } else {
                byte_index = remain_page_count / 8;
                bit_index = remain_page_count % 8 - 1;
            }
        }
    }

    // after initialization, vm_file->slot_pages will not change,
    // So there's no need to add a mutex.
    ctrl = UT_LIST_GET_FIRST(vm_file->slot_pages);
    for (uint32 i = 0; i < page_index; i++) {
        ctrl = UT_LIST_GET_NEXT(list_node, ctrl);
    }
    slot = (vm_page_slot_t *)(ctrl->val.data + sizeof(vm_page_slot_t) * slot_index);
    ut_a(slot->val.byte_bitmap[byte_index] & ((uint64)1 << bit_index));
#ifdef UNIV_MEMORY_DEBUG
    ut_a(slot->id == page_index * pool->slot_count_pre_page + slot_index);
#endif

    DBUG_PRINT("swap_page_id %lu: file %u page no %u slot index %u byte index %u bit index %u",
        swap_page_id, VM_GET_FILE_BY_SWAP_PAGE_ID(swap_page_id),
        VM_GET_PAGE_NO_BY_SWAP_PAGE_ID(swap_page_id), slot_index, byte_index, bit_index);

    mutex_enter(&vm_file->mutex, NULL);
    if (VM_PAGE_SLOT_IS_FULL(slot->val.bitmaps)) {
        slot->next = vm_file->free_slots;
        vm_file->free_slots = slot;
    }
    slot->val.byte_bitmap[byte_index] = ut_bit8_set_nth(slot->val.byte_bitmap[byte_index], bit_index, FALSE);
    mutex_exit(&vm_file->mutex);

    DBUG_VOID_RETURN;
}

static inline void vm_file_get_slot_page_index_by_slot(vm_pool_t *pool, vm_file_t *vm_file,
    vm_page_slot_t *slot, uint32 *slot_page_index, uint32 *slot_index)
{
    vm_ctrl_t *ctrl;

    DBUG_ENTER("vm_file_get_slot_page_index_by_slot");

    ut_ad(mutex_own(&vm_file->mutex));

    *slot_page_index = 0;
    ctrl = UT_LIST_GET_FIRST(vm_file->slot_pages);
    while ((char *)slot + pool->page_size < ctrl->val.data) {
        ctrl = UT_LIST_GET_NEXT(list_node, ctrl);
        *slot_page_index += 1;
    }

    *slot_index = (uint32)((char *)slot - (char *)ctrl->val.data) / sizeof(vm_page_slot_t);

    DBUG_PRINT("slot_page_index: %u slot_index: %u", *slot_page_index, *slot_index);

    DBUG_VOID_RETURN;
}

static inline uint64 vm_file_alloc_page_low(vm_pool_t *pool,
    vm_file_t *vm_file, vm_page_slot_t *slot, uint32 byte_index, vm_ctrl_t *ctrl)
{
    DBUG_ENTER("vm_file_alloc_page_low");

    uint64 swap_page_id  = INVALID_SWAP_PAGE_ID;

    ut_ad(mutex_own(&vm_file->mutex));
    ut_a(slot->val.byte_bitmap[byte_index] != 0xFF);

    for (uint32 bit_index = 0; bit_index < 8; bit_index++) {
        if (ut_bit8_get_nth(slot->val.byte_bitmap[byte_index], bit_index) == FALSE) {
            DBUG_PRINT("ctrl %p : file %u slot %p byte index %u bit index %u is free, value %u",
                ctrl, vm_file->id, slot, byte_index, bit_index, slot->val.byte_bitmap[byte_index]);
            slot->val.byte_bitmap[byte_index] = ut_bit8_set_nth(slot->val.byte_bitmap[byte_index], bit_index, TRUE);
            DBUG_PRINT("ctrl %p : file %u slot %p byte index %u bit index %u is set, value %u",
                ctrl, vm_file->id, slot, byte_index, bit_index, slot->val.byte_bitmap[byte_index]);

            uint32 slot_page_index;
            uint32 slot_index;
            vm_file_get_slot_page_index_by_slot(pool, vm_file, slot, &slot_page_index, &slot_index);

#ifdef UNIV_MEMORY_DEBUG
            ut_a(slot->id == slot_page_index * pool->slot_count_pre_page + slot_index);
#endif

            swap_page_id = ((uint64)vm_file->id << 32) +
                           slot_page_index * pool->slot_count_pre_page * pool->page_count_pre_slot +
                           slot_index * pool->page_count_pre_slot +
                           byte_index * 8 + bit_index;
            DBUG_PRINT("slot %p file %u page no %u��page index %u slot index %u byte index %d bit index %d",
                slot, VM_GET_FILE_BY_SWAP_PAGE_ID(swap_page_id), VM_GET_PAGE_NO_BY_SWAP_PAGE_ID(swap_page_id),
                slot_page_index, slot_index, byte_index, bit_index);
            break;
        }
    }

    DBUG_RETURN(swap_page_id);
}

static inline uint64 vm_file_alloc_page(vm_pool_t *pool, vm_file_t *vm_file, vm_ctrl_t *ctrl)
{
    DBUG_ENTER("vm_file_alloc_page");

    uint64 swap_page_id  = INVALID_SWAP_PAGE_ID;

    mutex_enter(&vm_file->mutex, NULL);
    if (vm_file->free_slots) {
        ut_a(!VM_PAGE_SLOT_IS_FULL(vm_file->free_slots->val.bitmaps));
        for (uint32 byte_index = 0; byte_index < 8; byte_index++) {
            if (vm_file->free_slots->val.byte_bitmap[byte_index] != 0xFF) {
                DBUG_PRINT("ctrl %p : file %u slot %p byte index %u is free",
                    ctrl, vm_file->id, vm_file->free_slots, byte_index);
                swap_page_id = vm_file_alloc_page_low(pool, vm_file, vm_file->free_slots, byte_index, ctrl);
                break;
            }
        }

        if (VM_PAGE_SLOT_IS_FULL(vm_file->free_slots->val.bitmaps)) {
            DBUG_PRINT("slot %p: is full", vm_file->free_slots);
            vm_file->free_slots = vm_file->free_slots->next;
        }
    }
    mutex_exit(&vm_file->mutex);

    DBUG_RETURN(swap_page_id);
}

static inline bool32 vm_file_swap_out(vm_pool_t *pool, vm_ctrl_t *ctrl, uint64 *swap_page_id)
{
    bool32            ret = TRUE;
    const char       *name = ""; // name of the file or path as a null-terminated string
    uint64            offset; // file offset where to read or write */
    uint32            timeout_us = 120 * 1000000; // 120 seconds
    vm_file_t        *vm_file = NULL;
    os_aio_context_t* aio_ctx = NULL;
    os_aio_slot_t*    aio_slot = NULL;
    int               aio_ret;

    DBUG_ENTER("vm_file_swap_out");

    *swap_page_id = INVALID_SWAP_PAGE_ID;

    mutex_enter(&pool->mutex, NULL);
    for (uint32 i = 0; i < VM_FILE_COUNT; i++) {
        if (pool->vm_files[i].name == NULL) {
            break;
        }

        *swap_page_id = vm_file_alloc_page(pool, &pool->vm_files[i], ctrl);
        if (*swap_page_id != INVALID_SWAP_PAGE_ID) {
            vm_file = &pool->vm_files[i];
            break;
        }
    }
    mutex_exit(&pool->mutex);

    if (*swap_page_id == INVALID_SWAP_PAGE_ID) {
        DBUG_PRINT("ctrl %p no free space", ctrl);
        DBUG_RETURN(FALSE);
    }

    offset = (*swap_page_id & 0xFFFFFFFF) * pool->page_size;
    DBUG_PRINT("ctrl %p swap_page_id: file %llu page no %u offset %llu", ctrl,
        VM_GET_FILE_BY_SWAP_PAGE_ID(*swap_page_id),
        VM_GET_PAGE_NO_BY_SWAP_PAGE_ID(*swap_page_id), offset);

    aio_ctx = os_aio_array_alloc_context(pool->aio_array);
    aio_slot = os_file_aio_submit(aio_ctx, OS_FILE_WRITE, name,
#ifdef __WIN__
        vm_file->handle[*swap_page_id & VM_FILE_HANDLE_COUNT],
#else
        vm_file->handle[0],
#endif
        (void *)ctrl->val.page, pool->page_size, offset);
    if (aio_slot == NULL) {
        //ret_val = os_file_get_last_error();
        vm_file_free_page(pool, vm_file, *swap_page_id);
        ret = FALSE;
        DBUG_PRINT("ctrl %p error for os_file_aio_submit", ctrl);
        goto err_exit;
    }
    aio_ret = os_file_aio_slot_wait(aio_slot, timeout_us);
    switch (aio_ret) {
    case OS_FILE_IO_COMPLETION:
        ret = TRUE;
        break;
    case OS_FILE_IO_TIMEOUT:
    default:
        ret = FALSE;
        vm_file_free_page(pool, vm_file, *swap_page_id);
        break;
    }

err_exit:

    if (aio_ctx) {
        os_aio_array_free_context(aio_ctx);
    }

    DBUG_RETURN(ret);
}

static inline bool32 vm_file_swap_in(vm_pool_t *pool, vm_page_t *page, vm_ctrl_t *ctrl)
{
    bool32            ret = TRUE;
    const char*       name = ""; // name of the file or path as a null-terminated string
    uint64            offset; // file offset where to read or write */
    uint32            timeout_us = 120 * 1000000; // 120 seconds
    vm_file_t*        vm_file = NULL;
    os_aio_context_t* aio_ctx = NULL;
    os_aio_slot_t*    aio_slot = NULL;
    int               aio_ret;

    DBUG_ENTER("vm_file_swap_in");

    vm_file = &pool->vm_files[VM_GET_FILE_BY_SWAP_PAGE_ID(ctrl->swap_page_id)];
    offset = VM_GET_OFFSET_BY_SWAP_PAGE_ID(ctrl->swap_page_id);
    DBUG_PRINT("ctrl %p swap_page_id: file %u page no %u offset %lu", ctrl,
        VM_GET_FILE_BY_SWAP_PAGE_ID(ctrl->swap_page_id),
        VM_GET_PAGE_NO_BY_SWAP_PAGE_ID(ctrl->swap_page_id), offset);

    aio_ctx = os_aio_array_alloc_context(pool->aio_array);
    aio_slot = os_file_aio_submit(aio_ctx, OS_FILE_READ, name,
#ifdef __WIN__
        vm_file->handle[ctrl->swap_page_id & VM_FILE_HANDLE_COUNT],
#else
        vm_file->handle[0],
#endif
        (void *)page, pool->page_size, offset);
    if (aio_slot == NULL) {
        ret = FALSE;
        DBUG_PRINT("ctrl %p error for os_file_aio_submit", ctrl);
        goto err_exit;
    }
    aio_ret = os_file_aio_slot_wait(aio_slot, timeout_us);
    switch (aio_ret) {
    case OS_FILE_IO_COMPLETION:
        ret = TRUE;
        vm_file_free_page(pool, vm_file, ctrl->swap_page_id);
        break;
    case OS_FILE_IO_TIMEOUT:
    default:
        ret = FALSE;
        break;
    }

err_exit:

    if (aio_ctx) {
        os_aio_array_free_context(aio_ctx);
    }

    DBUG_RETURN(ret);
}





// ==================================================================================
//                          vm_vardata
// ==================================================================================


inline vm_vardata_t* vm_vardata_create(vm_vardata_t* var,
    uint64 chunk_id, uint32 chunk_size, bool32 is_resident_memory, vm_pool_t* pool)

{
    if (pool->page_size < chunk_size) {
        return NULL;
    }

    var->chunk_id = chunk_id;
    var->chunk_size = chunk_size;
    var->pool = pool;
    var->current_page_used = 0;
    var->size = 0;
    var->current_open_ctrl = NULL;
    var->is_resident_memory = is_resident_memory;

    UT_LIST_INIT(var->ctrls);
    UT_LIST_INIT(var->used_chunks);
    UT_LIST_INIT(var->free_chunks);

    ut_d(var->buf_end = 0);

    return var;
}

inline void vm_vardata_destroy(vm_vardata_t* var)
{
    vm_ctrl_t* ctrl = UT_LIST_GET_FIRST(var->ctrls);
    while (ctrl) {
        UT_LIST_REMOVE(list_node, var->ctrls, ctrl);

        if (VM_CTRL_IS_OPEN(ctrl)) {
            ut_a(vm_close(var->pool, ctrl));
        }
        ut_a(vm_free(var->pool, ctrl));

        ctrl = UT_LIST_GET_FIRST(var->ctrls);
    }

    var->current_page_used = 0;
    var->size = 0;
    UT_LIST_INIT(var->used_chunks);
    UT_LIST_INIT(var->free_chunks);
    var->current_open_ctrl = NULL;

    ut_d(var->buf_end = 0);
}

static inline void vm_vardata_ctrl_open_if_not_open(vm_vardata_t* var, vm_ctrl_t* ctrl)
{
    if (var->is_resident_memory) {
        return;
    }

    if (var->current_open_ctrl == ctrl) {
        return;
    }

    if (var->current_open_ctrl) {
        ut_a(VM_CTRL_IS_OPEN(var->current_open_ctrl));
        ut_a(vm_close(var->pool, var->current_open_ctrl));
        var->current_open_ctrl = NULL;
    }

    ut_a(VM_CTRL_IS_OPEN(ctrl) == FALSE);
    ut_a(vm_open(var->pool, ctrl));
    var->current_open_ctrl = ctrl;
}

static inline void vm_vardata_chunk_open_if_not_open(vm_vardata_t* var, vm_vardata_chunk_t* chunk)
{
    ut_a(var == chunk->var);

    vm_vardata_ctrl_open_if_not_open(var, chunk->ctrl);
}

static inline char* vm_vardata_alloc_chunk_data(vm_vardata_t* var)
{
    char*      data;
    vm_ctrl_t* ctrl;

    ctrl = UT_LIST_GET_LAST(var->ctrls);
    if (ctrl == NULL || var->current_page_used + var->chunk_size > var->pool->page_size) {
        // old ctrl is full, need to close ctrl for reuse page
        if (!var->is_resident_memory && var->current_open_ctrl) {
            ut_a(VM_CTRL_IS_OPEN(var->current_open_ctrl));
            ut_a(vm_close(var->pool, var->current_open_ctrl));
            var->current_open_ctrl = NULL;
        }

        // alloc a new ctrl
        ctrl = vm_alloc(var->pool);
        if (ctrl == NULL) {
            return NULL;
        }
        var->current_page_used = 0;
        UT_LIST_ADD_LAST(list_node, var->ctrls, ctrl);

        // open page for ctrl
        ut_a(VM_CTRL_IS_OPEN(ctrl) == FALSE);
        if (vm_open(var->pool, ctrl) == FALSE) {
            UT_LIST_REMOVE(list_node, var->ctrls, ctrl);
            ut_a(vm_free(var->pool, ctrl));
            return NULL;
        }
        if (!var->is_resident_memory) {
            var->current_open_ctrl = ctrl;
        }
    }

    vm_vardata_ctrl_open_if_not_open(var, ctrl);

    data = VM_CTRL_GET_DATA_PTR(ctrl) + var->current_page_used;
    var->current_page_used += var->chunk_size;

    return data;
}

inline uint32 vm_vardata_chunk_get_used(vm_vardata_chunk_t* chunk)
{
    return ((chunk->used) & ~VM_VARDATA_CHUNK_FULL_FLAG);
}

static inline vm_vardata_chunk_t* vm_vardata_add_chunk(vm_vardata_t* var)
{
    // old block
    vm_vardata_chunk_t* chunk = vm_vardata_get_last_chunk(var);
    if (chunk) {
        chunk->used = chunk->used | VM_VARDATA_CHUNK_FULL_FLAG;
    }

    // alloc a new block
    chunk = UT_LIST_GET_FIRST(var->free_chunks);
    if (chunk == NULL) {  // create blocks
        // get buf for create chunks
        char *data = vm_vardata_alloc_chunk_data(var);
        if (data == NULL) {
            return NULL;
        }

        // create chunks and insert into free_chunks
        uint32 used = 0;
        const uint32 size = ut_align8(sizeof(vm_vardata_chunk_t));
        while (used + size <= var->chunk_size) {
            chunk = (vm_vardata_chunk_t *)((char *)data + used);
            chunk->data = NULL;
            chunk->var = var;
            chunk->chunk_seq = UT_LIST_GET_LEN(var->used_chunks) + UT_LIST_GET_LEN(var->free_chunks);
            chunk->ctrl = UT_LIST_GET_LAST(var->ctrls);
            UT_LIST_ADD_LAST(list_node, var->free_chunks, chunk);
            used += size;
        }

        // get new chunk from free_chunks
        chunk = UT_LIST_GET_FIRST(var->free_chunks);
    }

    // set data for new chunk
    if (chunk->data == NULL) {
        chunk->data = vm_vardata_alloc_chunk_data(var);
        if (chunk->data == NULL) {
            return NULL;
        }
    }

    // init
    chunk->used = 0;
    UT_LIST_ADD_LAST(list_node, var->used_chunks, chunk);
    UT_LIST_REMOVE(list_node, var->free_chunks, chunk);

    return chunk;
}

inline char* vm_vardata_open(vm_vardata_t* var, uint32 size)
{
    vm_vardata_chunk_t* chunk;

    ut_ad(size <= var->chunk_size);
    ut_ad(size);

    if ((uint64)vm_vardata_get_data_size(var) + size > VM_VARDATA_DATA_MAX_SIZE) {
        CM_SET_ERROR(ERR_VARIANT_DATA_TOO_BIG, (uint64)vm_vardata_get_data_size(var) + size);
        return NULL;
    }

    chunk = vm_vardata_get_last_chunk(var);
    if (chunk == NULL || chunk->used + size > var->chunk_size) {
        chunk = vm_vardata_add_chunk(var);
        if (chunk == NULL) {
            return NULL;
        }
    }

    ut_ad(chunk->used <= var->chunk_size);
    ut_ad(var->buf_end == 0);
    ut_d(var->buf_end = chunk->used + size);

    vm_vardata_chunk_open_if_not_open(var, chunk);

    return chunk->data + chunk->used;
}

inline void vm_vardata_close(vm_vardata_t* var, char* ptr)
{
    vm_vardata_chunk_t* chunk;

    chunk = vm_vardata_get_last_chunk(var);

    if (!var->is_resident_memory) {
        ut_a(var->current_open_ctrl == chunk->ctrl);
    }

    ut_a(VM_CTRL_IS_OPEN(chunk->ctrl));
    ut_ad(chunk->data + var->buf_end >= ptr);

    //
    var->size +=  (uint32)(ptr - chunk->data) - chunk->used;

    chunk->used = (uint32)(ptr - chunk->data);
    ut_ad(chunk->used <= var->chunk_size);
    ut_d(var->buf_end = 0);
}

inline void* vm_vardata_push(vm_vardata_t* var, uint32 size)
{
    vm_vardata_chunk_t *chunk;
    uint32 used;

    ut_ad(size <= var->chunk_size);
    ut_ad(size);

    if ((uint64)vm_vardata_get_data_size(var) + size >= VM_VARDATA_DATA_MAX_SIZE) {
        CM_SET_ERROR(ERR_VARIANT_DATA_TOO_BIG, (uint64)vm_vardata_get_data_size(var) + size);
        return NULL;
    }

    chunk = vm_vardata_get_last_chunk(var);
    if (chunk == NULL || chunk->used + size > var->chunk_size) {
        chunk = vm_vardata_add_chunk(var);
        if (chunk == NULL) {
            return NULL;
        }
    }

    used = chunk->used;
    chunk->used += size;
    ut_ad(chunk->used <= var->chunk_size);

    var->size += size;

    vm_vardata_chunk_open_if_not_open(var, chunk);

    return chunk->data + used;
}

inline bool32 vm_vardata_push_string(vm_vardata_t* var, char* str, uint32 len)
{
    byte*  ptr;
    uint32 n_copied;

    if ((uint64)vm_vardata_get_data_size(var) + len >= VM_VARDATA_DATA_MAX_SIZE) {
        CM_SET_ERROR(ERR_VARIANT_DATA_TOO_BIG, (uint64)vm_vardata_get_data_size(var) + len);
        return FALSE;
    }

    while (len > 0) {
        if (len > var->chunk_size) {
            n_copied = var->chunk_size;
        } else {
            n_copied = len;
        }

        ptr = (byte*)vm_vardata_push(var, n_copied);
        memcpy(ptr, str, n_copied);
        str += n_copied;
        len -= n_copied;
    }

    return TRUE;
}

inline void* vm_vardata_get_element(vm_vardata_t* var, uint64 offset)
{
    vm_vardata_chunk_t* chunk;
    uint32 used;

    chunk = vm_vardata_get_first_chunk(var);
    ut_ad(chunk);
    used = vm_vardata_chunk_get_used(chunk);

    while (offset >= used) {
        offset -= used;
        chunk = vm_vardata_get_next_chunk(var, chunk);
        ut_ad(chunk);
        used = vm_vardata_chunk_get_used(chunk);
    }

    ut_ad(chunk);
    ut_ad(vm_vardata_chunk_get_used(chunk) >= offset);

    vm_vardata_chunk_open_if_not_open(var, chunk);

    return chunk->data + offset;
}

inline uint32 vm_vardata_get_data_size(vm_vardata_t* var)
{
#ifdef UNIV_DEBUG

    vm_vardata_chunk_t* chunk;
    uint32 sum = 0;

    chunk = vm_vardata_get_first_chunk(var);
    while (chunk != NULL) {
        sum += vm_vardata_chunk_get_used(chunk);
        chunk = vm_vardata_get_next_chunk(var, chunk);
    }
    ut_a(sum == var->size);

#endif

    return var->size;
}

inline char* vm_vardata_chunk_get_data(vm_vardata_chunk_t* chunk)
{
    vm_vardata_chunk_open_if_not_open(chunk->var, chunk);

    return chunk->data;
}


