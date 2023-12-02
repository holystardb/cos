#include "cm_virtual_mem.h"
#include "cm_memory.h"
#include "cm_dbug.h"
#include "cm_log.h"
#include "cm_util.h"


#define VM_CTRL_PAGE_MAX_COUNT               4096  // max memory is 1TB

#define INVALID_SWAP_PAGE_ID                 (uint64)-1
#define INVALID_PAGE_SLOT                    0xFFFF
#define VM_GET_OFFSET_BY_SWAP_PAGE_ID(id)    ((id & 0xFFFFFFFF) * pool->page_size)
#define VM_GET_FILE_BY_SWAP_PAGE_ID(id)      (id >> 32)
#define VM_GET_PAGE_NO_BY_SWAP_PAGE_ID(id)   (id & 0xFFFFFFFF)
#define VM_PAGE_SLOT_IS_FULL(bits)           (bits == 0xFFFFFFFFFFFFFFFF)

static vm_page_t* vm_alloc_page(vm_pool_t *pool);
static vm_page_t* vm_swap_out_page(vm_pool_t *pool);

static void vm_file_free_page(vm_pool_t *pool, vm_file_t *vm_file, uint64 swap_page_id);
static bool32 vm_file_swap_in(vm_pool_t *pool, vm_page_t *page, vm_ctrl_t *ctrl);
static bool32 vm_file_swap_out(vm_pool_t *pool, vm_ctrl_t *ctrl, uint64 *swap_page_id);

vm_pool_t* vm_pool_create(uint64 memory_size, uint32 page_size)
{
    vm_pool_t *pool;
    uint64     tmp_memory_size;
    uint32     tmp_page_size;

    tmp_page_size = (page_size / (8 * 1024)) * (8 * 1024);
    if (tmp_page_size == 0 || tmp_page_size > (1024 * 1024)) {
        log_to_stderr(LOG_ERROR, "vm_pool_create: error, invalid page size %u", page_size);
        return FALSE;
    }
    tmp_memory_size = (memory_size / tmp_page_size) * tmp_page_size;
    if (tmp_memory_size < 1024 * 1024) {
        log_to_stderr(LOG_ERROR, "vm_pool_create: error, invalid memory size %lu", memory_size);
        return FALSE;
    }

    pool = (vm_pool_t *)ut_malloc(sizeof(vm_pool_t));
    if (pool == NULL) {
        log_to_stderr(LOG_ERROR, "vm_pool_create: error, can not alloc memory for vm_pool_t");
        return NULL;
    }

    pool->memory_size = tmp_memory_size;
    pool->buf = (char *)os_mem_alloc_large(&pool->memory_size);
    pool->page_hwm = 0;
    pool->page_size = tmp_page_size;
    pool->page_count = (uint32)(tmp_memory_size / tmp_page_size);
    pool->ctrl_page_count = 0;
    pool->ctrl_count_per_page = tmp_page_size / sizeof(vm_ctrl_t);
    pool->slot_count_pre_page = pool->page_size / sizeof(vm_page_slot_t);
    pool->page_count_pre_slot = 64;
    pool->page_count_pre_slot_page = pool->slot_count_pre_page * pool->page_count_pre_slot;
    pool->io_in_progress_ctrl_page = FALSE;
#ifdef DEBUG_OUTPUT
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
        pool->vm_files[i].handle = OS_FILE_INVALID_HANDLE;
        pool->vm_files[i].page_max_count = 0;
        pool->vm_files[i].free_slots = NULL;
        //pool->vm_files[i].full_slots = NULL;
        UT_LIST_INIT(pool->vm_files[i].slot_pages);
    }

    uint32 max_io_operation_count = 100;
    uint32 io_context_count = 100;
    pool->aio_array = os_aio_array_create(max_io_operation_count, io_context_count);
    if (pool->aio_array == NULL) {
        os_mem_free_large(pool->buf, pool->memory_size);
        ut_free(pool);
        log_to_stderr(LOG_ERROR, "vm_pool_create: error, can not create aio array");
        return NULL;
    }

    os_file_init();

    return pool;
}

void vm_pool_destroy(vm_pool_t *pool)
{
    //
    for (uint32 i = 0; i < VM_FILE_COUNT; i++) {
        if (pool->vm_files[i].handle != OS_FILE_INVALID_HANDLE) {
            os_close_file(pool->vm_files[i].handle);
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

bool32 vm_pool_add_file(vm_pool_t *pool, char *name, uint64 size)
{
    bool32     ret;
    os_file_t  file;
    vm_file_t *vm_file = NULL;

    if (size  > (uint64)1024 * 1024 * 1024 * 1024) {
        log_to_stderr(LOG_ERROR, "vm_pool_add_file: error, invalid size %lu", size);
        return FALSE;
    }
    size = size / (8 * 1024 * 1024) * (8 * 1024 * 1024);

    ret = os_open_file(name, OS_FILE_CREATE, OS_FILE_AIO, &file);
    if (ret == FALSE) {
        log_to_stderr(LOG_ERROR, "vm_pool_add_file: error, can not create file %s", name);
        return FALSE;
    }

    mutex_enter(&pool->mutex, NULL);
    for (uint32 i = 0; i < VM_FILE_COUNT; i++) {
        if (pool->vm_files[i].name == NULL) {
            vm_file = &pool->vm_files[i];
            vm_file->id = i;
            vm_file->name = (char *)malloc(strlen(name) + 1);
            strcpy_s(vm_file->name, strlen(name) + 1, name);
            vm_file->name[strlen(name)] = '\0';
            vm_file->page_max_count = (uint32)(size / pool->page_size);
            vm_file->handle = file;
            vm_file->free_slots = NULL;
            mutex_create(&vm_file->mutex);
            UT_LIST_INIT(vm_file->slot_pages);
            break;
        }
    }
    mutex_exit(&pool->mutex);

    if (vm_file == NULL) {
        os_close_file(file);
        log_to_stderr(LOG_ERROR, "vm_pool_add_file: error, file array is full, %s", name);
        return FALSE;
    }

    uint32 slot_page_count;
    uint32 slot_count_pre_page;

    slot_count_pre_page = pool->page_size / sizeof(vm_page_slot_t);
    slot_page_count = vm_file->page_max_count / slot_count_pre_page;
    if (slot_page_count * slot_count_pre_page != vm_file->page_max_count) {
        slot_page_count += 1;
    }

    vm_ctrl_t *ctrl = NULL;
    vm_page_slot_t *cur_slot = NULL;
    for (uint32 page_index = 0; page_index < slot_page_count; page_index++) {
        ctrl = vm_alloc(pool);
        if (ctrl == NULL) {
            log_to_stderr(LOG_ERROR, "vm_pool_add_file: error for alloc vm_ctrl");
            goto err_exit;
        }
        if (!vm_open(pool, ctrl)) {
            log_to_stderr(LOG_ERROR, "vm_pool_add_file: error for vm_open");
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
#ifdef DEBUG_OUTPUT
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

    return TRUE;

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

    free(vm_file->name);
    vm_file->name = NULL;

    os_close_file(file);

    return FALSE;
}

static void vm_pool_fill_ctrls_by_page(vm_pool_t *pool, vm_page_t *page)
{
    vm_ctrl_t * ctrl;

    ut_ad(mutex_own(&pool->mutex));

    for (uint32 i = 0; i < pool->ctrl_count_per_page; i++) {
        ctrl = (vm_ctrl_t *)((char *)page + i * sizeof(vm_ctrl_t));
        UT_LIST_ADD_FIRST(list_node, pool->free_ctrls, ctrl);
#ifdef DEBUG_OUTPUT
        ctrl->id = pool->ctrl_sequence++;
#endif
    }
}

static vm_page_t* vm_alloc_ctrls_from_free_pages(vm_pool_t *pool)
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

vm_ctrl_t* vm_alloc(vm_pool_t *pool)
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
        if (pool->ctrl_page_count >= VM_CTRL_PAGE_MAX_COUNT) {
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
        mutex_create(&ctrl->mutex);
        ctrl->swap_page_id = INVALID_SWAP_PAGE_ID;
        ctrl->val.page = NULL;
        ctrl->io_in_progress = FALSE;
        ctrl->is_free = FALSE;
        ctrl->is_in_closed_list = FALSE;
        ctrl->ref_num = 0;
    }

    return ctrl;
}

static void vm_lock_ctrl_for_free(vm_ctrl_t* ctrl)
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

bool32 vm_free(vm_pool_t *pool, vm_ctrl_t *ctrl)
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

static vm_page_t* vm_swap_out_page(vm_pool_t *pool)
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

static vm_page_t* vm_alloc_page(vm_pool_t *pool)
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

static void vm_lock_ctrl_for_open_close(vm_ctrl_t* ctrl)
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

bool32 vm_open(vm_pool_t *pool, vm_ctrl_t* ctrl)
{
    vm_page_t *page = NULL;

    DBUG_ENTER("vm_open");

    vm_lock_ctrl_for_open_close(ctrl);
    if (ctrl->is_free) {
        mutex_exit(&ctrl->mutex);
        DBUG_PRINT("error, ctrl %p is free", ctrl);
        DBUG_RETURN(FALSE);
    }

    if (ctrl->val.page == NULL) {
        ctrl->io_in_progress = TRUE;
    }

    if (ctrl->ref_num > 0) {
        ctrl->ref_num++;
        mutex_exit(&ctrl->mutex);
        DBUG_PRINT("ctrl %p : ref_num %u", ctrl, ctrl->ref_num);
        DBUG_RETURN(TRUE);
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

bool32 vm_close(vm_pool_t *pool, vm_ctrl_t *ctrl)
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

static void vm_file_free_page(vm_pool_t *pool, vm_file_t *vm_file, uint64 swap_page_id)
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
#ifdef DEBUG_OUTPUT
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

static void vm_file_get_slot_page_index_by_slot(vm_pool_t *pool, vm_file_t *vm_file,
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

static uint64 vm_file_alloc_page_low(vm_pool_t *pool,
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

#ifdef DEBUG_OUTPUT
            ut_a(slot->id == slot_page_index * pool->slot_count_pre_page + slot_index);
#endif

            swap_page_id = ((uint64)vm_file->id << 32) +
                           slot_page_index * pool->slot_count_pre_page * pool->page_count_pre_slot +
                           slot_index * pool->page_count_pre_slot +
                           byte_index * 8 + bit_index;
            DBUG_PRINT("slot %p file %u page no %u£ºpage index %u slot index %u byte index %d bit index %d",
                slot, VM_GET_FILE_BY_SWAP_PAGE_ID(swap_page_id), VM_GET_PAGE_NO_BY_SWAP_PAGE_ID(swap_page_id),
                slot_page_index, slot_index, byte_index, bit_index);
            break;
        }
    }

    DBUG_RETURN(swap_page_id);
}

static uint64 vm_file_alloc_page(vm_pool_t *pool, vm_file_t *vm_file, vm_ctrl_t *ctrl)
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

static bool32 vm_file_swap_out(vm_pool_t *pool, vm_ctrl_t *ctrl, uint64 *swap_page_id)
{
    bool32            ret = TRUE;
    const char       *name = ""; // name of the file or path as a null-terminated string
    uint64            offset; // file offset where to read or write */
    void*             message1 = NULL;
    void*             message2 = NULL;
    uint32            microseconds = 5000000; // 5 seconds
    vm_file_t        *vm_file = NULL;
    os_aio_context_t* aio_ctx;
    int               aio_ret;

    DBUG_ENTER("vm_file_swap_out");

    *swap_page_id = INVALID_SWAP_PAGE_ID;

    mutex_enter(&pool->mutex, NULL);
    //pool->vm_file_index = atomic32_inc(&pool->vm_file_index);
    for (uint32 i = 0; i < VM_FILE_COUNT; i++) {
        if (pool->vm_files[i].name == NULL) {
            break;
        }

        *swap_page_id = vm_file_alloc_page(pool, &pool->vm_files[i], ctrl);
        vm_file = &pool->vm_files[i];
        break;
    }
    mutex_exit(&pool->mutex);

    if (*swap_page_id == INVALID_SWAP_PAGE_ID) {
        DBUG_PRINT("ctrl %p no free space", ctrl);
        DBUG_RETURN(FALSE);
    }
    offset = (*swap_page_id & 0xFFFFFFFF) * pool->page_size;
    DBUG_PRINT("ctrl %p swap_page_id: file %u page no %u offset %lu", ctrl,
        VM_GET_FILE_BY_SWAP_PAGE_ID(*swap_page_id),
        VM_GET_PAGE_NO_BY_SWAP_PAGE_ID(*swap_page_id), offset);

    aio_ctx = os_aio_array_alloc_context(pool->aio_array);
    if (!os_file_aio_submit(aio_ctx, OS_FILE_WRITE,
                            name, vm_file->handle, (void *)ctrl->val.page, pool->page_size, offset,
                            message1, message2)) {
        //ret_val = os_file_get_last_error();
        vm_file_free_page(pool, vm_file, *swap_page_id);
        ret = FALSE;
        DBUG_PRINT("ctrl %p error for os_file_aio_submit", ctrl);
        goto err_exit;
    }
    aio_ret = os_file_aio_wait(aio_ctx, microseconds);
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

    os_aio_array_free_context(aio_ctx);

    DBUG_RETURN(ret);
}

static bool32 vm_file_swap_in(vm_pool_t *pool, vm_page_t *page, vm_ctrl_t *ctrl)
{
    bool32      ret = TRUE;
    const char* name = ""; // name of the file or path as a null-terminated string
    uint64      offset; // file offset where to read or write */
    void*       message1 = NULL;
    void*       message2 = NULL;
    uint32      microseconds = 5000000; // 5 seconds
    vm_file_t  *vm_file = NULL;
    os_aio_context_t* aio_ctx;
    int aio_ret;

    DBUG_ENTER("vm_file_swap_in");

    vm_file = &pool->vm_files[VM_GET_FILE_BY_SWAP_PAGE_ID(ctrl->swap_page_id)];
    offset = VM_GET_OFFSET_BY_SWAP_PAGE_ID(ctrl->swap_page_id);
    DBUG_PRINT("ctrl %p swap_page_id: file %u page no %u offset %lu", ctrl,
        VM_GET_FILE_BY_SWAP_PAGE_ID(ctrl->swap_page_id),
        VM_GET_PAGE_NO_BY_SWAP_PAGE_ID(ctrl->swap_page_id), offset);

    aio_ctx = os_aio_array_alloc_context(pool->aio_array);
    if (!os_file_aio_submit(aio_ctx, OS_FILE_READ,
                            name, vm_file->handle, (void *)page, pool->page_size, offset,
                            message1, message2)) {
        //ret_val = os_file_get_last_error();
        os_aio_array_free_context(aio_ctx);
        ret = FALSE;
        DBUG_PRINT("ctrl %p error for os_file_aio_submit", ctrl);
        goto err_exit;
    }
    aio_ret = os_file_aio_wait(aio_ctx, microseconds);
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

    os_aio_array_free_context(aio_ctx);

    DBUG_RETURN(ret);
}
