#include "cm_virtual_mem.h"
#include "cm_memory.h"
#include "cm_util.h"

#define INVALID_SWAP_PAGE_ID           (uint64)-1
#define INVALID_PAGE_SLOT              0xFFFF

static vm_page_t* vm_alloc_page(vm_pool_t *pool);

vm_pool_t* vm_pool_create(uint64 memory_size, vm_swapper_t *swapper)
{
    uint64 page_count = memory_size / VM_PAGE_SIZE;
    vm_pool_t *pool = (vm_pool_t *)malloc(ut_align8(sizeof(vm_pool_t)));
    if (pool == NULL) {
        return NULL;
    }

    pool->swapper = swapper;
    pool->memory_size = page_count * VM_PAGE_SIZE;
    pool->buf = (char *)os_mem_alloc_large(&pool->memory_size);
    pool->page_hwm = 0;
    pool->page_count = (uint32)page_count;
    pool->ctrl_page_count = 0;
    spin_lock_init(&pool->lock);
    UT_LIST_INIT(pool->free_ctrls);
    UT_LIST_INIT(pool->free_pages);

    for (uint32 i = 0; i < VM_FILE_COUNT; i++) {
        pool->vm_files[i].name = NULL;
        pool->vm_files[i].handle = OS_FILE_INVALID_HANDLE;
        pool->vm_files[i].page_max_count = 0;
        pool->vm_files[i].free_slots = NULL;
        pool->vm_files[i].full_slots = NULL;
        UT_LIST_INIT(pool->vm_files[i].slot_pages);
    }

    uint32 max_io_operation_count = 100;
    uint32 io_context_count = 100;
    pool->aio_array = os_aio_array_create(max_io_operation_count, io_context_count);

    return pool;
}

void vm_pool_destroy(vm_pool_t *pool)
{
    // swapper
    for (uint32 i = 0; i < VM_FILE_COUNT; i++) {
        if (pool->vm_files[i].handle != OS_FILE_INVALID_HANDLE) {
            os_close_file(pool->vm_files[i].handle);
        }
    }
    //
    os_mem_free_large(pool->buf, pool->memory_size);

    os_aio_array_free(pool->aio_array);

    free(pool);
}

bool32 vm_pool_add_file(vm_pool_t *pool, char *name, uint64 mb_size)
{
    bool32 ret;
    os_file_t file;
    vm_file_t *vm_file = NULL;

    ret = os_open_file(name, OS_FILE_CREATE, OS_FILE_AIO, &file);
    if (ret == FALSE) {
        return FALSE;
    }

    spin_lock(&pool->lock, NULL);
    for (uint32 i = 0; i < VM_FILE_COUNT; i++) {
        if (pool->vm_files[i].name != NULL) {
            vm_file = &pool->vm_files[i];
            vm_file->name = (char *)malloc(strlen(name) + 1);
            strcpy_s(vm_file->name, strlen(name) + 1, name);
            vm_file->name[strlen(name)] = '\0';
            vm_file->page_max_count = (uint32)((mb_size * 1024 * 1024) / VM_PAGE_SIZE);
            vm_file->handle = file;
            break;
        }
    }
    spin_unlock(&pool->lock);

    if (vm_file == NULL) {
        os_close_file(file);
        return FALSE;
    }

    uint32 slot_page_count;
    uint32 slot_count_pre_page;

    slot_count_pre_page = (VM_PAGE_SIZE - VM_PAGE_HEADER_SIZE) / sizeof(vm_page_slot_t);
    slot_page_count = vm_file->page_max_count / slot_count_pre_page;
    if (slot_page_count * slot_count_pre_page != vm_file->page_max_count) {
        slot_page_count += 1;
    }

    vm_file->free_slots = NULL;
    for (uint32 page_index = 0; page_index < slot_page_count; page_index++) {
        vm_page_t *slot_page = vm_alloc_page(pool);
        UT_LIST_ADD_FIRST(list_node, vm_file->slot_pages, slot_page);

        uint32 slot_count;
        if (page_index == slot_page_count - 1) {
            slot_count = vm_file->page_max_count % slot_count_pre_page / 64;
        } else {
            slot_count = slot_count_pre_page;
        }
        for (uint32 i = 0; i < slot_count_pre_page; i++) {
            vm_page_slot_t *slot = (vm_page_slot_t *)((char *)slot_page + VM_PAGE_HEADER_SIZE) + i;
            slot->val.bitmaps = 0;
            //
            slot->next = vm_file->free_slots;
            vm_file->free_slots = slot;
        }
    }

    return TRUE;
}

static void vm_lock_ctrl(vm_ctrl_t* ctrl)
{
    for (;;) {
        spin_lock(&ctrl->lock, NULL);
        if (ctrl->io_in_progress == 0) {
            break;
        }
        spin_unlock(&ctrl->lock);
        os_thread_sleep(1);
    }
}

static vm_page_t* vm_alloc_from_free_pages(vm_pool_t *pool)
{
    vm_page_t *page;

    page = UT_LIST_GET_FIRST(pool->free_pages);
    if (page != NULL) {
        UT_LIST_REMOVE(list_node, pool->free_pages, page);
    } else if (pool->page_hwm < pool->page_count) {
        page = (vm_page_t *)(pool->buf + pool->page_hwm * VM_PAGE_SIZE);
        pool->page_hwm++;
        pool->ctrl_page_count++;
    }

    return page;
}

static void vm_fill_ctrl(vm_pool_t *pool)
{
    vm_ctrl_t *ctrl = NULL;
    vm_page_t *page;

    spin_lock(&pool->lock, NULL);

    page = vm_alloc_from_free_pages(pool);
    if (page) {
        for (int i = 0; i < VM_CTRLS_PER_PAGE; i++) {
            ctrl = (vm_ctrl_t *)((char *)page + sizeof(vm_page_t) + i * ut_align8(sizeof(vm_ctrl_t)));
            UT_LIST_ADD_FIRST(list_node, pool->free_ctrls, ctrl);
        }
        return;
    }

    // swap out


    spin_unlock(&pool->lock);


}


vm_ctrl_t* vm_alloc(vm_pool_t *pool)
{
    vm_ctrl_t *ctrl = NULL;
    vm_page_t *page;

    spin_lock(&pool->lock, NULL);
    while (ctrl == NULL) {
        ctrl = UT_LIST_GET_FIRST(pool->free_ctrls);
        if (ctrl != NULL) {
            UT_LIST_REMOVE(list_node, pool->free_ctrls, ctrl);
            break;
        }

        page = UT_LIST_GET_FIRST(pool->free_pages);
        if (page != NULL) {
            UT_LIST_REMOVE(list_node, pool->free_pages, page);
        } else if (pool->page_hwm < pool->page_count) {
            page = (vm_page_t *)(pool->buf + pool->page_hwm * VM_PAGE_SIZE);
            pool->page_hwm++;
            pool->ctrl_page_count++;
        } else if (pool->ctrl_page_count < VM_MAX_CTRLS) {
            // alloc page
            break;
        }
        if (page == NULL) {
            break;
        }

        for (int i = 0; i < VM_CTRLS_PER_PAGE; i++) {
            vm_ctrl_t * tmp = (vm_ctrl_t *)((char *)page + sizeof(vm_page_t) + i * ut_align8(sizeof(vm_ctrl_t)));
            UT_LIST_ADD_FIRST(list_node, pool->free_ctrls, tmp);
        }
    }
    spin_unlock(&pool->lock);

    if (ctrl != NULL) {
        spin_lock_init(&ctrl->lock);
        ctrl->swap_page_id = INVALID_SWAP_PAGE_ID;
        ctrl->page = NULL;
        ctrl->io_in_progress = 0;
        ctrl->is_free = 0;
        ctrl->is_in_closed_list = 0;
        ctrl->ref_num = 0;
    }

    return ctrl;
}

void vm_free(vm_pool_t *pool, vm_ctrl_t *ctrl)
{
    uint64 swid;
    vm_page_t *page;

    if (ctrl->is_free) {
        return;
    }

    vm_lock_ctrl(ctrl);
    ctrl->is_free = 1;
    swid = ctrl->swap_page_id;
    page = ctrl->page;
    ctrl->page = NULL;
    spin_unlock(&ctrl->lock);

    spin_lock(&pool->lock, NULL);
    if (page) {
        UT_LIST_ADD_LAST(list_node, pool->free_pages, page);
    }
    if (ctrl->is_in_closed_list) {
        UT_LIST_REMOVE(list_node, pool->closed_page_ctrls, ctrl);
        ctrl->is_in_closed_list = 0;
    }
    UT_LIST_ADD_LAST(list_node, pool->free_ctrls, ctrl);
    spin_unlock(&pool->lock);

    if (swid != INVALID_SWAP_PAGE_ID) {
        pool->swapper->clean(swid);
    }
}

static vm_page_t* vm_swap_out_page(vm_pool_t *pool)
{
    vm_page_t *page = NULL;
    vm_ctrl_t *ctrl;

    for (;;) {
        spin_lock(&pool->lock, NULL);
        ctrl = UT_LIST_GET_FIRST(pool->closed_page_ctrls);
        if (ctrl != NULL) {
            UT_LIST_REMOVE(list_node, pool->closed_page_ctrls, ctrl);
            ctrl->is_in_closed_list = 0;
        }
        spin_unlock(&pool->lock);

        if (ctrl == NULL) {
            break;
        }

        spin_lock(&ctrl->lock, NULL);
        if (ctrl->ref_num == 0 && ctrl->page != NULL) {
            ctrl->io_in_progress = 1;
        }
        spin_unlock(&ctrl->lock);

        if (ctrl->io_in_progress) {
            if (pool->swapper->out(ctrl->page, &ctrl->swap_page_id)) {
                break;
            }
            page = ctrl->page;
            spin_lock(&ctrl->lock, NULL);
            ctrl->page = NULL;
            ctrl->io_in_progress = 0;
            spin_unlock(&ctrl->lock);
        }
    }

    return page;
}

static vm_page_t* vm_alloc_page(vm_pool_t *pool)
{
    vm_page_t *page;

    spin_lock(&pool->lock, NULL);
    page = UT_LIST_GET_FIRST(pool->free_pages);
    if (page != NULL) {
        UT_LIST_REMOVE(list_node, pool->free_pages, page);
    } else if (pool->page_hwm < pool->page_count) {
        page = (vm_page_t *)(pool->buf + pool->page_hwm * VM_PAGE_SIZE);
        //page->page_no = pool->page_hwm;
        pool->page_hwm++;
    }
    spin_unlock(&pool->lock);

    if (page == NULL) {
        page = vm_swap_out_page(pool);
    }

    return page;
}

bool32 vm_open(vm_pool_t *pool, vm_ctrl_t* ctrl)
{
    bool32 is_in_closed = FALSE;

    if (ctrl->is_free) {
        return FALSE;
    }

    vm_lock_ctrl(ctrl);
    if (ctrl->ref_num > 0) {
        ctrl->ref_num++;
        spin_unlock(&ctrl->lock);
        return TRUE;
    }

    ctrl->ref_num = 1;
    if (ctrl->page) {
        is_in_closed = TRUE;
    }
    spin_unlock(&ctrl->lock);

    spin_lock(&pool->lock, NULL);
    if (is_in_closed && ctrl->is_in_closed_list) {
        UT_LIST_REMOVE(list_node, pool->closed_page_ctrls, ctrl);
        ctrl->is_in_closed_list = 0;
    }
    spin_unlock(&pool->lock);

    if (ctrl->page) {
        return TRUE;
    }

    vm_page_t *page = vm_alloc_page(pool);
    if (page != NULL && ctrl->swap_page_id != INVALID_SWAP_PAGE_ID) {
        if (pool->swapper->in(page, ctrl->swap_page_id)) {
            vm_lock_ctrl(ctrl);
            ctrl->swap_page_id = INVALID_SWAP_PAGE_ID;
            ctrl->page = page;
            spin_unlock(&ctrl->lock);
        } else {
            spin_lock(&pool->lock, NULL);
            UT_LIST_ADD_LAST(list_node, pool->free_pages, page);
            spin_unlock(&pool->lock);
        }
    }

    return ctrl->page ? TRUE : FALSE;
}

void vm_close(vm_pool_t *pool, vm_ctrl_t *ctrl)
{
    bool32 is_closed = FALSE;

    if (ctrl->is_free) {
        return;
    }

    vm_lock_ctrl(ctrl);
    if (ctrl->ref_num == 0) {
        spin_unlock(&ctrl->lock);
        return;
    }
    ctrl->ref_num--;
    if (ctrl->ref_num == 0 && ctrl->page) {
        is_closed = TRUE;
    }
    spin_unlock(&ctrl->lock);

    spin_lock(&pool->lock, NULL);
    if (is_closed && !ctrl->is_in_closed_list) {
        UT_LIST_ADD_LAST(list_node, pool->closed_page_ctrls, ctrl);
        ctrl->is_in_closed_list = 1;
    }
    spin_unlock(&pool->lock);
}

static void vm_file_free_page(vm_file_t *vm_file, uint64 swap_page_id)
{
    uint32 page_no;
    vm_page_slot_t *slot;
    uint32 page_count_pre_slot = 64;
    uint32 slot_count_pre_page = (VM_PAGE_SIZE - sizeof(vm_page_t)) / sizeof(vm_page_slot_t);
    page_no = swap_page_id & 0xFFFFFFFF;
    uint32 page_count = page_no / slot_count_pre_page;
    uint32 slot_index = (page_no % slot_count_pre_page) / page_count_pre_slot;
    uint32 bit_pos = (page_no % slot_count_pre_page) % page_count_pre_slot;
    if (bit_pos > 0) {
        slot_index += 1;
    }

    vm_page_t *page = UT_LIST_GET_FIRST(vm_file->slot_pages);
    for (uint32 i = 0; i < page_count; i++) {
        page = UT_LIST_GET_NEXT(list_node, page);
    }
    slot = (vm_page_slot_t *)((char *)page + sizeof(vm_page_t) + sizeof(vm_page_slot_t) * slot_index);

    mutex_enter(&vm_file->mutex, NULL);
    if (slot->val.bitmaps == 0xFFFFFFFF) {
        slot->next = vm_file->free_slots;
        vm_file->free_slots = slot;
    }
    //slot->val.bitmaps = ut_bit_set_nth(slot->val.bitmaps, bit_pos, FALSE);
    mutex_exit(&vm_file->mutex);
}

static void vm_file_get_slot_page_index_by_slot(vm_file_t *vm_file, vm_page_slot_t *slot,
    uint32 *slot_page_index, uint32 *slot_index)
{
    vm_page_t *page;

    ut_a(mutex_own(&vm_file->mutex));

    page = UT_LIST_GET_FIRST(vm_file->slot_pages);

    *slot_page_index = 0;
    while ((char *)slot + VM_PAGE_SIZE < (char *)page) {
        page = UT_LIST_GET_NEXT(list_node, page);
        *slot_page_index += 1;
    }

    *slot_index = (uint32)((char *)slot - ((char *)page + sizeof(vm_page_t))) / sizeof(vm_page_slot_t);
}

static uint64 vm_file_alloc_page_low(vm_file_t *vm_file, vm_page_slot_t *slot)
{
    uint64 swap_page_id  = INVALID_SWAP_PAGE_ID;
    uint32 slot_count_pre_page = (VM_PAGE_SIZE - sizeof(vm_page_t)) / sizeof(vm_page_slot_t);

    ut_a(mutex_own(&vm_file->mutex));

    for (uint32 i = 0; i < 8; i++) {
        if (ut_bit_get_nth(slot->val.byte_bitmap[i], i)) {
            slot->val.byte_bitmap[i] = ut_bit_set_nth(slot->val.byte_bitmap[i], i, TRUE);

            uint32 slot_page_index;
            uint32 slot_index;
            vm_file_get_slot_page_index_by_slot(vm_file, slot, &slot_page_index, &slot_index);
            swap_page_id = (vm_file->id << 32) + slot_page_index * slot_count_pre_page + slot_index * 64 + i;

            if (slot->val.bitmaps == 0xFFFFFFFF) {
                vm_file->free_slots = slot->next;
            }
            break;
        }
    }

    return swap_page_id;
}


static uint64 vm_file_alloc_page(vm_file_t *vm_file)
{
    uint64 swap_page_id  = INVALID_SWAP_PAGE_ID;
    uint32 slot_count_pre_page = (VM_PAGE_SIZE - sizeof(vm_page_t)) / sizeof(vm_page_slot_t);

    mutex_enter(&vm_file->mutex, NULL);
    if (vm_file->free_slots) {
        ut_a(vm_file->free_slots->val.bitmaps != 0xFFFFFFFF);
        for (uint32 i = 0; i < 8; i++) {
            if (vm_file->free_slots->val.byte_bitmap[i] != 0xFF) {
                swap_page_id = vm_file_alloc_page_low(vm_file, vm_file->free_slots);
                break;
            }
        }
    }
    mutex_exit(&vm_file->mutex);

    return swap_page_id;
}

bool32 swap_out(vm_pool_t *pool, uint64 *swap_page_id, byte *page, uint32 page_size)
{
    const char* name = ""; // name of the file or path as a null-terminated string
    os_file_t   file; // handle to a file */
    void*       buf = page; // buffer where to read or from which to write */
    uint32      count = page_size; /// number of bytes to read or write */
    uint64      offset; // file offset where to read or write */
    void*       message1 = NULL;
    void*       message2 = NULL;
    uint32      microseconds = 5000000; // 5 seconds
    os_aio_array_t* array;
    vm_file_t  *vm_file = NULL;

    *swap_page_id = INVALID_SWAP_PAGE_ID;

    spin_lock(&pool->lock, NULL);
    //pool->vm_file_index = atomic32_inc(&pool->vm_file_index);
    for (uint32 i = 0; i < VM_FILE_COUNT; i++) {
        if (pool->vm_files[i].name == NULL) {
            break;
        }

        *swap_page_id = vm_file_alloc_page(&pool->vm_files[i]);
        vm_file = &pool->vm_files[i];
        break;
    }
    spin_unlock(&pool->lock);

    if (*swap_page_id == INVALID_SWAP_PAGE_ID) {
        return FALSE;
    }

    offset = (*swap_page_id & 0xFFFFFFFF) * VM_PAGE_SIZE;

    os_aio_context_t* aio_ctx;
    int ret;

    aio_ctx = os_aio_array_alloc_context(pool->aio_array);
    if (!os_file_aio_submit(aio_ctx, OS_FILE_WRITE,
                            name, vm_file->handle, buf, count, offset,
                            message1, message2)) {
        //ret_val = os_file_get_last_error();
        vm_file_free_page(vm_file, *swap_page_id);
    }
    ret = os_file_aio_wait(aio_ctx, microseconds);

    

    os_aio_array_free_context(aio_ctx);

    return TRUE;
}

