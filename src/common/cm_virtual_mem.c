#include "cm_virtual_mem.h"
#include "cm_util.h"

#define INVALID_SWAP_PAGE_ID           (uint64)-1

vm_pool_t* vm_pool_create(uint64 memory_size, vm_swapper_t *swapper)
{
    uint32 page_count = (uint32)(memory_size / VM_PAGE_SIZE);
    vm_pool_t *pool = (vm_pool_t *)malloc(page_count * VM_PAGE_SIZE + ut_align8(sizeof(vm_pool_t)));
    if (pool != NULL) {
        pool->swapper = swapper;
        pool->buf = (char *)pool + ut_align8(sizeof(vm_pool_t));
        pool->page_hwm = 0;
        pool->page_count = page_count;
        pool->ctrl_page_count = 0;
        pool->lock = 0;
        UT_LIST_INIT(pool->free_ctrls);
        UT_LIST_INIT(pool->free_pages);
    }

    return pool;
}

void vm_pool_destroy(vm_pool_t *pool)
{
    // swapper

    //
    free(pool);
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
        ctrl->lock = 0;
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

