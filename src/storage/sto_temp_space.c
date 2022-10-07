#include "sto_temp_space.h"
#include "cm_dbug.h"
#include "cm_util.h"

#define INVAID_PAGE_NO    0xFFFFFFFF

static temp_space_t fil_temp_space;

static uint32 temp_space_xdes_get_free_page(temp_space_xdesc_t *desc)
{
    byte bits = 0;
    uint32  bits_idx;

    for (bits_idx = 0; bits_idx < TEMP_SPACE_XDESC_BITMAP_SIZE; bits_idx++) {
        if (desc->bitmap.bits[bits_idx]) {
            bits = desc->bitmap.bits[bits_idx];
            break;
        }
    }

    ut_ad(bits == 0);

    for (uint32 i = 0; i < TEMP_SPACE_XDESC_BITMAP_SIZE; i++) {
        if (bits & (1 << i)) {
            return (uint32)desc->id * TEMP_SPACE_XDESC_BITMAP_SIZE * TEMP_SPACE_XDESC_BITMAP_SIZE + bits_idx * TEMP_SPACE_XDESC_BITMAP_SIZE + i;
        }
    }

    return INVAID_PAGE_NO;
}

static void temp_space_xdes_set_used_page(temp_space_xdesc_t *desc, uint32 page_no)
{
    byte bits = 0;
    uint32 idx = page_no % TEMP_SPACE_XDESC_BITMAP_SIZE;

    desc->bitmap.value = desc->bitmap.value & (~(1 << idx));
}

static void temp_space_xdes_set_free_page(temp_space_xdesc_t *desc, uint32 page_no)
{
    byte bits = 0;
    uint32 idx = page_no % TEMP_SPACE_XDESC_BITMAP_SIZE;

    desc->bitmap.value = desc->bitmap.value | ((uint64)1 << idx);
}

static bool32 temp_space_xdesc_extend(temp_space_fil_node_t *node)
{
    uint32 desc_count_pre_page = fil_temp_space.pool->page_size / ut_align8(sizeof(temp_space_xdesc_t));
    uint32 curr_page_count = desc_count_pre_page * UT_LIST_GET_LEN(node->xdesc_pages) * TEMP_SPACE_XDESC_BITMAP_SIZE;
    if (curr_page_count >= node->page_max_count) {
        return FALSE;
    }
    uint32 xdesc_count = (node->page_max_count - curr_page_count) / TEMP_SPACE_XDESC_BITMAP_SIZE;

    memory_page_t *page = mpool_alloc_page(fil_temp_space.pool);
    if (page == NULL) {
        return FALSE;
    }

    for (uint32 i = 0; i < desc_count_pre_page && i < xdesc_count; i++) {
#define PageData(page)  ((char *)page + ut_align8(sizeof(memory_page_t)))
        temp_space_xdesc_t *desc = (temp_space_xdesc_t *)(PageData(page) + ut_align8(sizeof(temp_space_xdesc_t)) * i);
        desc->id = UT_LIST_GET_LEN(node->xdesc_pages) * desc_count_pre_page + i;
        desc->bitmap.value = 0xFFFFFFFF;
        UT_LIST_ADD_LAST(list_node, node->free_xdescs, desc);
    }

    UT_LIST_ADD_LAST(list_node, node->xdesc_pages, page);

    return TRUE;
}

static void temp_space_fil_node_lock(temp_space_fil_node_t* node)
{
    for (;;) {
        spin_lock(&node->lock, NULL);
        if (node->io_in_progress == 0) {
            node->io_in_progress = 1;
            spin_unlock(&node->lock);
            break;
        }
        spin_unlock(&node->lock);
        os_thread_sleep(1);
    }
}

static void temp_space_fil_node_unlock(temp_space_fil_node_t* node)
{
    spin_lock(&node->lock, NULL);
    node->io_in_progress = 0;
    spin_unlock(&node->lock);
}

static bool32 temp_space_alloc_from_fil_node(temp_space_fil_node_t* node, uint64 *swid)
{
    temp_space_xdesc_t *desc;
    uint32 page_no = INVAID_PAGE_NO;

    spin_lock(&node->lock, NULL);
    for (;;) {
        desc = UT_LIST_GET_FIRST(node->free_xdescs);
        if (desc) {
            page_no = temp_space_xdes_get_free_page(desc);
            temp_space_xdes_set_used_page(desc, page_no);
            *swid = ((uint64)node->id << 32) + page_no;

            if (desc->bitmap.value == 0) {
                UT_LIST_REMOVE(list_node, node->free_xdescs, desc);
            }
            break;
        }

        if (!temp_space_xdesc_extend(node)) {
            *swid = TEMP_SPACE_INVALID_PAGE_ID;
            break;
        }
    }
    spin_unlock(&node->lock);

    if (page_no != INVAID_PAGE_NO && page_no > node->page_hwm) {
        char name[32];
        snprintf(name, 32, "temp%d.dbf", node->id + 1);

        temp_space_fil_node_lock(node);
        if (page_no > node->page_hwm) {
            if (!os_file_extend(name, node->file, node->extend_size)) {
                temp_space_xdes_set_free_page(desc, page_no);
                *swid = TEMP_SPACE_INVALID_PAGE_ID;
            } else {
                node->page_hwm += (uint32)(node->extend_size / TEMP_SPACE_PAGE_SIZE);
            }
        }
        temp_space_fil_node_unlock(node);
    }

    return *swid == TEMP_SPACE_INVALID_PAGE_ID ? FALSE : TRUE;
}


static volatile uint32 g_vm_fil_node_index = 0;

static bool32 temp_space_alloc_fil_page(uint64 *swid)
{
    uint32 idx = g_vm_fil_node_index++;
    temp_space_fil_node_t *fil_node;

    for (uint32 i = 0; i < fil_temp_space.node_count; i++) {
        fil_node = &fil_temp_space.nodes[(idx + i) % fil_temp_space.node_count];
        if (temp_space_alloc_from_fil_node(fil_node, swid)) {
            return TRUE;
        }
    }

    return FALSE;
}

temp_space_xdesc_t* temp_space_get_xdesc(temp_space_fil_node_t* node, uint32 page_no)
{
    uint32 desc_count_pre_page = fil_temp_space.pool->page_size / ut_align8(sizeof(temp_space_xdesc_t));
    uint32 xdesc_idx = page_no % desc_count_pre_page;

    memory_page_t *page = UT_LIST_GET_FIRST(node->xdesc_pages);
    for (;;) {
        if (page == NULL) {
            ut_ad(page);
        }
        if (page_no <= desc_count_pre_page) {
            // found
            break;
        } else {
            page_no -= desc_count_pre_page;
        }
        page = UT_LIST_GET_NEXT(list_node, page);
    }

    return (temp_space_xdesc_t *)(PageData(page) + ut_align8(sizeof(temp_space_xdesc_t)) * xdesc_idx);
}

static void temp_space_free_fil_page(uint64 swid)
{
    uint32 idx = swid >> 32;
    uint32 page_no = swid & 0xFFFFFFFF;
    temp_space_xdesc_t *desc = temp_space_get_xdesc(&fil_temp_space.nodes[idx], page_no);

    spin_lock(&fil_temp_space.nodes[idx].lock, NULL);
    temp_space_xdes_set_free_page(desc, page_no);
    spin_unlock(&fil_temp_space.nodes[idx].lock);
}

bool32 temp_space_swap_in(memory_page_t *page, uint64 swid)
{
    uint32 fil_node_idx = swid >> 32;
    uint32 page_no = swid & 0xFFFFFFFF;

    if (fil_node_idx >= fil_temp_space.node_count) {
        return FALSE;
    }

    if (fil_temp_space.nodes[fil_node_idx].file == OS_FILE_INVALID_HANDLE) {
        char name[32];
        snprintf(name, 32, "temp%d.dbf", fil_node_idx + 1);
        if (!os_open_file(name, OS_FILE_OPEN, 0, &fil_temp_space.nodes[fil_node_idx].file)) {
            return FALSE;
        }
    }

    uint32 read_size;
    uint64 offset = (uint64)page_no * TEMP_SPACE_PAGE_SIZE;
    if (os_pread_file(fil_temp_space.nodes[fil_node_idx].file, offset, PageData(page), TEMP_SPACE_PAGE_SIZE, &read_size)) {
        temp_space_free_fil_page(swid);
        return TRUE;
    }

    return FALSE;
}

bool32 temp_space_swap_out(memory_page_t *page, uint64 *swid)
{
    if (!temp_space_alloc_fil_page(swid)) {
        return FALSE;
    }

    uint32 fil_node_idx = *swid >> 32;
    uint32 page_no = *swid & 0xFFFFFFFF;

    if (fil_node_idx >= fil_temp_space.node_count) {
        return FALSE;
    }

    if (fil_temp_space.nodes[fil_node_idx].file == OS_FILE_INVALID_HANDLE) {
        char name[32];
        snprintf(name, 32, "temp%d.dbf", fil_node_idx + 1);
        if (!os_open_file(name, OS_FILE_OPEN, 0, &fil_temp_space.nodes[fil_node_idx].file)) {
            temp_space_free_fil_page(*swid);
            return FALSE;
        }
    }

    uint64 offset = (uint64)page_no * TEMP_SPACE_PAGE_SIZE;
    return os_pwrite_file(fil_temp_space.nodes[fil_node_idx].file, offset, PageData(page), TEMP_SPACE_PAGE_SIZE);
}

bool32 temp_space_swap_clean(uint64 swid)
{
    uint32 fil_node_idx = swid >> 32;
    uint32 page_no = swid & 0xFFFFFFFF;

    if (fil_node_idx >= fil_temp_space.node_count) {
        return FALSE;
    }

    temp_space_free_fil_page(swid);

    return TRUE;
}

void temp_space_init(memory_pool_t *pool)
{
    fil_temp_space.node_count = 0;
    fil_temp_space.pool = pool;
}

bool32 open_or_create_temp_space_file(char *file_name, bool32 is_create, uint64 size, uint64 max_size, uint64 extend_size)
{
    uint32 idx;
    char name[32];

    size = (size / 1024 / 1024) * 1024 * 1024;
    max_size = (max_size / 1024 / 1024) * 1024 * 1024;
    if (size == 0 || size > max_size) {
        return FALSE;
    }

    idx = fil_temp_space.node_count;
    fil_temp_space.node_count++;

    fil_temp_space.nodes[idx].id = idx;
    fil_temp_space.nodes[idx].lock = 0;
    fil_temp_space.nodes[idx].page_hwm = (uint32)(size / TEMP_SPACE_PAGE_SIZE);
    fil_temp_space.nodes[idx].page_max_count = (uint32)(max_size / TEMP_SPACE_PAGE_SIZE);
    fil_temp_space.nodes[idx].is_extend = (size != max_size);
    fil_temp_space.nodes[idx].extend_size = extend_size;
    UT_LIST_INIT(fil_temp_space.nodes[idx].free_xdescs);
    UT_LIST_INIT(fil_temp_space.nodes[idx].xdesc_pages);

    if (is_create) {
        snprintf(name, 32, "temp%d.dbf", idx + 1);
        if (!os_open_file(name, OS_FILE_CREATE, OS_FILE_NORMAL, &fil_temp_space.nodes[idx].file)) {
            return FALSE;
        }
        if (!os_file_extend(name, fil_temp_space.nodes[idx].file, size)) {
            return FALSE;
        }
    } else {
        snprintf(name, 32, "temp%d.dbf", idx + 1);
        if (!os_open_file(name, OS_FILE_OPEN, OS_FILE_NORMAL, &fil_temp_space.nodes[idx].file)) {
            return FALSE;
        }
    }

    return TRUE;
}

