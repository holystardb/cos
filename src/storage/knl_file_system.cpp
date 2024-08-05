#include "knl_file_system.h"
#include "cm_util.h"
#include "cm_log.h"
#include "knl_buf.h"
#include "knl_mtr.h"
#include "knl_flst.h"
#include "knl_page.h"
#include "knl_dict.h"

/* The file system. This variable is NULL before the module is initialized. */
fil_system_t     *fil_system = NULL;

/** The null file address */
fil_addr_t        fil_addr_null = {FIL_NULL, 0};


static bool32 fil_node_prepare_for_io(fil_node_t *node);
static void fil_node_complete_io(fil_node_t* node, uint32 type);





bool32 fil_system_init(memory_pool_t *pool, uint32 max_n_open,
                             uint32 space_max_count, uint32 fil_node_max_count)
{
    if (fil_node_max_count > 0xFFFFFF || space_max_count > 0xFFFFFF) {
        return FALSE;
    }
    fil_node_max_count = fil_node_max_count < 0xFFF ? 0xFFF : fil_node_max_count;
    space_max_count = space_max_count < 0xFFF ? 0xFFF : space_max_count;

    fil_system = (fil_system_t *)malloc(ut_align8(sizeof(fil_system_t)) +
                                        fil_node_max_count * ut_align8(sizeof(fil_node_t *)));
    if (fil_system == NULL) {
        return FALSE;
    }

    fil_system->spaces =  NULL;
    fil_system->name_hash = NULL;
    fil_system->mem_context = NULL;
    fil_system->open_pending_num = 0;
    fil_system->max_n_open = max_n_open;
    fil_system->space_max_count = space_max_count;
    fil_system->fil_node_num = 0;
    fil_system->fil_node_max_count = fil_node_max_count;
    fil_system->fil_nodes = (fil_node_t **)((char *)fil_system + ut_align8(sizeof(fil_system_t)));
    memset(fil_system->fil_nodes, 0x00, fil_node_max_count * ut_align8(sizeof(fil_node_t *)));
    mutex_create(&fil_system->mutex);
    mutex_create(&fil_system->lru_mutex);
    UT_LIST_INIT(fil_system->fil_spaces);
    UT_LIST_INIT(fil_system->fil_node_lru);

    fil_system->mem_area = pool->area;
    fil_system->mem_context = mcontext_create(pool);
    if (fil_system->mem_context == NULL) {
        LOGGER_ERROR(LOGGER, "fil_system_init: failed to create memory context");
        goto err_exit;
    }
    fil_system->spaces =  HASH_TABLE_CREATE(space_max_count);
    if (fil_system->spaces == NULL) {
        LOGGER_ERROR(LOGGER, "fil_system_init: failed to create spaces hashtable");
        goto err_exit;
    }
    fil_system->name_hash = HASH_TABLE_CREATE(space_max_count);
    if (fil_system->name_hash == NULL) {
        LOGGER_ERROR(LOGGER, "fil_system_init: failed to create name hashtable");
        goto err_exit;
    }

    fil_system->aio_pending_count_per_context = OS_AIO_N_PENDING_IOS_PER_THREAD;
    fil_system->aio_context_count = 16;
    fil_system->aio_array = os_aio_array_create(fil_system->aio_pending_count_per_context,
                                                fil_system->aio_context_count);
    if (fil_system->aio_array == NULL) {
        LOGGER_ERROR(LOGGER, "fil_system_init: failed to create aio array");
        goto err_exit;
    }

    return TRUE;

err_exit:

    if (fil_system->mem_context) {
        mcontext_destroy(fil_system->mem_context);
    }
    if (fil_system->spaces) {
        HASH_TABLE_FREE(fil_system->spaces);
    }
    if (fil_system->name_hash) {
        HASH_TABLE_FREE(fil_system->name_hash);
    }

    return FALSE;
}

fil_space_t* fil_space_create(char *name, uint32 space_id, uint32 purpose)
{
    fil_space_t *space, *tmp;
    uint32 size = ut_align8(sizeof(fil_space_t)) + (uint32)strlen(name) + 1;
    space = (fil_space_t *)my_malloc(fil_system->mem_context, size);
    if (space == NULL) {
        return NULL;
    }

    space->name = (char *)space + ut_align8(sizeof(fil_space_t));
    strncpy_s(space->name, strlen(name) + 1, name, strlen(name));
    space->name[strlen(name) + 1] = 0;

    space->id = space_id;
    space->size_in_header = 0;
    space->free_limit = 0;
    space->flags = 0;
    space->purpose = purpose;
    space->page_size = 0;
    space->n_reserved_extents = 0;
    space->magic_n = M_FIL_SPACE_MAGIC_N;
    space->refcount = 0;
    mutex_create(&space->mutex);
    rw_lock_create(&space->latch);
    space->io_in_progress = 0;
    UT_LIST_INIT(space->fil_nodes);
    UT_LIST_INIT(space->free_pages);

    spin_lock(&fil_system->mutex, NULL);
    tmp = UT_LIST_GET_FIRST(fil_system->fil_spaces);
    while (tmp) {
        if (strcmp(tmp->name, name) == 0 || tmp->id == space_id) {
            break;
        }
        tmp = UT_LIST_GET_NEXT(list_node, tmp);
    }
    if (tmp == NULL) {
        UT_LIST_ADD_LAST(list_node, fil_system->fil_spaces, space);
    }
    HASH_INSERT(fil_space_t, hash, fil_system->spaces, space_id, space);
    //HASH_INSERT(fil_space_t, name_hash, fil_system->name_hash, ut_fold_string(name), space);

    spin_unlock(&fil_system->mutex);

    if (tmp) { // name or spaceid already exists
        my_free((void *)space);
        space = NULL;
    }

    return space;
}

void fil_space_destroy(uint32 space_id)
{
    fil_space_t *space;
    fil_node_t *fil_node;
    UT_LIST_BASE_NODE_T(fil_node_t) fil_node_list;

    mutex_enter(&fil_system->mutex, NULL);
    space = fil_get_space_by_id(space_id);
    ut_a(space->magic_n == M_FIL_SPACE_MAGIC_N);
    mutex_exit(&fil_system->mutex);

    mutex_enter(&space->mutex, NULL);
    fil_node_list.count = space->fil_nodes.count;
    fil_node_list.start = space->fil_nodes.start;
    fil_node_list.end = space->fil_nodes.end;
    UT_LIST_INIT(space->fil_nodes);
    mutex_exit(&space->mutex);

    spin_lock(&fil_system->mutex, NULL);
    fil_node = UT_LIST_GET_FIRST(fil_node_list);
    while (fil_node != NULL) {
        if (fil_node->is_open) { /* The node is in the LRU list, remove it */
            UT_LIST_REMOVE(lru_list_node, fil_system->fil_node_lru, fil_node);
            fil_node->is_open = FALSE;
        }
        fil_system->fil_nodes[fil_node->id] = NULL;
        fil_node = UT_LIST_GET_NEXT(chain_list_node, fil_node);
    }
    spin_unlock(&fil_system->mutex);

    fil_node = UT_LIST_GET_FIRST(fil_node_list);
    while (fil_node != NULL) {
        os_close_file(fil_node->handle);

        UT_LIST_REMOVE(chain_list_node, fil_node_list, fil_node);
        my_free((void *)fil_node);

        fil_node = UT_LIST_GET_FIRST(fil_node_list);
    }

    my_free((void *)space);
}

fil_node_t* fil_node_create(fil_space_t *space, char *name, uint32 page_max_count, uint32 page_size, bool32 is_extend)
{
    fil_node_t *node, *tmp;

    spin_lock(&fil_system->mutex, NULL);
    if (fil_system->fil_node_num >= fil_system->fil_node_max_count) {
        spin_unlock(&fil_system->mutex);
        return NULL;
    }
    fil_system->fil_node_num++;
    spin_unlock(&fil_system->mutex);

    node = (fil_node_t *)my_malloc(fil_system->mem_context, ut_align8(sizeof(fil_node_t)) + (uint32)strlen(name) + 1);
    if (node == NULL) {
        return NULL;
    }

    node->space = space;
    node->name = (char *)node + ut_align8(sizeof(fil_node_t));
    strcpy_s(node->name, strlen(name) + 1, name);
    node->name[strlen(name) + 1] = 0;

    node->page_max_count = page_max_count;
    node->page_size = page_size;
    node->handle = OS_FILE_INVALID_HANDLE;
    node->magic_n = M_FIL_NODE_MAGIC_N;
    mutex_create(&node->mutex);
    node->is_open = 0;
    node->is_io_progress = 0;
    node->is_extend = is_extend;
    node->n_pending = 0;

    mutex_enter(&fil_system->mutex, NULL);
    for (uint32 i = 0; i < fil_system->fil_node_max_count; i++) {
        if (fil_system->fil_nodes[i] == NULL) {
            node->id = i;
            fil_system->fil_nodes[i] = node;
            break;
        }
    }
    mutex_exit(&fil_system->mutex);

    mutex_enter(&space->mutex, NULL);
    tmp = UT_LIST_GET_FIRST(space->fil_nodes);
    while (tmp) {
        if (strcmp(tmp->name, name) == 0) {
            break;
        }
        tmp = UT_LIST_GET_NEXT(chain_list_node, tmp);
    }
    if (tmp == NULL) { // name not already exists
        UT_LIST_ADD_LAST(chain_list_node, space->fil_nodes, node);
    }
    mutex_exit(&space->mutex);

    if (tmp) {
        spin_lock(&fil_system->mutex, NULL);
        fil_system->fil_nodes[node->id] = NULL;
        fil_system->fil_node_num--;
        spin_unlock(&fil_system->mutex);
        my_free((void *)node);
        node = NULL;
    };

    return node;
}

bool32 fil_node_destroy(fil_space_t *space, fil_node_t *node)
{
    fil_node_close(space, node);

    fil_page_t *page, *tmp;
    mutex_enter(&space->mutex, NULL);
    UT_LIST_ADD_LAST(chain_list_node, space->fil_nodes, node);
    //
    page = UT_LIST_GET_FIRST(space->free_pages);
    while (page) {
        tmp = UT_LIST_GET_NEXT(list_node, page);
        if (page->file == node->id) {
            UT_LIST_REMOVE(list_node, space->free_pages, page);
        }
        page = tmp;
    }
    mutex_exit(&space->mutex);

    spin_lock(&fil_system->mutex, NULL);
    fil_system->fil_nodes[node->id] = NULL;
    fil_system->fil_node_num--;
    spin_unlock(&fil_system->mutex);

    os_del_file(node->name);
    mutex_destroy(&node->mutex);
    my_free((void *)node);

    return TRUE;
}

bool32 fil_node_open(fil_space_t *space, fil_node_t *node)
{
    os_file_t handle;
    fil_node_t *last_node;

    if (node->is_open) {
        return TRUE;
    }

    /* File is closed */
    /* If too many files are open, close one */
    spin_lock(&fil_system->mutex, NULL);
    while (fil_system->open_pending_num + UT_LIST_GET_LEN(fil_system->fil_node_lru) + 1 >= fil_system->max_n_open) {
        last_node = UT_LIST_GET_LAST(fil_system->fil_node_lru);
        if (last_node == NULL) {
            fprintf(stderr,
                    "Error: cannot close any file to open another for i/o\n"
                    "Pending i/o's on %lu files exist\n",
                    fil_system->open_pending_num);
            ut_a(0);
        }
        handle = last_node->handle;
        last_node->handle = OS_FILE_INVALID_HANDLE;
        UT_LIST_REMOVE(lru_list_node, fil_system->fil_node_lru, node);
        node->is_open = FALSE;
        spin_unlock(&fil_system->mutex);

        if (!os_close_file(handle)) {
            fprintf(stderr,
                    "Error: cannot close any file to open another for i/o\n"
                    "Pending i/o's on %lu files exist\n",
                    fil_system->open_pending_num);
            ut_a(0);
        }

        spin_lock(&fil_system->mutex, NULL);
    }
    spin_unlock(&fil_system->mutex);

    if (!os_open_file(node->name, 0, 0, &node->handle)) {
        return FALSE;
    }

    spin_lock(&fil_system->mutex, NULL);
    node->is_open = TRUE;
    UT_LIST_ADD_LAST(lru_list_node, fil_system->fil_node_lru, node);
    spin_unlock(&fil_system->mutex);

    return TRUE;
}

bool32 fil_node_close(fil_space_t *space, fil_node_t *node)
{
    bool32 ret;

    ut_a(node->is_open);
    ut_a(node->n_pending == 0);

    ret = os_close_file(node->handle);
    ut_a(ret);

    /* The node is in the LRU list, remove it */
    spin_lock(&fil_system->mutex, NULL);
    if (node->is_open) {
        UT_LIST_REMOVE(lru_list_node, fil_system->fil_node_lru, node);
        node->is_open = FALSE;
    }
    spin_unlock(&fil_system->mutex);

    return TRUE;
}


//bool32 fil_space_extend_datafile(fil_space_t *space, bool32 need_redo)
//{
//    fil_node_t *node;
//    uint32 total_count = 100;
//    uint32 count = 0;
//
//    if (total_count == 0) {
//        return TRUE;
//    }
//
//    spin_lock(&space->lock, NULL);
//    node = UT_LIST_GET_FIRST(space->fil_nodes);
//    while (node) {
//        if (node->page_hwm < node->page_max_count) {
//            if (node->page_max_count - node->page_hwm >= total_count) {
//                node->page_hwm += total_count;
//                count = total_count;
//            } else {
//                node->page_hwm = node->page_max_count;
//                total_count -= node->page_max_count - node->page_hwm;
//                count = node->page_max_count - node->page_hwm;
//            }
//            break;
//        }
//        node = UT_LIST_GET_NEXT(chain_list_node, node);
//    }
//    spin_unlock(&space->lock);
//
//    if (count > 0 && node) {
//        
//    }
//
//    return TRUE;
//}


static bool32 fil_space_extend_node(fil_space_t *space,
    fil_node_t *node, uint32 page_hwm, uint32 size_increase, uint32 *actual_size)
{
    os_aio_context_t *aio_ctx = NULL;
    uint32            timeout_seconds = 300;
    memory_page_t    *page = NULL;
    uchar            *buf;
    uint32            size;
    uint64            offset;
    bool32            ret = FALSE;

    if (size_increase == 0) {
        *actual_size = 0;
        return TRUE ;
    }

    size = page_hwm + size_increase;
    ut_a(node->page_max_count >= size);

    if (!fil_node_prepare_for_io(node)) {
        return FALSE;
    }

    aio_ctx = os_aio_array_alloc_context(fil_system->aio_array);
    page = marea_alloc_page(fil_system->mem_area, UNIV_PAGE_SIZE);
    buf = (uchar *)MEM_PAGE_DATA_PTR(page);
    memset(buf, 0x00, UNIV_PAGE_SIZE);

    for (uint32 loop = 0; loop < size_increase; loop += OS_AIO_N_PENDING_IOS_PER_THREAD) {
        uint32 io_count = 0;
        for (uint32 i = page_hwm + loop; i < page_hwm + loop + OS_AIO_N_PENDING_IOS_PER_THREAD; i++) {
            offset = i * UNIV_PAGE_SIZE;
            mach_write_to_4(buf + FIL_PAGE_SPACE, space->id);
            mach_write_to_4(buf + FIL_PAGE_OFFSET, i);
            if (!os_file_aio_submit(aio_ctx, OS_FILE_WRITE, node->name, node->handle,
                                    (void *)buf, UNIV_PAGE_SIZE, offset, NULL, NULL)) {
                char errinfo[1024];
                os_file_get_last_error_desc(errinfo, 1024);
                LOGGER_FATAL(LOGGER,
                    "fil_space_extend: fail to write file, name %s error %s",
                    node->name, errinfo);
                goto err_exit;
            }
            io_count++;
        }

        int ret = os_file_aio_wait_all(aio_ctx, io_count, timeout_seconds * 1000000);
        switch (ret) {
        case OS_FILE_IO_COMPLETION:
            break;
        case OS_FILE_IO_TIMEOUT:
            LOGGER_FATAL(LOGGER,
                "fil_space_extend: IO timeout for writing file, name %s timeout %u seconds",
                node->name, timeout_seconds);
            goto err_exit;

        default:
            char err_info[1024];
            os_file_get_error_desc_by_err(ret, err_info, 1024);
            LOGGER_FATAL(LOGGER,
                "fil_space_extend: fail to write file, name %s error %s",
                node->name, err_info);
            goto err_exit;
        }
    }

    if (!os_fsync_file(node->handle)) {
        char errinfo[1024];
        os_file_get_last_error_desc(errinfo, 1024);
        LOGGER_FATAL(LOGGER,
            "fil_space_extend: fail to sync file, name %s error %s",
            node->name, errinfo);
        goto err_exit;
    }

    *actual_size = size_increase;
    ret = TRUE;

err_exit:

    if (aio_ctx) {
        os_aio_array_free_context(aio_ctx);
    }

    if (page) {
        marea_free_page(fil_system->mem_area, page, UNIV_PAGE_SIZE);
    }

    fil_node_complete_io(node, OS_FILE_WRITE);


    return ret;
}

bool32 fil_space_extend(fil_space_t* space, uint32 size_after_extend, uint32 *actual_size)
{
    fil_node_t       *node;
    uint32            size_increase;

    ut_ad(!srv_read_only_mode || fsp_is_system_temporary(space->id));

    if (space->size_in_header >= size_after_extend) {
        /* Space already big enough */
        *actual_size = space->size_in_header;
        return(TRUE);
    }

    size_increase = size_after_extend - space->size_in_header;
    *actual_size = 0;

    // find a node by space->size_in_header
    uint32 page_hwm = space->size_in_header;
    node = UT_LIST_GET_FIRST(space->fil_nodes);
    for (; node != NULL && page_hwm >= node->page_max_count;) {
        page_hwm -= node->page_max_count;
        node = UT_LIST_GET_NEXT(chain_list_node, node);
    }
    // extend nodes
    for (; node != NULL && size_increase > 0;) {
        uint32 free_cur_node = node->page_max_count - page_hwm;
        uint32 node_size_increase, node_actual_size;
        if (size_increase <= free_cur_node) {
            node_size_increase = size_increase;
        } else {
            node_size_increase = free_cur_node;
        }
        size_increase -= node_size_increase;

        bool32 ret = fil_space_extend_node(space,node,
            page_hwm, node_size_increase, &node_actual_size);
        if (!ret) {
            goto err_exit;
        }
        page_hwm = 0;
        *actual_size += node_actual_size;

        node = UT_LIST_GET_NEXT(chain_list_node, node);
    }

    return TRUE;

err_exit:

    return FALSE;
}

uint32 fil_space_get_size(uint32 space_id)
{
    fil_space_t *space;
    uint32       size;

    ut_ad(fil_system);
    mutex_enter(&fil_system->mutex);
    space = fil_get_space_by_id(space_id);
    size = space ? space->size_in_header : 0;
    mutex_exit(&fil_system->mutex);

    return(size);
}


static bool32 fsp_try_extend_data_file_with_pages(
	fil_space_t*  space,
	uint32        page_no,
	fsp_header_t* header,
	mtr_t*        mtr)
{
	bool32  success;
	uint32  size, actual_size = 0;

	ut_a(!is_system_tablespace(space->id));
	//ut_d(fsp_space_modify_check(space->id, mtr));

	size = mach_read_from_4(header + FSP_SIZE);
	ut_ad(size == space->size_in_header);

	ut_a(page_no >= size);

	success = fil_space_extend(space, page_no + 1, &actual_size);
	/* The size may be less than we wanted if we ran out of disk space. */
	mlog_write_uint32(header + FSP_SIZE, actual_size, MLOG_4BYTES, mtr);
	space->size_in_header = actual_size;

	return(success);
}


static uint32 fsp_get_autoextend_increment(fil_space_t* space)
{
    if (space->id >= FIL_USER_SPACE_ID) {
        
    } else if (space->id == FIL_SYSTEM_SPACE_ID) {
        return FSP_EXTENT_SIZE * FSP_FREE_ADD;
    } else {
    }

    return FSP_EXTENT_SIZE * FSP_FREE_ADD;
}

// Tries to extend the last data file of a tablespace if it is auto-extending.
static bool32 fsp_try_extend_data_file(
    uint32       *actual_increase,/*!< out: actual increase in pages */
    fil_space_t  *space,  /*!< in: space */
    fsp_header_t *header,  /*!< in/out: space header */
    mtr_t        *mtr)
{
    uint32    size;  /* current number of pages in the datafile */
    uint32    size_increase;  /* number of pages to extend this file */
    uint32    actual_size;

    //ut_d(fsp_space_modify_check(space->id, mtr));

    size = mach_read_from_4(header + FSP_SIZE);
    ut_ad(size == space->size_in_header);

    const page_size_t page_size(mach_read_from_4(header + FSP_SPACE_FLAGS));

    size_increase = fsp_get_autoextend_increment(space);
    if (size_increase == 0) {
        return FALSE;
    }

    if (!fil_space_extend(space, size + size_increase, &actual_size)) {
        return FALSE;
    }

    space->size_in_header += actual_size;
    mlog_write_uint32(header + FSP_SIZE, space->size_in_header, MLOG_4BYTES, mtr);

    return TRUE;
}

fil_space_t* fil_get_space_by_id(uint32 space_id)
{
    fil_space_t *space;

    ut_ad(mutex_own(&fil_system->mutex));

    HASH_SEARCH(hash, fil_system->spaces, space_id,
        fil_space_t*, space,
        ut_ad(space->magic_n == M_FIL_SPACE_MAGIC_N),
        space->id == space_id);
    if (space) {
        atomic32_inc(&space->refcount);
    }

    return space;
}

void fil_release_space(fil_space_t* space)
{
    atomic32_dec(&space->refcount);
}

rw_lock_t* fil_space_get_latch(uint32 space_id, uint32 *flags)
{
    fil_space_t *space;

    ut_ad(fil_system);

    mutex_enter(&fil_system->mutex);

    space = fil_get_space_by_id(space_id);

    ut_a(space);

    if (flags) {
        *flags = space->flags;
    }

    mutex_exit(&fil_system->mutex);

    return(&(space->latch));
}





bool fil_addr_is_null(fil_addr_t addr) /*!< in: address */
{
    return(addr.page == FIL_NULL);
}

static bool32 fil_space_belongs_in_lru(fil_space_t *space)
{
    return space->id >= FIL_USER_SPACE_ID;
}

// Closes a file
static void fil_node_close_file(fil_node_t *node)
{
    bool32  ret;

    ut_a(node->is_open);
    ut_a(node->n_pending == 0);
    ut_a(node->is_extend);

    ret = os_close_file(node->handle);
    ut_a(ret);

    if (fil_space_belongs_in_lru(node->space)) {
        mutex_enter(&(fil_system->lru_mutex), NULL);
        ut_a(UT_LIST_GET_LEN(fil_system->fil_node_lru) > 0);
        /* The node is in the LRU list, remove it */
        UT_LIST_REMOVE(lru_list_node, fil_system->fil_node_lru, node);
        mutex_exit(&(fil_system->lru_mutex));
    }

    mutex_enter(&node->mutex);
    node->is_open = FALSE;
    node->is_io_progress = FALSE;
    mutex_exit(&node->mutex);

    ut_ad(fil_system->open_pending_num > 0);
    atomic32_dec(&fil_system->open_pending_num);
}

// Opens a file of a node of a tablespace.
// The caller must own the fil_system mutex.
static bool32 fil_node_open_file(fil_node_t *node)
{
    bool32      ret;

    ut_a(node->n_pending == 0);
    ut_a(node->is_open == FALSE);

    /* Open the file for reading and writing */
    ret = os_open_file(node->name, OS_FILE_OPEN, OS_FILE_AIO, &node->handle);
    ut_a(ret);

    mutex_enter(&node->mutex);
    node->is_open = TRUE;
    node->is_io_progress = FALSE;
    mutex_exit(&node->mutex);

    atomic32_inc(&fil_system->open_pending_num);

    if (fil_space_belongs_in_lru(node->space)) {
        /* Put the node to the LRU list */
        mutex_enter(&fil_system->lru_mutex);
        UT_LIST_ADD_FIRST(lru_list_node, fil_system->fil_node_lru, node);
        mutex_exit(&fil_system->lru_mutex);
    }

    return TRUE;
}

static bool32 fil_try_to_close_file_in_LRU()
{
    fil_node_t  *node;

    mutex_enter(&(fil_system->lru_mutex), NULL);

    for (node = UT_LIST_GET_LAST(fil_system->fil_node_lru);
         node != NULL;
         node = UT_LIST_GET_PREV(lru_list_node, node)) {

        mutex_enter(&node->mutex);
        if (node->n_pending > 0 || node->is_open || node->is_io_progress) {
            mutex_exit(&node->mutex);
            continue;
        }
        node->is_io_progress = TRUE;
        mutex_exit(&node->mutex);

        break;
    }

    mutex_exit(&(fil_system->lru_mutex));

    if (node) {
        LOGGER_INFO(LOGGER, "close file %s", node->name);
        fil_node_close_file(node);

        return TRUE;
    }

    return FALSE;
}

// Prepares a file node for i/o.
// Opens the file if it is closed.
// Updates the pending i/o's field in the node and the system appropriately.
// Takes the node off the LRU list if it is in the LRU list.
// The caller must hold the fil_sys mutex.
static bool32 fil_node_prepare_for_io(fil_node_t *node)
{

open_retry:

    mutex_enter(&node->mutex);
    if (node->is_open) {
        node->n_pending++;
        //node->time = current_monotonic_time();
        mutex_exit(&node->mutex);
        return TRUE;
    }
    if (node->is_io_progress) {
        mutex_exit(&node->mutex);
        os_thread_sleep(100); // 100us
        goto open_retry;
    }
    node->is_io_progress = TRUE;
    mutex_exit(&node->mutex);

    // We keep log files and system tablespace files always open

    if (fil_system->open_pending_num > fil_system->max_n_open) {
        LOGGER_WARN(LOGGER, "Warning: open files %u exceeds the limit %u",
            fil_system->open_pending_num, fil_system->max_n_open);

close_retry:

        /* Too many files are open, try to close some */
        bool32 success = fil_try_to_close_file_in_LRU();
        if (success && atomic32_get(&fil_system->open_pending_num) >= fil_system->max_n_open) {
            os_thread_sleep(100); // 100us
            goto close_retry;
        }
    }

    /* File is closed: open it */

    ut_a(node->is_open == FALSE);
    ut_a(node->n_pending == 0);

    bool32 ret = fil_node_open_file(node);
    mutex_enter(&node->mutex);
    if (ret) {
        node->n_pending++;
    }
    node->is_io_progress = FALSE;
    mutex_exit(&node->mutex);

    return ret;
}

// Updates the data structures when an i/o operation finishes.
// Updates the pending i/o's field in the node appropriately.
static void fil_node_complete_io(
    fil_node_t* node, /*!< in: file node */
    uint32 type) /*!< in: OS_FILE_WRITE or OS_FILE_READ; marks the node as modified if type == OS_FILE_WRITE */
{
    mutex_enter(&node->mutex);
    ut_a(node->n_pending > 0);
    node->n_pending--;
    mutex_exit(&node->mutex);
}

dberr_t fil_io(
    uint32 type,
    bool32 sync, /*!< in: true if synchronous aio is desired */
    const page_id_t &page_id,
    const page_size_t &page_size,
    uint32 len, /*!< in: this must be a block size multiple */
    void*  buf, /*!< in/out: buffer where to store data read */
    void*  message) /*!< in: message for aio handler if non-sync aio used, else ignored */
{
    fil_space_t  *space;
    fil_node_t   *node;
    uint32        block_offset;

    mutex_enter(&fil_system->mutex, NULL);
    space = fil_get_space_by_id(page_id.space());
    fil_system_pin_space(space);
    mutex_exit(&fil_system->mutex);

    mutex_enter(&space->mutex);
    block_offset = page_id.page_no();
    node = UT_LIST_GET_FIRST(space->fil_nodes);
    for (;;) {
        if (node == NULL) {
            ut_error;
        } else if (node->page_max_count > block_offset) {
            /* Found! */
            break;
        } else {
            block_offset -= node->page_max_count;
            node = UT_LIST_GET_NEXT(chain_list_node, node);
        }
    }
    mutex_exit(&space->mutex);

    /* Open file if closed */
    if (!fil_node_prepare_for_io(node)) {
        LOGGER_ERROR(LOGGER,
            "Trying to do read i/o to a tablespace which exists without %s data file, "
            "space id %lu, page no %lu",
            node->name, page_id.space(), page_id.page_no());
        return(DB_TABLESPACE_DELETED);
    }

    os_aio_context_t *aio_ctx;
    if (sync) {
        uint32 ctx_index = page_id.page_no() % srv_sync_io_contexts;
        aio_ctx = os_aio_array_get_nth_context(srv_os_aio_sync_array, ctx_index);
    } else {
        uint32 ctx_index = page_id.page_no() % srv_read_io_threads;
        if (type == OS_FILE_READ) {
            aio_ctx = os_aio_array_get_nth_context(srv_os_aio_async_read_array, ctx_index);
        } else {
            aio_ctx = os_aio_array_get_nth_context(srv_os_aio_async_write_array, ctx_index);
        }
    }

    uint64 offset = block_offset << UNIV_PAGE_SIZE_SHIFT_DEF;
    if (!os_file_aio_submit(aio_ctx, type, node->name, node->handle,
                            (void *)buf, len, offset, NULL, NULL)) {
        char err_info[1024];
        os_file_get_last_error_desc(err_info, 1024);
        LOGGER_ERROR(LOGGER,
            "Error: Trying to do read i/o from %s data file, space id %lu, page no %lu, error %s",
            node->name, page_id.space(), page_id.page_no(), err_info);
        goto err_exit;
    }

    if (sync) {
        int ret = os_file_aio_wait(aio_ctx, srv_read_io_timeout_seconds * 1000000);
        switch (ret) {
        case OS_FILE_IO_COMPLETION:
            break;
        case OS_FILE_IO_TIMEOUT:
            LOGGER_ERROR(LOGGER,
                "Error: timeout(%lu seconds) for do read i/o from %s data file, space id %lu, page no %lu",
                srv_read_io_timeout_seconds, node->name, page_id.space(), page_id.page_no());
            goto err_exit;
        default:
            char err_info[1024];
            os_file_get_last_error_desc(err_info, 1024);
            LOGGER_ERROR(LOGGER,
                "Error: Trying to do read i/o from %s data file, space id %lu, page no %lu, error %s",
                node->name, page_id.space(), page_id.page_no(), err_info);
            goto err_exit;
        }

        mutex_enter(&fil_system->mutex, NULL);
        fil_system_unpin_space(space);
        mutex_exit(&fil_system->mutex);

    }

    return DB_SUCCESS;

err_exit:

    return DB_ERROR;
}


dberr_t fil_read(
    bool32 sync, /*!< in: true if synchronous aio is desired */
    const page_id_t &page_id,
    const page_size_t &page_size,
    uint32 len, /*!< in: this must be a block size multiple */
    void*  buf, /*!< in/out: buffer where to store data read */
    void*  message) /*!< in: message for aio handler if non-sync aio used, else ignored */
{
    return fil_io(OS_FILE_READ, sync, page_id, page_size, len, buf, message);
}

dberr_t fil_write(
    bool32 sync, /*!< in: true if synchronous aio is desired */
    const page_id_t &page_id,
    const page_size_t &page_size,
    uint32 len, /*!< in: this must be a block size multiple */
    void*  buf, /*!< in/out: buffer where to store data read */
    void*  message) /*!< in: message for aio handler if non-sync aio used, else ignored */
{
    return fil_io(OS_FILE_WRITE, sync, page_id, page_size, len, buf, message);
}


// Tries to reserve free extents in a file space.
//return TRUE if succeed
bool32 fil_space_reserve_free_extents(
    uint32  id,           /*!< in: space id */
    uint32  n_free_now,   /*!< in: number of free extents now */
    uint32  n_to_reserve) /*!< in: how many one wants to reserve */
{
    fil_space_t *space;
    bool32       success;

    ut_ad(fil_system);

    mutex_enter(&fil_system->mutex);

    space = fil_get_space_by_id(id);
    ut_a(space);

    if (space->n_reserved_extents + n_to_reserve > n_free_now) {
        success = FALSE;
    } else {
        space->n_reserved_extents += n_to_reserve;
        success = TRUE;
    }

    mutex_exit(&fil_system->mutex);

    return(success);
}

// Releases free extents in a file space.
void fil_space_release_free_extents(
    uint32 id,          /*!< in: space id */
    uint32 n_reserved)  /*!< in: how many one reserved */
{
    fil_space_t*	space;

    ut_ad(fil_system);

    mutex_enter(&fil_system->mutex);

    space = fil_get_space_by_id(id);

    ut_a(space);
    ut_a(space->n_reserved_extents >= n_reserved);

    space->n_reserved_extents -= n_reserved;

    mutex_exit(&fil_system->mutex);
}

