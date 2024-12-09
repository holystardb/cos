#include "knl_file_system.h"
#include "cm_util.h"
#include "cm_file.h"
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


static inline bool32 fil_node_prepare_for_io(fil_node_t *node);


bool32 fil_system_init(memory_pool_t *mem_pool, uint32 max_n_open)
{
    fil_system = (fil_system_t *)ut_malloc_zero(ut_align8(sizeof(fil_system_t)) +
        DB_SPACE_DATA_FILE_MAX_COUNT * sizeof(fil_node_t *));
    if (fil_system == NULL) {
        return FALSE;
    }

    fil_system->space_id_hash =  NULL;
    fil_system->name_hash = NULL;
    fil_system->mem_context = NULL;
    fil_system->open_pending_num = 0;
    fil_system->max_n_open = max_n_open;
    fil_system->space_max_count = USER_SPACE_MAX_COUNT;
    fil_system->fil_node_num = 0;
    fil_system->fil_node_max_count = DB_SPACE_DATA_FILE_MAX_COUNT;
    fil_system->fil_nodes = (fil_node_t **)((char *)fil_system + ut_align8(sizeof(fil_system_t)));

    mutex_create(&fil_system->mutex);
    mutex_create(&fil_system->lru_mutex);

    UT_LIST_INIT(fil_system->fil_spaces);
    UT_LIST_INIT(fil_system->fil_node_lru);
    UT_LIST_INIT(fil_system->fil_node_unflushed);

    fil_system->mem_area = mem_pool->area;
    fil_system->mem_pool = mem_pool;
    fil_system->mem_context = mcontext_create(mem_pool);
    if (fil_system->mem_context == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_TABLESPACE, "fil_system_init: failed to create memory context");
        goto err_exit;
    }

    for (uint32 i = 0; i < M_FIL_SYSTEM_HASH_LOCKS; i++) {
        rw_lock_create(&fil_system->rw_lock[i]);
    }

    fil_system->space_id_hash =  HASH_TABLE_CREATE(USER_SPACE_MAX_COUNT, HASH_TABLE_SYNC_RW_LOCK, 4096);
    if (fil_system->space_id_hash == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_TABLESPACE, "fil_system_init: failed to create spaces hashtable");
        goto err_exit;
    }
    fil_system->name_hash = HASH_TABLE_CREATE(USER_SPACE_MAX_COUNT, HASH_TABLE_SYNC_RW_LOCK, 4096);
    if (fil_system->name_hash == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_TABLESPACE, "fil_system_init: failed to create name hashtable");
        goto err_exit;
    }

    fil_system->aio_pending_count_per_context = OS_AIO_N_PENDING_IOS_PER_THREAD;
    fil_system->aio_context_count = 16;
    fil_system->aio_array = os_aio_array_create(fil_system->aio_pending_count_per_context,
                                                fil_system->aio_context_count);
    if (fil_system->aio_array == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_TABLESPACE, "fil_system_init: failed to create aio array");
        goto err_exit;
    }

    return TRUE;

err_exit:

    if (fil_system->mem_context) {
        mcontext_destroy(fil_system->mem_context);
    }
    if (fil_system->space_id_hash) {
        HASH_TABLE_FREE(fil_system->space_id_hash);
    }
    if (fil_system->name_hash) {
        HASH_TABLE_FREE(fil_system->name_hash);
    }

    return FALSE;
}

inline rw_lock_t* fil_system_get_hash_lock(uint32 space_id)
{
    uint32 lock_id = space_id % M_FIL_SYSTEM_HASH_LOCKS;
    return &fil_system->rw_lock[lock_id];
}

inline fil_space_t* fil_system_get_space_by_id(uint32 space_id)
{
    fil_space_t *space;
    rw_lock_t *hash_lock;

    hash_lock = fil_system_get_hash_lock(space_id);
    rw_lock_s_lock(hash_lock);

    HASH_SEARCH(hash, fil_system->space_id_hash, space_id,
        fil_space_t*, space,
        ut_ad(space->magic_n == M_FIL_SPACE_MAGIC_N),
        space->id == space_id);

    if (space) {
        fil_system_pin_space(space);
    }

    rw_lock_s_unlock(hash_lock);

    return space;
}

inline void fil_system_insert_space_to_hash_table(fil_space_t* space)
{
    rw_lock_t *hash_lock;

    hash_lock = fil_system_get_hash_lock(space->id);
    rw_lock_x_lock(hash_lock);

    HASH_INSERT(fil_space_t, hash, fil_system->space_id_hash, space->id, space);

    rw_lock_x_unlock(hash_lock);
}

inline void fil_system_remove_space_from_hash_table(fil_space_t* space)
{
    rw_lock_t *hash_lock;

    hash_lock = fil_system_get_hash_lock(space->id);
    rw_lock_x_lock(hash_lock);

    HASH_DELETE(fil_space_t, hash, fil_system->space_id_hash, space->id, space);

    rw_lock_x_unlock(hash_lock);
}

// Flushes to disk the writes in file spaces
bool32 fil_system_flush_filnodes()
{
    bool32 ret = TRUE;
    fil_node_t* node;

    // only checkpoint thread
    for (node = UT_LIST_GET_FIRST(fil_system->fil_node_unflushed);
         node;
         node = UT_LIST_GET_NEXT(unflushed_list_node, node)) {

        // file is closed
        mutex_enter(&node->mutex);
        if (!node->is_open || node->handle == OS_FILE_INVALID_HANDLE) {
            mutex_exit(&node->mutex);
            continue;
        }
        mutex_exit(&node->mutex);

        // sync file
        if (!os_fsync_file(node->handle)) {
            char err_info[CM_ERR_MSG_MAX_LEN];
            os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
            LOGGER_ERROR(LOGGER, LOG_MODULE_TABLESPACE,
                "fil_flush_space_filnodes: fail to flush file, name %s error %s",
                node->name, err_info);

            ret = FALSE;
        }
    }

    return ret;
}


fil_space_t* fil_space_create(char *name, uint32 space_id, uint32 flags)
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
    space->flags = flags;
    space->page_size = 0;
    space->n_reserved_extents = 0;
    space->magic_n = M_FIL_SPACE_MAGIC_N;
    space->refcount = 0;
    space->io_in_progress = 0;
    mutex_create(&space->mutex);
    rw_lock_create(&space->rw_lock);
    UT_LIST_INIT(space->fil_nodes);

    mutex_enter(&fil_system->mutex, NULL);
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

    mutex_exit(&fil_system->mutex);

    if (tmp) { // name or spaceid already exists
        my_free((void *)space);
        space = NULL;
    } else {
        fil_system_insert_space_to_hash_table(space);
    }

    return space;
}

void fil_space_destroy(uint32 space_id)
{
    fil_space_t *space;
    fil_node_t *fil_node;

    space = fil_system_get_space_by_id(space_id);
    ut_a(space->magic_n == M_FIL_SPACE_MAGIC_N);

    rw_lock_x_lock(&space->rw_lock);
    fil_node = UT_LIST_GET_FIRST(space->fil_nodes);
    while (fil_node != NULL) {
        fil_node_destroy(space, fil_node, FALSE);
        fil_node = UT_LIST_GET_FIRST(space->fil_nodes);
    }
    rw_lock_x_unlock(&space->rw_lock);

    fil_system_remove_space_from_hash_table(space);

    mutex_destroy(&space->mutex);
    rw_lock_destroy(&space->rw_lock);

    my_free((void *)space);
}

fil_node_t* fil_node_create(fil_space_t *space, uint32 node_id,
    char *name, uint32 page_max_count, uint32 page_size, bool32 is_extend)
{
    fil_node_t *node, *tmp;

    if (node_id != DB_DATA_FILNODE_INALID_ID && node_id >= DB_SPACE_DATA_FILE_MAX_COUNT) {
        return NULL;
    }

    // check number of all filnode
    mutex_enter(&fil_system->mutex, NULL);
    if (fil_system->fil_node_num >= fil_system->fil_node_max_count) {
        mutex_exit(&fil_system->mutex);
        return NULL;
    }
    fil_system->fil_node_num++;
    mutex_exit(&fil_system->mutex);

    //
    node = (fil_node_t *)my_malloc(fil_system->mem_context, sizeof(fil_node_t) + (uint32)strlen(name) + 1);
    if (node == NULL) {
        return NULL;
    }

    memset(node, 0x00, sizeof(fil_node_t));

    node->space = space;
    node->name = (char *)node + sizeof(fil_node_t);
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
    node->n_pending_flushes = 0;
    
    // insert into filnodes array
    mutex_enter(&fil_system->mutex, NULL);
    if (node_id == DB_DATA_FILNODE_INALID_ID) {
        for (uint32 i = 0; i < fil_system->fil_node_max_count; i++) {
            if (fil_system->fil_nodes[i] == NULL) {
                node->id = i;
                fil_system->fil_nodes[i] = node;
                break;
            }
        }
    } else {
        if (fil_system->fil_nodes[node_id] != NULL) {
            fil_system->fil_node_num--;
            mutex_exit(&fil_system->mutex);
            my_free((void *)node);
            return NULL;
        }
        node->id = node_id;
        fil_system->fil_nodes[node_id] = node;
    }
    mutex_exit(&fil_system->mutex);

    // check if node name already exists
    rw_lock_x_lock(&space->rw_lock);
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
    rw_lock_x_unlock(&space->rw_lock);

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

bool32 fil_node_destroy(fil_space_t *space, fil_node_t *node, bool32 need_space_rwlock)
{
    fil_node_close(space, node);

    if (need_space_rwlock) {
        rw_lock_x_lock(&space->rw_lock);
    }
    UT_LIST_REMOVE(chain_list_node, space->fil_nodes, node);
    if (need_space_rwlock) {
        rw_lock_x_unlock(&space->rw_lock);
    }

    spin_lock(&fil_system->mutex, NULL);
    fil_system->fil_nodes[node->id] = NULL;
    ut_a(fil_system->fil_node_num > 0);
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
            LOGGER_ERROR(LOGGER, LOG_MODULE_TABLESPACE,
                    "Error: cannot close any file to open another for i/o\n"
                    "Pending i/o's on %lu files exist\n",
                    fil_system->open_pending_num);
            ut_error;
        }
        handle = last_node->handle;
        last_node->handle = OS_FILE_INVALID_HANDLE;
        UT_LIST_REMOVE(lru_list_node, fil_system->fil_node_lru, node);
        node->is_open = FALSE;
        spin_unlock(&fil_system->mutex);

        if (!os_close_file(handle)) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_TABLESPACE,
                    "Error: cannot close any file to open another for i/o\n"
                    "Pending i/o's on %lu files exist\n",
                    fil_system->open_pending_num);
            ut_error;
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

typedef struct st_fil_node_extend {
    mutex_t         mutex;
    volatile uint32 extend_page_count;
    volatile bool32 is_disk_full;
    fil_node_t*     node;
} fil_node_extend_t;

static status_t fil_space_extend_node_callback(int32 code, os_aio_slot_t* slot)
{
    fil_node_extend_t* node_extend = (fil_node_extend_t*) slot->message2;

    mutex_enter(&node_extend->mutex);
    node_extend->extend_page_count++;
    mutex_exit(&node_extend->mutex);

    switch (code) {
    case OS_FILE_IO_COMPLETION:
        break;
    case OS_FILE_DISK_FULL:
        node_extend->is_disk_full = TRUE;
        break;
    default:
        char err_info[CM_ERR_MSG_MAX_LEN];
        os_file_get_error_desc_by_err(code, err_info, CM_ERR_MSG_MAX_LEN);
        LOGGER_FATAL(LOGGER, LOG_MODULE_TABLESPACE,
            "fil_space_extend_node: fatal error occurred, node %s error = %d err desc = %s, service exited",
            node_extend->node->name, slot->ret, err_info);
        ut_error;
        break;
    }

    ut_ad(node_extend->node);
    ut_ad(node_extend->node == (fil_node_t*)slot->message1);
    fil_node_complete_io(node_extend->node, slot->type);

    return CM_SUCCESS;
}

static bool32 fil_space_extend_node(fil_node_t* node,
    uint32 page_hwm, uint32 size_increase, uint32 *actual_size)
{
    uint32            timeout_seconds = 300;
    uchar             buf[UNIV_PAGE_SIZE];
    const page_size_t page_size(node->space->id);
    page_id_t         page_id;
    fil_node_extend_t node_extend;

    ut_ad(fil_space_is_pinned(node->space));
    ut_a(node->page_max_count >= page_hwm + size_increase);

    *actual_size = 0;
    if (size_increase == 0) {
        return TRUE ;
    }

    //
    mutex_create(&node_extend.mutex);
    node_extend.node = node;
    node_extend.is_disk_full = FALSE;
    node_extend.extend_page_count = 0;

    memset(buf, 0x00, UNIV_PAGE_SIZE);

    for (uint32 page_no = page_hwm; page_no < page_hwm + size_increase; page_no++) {
        // page header
        mach_write_to_4(buf + FIL_PAGE_SPACE, node->space->id);
        mach_write_to_4(buf + FIL_PAGE_OFFSET, page_no);
        // write to file
        page_id.reset(node->space->id, page_no);
        status_t err = fil_write(FALSE, page_id, page_size, page_size.physical(),
            (void *)buf, fil_space_extend_node_callback, &node_extend);
        if (err != CM_SUCCESS) {
            LOGGER_FATAL(LOGGER, LOG_MODULE_TABLESPACE, "fil_space_extend: fail to write file, name %s", node->name);
            goto err_exit;
        }
    }

    //
    uint32 wait_loop = 0, wait_count = timeout_seconds * MILLISECS_PER_SECOND;
    while (node_extend.extend_page_count < size_increase && wait_loop < wait_count) {
        os_thread_sleep(1000);
        wait_loop++;
    }
    if (wait_loop == wait_count) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_TABLESPACE,
            "fil_space_extend: IO timeout for writing file, name %s timeout %u seconds",
            node->name, timeout_seconds);
        goto err_exit;
    }

    //
    if (!os_fsync_file(node->handle)) {
        int32 err = os_file_get_last_error();
        if (err == OS_FILE_DISK_FULL) {
            node_extend.is_disk_full = TRUE;
            LOGGER_ERROR(LOGGER, LOG_MODULE_TABLESPACE, "fil_space_extend: disk is full, name %s", node->name);
        } else {
            char err_info[CM_ERR_MSG_MAX_LEN];
            os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
            LOGGER_FATAL(LOGGER, LOG_MODULE_TABLESPACE,
                "fil_space_extend: fail to sync file, name = %s error = %s",
                node->name, err_info);
            goto err_exit;
        }
    }

    if (!node_extend.is_disk_full) {
        *actual_size = size_increase;
    }

    return node_extend.is_disk_full ? FALSE : TRUE;

err_exit:

    LOGGER_FATAL(LOGGER, LOG_MODULE_TABLESPACE, "fil_space_extend: A fatal error occurred, service exited.");
    ut_error;

    return FALSE;
}


static bool32 fil_space_extend_node1(fil_space_t *space,
    fil_node_t *node, uint32 page_hwm, uint32 size_increase, uint32 *actual_size)
{
    os_aio_context_t *aio_ctx = NULL;
    uint32            timeout_seconds = 300;
    memory_page_t    *page = NULL;
    uchar            *buf;
    uint32            size;
    uint64            offset;
    bool32            is_disk_full = FALSE;

    ut_ad(fil_space_is_pinned(space));

    *actual_size = 0;
    if (size_increase == 0) {
        return TRUE ;
    }

    size = page_hwm + size_increase;
    ut_a(node->page_max_count >= size);

    // The purpose of fil_system_pin_space here is to call fil_node_complete_io later,
    // fil_node_complete_io will perform UNPIN SPACE operation.
    fil_system_pin_space(space);

    if (!fil_node_prepare_for_io(node)) {
        fil_system_unpin_space(space);
        return FALSE;
    }

    aio_ctx = os_aio_array_alloc_context(fil_system->aio_array);
    page = marea_alloc_page(fil_system->mem_area, UNIV_PAGE_SIZE);
    buf = (uchar *)MEM_PAGE_DATA_PTR(page);
    memset(buf, 0x00, UNIV_PAGE_SIZE);

    for (uint32 loop = 0; loop < size_increase && !is_disk_full; loop += OS_AIO_N_PENDING_IOS_PER_THREAD) {
        uint32 io_count = 0;
        for (uint32 i = page_hwm + loop; i < page_hwm + loop + OS_AIO_N_PENDING_IOS_PER_THREAD; i++) {
            offset = i * UNIV_PAGE_SIZE;
            mach_write_to_4(buf + FIL_PAGE_SPACE, space->id);
            mach_write_to_4(buf + FIL_PAGE_OFFSET, i);
            if (os_file_aio_submit(aio_ctx, OS_FILE_WRITE, node->name, node->handle,
                                   (void *)buf, UNIV_PAGE_SIZE, offset) == NULL) {
                char err_info[CM_ERR_MSG_MAX_LEN];
                os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
                LOGGER_FATAL(LOGGER, LOG_MODULE_TABLESPACE,
                    "fil_space_extend: fail to write file, name %s error %s",
                    node->name, err_info);
                goto err_exit;
            }
            io_count++;
        }

        // wait for io-complete
        uint32 timeout_seconds = 300; // 300 seconds
        os_aio_slot_t* aio_slot = NULL;
        while (io_count > 0) {
            int32 err = os_file_aio_context_wait(aio_ctx, &aio_slot, timeout_seconds * 1000000);
            switch (err) {
            case OS_FILE_IO_COMPLETION:
                break;
            case OS_FILE_DISK_FULL:
                LOGGER_DEBUG(LOGGER, LOG_MODULE_TABLESPACE, "fil_space_extend: disk is full, name %s", node->name);
                is_disk_full = TRUE;
                break;
            case OS_FILE_IO_TIMEOUT:
                LOGGER_ERROR(LOGGER, LOG_MODULE_TABLESPACE,
                    "fil_space_extend: IO timeout for writing file, name %s timeout %u seconds",
                    node->name, timeout_seconds);
                goto err_exit;
            default:
                char err_info[CM_ERR_MSG_MAX_LEN];
                os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
                LOGGER_ERROR(LOGGER, LOG_MODULE_TABLESPACE,
                    "fil_space_extend: failed to write file, name %s error %s",
                    node->name, err_info);
                goto err_exit;
            }

            //
            ut_a(aio_slot);
            os_aio_context_free_slot(aio_slot);

            io_count--;
        }

    }

    if (!os_fsync_file(node->handle)) {
        int32 err = os_file_get_last_error();
        if (err == OS_FILE_DISK_FULL) {
            is_disk_full = TRUE;
            LOGGER_ERROR(LOGGER, LOG_MODULE_TABLESPACE, "fil_space_extend: disk is full, name %s", node->name);
        } else {
            char err_info[CM_ERR_MSG_MAX_LEN];
            os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
            LOGGER_FATAL(LOGGER, LOG_MODULE_TABLESPACE,
                "fil_space_extend: fail to sync file, name = %s error = %s",
                node->name, err_info);
            goto err_exit;
        }
    }

    if (!is_disk_full) {
        *actual_size = size_increase;
    }

    os_aio_array_free_context(aio_ctx);
    marea_free_page(fil_system->mem_area, page, UNIV_PAGE_SIZE);

    fil_node_complete_io(node, OS_FILE_WRITE);

    return is_disk_full ? FALSE : TRUE;

err_exit:

    LOGGER_FATAL(LOGGER, LOG_MODULE_TABLESPACE, "fil_space_extend: A fatal error occurred, service exited.");
    ut_error;

    return FALSE;
}

bool32 fil_space_extend(fil_space_t* space, uint32 size_after_extend, uint32 *actual_size)
{
    fil_node_t       *node;
    uint32            size_increase;

    ut_ad(!srv_read_only_mode || fsp_is_system_temporary(space->id));
    ut_ad(fil_space_is_pinned(space));

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
        uint32 node_size_increase, node_actual_size = 0;
        if (size_increase <= free_cur_node) {
            node_size_increase = size_increase;
        } else {
            node_size_increase = free_cur_node;
        }

        bool32 ret = fil_space_extend_node(node, page_hwm, node_size_increase, &node_actual_size);
        if (!ret) {
            goto err_exit;
        }
        page_hwm = 0;
        *actual_size += node_actual_size;
        size_increase -= node_actual_size;

        node = UT_LIST_GET_NEXT(chain_list_node, node);
    }

    return TRUE;

err_exit:

    return FALSE;
}

uint32 fil_space_get_size(uint32 space_id)
{
    fil_space_t* space;
    uint32       size = 0;

    space = fil_system_get_space_by_id(space_id);
    if (space) {
        size = space->size_in_header;
        fil_system_unpin_space(space);
    }

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

    uint32 size, size_increase;
    if (size < FSP_EXTENT_SIZE) {
        size_increase = FSP_EXTENT_SIZE;
    } else if (size < 32 * FSP_EXTENT_SIZE) {
        size_increase = FSP_EXTENT_SIZE;
    } else {
        size_increase = FSP_FREE_ADD * FSP_EXTENT_SIZE;
    }

    return FSP_EXTENT_SIZE * FSP_FREE_ADD;
}

// Tries to extend the last data file of a tablespace if it is auto-extending.
static bool32 fsp_try_extend_data_file(
    uint32*       actual_increase,/*!< out: actual increase in pages */
    fil_space_t*  space,  /*!< in: space */
    fsp_header_t* header,  /*!< in/out: space header */
    mtr_t*        mtr)
{
    uint32 size;  /* current number of pages in the datafile */
    uint32 size_increase;  /* number of pages to extend this file */
    uint32 actual_size;
    const page_size_t page_size(space->id);

    size_increase = fsp_get_autoextend_increment(space);
    if (size_increase == 0) {
        return FALSE;
    }

    size = mach_read_from_4(header + FSP_SIZE);
    ut_ad(size == space->size_in_header);
    if (!fil_space_extend(space, size + size_increase, &actual_size)) {
        return FALSE;
    }

    space->size_in_header += actual_size;
    mlog_write_uint32(header + FSP_SIZE, space->size_in_header, MLOG_4BYTES, mtr);

    return TRUE;
}

inline bool32 fil_addr_is_null(fil_addr_t addr) /*!< in: address */
{
    return(addr.page == FIL_NULL);
}

static inline bool32 fil_space_belongs_in_lru(fil_space_t *space)
{
    return space->id >= FIL_USER_SPACE_ID;
}

// Closes a file
static bool32 fil_node_close_file(fil_node_t *node)
{
    ut_ad(node->is_open);
    ut_ad(node->n_pending == 0);
    ut_ad(node->n_pending_flushes == 0);
    ut_ad(node->is_io_progress);
    ut_ad(!node->is_extend);

    if (!os_fsync_file(node->handle)) {
        char err_info[CM_ERR_MSG_MAX_LEN];
        os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
        LOGGER_ERROR(LOGGER, LOG_MODULE_TABLESPACE,
            "fil_node_close_file: failed to sync filnode, err desc = %s",
            err_info);
        return FALSE;
    }
    if (!os_close_file(node->handle)) {
        char err_info[CM_ERR_MSG_MAX_LEN];
        os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
        LOGGER_ERROR(LOGGER, LOG_MODULE_TABLESPACE,
            "fil_node_close_file: failed to close filnode, err desc = %s",
            err_info);
        return FALSE;
    }

    node->handle = OS_FILE_INVALID_HANDLE;

    return TRUE;
}

// Opens a file of a node of a tablespace
static inline bool32 fil_node_open_file(fil_node_t *node)
{
    ut_ad(node->n_pending == 0);
    ut_ad(node->is_open == FALSE);
    ut_ad(node->is_io_progress);

    // Open the file for reading and writing
    bool32 ret = os_open_file(node->name, OS_FILE_OPEN, OS_FILE_AIO, &node->handle);
    if (UNLIKELY(!ret)) {
        char err_info[CM_ERR_MSG_MAX_LEN];
        os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
        LOGGER_ERROR(LOGGER, LOG_MODULE_TABLESPACE,
            "fil_node_open_file: failed to open file, name = %s err desc = %s",
            node->name, err_info);
    }

    return ret;
}

static inline bool32 fil_try_to_close_file_in_LRU()
{
    fil_node_t  *node;

    // We keep log files and system tablespace files always open

    mutex_enter(&(fil_system->lru_mutex), NULL);

    for (node = UT_LIST_GET_LAST(fil_system->fil_node_lru);
         node != NULL;
         node = UT_LIST_GET_PREV(lru_list_node, node)) {
        //
        mutex_enter(&node->mutex);
        if (FIL_NODE_IS_IN_READ_WRITE(node) || FIL_NODE_IS_IN_SYNC(node) || FIL_NODE_IS_IN_OPEN_CLOSE(node)) {
            mutex_exit(&node->mutex);
            continue;
        }
        node->is_io_progress = TRUE;
        mutex_exit(&node->mutex);
        // found a node
        break;
    }

    mutex_exit(&(fil_system->lru_mutex));

    if (node == NULL) {
        LOGGER_DEBUG(LOGGER, LOG_MODULE_TABLESPACE,
                "fil_try_to_close_file_in_LRU: cannot close any file to open another for i/o, "
                "pending i/o's on %lu files exist\n",
                fil_system->open_pending_num);
        return FALSE;
    }

    if (LIKELY(fil_node_close_file(node))) {
        LOGGER_DEBUG(LOGGER, LOG_MODULE_TABLESPACE, "fil_try_to_close_file_in_LRU: close file %s", node->name);
    } else {
        LOGGER_FATAL(LOGGER, LOG_MODULE_TABLESPACE, "fil_try_to_close_file_in_LRU: fatal error occurred, service exited");
        ut_error;
    }

    // if fil node is in the LRU list, remove it
    if (fil_space_belongs_in_lru(node->space)) {
        mutex_enter(&(fil_system->lru_mutex), NULL);
        ut_a(UT_LIST_GET_LEN(fil_system->fil_node_lru) > 0);
        UT_LIST_REMOVE(lru_list_node, fil_system->fil_node_lru, node);
        mutex_exit(&(fil_system->lru_mutex));
    }

    // 
    mutex_enter(&node->mutex);
    node->is_open = FALSE;
    node->is_io_progress = FALSE;
    mutex_exit(&node->mutex);

    //
    ut_ad(fil_system->open_pending_num > 0);
    atomic32_dec(&fil_system->open_pending_num);

    return TRUE;
}

// Prepares a file node for i/o.
// Opens the file if it is closed.
// Updates the pending i/o's field in the node and the system appropriately.
// Takes the node off the LRU list if it is in the LRU list.
// The caller must hold the fil_sys mutex.
static inline bool32 fil_node_prepare_for_io(fil_node_t *node)
{
    ut_ad(fil_space_is_pinned(node->space));

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

    // check number of open files
    atomic32_t open_pending_num = atomic32_inc(&fil_system->open_pending_num);
    if ((uint32)open_pending_num > fil_system->max_n_open) {
        LOGGER_INFO(LOGGER, LOG_MODULE_TABLESPACE,
            "Warning: open files %u exceeds the limit %u",
            open_pending_num, fil_system->max_n_open);

        uint64 wait_count = 0;
        // Too many files are open, try to close some
        // We keep log files and system tablespace files always open
        while (fil_try_to_close_file_in_LRU() == FALSE) {
            os_thread_sleep(1000); // 1ms
            wait_count++;
        }
        if (wait_count > 0) {
            srv_stats.filnode_close_wait_count.add(wait_count);
        }
    }

    // File is closed: open it

    ut_ad(node->is_open == FALSE);
    ut_ad(node->n_pending == 0);

    bool32 ret = fil_node_open_file(node);
    if (ret) {
        ut_ad(node->handle != OS_FILE_INVALID_HANDLE);
        mutex_enter(&node->mutex);
        node->is_open = TRUE;
        node->n_pending++;
        node->is_io_progress = FALSE;
        mutex_exit(&node->mutex);

        if (fil_space_belongs_in_lru(node->space)) {
            /* Put the node to the LRU list */
            mutex_enter(&fil_system->lru_mutex);
            UT_LIST_ADD_FIRST(lru_list_node, fil_system->fil_node_lru, node);
            mutex_exit(&fil_system->lru_mutex);
        }
    } else {
        mutex_enter(&node->mutex);
        node->is_io_progress = FALSE;
        mutex_exit(&node->mutex);

        atomic32_dec(&fil_system->open_pending_num);
    }

    return ret;
}

// Updates the data structures when an i/o operation finishes.
// Updates the pending i/o's field in the node appropriately.
inline void fil_node_complete_io(fil_node_t* node, uint32 type) // OS_FILE_WRITE or OS_FILE_READ
{
    fil_space_t *space = node->space;
    ut_ad(space);
    ut_ad(fil_space_is_pinned(space));

    //
    mutex_enter(&node->mutex);
    ut_a(node->n_pending > 0);
    node->n_pending--;
    mutex_exit(&node->mutex);

    //
    fil_system_unpin_space(space);
}

inline fil_node_t* fil_node_get_by_page_id(fil_space_t* space, const page_id_t &page_id)
{
    fil_node_t* node = NULL;
    uint32 block_offset;

    ut_ad(rw_lock_own(&space->rw_lock, RW_LOCK_SHARED) || rw_lock_own(&space->rw_lock, RW_LOCK_EXCLUSIVE));

    block_offset = page_id.page_no();
    node = UT_LIST_GET_FIRST(space->fil_nodes);
    for (;;) {
        if (node == NULL) {
            LOGGER_DEBUG(LOGGER, LOG_MODULE_TABLESPACE,
                "fil_node_get_by_page_id: failed to find fil_node (space id = %lu page no = %lu)",
                page_id.space_id(), page_id.page_no());
            return NULL;
        } else if (node->page_max_count > block_offset) {
            // Found
            break;
        } else {
            block_offset -= node->page_max_count;
            node = UT_LIST_GET_NEXT(chain_list_node, node);
        }
    }

    return node;
}

inline status_t fil_io(
    uint32 type, // in: OS_FILE_READ, OS_FILE_WRITE
    bool32 sync, // in: true if synchronous aio is desired
    const page_id_t &page_id, // in:
    const page_size_t &page_size, // in:
    uint32 byte_offset, // in: remainder of offset in bytes;
                        // in aio this must be divisible by the OS block size
    uint32 len, // in: this must be a block size multiple
    void*  buf, // in/out: buffer where to store data read
    aio_slot_func slot_func,
    void*  message) // in: message for aio handler if non-sync aio used, else ignored
{
    fil_space_t*      space;
    fil_node_t*       node = NULL;
    os_aio_slot_t*    tmp_slot = NULL;
    os_aio_context_t* aio_ctx = NULL;
    uint32            block_offset;

    ut_ad(byte_offset < UNIV_PAGE_SIZE);
    ut_ad(byte_offset % OS_FILE_LOG_BLOCK_SIZE == 0);
    ut_ad(len % OS_FILE_LOG_BLOCK_SIZE == 0);

    if (type == OS_FILE_READ) {
        srv_stats.data_read.add(len);
    } else if (type == OS_FILE_WRITE) {
        ut_ad(!srv_read_only_mode);
        srv_stats.data_written.add(len);
    }

    // 1. get fil_space by page_id and pin
    space = fil_system_get_space_by_id(page_id.space_id());

    // 2. get fil_node by page_id

    rw_lock_s_lock(&space->rw_lock);

    block_offset = page_id.page_no();
    node = UT_LIST_GET_FIRST(space->fil_nodes);
    for (;;) {
        if (node == NULL) {
            rw_lock_s_unlock(&space->rw_lock);
            LOGGER_ERROR(LOGGER, LOG_MODULE_TABLESPACE,
                "fil_io: failed to find fil_node (io type = %s space id = %lu page no = %lu)",
                type == OS_FILE_READ ? "read" : "write", page_id.space_id(), page_id.page_no());
            goto err_exit;
        } else if (node->page_max_count > block_offset) {
            // Found
            break;
        } else {
            block_offset -= node->page_max_count;
            node = UT_LIST_GET_NEXT(chain_list_node, node);
        }
    }

    // Open file if closed
    if (!fil_node_prepare_for_io(node)) {
        rw_lock_s_unlock(&space->rw_lock);
        LOGGER_ERROR(LOGGER, LOG_MODULE_TABLESPACE,
            "fil_io: failed to %s i/o to tablespace space_id %lu, page_no %lu",
            type == OS_FILE_READ ? "read" : "write",
            node->name, page_id.space_id(), page_id.page_no());
        goto err_exit;
    }

    rw_lock_s_unlock(&space->rw_lock);

    // 3. file offset for filnode
    uint64 offset = block_offset * page_size.physical() + byte_offset;

    // 4. get i/o context

    if (sync) {
        uint32 ctx_index = page_id.page_no() % srv_sync_io_contexts;
        aio_ctx = os_aio_array_get_nth_context(srv_os_aio_sync_array, ctx_index);
    } else {
        uint32 ctx_index;
        if (type == OS_FILE_READ) {
            ctx_index = page_id.page_no() % srv_read_io_threads;
            aio_ctx = os_aio_array_get_nth_context(srv_os_aio_async_read_array, ctx_index);
        } else {
            ctx_index = page_id.page_no() % srv_write_io_threads;
            aio_ctx = os_aio_array_get_nth_context(srv_os_aio_async_write_array, ctx_index);
        }
    }

    // 5. do i/o

    tmp_slot = os_file_aio_submit(aio_ctx, type, node->name,
        node->handle, (void *)buf, len, offset, slot_func, node, message);
    if (tmp_slot == NULL) {
        char err_info[CM_ERR_MSG_MAX_LEN];
        os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
        LOGGER_ERROR(LOGGER, LOG_MODULE_TABLESPACE,
            "fil_io: failed to do %s i/o from %s data file, space id %lu, page no %lu, error %s",
            type == OS_FILE_READ ? "read" : "write",
            node->name, page_id.space_id(), page_id.page_no(), err_info);
        goto err_exit;
    }

    if (sync) {
        uint32 io_timeout = type == OS_FILE_READ ? srv_read_io_timeout_seconds : srv_write_io_timeout_seconds;
        int32 ret = os_file_aio_slot_wait(tmp_slot, io_timeout * 1000000);
        switch (ret) {
        case OS_FILE_IO_COMPLETION:
            break;
        case OS_FILE_IO_TIMEOUT:
            LOGGER_ERROR(LOGGER, LOG_MODULE_TABLESPACE,
                "fil_io: timeout(%lu seconds) for do %s i/o from %s data file, space id %lu, page no %lu",
                type == OS_FILE_READ ? "read" : "write",
                io_timeout, node->name, page_id.space_id(), page_id.page_no());
            goto err_exit;
        default:
            char err_info[CM_ERR_MSG_MAX_LEN];
            os_file_get_error_desc_by_err(ret, err_info, CM_ERR_MSG_MAX_LEN);
            LOGGER_ERROR(LOGGER, LOG_MODULE_TABLESPACE,
                "fil_io: failed to do %s i/o from %s data file, space id %lu, page no %lu, error %s",
                type == OS_FILE_READ ? "read" : "write",
                node->name, page_id.space_id(), page_id.page_no(), err_info);
            goto err_exit;
        }

        fil_node_complete_io(node, type);

    }

    return CM_SUCCESS;

err_exit:

    LOGGER_FATAL(LOGGER, LOG_MODULE_TABLESPACE, "fil_io: fatal error occurred, service exited");
    ut_error;

    return CM_ERROR;
}

inline status_t fil_read(
    bool32 sync, const page_id_t &page_id, const page_size_t &page_size,
    uint32 len, void* buf, aio_slot_func slot_func, void* message)
{
    return fil_io(OS_FILE_READ, sync, page_id, page_size, 0, len, buf, slot_func, message);
}

inline status_t fil_write(
    bool32 sync, const page_id_t &page_id, const page_size_t &page_size,
    uint32 len, void* buf, aio_slot_func slot_func, void* message)
{
    return fil_io(OS_FILE_WRITE, sync, page_id, page_size, 0, len, buf, slot_func, message);
}


// Waits for an aio operation to complete.
inline void fil_aio_reader_and_writer_wait(os_aio_context_t* context)
{
    int32          ret;
    os_aio_slot_t* slot = NULL;

    ret = os_file_aio_context_wait(context, &slot, OS_WAIT_INFINITE_TIME);

    if (srv_shutdown_state == SHUTDOWN_EXIT_THREADS) {
        return;
    }

    //if (ret != OS_FILE_IO_COMPLETION && ret != OS_FILE_IO_TIMEOUT) {
    //    LOGGER_FATAL(LOGGER, "A fatal error occurred in reader thread or writes thread, service exited.");
    //    ut_error;
    //}

    if (slot) {
        if (slot->callback_func) {
            slot->callback_func(ret, slot);
        }

        os_aio_context_free_slot(slot);
    }
}

