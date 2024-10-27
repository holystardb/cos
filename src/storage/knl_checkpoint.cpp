#include "knl_checkpoint.h"
#include "cm_timer.h"



checkpoint_t   g_checkpoint = {0};

status_t checkpoint_init(char* dbwr_file_name, uint32 dbwr_file_size)
{
    checkpoint_t* checkpoint = &g_checkpoint;

    memset(checkpoint, 0x00, sizeof(checkpoint_t));

    checkpoint->checkpoint_event = os_event_create(NULL);
    os_event_set(checkpoint->checkpoint_event);

    mutex_create(&checkpoint->mutex);
    checkpoint->group.item_count = 0;
    checkpoint->group.buf_size = CHECKPOINT_GROUP_MAX_SIZE * UNIV_PAGE_SIZE;
    checkpoint->group.buf = (char *)ut_malloc(checkpoint->group.buf_size);
    if (checkpoint->group.buf == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_CHECKPOINT, "checkpoint_init: failed to malloc doublewrite memory");
        return CM_ERROR;
    }
    checkpoint->flush_timeout_us = 1000000 * 300; // 300s
    
    checkpoint->enable_double_write = TRUE;
    checkpoint->double_write.name = dbwr_file_name;
    checkpoint->double_write.size = dbwr_file_size;

    if (!os_open_file(dbwr_file_name, OS_FILE_OPEN, OS_FILE_AIO, &checkpoint->double_write.handle)) {
        char err_info[CM_ERR_MSG_MAX_LEN];
        os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
        LOGGER_ERROR(LOGGER, LOG_MODULE_CHECKPOINT,
            "checkpoint_init: failed to open doublewrite file, name %s, error desc %s",
            dbwr_file_name, err_info);

        ut_free(checkpoint->group.buf);
        return CM_ERROR;
    }

    //
    uint32 io_pending_count_per_context = 8;
    uint32 io_context_count = 1;
    checkpoint->double_write.aio_array = os_aio_array_create(io_pending_count_per_context, io_context_count);
    if (checkpoint->double_write.aio_array == NULL) {
        ut_free(checkpoint->group.buf);
        os_close_file(checkpoint->double_write.handle);
        LOGGER_ERROR(LOGGER, LOG_MODULE_CHECKPOINT, "checkpoint_init: failed to create aio array for doublewrite");
        return CM_ERROR;
    }

    return CM_SUCCESS;
}

status_t checkpoint_full_checkpoint()
{
    return CM_SUCCESS;
}

status_t checkpoint_increment_checkpoint()
{
    return CM_SUCCESS;
}

static void checkpoint_copy_item(checkpoint_t* checkpoint,
    buf_pool_t* buf_pool, buf_block_t* block)
{
    // 1 copy data
    // if it is transaction slot page, 
    rw_lock_s_lock(&(block->rw_lock));
    memcpy(checkpoint->group.buf + UNIV_PAGE_SIZE * checkpoint->group.item_count, block->frame, UNIV_PAGE_SIZE);
    //knl_securec_check(ret);
    rw_lock_s_unlock(&(block->rw_lock));

    // 2 reset page.recovery_lsn and remove block from buf_pool->flush_list
    mutex_enter(&block->mutex);
    mutex_enter(&buf_pool->flush_list_mutex);
    block->page.recovery_lsn = 0;
    block->page.in_flush_list = FALSE;
    UT_LIST_REMOVE(list_node_flush, buf_pool->flush_list, block);
    mutex_exit(&buf_pool->flush_list_mutex);
    mutex_exit(&block->mutex);

    // 3 insert into group items
    checkpoint_sort_item_t* item = &checkpoint->group.items[checkpoint->group.item_count];
    item->page_id.copy_from(block->page.id);
    item->buf_id = checkpoint->group.item_count;
    item->is_flushed = FALSE;
    checkpoint->group.item_count++;
}

static void checkpoint_copy_item_and_neighbors(checkpoint_t* checkpoint,
    buf_block_t* block, lsn_t least_recovery_point)
{
    mutex_t*    block_mutex;
    rw_lock_t*  hash_lock;
    buf_page_t* bpage;
    buf_pool_t* buf_pool;
    page_id_t   page_id;

    //
    const uint32 buf_flush_area = 64;
    uint32 low = ut_uint32_align_down(block->get_page_no(), buf_flush_area);
    uint32 high = ut_uint32_align_up(block->get_page_no(), buf_flush_area);
    if (high == 0) {
        high = buf_flush_area;
    }
    if (high > fil_space_get_size(block->get_space_id())) {
        high = fil_space_get_size(block->get_space_id());
    }

    for (uint32 page_no = low; page_no < high; page_no++) {

        // 1 We have already flushed enough pages
        if (checkpoint->group.item_count >= CHECKPOINT_GROUP_MAX_SIZE) {
            break;
        }

        // 2 get block
        page_id.reset(block->get_space_id(), page_no);
        buf_pool = buf_pool_from_page_id(page_id);
        ut_ad(buf_pool);

        bpage = buf_page_hash_get_locked(buf_pool, page_id, &hash_lock, RW_LOCK_SHARED);
        if (!bpage) {
            continue;
        }

        // 3 pin block

        block_mutex = buf_page_get_mutex(bpage);
        mutex_enter(block_mutex);

        // Now safe to release page_hash rw_lock
        rw_lock_s_unlock(hash_lock);

        if (bpage->recovery_lsn == 0 ||
            bpage->recovery_lsn > least_recovery_point) {
            mutex_exit(block_mutex);
            continue;
        }

        buf_page_fix(bpage);

        mutex_exit(block_mutex);

        // 4 copy page data
        checkpoint_copy_item(checkpoint, buf_pool, (buf_block_t *)bpage);

        // 5 unpin block
        buf_page_unfix(bpage);

        LOGGER_DEBUG(LOGGER, LOG_MODULE_CHECKPOINT,
            "checkpoint_copy_item_and_neighbors: block (space id %lu, page no %lu)",
            page_id.space_id(), page_id.page_no());
    }
}


static status_t checkpoint_copy_dirty_pages(checkpoint_t* checkpoint, lsn_t least_recovery_point)
{
    buf_block_t* block;
    uint32 buf_pool_instances = buf_pool_get_instances();

    // When we traverse all the flush lists
    // we don't want another thread to add a dirty page to any flush list.
    //log_flush_order_mutex_enter();

    for (uint32 i = 0; i < buf_pool_instances && checkpoint->group.item_count < CHECKPOINT_GROUP_MAX_SIZE; i++) {
        buf_pool_t* buf_pool = buf_pool_get(i);

        mutex_enter(&buf_pool->flush_list_mutex);
        block = UT_LIST_GET_LAST(buf_pool->flush_list);
        while (block != NULL &&
               block->page.recovery_lsn <= least_recovery_point &&
               checkpoint->group.item_count < CHECKPOINT_GROUP_MAX_SIZE) {
            mutex_exit(&buf_pool->flush_list_mutex);

            checkpoint_copy_item_and_neighbors(checkpoint, block, least_recovery_point);

            mutex_enter(&buf_pool->flush_list_mutex);
            block = UT_LIST_GET_LAST(buf_pool->flush_list);
        }
        mutex_exit(&buf_pool->flush_list_mutex);
    }

    //log_flush_order_mutex_exit();

    return CM_SUCCESS;
}

static int32 checkpoint_flush_sort_comparator(const void *pa, const void *pb)
{
    const checkpoint_sort_item_t *a = (const checkpoint_sort_item_t *)pa;
    const checkpoint_sort_item_t *b = (const checkpoint_sort_item_t *)pb;

    /* compare space no */
    if (a->page_id.space_id() < b->page_id.space_id()) {
        return -1;
    } else if (a->page_id.space_id() > b->page_id.space_id()) {
        return 1;
    }

    /* compare page no */
    if (a->page_id.page_no() < b->page_id.page_no()) {
        return -1;
    } else if (a->page_id.page_no() > b->page_id.page_no()) {
        return 1;
    }

    /* equal pageid is impossible */
    return 0;
}

static inline status_t checkpoint_sort_pages(checkpoint_t* checkpoint)
{
    qsort(checkpoint->group.items, checkpoint->group.item_count,
        sizeof(checkpoint_sort_item_t), checkpoint_flush_sort_comparator);
    return CM_SUCCESS;
}

static uint32 checkpoint_adjust_io_capacity(checkpoint_t* checkpoint)
{
    //uint32 ckpt_io_capacity = attr->ckpt_io_capacity;

    return checkpoint->group.item_count;
}

static inline void checkpoint_delay(checkpoint_t* checkpoint, uint32 ckpt_io_capacity)
{
    /* max capacity, skip sleep */
    if (checkpoint->group.item_count == ckpt_io_capacity) {
        return;
    }

    os_thread_sleep(1000000); // 1s
}

static status_t checkpoint_flush_callback(int32 code, os_aio_slot_t* slot)
{
    checkpoint_sort_item_t* item = (checkpoint_sort_item_t*) slot->message2;

    if (code != OS_FILE_IO_COMPLETION) {
        char err_info[CM_ERR_MSG_MAX_LEN];
        os_file_get_error_desc_by_err(code, err_info, CM_ERR_MSG_MAX_LEN);
        LOGGER_FATAL(LOGGER, LOG_MODULE_CHECKPOINT,
            "checkpoint_flush: fatal error occurred, error = %d err desc = %s, service exited",
            slot->ret, err_info);
        ut_error;
    }

    item->is_flushed = TRUE;

    fil_node_t* node = (fil_node_t*)slot->message1;
    ut_ad(node);

    fil_node_complete_io(node, slot->type);

    return CM_SUCCESS;
}

static status_t checkpoint_write_pages(checkpoint_t* checkpoint, uint32 begin, uint32 end)
{
    status_t err;
    checkpoint_sort_item_t* item;
    const page_size_t page_size(0);

    for (uint32 i = begin; i < end; i++) {
        item = &checkpoint->group.items[i];

        err = fil_write(FALSE, item->page_id, page_size, page_size.physical(),
            checkpoint->group.buf + page_size.physical() * item->buf_id,
            checkpoint_flush_callback, item);
        if (err != CM_SUCCESS) {
            LOGGER_FATAL(LOGGER, LOG_MODULE_CHECKPOINT,
                "checkpoint_flush: fatal error occurred for fil_write, service exited");
            ut_error;
        }
    }

    uint32 count = 0;
    date_t begin_time_us = g_timer()->now;
    while (g_timer()->now < begin_time_us + checkpoint->flush_timeout_us) {
        count = 0;
        for (uint32 i = begin; i < end; i++) {
            item = &checkpoint->group.items[i];
            if (item->is_flushed) {
                count++;
            }
        }
        if (count == end - begin) {
            break;
        }
        os_thread_sleep(1000); // 1ms
    }

    if (count != end - begin) {
        LOGGER_FATAL(LOGGER, LOG_MODULE_CHECKPOINT,
            "checkpoint_flush: fatal error occurred, timeout(%llu milliseconds) and service exited",
            checkpoint->flush_timeout_us / MICROSECS_PER_MILLISEC);
        ut_error;
    }

    return CM_SUCCESS;
}

static status_t checkpoint_sync_pages(checkpoint_t* checkpoint, uint32 begin, uint32 end)
{
    checkpoint_sort_item_t* item;
    const page_size_t page_size(0);
    fil_node_t* node;
    fil_space_t* space;

    // 1. init
    UT_LIST_INIT(fil_system->fil_node_unflushed);

    // 2. get fil_node and append unflushed list
    for (uint32 i = begin; i < end; i++) {
        item = &checkpoint->group.items[i];

        // 2-1. get fil_space by page_id
        space = fil_system_get_space_by_id(item->page_id.space_id());
        if (space == NULL) {
            continue;
        }

        rw_lock_s_lock(&space->rw_lock);

        // 2-2. get fil_node by page_id and append to unflushed list
        node = fil_node_get_by_page_id(space, item->page_id);
        if (node) {
            mutex_enter(&node->mutex);
            if (!node->is_in_unflushed_list) {
                node->n_pending_flushes++;
                node->is_in_unflushed_list = TRUE;
                UT_LIST_ADD_FIRST(unflushed_list_node, fil_system->fil_node_unflushed, node);
            }
            mutex_exit(&node->mutex);
        }

        rw_lock_s_unlock(&space->rw_lock);

        // 2-3. release space
        fil_system_unpin_space(space);
    }

    // 3. flush fil_node
    if (!fil_system_flush_filnodes()) {
        LOGGER_FATAL(LOGGER, LOG_MODULE_CHECKPOINT, "checkpoint_sync_pages: fatal error occurred, service exited");
        ut_error;
    }

    // 4. clean
    node  = UT_LIST_GET_FIRST(fil_system->fil_node_unflushed);
    while (node) {
        ut_ad(node->is_in_unflushed_list);
        ut_ad(node->n_pending_flushes > 0);
        UT_LIST_REMOVE(unflushed_list_node, fil_system->fil_node_unflushed, node);

        mutex_enter(&node->mutex);
        node->is_in_unflushed_list = FALSE;
        node->n_pending_flushes--;
        mutex_exit(&node->mutex);
        //
        node = UT_LIST_GET_FIRST(fil_system->fil_node_unflushed);
    }

    return CM_SUCCESS;
}

static bool32 checkpoint_double_write(checkpoint_t* checkpoint)
{
    os_aio_context_t* aio_ctx = NULL;
    os_aio_slot_t*    aio_slot = NULL;

    aio_ctx = os_aio_array_alloc_context(checkpoint->double_write.aio_array);
    ut_ad(aio_ctx);
    aio_slot = os_file_aio_submit(aio_ctx, OS_FILE_WRITE,
        checkpoint->double_write.name, checkpoint->double_write.handle,
        (void *)checkpoint->group.buf, UNIV_PAGE_SIZE *checkpoint->group.item_count, 0);
    if (aio_slot == NULL) {
        char err_info[CM_ERR_MSG_MAX_LEN];
        os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
        LOGGER_ERROR(LOGGER, LOG_MODULE_CHECKPOINT,
            "checkpoint_double_write: fail to write file, name %s error %s",
            checkpoint->double_write.name, err_info);
        goto err_exit;
    }

    bool32 ret = TRUE;
    int32 err = os_file_aio_context_wait(aio_ctx, &aio_slot, checkpoint->flush_timeout_us);
    switch (err) {
    case OS_FILE_IO_COMPLETION:
        break;
    case OS_FILE_IO_TIMEOUT:
        LOGGER_ERROR(LOGGER, LOG_MODULE_CHECKPOINT,
            "checkpoint_double_write: IO timeout for writing file, name %s timeout %u seconds",
            checkpoint->double_write.name, checkpoint->flush_timeout_us / MICROSECS_PER_SECOND);
        goto err_exit;
    case OS_FILE_DISK_FULL:
        LOGGER_ERROR(LOGGER, LOG_MODULE_CHECKPOINT, "checkpoint_double_write: disk is full");
        ret = FALSE;
        break;
    default:
        char err_info[CM_ERR_MSG_MAX_LEN];
        os_file_get_error_desc_by_err(err, err_info, CM_ERR_MSG_MAX_LEN);
        LOGGER_ERROR(LOGGER, LOG_MODULE_CHECKPOINT,
            "checkpoint_double_write: fail to write file, name = %s error = %s",
            checkpoint->double_write.name, err_info);
        goto err_exit;
    }

    if (!os_fsync_file(checkpoint->double_write.handle)) {
        char err_info[CM_ERR_MSG_MAX_LEN];
        os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
        LOGGER_ERROR(LOGGER, LOG_MODULE_CHECKPOINT,
            "checkpoint_double_write: fail to sync file, name = %s error = %s",
            checkpoint->double_write.name, err_info);
        goto err_exit;
    }

    //
    os_aio_context_free_slot(aio_slot);
    os_aio_array_free_context(aio_ctx);

    return ret;

err_exit:

    LOGGER_FATAL(LOGGER, LOG_MODULE_CHECKPOINT, "checkpoint_double_write: fatal error occurred, service exited");
    ut_error;

    return FALSE;
}

static status_t checkpoint_write_and_sync_pages(checkpoint_t* checkpoint)
{
    uint32 ckpt_io_capacity;
    uint32 begin;
    uint32 end;
    timeval_t tv_begin, tv_end;

    ckpt_io_capacity = checkpoint_adjust_io_capacity(checkpoint);

    begin = 0;
    while (begin < checkpoint->group.item_count) {
        end = ut_min(begin + ckpt_io_capacity, checkpoint->group.item_count);

        (void)cm_gettimeofday(&tv_begin);

        // write data file
        if (checkpoint_write_pages(checkpoint, begin, end) != CM_SUCCESS) {
            return CM_ERROR;
        }

        (void)cm_gettimeofday(&tv_end);
        checkpoint->stat.disk_writes += end - begin;
        checkpoint->stat.disk_write_time += (uint64)TIMEVAL_DIFF_US(&tv_begin, &tv_end);

        checkpoint_delay(checkpoint, ckpt_io_capacity);

        begin  = end;
    }

    // sync data file
    if (checkpoint_sync_pages(checkpoint, begin, end) != CM_SUCCESS) {
        return CM_ERROR;
    }

    return CM_SUCCESS;
}

static status_t checkpoint_perform(checkpoint_t* checkpoint)
{
    status_t err;
    lsn_t least_recovery_point;

    // reset
    uint32 pre_item_count = 0;
    checkpoint->group.item_count = 0;

    // 1 get checkpoint lsn
    //   Search twice to ensure the minimum value for checkpoint->least_recovery_point
    lsn_t write_to_buf_lsn = log_get_writed_to_buffer_lsn();
    lsn_t flushed_to_disk_lsn = log_get_flushed_to_disk_lsn();

retry_more:

    // double get
    buf_pool_get_recovery_lsn();
    least_recovery_point = buf_pool_get_recovery_lsn();
    if (least_recovery_point == 0 && checkpoint->group.item_count == 0) {
        if (srv_recovery_on) {
            return CM_SUCCESS;
        }
        if (write_to_buf_lsn != flushed_to_disk_lsn) {
            return CM_SUCCESS;
        }
        // Long time no write operation, refresh the last CHECK POINT
        if (checkpoint->least_recovery_point < flushed_to_disk_lsn + 1) {
            log_checkpoint(flushed_to_disk_lsn + 1);
            checkpoint->least_recovery_point = flushed_to_disk_lsn + 1;
            LOGGER_INFO(LOGGER, LOG_MODULE_CHECKPOINT,
                "checkpoint: set checkpoint point, least_recovery_point=%llu",
                checkpoint->least_recovery_point);
        }
        return CM_SUCCESS;
    }

    if (least_recovery_point != 0) {
        LOGGER_DEBUG(LOGGER, LOG_MODULE_CHECKPOINT,
            "checkpoint: starting, recovery_lsn from %llu to %llu",
            checkpoint->least_recovery_point, least_recovery_point);
    }

    // 2 flush redo log to update lrp point.
    log_write_up_to(least_recovery_point);

    // 3 write and sync pages
    while (TRUE) {
        // 3.1 copy all dirty pages to dirty pages list of checkpoint for all buffer pool
        err = checkpoint_copy_dirty_pages(checkpoint, least_recovery_point);
        CM_RETURN_IF_ERROR(err);

        if (checkpoint->group.item_count == 0) {
            // all dirty pages have been already writed and synchronized
            break;
        }

        if (pre_item_count != checkpoint->group.item_count &&
            checkpoint->group.item_count < CHECKPOINT_GROUP_MAX_SIZE) {
            // get more pages
            pre_item_count = checkpoint->group.item_count;
            goto retry_more;
        }
        
        // 3.2 double write pages to be flushed if need.
        if (checkpoint->enable_double_write && !checkpoint_double_write(checkpoint)) {
            return CM_ERROR;
        }

        // 3.3 sort pages by space id and page no
        err = checkpoint_sort_pages(checkpoint);
        CM_RETURN_IF_ERROR(err);

        // 3.4 write and sync pages to disk
        err = checkpoint_write_and_sync_pages(checkpoint);
        CM_RETURN_IF_ERROR(err);

        // reset
        pre_item_count = 0;
        checkpoint->group.item_count = 0;
    }

    // 3. save checkpoint info
    if (!srv_recovery_on) {
        log_checkpoint(least_recovery_point);
        checkpoint->least_recovery_point = least_recovery_point;
    }

    return CM_SUCCESS;
}

void* checkpoint_proc_thread(void *arg)
{
    checkpoint_t* checkpoint = &g_checkpoint;
    uint64 signal_count = 0;
    uint32 timeout_microseconds = 1000000;

    LOGGER_INFO(LOGGER, LOG_MODULE_CHECKPOINT,"checkpoint thread starting ...");

    while (srv_shutdown_state != SHUTDOWN_EXIT_THREADS) {
        if (checkpoint_perform(checkpoint) != CM_SUCCESS) {
            LOGGER_FATAL(LOGGER, LOG_MODULE_CHECKPOINT, "checkpoint: fatal error occurred, service exited");
            ut_error;
        }

        // wake up by checkpoint thread
        os_event_wait_time(checkpoint->checkpoint_event, timeout_microseconds, signal_count);
        signal_count = os_event_reset(checkpoint->checkpoint_event);
    }

    return NULL;
}

inline void checkpoint_wake_up_thread()
{
    checkpoint_t* checkpoint = &g_checkpoint;

    os_event_set(checkpoint->checkpoint_event);
}

