#include "cm_log.h"
#include "knl_start.h"
#include "knl_server.h"
#include "knl_buf.h"
#include "knl_redo.h"
#include "knl_dict.h"
#include "knl_fsp.h"
#include "knl_trx.h"
#include "knl_recovery.h"
#include "knl_trx_rseg.h"
#include "knl_checkpoint.h"
#include "knl_undo_fsm.h"

#define SRV_MAX_READ_IO_THREADS    32
#define SRV_MAX_WRITE_IO_THREADS   32

/** io_handler_thread parameters for thread identification */
static uint32         read_thread_idents[SRV_MAX_READ_IO_THREADS];
static os_thread_t    read_threads[SRV_MAX_READ_IO_THREADS];
static os_thread_id_t read_thread_ids[SRV_MAX_READ_IO_THREADS];
static uint32         write_thread_idents[SRV_MAX_WRITE_IO_THREADS];
static os_thread_t    write_threads[SRV_MAX_WRITE_IO_THREADS];
static os_thread_id_t write_thread_ids[SRV_MAX_WRITE_IO_THREADS];

static os_thread_t    checkpoint_thread;
static os_thread_id_t checkpoint_thread_id;
static os_thread_t    buf_LRU_free_block_thread;
static os_thread_id_t buf_LRU_free_block_thread_id;

status_t server_read_control_file()
{
    const uint32 ctrl_file_count = 3;
    char file_name[CM_FILE_PATH_BUF_SIZE];
    db_ctrl_t* ctrl_file[3];
    uint64 version = 0, version_num = 0, ctrl_file_idx = ctrl_file_count;

    ctrl_file[0] = new db_ctrl_t();
    ctrl_file[1] = new db_ctrl_t();
    ctrl_file[2] = new db_ctrl_t();

    for (uint32 i = 0; i < ctrl_file_count; i++) {
        sprintf_s(file_name, CM_FILE_PATH_MAX_LEN, "%s%c%s%d", srv_data_home, SRV_PATH_SEPARATOR, "ctrl", i+1);
        if (read_ctrl_file(file_name, ctrl_file[i]) != CM_SUCCESS) {
            ctrl_file[i]->version = 0;
            ctrl_file[i]->ver_num = 0;
            continue;
        }

        if (version_num < ctrl_file[i]->ver_num) {
            version_num = ctrl_file[i]->ver_num;
            ctrl_file_idx = i;
        }
    }

    for (uint32 i = 0; i < ctrl_file_count; i++) {
        if (i == ctrl_file_idx) {
            srv_ctrl_file = ctrl_file[ctrl_file_idx];
        } else {
            delete ctrl_file[i];
        }
    }

    return ctrl_file_idx == ctrl_file_count ? CM_ERROR : CM_SUCCESS;
}

static status_t read_write_aio_init()
{
    LOGGER_INFO(LOGGER, LOG_MODULE_STARTUP, "aio of read and write is initializing ...");

    uint32 io_limit = OS_AIO_N_PENDING_IOS_PER_THREAD;
#ifndef __WIN__
    io_limit = 8 * OS_AIO_N_PENDING_IOS_PER_THREAD;
#endif

    srv_os_aio_async_read_array = os_aio_array_create(io_limit, srv_read_io_threads);
    if (srv_os_aio_async_read_array == NULL) {
        LOGGER_FATAL(LOGGER, LOG_MODULE_STARTUP, "FATAL for create aio read_array");
        return CM_ERROR;
    }
    srv_os_aio_async_write_array = os_aio_array_create(io_limit, srv_write_io_threads);
    if (srv_os_aio_async_write_array == NULL) {
        LOGGER_FATAL(LOGGER, LOG_MODULE_STARTUP, "FATAL for create aio write_array");
        return CM_ERROR;
    }
    srv_os_aio_sync_array = os_aio_array_create(io_limit, srv_sync_io_contexts);
    if (srv_os_aio_sync_array == NULL) {
        LOGGER_FATAL(LOGGER, LOG_MODULE_STARTUP, "FATAL for create aio sync array");
        return CM_ERROR;
    }

    LOGGER_INFO(LOGGER, LOG_MODULE_STARTUP, "aio of read and write initialized");

    return CM_SUCCESS;
}


static void* write_io_handler_thread(void *arg)
{
    os_aio_context_t* context;
    uint32 index = *(uint32 *)arg;

    ut_ad(index < srv_write_io_threads);
    LOGGER_INFO(LOGGER, LOG_MODULE_STARTUP, "write io thread (id = %lu) starting ...", index);

    context = os_aio_array_get_nth_context(srv_os_aio_async_write_array, index);

    while (srv_shutdown_state != SHUTDOWN_EXIT_THREADS) {
        fil_aio_reader_and_writer_wait(context);
    }

    os_thread_exit(NULL);
    OS_THREAD_DUMMY_RETURN;
}

static void* read_io_handler_thread(void *arg)
{
    os_aio_context_t* context;
    uint32 index = *(uint32 *)arg;

    LOGGER_INFO(LOGGER, LOG_MODULE_STARTUP, "read io thread (id = %lu) starting ...", index);
    ut_ad(index < srv_read_io_threads);

    context = os_aio_array_get_nth_context(srv_os_aio_async_read_array, index);;

    while (srv_shutdown_state != SHUTDOWN_EXIT_THREADS) {
        fil_aio_reader_and_writer_wait(context);
    }

    os_thread_exit(NULL);
    OS_THREAD_DUMMY_RETURN;
}

static status_t read_write_threads_startup()
{
    LOGGER_INFO(LOGGER, LOG_MODULE_STARTUP, "create io threads: read threads = %u, write threads = %u",
        srv_read_io_threads, srv_write_io_threads);

    /* Create i/o-handler threads: */
    for (uint32 i = 0; i < srv_read_io_threads && i < SRV_MAX_READ_IO_THREADS; ++i) {
        read_thread_idents[i] = i;
        read_threads[i] = os_thread_create(read_io_handler_thread, &read_thread_idents[i], &read_thread_ids[i]);
    }
    for (uint32 i = 0; i < srv_write_io_threads && i < SRV_MAX_WRITE_IO_THREADS; ++i) {
        write_thread_idents[i] = i;
        write_threads[i] = os_thread_create(write_io_handler_thread, &write_thread_idents[i], &write_thread_ids[i]);
    }

    return CM_SUCCESS;
}

status_t checkpoint_thread_startup()
{
    checkpoint_thread = os_thread_create(checkpoint_proc_thread, NULL, &checkpoint_thread_id);
    return CM_SUCCESS;
}

status_t buf_LRU_scan_and_free_block_thread_startup()
{
    buf_LRU_free_block_thread = os_thread_create(buf_LRU_scan_and_free_block_thread, NULL, &buf_LRU_free_block_thread_id);
    return CM_SUCCESS;
}

status_t server_create_data_files()
{
    if (srv_create_ctrl_files() != CM_SUCCESS) {
        return CM_ERROR;
    }
    if (srv_create_data_files_at_createdatabase() != CM_SUCCESS) {
        return CM_ERROR;
    }

    return CM_SUCCESS;
}

status_t memory_pool_create(attr_memory_t* attr)
{
    uint32 page_size_128K = SIZE_K(128);
    uint32 page_size_64K = SIZE_K(64);
    uint32 page_size_32K = SIZE_K(32);
    uint32 page_size_16K = SIZE_K(16);
    uint32 initial_page_count;
    uint32 lower_page_count;

    LOGGER_INFO(LOGGER, LOG_MODULE_STARTUP, "memory pool is initializing ...");

    uint64 total_memory_size =
        attr->common_memory_cache_size +
        attr->temp_memory_cache_size +
        attr->dictionary_memory_cache_size +
        attr->plan_memory_cache_size;
    srv_memory_sga = marea_create(total_memory_size, FALSE);
    if (srv_memory_sga == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_STARTUP, "Failed to create memory area, size %llu", total_memory_size);
        return CM_ERROR;
    }

    lower_page_count = (uint32)(attr->common_memory_cache_size / page_size_16K);
    initial_page_count = 0;
    srv_common_mpool = mpool_create(srv_memory_sga, "common_memory_pool",
        attr->common_memory_cache_size + attr->common_ext_memory_cache_size,
        page_size_16K, initial_page_count, lower_page_count);
    if (srv_common_mpool == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_STARTUP, "Failed to create common memory pool");
        return CM_ERROR;
    }

    initial_page_count = SIZE_M(1) / page_size_16K;
    if (attr->mtr_memory_cache_size < SIZE_M(1)) {
        attr->mtr_memory_cache_size = SIZE_M(1);
    }
    lower_page_count = (uint32)(attr->mtr_memory_cache_size / page_size_16K);
    srv_mtr_memory_pool = mpool_create(srv_memory_sga, "mtr_memory_pool",
        attr->mtr_memory_cache_size, page_size_16K, initial_page_count, lower_page_count);
    if (srv_mtr_memory_pool == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_STARTUP, "Failed to create mtr memory pool");
        return CM_ERROR;
    }

    initial_page_count = 0;
    lower_page_count = (uint32)(attr->dictionary_memory_cache_size / page_size_16K);
    srv_dictionary_mem_pool = mpool_create(srv_memory_sga, "dictionary_memory_pool",
        attr->dictionary_memory_cache_size, page_size_16K, initial_page_count, lower_page_count);
    if (srv_dictionary_mem_pool == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_STARTUP, "Failed to create dictionary memory pool");
        return CM_ERROR;
    }

    initial_page_count = 0;
    lower_page_count = (uint32)(attr->plan_memory_cache_size / page_size_16K);
    srv_plan_mem_pool = mpool_create(srv_memory_sga, "plan_memory_pool",
        attr->plan_memory_cache_size, page_size_16K, initial_page_count, lower_page_count);
    if (srv_plan_mem_pool == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_STARTUP, "Failed to create plan memory pool");
        return CM_ERROR;
    }

    srv_temp_mem_pool = vm_pool_create(attr->temp_memory_cache_size, page_size_128K);
    if (srv_temp_mem_pool == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_STARTUP, "Failed to create temporary memory pool");
        return CM_ERROR;
    }

    LOGGER_INFO(LOGGER, LOG_MODULE_STARTUP, "memory pool initialized");

    return CM_SUCCESS;
}



status_t server_config_init(char* base_dir, attribute_t* attr)
{
    uint32 base_dir_len = (uint32)strlen(base_dir);

    if (base_dir_len + 5 > srv_data_home_len) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_STARTUP,
            "length of base_dir %s exceeds the limit %lu",
            base_dir, srv_data_home_len);
        return CM_ERROR;
    }

    if (base_dir[base_dir_len - 1] != SRV_PATH_SEPARATOR) {
        sprintf_s(srv_data_home, 1023, "%s%c%s", base_dir, SRV_PATH_SEPARATOR, "data");
    } else {
        sprintf_s(srv_data_home, 1023, "%s%s", base_dir, "data");
    }

    return CM_SUCCESS;
}

static status_t server_is_create_new_db(bool32* is_create_new_db)
{
    char       file_name[CM_FILE_PATH_BUF_SIZE];
    os_file_t  file;
    status_t   ret = CM_SUCCESS;

    sprintf_s(file_name, CM_FILE_PATH_MAX_LEN, "%s%c%s%d", srv_data_home, SRV_PATH_SEPARATOR, "ctrl", 1);
    if (os_open_file(file_name, OS_FILE_OPEN, 0, &file) == FALSE) {
        uint32 err = os_file_get_last_error();
        if (err == OS_FILE_NOT_FOUND || err == OS_FILE_PATH_NOT_FOUND) {
            *is_create_new_db = TRUE;
        } else {
            ret = CM_ERROR;
        }
    } else {
        os_close_file(file);
        *is_create_new_db = FALSE;
    }

    return ret;
}

static status_t server_create_file_system(uint32 open_files_limit)
{
    bool32 ret;

    LOGGER_INFO(LOGGER, LOG_MODULE_STARTUP, "fil_system is initializing ...");

    // file system
    ret = fil_system_init(srv_common_mpool, open_files_limit);
    if (!ret) {
        return CM_ERROR;
    }

    return CM_SUCCESS;
}

static status_t server_create_table_spaces()
{
    status_t err;
    db_space_t* db_space;
    db_data_file_t* data_file;

    // system tablespace: Filespace Header/Extent Descriptor
    data_file = srv_ctrl_file->get_data_file_by_node_id(DB_SYSTEM_FILNODE_ID);
    err = fsp_init_space(FIL_SYSTEM_SPACE_ID, data_file->size, data_file->max_size, 0);
    CM_RETURN_IF_ERROR(err);
    err = fsp_reserve_system_space();
    CM_RETURN_IF_ERROR(err);

    // systrans
    data_file = srv_ctrl_file->get_data_file_by_node_id(DB_SYSTRANS_FILNODE_ID);
    uint64 rseg_count = data_file->max_size;
    ut_a(rseg_count >= TRX_RSEG_MIN_COUNT && rseg_count <= TRX_RSEG_MAX_COUNT);
    uint64 size = (FSP_FIRST_RSEG_PAGE_NO + rseg_count * TRX_SLOT_PAGE_COUNT_PER_RSEG) * UNIV_SYSTRANS_PAGE_SIZE;
    ut_a(data_file->size == size);
    err = fsp_init_space(FIL_SYSTRANS_SPACE_ID, data_file->size, data_file->size, 0);
    CM_RETURN_IF_ERROR(err);

    // undo tablespace
    for (uint32 i = 0; i < DB_UNDO_SPACE_MAX_COUNT; i++) {
        data_file = srv_ctrl_file->get_data_file_by_node_id(DB_UNDO_START_FILNODE_ID + i);
        if (data_file->node_id == DB_DATA_FILNODE_INVALID_ID) {
            continue;
        }
        ut_a(data_file->size >= 4 * 1024 * 1024);
        err = undo_fsm_tablespace_init(data_file->space_id, data_file->size, data_file->max_size);
        CM_RETURN_IF_ERROR(err);
    }

    // user tablespace
    for (uint32 i = 0; i < DB_USER_SPACE_MAX_COUNT; i++) {
        db_space = srv_ctrl_file->get_user_space_by_space_id(DB_USER_SPACE_FIRST_ID + i);
        if (db_space == NULL || db_space->space_id == DB_SPACE_INVALID_ID) {
            continue;
        }
        err = fsp_init_space(db_space->space_id, db_space->size, db_space->max_size, 0);
        CM_RETURN_IF_ERROR(err);
    }

    return CM_SUCCESS;
}

static status_t server_open_table_spaces()
{
    status_t err;

    // system tablespace
    err = fsp_open_space(FIL_SYSTEM_SPACE_ID);
    CM_RETURN_IF_ERROR(err);

    // systrans tablespace
    err = fsp_open_space(FIL_SYSTRANS_SPACE_ID);
    CM_RETURN_IF_ERROR(err);

    // dbwr tablespace
    //err = fsp_open_space(FIL_DBWR_SPACE_ID);
    //CM_RETURN_IF_ERROR(err);

    // undo tablespace
    for (uint32 i = 0; i < DB_UNDO_SPACE_MAX_COUNT; i++) {
        db_space_t* db_space = srv_ctrl_file->get_system_space_by_space_id(DB_UNDO_START_SPACE_ID + i);
        if (db_space->space_id == DB_SPACE_INVALID_ID) {
            continue;
        }
        if (UT_LIST_GET_LEN(db_space->data_file_list) == 0 && db_space->size == 0) {
            continue;
        }
        err = fsp_open_space(db_space->space_id);
        CM_RETURN_IF_ERROR(err);
    }

    // user tablespace
    for (uint32 i = 0; i < DB_USER_SPACE_MAX_COUNT; i++) {
        db_space_t* db_space = srv_ctrl_file->get_user_space_by_space_id(DB_USER_SPACE_FIRST_ID + i);
        if (db_space == NULL || db_space->space_id == DB_SPACE_INVALID_ID) {
            continue;
        }
        err = fsp_open_space(db_space->space_id);
        CM_RETURN_IF_ERROR(err);
    }

    return CM_SUCCESS;
}

static status_t server_open_tablespace_data_files()
{
    LOGGER_INFO(LOGGER, LOG_MODULE_STARTUP, "open data files of table space");

    db_data_file_t* data_file;
    db_space_t* db_space;
    fil_node_t* fil_node;
    fil_space_t* fil_space;


    // system tablespace
    fil_space = fil_space_create("system", FIL_SYSTEM_SPACE_ID, 0);
    if (fil_space == NULL) {
        return CM_ERROR;
    }
    data_file = srv_ctrl_file->get_data_file_by_node_id(DB_SYSTEM_FILNODE_ID);
    if (data_file == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_STARTUP, "Failed to find data file for system tablespace");
        return CM_ERROR;
    }
    fil_node = fil_node_create(fil_space, data_file->node_id, data_file->file_name,
        (uint32)(data_file->max_size / UNIV_PAGE_SIZE), UNIV_PAGE_SIZE, data_file->autoextend);
    if (!fil_node) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_STARTUP,
            "Failed to create fil_node for system tablespace, name %s", data_file->file_name);
        return CM_ERROR;
    }

    // systrans tablespace
    fil_space = fil_space_create("systrans", FIL_SYSTRANS_SPACE_ID, 0);
    if (fil_space == NULL) {
        return CM_ERROR;
    }
    data_file = srv_ctrl_file->get_data_file_by_node_id(DB_SYSTRANS_FILNODE_ID);
    if (data_file == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_STARTUP, "Failed to find data file for systrans tablespace");
        return CM_ERROR;
    }
    fil_node = fil_node_create(fil_space, data_file->node_id, data_file->file_name,
        (uint32)(data_file->size / UNIV_SYSTRANS_PAGE_SIZE), UNIV_SYSTRANS_PAGE_SIZE, FALSE);
    if (!fil_node) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_STARTUP,
            "Failed to create fil_node for systrans tablespace, name %s", data_file->file_name);
        return CM_ERROR;
    }

    // double write tablespace
    fil_space = fil_space_create("dbwr", FIL_DBWR_SPACE_ID, 0);
    if (fil_space == NULL) {
        return CM_ERROR;
    }
    data_file = srv_ctrl_file->get_data_file_by_node_id(DB_DBWR_FILNODE_ID);
    if (data_file == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_STARTUP, "Failed to find data file for dbwr tablespace");
        return CM_ERROR;
    }
    fil_node = fil_node_create(fil_space, data_file->node_id, data_file->file_name,
        (uint32)(data_file->max_size / UNIV_PAGE_SIZE), UNIV_PAGE_SIZE, data_file->autoextend);
    if (!fil_node) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_STARTUP,
            "Failed to create fil_node for dbwr tablespace, name %s", data_file->file_name);
        return CM_ERROR;
    }

    // redo log files
    db_space = srv_ctrl_file->get_system_space_by_space_id(DB_REDO_SPACE_ID);
    if (UT_LIST_GET_LEN(db_space->data_file_list) == 0) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_STARTUP,
            "Failed to find data file for redo tablespace %s", db_space->space_name);
        return CM_ERROR;
    }
    data_file = UT_LIST_GET_FIRST(db_space->data_file_list);
    while (data_file) {
        CM_RETURN_IF_ERROR( log_group_add(data_file->file_name, data_file->size));
        data_file = UT_LIST_GET_NEXT(list_node, data_file);
    }

    // undo tablespace
    for (uint32 i = 0; i < DB_UNDO_SPACE_MAX_COUNT; i++) {
        db_space = srv_ctrl_file->get_system_space_by_space_id(DB_UNDO_START_SPACE_ID + i);
        if (db_space->space_id == DB_SPACE_INVALID_ID) {
            continue;
        }
        if (UT_LIST_GET_LEN(db_space->data_file_list) == 0 && db_space->size == 0) {
            continue;
        }
        data_file = UT_LIST_GET_FIRST(db_space->data_file_list);
        if (data_file == NULL) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_STARTUP, "Failed to find data file for undo tablespace");
            return CM_ERROR;
        }

        fil_space = fil_space_create(db_space->space_name, db_space->space_id, 0);
        if (fil_space == NULL) {
            return CM_ERROR;
        }
        fil_node = fil_node_create(fil_space, data_file->node_id, data_file->file_name,
            (uint32)(data_file->max_size / UNIV_PAGE_SIZE), UNIV_PAGE_SIZE, data_file->autoextend);
        if (fil_node == NULL) {
            return CM_ERROR;
        }
    }

    // user tablespace
    for (uint32 i = 0; i < DB_USER_SPACE_MAX_COUNT; i++) {
        db_space = srv_ctrl_file->get_user_space_by_pos(i);
        if (db_space == NULL || db_space->space_id == DB_SPACE_INVALID_ID) {
            continue;
        }

        if (UT_LIST_GET_LEN(db_space->data_file_list) == 0) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_STARTUP,
                "Failed to find data file for user tablespace %s", db_space->space_name);
            return CM_ERROR;
        }

        fil_space = fil_space_create(db_space->space_name, db_space->space_id, 0);
        if (fil_space == NULL) {
            return CM_ERROR;
        }

        data_file = UT_LIST_GET_FIRST(db_space->data_file_list);
        while (data_file) {
            fil_node = fil_node_create(fil_space, data_file->node_id, data_file->file_name,
                (uint32)(data_file->max_size / UNIV_PAGE_SIZE), UNIV_PAGE_SIZE, data_file->autoextend);
            if (fil_node == NULL) {
                return CM_ERROR;
            }

            data_file = UT_LIST_GET_NEXT(list_node, data_file);
        }
    }

    // temp tablespace
    db_space = srv_ctrl_file->get_system_space_by_space_id(DB_TEMP_SPACE_ID);
    if (UT_LIST_GET_LEN(db_space->data_file_list) == 0) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_STARTUP,
            "Failed to find data file for user tablespace %s", db_space->space_name);
        return CM_ERROR;
    }
    data_file = UT_LIST_GET_FIRST(db_space->data_file_list);
    while (data_file) {
        CM_RETURN_IF_ERROR(data_file->delete_data_file());
        CM_RETURN_IF_ERROR(vm_pool_add_file(srv_temp_mem_pool, data_file->file_name, data_file->max_size));
        data_file = UT_LIST_GET_NEXT(list_node, data_file);
    }

    return CM_SUCCESS;
}

status_t server_open_or_create_database(char* base_dir, attribute_t* attr)
{
    mtr_t   mtr;
    status_t err;
    db_data_file_t* data_file;

    LOGGER_NOTICE(LOGGER, LOG_MODULE_STARTUP, "Service is starting ...");

    err = server_config_init(base_dir, attr);
    CM_RETURN_IF_ERROR(err);

    // Initializes the synchronization data structures
    err = sync_init();
    CM_RETURN_IF_ERROR(err);

    // Initializes the memory pool
    err = memory_pool_create(&attr->attr_memory);
    CM_RETURN_IF_ERROR(err);

    // Initializes the asynchronous io system,
    // Creates separate aio array for read and write.
    err = read_write_aio_init();
    CM_RETURN_IF_ERROR(err);

    // Initializes the file system
    err = server_create_file_system(attr->attr_common.open_files_limit);
    CM_RETURN_IF_ERROR(err);

    // number of locks to protect buf_pool->page_hash
    uint32 page_hash_lock_count = 4096;
    uint64 buffer_pool_size = attr->attr_storage.buffer_pool_size + attr->attr_storage.undo_cache_size;
    err = buf_pool_init(buffer_pool_size, attr->attr_storage.buffer_pool_instances, page_hash_lock_count);
    if (err != CM_SUCCESS) {
        LOGGER_FATAL(LOGGER, LOG_MODULE_STARTUP, "FATAL in initializing data buffer pool.");
        return CM_ERROR;
    }

    // Initializes redo log
    err = log_init((uint32)attr->attr_storage.redo_log_buffer_size);
    CM_RETURN_IF_ERROR(err);

    // mini-transaction initialize
    ut_ad(srv_mtr_memory_pool);
    mtr_init(srv_mtr_memory_pool);

    // Creates or opens database data files
    bool32 is_create_new_db = FALSE;
    err = server_is_create_new_db(&is_create_new_db);
    CM_RETURN_IF_ERROR(err);
    //is_create_new_db = TRUE;

    if (is_create_new_db) {
        LOGGER_NOTICE(LOGGER, LOG_MODULE_STARTUP, "Creating data file");
        err = server_create_data_files();
        CM_RETURN_IF_ERROR(err);
    } else {
        LOGGER_NOTICE(LOGGER, LOG_MODULE_STARTUP, "Reading data file");
        err = server_read_control_file();
        CM_RETURN_IF_ERROR(err);
    }

    // Creates the lock system at database start
    //lock_sys_create(srv_lock_table_size);

    // Create i/o-handler threads
    err = read_write_threads_startup();
    CM_RETURN_IF_ERROR(err);

    if (!is_create_new_db) {
        /* If we are using the doublewrite method, we will
           check if there are half-written pages in data files,
           and restore them from the doublewrite buffer if possible */
        //trx_sys_doublewrite_restore_corrupt_pages();
    }

    // temp file
    //err = srv_create_temp_files();
    //CM_RETURN_IF_ERROR(err);

    // create file node for data files
    err = server_open_tablespace_data_files();
    CM_RETURN_IF_ERROR(err);

    // checkpoint init
    data_file = srv_ctrl_file->get_data_file_by_node_id(DB_DBWR_FILNODE_ID);
    err = checkpoint_init(data_file->file_name, data_file->max_size);
    CM_RETURN_IF_ERROR(err);
    // Create and startup checkpoint thread
    err = checkpoint_thread_startup();
    CM_RETURN_IF_ERROR(err);
    err = buf_LRU_scan_and_free_block_thread_startup();
    CM_RETURN_IF_ERROR(err);

    // Creates trx_sys at a database start
    data_file = srv_ctrl_file->get_data_file_by_node_id(DB_SYSTRANS_FILNODE_ID);
    uint32 rseg_count = (uint32)data_file->max_size;
    err = trx_sys_create(srv_common_mpool, rseg_count, srv_ctrl_file->undo_data_file_count);
    CM_RETURN_IF_ERROR(err);

    //
    if (is_create_new_db) {
        err = server_create_table_spaces();
        CM_RETURN_IF_ERROR(err);

        // Creates transaction slots at a database start
        err = trx_sys_init_at_db_start(is_create_new_db);
        CM_RETURN_IF_ERROR(err);

        // Data Dictionary Header
        err = dict_create();
        CM_RETURN_IF_ERROR(err);

        // Create dict_sys and hash tables, load basic system table
        ut_ad(srv_dictionary_mem_pool);
        err = dict_boot(srv_dictionary_mem_pool,
                        attr->attr_memory.dictionary_memory_cache_size,
                        attr->attr_memory.table_hash_array_size);
        CM_RETURN_IF_ERROR(err);

        //srv_startup_is_before_trx_rollback_phase = FALSE;
    } else if (srv_archive_recovery) {
        LOGGER_NOTICE(LOGGER, LOG_MODULE_STARTUP, "Starting archive recovery from a backup...");
        //err = recv_recovery_from_archive_start(min_flushed_lsn,
        //    srv_archive_recovery_limit_lsn, min_arch_log_no);
        //if (err != DB_SUCCESS) {
        //    return(DB_ERROR);
        //}
        //dict_boot();
        //trx_sys_init_at_db_start();
        //srv_startup_is_before_trx_rollback_phase = FALSE;
        //recv_recovery_from_archive_finish();
    } else {
        // We always try to do a recovery,
        // even if the database had been shut down normally
        LOGGER_NOTICE(LOGGER, LOG_MODULE_STARTUP, "Starting recovery from redo log ...");

        err = server_open_table_spaces();
        CM_RETURN_IF_ERROR(err);

        recovery_sys_t* recv_sys = recovery_init(srv_common_mpool);
        if (recv_sys == NULL) {
            return CM_ERROR;
        }

        err = recovery_from_checkpoint_start(recv_sys);
        CM_RETURN_IF_ERROR(err);

        LOGGER_NOTICE(LOGGER, LOG_MODULE_STARTUP, "recovery done1");

        // Create dict_sys and hash tables, load basic system table
        ut_ad(srv_dictionary_mem_pool);
        err = dict_boot(srv_dictionary_mem_pool,
                        attr->attr_memory.dictionary_memory_cache_size,
                        attr->attr_memory.table_hash_array_size);
        CM_RETURN_IF_ERROR(err);

        // load transaction slots at a database start
        err = trx_sys_init_at_db_start(is_create_new_db);
        CM_RETURN_IF_ERROR(err);
        err = trx_sys_recovery_at_db_start();
        CM_RETURN_IF_ERROR(err);

        err = recovery_from_checkpoint_finish(recv_sys);
        CM_RETURN_IF_ERROR(err);

        //srv_startup_is_before_trx_rollback_phase = FALSE;
        LOGGER_NOTICE(LOGGER, LOG_MODULE_STARTUP, "recovery done");
    }

    // Create the thread which watches the timeouts for lock waits and prints monitor info
    //os_thread_create(&srv_lock_timeout_and_monitor_thread, NULL, thread_ids);	

    //err = dict_create_or_check_foreign_constraint_tables();
    //if (err != CM_SUCCESS) {
    //    return CM_ERROR;
    //}

    // Create the master thread which monitors the database server,
    // and does purge and other utility operations
    //os_thread_create(&srv_master_thread, NULL, thread_ids + 1 + SRV_MAX_N_IO_THREADS);

    if (is_create_new_db) {
        log_checkpoint(LOG_BLOCK_HDR_SIZE);
        // Makes a checkpoint at a given lsn or later
        log_make_checkpoint_at(ut_duint64_max);
    }

    LOGGER_NOTICE(LOGGER, LOG_MODULE_STARTUP,
        "checkpoint: writed_to_buffer_lsn=%llu writed_to_file_lsn=%llu flushed_to_disk_lsn=%llu "
        "checkpoint_no=%llu checkpoint_lsn=%llu",
        log_get_writed_to_buffer_lsn(), log_get_writed_to_file_lsn(), log_get_flushed_to_disk_lsn(),
        log_sys->next_checkpoint_no - 1, log_sys->last_checkpoint_lsn);

    LOGGER_NOTICE(LOGGER, LOG_MODULE_STARTUP, "Service started");

    return CM_SUCCESS;
}

status_t server_shutdown_database()
{
    return CM_SUCCESS;
}

