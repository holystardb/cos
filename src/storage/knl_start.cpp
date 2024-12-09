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


status_t server_read_control_file()
{
    char      file_name[CM_FILE_PATH_BUF_SIZE];
    db_ctrl_t ctrl_file[3] = {0};
    uint64    version = 0, version_num = 0, idx = 0xFF;

    for (uint32 i = 0; i < 3; i++) {
        sprintf_s(file_name, CM_FILE_PATH_MAX_LEN, "%s%c%s%d", srv_data_home, SRV_PATH_SEPARATOR, "ctrl", i+1);
        if (read_ctrl_file(file_name, &ctrl_file[i]) != CM_SUCCESS) {
            ctrl_file[i].version = 0;
            ctrl_file[i].ver_num = 0;
            continue;
        }

        if (version_num < ctrl_file[i].ver_num) {
            version_num = ctrl_file[i].ver_num;
            idx = i;
        }
    }

    if (idx != 0xFF) {
        srv_ctrl_file = ctrl_file[idx];
    }

    return idx == 0xFF ? CM_ERROR : CM_SUCCESS;
}



status_t read_write_aio_init()
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

status_t read_write_threads_startup()
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


status_t server_create_data_files()
{
    if (!srv_create_ctrl_files()) {
        return CM_ERROR;
    }
    if (srv_create_redo_log_files() != CM_SUCCESS) {
        return CM_ERROR;
    }
    if (srv_create_undo_log_files() != CM_SUCCESS) {
        return CM_ERROR;
    }
    if (srv_create_double_write_file() != CM_SUCCESS) {
        return CM_ERROR;
    }
    if (srv_create_system_file() != CM_SUCCESS) {
        return CM_ERROR;
    }
    if (srv_create_systrans_file() != CM_SUCCESS) {
        return CM_ERROR;
    }
    if (srv_create_user_data_files() != CM_SUCCESS) {
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
    uint32 max_page_count;

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

    max_page_count = (uint32)(attr->common_memory_cache_size / page_size_16K);
    srv_common_mpool = mpool_create(srv_memory_sga, 0, max_page_count, UINT32_UNDEFINED, page_size_16K);
    if (srv_common_mpool == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_STARTUP, "Failed to create common memory pool");
        return CM_ERROR;
    }

    initial_page_count = SIZE_M(1) / page_size_16K;
    max_page_count = (uint32)(attr->mtr_memory_cache_size / page_size_16K);
    srv_mtr_memory_pool = mpool_create(srv_memory_sga, initial_page_count, max_page_count, UINT32_UNDEFINED, page_size_16K);
    if (srv_mtr_memory_pool == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_STARTUP, "Failed to create mtr memory pool");
        return CM_ERROR;
    }

    max_page_count = (uint32)(attr->dictionary_memory_cache_size / page_size_16K);
    srv_dictionary_mem_pool = mpool_create(srv_memory_sga, 0, max_page_count, max_page_count, page_size_16K);
    if (srv_dictionary_mem_pool == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_STARTUP, "Failed to create dictionary memory pool");
        return CM_ERROR;
    }

    max_page_count = (uint32)(attr->plan_memory_cache_size / page_size_16K);
    srv_plan_mem_pool = mpool_create(srv_memory_sga, 0, max_page_count, max_page_count, page_size_16K);
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

    // system tablespace: Filespace Header/Extent Descriptor
    ut_a(srv_ctrl_file.system.size >= 16 * 1024 * FSP_DYNAMIC_FIRST_PAGE_NO);
    err = fsp_init_space(FIL_SYSTEM_SPACE_ID, srv_ctrl_file.system.size, srv_ctrl_file.system.max_size, 0);
    CM_RETURN_IF_ERROR(err);

    err = fsp_reserve_system_space();
    CM_RETURN_IF_ERROR(err);

    // undo tablespace
    for (uint32 i = 0; i < srv_ctrl_file.undo_count; i++) {
        ut_a(srv_ctrl_file.undo_group[i].size >= 4 * 1024 * 1024);
        err = undo_fsm_tablespace_init(FIL_UNDO_START_SPACE_ID + i,
            srv_ctrl_file.undo_group[i].size, srv_ctrl_file.undo_group[i].max_size);
        CM_RETURN_IF_ERROR(err);
    }

    return CM_SUCCESS;
}

static status_t server_open_table_spaces()
{

    return CM_SUCCESS;
}

static uint64 get_undo_segment_size()
{
    uint64 size = 0;

    for (uint32 i = 0; i < srv_ctrl_file.undo_count; i++) {
        size += srv_ctrl_file.undo_group[i].max_size;
    }

    return size;
}

static status_t server_open_tablespace_data_files()
{
    LOGGER_INFO(LOGGER, LOG_MODULE_STARTUP, "open data files of table space");

    // system tablespace
    fil_space_t* system_space = fil_space_create("system", FIL_SYSTEM_SPACE_ID, 0);
    if (system_space == NULL) {
        return CM_ERROR;
    }
    fil_node_t *node = fil_node_create(system_space, 0, srv_ctrl_file.system.name,
        (uint32)(srv_ctrl_file.system.max_size / UNIV_PAGE_SIZE), UNIV_PAGE_SIZE, srv_ctrl_file.system.autoextend);
    if (!node) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_STARTUP,
            "Failed to create fil_node for system tablespace, name %s", srv_ctrl_file.system.name);
        return CM_ERROR;
    }

    // systrans tablespace
    fil_space_t* trans_space = fil_space_create("systrans", FIL_SYSTRANS_SPACE_ID, 0);
    if (trans_space == NULL) {
        return CM_ERROR;
    }
    node = fil_node_create(trans_space, DB_SYSTRANS_FILNODE_ID, srv_ctrl_file.systrans.name,
        (uint32)(srv_ctrl_file.systrans.max_size / UNIV_SYSTRANS_PAGE_SIZE), UNIV_SYSTRANS_PAGE_SIZE,
        srv_ctrl_file.systrans.autoextend);
    if (!node) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_STARTUP,
            "Failed to create fil_node for systrans tablespace, name %s", srv_ctrl_file.systrans.name);
        return CM_ERROR;
    }

    // double write tablespace
    fil_space_t* dbwr_space = fil_space_create("dbwr", FIL_DBWR_SPACE_ID, 0);
    if (dbwr_space == NULL) {
        return CM_ERROR;
    }
    node = fil_node_create(dbwr_space, DB_DBWR_FILNODE_ID, srv_ctrl_file.dbwr.name,
        (uint32)(srv_ctrl_file.dbwr.max_size / UNIV_PAGE_SIZE), UNIV_PAGE_SIZE, srv_ctrl_file.dbwr.autoextend);
    if (!node) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_STARTUP,
            "Failed to create fil_node for double write tablespace, name %s", srv_ctrl_file.dbwr.name);
        return CM_ERROR;
    }

    // redo log files
    for (uint32 i = 0; i < srv_ctrl_file.redo_count; i++) {
        bool32 ret = log_group_add(srv_ctrl_file.redo_group[i].name, srv_ctrl_file.redo_group[i].size);
        if (!ret) {
            return CM_ERROR;
        }
    }

    // undo tablespace
    for (uint32 i = 0; i < srv_ctrl_file.undo_count; i++) {
        char undo_space_name[DB_OBJECT_NAME_MAX_LEN];
        sprintf_s(undo_space_name, DB_OBJECT_NAME_MAX_LEN, "undo%02u", i);
        db_data_file_t* data_file = &srv_ctrl_file.undo_group[i];
        fil_space_t* space = fil_space_create(undo_space_name, data_file->space_id, 0);
        if (space == NULL) {
            return CM_ERROR;
        }
        fil_node_t *node = fil_node_create(space, data_file->node_id, data_file->name,
            (uint32)(data_file->max_size / UNIV_PAGE_SIZE), UNIV_PAGE_SIZE, data_file->autoextend);
        if (node == NULL) {
            return CM_ERROR;
        }
    }

    // user tablespace
    for (uint32 i = 0; i < srv_ctrl_file.user_space_count; i++) {
       db_space_t* space = &srv_ctrl_file.user_spaces[i];
        if (fil_space_create(space->name, space->space_id, 0) == NULL) {
            return CM_ERROR;
        }
    }

    // file for user tablespace
    for (uint32 i = 0; i < srv_ctrl_file.user_space_data_file_count; i++) {
        db_data_file_t* data_file = &srv_ctrl_file.user_space_data_files[i];
        fil_space_t* space = fil_system_get_space_by_id(data_file->space_id);
        if (space == NULL) {
            return CM_ERROR;
        }
        fil_node_t *node = fil_node_create(space, data_file->node_id, data_file->name,
            (uint32)(data_file->max_size / UNIV_PAGE_SIZE), UNIV_PAGE_SIZE, data_file->autoextend);
        if (node == NULL) {
            return CM_ERROR;
        }
    }

    return CM_SUCCESS;
}

status_t server_open_or_create_database(char* base_dir, attribute_t* attr)
{
    mtr_t   mtr;
    status_t err;

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

    // Creates a session system at a database start
    //sess_sys_init_at_db_start();


    if (!is_create_new_db) {
        /* If we are using the doublewrite method, we will
           check if there are half-written pages in data files,
           and restore them from the doublewrite buffer if possible */
        //trx_sys_doublewrite_restore_corrupt_pages();
    }

    // temp file
    err = srv_create_temp_files();
    CM_RETURN_IF_ERROR(err);

    // create file node for data files
    err = server_open_tablespace_data_files();
    CM_RETURN_IF_ERROR(err);

    // checkpoint init
    err = checkpoint_init(srv_ctrl_file.dbwr.name, (uint32)srv_ctrl_file.dbwr.max_size);
    CM_RETURN_IF_ERROR(err);
    // Create checkpoint thread
    err = checkpoint_thread_startup();
    CM_RETURN_IF_ERROR(err);

    //
    if (is_create_new_db) {
        err = server_create_table_spaces();
        CM_RETURN_IF_ERROR(err);

        err = trx_sys_init_at_db_start(srv_common_mpool, srv_ctrl_file.undo_count, is_create_new_db);
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

        // Create dict_sys and hash tables, load basic system table
        ut_ad(srv_dictionary_mem_pool);
        err = dict_boot(srv_dictionary_mem_pool,
                        attr->attr_memory.dictionary_memory_cache_size,
                        attr->attr_memory.table_hash_array_size);
        CM_RETURN_IF_ERROR(err);

        // The following needs trx lists which are initialized in trx_sys_init_at_db_start
        //srv_startup_is_before_trx_rollback_phase = FALSE;
        err = trx_sys_init_at_db_start(srv_common_mpool, srv_ctrl_file.undo_count, is_create_new_db);
        CM_RETURN_IF_ERROR(err);

        err = recovery_from_checkpoint_finish(recv_sys);
        CM_RETURN_IF_ERROR(err);
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

