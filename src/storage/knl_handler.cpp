#include "cm_log.h"
#include "knl_handler.h"
#include "knl_server.h"
#include "knl_buf.h"
#include "knl_log.h"
#include "knl_dict.h"
#include "knl_fsp.h"
#include "knl_trx.h"

// Create undo tablespace.
static dberr_t srv_undo_tablespace_create(
    const char* name, /*!< in: tablespace name */
    uint32 size) /*!< in: tablespace size in pages */
{
}

bool32 knl_open_or_create_data_file(memory_area_t* marea, bool32 *create_new_db)
{
    dberr_t err;

    memory_pool_t* fil_mpool;
    uint32 local_page_count = 8;
    uint32 max_page_count = MEM_POOL_PAGE_UNLIMITED;
    uint32 page_size = 1024 * 8;
    fil_mpool = mpool_create(marea, local_page_count, max_page_count, page_size);

    bool32 ret;
    fil_space_t *system_fil_space;
    fil_node_t  *system_fil_node;
    char        name[2048];
    char       *system_space_name = "system";
    uint32 page_max_count = 1024;
    bool32 is_extend = TRUE;

    *create_new_db = FALSE;

    os_file_t file;
    uint32 dirname_len = (uint32)strlen(srv_data_home);
    if (srv_data_home[dirname_len - 1] != SRV_PATH_SEPARATOR) {
        sprintf_s(name, 2048, "%s%c%s.dat", srv_data_home, SRV_PATH_SEPARATOR, system_space_name);
    } else {
        sprintf_s(name, 2048, "%s%s.dat", srv_data_home, system_space_name);
    }

    //ret = os_open_file(name, OS_FILE_CREATE, 0, &file);
    ret = os_open_file(name, OS_FILE_OPEN, 0, &file);
    
    if (!ret) {
        //return DB_ERROR;
    }
    ret = os_file_extend(name, file, srv_system_file_size);
    if (!ret) {
        LOGGER_FATAL(LOGGER, "Error in creating %s: probably out of disk space", name);
        return DB_ERROR;
    }

    os_close_file(file);

    // file system
    ret = fil_system_init(fil_mpool, srv_max_n_open, srv_space_max_count, srv_fil_node_max_count);
    if (!ret) {
        return DB_ERROR;
    }

    system_fil_space = fil_space_create(system_space_name, 0, 0);
    if (!system_fil_space) {
        return DB_ERROR;
    }

    system_fil_node = fil_node_create(system_fil_space, system_space_name, page_max_count, page_size, is_extend);
    if (!system_fil_node) {
        return DB_ERROR;
    }
    system_fil_node->handle = file;

    *create_new_db = TRUE;

    return DB_SUCCESS;
}


bool32 knl_server_start(memory_area_t* marea)
{
    dberr_t   err;
    db_ctrl_t ctrl;
    bool32    ret;
    char      name[1024];

    for (uint32 i = 1; i <= 3; i++) {
        ret = read_ctrl_file(name, &ctrl);
        if (ret) {
            break;
        }
    }
    if (!ret) {
        LOGGER_FATAL(LOGGER, "Startup failed, control file damaged");
        return FALSE;
    }

    sync_init();
    mtr_init(marea);

    if (!log_init()) {
        return FALSE;
    }
    for (uint32 i = 0; i < ctrl.redo_count; i++) {
        ret = log_group_add(ctrl.redo_group[i].name, ctrl.redo_group[i].size);
        if (!ret) {
            return FALSE;
        }
    }

    // buffer pool
    err = buf_pool_init(srv_buf_pool_size, srv_buf_pool_instances);
    if (err != DB_SUCCESS) {
        LOGGER_FATAL(LOGGER, "FATAL in initializing data buffer pool");
        return DB_ERROR;
    }

    //redo_log_init();

    /* Create i/o-handler threads: */
    for (uint32 i = 0; i < srv_read_io_threads; ++i) {
        //os_thread_create(read_io_handler_thread, n + i, thread_ids + i);
    }
    for (uint32 i = 0; i < srv_write_io_threads; ++i) {
        //os_thread_create(read_io_handler_thread, n + i, thread_ids + i);
    }

    /*
    char *name_prefix = "redo";
    if (data_home[dirname_len - 1] != SRV_PATH_SEPARATOR) {
        sprintf_s(group->name, log_file_name_len,
            "%s%c%s%d", data_home, SRV_PATH_SEPARATOR, name_prefix, i+1);
    } else {
        sprintf_s(group->name, log_file_name_len, "%s%s%d", data_home, name_prefix, i+1);
    }
    */

    // redo buffer

    // undo buffer

    // dictionary table

    //
    bool32 create_new_db;

    ret = knl_open_or_create_data_file(marea, &create_new_db);
    if (!ret) {
        return DB_ERROR;
    }

    if (create_new_db) {
        mtr_t mtr;
        uint64 sum_of_new_sizes = srv_system_file_size;

        mtr_start(&mtr);
        fsp_header_init(0, sum_of_new_sizes, &mtr);
        mtr_commit(&mtr);

    }

    /* Open all log files and data files in the system tablespace:
       we keep them open until database shutdown */
    //fil_open_log_and_system_tablespace_files();

    trx_sys_create();

    err = dict_boot();
    if (err != DB_SUCCESS) {
        return(err);
    }


    //err = create_log_files(create_new_db, logfilename, dirnamelen, max_flushed_lsn, logfile0);


    return DB_SUCCESS;

}

bool32 knl_server_init_db(memory_area_t* marea)
{
    mtr_t   mtr;
    dberr_t err;
    bool32  ret;

    if (!srv_create_ctrls(srv_data_home)) {
        return FALSE;
    }
    if (srv_create_redo_logs() != DB_SUCCESS) {
        return FALSE;
    }
    if (srv_create_undo_log() != DB_SUCCESS) {
        return FALSE;
    }
    if (srv_create_temp() != DB_SUCCESS) {
        return FALSE;
    }
    if (srv_create_system() != DB_SUCCESS) {
        return FALSE;
    }

    sync_init();   
    mtr_init(marea);  // mini-transaction initialize

    if (!log_init()) {  // redo log
        return FALSE;
    }
    for (uint32 i = 0; i < srv_db_ctrl.redo_count; i++) {
        ret = log_group_add(srv_db_ctrl.redo_group[i].name, srv_db_ctrl.redo_group[i].size);
        if (!ret) {
            return FALSE;
        }
    }

    // buffer pool
    err = buf_pool_init(srv_buf_pool_size, srv_buf_pool_instances);
    if (err != DB_SUCCESS) {
        LOGGER_FATAL(LOGGER, "FATAL in initializing data buffer pool.");
        return FALSE;
    }

    memory_pool_t* fil_mpool;
    uint32 local_page_count = 8;
    uint32 max_page_count = MEM_POOL_PAGE_UNLIMITED;
    uint32 page_size = 1024 * 8;
    fil_mpool = mpool_create(marea, local_page_count, max_page_count, page_size);


    // file system
    ret = fil_system_init(fil_mpool, srv_max_n_open, srv_space_max_count, srv_fil_node_max_count);
    if (!ret) {
        return DB_ERROR;
    }

    fil_space_t *system_space = fil_space_create("system", SRV_SYSTEM_SPACE_ID, 0);
    if (!system_space) {
        return DB_ERROR;
    }
    fil_node_t *node = fil_node_create(system_space, srv_db_ctrl.system.name,
        srv_db_ctrl.system.max_size / UNIV_PAGE_SIZE, UNIV_PAGE_SIZE, srv_db_ctrl.system.autoextend);
    if (!node) {
        return DB_ERROR;
    }

    /* Open all log files and data files in the system tablespace:
       we keep them open until database shutdown */
    //fil_open_log_and_system_tablespace_files();

    trx_sys_create();

    mtr_start(&mtr);
    fsp_header_init(system_space, srv_db_ctrl.system.size / UNIV_PAGE_SIZE, &mtr);
    mtr_commit(&mtr);

    trx_sys_create_sys_pages();

    //ib_bh = trx_sys_init_at_db_start();
    //n_recovered_trx = UT_LIST_GET_LEN(trx_sys->rw_trx_list);
    
    /* The purge system needs to create the purge view and
    therefore requires that the trx_sys is inited. */
    //trx_purge_sys_create(srv_n_purge_threads, ib_bh);
    
    err = dict_create();
    if (err != DB_SUCCESS) {
        return(err);
    }
    
    //srv_startup_is_before_trx_rollback_phase = FALSE;
    
    //bool success = buf_flush_list(UINT32_MAX, LSN_MAX, NULL);
    //ut_a(success);





    uint64 lsn = mtr.end_lsn;
    log_write_up_to(lsn);

    /* To maintain backward compatibility we create only
    the first rollback segment before the double write buffer.
    All the remaining rollback segments will be created later,
    after the double write buffer has been created. */
    //trx_sys_create_sys_pages();

    //ib_bh = trx_sys_init_at_db_start();
    //n_recovered_trx = UT_LIST_GET_LEN(trx_sys->rw_trx_list);
    
    /* The purge system needs to create the purge view and
    therefore requires that the trx_sys is inited. */
    //trx_purge_sys_create(srv_n_purge_threads, ib_bh);

    //if (dict_create() != DB_SUCCESS) {
    //    return(err);
    //}

    //srv_startup_is_before_trx_rollback_phase = FALSE;

    //bool success = buf_flush_list(ULINT_MAX, LSN_MAX, NULL);
    //ut_a(success);

    //min_flushed_lsn = max_flushed_lsn = log_get_lsn();
    //buf_flush_wait_batch_end(NULL, BUF_FLUSH_LIST);
    /* Stamp the LSN to the data files. */
    //fil_write_flushed_lsn_to_data_files(max_flushed_lsn, 0);
    //fil_flush_file_spaces(FIL_TABLESPACE);

    //create_log_files_rename(logfilename, dirnamelen, max_flushed_lsn, logfile0);


    return DB_SUCCESS;
}

