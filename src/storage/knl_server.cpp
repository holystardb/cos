#include "knl_server.h"

/*------------------------- global config ------------------------ */

char *srv_data_home;
uint64 srv_system_file_size;
uint64 srv_system_file_max_size;
uint64 srv_system_file_auto_extend_size;
char *srv_system_charset_name = "latin1_swedish_ci";

uint64 srv_redo_log_buffer_size;
uint64 srv_redo_log_file_size;
uint64 srv_redo_log_file_count;

uint64 srv_undo_buffer_size;
uint64 srv_undo_file_max_size;
uint64 srv_undo_file_auto_extend_size;

uint64 srv_temp_buffer_size;
uint64 srv_temp_file_size;
uint64 srv_temp_file_max_size;
uint64 srv_temp_file_auto_extend_size;

uint64 srv_buf_pool_size;
uint32 srv_buf_pool_instances;
uint32 srv_buf_pool_chunk_unit;
bool32 buf_pool_should_madvise;
uint32 srv_n_page_hash_locks; /*!< number of locks to protect buf_pool->page_hash */

uint32 srv_max_n_open;
uint32 srv_space_max_count;
uint32 srv_fil_node_max_count;

/* The number of purge threads to use.*/
uint32 srv_purge_threads;
/* Use srv_n_io_[read|write]_threads instead. */
uint32 srv_n_file_io_threads;
uint32 srv_read_io_threads;
uint32 srv_write_io_threads;

/** in read-only mode. We don't do any
recovery and open all tables in RO mode instead of RW mode. We don't
sync the max trx id to disk either. */
bool32 srv_read_only_mode;

//TRUE means that recovery is running and no operations on the log files are allowed yet.
bool32 recv_no_ibuf_operations;

/** TRUE if writing to the redo log (mtr_commit) is forbidden.
Protected by log_sys->mutex. */
bool32 recv_no_log_write = FALSE;

/*-------------------------------------------------- */



srv_stats_t  srv_stats;

db_ctrl_t srv_db_ctrl;


extern uint64 srv_lock_wait_timeout;

typedef struct st_db_ctrl {
    uint64 version;
    uint64 system_file_size;
    uint64 system_file_max_size;
    uint64 system_file_auto_extend_size;
    uint64 redo_file_size;
    uint64 redo_file_count;
    uint64 undo_file_size;
    uint64 undo_file_max_size;
    uint64 undo_file_auto_extend_size;
    uint64 temp_file_size;
    uint64 temp_file_max_size;
    uint64 temp_file_auto_extend_size;
    CHARSET_INFO *charset_info;

} db_ctrl_t;

typedef struct st_db_charset_info {
    char* name;
    CHARSET_INFO *charset_info;
} db_charset_info_t;

db_charset_info_t srv_db_charset_info[] =
{
    {my_charset_bin.name, &my_charset_bin},
    {my_charset_latin1.name, &my_charset_latin1},
    {my_charset_filename.name, &my_charset_filename},
    {my_charset_big5_chinese_ci.name, &my_charset_big5_chinese_ci},
    {my_charset_big5_bin.name, &my_charset_big5_bin},
    {my_charset_cp932_japanese_ci.name, &my_charset_cp932_japanese_ci},
    {my_charset_cp932_bin.name, &my_charset_cp932_bin},
    {"", &my_charset_cp1250_czech_ci},
    {"", &my_charset_eucjpms_japanese_ci},
    {"", &my_charset_eucjpms_bin},
    {"", &my_charset_euckr_korean_ci},
    {"", &my_charset_euckr_bin},
    {"", &my_charset_gb2312_chinese_ci},
    {"", &my_charset_gb2312_bin},
    {"", &my_charset_gbk_chinese_ci},
    {"", &my_charset_gbk_bin},
    {"", &my_charset_gb18030_chinese_ci},
    {"", &my_charset_gb18030_bin},
    {"", &my_charset_latin1_german2_ci},
    {"", &my_charset_latin1_bin},
    {"", &my_charset_latin2_czech_ci},
    {"", &my_charset_sjis_japanese_ci},
    {"", &my_charset_sjis_bin},
    {"", &my_charset_tis620_thai_ci},
    {"", &my_charset_tis620_bin},
    {"", &my_charset_ucs2_general_ci},
    {"", &my_charset_ucs2_bin},
    {"", &my_charset_ucs2_unicode_ci},
    {"", &my_charset_ucs2_general_mysql500_ci},
    {"", &my_charset_ujis_japanese_ci},
    {"", &my_charset_ujis_bin},
    {"", &my_charset_utf16_bin},
    {"", &my_charset_utf16_general_ci},
    {"", &my_charset_utf16_unicode_ci},
    {"", &my_charset_utf16le_bin},
    {"", &my_charset_utf16le_general_ci},
    {"", &my_charset_utf32_bin},
    {"", &my_charset_utf32_general_ci},
    {"", &my_charset_utf32_unicode_ci},
    {"", &my_charset_utf8_general_ci},
    {"", &my_charset_utf8_tolower_ci},
    {"", &my_charset_utf8_unicode_ci},
    {"", &my_charset_utf8_bin},
    {"", &my_charset_utf8_general_mysql500_ci},
    {"", &my_charset_utf8mb4_bin},
    {"", &my_charset_utf8mb4_general_ci},
    {"", &my_charset_utf8mb4_unicode_ci},

    {NULL, NULL},
};

#define DB_CTRL_FILE_MAGIC   97937874

static bool32 write_ctrl_file(char *name, db_ctrl_t *ctrl)
{
    os_file_t file;
    bool32 ret;
    char buf[512];
    uint32 size;
    uint64 checksum = 0;

    ret = os_open_file(name, OS_FILE_CREATE, 0, &file);
    if (!ret) {
        return FALSE;
    }

    size = 4;
    mach_write_to_8(buf + size, DB_CTRL_FILE_MAGIC);
    size += 8;
    mach_write_to_8(buf + size, ctrl->version);
    size += 8;
    mach_write_to_8(buf + size, ctrl->system_file_size);
    size += 8;
    mach_write_to_8(buf + size, ctrl->system_file_max_size);
    size += 8;
    mach_write_to_8(buf + size, ctrl->system_file_auto_extend_size);
    size += 8;
    mach_write_to_8(buf + size, ctrl->redo_file_size);
    size += 8;
    mach_write_to_8(buf + size, ctrl->redo_file_count);
    size += 8;
    mach_write_to_8(buf + size, ctrl->undo_file_size);
    size += 8;
    mach_write_to_8(buf + size, ctrl->undo_file_max_size);
    size += 8;
    mach_write_to_8(buf + size, ctrl->undo_file_auto_extend_size);
    size += 8;
    mach_write_to_8(buf + size, ctrl->temp_file_size);
    size += 8;
    mach_write_to_8(buf + size, ctrl->temp_file_max_size);
    size += 8;
    mach_write_to_8(buf + size, ctrl->temp_file_auto_extend_size);
    size += 8;
    memcpy(buf + size, ctrl->charset_info.name, strlen(ctrl->charset_info.name) + 1);
    size += strlen(ctrl->charset_info.name) + 1;
    // check sum
    for (uint32 i = 0; i < size; i++) {
        checksum += buf[i];
    }
    mach_write_to_8(buf + size, checksum);
    size += 8;

    // totle size
    mach_write_to_4(buf, size - 4);
    //
    ret = os_pwrite_file(file, 0, buf, size);
    if (!ret) {
        os_close_file(file);
        return FALSE;
    }

    ret = os_fsync_file(file);
    if (!ret) {
        os_close_file(file);
        return FALSE;
    }

    os_close_file(file);

    return TRUE;
}

static bool32 read_ctrl_file(char *name, db_ctrl_t *ctrl)
{
    os_file_t file;
    bool32 ret;
    char buf[512];
    uint32 size, pos, total_size;
    uint64 checksum = 0;

    ret = os_open_file(name, OS_FILE_CREATE, 0, &file);
    if (!ret) {
        return FALSE;
    }
    ret = os_pread_file(file, 0, buf, 4, &size);
    if (!ret || size != 4) {
        os_close_file(file);
        return FALSE;
    }
    total_size = mach_read_from_4(buf);
    ret = os_pread_file(file, 4, buf, 512, &size);
    if (!ret || size != total_size) {
        os_close_file(file);
        return FALSE;
    }
    os_close_file(file);

    // magic
    if (mach_read_from_8(buf) != DB_CTRL_FILE_MAGIC) {
        return FALSE;
    }
    pos = 8;
    ctrl->version = mach_read_from_8(buf + pos);
    pos += 8;
    ctrl->system_file_size = mach_read_from_8(buf + pos);
    pos += 8;
    ctrl->system_file_max_size = mach_read_from_8(buf + pos);
    pos += 8;
    ctrl->system_file_auto_extend_size = mach_read_from_8(buf + pos);
    pos += 8;
    ctrl->redo_file_size = mach_read_from_8(buf + pos);
    pos += 8;
    ctrl->redo_file_count = mach_read_from_8(buf + pos);
    pos += 8;
    ctrl->undo_file_size = mach_read_from_8(buf + pos);
    pos += 8;
    ctrl->undo_file_max_size = mach_read_from_8(buf + pos);
    pos += 8;
    ctrl->undo_file_auto_extend_size = mach_read_from_8(buf + pos);
    pos += 8;
    ctrl->temp_file_size = mach_read_from_8(buf + pos);
    pos += 8;
    ctrl->temp_file_max_size = mach_read_from_8(buf + pos);
    pos += 8;
    ctrl->temp_file_auto_extend_size = mach_read_from_8(buf + pos);
    pos += 8;

    for (uint32 i = 0; srv_db_charset_info[i].name; i++) {
        if (strcmp(buf + pos, srv_db_charset_info[i].name) == 0) {
            ctrl.charset_info = srv_db_charset_info.charset_info;
            break;
        }
    }
    if (ctrl.charset_info == NULL) {
        LOG_PRINT_FATAL("FATAL: invalid control file, not found charset name");
        return FALSE;
    }
    pos += strlen(buf) + 1;

    // check sum
    for (uint32 i = 0; i < pos; i++) {
        checksum += buf[i];
    }
    if (checksum != mach_read_from_8(buf + pos)) {
        return FALSE;
    }

    return TRUE;
}


static bool32 srv_init_ctrl()
{
    srv_db_ctrl.version = 0;
    srv_db_ctrl.system_file_size = srv_system_file_size;
    srv_db_ctrl.system_file_max_size = srv_system_file_max_size;
    srv_db_ctrl.system_file_auto_extend_size = srv_system_file_auto_extend_size;
    srv_db_ctrl.redo_file_size = srv_redo_log_file_size;
    srv_db_ctrl.redo_file_count = srv_redo_log_file_count;
    srv_db_ctrl.undo_file_size = srv_undo_buffer_size;
    srv_db_ctrl.undo_file_max_size = srv_undo_file_max_size;
    srv_db_ctrl.undo_file_auto_extend_size = srv_undo_file_auto_extend_size;
    srv_db_ctrl.temp_file_size = srv_temp_file_size;
    srv_db_ctrl.temp_file_max_size = srv_temp_file_max_size;
    srv_db_ctrl.temp_file_auto_extend_size = srv_temp_file_auto_extend_size;

    srv_db_ctrl.charset_info = NULL;
    for (uint32 i = 0; srv_db_charset_info[i].name; i++) {
        if (strcmp(srv_system_charset_name, srv_db_charset_info[i].name) == 0) {
            srv_db_ctrl.charset_info = srv_db_charset_info.charset_info;
            break;
        }
    }
    if (srv_db_ctrl.charset_info == NULL) {
        LOG_PRINT_FATAL("FATAL: invalid charset name %s", charset_name);
        return FALSE;
    }

    return TRUE;
}

bool32 srv_create_ctrls(char *data_home)
{
    bool32 ret;
    uint32 dirname_len = strlen(data_home);

    srv_init_ctrl();

    char *filename_prefix = "ctrl", *filename;
    uint32 filename_size = strlen(data_home) + 1 /*PATH_SEPARATOR*/
        + strlen(filename_prefix) + 1 /*index*/ + 4 /* .dat */ + 1 /*'\0'*/;
    filename = (char *)malloc(filename_size);

    for (uint32 i = 1; i <= 3; i++) {
        if (data_home[dirname_len - 1] != SRV_PATH_SEPARATOR) {
            sprintf_s(filename, filename_size, "%s%c%s%d.dat", data_home, SRV_PATH_SEPARATOR, filename_prefix, 1);
        } else {
            sprintf_s(filename, filename_size, "%s%s%d.dat", data_home, filename_prefix, 1);
        }

        ret = write_ctrl_file(filename, &srv_db_ctrl);
        if (!ret) {
            return FALSE;
        }
    }

    free(filename);

    return TRUE;
}

static dberr_t create_log_file(const char* name)
{
    bool32 ret;
    os_file_t file;

    ret = os_open_file(name, OS_FILE_CREATE, OS_FILE_SYNC, &file);

    if (!ret) {
        LOG_PRINT_ERROR("Cannot create %s", name);
        return(DB_ERROR);
    }

    ret = os_file_extend(name, file, srv_redo_log_file_size);
    if (!ret) {
        LOG_PRINT_ERROR("Cannot set log file %s to size %lu MB",
             name, srv_redo_log_file_size);
        return(DB_ERROR);
    }

    ret = os_close_file(file);
    ut_a(ret);

    return DB_SUCCESS;
}


static dberr_t create_redo_log_files(
    bool32 create_new_db, /*!< in: TRUE if new database is being created */
    char* logfilename, /*!< in/out: buffer for log file name */
    size_t dirnamelen) /*!< in: length of the directory path */
{
    if (srv_read_only_mode) {
        LOG_PRINT_ERROR("Cannot create log files in read-only mode");
        return(DB_READ_ONLY);
    }

    if (!create_new_db) {
        /* Remove any old log files. */
        for (unsigned i = 0; i < SRV_N_LOG_FILES_MAX; i++) {
            sprintf(logfilename + dirnamelen, "redo%u", i);

            /* Ignore errors about non-existent files or files
            that cannot be removed. The create_log_file() will
            return an error when the file exists. */
#ifdef __WIN__
            DeleteFile((LPCTSTR) logfilename);
#else
            unlink(logfilename);
#endif
            /* Crashing after deleting the first
            file should be recoverable. The buffer
            pool was clean, and we can simply create
            all log files from the scratch. */
            //RECOVERY_CRASH(6);
        }
    }

    for (unsigned i = 0; i < srv_redo_log_file_count; i++) {
        sprintf(logfilename + dirnamelen, "redo%u", i);
        dberr_t err = create_log_file(logfilename);
        if (err != DB_SUCCESS) {
            return(err);
        }
    }

    fil_space_t* space = fil_space_create(logfilename, FIL_REDO_SPACE_ID, FIL_LOG);
    for (uint32 0 = 1; i < srv_redo_log_file_count; i++) {
        sprintf(logfilename + dirnamelen, "redo%u", i);
        if (!fil_node_create(space, logfilename,
            srv_redo_log_file_size / OS_FILE_LOG_BLOCK_SIZE,
            OS_FILE_LOG_BLOCK_SIZE, FALSE)) {
            ut_error;
        }
    }

    log_group_init(0, srv_redo_log_file_count,
        srv_redo_log_file_size, FIL_REDO_SPACE_ID, FIL_ARCH_LOG_SPACE_ID);

    //fil_open_log_and_system_tablespace_files();

    return DB_SUCCESS;
}


dberr_t srv_create_redo_logs(char *data_home)
{
    bool32 create_new_db;

    uint32 dirname_len = strlen(data_home);

    char *filename_prefix = "redo", *filename;
    uint32 filename_size = strlen(data_home) + 1 /*PATH_SEPARATOR*/
        + strlen(filename_prefix) + 2 /*index*/ + 1 /*'\0'*/;
    filename = (char *)malloc(filename_size);

    for (uint32 i = 1; i <= srv_redo_log_file_count; i++) {
        if (data_home[dirname_len - 1] != SRV_PATH_SEPARATOR) {
            sprintf_s(filename, filename_size, "%s%c%s%d", data_home, SRV_PATH_SEPARATOR, filename_prefix, 1);
        } else {
            sprintf_s(filename, filename_size, "%s%s%d", data_home, filename_prefix, 1);
        }

        ret = create_redo_log_files(create_new_db, filename, strlen(filename) - strlen(filename_prefix));
        if (!ret) {
            return ret;
        }
    }

    free(filename);

    return DB_SUCCESS;
}

dberr_t srv_create_undo_log(char *data_home)
{
    return DB_SUCCESS;
}

dberr_t srv_create_temp(char *data_home)
{
    return DB_SUCCESS;
}

dberr_t srv_create_system(char *data_home)
{
    bool32 ret;

    uint32 dirname_len = strlen(data_home);

    char *filename_prefix = "system", *filename;
    uint32 filename_size = strlen(data_home) + 1 /*PATH_SEPARATOR*/
        + strlen(filename_prefix) + 1 /*'\0'*/;
    filename = (char *)malloc(filename_size);

    char *page = (char *)malloc(UNIV_PAGE_SIZE_DEF);
    uint32 page_no = 0;
    uint32 page_count = srv_db_ctrl.system_file_size / UNIV_PAGE_SIZE_DEF;
    while (page_no < page_count) {
        
        page_no++;
    }

/*
    os_file_t file;
    ret = os_open_file(filename, OS_FILE_CREATE, 0, &file);
    if (!ret) {
        return DB_ERROR;
    }

    ret = os_pwrite_file(file, 0, buf, size);
    if (!ret) {
        os_close_file(file);
        return DB_ERROR;
    }

    ret = os_fsync_file(file);
    if (!ret) {
        os_close_file(file);
        return DB_ERROR;
    }

    os_close_file(file);
*/
    return DB_SUCCESS;
}

