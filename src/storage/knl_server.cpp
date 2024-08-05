#include "knl_server.h"
#include "cm_file.h"
#include "cm_util.h"
#include "cm_log.h"
#include "knl_fsp.h"
#include "knl_log.h"

#include "m_ctype.h"

/*------------------------- global config ------------------------ */

char *srv_data_home;
uint64 srv_system_file_size;
uint64 srv_system_file_max_size;
uint64 srv_system_file_auto_extend_size;
bool32 srv_auto_extend_last_data_file;
char *srv_system_charset_name = "utf8mb4_bin";

uint32 srv_redo_log_buffer_size;
uint32 srv_redo_log_file_size;
uint32 srv_redo_log_file_count;

uint64 srv_undo_buffer_size;
uint64 srv_undo_file_max_size;
uint64 srv_undo_file_auto_extend_size;

uint64 srv_temp_buffer_size;
uint64 srv_temp_file_size;
uint64 srv_temp_file_max_size;
uint64 srv_temp_file_auto_extend_size;

uint64 srv_buf_pool_size;
uint32 srv_buf_pool_instances;

bool32 buf_pool_should_madvise;
uint32 srv_n_page_hash_locks = 16; /*!< number of locks to protect buf_pool->page_hash */

uint32 srv_max_n_open;
uint32 srv_space_max_count;
uint32 srv_fil_node_max_count;

/* The number of purge threads to use.*/
uint32 srv_purge_threads;
/* Use srv_n_io_[read|write]_threads instead. */
uint32 srv_n_file_io_threads;
uint32 srv_read_io_threads = 4;
uint32 srv_write_io_threads = 4;
uint32 srv_sync_io_contexts = 16;

uint32 srv_read_io_timeout_seconds = 30;
uint32 srv_write_io_timeout_seconds = 30;

uint32 srv_buf_LRU_old_threshold_ms = 1000;


/** in read-only mode. We don't do any
recovery and open all tables in RO mode instead of RW mode. We don't
sync the max trx id to disk either. */
bool32 srv_read_only_mode = FALSE;

//TRUE means that recovery is running and no operations on the log files are allowed yet.
bool32 recv_no_ibuf_operations;

/** TRUE if writing to the redo log (mtr_commit) is forbidden.
Protected by log_sys->mutex. */
bool32 recv_no_log_write = FALSE;

shutdown_state_enum_t srv_shutdown_state = SHUTDOWN_NONE;

os_aio_array_t* srv_os_aio_async_read_array = NULL;
os_aio_array_t* srv_os_aio_async_write_array = NULL;
os_aio_array_t* srv_os_aio_sync_array = NULL;

memory_area_t*  srv_memory_sga = NULL;


/*-------------------------------------------------- */



db_charset_info_t srv_db_charset_info[] =
{
//    {my_charset_big5_chinese_ci.name, &my_charset_big5_chinese_ci},
//    {my_charset_big5_bin.name, &my_charset_big5_bin},
    {my_charset_bin.name, &my_charset_bin},
//    {my_charset_cp932_japanese_ci.name, &my_charset_cp932_japanese_ci},
//    {my_charset_cp932_bin.name, &my_charset_cp932_bin},
//    {"", &my_charset_cp1250_czech_ci},
//    {"", &my_charset_eucjpms_japanese_ci},
//    {"", &my_charset_eucjpms_bin},
//    {"", &my_charset_euckr_korean_ci},
//    {"", &my_charset_euckr_bin},
    {my_charset_filename.name, &my_charset_filename},
    {my_charset_gb2312_chinese_ci.name, &my_charset_gb2312_chinese_ci},
    {my_charset_gb2312_bin.name, &my_charset_gb2312_bin},
    {my_charset_gbk_chinese_ci.name, &my_charset_gbk_chinese_ci},
    {my_charset_gbk_bin.name, &my_charset_gbk_bin},
    {my_charset_gb18030_chinese_ci.name, &my_charset_gb18030_chinese_ci},
    {my_charset_gb18030_bin.name, &my_charset_gb18030_bin},
//    {my_charset_latin1_german2_ci.name, &my_charset_latin1_german2_ci},
    {my_charset_latin1.name, &my_charset_latin1},
    {my_charset_latin1_bin.name, &my_charset_latin1_bin},
/*
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
*/
    {my_charset_utf8_general_ci.name, &my_charset_utf8_general_ci},
    {my_charset_utf8_tolower_ci.name, &my_charset_utf8_tolower_ci},
    {my_charset_utf8_unicode_ci.name, &my_charset_utf8_unicode_ci},
    {my_charset_utf8_bin.name, &my_charset_utf8_bin},
    {my_charset_utf8_general_mysql500_ci.name, &my_charset_utf8_general_mysql500_ci},
    {my_charset_utf8mb4_bin.name, &my_charset_utf8mb4_bin},
    {my_charset_utf8mb4_general_ci.name, &my_charset_utf8mb4_general_ci},
    {my_charset_utf8mb4_unicode_ci.name, &my_charset_utf8mb4_unicode_ci},

    {NULL, NULL},
};


srv_stats_t  srv_stats;

db_ctrl_t srv_db_ctrl;

extern uint64 srv_lock_wait_timeout;

#define DB_CTRL_FILE_MAGIC   97937874

static bool32 write_ctrl_file(char *name, db_ctrl_t *ctrl)
{
    os_file_t file;
    uchar    *buf;
    uchar    *ptr;
    uint32    ctrl_file_size = 1024 * 1024;

    buf = (uchar *)malloc(ctrl_file_size);
    memset(buf, 0x00, ctrl_file_size);
    ptr = buf;

    ptr += 4; // length
    mach_write_to_8(ptr, DB_CTRL_FILE_MAGIC);
    ptr += 8;
    mach_write_to_8(ptr, ctrl->version);
    ptr += 8;

    memcpy(ptr, ctrl->database_name, strlen(ctrl->database_name) + 1);
    ptr += strlen(ctrl->database_name) + 1;
    memcpy(ptr, ctrl->charset_name, strlen(ctrl->charset_name) + 1);
    ptr += strlen(ctrl->charset_name) + 1;

    // system
    memcpy(ptr, ctrl->system.name, strlen(ctrl->system.name) + 1);
    ptr += strlen(ctrl->system.name) + 1;
    mach_write_to_8(ptr, ctrl->system.size);
    ptr += 8;
    mach_write_to_8(ptr, ctrl->system.max_size);
    ptr += 8;
    mach_write_to_4(ptr, ctrl->system.autoextend);
    ptr += 4;

    // redo
    mach_write_to_1(ptr, ctrl->redo_count);
    ptr += 1;
    for (uint32 i = 0; i < ctrl->redo_count; i++) {
        memcpy(ptr, ctrl->redo_group[i].name, strlen(ctrl->redo_group[i].name) + 1);
        ptr += strlen(ctrl->redo_group[i].name) + 1;
        mach_write_to_8(ptr, ctrl->redo_group[i].size);
        ptr += 8;
        mach_write_to_8(ptr, ctrl->redo_group[i].max_size);
        ptr += 8;
        mach_write_to_4(ptr, ctrl->redo_group[i].autoextend);
        ptr += 4;
    }

    // undo
    mach_write_to_1(ptr, ctrl->undo_count);
    ptr += 1;
    for (uint32 i = 0; i < ctrl->undo_count; i++) {
        memcpy(ptr, ctrl->undo_group[i].name, strlen(ctrl->undo_group[i].name) + 1);
        ptr += strlen(ctrl->undo_group[i].name) + 1;
        mach_write_to_8(ptr, ctrl->undo_group[i].size);
        ptr += 8;
        mach_write_to_8(ptr, ctrl->undo_group[i].max_size);
        ptr += 8;
        mach_write_to_4(ptr, ctrl->undo_group[i].autoextend);
        ptr += 4;
    }

    // temp
    mach_write_to_1(ptr, ctrl->temp_count);
    ptr += 1;
    for (uint32 i = 0; i < ctrl->temp_count; i++) {
        memcpy(ptr, ctrl->temp_group[i].name, strlen(ctrl->temp_group[i].name) + 1);
        ptr += strlen(ctrl->temp_group[i].name) + 1;
        mach_write_to_8(ptr, ctrl->temp_group[i].size);
        ptr += 8;
        mach_write_to_8(ptr, ctrl->temp_group[i].max_size);
        ptr += 8;
        mach_write_to_4(ptr, ctrl->temp_group[i].autoextend);
        ptr += 4;
    }

    // check sum
    uint32 size = (uint32)(ptr - buf);
    uint64 align_size = ut_uint64_align_up(size, 512);
    if (size + 8 > align_size) {
        align_size += 512;
    }
    mach_write_to_4(buf, (uint32)align_size);

    uint64 checksum = 0;
    for (uint32 i = 0; i < (align_size - 8); i++) {
        checksum += buf[i];
    }
    mach_write_to_8(buf + align_size - 8, checksum);

    bool32 ret = os_open_file(name, OS_FILE_CREATE, 0, &file);
    if (!ret) {
        free(buf);
        LOGGER_ERROR(LOGGER, "write_ctrl_file: failed to create file, name = %s", name);
        return FALSE;
    }
    ret = os_pwrite_file(file, 0, buf, (uint32)align_size);
    if (!ret) {
        free(buf);
        os_close_file(file);
        return FALSE;
    }
    ret = os_fsync_file(file);
    if (!ret) {
        free(buf);
        os_close_file(file);
        return FALSE;
    }
    os_close_file(file);

    free(buf);

    return TRUE;
}

bool32 read_ctrl_file(char *name, db_ctrl_t *ctrl)
{
    os_file_t file;
    bool32    ret;
    uchar    *buf;
    uchar    *ptr;
    uint32    size = 1024 * 1024;
    uint64    checksum = 0;

    buf = (uchar *)malloc(size);
    memset(buf, 0x00, size);
    ptr = buf;

    ret = os_open_file(name, OS_FILE_OPEN, 0, &file);
    if (!ret) {
        free(buf);
        LOGGER_FATAL(LOGGER, "invalid control file, can not open ctrl file, name = %s", name);
        return FALSE;
    }
    ret = os_pread_file(file, 0, buf, 512, &size);
    if (!ret || size <= 4 || mach_read_from_4(buf) != size) {
        free(buf);
        os_close_file(file);
        LOGGER_FATAL(LOGGER, "invalid control file, size = %d", size);
        return FALSE;
    }
    os_close_file(file);

    // check sum
    for (uint32 i = 0; i < (size - 4); i++) {
        checksum += buf[i];
    }
    if (checksum != mach_read_from_8(buf + size - 4)) {
        free(buf);
        LOGGER_FATAL(LOGGER, "invalid control file, wrong checksum = %lu", checksum);
        return FALSE;
    }

    // magic
    if (mach_read_from_8(buf+4) != DB_CTRL_FILE_MAGIC) {
        free(buf);
        LOGGER_FATAL(LOGGER, "invalid control file, wrong magic = %lu", mach_read_from_8(buf + 4));
        return FALSE;
    }

    ptr = buf + 12;

    memcpy(ctrl->database_name, (const char*)ptr, strlen((const char*)ptr) + 1);
    ptr += strlen((const char*)ptr) + 1;
    memcpy( ctrl->charset_name, (const char*)ptr, strlen((const char*)ptr) + 1);
    ptr += strlen((const char*)ptr) + 1;

    // system
    memcpy(ctrl->system.name, (const char*)ptr, strlen((const char*)ptr) + 1);
    ptr += strlen((const char*)ptr) + 1;
    ctrl->system.size = mach_read_from_8(ptr);
    ptr += 8;
    ctrl->system.max_size = mach_read_from_8(ptr);
    ptr += 8;
    ctrl->system.autoextend = mach_read_from_4(ptr);
    ptr += 4;

    // redo
    ctrl->redo_count = mach_read_from_1(ptr);
    ptr += 1;
    for (uint32 i = 0; i < ctrl->redo_count; i++) {
        memcpy(ctrl->redo_group[i].name, ptr, strlen((const char*)ptr) + 1);
        ptr += strlen((const char*)ptr) + 1;
        ctrl->redo_group[i].size = mach_read_from_8(ptr);
        ptr += 8;
        ctrl->redo_group[i].max_size = mach_read_from_8(ptr);
        ptr += 8;
        ctrl->redo_group[i].autoextend = mach_read_from_4(ptr);
        ptr += 4;
    }

    // undo
    ctrl->undo_count = mach_read_from_1(ptr);
    ptr += 1;
    for (uint32 i = 0; i < ctrl->undo_count; i++) {
        memcpy(ctrl->undo_group[i].name, ptr, strlen((const char*)ptr) + 1);
        ptr += strlen((const char*)ptr) + 1;
        ctrl->undo_group[i].size = mach_read_from_8(ptr);
        ptr += 8;
        ctrl->undo_group[i].max_size = mach_read_from_8(ptr);
        ptr += 8;
        ctrl->undo_group[i].autoextend = mach_read_from_4(ptr);
        ptr += 4;
    }

    // temp
    ctrl->temp_count = mach_read_from_1(ptr);
    ptr += 1;
    for (uint32 i = 0; i < ctrl->temp_count; i++) {
        memcpy(ctrl->temp_group[i].name, ptr, strlen((const char*)ptr) + 1);
        ptr += strlen((const char*)ptr) + 1;
        ctrl->temp_group[i].size = mach_read_from_8(ptr);
        ptr += 8;
        ctrl->temp_group[i].max_size = mach_read_from_8(ptr);
        ptr += 8;
        ctrl->temp_group[i].autoextend = mach_read_from_4(ptr);
        ptr += 4;
    }

    //
    for (uint32 i = 0; srv_db_charset_info[i].name; i++) {
        if (strcmp((const char *)ctrl->charset_name, (const char *)srv_db_charset_info[i].name) == 0) {
            ctrl->charset_info = srv_db_charset_info[i].charset_info;
            break;
        }
    }
    if (ctrl->charset_info == NULL) {
        free(buf);
        LOGGER_FATAL(LOGGER, "invalid control file, not found charset name");
        return FALSE;
    }

    free(buf);

    return TRUE;
}

static void db_ctrl_set_file(db_ctrl_file_t *file, char *name, uint64 size, uint64 max_size, bool32 autoextend)
{
    file->size = size;
    file->max_size = max_size;
    file->autoextend = autoextend;
    file->name = (char*)malloc(strlen(name) + 1);
    sprintf_s(file->name, strlen(name) + 1, "%s", name);
    file->name[strlen(name)] = '\0';
}


bool32 db_ctrl_createdatabase(char *database_name, char *charset_name)
{
    memset(&srv_db_ctrl, 0x00, sizeof(srv_db_ctrl));

    srv_db_ctrl.version = DB_CTRL_FILE_VERSION;

    srv_db_ctrl.database_name = (char*)malloc(strlen(database_name) + 1);
    sprintf_s(srv_db_ctrl.database_name, strlen(database_name) + 1, "%s", database_name);
    srv_db_ctrl.database_name[strlen(database_name)] = '\0';

    srv_db_ctrl.charset_name = (char*)malloc(strlen(charset_name) + 1);
    sprintf_s(srv_db_ctrl.charset_name, strlen(charset_name) + 1, "%s", charset_name);
    srv_db_ctrl.charset_name[strlen(charset_name)] = '\0';

    return TRUE;
}

bool32 db_ctrl_add_system(char *name, uint64 size, uint64 max_size, bool32 autoextend)
{
    db_ctrl_file_t *file = &srv_db_ctrl.system;

    db_ctrl_set_file(file, name, size, max_size, autoextend);

    return TRUE;
}

bool32 db_ctrl_add_redo(char *name, uint64 size, uint64 max_size, bool32 autoextend)
{

    if (srv_db_ctrl.redo_count >= DB_REDO_FILE_MAX_COUNT) {
        LOGGER_ERROR(LOGGER, "db_ctrl_add_redo: Error, REDO file has reached the maximum limit");
        return FALSE;
    }

    db_ctrl_file_t *file = &srv_db_ctrl.redo_group[srv_db_ctrl.redo_count];
    srv_db_ctrl.redo_count++;

    db_ctrl_set_file(file, name, size, max_size, autoextend);

    return TRUE;
}

bool32 db_ctrl_add_undo(char *name, uint64 size, uint64 max_size, bool32 autoextend)
{

    if (srv_db_ctrl.undo_count >= DB_UNDO_FILE_MAX_COUNT) {
        LOGGER_ERROR(LOGGER, "db_ctrl_add_undo: Error, UNDO file has reached the maximum limit");
        return FALSE;
    }

    db_ctrl_file_t *file = &srv_db_ctrl.undo_group[srv_db_ctrl.undo_count];
    srv_db_ctrl.undo_count++;

    db_ctrl_set_file(file, name, size, max_size, autoextend);

    return TRUE;
}

bool32 db_ctrl_add_temp(char *name, uint64 size, uint64 max_size, bool32 autoextend)
{

    if (srv_db_ctrl.temp_count >= DB_TEMP_FILE_MAX_COUNT) {
        LOGGER_ERROR(LOGGER, "db_ctrl_add_temp: Error, TEMP file has reached the maximum limit");
        return FALSE;
    }

    db_ctrl_file_t *file = &srv_db_ctrl.temp_group[srv_db_ctrl.temp_count];
    srv_db_ctrl.temp_count++;

    db_ctrl_set_file(file, name, size, max_size, autoextend);

    return TRUE;
}


bool32 srv_create_ctrls(char *data_home)
{
    bool32 ret;
    uint32 dirname_len = (uint32)strlen(data_home);
    char *filename_prefix = "ctrl", filename[1024];
    uint32 filename_size = (uint32)strlen(data_home) + 1 /*PATH_SEPARATOR*/
        + (uint32)strlen(filename_prefix) + 1 /*index*/ + 1 /*'\0'*/;
    ut_a(filename_size < 1024);

    for (uint32 i = 1; i <= 3; i++) {
        if (data_home[dirname_len - 1] != SRV_PATH_SEPARATOR) {
            sprintf_s(filename, filename_size, "%s%c%s%d", data_home, SRV_PATH_SEPARATOR, filename_prefix, i);
        } else {
            sprintf_s(filename, filename_size, "%s%s%d", data_home, filename_prefix, i);
        }

        /* Remove any old ctrl files. */
#ifdef __WIN__
        DeleteFile((LPCTSTR)filename);
#else
        unlink(filename);
#endif

        ret = write_ctrl_file(filename, &srv_db_ctrl);
        if (!ret) {
            return FALSE;
        }
    }

    return TRUE;
}

static dberr_t create_db_file(db_ctrl_file_t *ctrl_file)
{
    bool32 ret;
    os_file_t file;

    ret = os_open_file(ctrl_file->name, OS_FILE_CREATE, OS_FILE_SYNC, &file);
    if (!ret) {
        LOGGER_ERROR(LOGGER, "can not create file, name = %s", ctrl_file->name);
        return(DB_ERROR);
    }

    ret = os_file_extend(ctrl_file->name, file, ctrl_file->size);
    if (!ret) {
        LOGGER_ERROR(LOGGER, "can not extend file %s to size %lu MB",
             ctrl_file->name, ctrl_file->size);
        return(DB_ERROR);
    }

    ret = os_close_file(file);
    ut_a(ret);

    return DB_SUCCESS;
}

dberr_t srv_create_redo_logs()
{
    db_ctrl_file_t *ctrl_file;

    for (uint32 i = 0; i < srv_db_ctrl.redo_count; i++) {
        ctrl_file = &srv_db_ctrl.redo_group[i];
        os_del_file(ctrl_file->name);
        dberr_t err = create_db_file(ctrl_file);
        if (err != DB_SUCCESS) {
            return err;
        }
    }

    return DB_SUCCESS;
}

dberr_t srv_create_undo_log()
{
    db_ctrl_file_t *ctrl_file;

    for (uint32 i = 0; i < srv_db_ctrl.undo_count; i++) {
        ctrl_file = &srv_db_ctrl.undo_group[i];
        os_del_file(ctrl_file->name);
        dberr_t err = create_db_file(ctrl_file);
        if (err != DB_SUCCESS) {
            return err;
        }
    }

    return DB_SUCCESS;
}

dberr_t srv_create_temp()
{
    db_ctrl_file_t *ctrl_file;

    for (uint32 i = 0; i < srv_db_ctrl.temp_count; i++) {
        ctrl_file = &srv_db_ctrl.temp_group[i];
        os_del_file(ctrl_file->name);
        dberr_t err = create_db_file(ctrl_file);
        if (err != DB_SUCCESS) {
            return err;
        }
    }

    return DB_SUCCESS;
}

dberr_t srv_create_system()
{
    db_ctrl_file_t *ctrl_file;

    ctrl_file = &srv_db_ctrl.system;
    os_del_file(ctrl_file->name);

    //dberr_t err = create_db_file(ctrl_file);
    //if (err != DB_SUCCESS) {
    //    return err;
    //}
    bool32 ret;
    os_file_t file;

    ret = os_open_file(ctrl_file->name, OS_FILE_CREATE, OS_FILE_SYNC, &file);
    if (!ret) {
        LOGGER_ERROR(LOGGER, "can not create file, name = %s", ctrl_file->name);
        return(DB_ERROR);
    }
    ret = os_close_file(file);
    ut_a(ret);

    return DB_SUCCESS;
}

void* write_io_handler_thread(void *arg)
{
    os_aio_context_t* context;
    uint32 index = *(uint32 *)arg;

    ut_ad(index < srv_write_io_threads);

    context = os_aio_array_get_nth_context(srv_os_aio_async_write_array, index);

    while (srv_shutdown_state != SHUTDOWN_EXIT_THREADS) {
        os_file_aio_wait(context, srv_write_io_timeout_seconds * 1000000);
    }

    os_thread_exit(NULL);
    OS_THREAD_DUMMY_RETURN;
}

void* read_io_handler_thread(void *arg)
{
    os_aio_context_t* context;
    uint32 index = *(uint32 *)arg;

    ut_ad(index < srv_read_io_threads);

    context = os_aio_array_get_nth_context(srv_os_aio_async_read_array, index);;

    while (srv_shutdown_state != SHUTDOWN_EXIT_THREADS) {
        os_file_aio_wait(context, srv_read_io_timeout_seconds * 1000000);
    }

    os_thread_exit(NULL);
    OS_THREAD_DUMMY_RETURN;
}

