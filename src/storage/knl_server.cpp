#include "knl_server.h"
#include "cm_file.h"
#include "cm_util.h"
#include "cm_log.h"
#include "knl_fsp.h"
#include "knl_log.h"

#include "m_ctype.h"

/*------------------------- global config ------------------------ */




bool32 buf_pool_should_madvise;


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

bool32 srv_archive_recovery = FALSE;


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
vm_pool_t*      srv_temp_mem_pool = NULL;
memory_pool_t*  srv_common_mpool = NULL;
memory_pool_t*  srv_plan_mem_pool = NULL;
memory_pool_t*  srv_mtr_memory_pool = NULL;
memory_pool_t*  srv_dictionary_mem_pool = NULL;

CHARSET_INFO    all_charsets[MY_ALL_CHARSETS_SIZE];


srv_stats_t     srv_stats;

db_ctrl_t       srv_ctrl_file = {0};
char            srv_data_home[1024] = {0};
const uint32    srv_data_home_len = 1023;

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





#define DB_CTRL_FILE_MAGIC   97937874

static status_t write_ctrl_file(char *name, db_ctrl_t *ctrl)
{
    status_t  err = CM_ERROR;
    os_file_t file = OS_FILE_INVALID_HANDLE;
    uchar    *buf = NULL, *ptr;
    uint32    size = SIZE_M(8);

    buf = (uchar *)ut_malloc_zero(size);
    ptr = buf;

    // len(4B) + magic(4B) + checksum(8B) + ctrl file

    ptr += 4; // length
    mach_write_to_4(ptr, DB_CTRL_FILE_MAGIC);
    ptr += 4;
    ptr += 8; // checksum

    // ctrl file
    mach_write_to_8(ptr, ctrl->version);
    ptr += 8;
    mach_write_to_8(ptr, ctrl->ver_num);
    ptr += 8;

    // database name
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

    // double write
    memcpy(ptr, ctrl->dbwr.name, strlen(ctrl->dbwr.name) + 1);
    ptr += strlen(ctrl->dbwr.name) + 1;
    mach_write_to_8(ptr, ctrl->dbwr.size);
    ptr += 8;
    mach_write_to_8(ptr, ctrl->dbwr.max_size);
    ptr += 8;
    mach_write_to_4(ptr, ctrl->dbwr.autoextend);
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

    // user space
    mach_write_to_4(ptr, ctrl->user_space_count);
    ptr += 4;
    for (uint32 i = 0; i < DB_USER_SPACE_MAX_COUNT; i++) {
        if (ctrl->user_spaces[i].space_id == DB_SPACE_INALID_ID) {
            continue;
        }
        memcpy(ptr, ctrl->user_spaces[i].name, strlen(ctrl->user_spaces[i].name) + 1);
        ptr += strlen(ctrl->user_spaces[i].name) + 1;
        mach_write_to_4(ptr, ctrl->user_spaces[i].space_id);
        ptr += 4;
        mach_write_to_4(ptr, ctrl->user_spaces[i].purpose);
        ptr += 4;
    }

    // data files
    mach_write_to_4(ptr, ctrl->user_space_data_file_count);
    ptr += 4;
    mach_write_to_4(ptr, ctrl->user_space_data_file_array_size);
    ptr += 4;
    ut_ad(ctrl->user_space_data_file_count <= DB_SPACE_DATA_FILE_MAX_COUNT);
    for (uint32 i = 0; i < ctrl->user_space_data_file_count; i++) {
        if (ctrl->user_space_data_files[i].node_id == DB_DATA_FILNODE_INALID_ID) {
            continue;
        }

        mach_write_to_4(ptr, ctrl->user_space_data_files[i].node_id);
        ptr += 4;
        mach_write_to_4(ptr, ctrl->user_space_data_files[i].space_id);
        ptr += 4;
        memcpy(ptr, ctrl->user_space_data_files[i].name, strlen(ctrl->user_space_data_files[i].name) + 1);
        ptr += strlen(ctrl->user_space_data_files[i].name) + 1;
        mach_write_to_8(ptr, ctrl->user_space_data_files[i].size);
        ptr += 8;
        mach_write_to_8(ptr, ctrl->user_space_data_files[i].max_size);
        ptr += 8;
        mach_write_to_4(ptr, ctrl->user_space_data_files[i].autoextend);
        ptr += 4;
        mach_write_to_4(ptr, ctrl->user_space_data_files[i].status);
        ptr += 4;
    }

    // len
    uint32 file_size = (uint32)(ptr - buf);
    mach_write_to_4(buf, file_size);

    uint64 align_size = ut_uint64_align_up(file_size, 512);

    // check sum
    uint64 checksum = 0;
    for (uint32 i = 16; i < file_size; i++) {
        checksum += buf[i];
    }
    mach_write_to_8(buf + 8, checksum);

    // write ctrl file

    bool32 ret = os_open_file(name, OS_FILE_CREATE, 0, &file);
    if (!ret) {
        LOGGER_ERROR(LOGGER, "write_ctrl_file: failed to create file, name = %s", name);
        goto err_exit;
    }
    ret = os_pwrite_file(file, 0, buf, (uint32)align_size);
    if (!ret) {
        LOGGER_ERROR(LOGGER, "write_ctrl_file: failed to write file, name = %s", name);
        goto err_exit;
    }
    ret = os_fsync_file(file);
    if (!ret) {
        LOGGER_ERROR(LOGGER, "write_ctrl_file: failed to sync file, name = %s", name);
        goto err_exit;
    }

    err = CM_SUCCESS;

err_exit:

    if (file != OS_FILE_INVALID_HANDLE) {
        os_close_file(file);
    }

    if (buf) {
        ut_free(buf);
    }

    return err;
}

status_t read_ctrl_file(char* name, db_ctrl_t* ctrl)
{
    status_t  err = CM_ERROR;
    os_file_t file = OS_FILE_INVALID_HANDLE;
    uchar     *buf = NULL, *ptr;
    uint32    size = SIZE_M(8), read_bytes = 0;
    uint64    checksum = 0;

    bool32 ret = os_open_file(name, OS_FILE_OPEN, 0, &file);
    if (!ret) {
        LOGGER_FATAL(LOGGER, "invalid control file, can not open ctrl file, name = %s", name);
        goto err_exit;
    }

    buf = (uchar *)ut_malloc_zero(size);

    // len(4B) + magic(4B) + checksum(8B) + ctrl file
    ret = os_pread_file(file, 0, buf, size, &read_bytes);
    if (!ret || read_bytes <= 4 || mach_read_from_4(buf) > read_bytes) {
        LOGGER_FATAL(LOGGER, "invalid control file, read size = %d ctrl file size %d",
            read_bytes, mach_read_from_4(buf));
        goto err_exit;
    }

    // check magic
    if (mach_read_from_4(buf + 4) != DB_CTRL_FILE_MAGIC) {
        //LOGGER_FATAL(LOGGER, "invalid control file, wrong magic = %lu", mach_read_from_4(ptr));
        goto err_exit;
    }

    // check checksum
    uint32 file_size = mach_read_from_4(buf);
    for (uint32 i = 16; i < file_size; i++) {
        checksum += buf[i];
    }
    if (checksum != mach_read_from_8(buf + 8)) {
        LOGGER_FATAL(LOGGER, "checksum mismatch, damaged control file");
        goto err_exit;
    }

    // ctrl file
    ptr = buf + 16;

    //
    ctrl->version = mach_read_from_8(ptr);
    ptr += 8;
    ctrl->ver_num = mach_read_from_8(ptr);
    ptr += 8;

    // database name
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

    // double write
    memcpy(ctrl->dbwr.name, (const char*)ptr, strlen((const char*)ptr) + 1);
    ptr += strlen((const char*)ptr) + 1;
    ctrl->dbwr.size = mach_read_from_8(ptr);
    ptr += 8;
    ctrl->dbwr.max_size = mach_read_from_8(ptr);
    ptr += 8;
    ctrl->dbwr.autoextend = mach_read_from_4(ptr);
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

    // user space
    ctrl->user_space_count = mach_read_from_4(ptr);
    ptr += 4;
    for (uint32 i = 0; i < ctrl->user_space_count; i++) {
        memcpy(ctrl->user_spaces[i].name, ptr, strlen((const char*)ptr) + 1);
        ptr += strlen((const char*)ptr) + 1;
        ctrl->user_spaces[i].space_id = mach_read_from_4(ptr);
        ptr += 4;
        ctrl->user_spaces[i].purpose = mach_read_from_4(ptr);
        ptr += 4;
    }

    // data file
    ctrl->user_space_data_file_count = mach_read_from_4(ptr);
    ptr += 4;
    ctrl->user_space_data_file_array_size = mach_read_from_4(ptr);
    ptr += 4;
    ctrl->user_space_data_files = (db_data_file_t *)ut_malloc_zero(sizeof(db_data_file_t) * ctrl->user_space_data_file_array_size);
    for (uint32 i = 0; i < ctrl->user_space_data_file_count; i++) {
        uint32 node_id = mach_read_from_4(ptr);
        ut_a(node_id != DB_DATA_FILNODE_INALID_ID);
        ut_a(node_id < DB_SPACE_DATA_FILE_MAX_COUNT);

        ctrl->user_space_data_files[node_id].node_id = mach_read_from_4(ptr);
        ptr += 4;
        ctrl->user_space_data_files[node_id].space_id = mach_read_from_4(ptr);
        ptr += 4;
        memcpy(ctrl->user_space_data_files[node_id].name, ptr, strlen((const char*)ptr) + 1);
        ptr += strlen((const char*)ptr) + 1;
        ctrl->user_space_data_files[node_id].size = mach_read_from_8(ptr);
        ptr += 8;
        ctrl->user_space_data_files[node_id].max_size = mach_read_from_8(ptr);
        ptr += 8;
        ctrl->user_space_data_files[node_id].autoextend = mach_read_from_4(ptr);
        ptr += 4;
        ctrl->user_space_data_files[node_id].status = mach_read_from_4(ptr);
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
        LOGGER_FATAL(LOGGER, "invalid control file, not found charset name %s", ctrl->charset_name);
        goto err_exit;
    }

    err = CM_SUCCESS;

err_exit:

    if (file != OS_FILE_INVALID_HANDLE) {
        os_close_file(file);
    }

    if (buf) {
        ut_free(buf);
    }

    return err;
}

static bool32 db_ctrl_check_space_file(char* file_name)
{
    if (strlen(file_name) == strlen(srv_ctrl_file.system.name) &&
        strcmp(file_name, srv_ctrl_file.system.name) == 0) {
        return FALSE;
    }
    for (uint32 i = 0; i < srv_ctrl_file.redo_count; i++) {
        if (strlen(file_name) == strlen(srv_ctrl_file.redo_group[i].name) &&
            strcmp(file_name, srv_ctrl_file.redo_group[i].name) == 0) {
            return FALSE;
        }
    }
    for (uint32 i = 0; i < srv_ctrl_file.undo_count; i++) {
        if (strlen(file_name) == strlen(srv_ctrl_file.undo_group[i].name) &&
            strcmp(file_name, srv_ctrl_file.undo_group[i].name) == 0) {
            return FALSE;
        }
    }
    for (uint32 i = 0; i < srv_ctrl_file.temp_count; i++) {
        if (strlen(file_name) == strlen(srv_ctrl_file.temp_group[i].name) &&
            strcmp(file_name, srv_ctrl_file.temp_group[i].name) == 0) {
            return FALSE;
        }
    }

    for (uint32 i = 0; i < srv_ctrl_file.user_space_data_file_array_size; i++) {
        if (strlen(file_name) == strlen(srv_ctrl_file.user_space_data_files[i].name) &&
            strcmp(file_name, srv_ctrl_file.user_space_data_files[i].name) == 0) {
            return FALSE;
        }
    }

    return TRUE;
}

static void db_ctrl_set_file(db_data_file_t* file, uint32 space_id, uint32 node_id,
    char* name, uint64 size, uint64 max_size, bool32 autoextend)
{
    file->space_id = space_id;
    file->node_id = node_id;
    file->size = size;
    file->max_size = max_size;
    file->autoextend = autoextend;
    //file->name = (char*)malloc(strlen(name) + 1);
    sprintf_s(file->name, strlen(name) + 1, "%s", name);
    file->name[strlen(name)] = '\0';
}

bool32 db_ctrl_add_system(char* data_file_name, uint64 size, uint64 max_size, bool32 autoextend)
{
    db_data_file_t *file = &srv_ctrl_file.system;

    db_ctrl_set_file(file, DB_SYSTEM_SPACE_ID, DB_SYSTEM_FILNODE_ID, data_file_name, size, max_size, autoextend);

    return TRUE;
}

bool32 db_ctrl_add_redo(char* data_file_name, uint64 size, uint64 max_size, bool32 autoextend)
{
    if (srv_ctrl_file.redo_count >= DB_REDO_FILE_MAX_COUNT) {
        LOGGER_ERROR(LOGGER, "db_ctrl_add_redo: Error, REDO file has reached the maximum limit");
        return FALSE;
    }

    db_data_file_t *file = &srv_ctrl_file.redo_group[srv_ctrl_file.redo_count];
    srv_ctrl_file.redo_count++;

    db_ctrl_set_file(file, DB_REDO_SPACE_ID, DB_DATA_FILNODE_INALID_ID, data_file_name, size, max_size, autoextend);

    return TRUE;
}

bool32 db_ctrl_add_dbwr(char* data_file_name, uint64 size)
{
    db_data_file_t *file = &srv_ctrl_file.dbwr;

    db_ctrl_set_file(file, DB_DBWR_SPACE_ID, DB_DBWR_FILNODE_ID, data_file_name, size, size, FALSE);

    return TRUE;
}

bool32 db_ctrl_add_undo(char* data_file_name, uint64 size, uint64 max_size, bool32 autoextend)
{
    if (srv_ctrl_file.undo_count >= DB_UNDO_FILE_MAX_COUNT) {
        LOGGER_ERROR(LOGGER, "db_ctrl_add_undo: Error, UNDO file has reached the maximum limit");
        return FALSE;
    }

    db_data_file_t *file = &srv_ctrl_file.undo_group[srv_ctrl_file.undo_count];
    srv_ctrl_file.undo_count++;

    db_ctrl_set_file(file, DB_UNDO_SPACE_ID, DB_DATA_FILNODE_INALID_ID, data_file_name, size, max_size, autoextend);

    return TRUE;
}

bool32 db_ctrl_add_temp(char* data_file_name, uint64 size, uint64 max_size, bool32 autoextend)
{
    if (srv_ctrl_file.temp_count >= DB_TEMP_FILE_MAX_COUNT) {
        LOGGER_ERROR(LOGGER, "db_ctrl_add_temp: Error, TEMP file has reached the maximum limit");
        return FALSE;
    }

    db_data_file_t *file = &srv_ctrl_file.temp_group[srv_ctrl_file.temp_count];
    srv_ctrl_file.temp_count++;

    db_ctrl_set_file(file, DB_TEMP_SPACE_ID, DB_DATA_FILNODE_INALID_ID, data_file_name, size, max_size, autoextend);

    return TRUE;
}

bool32 db_ctrl_add_system_space(char* space_name, uint32 space_id)
{
    if (space_id >= DB_SYSTEM_SPACE_MAX_COUNT || strlen(space_name) > DB_OBJECT_NAME_MAX_LEN) {
        LOGGER_ERROR(LOGGER, "db_ctrl_add_system_space: Error, invalid table space. name %s id %lu", space_name, space_id);
        return FALSE;
    }

    for (uint32 i = 0; i < DB_SYSTEM_SPACE_MAX_COUNT; i++) {
        if (space_id == srv_ctrl_file.system_spaces[i].space_id) {
            LOGGER_ERROR(LOGGER, "db_ctrl_add_system_space: Error, id of system space already exists, name %s id %lu", space_name, space_id);
            return FALSE;
        }
        if (strlen(space_name) == strlen(srv_ctrl_file.system_spaces[i].name) &&
            strcmp(space_name, srv_ctrl_file.system_spaces[i].name) == 0) {
            LOGGER_ERROR(LOGGER, "db_ctrl_add_system_space: Error, name of system space already exists, name %s id %lu", space_name, space_id);
            return FALSE;
        }
    }

    if (srv_ctrl_file.system_spaces[space_id].space_id != DB_SPACE_INALID_ID) {
        LOGGER_ERROR(LOGGER, "db_ctrl_add_system_space: Error, id of system space already exists, name %s id %lu", space_name, space_id);
        return FALSE;
    }

    if (srv_ctrl_file.system_spaces[space_id].name[0] != '\0') {
        LOGGER_ERROR(LOGGER, "db_ctrl_add_system_space: Error, name of system space already exists, name %s id %lu", space_name, space_id);
        return FALSE;
    }

    srv_ctrl_file.system_spaces[space_id].space_id = space_id;
    memcpy(srv_ctrl_file.system_spaces[space_id].name, space_name, strlen(space_name) + 1);
    srv_ctrl_file.system_space_count++;

    return TRUE;
}

bool32 db_ctrl_add_user_space(char* space_name)
{
    if (srv_ctrl_file.user_space_count >= DB_USER_SPACE_MAX_COUNT) {
        LOGGER_ERROR(LOGGER, "db_ctrl_add_user_space: Error, user space has reached the maximum limit");
        return FALSE;
    }
    if (strlen(space_name) > DB_OBJECT_NAME_MAX_LEN) {
        LOGGER_ERROR(LOGGER, "db_ctrl_add_user_space: Error, invalid user space name %s", space_name);
        return FALSE;
    }

    // system space
    for (uint32 i = 0; i < DB_SYSTEM_SPACE_MAX_COUNT; i++) {
        if (strlen(space_name) == strlen(srv_ctrl_file.system_spaces[i].name) &&
            strcmp(space_name, srv_ctrl_file.system_spaces[i].name) == 0) {
            LOGGER_ERROR(LOGGER, "db_ctrl_add_user_space: Error, name of system space already exists, name %s", space_name);
            return FALSE;
        }
    }

    // user space
    uint32 space_id = DB_SPACE_INALID_ID;
    for (uint32 i = 0; i < DB_USER_SPACE_MAX_COUNT; i++) {
        if (strlen(space_name) == strlen(srv_ctrl_file.user_spaces[i].name) &&
            strcmp(space_name, srv_ctrl_file.user_spaces[i].name) == 0) {
            LOGGER_ERROR(LOGGER, "db_ctrl_add_user_space: Error, name of user space already exists, name %s", space_name);
            return FALSE;
        }
        if (space_id == DB_SPACE_INALID_ID &&
            srv_ctrl_file.user_spaces[i].space_id == DB_SPACE_INALID_ID) {
            space_id = i;
        }
    }
    if (space_id == DB_SPACE_INALID_ID) {
        LOGGER_ERROR(LOGGER, "db_ctrl_add_user_space: Error, user space has reached the maximum limit");
        return FALSE;
    }

    srv_ctrl_file.user_spaces[space_id].space_id = space_id + DB_USER_SPACE_FIRST_ID;
    memcpy(srv_ctrl_file.user_spaces[space_id].name, space_name, strlen(space_name) + 1);
    srv_ctrl_file.user_space_count++;

    return TRUE;
}

bool32 db_ctrl_add_space_file(char* space_name, char* data_file_name, uint64 size, uint64 max_size, bool32 autoextend)
{
    if (strlen(data_file_name) > DB_DATA_FILE_NAME_MAX_LEN) {
        LOGGER_ERROR(LOGGER, "db_ctrl_add_space_file: Error, name of data file has reached the maximum limit %s", data_file_name);
        return FALSE;
    }

    if (srv_ctrl_file.user_space_data_file_count >= DB_SPACE_DATA_FILE_MAX_COUNT) {
        LOGGER_ERROR(LOGGER, "db_ctrl_add_space_file: Error, data file has reached the maximum limit");
        return FALSE;
    }

    // check user space
    uint32 space_id = DB_SPACE_INALID_ID;
    for (uint32 i = 0; i < DB_USER_SPACE_MAX_COUNT; i++) {
        if (strlen(space_name) == strlen(srv_ctrl_file.user_spaces[i].name) &&
            strcmp(space_name, srv_ctrl_file.user_spaces[i].name) == 0) {
            space_id = srv_ctrl_file.user_spaces[i].space_id;
            break;
        }
    }
    if (space_id == DB_SPACE_INALID_ID) {
        LOGGER_ERROR(LOGGER, "db_ctrl_add_space_file: Error, invalid space name %s", space_name);
        return FALSE;
    }

    // check data file
    if (!db_ctrl_check_space_file(data_file_name)) {
        LOGGER_ERROR(LOGGER, "db_ctrl_add_space_file: Error, data file exists, space %s name %s",
            space_name, data_file_name);
        return FALSE;
    }

    //
    if (srv_ctrl_file.user_space_data_file_count == srv_ctrl_file.user_space_data_file_array_size) {
        // malloc data_file array
        srv_ctrl_file.user_space_data_file_array_size += 32;
        db_data_file_t* data_files =
            (db_data_file_t *)ut_malloc_zero(sizeof(db_data_file_t) * srv_ctrl_file.user_space_data_file_array_size);
        for (uint32 i = srv_ctrl_file.user_space_data_file_count; i < srv_ctrl_file.user_space_data_file_array_size; i++) {
            data_files[i].node_id = DB_DATA_FILNODE_INALID_ID;
        }
        // copy data
        if (srv_ctrl_file.user_space_data_file_count > 0) {
            ut_a(srv_ctrl_file.user_space_data_files);
            memcpy(data_files, srv_ctrl_file.user_space_data_files,
                sizeof(db_data_file_t) * srv_ctrl_file.user_space_data_file_count);
            ut_free(srv_ctrl_file.user_space_data_files);
        }
        srv_ctrl_file.user_space_data_files = data_files;
    }
    // find a free cell of data file array
    uint32 node_id = DB_DATA_FILNODE_INALID_ID;
    for (uint32 i = 0; i < srv_ctrl_file.user_space_data_file_array_size; i++) {
        if (srv_ctrl_file.user_space_data_files[i].node_id == DB_DATA_FILNODE_INALID_ID) {
            node_id = i;
            break;
        }
    }
    if (node_id == DB_DATA_FILNODE_INALID_ID) {
        LOGGER_ERROR(LOGGER, "db_ctrl_add_space_file: Error, data file has reached the maximum limit");
        return FALSE;
    }

    db_data_file_t *file = &srv_ctrl_file.user_space_data_files[node_id];
    db_ctrl_set_file(file, space_id, node_id + DB_USER_DATA_FILE_FIRST_NODE_ID,
        data_file_name, size, max_size, autoextend);
    srv_ctrl_file.user_space_data_file_count++;

    return TRUE;
}

bool32 srv_create_ctrl_files()
{
    char file_name[CM_FILE_PATH_BUF_SIZE];

    for (uint32 i = 1; i <= 3; i++) {
        sprintf_s(file_name, CM_FILE_PATH_MAX_LEN, "%s%c%s%d", srv_data_home, SRV_PATH_SEPARATOR, "ctrl", i);

        /* Remove any old ctrl files. */
#ifdef __WIN__
        DeleteFile((LPCTSTR)file_name);
#else
        unlink(file_name);
#endif

        if (write_ctrl_file(file_name, &srv_ctrl_file) != CM_SUCCESS) {
            return FALSE;
        }
    }

    return TRUE;
}

static status_t create_db_file(db_data_file_t *ctrl_file)
{
    bool32 ret;
    os_file_t file;

    ret = os_open_file(ctrl_file->name, OS_FILE_CREATE, OS_FILE_SYNC, &file);
    if (!ret) {
        char err_info[CM_ERR_MSG_MAX_LEN];
        os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
        LOGGER_ERROR(LOGGER, "can not create file, name = %s error = %s",
            ctrl_file->name, err_info);
        return CM_ERROR;
    }

    ret = os_file_extend(ctrl_file->name, file, ctrl_file->size);
    if (!ret) {
        char err_info[CM_ERR_MSG_MAX_LEN];
        os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
        LOGGER_ERROR(LOGGER, "can not extend file %s to size %lu MB, error = %s",
             ctrl_file->name, ctrl_file->size, err_info);
        return CM_ERROR;
    }

    ret = os_close_file(file);
    ut_a(ret);

    return CM_SUCCESS;
}

static status_t srv_delete_db_file(char* name)
{
    bool32 ret = os_del_file(name);
    if (!ret && os_file_get_last_error() != OS_FILE_NOT_FOUND) {
        char err_info[CM_ERR_MSG_MAX_LEN];
        os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
        LOGGER_ERROR(LOGGER, "failed to delete file, name = %s error = %s",
             name, err_info);
        return CM_ERROR;
    }

    return CM_SUCCESS;
}

status_t srv_create_redo_log_files()
{
    db_data_file_t *ctrl_file;

    for (uint32 i = 0; i < srv_ctrl_file.redo_count; i++) {
        ctrl_file = &srv_ctrl_file.redo_group[i];
        CM_RETURN_IF_ERROR(srv_delete_db_file(ctrl_file->name));
        status_t err = create_db_file(ctrl_file);
        if (err != CM_SUCCESS) {
            return err;
        }
    }

    return CM_SUCCESS;
}

status_t srv_create_undo_log_files()
{
    db_data_file_t *ctrl_file;

    for (uint32 i = 0; i < srv_ctrl_file.undo_count; i++) {
        ctrl_file = &srv_ctrl_file.undo_group[i];
        CM_RETURN_IF_ERROR(srv_delete_db_file(ctrl_file->name));
        status_t err = create_db_file(ctrl_file);
        if (err != CM_SUCCESS) {
            return err;
        }
    }

    return CM_SUCCESS;
}

status_t srv_create_temp_files()
{
    status_t err = CM_SUCCESS;
    db_data_file_t *ctrl_file;

    for (uint32 i = 0; i < srv_ctrl_file.temp_count; i++) {
        ctrl_file = &srv_ctrl_file.temp_group[i];
        CM_RETURN_IF_ERROR(srv_delete_db_file(ctrl_file->name));
        //status_t err = create_db_file(ctrl_file);
        err = vm_pool_add_file(srv_temp_mem_pool, ctrl_file->name, ctrl_file->max_size);
        CM_RETURN_IF_ERROR(err);
    }

    return CM_SUCCESS;
}

status_t srv_create_system_file()
{
    db_data_file_t *ctrl_file = &srv_ctrl_file.system;

    CM_RETURN_IF_ERROR(srv_delete_db_file(ctrl_file->name));

    status_t err = create_db_file(ctrl_file);
    if (err != CM_SUCCESS) {
        return err;
    }

    return CM_SUCCESS;
}

status_t srv_create_double_write_file()
{
    db_data_file_t *ctrl_file = &srv_ctrl_file.dbwr;

    CM_RETURN_IF_ERROR(srv_delete_db_file(ctrl_file->name));

    status_t err = create_db_file(ctrl_file);
    if (err != CM_SUCCESS) {
        return err;
    }

    return CM_SUCCESS;
}

status_t srv_create_user_data_files()
{
    for (uint32 i = 0; i < srv_ctrl_file.user_space_data_file_array_size && i < DB_SPACE_DATA_FILE_MAX_COUNT; i++) {
        if (srv_ctrl_file.user_space_data_files[i].node_id == DB_DATA_FILNODE_INALID_ID) {
            continue;
        }

        db_data_file_t* data_file = &srv_ctrl_file.user_space_data_files[i];

        CM_RETURN_IF_ERROR(srv_delete_db_file(data_file->name));
        status_t err = create_db_file(data_file);
        if (err != CM_SUCCESS) {
            return err;
        }
    }

    return CM_SUCCESS;
}

bool32 db_ctrl_createdatabase(char* database_name, char* charset_name)
{
    db_ctrl_t* ctrl_file = &srv_ctrl_file;

    memset(ctrl_file, 0x00, sizeof(db_ctrl_t));

    ctrl_file->version = DB_CTRL_FILE_VERSION;

    for (uint32 i = 0; i < DB_SYSTEM_SPACE_MAX_COUNT; i++) {
        ctrl_file->system_spaces[i].space_id = DB_SPACE_INALID_ID;
    }
    db_ctrl_add_system_space("system", DB_SYSTEM_SPACE_ID);
    db_ctrl_add_system_space("sysaux", DB_SYSAUX_SPACE_ID);
    db_ctrl_add_system_space("redo", DB_REDO_SPACE_ID);
    db_ctrl_add_system_space("undo", DB_UNDO_SPACE_ID);
    db_ctrl_add_system_space("temporary", DB_TEMP_SPACE_ID);
    db_ctrl_add_system_space("dictionary", DB_DICT_SPACE_ID);
    db_ctrl_add_system_space("dbwr", DB_DBWR_SPACE_ID);

    for (uint32 i = 0; i < DB_USER_SPACE_MAX_COUNT; i++) {
        ctrl_file->user_spaces[i].space_id = DB_SPACE_INALID_ID;
    }

    ctrl_file->user_space_data_file_count = 0;
    ctrl_file->user_space_data_file_array_size = 0;
    ctrl_file->user_space_data_files = NULL;

    sprintf_s(ctrl_file->database_name, strlen(database_name) + 1, "%s", database_name);
    ctrl_file->database_name[strlen(database_name)] = '\0';

    sprintf_s(ctrl_file->charset_name, strlen(charset_name) + 1, "%s", charset_name);
    ctrl_file->charset_name[strlen(charset_name)] = '\0';
    
    return TRUE;
}


void* write_io_handler_thread(void *arg)
{
    os_aio_context_t* context;
    uint32 index = *(uint32 *)arg;

    ut_ad(index < srv_write_io_threads);

    context = os_aio_array_get_nth_context(srv_os_aio_async_write_array, index);

    while (srv_shutdown_state != SHUTDOWN_EXIT_THREADS) {
        fil_aio_reader_and_writer_wait(context);
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
        fil_aio_reader_and_writer_wait(context);
    }

    os_thread_exit(NULL);
    OS_THREAD_DUMMY_RETURN;
}

