#include "knl_ctrl_file.h"
#include "cm_file.h"
#include "cm_util.h"
#include "cm_log.h"
#include "m_ctype.h"
#include "knl_server.h"
#include "knl_fsp.h"
#include "knl_trx_types.h"


/*-------------------------------------------------- */



#define DB_CTRL_FILE_MAGIC   97937874

const uint32 SPACE_COUNT_PER_PAGE = UNIV_PAGE_SIZE / sizeof(db_space_t);
const uint32 DATA_FILE_COUNT_PER_PAGE = UNIV_PAGE_SIZE / sizeof(db_data_file_t);

const uint32 SYSTEM_SPACE_SIZE = (DB_SYSTEM_SPACE_MAX_COUNT / SPACE_COUNT_PER_PAGE + 1) * UNIV_PAGE_SIZE;
const uint32 USER_SPACE_SIZE = (DB_USER_SPACE_MAX_COUNT / SPACE_COUNT_PER_PAGE + 1) * UNIV_PAGE_SIZE;
const uint32 SYSTEM_FILE_SIZE = (DB_SYSTEM_DATA_FILE_MAX_COUNT / DATA_FILE_COUNT_PER_PAGE + 1) * UNIV_PAGE_SIZE;
const uint32 USER_FILE_SIZE = (DB_USER_DATA_FILE_MAX_COUNT / DATA_FILE_COUNT_PER_PAGE + 1) * UNIV_PAGE_SIZE;
const uint32 TOTOAL_SIZE = UNIV_PAGE_SIZE + SYSTEM_SPACE_SIZE + USER_SPACE_SIZE + SYSTEM_FILE_SIZE + USER_FILE_SIZE;

byte g_ctrl_file_data_buffer[TOTOAL_SIZE];


db_data_file_t::db_data_file_t()
{
    node_id = DB_DATA_FILNODE_INVALID_ID;
    space_id = DB_SPACE_INVALID_ID;
    file_name[0] = '\0';
    size = 0;
    max_size = 0;
    autoextend = FALSE;
    status = 0;
}

byte* db_data_file_t::serialize(byte* buf_ptr, byte* end_ptr)
{
    byte* ptr = buf_ptr;

    mach_write_to_4(ptr, node_id);
    ptr += 4;
    mach_write_to_4(ptr, space_id);
    ptr += 4;
    memcpy(ptr, file_name, strlen(file_name) + 1);
    ptr += DB_DATA_FILE_NAME_MAX_LEN;
    mach_write_to_8(ptr, size);
    ptr += 8;
    mach_write_to_8(ptr, max_size);
    ptr += 8;
    mach_write_to_4(ptr, autoextend);
    ptr += 4;

    return ptr;
}

byte* db_data_file_t::deserialize(byte* buf_ptr, byte* end_ptr)
{
    byte* ptr = buf_ptr;

    node_id = mach_read_from_4(ptr);
    ptr += 4;
    space_id = mach_read_from_4(ptr);
    ptr += 4;

    size_t file_name_len = strlen((const char*)ptr);
    if (file_name_len >= DB_DATA_FILE_NAME_MAX_LEN) {
        //ut_error;
        return NULL;
    }
    memcpy(file_name, ptr, file_name_len + 1);
    ptr += DB_DATA_FILE_NAME_MAX_LEN;
    size = mach_read_from_8(ptr);
    ptr += 8;
    max_size = mach_read_from_8(ptr);
    ptr += 8;
    autoextend = mach_read_from_4(ptr);
    ptr += 4;

    return ptr;
}

status_t db_data_file_t::delete_data_file()
{
    if (file_name[0] == '\0') {
        ut_ad(node_id == DB_DATA_FILNODE_INVALID_ID);
        ut_ad(space_id == DB_SPACE_INVALID_ID);
        return CM_SUCCESS;
    }

    bool32 ret = os_del_file(file_name);
    if (!ret && os_file_get_last_error() != OS_FILE_NOT_FOUND) {
        char err_info[CM_ERR_MSG_MAX_LEN];
        os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
        LOGGER_ERROR(LOGGER, LOG_MODULE_CTRLFILE, "failed to delete file, name = %s error = %s",
             file_name, err_info);
        return CM_ERROR;
    }

    return CM_SUCCESS;
}

status_t db_data_file_t::create_data_file(bool32 is_extend_file)
{
    status_t status = CM_SUCCESS;
    bool32 ret;
    os_file_t file;

    if (file_name[0] == '\0') {
        ut_ad(node_id == DB_DATA_FILNODE_INVALID_ID);
        ut_ad(space_id == DB_SPACE_INVALID_ID);
        return CM_SUCCESS;
    }

    ret = os_open_file(file_name, OS_FILE_CREATE, OS_FILE_SYNC, &file);
    if (!ret) {
        char err_info[CM_ERR_MSG_MAX_LEN];
        os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
        LOGGER_ERROR(LOGGER, LOG_MODULE_CTRLFILE, "can not create file, name = %s error = %s",
            file_name, err_info);
        return CM_ERROR;
    }

    if (is_extend_file) {
        ret = os_file_extend(file_name, file, size);
        if (!ret) {
            char err_info[CM_ERR_MSG_MAX_LEN];
            os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
            LOGGER_ERROR(LOGGER, LOG_MODULE_CTRLFILE, "can not extend file %s to size %lu MB, error = %s",
                 file_name, size, err_info);
            status = CM_ERROR;
        }
    }

    ret = os_close_file(file);
    ut_a(ret);

    return status;
}

db_space_t::db_space_t()
{
    space_name[0] = '\0';
    space_id = DB_SPACE_INVALID_ID;
    page_hwm = 0;
    size = 0;
    max_size = 0;
    flags = 0;
    mutex_create(&mutex);
    UT_LIST_INIT(data_file_list);
}

byte* db_space_t::serialize(byte* buf_ptr, byte* end_ptr)
{
    byte* ptr = buf_ptr;

    mutex_enter(&mutex);

    memcpy(ptr, space_name, strlen(space_name) + 1);
    ptr += DB_OBJECT_NAME_MAX_LEN;
    mach_write_to_4(ptr, space_id);
    ptr += 4;
    mach_write_to_4(ptr, page_hwm);
    ptr += 4;
    mach_write_to_8(ptr, size);
    ptr += 8;
    mach_write_to_8(ptr, max_size);
    ptr += 8;
    mach_write_to_4(ptr, flags);
    ptr += 4;

    mutex_exit(&mutex);

    return ptr;
}

byte* db_space_t::deserialize(byte* buf_ptr, byte* end_ptr)
{
    byte* ptr = buf_ptr;

    size_t space_name_len = strlen((const char*)ptr);
    if (space_name_len >= DB_OBJECT_NAME_MAX_LEN) {
        //ut_error;
        return NULL;
    }

    mutex_enter(&mutex);

    memcpy(space_name, (const char*)ptr, strlen((const char*)ptr) + 1);
    ptr += DB_OBJECT_NAME_MAX_LEN;
    space_id = mach_read_from_4(ptr);
    ptr += 4;
    page_hwm = mach_read_from_4(ptr);
    ptr += 4;
    size = mach_read_from_8(ptr);
    ptr += 8;
    max_size = mach_read_from_8(ptr);
    ptr += 8;
    flags = mach_read_from_4(ptr);
    ptr += 4;

    mutex_exit(&mutex);

    return ptr;
}

status_t db_space_t::create_data_files(bool32 is_extend_file)
{
    status_t err = CM_SUCCESS;
    db_data_file_t* data_file;

    data_file = UT_LIST_GET_FIRST(data_file_list);
    while(data_file) {
        CM_RETURN_IF_ERROR(data_file->delete_data_file());
        CM_RETURN_IF_ERROR(data_file->create_data_file(is_extend_file));

        data_file = UT_LIST_GET_NEXT(list_node, data_file);
    }

    return CM_SUCCESS;
}

status_t db_space_t::add_data_file(db_data_file_t* data_file)
{
    if (data_file == NULL) {
        return CM_ERROR;
    }

    db_data_file_t* prev_data_file;

    mutex_enter(&mutex);

    UT_LIST_ADD_LAST(list_node, data_file_list, data_file);
    prev_data_file = UT_LIST_GET_PREV(list_node, data_file);
    if (prev_data_file) {
        prev_data_file->autoextend = FALSE;
    }

    size += data_file->size;
    max_size += data_file->max_size;
    is_autoextend = data_file->autoextend;

    mutex_exit(&mutex);

    return CM_SUCCESS;
}


status_t db_ctrl_t::set_database_info(char* db_name, CHARSET_INFO* charset)
{
    if (db_name == NULL || strlen(db_name) >= DB_OBJECT_NAME_MAX_LEN) {
        return CM_ERROR;
    }

    if (charset == NULL) {
        return CM_ERROR;
    }

    memcpy(database_name, db_name, DB_OBJECT_NAME_MAX_LEN);
    database_name[DB_OBJECT_NAME_MAX_LEN - 1] = '\0';

    charset_info = charset;

    return CM_SUCCESS;
}

db_ctrl_t::db_ctrl_t()
{
    mutex_create(&mutex);
    io_in_progress = FALSE;

    version = DB_CTRL_FILE_VERSION;
    ver_num = DB_CTRL_FILE_VERSION_NUM;

    init_time = 0;
    start_time = 0;
    scn = 0;
    database_name[0] = '\0';
    charset_info = NULL;
    redo_data_file_count = 0;
    undo_data_file_count = 0;
    temp_data_file_count = 0;
    system_space_count = 0;
    user_space_count = 0;
    user_space_data_file_count = 0;

    for (uint32 i = 0; i < DB_SYSTEM_SPACE_MAX_COUNT; i++) {
        system_spaces[i].space_id = DB_SPACE_INVALID_ID;
    }
    for (uint32 i = 0; i < DB_USER_SPACE_MAX_COUNT; i++) {
        user_spaces[i].space_id = DB_SPACE_INVALID_ID;
    }
    for (uint32 i = 0; i < DB_SYSTEM_DATA_FILE_MAX_COUNT; i++) {
        system_data_files[i].node_id = DB_DATA_FILNODE_INVALID_ID;
        system_data_files[i].space_id = DB_SPACE_INVALID_ID;
    }
    for (uint32 i = 0; i < DB_USER_DATA_FILE_MAX_COUNT; i++) {
        user_data_files[i].node_id = DB_DATA_FILNODE_INVALID_ID;
        user_data_files[i].space_id = DB_SPACE_INVALID_ID;
    }

    add_system_space("system", DB_SYSTEM_SPACE_ID);
    add_system_space("systrans", DB_SYSTRANS_SPACE_ID);
    add_system_space("sysaux", DB_SYSAUX_SPACE_ID);
    add_system_space("redo", DB_REDO_SPACE_ID);
    for (uint32 i = 0; i < DB_UNDO_SPACE_MAX_COUNT; i++) {
        char undo_space_name[DB_OBJECT_NAME_MAX_LEN];
        sprintf_s(undo_space_name, DB_OBJECT_NAME_MAX_LEN, "undo%02u", i+1);
        add_system_space(undo_space_name, DB_UNDO_START_SPACE_ID + i);
    }
    add_system_space("temporary", DB_TEMP_SPACE_ID);
    add_system_space("dictionary", DB_DICT_SPACE_ID);
    add_system_space("dbwr", DB_DBWR_SPACE_ID);
}

status_t db_ctrl_t::add_system_space(char* space_name, uint32 space_id)
{
    if (space_id >= DB_SYSTEM_SPACE_MAX_COUNT || strlen(space_name) >= DB_OBJECT_NAME_MAX_LEN) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_CTRLFILE,
            "add_system_space: Error, invalid table space, name %s id %lu",
            space_name, space_id);
        return CM_ERROR;
    }

    if (system_spaces[space_id].space_id != DB_SPACE_INVALID_ID) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_CTRLFILE,
            "add_system_space: Error, system space id already exists, name %s id %lu",
            space_name, space_id);
        return CM_ERROR;
    }

    for (uint32 i = 0; i < DB_SYSTEM_SPACE_MAX_COUNT; i++) {
        if (system_spaces[i].space_id == DB_SPACE_INVALID_ID) {
            continue;
        }
        if (strlen(space_name) == strlen(system_spaces[i].space_name) &&
            strcmp(space_name, system_spaces[i].space_name) == 0) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_CTRLFILE,
                "add_system_space: Error, name of system space already exists, name %s id %lu",
                space_name, space_id);
            return CM_ERROR;
        }
    }

    system_spaces[space_id].space_id = space_id;
    ut_a(system_spaces[space_id].space_name[0] == '\0');
    memcpy(system_spaces[space_id].space_name, space_name, strlen(space_name) + 1);
    system_space_count++;

    return CM_SUCCESS;
}

status_t db_ctrl_t::add_user_space(char* space_name)
{
    if (user_space_count >= DB_USER_SPACE_MAX_COUNT) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_CTRLFILE,
            "add_user_space: Error, user space has reached the maximum limit");
        return CM_ERROR;
    }
    if (strlen(space_name) > DB_OBJECT_NAME_MAX_LEN) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_CTRLFILE,
            "add_user_space: Error, invalid user space name %s", space_name);
        return CM_ERROR;
    }

    // system space
    for (uint32 i = 0; i < DB_SYSTEM_SPACE_MAX_COUNT; i++) {
        if (strlen(space_name) == strlen(system_spaces[i].space_name) &&
            strcmp(space_name, system_spaces[i].space_name) == 0) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_CTRLFILE,
                "add_user_space: Error, name of system space already exists, name %s",
                space_name);
            return CM_ERROR;
        }
    }

    // user space
    uint32 space_id = DB_SPACE_INVALID_ID;
    for (uint32 i = 0; i < DB_USER_SPACE_MAX_COUNT; i++) {
        if (strlen(space_name) == strlen(user_spaces[i].space_name) &&
            strcmp(space_name, user_spaces[i].space_name) == 0) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_CTRLFILE,
                "add_user_space: Error, name of user space already exists, name %s", space_name);
            return CM_ERROR;
        }
        if (space_id == DB_SPACE_INVALID_ID &&
            user_spaces[i].space_id == DB_SPACE_INVALID_ID) {
            space_id = i;
        }
    }
    if (space_id == DB_SPACE_INVALID_ID) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_CTRLFILE,
            "add_user_space: Error, user space has reached the maximum limit");
        return CM_ERROR;
    }

    user_spaces[space_id].space_id = space_id + DB_USER_SPACE_FIRST_ID;
    memcpy(user_spaces[space_id].space_name, space_name, strlen(space_name) + 1);
    user_space_count++;


    return CM_SUCCESS;
}


status_t db_ctrl_t::set_data_file(db_data_file_t* file, uint32 space_id, uint32 node_id,
    char* name, uint64 size, uint64 max_size, bool32 autoextend)
{
    if (file->node_id != DB_DATA_FILNODE_INVALID_ID) {
        return CM_ERROR;
    }
    if (file->space_id != DB_SPACE_INVALID_ID) {
        return CM_ERROR;
    }
    if (strlen(name) >= DB_DATA_FILE_NAME_MAX_LEN) {
        return CM_ERROR;
    }

    for (uint32 i = 0; i < DB_SYSTEM_DATA_FILE_MAX_COUNT; i++) {
        if (system_data_files[i].node_id == DB_DATA_FILNODE_INVALID_ID) {
            continue;
        }
        if (strlen(system_data_files[i].file_name) == strlen(name) &&
            strcmp(system_data_files[i].file_name, name) == 0) {
            return CM_ERROR;
        }
    }
    for (uint32 i = 0; i < DB_USER_DATA_FILE_MAX_COUNT; i++) {
        if (user_data_files[i].node_id == DB_DATA_FILNODE_INVALID_ID) {
            continue;
        }
        if (strlen(user_data_files[i].file_name) == strlen(name) &&
            strcmp(user_data_files[i].file_name, name) == 0) {
            return CM_ERROR;
        }
    }

    file->space_id = space_id;
    file->node_id = node_id;
    file->size = size;
    file->max_size = max_size;
    file->autoextend = autoextend;
    memcpy(file->file_name, name, strlen(name) + 1);

    return CM_SUCCESS;
}

status_t db_ctrl_t::add_dbwr_file(char* file_name, uint64 size)
{
    db_data_file_t *data_file = &system_data_files[DB_DBWR_FILNODE_ID];

    status_t err = set_data_file(data_file, DB_DBWR_SPACE_ID, DB_DBWR_FILNODE_ID, file_name, size, size, FALSE);
    CM_RETURN_IF_ERROR(err);

    //
    db_space_t* db_space = &system_spaces[DB_DBWR_SPACE_ID];
    db_space->add_data_file(data_file);

    return CM_SUCCESS;
}

status_t db_ctrl_t::add_systrans_file(char* file_name, uint32 rseg_count)
{
    db_data_file_t *data_file = &system_data_files[DB_SYSTRANS_FILNODE_ID];

    if (rseg_count < TRX_RSEG_MIN_COUNT) {
        rseg_count = TRX_RSEG_MIN_COUNT;
    }
    if (rseg_count > TRX_RSEG_MAX_COUNT) {
        rseg_count = TRX_RSEG_MAX_COUNT;
    }

    uint64 size = (FSP_FIRST_RSEG_PAGE_NO + rseg_count * TRX_SLOT_PAGE_COUNT_PER_RSEG) * UNIV_SYSTRANS_PAGE_SIZE;
    status_t err = set_data_file(data_file, DB_SYSTRANS_SPACE_ID, DB_SYSTRANS_FILNODE_ID, file_name, size, rseg_count, FALSE);
    CM_RETURN_IF_ERROR(err);

    //
    db_space_t* db_space = &system_spaces[DB_SYSTRANS_SPACE_ID];
    db_space->add_data_file(data_file);

    return CM_SUCCESS;
}

status_t db_ctrl_t::add_redo_file(char* file_name, uint64 size)
{
    if (redo_data_file_count >= DB_REDO_FILE_MAX_COUNT) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_CTRLFILE,
            "add_redo_file: Error, count of REDO file has reached the maximum limit");
        return CM_ERROR;
    }


    uint32 redo_filnode_id = DB_REDO_START_FILNODE_ID + redo_data_file_count;
    db_data_file_t *data_file = &system_data_files[redo_filnode_id];

    status_t err = set_data_file(data_file, DB_REDO_SPACE_ID, redo_filnode_id, file_name, size, size, FALSE);
    CM_RETURN_IF_ERROR(err);

    redo_data_file_count++;

    //
    db_space_t* db_space = &system_spaces[DB_REDO_SPACE_ID];
    db_space->add_data_file(data_file);

    return CM_SUCCESS;
}

status_t db_ctrl_t::add_undo_file(char* file_name, uint64 size, uint64 max_size)
{
    if (undo_data_file_count >= DB_UNDO_FILE_MAX_COUNT) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_CTRLFILE,
            "add_undo_file: Error, count of UNDO file has reached the maximum limit");
        return CM_ERROR;
    }

    uint32 undo_filnode_id = DB_UNDO_START_FILNODE_ID + undo_data_file_count;
    uint32 undo_space_id = DB_UNDO_START_SPACE_ID + undo_data_file_count;
    db_data_file_t *data_file = &system_data_files[undo_filnode_id];

    status_t err = set_data_file(data_file, undo_space_id, undo_filnode_id, file_name, size, max_size, FALSE);
    CM_RETURN_IF_ERROR(err);

    undo_data_file_count++;

    db_space_t* db_space = &system_spaces[undo_space_id];
    db_space->add_data_file(data_file);

    return CM_SUCCESS;
}

status_t db_ctrl_t::add_system_file(char* file_name, uint64 size, uint64 max_size, bool32 autoextend)
{
    db_data_file_t* data_file = &system_data_files[DB_SYSTEM_FILNODE_ID];

    if (size < FSP_DYNAMIC_FIRST_PAGE_NO * UNIV_PAGE_SIZE) {
        size = FSP_DYNAMIC_FIRST_PAGE_NO * UNIV_PAGE_SIZE;
    }
    status_t err = set_data_file(data_file, DB_SYSTEM_SPACE_ID, DB_SYSTEM_FILNODE_ID, file_name, size, max_size, autoextend);
    CM_RETURN_IF_ERROR(err);

    db_space_t* db_space = &system_spaces[DB_SYSTEM_SPACE_ID];
    db_space->add_data_file(data_file);

    return CM_SUCCESS;
}

status_t db_ctrl_t::add_temp_file(char* file_name, uint64 size, uint64 max_size)
{
    if (temp_data_file_count >= DB_TEMP_FILE_MAX_COUNT) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_CTRLFILE,
            "add_temp_file: Error, count of TEMP file has reached the maximum limit");
        return CM_ERROR;
    }

    uint32 temp_filnode_id = DB_TEMP_START_FILNODE_ID + temp_data_file_count;
    db_data_file_t *data_file = &system_data_files[temp_filnode_id];

    status_t err = set_data_file(data_file, DB_TEMP_SPACE_ID, temp_filnode_id, file_name, size, max_size, FALSE);
    CM_RETURN_IF_ERROR(err);

    temp_data_file_count++;

    //
    db_space_t* db_space = &system_spaces[DB_TEMP_SPACE_ID];
    db_space->add_data_file(data_file);

    return CM_SUCCESS;
}

status_t db_ctrl_t::add_user_space_file(char* space_name, char* file_name, uint64 size, uint64 max_size, bool32 autoextend)
{
    if (strlen(file_name) >= DB_DATA_FILE_NAME_MAX_LEN) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_CTRLFILE,
            "add_user_space_file: Error, name of data file has reached the maximum limit %s", file_name);
        return CM_ERROR;
    }

    if (user_space_data_file_count >= DB_SPACE_DATA_FILE_MAX_COUNT) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_CTRLFILE,
            "add_user_space_file: Error, data file has reached the maximum limit");
        return CM_ERROR;
    }

    // check user space
    db_space_t* db_space = NULL;
    for (uint32 i = 0; i < DB_USER_SPACE_MAX_COUNT; i++) {
        if (strlen(space_name) == strlen(user_spaces[i].space_name) &&
            strcmp(space_name, user_spaces[i].space_name) == 0) {
            db_space = &user_spaces[i];
            break;
        }
    }
    if (db_space == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_CTRLFILE, "add_user_space_file: Error, invalid space name %s", space_name);
        return CM_ERROR;
    }

    // find a free cell of data file array
    uint32 node_id = DB_DATA_FILNODE_INVALID_ID;
    for (uint32 i = 0; i < DB_USER_DATA_FILE_MAX_COUNT; i++) {
        if (user_data_files[i].node_id == DB_DATA_FILNODE_INVALID_ID) {
            node_id = i;
            break;
        }
    }
    if (node_id == DB_DATA_FILNODE_INVALID_ID) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_CTRLFILE,
            "add_user_space_file: Error, data file has reached the maximum limit");
        return CM_ERROR;
    }

    db_data_file_t *data_file = &user_data_files[node_id];
    set_data_file(data_file, db_space->space_id, node_id + DB_USER_DATA_FILE_FIRST_NODE_ID,
        file_name, size, max_size, autoextend);
    user_space_data_file_count++;

    //
    db_space->add_data_file(data_file);

    return CM_SUCCESS;
}

status_t db_ctrl_t::serialize_core(byte* buf, uint32 buf_size)
{
    byte* ptr = buf;

    // len(4B) + magic(4B) + checksum(8B) + ctrl file
    mach_write_to_4(ptr, 0);
    ptr += 4; // length
    mach_write_to_4(ptr, DB_CTRL_FILE_MAGIC);
    ptr += 4;
    mach_write_to_8(ptr, 0);
    ptr += 8; // checksum

    // ctrl file
    mach_write_to_8(ptr, this->version);
    ptr += 8;
    mach_write_to_8(ptr, this->ver_num);
    ptr += 8;

    // database name
    memcpy(ptr, database_name, strlen(database_name) + 1);
    ptr += DB_OBJECT_NAME_MAX_LEN;
    memcpy(ptr, charset_info->name, strlen(charset_info->name) + 1);
    ptr += DB_OBJECT_NAME_MAX_LEN;

    mach_write_to_4(ptr, redo_data_file_count);
    ptr += 4;
    mach_write_to_4(ptr, undo_data_file_count);
    ptr += 4;
    mach_write_to_4(ptr, temp_data_file_count);
    ptr += 4;
    mach_write_to_4(ptr, system_space_count);
    ptr += 4;
    mach_write_to_4(ptr, user_space_count);
    ptr += 4;
    mach_write_to_4(ptr, user_space_data_file_count);
    ptr += 4;
    mach_write_to_8(ptr, init_time);
    ptr += 8;
    mach_write_to_8(ptr, start_time);
    ptr += 8;
    mach_write_to_8(ptr, scn);
    ptr += 8;

    // len
    mach_write_to_4(buf, buf_size);

    // check sum
    uint64 checksum = 0;
    for (uint32 i = 16; i < buf_size; i++) {
        checksum += buf[i];
    }
    mach_write_to_8(buf + 8, checksum);

    return CM_SUCCESS;
}

status_t db_ctrl_t::serialize_space(byte* buf_ptr, byte* end_ptr, uint32 space_count, db_space_t* spaces)
{
    byte* ptr = buf_ptr;

    for (uint32 i = 0; i < space_count; i++) {
        db_space_t* db_space = &spaces[i];
        ptr = db_space->serialize(ptr, end_ptr);

        if ((i + 1) % SPACE_COUNT_PER_PAGE == 0) {
            ptr = buf_ptr + UNIV_PAGE_SIZE;
            buf_ptr = ptr;
        }
    }
    return CM_SUCCESS;
}

status_t db_ctrl_t::serialize_data_file(byte* buf_ptr, byte* end_ptr, uint32 file_count, db_data_file_t* files)
{
    byte* ptr = buf_ptr;

    for (uint32 i = 0; i < file_count; i++) {
        db_data_file_t* db_file = &files[i];
        ptr = db_file->serialize(ptr, end_ptr);

        if ((i + 1) % DATA_FILE_COUNT_PER_PAGE == 0) {
            ptr = buf_ptr + UNIV_PAGE_SIZE;
            buf_ptr = ptr;
        }
    }
    return CM_SUCCESS;
}

status_t db_ctrl_t::serialize(byte* buf, uint32 buf_size)
{
    byte *begin_ptr, *end_ptr;

    ut_ad(buf_size == TOTOAL_SIZE);

    begin_ptr = buf + UNIV_PAGE_SIZE;
    end_ptr = begin_ptr + SYSTEM_SPACE_SIZE;
    serialize_space(begin_ptr, end_ptr, DB_SYSTEM_SPACE_MAX_COUNT, system_spaces);

    begin_ptr = end_ptr;
    end_ptr = begin_ptr + USER_SPACE_SIZE;
    serialize_space(begin_ptr, end_ptr, DB_USER_SPACE_MAX_COUNT, user_spaces);

    begin_ptr = end_ptr;
    end_ptr = begin_ptr + SYSTEM_FILE_SIZE;
    serialize_data_file(begin_ptr, end_ptr, DB_SYSTEM_DATA_FILE_MAX_COUNT, system_data_files);

    begin_ptr = end_ptr;
    end_ptr = begin_ptr + USER_FILE_SIZE;
    serialize_data_file(begin_ptr, end_ptr, DB_USER_DATA_FILE_MAX_COUNT, user_data_files);

    serialize_core(buf, TOTOAL_SIZE);

    return CM_SUCCESS;
}


status_t db_ctrl_t::deserialize_core(byte* buf, uint32 buf_size)
{
    uint64 checksum = 0;

    // check magic
    if (mach_read_from_4(buf + 4) != DB_CTRL_FILE_MAGIC) {
        LOGGER_FATAL(LOGGER, LOG_MODULE_CTRLFILE,
            "invalid control file, wrong magic = %lu", mach_read_from_4(buf));
        return CM_ERROR;
    }

    // check checksum
    uint32 file_size = mach_read_from_4(buf);
    for (uint32 i = 16; i < file_size; i++) {
        checksum += buf[i];
    }
    if (checksum != mach_read_from_8(buf + 8)) {
        LOGGER_FATAL(LOGGER, LOG_MODULE_CTRLFILE, "checksum mismatch, damaged control file");
        return CM_ERROR;
    }

    // ctrl file
    byte* ptr = buf + 16; // magic len + file_size len + checksum len

    //
    version = mach_read_from_8(ptr);
    ptr += 8;
    ver_num = mach_read_from_8(ptr);
    ptr += 8;

    // database name
    memcpy(database_name, (const char*)ptr, strlen((const char*)ptr) + 1);
    ptr += DB_OBJECT_NAME_MAX_LEN;

    char charset_name[DB_OBJECT_NAME_MAX_LEN];
    memcpy(charset_name, (const char*)ptr, strlen((const char*)ptr) + 1);
    ptr += DB_OBJECT_NAME_MAX_LEN;
    charset_info = db_ctrl_get_charset_info(charset_name);
    if (charset_info == NULL) {
        LOGGER_FATAL(LOGGER, LOG_MODULE_CTRLFILE, "invalid charset name %s, damaged control file", charset_name);
        return CM_ERROR;
    }

    redo_data_file_count = mach_read_from_4(ptr);
    ptr += 4;
    undo_data_file_count = mach_read_from_4(ptr);
    ptr += 4;
    temp_data_file_count = mach_read_from_4(ptr);
    ptr += 4;
    system_space_count = mach_read_from_4(ptr);
    ptr += 4;
    user_space_count = mach_read_from_4(ptr);
    ptr += 4;
    user_space_data_file_count = mach_read_from_4(ptr);
    ptr += 4;
    init_time = mach_read_from_8(ptr);
    ptr += 8;
    start_time = mach_read_from_8(ptr);
    ptr += 8;
    scn = mach_read_from_8(ptr);
    ptr += 8;

    return CM_SUCCESS;
}

status_t db_ctrl_t::deserialize_space(byte* buf_ptr, byte* end_ptr, uint32 space_count, db_space_t* spaces)
{
    byte* ptr = buf_ptr;

    for (uint32 i = 0; i < space_count; i++) {
        db_space_t* db_space = &spaces[i];
        ptr = db_space->deserialize(ptr, end_ptr);
        if (ptr == NULL) {
            return CM_ERROR;
        }

        if ((i + 1) % SPACE_COUNT_PER_PAGE == 0) {
            ptr = buf_ptr + UNIV_PAGE_SIZE;
            buf_ptr = ptr;
        }
    }

    return CM_SUCCESS;
}

status_t db_ctrl_t::deserialize_data_file(byte* buf_ptr, byte* end_ptr, uint32 file_count, db_data_file_t* files)
{
    byte* ptr = buf_ptr;

    for (uint32 i = 0; i < file_count; i++) {
        db_data_file_t* db_file = &files[i];
        ptr = db_file->deserialize(ptr, end_ptr);
        if (ptr == NULL) {
            return CM_ERROR;
        }

        if ((i + 1) % DATA_FILE_COUNT_PER_PAGE == 0) {
            ptr = buf_ptr + UNIV_PAGE_SIZE;
            buf_ptr = ptr;
        }
    }

    return CM_SUCCESS;
}


status_t db_ctrl_t::deserialize(byte* buf, uint32 buf_size)
{
    byte *begin_ptr, *end_ptr;
    status_t err;

    ut_ad(buf_size == TOTOAL_SIZE);
    err = deserialize_core(buf, buf_size);
    CM_RETURN_IF_ERROR(err);

    begin_ptr = buf + UNIV_PAGE_SIZE;
    end_ptr = begin_ptr + SYSTEM_SPACE_SIZE;
    err = deserialize_space(begin_ptr, end_ptr, DB_SYSTEM_SPACE_MAX_COUNT, system_spaces);
    CM_RETURN_IF_ERROR(err);

    begin_ptr = end_ptr;
    end_ptr = begin_ptr + USER_SPACE_SIZE;
    err = deserialize_space(begin_ptr, end_ptr, DB_USER_SPACE_MAX_COUNT, user_spaces);
    CM_RETURN_IF_ERROR(err);

    begin_ptr = end_ptr;
    end_ptr = begin_ptr + SYSTEM_FILE_SIZE;
    err = deserialize_data_file(begin_ptr, end_ptr, DB_SYSTEM_DATA_FILE_MAX_COUNT, system_data_files);
    CM_RETURN_IF_ERROR(err);

    begin_ptr = end_ptr;
    end_ptr = begin_ptr + USER_FILE_SIZE;
    err = deserialize_data_file(begin_ptr, end_ptr, DB_USER_DATA_FILE_MAX_COUNT, user_data_files);
    CM_RETURN_IF_ERROR(err);

    //
    db_space_t* space;
    db_data_file_t* data_file;

    for (uint32 i = 0; i < DB_SYSTEM_DATA_FILE_MAX_COUNT; i++) {
        data_file = &system_data_files[i];
        if (data_file->node_id == DB_DATA_FILNODE_INVALID_ID) {
            continue;
        }

        space = get_system_space_by_space_id(data_file->space_id);
        ut_ad(space);

        UT_LIST_ADD_LAST(list_node, space->data_file_list, data_file);
    }

    for (uint32 i = 0; i < DB_USER_DATA_FILE_MAX_COUNT; i++) {
        data_file = &user_data_files[i];
        if (data_file->node_id == DB_DATA_FILNODE_INVALID_ID) {
            continue;
        }

        space = get_user_space_by_space_id(data_file->space_id);
        ut_ad(space);

        UT_LIST_ADD_LAST(list_node, space->data_file_list, data_file);
    }

    return CM_SUCCESS;
}

db_space_t* db_ctrl_t::get_system_space_by_space_id(uint32 space_id)
{
    db_space_t* space;

    for (uint32 i = 0; i < DB_SYSTEM_SPACE_MAX_COUNT; i++) {
        space = &system_spaces[i];
        if (space->space_id == space_id) {
            return space;
        }
    }

    return NULL;
}

db_space_t* db_ctrl_t::get_user_space_by_space_id(uint32 space_id)
{
    db_space_t* space;

    if (space_id < DB_USER_SPACE_FIRST_ID) {
        return NULL;
    }

    for (uint32 i = 0; i < DB_USER_SPACE_MAX_COUNT; i++) {
        space = &user_spaces[i];
        if (space->space_id == space_id) {
            return space;
        }
    }

    return NULL;
}

db_data_file_t* db_ctrl_t::get_data_file_by_node_id(uint32 node_id)
{
    if (node_id < DB_SYSTEM_DATA_FILE_MAX_COUNT) {
        return &system_data_files[node_id];
    } else if (node_id < DB_SYSTEM_DATA_FILE_MAX_COUNT + DB_USER_DATA_FILE_MAX_COUNT) {
        return &user_data_files[node_id - DB_SYSTEM_DATA_FILE_MAX_COUNT];
    }

    return NULL;
}

status_t db_ctrl_t::save_to_ctrl_file(char* name, byte* buf, uint32 buf_size)
{
    status_t  err = CM_ERROR;
    os_file_t file = OS_FILE_INVALID_HANDLE;

    bool32 ret = os_open_file(name, OS_FILE_CREATE, 0, &file);
    if (!ret) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_CTRLFILE, "write_ctrl_file: failed to create file, name = %s", name);
        goto err_exit;
    }
    ret = os_pwrite_file(file, 0, buf, (uint32)buf_size);
    if (!ret) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_CTRLFILE, "write_ctrl_file: failed to write file, name = %s", name);
        goto err_exit;
    }
    ret = os_fsync_file(file);
    if (!ret) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_CTRLFILE, "write_ctrl_file: failed to sync file, name = %s", name);
        goto err_exit;
    }

    err = CM_SUCCESS;

err_exit:

    if (file != OS_FILE_INVALID_HANDLE) {
        os_close_file(file);
    }

    return err;
}

status_t db_ctrl_t::read_from_ctrl_file(char* name, byte* buf, uint32 buf_size)
{
    status_t  err = CM_ERROR;
    os_file_t file = OS_FILE_INVALID_HANDLE;
    uint32 read_bytes = 0;

    if (!os_open_file(name, OS_FILE_OPEN, 0, &file)) {
        LOGGER_FATAL(LOGGER, LOG_MODULE_CTRLFILE, "read_from_ctrl_file: failed to open ctrl file, name=%s", name);
        goto err_exit;
    }

    if (!os_pread_file(file, 0, buf, buf_size, &read_bytes)) {
        LOGGER_FATAL(LOGGER, LOG_MODULE_CTRLFILE, "read_from_ctrl_file: failed to read ctrl file, name=%s", name);
        goto err_exit;
    }

    err = CM_SUCCESS;

err_exit:

    if (file != OS_FILE_INVALID_HANDLE) {
        os_close_file(file);
    }

    return err;
}


static status_t write_ctrl_file(char *name, db_ctrl_t *ctrl)
{
    status_t err;

    memset(g_ctrl_file_data_buffer, 0x00, TOTOAL_SIZE);

    err = ctrl->serialize(g_ctrl_file_data_buffer, TOTOAL_SIZE);
    CM_RETURN_IF_ERROR(err);

    err = ctrl->save_to_ctrl_file(name, g_ctrl_file_data_buffer, TOTOAL_SIZE);
    CM_RETURN_IF_ERROR(err);

    return CM_SUCCESS;
}

status_t read_ctrl_file(char* name, db_ctrl_t* ctrl)
{
    status_t err;

    memset(g_ctrl_file_data_buffer, 0x00, TOTOAL_SIZE);

    err = ctrl->read_from_ctrl_file(name, g_ctrl_file_data_buffer, TOTOAL_SIZE);
    CM_RETURN_IF_ERROR(err);

    err = ctrl->deserialize(g_ctrl_file_data_buffer, TOTOAL_SIZE);
    CM_RETURN_IF_ERROR(err);

    return CM_SUCCESS;
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
    sprintf_s(file->file_name, strlen(name) + 1, "%s", name);
    file->file_name[strlen(name)] = '\0';
}

status_t db_ctrl_add_system_file(char* file_name, uint64 size, uint64 max_size, bool32 autoextend)
{
    return srv_ctrl_file->add_system_file(file_name, size, max_size, autoextend);
}

status_t db_ctrl_add_systrans_file(char* file_name, uint32 rseg_count)
{
    return srv_ctrl_file->add_systrans_file(file_name, rseg_count);
}

status_t db_ctrl_add_redo_file(char* file_name, uint64 size)
{
    return srv_ctrl_file->add_redo_file(file_name, size);
}

status_t db_ctrl_add_dbwr_file(char* file_name, uint64 size)
{
    return srv_ctrl_file->add_dbwr_file(file_name, size);
}

status_t db_ctrl_add_undo_file(char* file_name, uint64 size, uint64 max_size)
{
    return srv_ctrl_file->add_undo_file(file_name, size, max_size);
}

status_t db_ctrl_add_temp_file(char* file_name, uint64 size, uint64 max_size)
{
    return srv_ctrl_file->add_temp_file(file_name, size, max_size);
}

status_t db_ctrl_add_user_space(char* space_name)
{
    return srv_ctrl_file->add_user_space(space_name);
}

status_t db_ctrl_add_user_space_file(char* space_name, char* file_name, uint64 size, uint64 max_size, bool32 autoextend)
{
    return srv_ctrl_file->add_user_space_file(space_name, file_name, size, max_size, autoextend);
}

status_t srv_create_ctrl_files()
{
    char file_name[CM_FILE_PATH_BUF_SIZE];

    for (uint32 i = 1; i <= 3; i++) {
        sprintf_s(file_name, CM_FILE_PATH_MAX_LEN, "%s%c%s%d", srv_data_home, SRV_PATH_SEPARATOR, "ctrl", i);

        /* Remove any old ctrl files. */
        os_del_file(file_name);

        //
        CM_RETURN_IF_ERROR(write_ctrl_file(file_name, srv_ctrl_file));
    }

    return CM_SUCCESS;
}

status_t srv_create_data_files_at_createdatabase()
{
    db_space_t* space;

    space = srv_ctrl_file->get_system_space_by_space_id(DB_SYSTEM_SPACE_ID);
    CM_RETURN_IF_ERROR(space->create_data_files(FALSE));

    space = srv_ctrl_file->get_system_space_by_space_id(DB_SYSTRANS_SPACE_ID);
    CM_RETURN_IF_ERROR(space->create_data_files(FALSE));

    space = srv_ctrl_file->get_system_space_by_space_id(DB_SYSAUX_SPACE_ID);
    CM_RETURN_IF_ERROR(space->create_data_files(FALSE));

    space = srv_ctrl_file->get_system_space_by_space_id(DB_DBWR_SPACE_ID);
    CM_RETURN_IF_ERROR(space->create_data_files(FALSE));

    space = srv_ctrl_file->get_system_space_by_space_id(DB_REDO_SPACE_ID);
    CM_RETURN_IF_ERROR(space->create_data_files(TRUE));

    space = srv_ctrl_file->get_system_space_by_space_id(DB_TEMP_SPACE_ID);
    CM_RETURN_IF_ERROR(space->create_data_files(FALSE));

    space = srv_ctrl_file->get_system_space_by_space_id(DB_DICT_SPACE_ID);
    CM_RETURN_IF_ERROR(space->create_data_files(FALSE));

    for (uint32 i = 0; i < DB_UNDO_SPACE_MAX_COUNT; i++) {
        space = srv_ctrl_file->get_system_space_by_space_id(DB_UNDO_START_SPACE_ID + i);
        CM_RETURN_IF_ERROR(space->create_data_files(FALSE));
    }

    for (uint32 i = 0; i < DB_USER_SPACE_MAX_COUNT; i++) {
        space = srv_ctrl_file->get_user_space_by_space_id(DB_USER_SPACE_FIRST_ID + i);
        if (space) {
            CM_RETURN_IF_ERROR(space->create_data_files(FALSE));
        }
    }

    return CM_SUCCESS;
}

status_t db_ctrl_create_database(char* database_name, char* charset_name)
{
    if (database_name == NULL || charset_name == NULL) {
        return CM_ERROR;
    }

    CHARSET_INFO* charset_info = db_ctrl_get_charset_info(charset_name);
    if (charset_info == NULL) {
        LOGGER_FATAL(LOGGER, LOG_MODULE_CTRLFILE, "invalid charset name %s", charset_name);
        return CM_ERROR;
    }

    srv_ctrl_file = new db_ctrl_t();

    return srv_ctrl_file->set_database_info(database_name, charset_info);
}

CHARSET_INFO* db_ctrl_get_charset_info(char* charset_name)
{
    CHARSET_INFO* charset_info = NULL;

    for (uint32 i = 0; srv_db_charset_info[i].name; i++) {
        if (strcmp((const char *)charset_name, (const char *)srv_db_charset_info[i].name) == 0) {
            charset_info = srv_db_charset_info[i].charset_info;
            break;
        }
    }

    return charset_info;
}

