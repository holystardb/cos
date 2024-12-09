#include "cm_type.h"
#include "cm_memory.h"
#include "cm_getopt.h"
#include "cm_log.h"
#include "cm_dbug.h"
#include "cm_file.h"
#include "cm_date.h"
#include "cm_timer.h"

#include "knl_handler.h"
#include "knl_server.h"
#include "knl_dict.h"
#include "knl_trx.h"
#include "guc.h"

#ifndef HAVE_CHARSET_gb2312
#define HAVE_CHARSET_gb2312
#endif
#ifndef HAVE_CHARSET_gbk
#define HAVE_CHARSET_gbk
#endif
#ifndef HAVE_CHARSET_gb18030
#define HAVE_CHARSET_gb18030
#endif
#ifndef HAVE_CHARSET_utf8
#define HAVE_CHARSET_utf8
#endif
#ifndef HAVE_CHARSET_utf8mb4
#define HAVE_CHARSET_utf8mb4
#endif



status_t create_database(char* base_dir, attribute_t* attr)
{
    db_ctrl_create_database("cosdb", "utf8mb4_bin");

    db_ctrl_add_system("D:\\MyWork\\cos\\data\\system.dbf", 5 * 1024 * 1024, 100 * 1024 * 1024, TRUE);
    db_ctrl_add_systrans("D:\\MyWork\\cos\\data\\systrans", 4 * 1024 * 1024);

    db_ctrl_add_redo("D:\\MyWork\\cos\\data\\redo01", 4 * 1024 * 1024);
    db_ctrl_add_redo("D:\\MyWork\\cos\\data\\redo02", 4 * 1024 * 1024);
    db_ctrl_add_redo("D:\\MyWork\\cos\\data\\redo03", 4 * 1024 * 1024);

    db_ctrl_add_undo("D:\\MyWork\\cos\\data\\undo01", 4 * 1024 * 1024, 4 * 1024 * 1024, FALSE);
    db_ctrl_add_undo("D:\\MyWork\\cos\\data\\undo02", 4 * 1024 * 1024, 4 * 1024 * 1024, FALSE);

    db_ctrl_add_dbwr("D:\\MyWork\\cos\\data\\dbwr", 4 * 1024 * 1024);

    db_ctrl_add_temp("D:\\MyWork\\cos\\data\\temp01", 4 * 1024 * 1024, 8 * 1024 * 1024, TRUE);

    db_ctrl_add_user_space("default_user_space");
    db_ctrl_add_space_file("default_user_space",
        "D:\\MyWork\\cos\\data\\user01", 4 * 1024 * 1024, 100 * 1024 * 1024, FALSE);
    db_ctrl_add_space_file("default_user_space",
        "D:\\MyWork\\cos\\data\\user02", 4 * 1024 * 1024, 100 * 1024 * 1024, TRUE);

    return knl_server_init(base_dir, attr);
}

status_t start_database(char* base_dir, attribute_t* attr)
{
    db_ctrl_init_database();

    return knl_server_init(base_dir, attr);
}

void* log_flush_thread(void *arg)
{
    while (TRUE) {
        LOGGER.log_file_flush();
        os_thread_sleep(1000000);
    }
}




dtuple_t* dict_create_users_tuple(    memory_stack_context_t* mem_stack_ctx, char* user_name, char* password)
{
    dtuple_t* entry;
    dfield_t* dfield;
    byte* ptr;

    entry = dtuple_create(mem_stack_ctx, 10);

    dfield = dtuple_get_nth_field(entry, DICT_COL_SYS_USERS_ID);
    ptr = (byte *)mcontext_stack_push(mem_stack_ctx, 8);
    mach_write_to_8(ptr, 257);
    dfield_set_data(dfield, ptr, 8);

    dfield = dtuple_get_nth_field(entry, DICT_COL_SYS_USERS_NAME);
    ptr = (byte *)mcontext_stack_push(mem_stack_ctx, (uint32)strlen(user_name) + 1);
    memcpy(ptr, user_name, (uint32)strlen(user_name) + 1);
    dfield_set_data(dfield, ptr, (uint32)strlen(user_name) + 1);

    dfield = dtuple_get_nth_field(entry, DICT_COL_SYS_USERS_PASSWORD);
    ptr = (byte *)mcontext_stack_push(mem_stack_ctx, (uint32)strlen(password) + 1);
    memcpy(ptr, password, (uint32)strlen(password) + 1);
    dfield_set_data(dfield, ptr, (uint32)strlen(password) + 1);

    dfield = dtuple_get_nth_field(entry, DICT_COL_SYS_USERS_CREATE_TIME);
    ptr = (byte *)mcontext_stack_push(mem_stack_ctx, 8);
    mach_write_to_8(ptr, 0);
    dfield_set_data(dfield, ptr, 8);

    dfield = dtuple_get_nth_field(entry, DICT_COL_SYS_USERS_EXPIRED_TIME);
    ptr = (byte *)mcontext_stack_push(mem_stack_ctx, 8);
    mach_write_to_8(ptr, 0);
    dfield_set_data(dfield, ptr, 8);

    dfield = dtuple_get_nth_field(entry, DICT_COL_SYS_USERS_LOCKED_TIME);
    ptr = (byte *)mcontext_stack_push(mem_stack_ctx, 8);
    mach_write_to_8(ptr, 0);
    dfield_set_data(dfield, ptr, 8);

    dfield = dtuple_get_nth_field(entry, DICT_COL_SYS_USERS_OPTIONS);
    ptr = (byte *)mcontext_stack_push(mem_stack_ctx, 4);
    mach_write_to_4(ptr, 0);
    dfield_set_data(dfield, ptr, 4);

    dfield = dtuple_get_nth_field(entry, DICT_COL_SYS_USERS_DATA_SPACE);
    ptr = (byte *)mcontext_stack_push(mem_stack_ctx, 4);
    mach_write_to_4(ptr, 0);
    dfield_set_data(dfield, ptr, 4);

    dfield = dtuple_get_nth_field(entry, DICT_COL_SYS_USERS_TEMP_SPACE);
    ptr = (byte *)mcontext_stack_push(mem_stack_ctx, 4);
    mach_write_to_4(ptr, 0);
    dfield_set_data(dfield, ptr, 4);

    dfield = dtuple_get_nth_field(entry, DICT_COL_SYS_USERS_STATUS);
    ptr = (byte *)mcontext_stack_push(mem_stack_ctx, 4);
    mach_write_to_4(ptr, 0);
    dfield_set_data(dfield, ptr, 4);

    return entry;
}

bool32 create_user(que_sess_t* sess)
{
    dict_table_t* sys_users;
    status_t status = dict_get_table(sess, 0, "SYS_USERS", &sys_users);
    if (sys_users == NULL) {
        return FALSE;
    }

    //
    insert_node_t insert_node;
    insert_node.type = 0;
    insert_node.table = sys_users;
    insert_node.heap_row = dict_create_users_tuple(sess->mcontext_stack, "sys", "sys_pwd");

    knl_handler handler;
    scan_cursor_t cursor(sess->mcontext_stack);
    cursor.insert_node = &insert_node;
    status_t err = handler.insert_row(sess, &cursor);
    if (err != CM_SUCCESS) {
        handler.rollback(sess);
        LOGGER_ERROR(LOGGER, LOG_MODULE_COMMON, "Failed to knl_insert");
        return FALSE;
    }

    handler.commit(sess);

    dict_release_table(sys_users);

    que_sess_free(sess);

    return TRUE;
}


int main(int argc, const char *argv[])
{
    char* base_dir = "D:\\MyWork\\cos";
    status_t err = CM_SUCCESS;

    // 1. only for windows platform
    os_file_init();

    // 2.
    bool32 batch_flush = TRUE;
    char* log_path = "D:\\MyWork\\cos\\data\\";
    uint32 log_level = LOG_LEVEL_ALL;
    //log_level = LOG_LEVEL_DEFAULT;

    LOGGER.log_init(log_level, log_path, "initdb", batch_flush);
    //LOGGER.set_module_log_level(LOG_MODULE_REDO, LOG_LEVEL_ALL);
    //LOGGER.set_module_log_level(LOG_MODULE_RECOVERY, LOG_LEVEL_ALL);
    //LOGGER.set_module_log_level(LOG_MODULE_MTR, LOG_LEVEL_ALL);

    if (batch_flush) {
        os_thread_id_t thread_id;
        os_thread_create(log_flush_thread, NULL, &thread_id);
    }

    // 3.
    char err_file[1024];
    sprintf_s(err_file, 1024, "%s\\share\\english\\errmsg.txt", base_dir);
    if (!error_message_init(err_file)) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_COMMON, "Failed to init error messages, Service exited");
        return CM_ERROR;
    }

    // 4. guc
    char config_file[1024];
    sprintf_s(config_file, 1024, "%s\\etc\\server.ini", base_dir);
    attribute_t attr = { 0 };
    err = initialize_guc_options(config_file, &attr);
    CM_RETURN_IF_ERROR(err);

    // 5. timer
    cm_timer_t* timer = g_timer();
    cm_start_timer(timer);

    // 6.
    err = create_database(base_dir, &attr);
    //err = start_database(base_dir, &attr);
    if (err != CM_SUCCESS) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_COMMON, "Failed to create database, Service exited");
        goto err_exit;
    }

    //
    LOGGER.log_file_flush();

    uint32 sess_count = 100;
    uint32 session_stack_size  = SIZE_M(1);
    sess_pool_create(sess_count, session_stack_size);

    //
    que_sess_t* sess = que_sess_alloc();
    create_user(sess);

    que_sess_free(sess);

    while (TRUE) {
        os_thread_sleep(100000);
    }

err_exit:

    return 0;
}

