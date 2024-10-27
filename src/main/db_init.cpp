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

uint32 log_level = LOG_LEVEL_ALL;

status_t create_database(char* base_dir)
{
    status_t err = CM_SUCCESS;

    // 1. only for windows platform
    os_file_init();

    // 2.
    char *log_path = "D:\\MyWork\\cos\\data\\";
    LOGGER.log_init(log_level, log_path, "initdb");
    LOGGER.set_module_log_level(LOG_MODULE_REDO, LOG_LEVEL_ALL);

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

    // 5.
    db_ctrl_createdatabase("cosdb", "utf8mb4_bin");

    db_ctrl_add_system("D:\\MyWork\\cos\\data\\system.dbf", 5 * 1024 * 1024, 100 * 1024 * 1024, TRUE);

    db_ctrl_add_redo("D:\\MyWork\\cos\\data\\redo01", 4 * 1024 * 1024, 4 * 1024 * 1024, FALSE);
    db_ctrl_add_redo("D:\\MyWork\\cos\\data\\redo02", 4 * 1024 * 1024, 4 * 1024 * 1024, FALSE);
    db_ctrl_add_redo("D:\\MyWork\\cos\\data\\redo03", 4 * 1024 * 1024, 4 * 1024 * 1024, FALSE);

    db_ctrl_add_undo("D:\\MyWork\\cos\\data\\undo01", 4 * 1024 * 1024, 4 * 1024 * 1024, FALSE);
    db_ctrl_add_undo("D:\\MyWork\\cos\\data\\undo02", 4 * 1024 * 1024, 4 * 1024 * 1024, FALSE);

    db_ctrl_add_dbwr("D:\\MyWork\\cos\\data\\dbwr", 4 * 1024 * 1024);

    db_ctrl_add_temp("D:\\MyWork\\cos\\data\\temp01", 4 * 1024 * 1024, 8 * 1024 * 1024, TRUE);

    db_ctrl_add_user_space("default_user_space");
    db_ctrl_add_space_file("default_user_space",
        "D:\\MyWork\\cos\\data\\user01", 4 * 1024 * 1024, 100 * 1024 * 1024, FALSE);
    db_ctrl_add_space_file("default_user_space",
        "D:\\MyWork\\cos\\data\\user02", 4 * 1024 * 1024, 100 * 1024 * 1024, TRUE);

    err = server_open_or_create_database(base_dir, &attr);

    return err;
}

status_t start_database(char* base_dir)
{
    status_t err = CM_SUCCESS;

    // 1. only for windows platform
    os_file_init();

    // 2.
    char *log_path = "D:\\MyWork\\cos\\data\\";
    LOGGER.log_init(log_level, log_path, "initdb");
    LOGGER.set_module_log_level(LOG_MODULE_REDO, LOG_LEVEL_ALL);

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
    const attribute_t attr = { 0 };
    //err = initialize_guc_options(config_file, &attr);
    CM_RETURN_IF_ERROR(err);

    //err = server_open_or_create_database(base_dir, &attr);

    return err;
}



int main(int argc, const char *argv[])
{
    status_t err;
    char*    base_dir = "D:\\MyWork\\cos";

    //log_level = LOG_LEVEL_CRITICAL | LOG_LEVEL_INFO;
    log_level = LOG_LEVEL_ALL;

    cm_timer_t* timer = g_timer();
    cm_start_timer(timer);

    err = create_database(base_dir);
    if (err != CM_SUCCESS) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_COMMON, "Failed to create database, Service exited");
        goto err_exit;
    }

    os_thread_sleep(1000000);
    LOGGER.log_file_flush();
    
    dict_table_t* table;
    uint32 status = dict_get_table_from_cache_by_name("SYS_TABLES", &table);
    if (table == NULL) {
        if (status & DICT_TABLE_NOT_FOUND) {
        }
        goto err_exit;
    }

    que_sess_t sess;
    uint32 stack_size = 1024 * 1024;
    que_sess_init(&sess, 0, stack_size);

    insert_node_t insert_node;
    insert_node.type = 0;
    insert_node.table = table;

    err = knl_insert(&sess, &insert_node);
    if (err != CM_SUCCESS) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_COMMON, "Failed to knl_insert");
        goto err_exit;
    }

    dict_release_table(table);



    err = start_database(base_dir);
    if (err != CM_SUCCESS) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_COMMON, "Failed to start database, Service exited");
        goto err_exit;
    }

    while (TRUE) {
        os_thread_sleep(100000);
    }

err_exit:

    return 0;
}

