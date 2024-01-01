#include "cm_type.h"
#include "cm_memory.h"
#include "cm_getopt.h"
#include "cm_log.h"
#include "cm_dbug.h"
#include "cm_file.h"

#include "knl_handler.h"
#include "knl_server.h"
#include "guc.h"


int main(int argc, const char *argv[])
{
    os_file_init(); // only for windows platform

    srv_buf_pool_size = 100 * 1024 * 1024; // 100MB
    srv_buf_pool_instances = 1;

    srv_data_home = "D:\\MyWork\\cos\\data";

    srv_system_file_size = 1024 * 1024;
    srv_system_file_max_size = 100 * 1024 * 1024;
    srv_system_file_auto_extend_size = 1024 * 1024;

    srv_redo_log_buffer_size = 8 * 1024 * 1024; // 8MB
    srv_redo_log_file_size = 8 * 1024 * 1024;
    srv_redo_log_file_count = 3;

    srv_undo_buffer_size = 8 * 1024 * 1024;
    srv_undo_file_max_size = 8 * 1024 * 1024;
    srv_undo_file_auto_extend_size = 3;

    srv_max_n_open = 256;
    srv_space_max_count = 10;
    srv_fil_node_max_count = 10;


    //
    uint64 total_memory_size = 64 * 1024 * 1024;  // 64MB
    memory_area_t* marea = marea_create(total_memory_size, FALSE);

    //
    db_ctrl_createdatabase("cosdb", "utf8mb4_bin");

    db_ctrl_add_system("D:\\MyWork\\cos\\data\\system.dbf",
        srv_system_file_size, srv_system_file_max_size, TRUE);

    db_ctrl_add_redo("D:\\MyWork\\cos\\data\\redo01.log",
        srv_redo_log_file_size, srv_redo_log_file_size, TRUE);
    db_ctrl_add_redo("D:\\MyWork\\cos\\data\\redo02.log",
        srv_redo_log_file_size, srv_redo_log_file_size, TRUE);
    db_ctrl_add_redo("D:\\MyWork\\cos\\data\\redo03.log",
        srv_redo_log_file_size, srv_redo_log_file_size, TRUE);

    knl_server_init_db(marea);

#if 0
    bool32 create_new_db = TRUE;

    uint32 dirname_len = (uint32)strlen(data_home);

    char *filename_prefix = "redo", filename[1024];
    uint32 filename_size = (uint32)strlen(data_home) + 1 /*PATH_SEPARATOR*/
        + (uint32)strlen(filename_prefix) + 2 /*index*/ + 1 /*'\0'*/;
    ut_a(filename_size < 1024);

    if (data_home[dirname_len - 1] != SRV_PATH_SEPARATOR) {
        sprintf_s(filename, filename_size, "%s%c%s%d", data_home, SRV_PATH_SEPARATOR, filename_prefix, i);
    } else {
        sprintf_s(filename, filename_size, "%s%s%d", data_home, filename_prefix, i);
    }
#endif

    while (TRUE) {
        os_thread_sleep(100000);
    }
    return 0;
}

