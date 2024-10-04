#include "cm_type.h"
#include "cm_memory.h"
#include "cm_getopt.h"
#include "cm_log.h"
#include "cm_dbug.h"
#include "cm_config.h"
#include "cm_file.h"
#include "cm_thread.h"

#include "knl_handler.h"
#include "knl_server.h"
#include "guc.h"

static int get_options(int argc, char **argv)
{
    struct option long_options[] = {
        { "config",   required_argument,   NULL,    'c' },
        { "help",     no_argument,         NULL,    'h' },
        { 0,     0,                   0,       0 },
    };

    int ch;
    while ((ch = getopt_long(argc, argv, "c:h", long_options, NULL)) != -1)
    {
        switch (ch)
        {
        case 'c':
            //strcpy_s(g_config_file, 255, optarg);
            break;
        case 'h':
            printf("socks -c socks.ini\n");
            break;
        default:
            printf("unknow option:%c\n", ch);
        }
    }
    return 0;
}

bool32 knl_server_init(void)
{
    //srv_buf_pool_size = (g_buffer_pool_size * 1024 * 1024) / g_buffer_pool_instances;
    //srv_buf_pool_size = srv_buf_pool_size * g_buffer_pool_instances;
    //srv_buf_pool_instances = g_buffer_pool_instances;

    //srv_data_home = g_base_directory;
    //srv_system_file_size = 1024 * 1024;
    //srv_system_file_max_size = 10 * 1024 * 1024;
    //srv_system_file_auto_extend_size = 1024 * 1024;

    //srv_redo_log_buffer_size = g_redo_log_buffer_size * 1024 * 1024;
    //srv_redo_log_file_size = g_redo_log_file_size * 1024 * 1024;
    //srv_redo_log_file_count = g_redo_log_files;

    //srv_undo_buffer_size = g_undo_buffer_size * 1024 * 1024;

    //srv_max_n_open = g_open_files_limit;
    //srv_space_max_count = 10;
    //srv_fil_node_max_count = 10;


    return CM_SUCCESS;
}

int main1(int argc, const char *argv[])
{
    char *config_file = "D:\\MyWork\\cos\\etc\\server.ini";

    //initialize_guc_options(config_file);

    os_file_init();


    memory_area_t* marea;
    uint64 total_memory_size = 1024 * 1024;
    bool32 is_extend = FALSE;
    marea = marea_create(total_memory_size, is_extend);


    //knl_server_init();

    //knl_server_start(marea);

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



