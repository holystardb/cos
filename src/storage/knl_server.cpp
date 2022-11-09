#include "knl_server.h"
#include "knl_buf.h"



/*------------------------- DATA ------------------------ */


char *srv_data_home = NULL;

/*------------------------- UNDO ------------------------ */


char *srv_undo_dir = NULL;

/*------------------------- REDO ------------------------ */

#define SRV_N_LOG_FILES_MAX 100

char *srv_log_group_home_dir = NULL;
uint32 srv_n_log_files = SRV_N_LOG_FILES_MAX;

/*------------------------- BUF POOL -------------------- */

uint64 srv_buf_pool_size;
uint64 srv_buf_pool_min_size = 5 * 1024 * 1024;
uint32 srv_buf_pool_instances;
uint32 srv_buf_pool_chunk_unit;
bool32 buf_pool_should_madvise;


/*------------------------- LOG ------------------------- */

uint64 srv_log_file_size;
uint32 srv_log_buffer_size;

/* The number of purge threads to use.*/
uint32 srv_n_purge_threads = 4;

static void srv_boot(void)
{
    dberr_t err;

    srv_data_home = "D:\\MyWork\\cos\\data";


    srv_buf_pool_size = 5 * 1024 * 1024;
    srv_buf_pool_instances = 1;
    err = buf_pool_init(srv_buf_pool_size, srv_buf_pool_instances);
}


dberr_t srv_start(bool32 create_new_db)
{
    srv_boot();

    return DB_SUCCESS;
}