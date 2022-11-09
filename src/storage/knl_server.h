#ifndef _KNL_SERVER_H
#define _KNL_SERVER_H

#include "cm_type.h"
#include "cm_error.h"

extern char *srv_data_home;

/*------------------------- UNDO ------------------------ */


extern char *srv_undo_dir;

/*------------------------- REDO ------------------------ */

extern char *srv_log_group_home_dir;
extern uint32 srv_n_log_files;

/*------------------------- BUF POOL -------------------- */

extern uint64 srv_buf_pool_size;
extern uint64 srv_buf_pool_min_size;
extern uint32 srv_buf_pool_instances;
extern uint32 srv_buf_pool_chunk_unit;
extern bool32 buf_pool_should_madvise;


/*------------------------- LOG ------------------------- */

extern uint64 srv_log_file_size;
extern uint32 srv_log_buffer_size;



/* The number of purge threads to use.*/
extern uint32 srv_n_purge_threads;


dberr_t srv_start(bool32 create_new_db);



#endif  /* _KNL_SERVER_H */
