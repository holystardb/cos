#ifndef _KNL_CHECKPOINT_H
#define _KNL_CHECKPOINT_H

#include "cm_mutex.h"
#include "cm_type.h"
#include "knl_mtr.h"
#include "knl_server.h"
#include "knl_fsp.h"
#include "knl_trx_types.h"
#include "knl_trx_undo.h"
#include "knl_session.h"

#define CHECKPOINT_GROUP_MAX_SIZE         1024  // 16MB

typedef struct st_checkpoint_sort_item {
    page_id_t       page_id;
    uint32          buf_id;
    volatile bool32 is_flushed;
} checkpoint_sort_item_t;

typedef struct st_checkpoint_group {
    uint32 item_count;
    uint32 buf_size;
    char*  buf;
    checkpoint_sort_item_t items[CHECKPOINT_GROUP_MAX_SIZE];
} checkpoint_group_t;

typedef struct st_checkpoint_double_write {
    os_file_t       handle;
    uint32          size;
    char*           name;
    os_aio_array_t* aio_array;
} checkpoint_double_write_t;


typedef struct st_checkpoint_stat {
    uint64 double_writes;
    uint64 double_write_time;
    uint64 disk_writes;
    uint64 disk_write_time;
    uint64 ckpt_total_neighbors_times;
    uint64 ckpt_total_neighbors_len;
    uint32 ckpt_last_neighbors_len;
    uint32 ckpt_curr_neighbors_times;
    uint32 ckpt_curr_neighbors_len;
    //uint64 task_count[CKPT_MODE_NUM]; // FOR TEST: viewing
    //uint64 task_us[CKPT_MODE_NUM];
    //uint64 flush_pages[CKPT_MODE_NUM];
    //uint64 clean_edp_count[CKPT_MODE_NUM];
    uint64 proc_wait_cnt;
    //ckpt_part_stat_t part_stat[CT_MAX_DBWR_PROCESS];
    // ckpt_begin_time[CKPT_MODE_NUM];
} checkpoint_stat_t;

typedef struct st_checkpoint {
    thread_t      thread;

    uint64        flush_timeout_us;

    lsn_t         least_recovery_point;

    bool32        enable_double_write;

    checkpoint_group_t   group;

    checkpoint_stat_t    stat;

    mutex_t              mutex;

    checkpoint_double_write_t  double_write;
} checkpoint_t;




//-----------------------------------------------------------------

extern checkpoint_t* checkpoint_init(char* dbwr_file_name, uint32 dbwr_file_size);
extern void* checkpoint_proc_thread(void *arg);
extern status_t ckpt_increment_checkpoint();
extern status_t ckpt_full_checkpoint();
//-----------------------------------------------------------------



#endif  /* _KNL_TRX_H */
