#ifndef _KNL_RECOVERY_H
#define _KNL_RECOVERY_H

#include "cm_type.h"
#include "cm_rbt.h"
#include "cm_memory.h"

#include "knl_mtr.h"





// Recovery system data structure
typedef struct st_recovery_sys recovery_sys_t;
struct st_recovery_sys{
    mutex_t     mutex;

    // recovery buffer
    byte*       recovered_buf; // buffer for parsing log records
    uint32      recovered_buf_size;
    uint32      recovered_buf_data_len; // amount of data in buf
    uint32      recovered_buf_offset;// start offset of non-parsed log records in buf
    lsn_t       recovered_buf_lsn; // lsn of recovered_buf[0]

    // last checkpoint point
    uint64      checkpoint_no;
    lsn_t       checkpoint_lsn;
    uint32      checkpoint_group_id;
    uint64      checkpoint_group_offset;
    lsn_t       archived_lsn;

    //
    byte*       log_block;
    uint32      log_block_read_offset;
    uint32      log_block_first_rec_offset;
    lsn_t       log_block_lsn;

    byte*       last_log_block;
    uint64      last_log_block_writed_offset;

    byte        log_rec_buf[65536];
    byte*       log_rec;
    lsn_t       log_rec_start_lsn;
    lsn_t       log_rec_end_lsn;
    uint32      log_rec_len;    // rec length
    uint32      log_rec_offset; // amount of data in log_rec_buf
    // 
    lsn_t       base_lsn;  //
    bool32      is_first_rec;
    bool32      is_read_log_done;
    lsn_t		recovered_lsn;   // the log records have been parsed up to this lsn
    lsn_t		limit_lsn;       // recovery should be made at most up to this lsn
    uint32      cur_group_id;
    uint64      cur_group_offset;
    uint64      last_hdr_no;
    uint64      last_checkpoint_no;
    memory_pool_t* mem_pool;
    ib_rbt_t*   block_rbt;
};


// Values used as flags
#define LOG_FLUSH       7652559
#define LOG_CHECKPOINT  78656949
#define LOG_ARCHIVE     11122331
#define LOG_RECOVER     98887331


typedef struct st_mlog_dispatch {
    uint32 type;

    byte* (*log_rec_replay)(uint32 type, byte* log_rec_ptr, byte* log_end_ptr, void* block);
    bool32 (*log_rec_check)(uint32 type, byte* log_rec_ptr, byte* log_end_ptr);
} mlog_dispatch_t;


extern inline bool32 mlog_replay_check(uint32 type, byte* log_rec_ptr, byte* log_end_ptr);

extern recovery_sys_t* recovery_init(memory_pool_t* mem_pool);
extern void recovery_destroy(recovery_sys_t* recv_sys);
extern status_t recovery_from_checkpoint_start(recovery_sys_t* recv_sys);
extern status_t recovery_from_checkpoint_finish(recovery_sys_t* recv_sys);

#endif  /* _KNL_RECOVERY_H */
