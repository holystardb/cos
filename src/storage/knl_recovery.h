#ifndef _KNL_RECOVERY_H
#define _KNL_RECOVERY_H

#include "cm_type.h"

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
    uint32      checkpoint_group_offset;
    lsn_t       archived_lsn;

    //
    byte*       log_block;
    uint32      log_block_read_offset;
    uint32      log_block_first_rec_offset;
    lsn_t       log_block_lsn;

    byte*       last_log_block;
    uint32      last_log_block_writed_offset;

    byte        log_rec_buf[UNIV_PAGE_SIZE];
    byte*       log_rec;
    lsn_t       log_rec_lsn;
    uint32      log_rec_len;    // rec length
    uint32      log_rec_offset; // amount of data in log_rec_buf
    // 
    lsn_t       base_lsn;  //
    bool32      is_first_rec;
    bool32      is_read_log_done;
    lsn_t		recovered_lsn;   // the log records have been parsed up to this lsn
    lsn_t		limit_lsn;       // recovery should be made at most up to this lsn
    uint32      cur_group_id;
    uint32      cur_group_offset;
    uint64      last_hdr_no;
    uint64      last_checkpoint_no;
    memory_pool_t* mem_pool;

};


// Values used as flags
#define LOG_FLUSH       7652559
#define LOG_CHECKPOINT  78656949
#define LOG_ARCHIVE     11122331
#define LOG_RECOVER     98887331


#define MLOG_MAX_ID         256

typedef struct mlog_record {
    uint32      len; /* total len of entire record */
    mlog_id_t   mlog_id;

    // data follow, no padding
} mlog_record_t;

typedef struct st_mlog_reader {
    char*          read_buf;
    uint32         read_len;

    mlog_record_t* decoded_record; // currently decoded record


    lsn_t read_rec_ptr; // start of last record read
    lsn_t end_rec_ptr;  // end+1 of last record read

} mlog_reader_t;

typedef struct st_mlog_dispatch {
    bool32 (*mlog_dispatch)(mlog_reader_t* record);
    bool32 (*mlog_check)(mlog_reader_t* record, uint8 min_info, uint8 max_info);
    mlog_id_t   mlog_id;
    uint8       min_info;
    uint8       max_info;
} mlog_dispatch_t;

extern inline bool32 mlog_replay_check(mlog_reader_t* record, uint8 min_info, uint8 max_info);
extern inline bool32 mlog_replay_nbytes(mlog_reader_t* record);



// type: LOG_CHECKPOINT or LOG_ARCHIVE
extern status_t recovery_from_checkpoint_start(uint32 type);
extern status_t recovery_from_checkpoint_finish();

#endif  /* _KNL_RECOVERY_H */
