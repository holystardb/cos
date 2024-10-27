#include "knl_recovery.h"

#include "cm_log.h"

#include "knl_file_system.h"

recovery_sys_t       g_recovery_sys;


static const mlog_dispatch_t g_mlog_dispatch_table[MLOG_MAX_ID] = {
    { mlog_replay_nbytes, mlog_replay_check, MLOG_1BYTE, 0, 0 },
    { mlog_replay_nbytes, mlog_replay_check, MLOG_2BYTES, 0, 0 },
    { mlog_replay_nbytes, mlog_replay_check, MLOG_4BYTES, 0, 0 },
    { mlog_replay_nbytes, mlog_replay_check, MLOG_8BYTES, 0, 0 },
    { NULL, NULL, MLOG_BIGGEST_TYPE, 0, 0 },
};


static recovery_sys_t* recovery_init()
{
    recovery_sys_t* recv_sys = &g_recovery_sys;

    recv_sys->recovered_buf = log_sys->buf;
    recv_sys->recovered_buf_size = log_sys->buf_size;
    recv_sys->recovered_buf_data_len = 0;
    recv_sys->recovered_buf_offset = 0;
    recv_sys->last_hdr_no = 0;
    recv_sys->last_checkpoint_no = 0;
    recv_sys->is_first_rec = TRUE;
    recv_sys->log_rec_len = 0;
    recv_sys->log_rec_offset = 0;

    recv_sys->is_read_log_done = FALSE;

    srv_recovery_on = TRUE;

    return recv_sys;
}

static void recovery_destroy()
{
    srv_recovery_on = FALSE;
}


inline bool32 mlog_replay_check(mlog_reader_t* record, uint8 min_info, uint8 max_info)
{
    return TRUE;
}


inline bool32 mlog_replay_nbytes(mlog_reader_t* record)
{
    return TRUE;
}


bool32 log_recovery()
{
    //log_sys->current_group = 0;
    return TRUE;
}


#define LOG_BLOCK_REMAIN_DATA_LEN(recv_sys)  \
    (OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE - recv_sys->log_block_read_offset)

#define LOG_BLOCK_GET_DATA(recv_sys)         \
    (recv_sys->log_block + recv_sys->log_block_read_offset)


// Looks for the maximum consistent checkpoint from the log groups.
static status_t recv_find_max_checkpoint(uint32* max_field) // out: LOG_CHECKPOINT_1 or LOG_CHECKPOINT_2
{
    uint64          checkpoint_no, max_checkpoint_no = 0, lsn;
    uint32          group_id, group_write_offset;
    uint32          field;
    uint32          fold;
    byte*           buf = log_sys->checkpoint_buf;

    for (field = LOG_CHECKPOINT_1; field <= LOG_CHECKPOINT_2; field += LOG_CHECKPOINT_2 - LOG_CHECKPOINT_1) {

        log_checkpoint_read(field);

        // Check the consistency of the checkpoint info
        fold = ut_fold_binary(buf, LOG_CHECKPOINT_CHECKSUM_1);
        if ((fold & 0xFFFFFFFF) != mach_read_from_4(buf + LOG_CHECKPOINT_CHECKSUM_1)) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_RECOVERY,
                "Checkpoint is invalid at %lu, calc checksum %lu, checksum1 = %lu\n",
                field, fold & 0xFFFFFFFF, mach_read_from_4(buf + LOG_CHECKPOINT_CHECKSUM_1));
            goto not_consistent;
        }

        fold = ut_fold_binary(buf + LOG_CHECKPOINT_LSN, LOG_CHECKPOINT_CHECKSUM_2 - LOG_CHECKPOINT_LSN);
        if ((fold & 0xFFFFFFFF) != mach_read_from_4(buf + LOG_CHECKPOINT_CHECKSUM_2)) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_RECOVERY,
                "Checkpoint is invalid at %lu, calc checksum %lu, checksum2 %lu\n",
                field, fold & 0xFFFFFFFF, mach_read_from_4(buf + LOG_CHECKPOINT_CHECKSUM_2));
            goto not_consistent;
        }

        lsn = mach_read_from_8(buf + LOG_CHECKPOINT_LSN);
        group_id = mach_read_from_4(buf + LOG_CHECKPOINT_OFFSET_LOW32);
        group_write_offset = mach_read_from_4(buf + LOG_CHECKPOINT_OFFSET_HIGH32);
        checkpoint_no = mach_read_from_8(buf + LOG_CHECKPOINT_NO);

        LOGGER_INFO(LOGGER, LOG_MODULE_RECOVERY,
            "Checkpoint point (lsn %llu, checkpoint no %llu) found in group (id %lu, offset %lu)",
            lsn, checkpoint_no, group_id, group_write_offset);

        if (checkpoint_no >= max_checkpoint_no) {
            max_checkpoint_no = checkpoint_no;
            *max_field = field;
        }

not_consistent:
        ;
    }

    return CM_SUCCESS;
}

static inline uint32 recovery_get_log_rec_prefix_len(byte* log_rec)
{
    uint32 len = 1;
    uint32 space_id = mach_read_compressed(log_rec + len);
    len += mach_get_compressed_size(space_id);
    uint32 page_no = mach_read_compressed(log_rec + len);
    len += mach_get_compressed_size(page_no);

    return len;
}

static status_t recovery_read_log_blocks(recovery_sys_t* recv_sys)
{
    status_t err;
    uint32 offset, read_len = UNIV_PAGE_SIZE * 128;  // 2MB
    log_group_t* group = &log_sys->groups[recv_sys->cur_group_id];

    // 1 no remaining data in the current group, switch to group file
    if (group->file_size == recv_sys->cur_group_offset) {
        recv_sys->cur_group_id = (recv_sys->cur_group_id + 1) % log_sys->group_count;
        recv_sys->cur_group_offset = LOG_BUF_WRITE_MARGIN;
    }

    // 2 calc read length
    group = &log_sys->groups[recv_sys->cur_group_id];
    offset = recv_sys->cur_group_offset;
    ut_ad(recv_sys->recovered_buf_size > recv_sys->recovered_buf_data_len);
    if (recv_sys->recovered_buf_data_len + read_len > recv_sys->recovered_buf_size) {
        read_len = recv_sys->recovered_buf_size - recv_sys->recovered_buf_data_len;
    }
    ut_ad(group->file_size >= recv_sys->cur_group_offset);
    if (group->file_size < recv_sys->cur_group_offset + read_len) {
        // no remaining data in the current group, switch to next group file
        read_len = group->file_size - recv_sys->cur_group_offset;
        recv_sys->cur_group_id = (recv_sys->cur_group_id + 1) % log_sys->group_count;
        recv_sys->cur_group_offset = LOG_BUF_WRITE_MARGIN;
    }

    // 3 read file
    err = log_group_file_read(group, recv_sys->recovered_buf + recv_sys->recovered_buf_data_len, read_len, offset);
    if (err != CM_SUCCESS) {
        return CM_ERROR;
    }


    // 4 check for valid log block
    for (uint32 i = 0; i < recv_sys->recovered_buf_data_len + read_len; i += OS_FILE_LOG_BLOCK_SIZE) {
        recv_sys->last_log_block = recv_sys->recovered_buf + i;
        uint64 hdr_no = log_block_get_hdr_no(recv_sys->last_log_block);
        uint64 checkpoint_no = log_block_get_checkpoint_no(recv_sys->last_log_block);
        if (recv_sys->last_hdr_no > hdr_no) {
            recv_sys->last_log_block_writed_offset =
                recv_sys->cur_group_offset +  //
                (i - recv_sys->recovered_buf_data_len) +  //
                log_block_get_data_len(recv_sys->last_log_block);
            recv_sys->recovered_buf_data_len = i + log_block_get_data_len(recv_sys->last_log_block);
            recv_sys->limit_lsn = recv_sys->recovered_buf_lsn + recv_sys->recovered_buf_data_len;
            recv_sys->is_read_log_done = TRUE;
            return CM_SUCCESS;
        }

        recv_sys->last_hdr_no = hdr_no;
        recv_sys->last_checkpoint_no = checkpoint_no;
    }

    recv_sys->recovered_buf_data_len += read_len;
    recv_sys->cur_group_offset += read_len;

    return CM_SUCCESS;
}

static inline uint32 recovery_parse_next_log_rec_size(recovery_sys_t* recv_sys)
{
    byte* log_rec;
    uint32 remain_len;

    ut_ad(recv_sys->log_block);

    remain_len = LOG_BLOCK_REMAIN_DATA_LEN(recv_sys);
    if (remain_len < MTR_LOG_LEN_SIZE) {
        //
        ut_ad(recv_sys->recovered_buf_data_len >=
            recv_sys->log_block - recv_sys->recovered_buf + OS_FILE_LOG_BLOCK_SIZE + OS_FILE_LOG_BLOCK_SIZE);

        byte buf[OS_FILE_LOG_BLOCK_SIZE];
        memcpy(buf, LOG_BLOCK_GET_DATA(recv_sys), remain_len);
        memcpy(buf + remain_len,
            recv_sys->log_block + OS_FILE_LOG_BLOCK_SIZE + LOG_BLOCK_HDR_SIZE,
            MTR_LOG_LEN_SIZE - remain_len);
        log_rec = buf;
    } else {
        log_rec = LOG_BLOCK_GET_DATA(recv_sys);
    }

    return mach_read_from_2(log_rec);
}


static status_t recovery_read_next_log_rec(recovery_sys_t* recv_sys)
{
    status_t err;
    uint32 log_rec_len;

    // init log_rec
    recv_sys->log_rec_len = 0;
    recv_sys->log_rec_offset = 0;
    recv_sys->log_rec = NULL;

retry_copy_remain:

    // check
    if (!recv_sys->is_read_log_done &&
        recv_sys->recovered_buf_data_len <= recv_sys->recovered_buf_offset + OS_FILE_LOG_BLOCK_SIZE) {

        // move data
        if (recv_sys->recovered_buf_data_len > 0) {
            memmove(recv_sys->recovered_buf,
                recv_sys->recovered_buf + recv_sys->recovered_buf_offset,
                recv_sys->recovered_buf_data_len - recv_sys->recovered_buf_offset);
            recv_sys->recovered_buf_data_len -= recv_sys->recovered_buf_offset;
            recv_sys->recovered_buf_offset = 0;
            recv_sys->recovered_buf_lsn += recv_sys->recovered_buf_offset;
        }

        //
        err = recovery_read_log_blocks(recv_sys);
        if (err != CM_SUCCESS) {
            return CM_ERROR;
        }
    }


    // finish
    if (recv_sys->is_read_log_done && recv_sys->recovered_buf_offset == recv_sys->recovered_buf_data_len) {
        return CM_SUCCESS;
    }

    //
    if (recv_sys->is_first_rec) {
        recv_sys->is_first_rec = FALSE;

        recv_sys->log_block = recv_sys->recovered_buf;
        recv_sys->recovered_buf_offset += OS_FILE_LOG_BLOCK_SIZE;

        recv_sys->log_block_first_rec_offset = log_block_get_first_rec_group(recv_sys->log_block);
        recv_sys->log_block_read_offset = recv_sys->log_block_first_rec_offset;
        recv_sys->log_block_lsn =
            recv_sys->checkpoint_lsn -
            (recv_sys->checkpoint_group_offset - recv_sys->cur_group_offset);
        recv_sys->recovered_buf_lsn = recv_sys->log_block_lsn;
    }

    //
    if (recv_sys->log_block == NULL) {
        ut_ad(recv_sys->log_block_read_offset == 0);

        recv_sys->log_block = recv_sys->recovered_buf + recv_sys->recovered_buf_offset;
        recv_sys->recovered_buf_offset += OS_FILE_LOG_BLOCK_SIZE;

        recv_sys->log_block_lsn += OS_FILE_LOG_BLOCK_SIZE;
        recv_sys->log_block_first_rec_offset = log_block_get_first_rec_group(recv_sys->log_block);
        recv_sys->log_block_read_offset = LOG_BLOCK_HDR_SIZE;
    }

    // a new log_rec
    if (recv_sys->log_rec_len == 0) {
        recv_sys->log_rec_len = recovery_parse_next_log_rec_size(recv_sys) + MTR_LOG_LEN_SIZE;
        ut_ad(recv_sys->log_rec_len > 0);
        recv_sys->log_rec_offset = 0;
        recv_sys->log_rec_lsn = recv_sys->log_block_lsn + recv_sys->log_block_read_offset;
    }

    // copy data
    uint32 copy_data_len;
    if (recv_sys->log_rec_len < recv_sys->log_rec_offset + LOG_BLOCK_REMAIN_DATA_LEN(recv_sys)) {
        copy_data_len = recv_sys->log_rec_len - recv_sys->log_rec_offset;

        memcpy(recv_sys->log_rec_buf + recv_sys->log_rec_offset,
            LOG_BLOCK_GET_DATA(recv_sys), copy_data_len);
        recv_sys->log_rec_offset += copy_data_len;

        recv_sys->log_block_read_offset += copy_data_len;
    } else {
        copy_data_len = LOG_BLOCK_REMAIN_DATA_LEN(recv_sys);

        memcpy(recv_sys->log_rec_buf + recv_sys->log_rec_offset,
            LOG_BLOCK_GET_DATA(recv_sys), copy_data_len);
        recv_sys->log_rec_offset += copy_data_len;

        recv_sys->log_block = NULL;
        recv_sys->log_block_read_offset  = 0;
    }

    // If it is not a complete record
    if (recv_sys->log_rec_len > recv_sys->log_rec_offset) {
        goto retry_copy_remain;
    }

    //
    recv_sys->log_rec = recv_sys->log_rec_buf;

    return CM_SUCCESS;
}

static status_t recovery_replay_log_rec(recovery_sys_t* recv_sys)
{
    return CM_SUCCESS;
}

static status_t recovery_get_last_checkpoint_info(recovery_sys_t* recv_sys)
{
    status_t err;
    uint32 max_ckpt_field = LOG_GROUP_MAX_COUNT;

    // 1 Look for the latest checkpoint from any of the log groups
    err = recv_find_max_checkpoint(&max_ckpt_field);
    if (err != CM_SUCCESS) {
        return CM_ERROR;
    }

    // 2 read and set checkpoint info
    log_checkpoint_read(max_ckpt_field);

    recv_sys->checkpoint_lsn = mach_read_from_8(log_sys->checkpoint_buf + LOG_CHECKPOINT_LSN);
    recv_sys->checkpoint_no = mach_read_from_8(log_sys->checkpoint_buf + LOG_CHECKPOINT_NO);
    recv_sys->archived_lsn = mach_read_from_8(log_sys->checkpoint_buf + LOG_CHECKPOINT_ARCHIVED_LSN);
    recv_sys->checkpoint_group_id = mach_read_from_4(log_sys->checkpoint_buf + LOG_CHECKPOINT_OFFSET_LOW32);
    recv_sys->checkpoint_group_offset = mach_read_from_4(log_sys->checkpoint_buf + LOG_CHECKPOINT_OFFSET_HIGH32);

    ut_a(recv_sys->checkpoint_group_id < log_sys->group_count);
    ut_a(recv_sys->checkpoint_group_offset >= LOG_BUF_WRITE_MARGIN);

    recv_sys->cur_group_id = recv_sys->checkpoint_group_id;

    return CM_SUCCESS;
}

static void recovery_reset_log_sys(recovery_sys_t* recv_sys)
{
    uint32 file_size = 0;
    log_group_t* checkpoint_group = &log_sys->groups[recv_sys->checkpoint_group_id];
    checkpoint_group->base_lsn = recv_sys->base_lsn;
    checkpoint_group->status = LogGroupStatus::ACTIVE;
    file_size = log_group_get_capacity(checkpoint_group);

    for (uint32 i = 1; i < log_sys->group_count; i++) {
        log_group_t* group = &log_sys->groups[(recv_sys->checkpoint_group_id + i) % log_sys->group_count];

        group->base_lsn = checkpoint_group->base_lsn + file_size;

        //
        if (recv_sys->checkpoint_group_id > recv_sys->cur_group_id) {
            if (group->id < recv_sys->checkpoint_group_id &&
                group->id > recv_sys->cur_group_id) {
                checkpoint_group->status = LogGroupStatus::INACTIVE;
            } else {
                checkpoint_group->status = LogGroupStatus::ACTIVE;
            }
        } else {
            if (group->id > recv_sys->checkpoint_group_id &&
                group->id <= recv_sys->cur_group_id) {
                checkpoint_group->status = LogGroupStatus::ACTIVE;
            } else {
                checkpoint_group->status = LogGroupStatus::INACTIVE;
            }
        }

        //
        file_size += (group->file_size - LOG_BUF_WRITE_MARGIN);
    }

    // checkpoint
    log_sys->last_checkpoint_lsn = recv_sys->checkpoint_lsn;
    log_sys->next_checkpoint_no = recv_sys->checkpoint_no + 1;

    // base lsn for log_buffer
    log_sys->buf_base_lsn = checkpoint_group->base_lsn;
    // Move the data of last BLOCK to the forefront of log_buffer
    if (recv_sys->recovered_buf_offset > 0) {
        memmove(recv_sys->recovered_buf,
            recv_sys->recovered_buf + recv_sys->recovered_buf_offset,
            recv_sys->recovered_buf_data_len - recv_sys->recovered_buf_offset);
    }
    // buf_lsn
    log_sys->buf_lsn.val.lsn = recv_sys->limit_lsn;
    log_sys->buf_lsn.val.slot_index = 0;
    log_sys->slot_write_pos = 0;
    // position for writer thread and flusher thread
    log_sys->writer_writed_lsn = recv_sys->limit_lsn;
    log_sys->current_write_group = recv_sys->cur_group_id;
    log_sys->flusher_flushed_lsn = recv_sys->limit_lsn;
    log_sys->current_flush_group = recv_sys->cur_group_id;
}


// Recovers from a checkpoint.
// When this function returns, the database is able to start processing of new user transactions,
// but the function recv_recovery_from_checkpoint_finish should be called later to complete
// the recovery and free the resources used in it.
status_t recovery_from_checkpoint_start(uint32 type) // in: LOG_CHECKPOINT or LOG_ARCHIVE
{
    status_t        err;
    recovery_sys_t* recv_sys = recovery_init();

    // 1
    err = recovery_get_last_checkpoint_info(recv_sys);
    if (err != CM_SUCCESS) {
        return CM_ERROR;
    }

    // 2 read log and replay

    // init position for read
    recv_sys->cur_group_id = recv_sys->checkpoint_group_id;
    recv_sys->cur_group_offset = ut_uint32_align_down(recv_sys->checkpoint_group_offset, OS_FILE_LOG_BLOCK_SIZE);
    recv_sys->base_lsn = recv_sys->checkpoint_lsn -
        (recv_sys->checkpoint_group_offset - LOG_BUF_WRITE_MARGIN); // lsn for block0

    // read and replay log_rec
    byte* log_rec = NULL;
    err = recovery_read_next_log_rec(recv_sys);
    if (err != CM_SUCCESS) {
        return CM_ERROR;
    }
    while (recv_sys->log_rec) {
        // replay
        err = recovery_replay_log_rec(recv_sys);
        if (err != CM_SUCCESS) {
            return CM_ERROR;
        }

        // get next log_rec
        err = recovery_read_next_log_rec(recv_sys);
        if (err != CM_SUCCESS) {
            return CM_ERROR;
        }
    }

    // 3 
    recovery_reset_log_sys(recv_sys);

    return CM_SUCCESS;
}

// Completes recovery from a checkpoint.
status_t recovery_from_checkpoint_finish()
{
    // Apply the hashed log records to the respective file pages

    //if (srv_force_recovery < SRV_FORCE_NO_LOG_REDO) {
    //    recv_apply_hashed_log_recs(TRUE);
    //}

    /* By acquring the mutex we ensure that the recv_writer thread
       won't trigger any more LRU batchtes. Now wait for currently
       in progress batches to finish. */
    //buf_flush_wait_LRU_batch_end();

    //trx_rollback_or_clean_recovered(FALSE);

    recovery_destroy();

    return CM_SUCCESS;
}

