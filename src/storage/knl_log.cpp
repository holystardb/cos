#include "knl_log.h"
#include "cm_dbug.h"
#include "knl_server.h"
#include "cm_file.h"
#include "knl_buf.h"
#include "cm_log.h"


/* Margins for free space in the log buffer after a log entry is catenated */
#define LOG_BUF_FLUSH_RATIO     2
#define LOG_BUF_FLUSH_MARGIN    (LOG_BUF_WRITE_MARGIN + 4 * UNIV_PAGE_SIZE)

constexpr uint32 LOG_BLOCK_DATA_SIZE =
    OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_HDR_SIZE - LOG_BLOCK_TRL_SIZE;

/* A margin for free space in the log buffer before a log entry is catenated */
constexpr uint32 LOG_BUF_WRITE_MARGIN = 4 * OS_FILE_LOG_BLOCK_SIZE;

bool32 log_block_get_flush_bit(const byte* log_block)
{
    if (LOG_BLOCK_FLUSH_BIT_MASK & mach_read_from_4(log_block + LOG_BLOCK_HDR_NO)) {
        return(TRUE);
    }

    return(FALSE);
}

//Sets the log block flush bit. */
void log_block_set_flush_bit(byte* log_block, bool32 val)
{
    uint32 field;

    field = mach_read_from_4(log_block + LOG_BLOCK_HDR_NO);

    if (val) {
        field = field | LOG_BLOCK_FLUSH_BIT_MASK;
    } else {
        field = field & ~LOG_BLOCK_FLUSH_BIT_MASK;
    }

    mach_write_to_4(log_block + LOG_BLOCK_HDR_NO, field);
}

uint64 log_block_get_hdr_no(const byte* log_block)
{
    return mach_read_from_8(log_block + LOG_BLOCK_HDR_NO);
}

void log_block_set_hdr_no(byte* log_block, uint64 n)
{
    mach_write_to_8(log_block + LOG_BLOCK_HDR_NO, n);
}

uint32 log_block_get_data_len(const byte* log_block)
{
    return mach_read_from_2(log_block + LOG_BLOCK_HDR_DATA_LEN);
}

void log_block_set_data_len(byte* log_block, uint32 len)
{
    mach_write_to_2(log_block + LOG_BLOCK_HDR_DATA_LEN, len);
}

//Gets a log block first mtr log record group offset.
//first mtr log record group byte offset from the block start, 0 if none.
uint32 log_block_get_first_rec_group(const byte* log_block)
{
    return mach_read_from_2(log_block + LOG_BLOCK_FIRST_REC_GROUP);
}

void log_block_set_first_rec_group(byte* log_block, uint32 offset)
{
    mach_write_to_2(log_block + LOG_BLOCK_FIRST_REC_GROUP, offset);
}

uint64 log_block_convert_lsn_to_no(lsn_t lsn)
{
    //return lsn / OS_FILE_LOG_BLOCK_SIZE;
    return lsn;
}

//Calculates the checksum for a log block.
uint32 log_block_calc_checksum(const byte* block)
{
    uint32 sum;
    uint32 sh;
    uint32 i;

    sum = 1;
    sh = 0;

    for (i = 0; i < OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE; i++) {
        uint32 b = (uint32) block[i];
        sum &= 0x7FFFFFFFUL;
        sum += b;
        sum += b << sh;
        sh++;
        if (sh > 24) {
            sh = 0;
        }
    }

    return sum;
}

uint32 log_block_get_checksum(const byte* log_block)
{
    return(mach_read_from_4(log_block + OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_CHECKSUM));
}

void log_block_set_checksum(byte* log_block, uint32 checksum)
{
    mach_write_to_4(log_block + OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_CHECKSUM, checksum);
}

//Stores a 4-byte checksum to the trailer checksum field of a log block
//before writing it to a log file. This checksum is used in recovery to
//check the consistency of a log block.
static void log_block_store_checksum(byte* block) //in/out: pointer to a log block
{
    log_block_set_checksum(block, log_block_calc_checksum(block));
}


//Initializes a log block in the log buffer
static void log_block_init(byte* log_block, lsn_t lsn)
{
    uint64 no;
    no = log_block_convert_lsn_to_no(lsn);
    log_block_set_hdr_no(log_block, no);

    log_block_set_data_len(log_block, LOG_BLOCK_HDR_SIZE);
    log_block_set_first_rec_group(log_block, 0);
}

// This function is called, e.g., when a transaction wants to commit.
inline void log_write_up_to(lsn_t lsn)
{
    ut_ad(!srv_read_only_mode);

    if (log_sys->flusher_flushed_lsn >= lsn) {
        return;
    }

    // notify flusher thread
    os_event_set(log_sys->flusher_event);

    //
    uint64 trx_sync_log_waits = 0, signal_count = 0;
    uint32 timeout_microseconds = 1000;
    uint32 idx = (lsn / OS_FILE_LOG_BLOCK_SIZE) & (LOG_SESSION_WAIT_EVENT_COUNT - 1);
    while (log_sys->flusher_flushed_lsn < lsn) {
        trx_sync_log_waits++;
        os_event_wait_time(log_sys->session_wait_event[idx], timeout_microseconds, signal_count);
        signal_count = os_event_reset(log_sys->session_wait_event[idx]);
    }

    if (trx_sync_log_waits > 0) {
        srv_stats.trx_sync_log_waits.add(trx_sync_log_waits);
    }
}

// Waits for an aio operation to complete.
inline void log_writer_write_wait_io_complete(os_aio_context_t* context, uint32 timeout_us)
{
    int32          ret;
    os_aio_slot_t* slot = NULL;

    ret = os_file_aio_context_wait(context, &slot, timeout_us);

    if (srv_shutdown_state == SHUTDOWN_EXIT_THREADS) {
        return;
    }

    //if (ret != OS_FILE_IO_COMPLETION && ret != OS_FILE_IO_TIMEOUT) {
    //    LOGGER_FATAL(LOGGER, "A fatal error occurred in reader thread or writes thread, service exited.");
    //    ut_error;
    //}

    if (slot) {
        if (slot->callback_func) {
            slot->callback_func(ret, slot);
        }

        os_aio_context_free_slot(slot);
    }
}


static bool32 log_writer_write_to_file(log_group_t *group, uint64 start_pos, uint64 data_len)
{
    uint64 offset = group->write_offset;
    uint32 slot_count;

    if (start_pos + data_len <= log_sys->buf_size) {
        if (os_file_aio_submit(log_sys->aio_ctx_log_write, OS_FILE_WRITE, group->name, group->handle,
                               (void *)(log_sys->buf + start_pos), (uint32)data_len, offset) == NULL) {
            char err_info[CM_ERR_MSG_MAX_LEN];
            os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
            LOGGER_ERROR(LOGGER, "log_writer: fail to write redo file, name %s error %s", group->name, err_info);
            goto err_exit;
        }
        slot_count = 1;
    } else {
        if (os_file_aio_submit(log_sys->aio_ctx_log_write, OS_FILE_WRITE, group->name, group->handle,
                               (void *)(log_sys->buf + start_pos),
                               log_sys->buf_size - (uint32)start_pos, offset) == NULL) {
            char err_info[CM_ERR_MSG_MAX_LEN];
            os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
            LOGGER_ERROR(LOGGER, "log_writer: fail to write redo file, name %s error %s", group->name, err_info);
            goto err_exit;
        }
        offset += log_sys->buf_size - start_pos;
        if (os_file_aio_submit(log_sys->aio_ctx_log_write, OS_FILE_WRITE, group->name, group->handle,
                                (void *)log_sys->buf, (uint32)(data_len - (log_sys->buf_size - (uint32)start_pos)),
                                offset) == NULL) {
            char err_info[CM_ERR_MSG_MAX_LEN];
            os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
            LOGGER_ERROR(LOGGER, "log_writer: fail to write redo file, name %s error %s", group->name, err_info);
            goto err_exit;
        }
        slot_count = 2;
    }

    // wait for io-complete
    bool32 is_disk_full = FALSE;
    uint32 timeout_seconds = 300; // 300 seconds
    os_aio_slot_t* aio_slot = NULL;
    while (slot_count > 0) {
        int32 err = os_file_aio_context_wait(log_sys->aio_ctx_log_write, &aio_slot, timeout_seconds * 1000000);
        switch (err) {
        case OS_FILE_IO_COMPLETION:
            break;
        case OS_FILE_DISK_FULL:
            LOGGER_DEBUG(LOGGER, "log_writer: disk is full, name %s", group->name);
            is_disk_full = TRUE;
            break;
        case OS_FILE_IO_TIMEOUT:
            LOGGER_ERROR(LOGGER,
                "log_writer: IO timeout for writing redo file, name %s timeout %u seconds",
                group->name, timeout_seconds);
            goto err_exit;
        default:
            char err_info[CM_ERR_MSG_MAX_LEN];
            os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
            LOGGER_ERROR(LOGGER,
                "log_writer: failed to write redo file, name %s error %s",
                group->name, err_info);
            goto err_exit;
        }

        //
        ut_a(aio_slot);
        os_aio_context_free_slot(aio_slot);

        slot_count--;
    }

    if (!is_disk_full) {
        LOGGER_DEBUG(LOGGER, "log_writer: write redo to group file, group (%u, %s) position (%lu - %lu)",
            group->id, group->name, group->write_offset, group->write_offset + data_len);
        group->write_offset += data_len;
    }

    return is_disk_full ? FALSE : TRUE;

err_exit:

    LOGGER_FATAL(LOGGER, "log_writer: A fatal error occurred, service exited");
    ut_error;

    return FALSE;
}

static void log_writer_wait_for_checkpoint(uint64 write_lsn)
{
    uint64 signal_count = 0;
    uint64 log_checkpoint_waits = 0;
    uint32 timeout_microseconds = 1000;
    uint64 checkpoint_lsn, write_up_lsn;

    checkpoint_lsn = ut_uint64_align_down(log_sys->last_checkpoint_lsn, OS_FILE_LOG_BLOCK_SIZE);
    write_up_lsn = ut_uint64_align_up(write_lsn, OS_FILE_LOG_BLOCK_SIZE);
    ut_ad(write_up_lsn >= checkpoint_lsn);

    while (write_up_lsn - checkpoint_lsn >= log_sys->file_size) {
        log_checkpoint_waits++;

        //os_event_set(log_sys->checkpoint_event);

        // wake up by checkpoint thread
        os_event_wait_time(log_sys->writer_event, timeout_microseconds, signal_count);
        signal_count = os_event_reset(log_sys->writer_event);
    }

    //if (group->status == LogGroupStatus::INACTIVE) {
        // modify log file status
    //} else if (group->status == LogGroupStatus::ACTIVE) {
        //os_thread_sleep(1000); // 0.1 second
    //}


    if (log_checkpoint_waits > 0) {
        srv_stats.log_checkpoint_waits.add(log_checkpoint_waits);
    }
}

static void log_block_set_data_len_and_first_rec_group(log_slot_t *slot)
{
    uint64  slot_start_len = slot->lsn;
    uint64  slot_end_lsn = slot->lsn + slot->data_len;
    uint64  slot_down_start_lsn = ut_uint64_align_down(slot_start_len, OS_FILE_LOG_BLOCK_SIZE);
    uint64  slot_down_end_lsn = ut_uint64_align_down(slot_end_lsn, OS_FILE_LOG_BLOCK_SIZE);
    uint32  buf_free;
    byte   *log_block;

    if (slot_down_start_lsn == slot_down_end_lsn) {
        /* The string fits within the current log block */
        buf_free = (slot_start_len - log_sys->buf_base_lsn) % log_sys->buf_size;
        log_block = (byte*)ut_align_down(log_sys->buf + buf_free, OS_FILE_LOG_BLOCK_SIZE);
        if (log_block_get_data_len(log_block) == OS_FILE_LOG_BLOCK_SIZE) {
            /* This block is full, data_len of block is setted in log_write_low */
            // nothing to do
        } else {
            log_block_set_data_len(log_block, log_block_get_data_len(log_block) + slot->data_len);
        }
    } else {
        // get last block, and set data_len and first_rec_group in last block,
        // middle block is full, data_len of block is setted in log_write_low
        buf_free = (slot_end_lsn - log_sys->buf_base_lsn) % log_sys->buf_size;
        log_block = (byte*)ut_align_down(log_sys->buf + buf_free, OS_FILE_LOG_BLOCK_SIZE);
        log_block_set_first_rec_group(log_block, (uint32)(slot_end_lsn - slot_down_end_lsn + 1));
        log_block_set_data_len(log_block, log_block_get_data_len(log_block) + (uint32)(slot_end_lsn - slot_down_end_lsn));
    }
}

static uint64 log_compute_how_much_to_write(log_group_t *group, uint64 start_lsn, uint64 end_lsn)
{
    uint64 data_len = end_lsn - start_lsn;

    if (data_len <= group->file_size - group->write_offset) {
        return data_len;
    }

    return group->file_size - group->write_offset;
}

static void log_start_next_file(uint64 base_lsn)
{
    log_group_t *cur_group, *next_group;

    cur_group = &log_sys->groups[log_sys->current_write_group];

    log_sys->current_write_group = (log_sys->current_write_group + 1) % log_sys->group_count;
    next_group = &log_sys->groups[log_sys->current_write_group];
    next_group->write_offset = LOG_BUF_WRITE_MARGIN;
    next_group->base_lsn = base_lsn;
    next_group->status = LogGroupStatus::CURRENT;

    cur_group->status = LogGroupStatus::ACTIVE;

    LOGGER_DEBUG(LOGGER,
        "log_start_next_file: switch group (%u, %s) to group (%u, %s)",
        cur_group->id, cur_group->name, next_group->id, next_group->name);
}

static void* log_writer_thread_entry(void *arg)
{
    uint64 signal_count = 0;
    uint64 slot_write_pos;
    uint32 slot_sleep_microseconds = 1000;

    while (TRUE) {

        slot_write_pos = log_sys->slot_write_pos;
        while (log_sys->slots[log_sys->slot_write_pos].status == LogSlotStatus::COPIED) {
            log_sys->slot_write_pos++;
        }
        if (slot_write_pos == log_sys->slot_write_pos) {
            os_event_wait_time(log_sys->writer_event, slot_sleep_microseconds, signal_count);
            signal_count = os_event_reset(log_sys->writer_event);
            continue;
        }

        uint64 start_lsn = log_sys->slots[slot_write_pos].lsn;
        uint64 end_lsn = log_sys->slots[log_sys->slot_write_pos - 1].lsn
            + log_sys->slots[log_sys->slot_write_pos - 1].data_len;
        uint64 data_len = end_lsn - start_lsn;

        // set data_len_and_first_rec_group of block
        for (uint64 i = slot_write_pos; i < log_sys->slot_write_pos; i++) {
            log_block_set_data_len_and_first_rec_group(&log_sys->slots[i % LOG_SLOT_MAX_COUNT]);
        }

        // write to file
        uint32 sleep_time = 1000000;
        uint32 disk_full_wait_count = 0, disk_full_log_interval = 60;
        while (data_len > 0) {

            log_group_t *group = &log_sys->groups[log_sys->current_write_group];
            uint64 write_size = log_compute_how_much_to_write(group, start_lsn, end_lsn);
            if (write_size == 0) {
                log_start_next_file(start_lsn);
                continue;
            }

            /* Wait until there is free space in log files.*/
            log_writer_wait_for_checkpoint(start_lsn + write_size);

            uint64 buf_begin_pos = (start_lsn - log_sys->buf_base_lsn) % log_sys->buf_size;
            if (!log_writer_write_to_file(group, buf_begin_pos, write_size)) {
                // disk is full
                if (disk_full_wait_count % disk_full_log_interval == 0) {
                    LOGGER_ERROR(LOGGER, "log_writer: disk is full, name %s", group->name);
                }
                disk_full_wait_count++;

                os_thread_sleep(sleep_time);
                continue;
            }

            data_len -= write_size;
            start_lsn += write_size;
        }

        //
        for (uint64 i = slot_write_pos; i < log_sys->slot_write_pos; i++) {
            log_sys->slots[i % LOG_SLOT_MAX_COUNT].status = LogSlotStatus::INIT;
        }

        log_sys->writer_writed_lsn = end_lsn;
        LOGGER_DEBUG(LOGGER, "LOG_WRITER: set writer_writed_lsn = %llu", log_sys->writer_writed_lsn);

        // wake up flusher
        os_event_set(log_sys->flusher_event);
    }

    /* We count the number of threads in os_thread_exit().
    A created thread should always use that to exit and not use return() to exit. */
    os_thread_exit(NULL);
    OS_THREAD_DUMMY_RETURN;
}

static void* log_flusher_thread_entry(void *arg)
{
    uint64 signal_count = 0;
    lsn_t  last_flush_lsn, flush_up_to_lsn, notified_up_to_lsn;
    uint32 microseconds = 1000;

    while (TRUE) {
        // wait event
        os_event_wait_time(log_sys->flusher_event, microseconds, signal_count);
        signal_count = os_event_reset(log_sys->flusher_event);

        last_flush_lsn = log_sys->flusher_flushed_lsn;
        flush_up_to_lsn = log_sys->writer_writed_lsn;
        if (flush_up_to_lsn <= last_flush_lsn) {
            continue;
        }

        //
        uint32 sleep_time = 1000000;
        uint32 disk_full_wait_count = 0, disk_full_log_interval = 60;
        uint8 flush_group = log_sys->current_flush_group;
        uint8 write_group = log_sys->current_write_group;
        for (;;) {
            log_group_t *group = &log_sys->groups[flush_group];
            if (!os_fsync_file(group->handle)) {
                int32 err = os_file_get_last_error();
                if (err == OS_FILE_DISK_FULL) {
                    if (disk_full_wait_count % disk_full_log_interval == 0) {
                        LOGGER_ERROR(LOGGER, "log_writer: disk is full, name %s", group->name);
                    }
                    disk_full_wait_count++;

                    os_thread_sleep(sleep_time);
                    continue;
                }
                //
                char err_info[CM_ERR_MSG_MAX_LEN];
                os_file_get_error_desc_by_err(err, err_info, CM_ERR_MSG_MAX_LEN);
                LOGGER_FATAL(LOGGER, "LOG_FLUSHER: fail to flush redo file, name %s error %s", group->name, err_info);
                goto err_exit;
            }

            if (flush_group == write_group) {
                break;
            }
            flush_group = (flush_group + 1) % log_sys->group_count;
        }

        log_sys->flusher_flushed_lsn = flush_up_to_lsn;
        LOGGER_DEBUG(LOGGER, "LOG_FLUSHER: set flusher_flushed_lsn = %llu", log_sys->flusher_flushed_lsn);
        if (write_group != flush_group) {
            LOGGER_DEBUG(LOGGER,
                "LOG_FLUSHER: switch flush group (id = %u) to group (id = %u)",
                log_sys->current_flush_group, write_group);
            log_sys->current_flush_group = write_group;
        }

        // awake session_thread
        notified_up_to_lsn = ut_uint64_align_up(flush_up_to_lsn, OS_FILE_LOG_BLOCK_SIZE);
        while (last_flush_lsn <= notified_up_to_lsn) {
            uint32 idx = ((last_flush_lsn - 1) / OS_FILE_LOG_BLOCK_SIZE) & (LOG_SESSION_WAIT_EVENT_COUNT - 1);
            last_flush_lsn += OS_FILE_LOG_BLOCK_SIZE;
            os_event_set(log_sys->session_wait_event[idx]);
        }

    }

    /* We count the number of threads in os_thread_exit().
    A created thread should always use that to exit and not use return() to exit. */
    os_thread_exit(NULL);
    OS_THREAD_DUMMY_RETURN;

err_exit:

    LOGGER_FATAL(LOGGER, "LOG_FLUSHER: A fatal error occurred, service exited.");
    ut_error;

    return NULL;
}

inline lsn_t log_get_flushed_lsn()
{
    return log_sys->flusher_flushed_lsn;
}

static uint32 log_calc_data_size(uint64 start_lsn, uint32 len)
{
    uint32 data_len;
    uint32 offset = start_lsn % OS_FILE_LOG_BLOCK_SIZE;

    /* Calculate a part length */
    if (offset + len <= OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
        /* The string fits within the current log block */
        data_len = len;
    } else {
        uint32 first_part_len, left_part_len;
        first_part_len = OS_FILE_LOG_BLOCK_SIZE - offset - LOG_BLOCK_TRL_SIZE;
        left_part_len = len - first_part_len;
        data_len = first_part_len;
        data_len += (left_part_len / LOG_BLOCK_DATA_SIZE) * OS_FILE_LOG_BLOCK_SIZE;
        if (left_part_len % LOG_BLOCK_DATA_SIZE > 0) {
            data_len += left_part_len % LOG_BLOCK_DATA_SIZE + LOG_BLOCK_HDR_SIZE;
        }
    }

    return data_len;
}

// Opens the log for log_write_low. The log must be closed with log_close and released with log_release.
// return: start lsn of the log record */
inline log_buf_lsn_t log_buffer_reserve(uint32 len) /*!< in: length of data to be catenated */
{
    log_buf_lsn_t cur_log_lsn;

    ut_a(len < log_sys->buf_size / 2);

#ifdef __WIN__

    mutex_enter(&(log_sys->mutex));
    cur_log_lsn = log_sys->buf_lsn;
    cur_log_lsn.data_len = log_calc_data_size(log_sys->buf_lsn.val.lsn, len);
    log_sys->buf_lsn.val.lsn += cur_log_lsn.data_len;
    log_sys->buf_lsn.val.slot_index++;
    mutex_exit(&(log_sys->mutex));

#else

    log_buf_lsn_t next_log_lsn;

retry_loop:

    cur_log_lsn = log_sys->buf_lsn;
    next_log_lsn = cur_log_lsn;
    cur_log_lsn.data_len += log_calc_data_size(log_sys->buf_lsn.val.lsn, len);
    next_log_lsn.val.lsn += cur_log_lsn.data_len;
    next_log_lsn.val.slot_index++;
    if (!atomic128_compare_and_swap(&log_sys->buf_lsn.val.value, cur_log_lsn.val.value, next_log_lsn.val.value)) {
        goto retry_loop;
    }

#endif

    return cur_log_lsn;
}

static void log_wait_for_write(uint64 start_lsn, uint32 str_len)
{
    uint64 log_waits = 0;

retry_check:

    ut_a(start_lsn >= log_sys->writer_writed_lsn);

    if (start_lsn - log_sys->writer_writed_lsn >= log_sys->buf_size) {
        log_waits++;
        os_thread_sleep(100);
        goto retry_check;
    }

    if (log_sys->buf_size - (start_lsn - log_sys->writer_writed_lsn) < str_len) {
        log_waits++;
        os_thread_sleep(100);
        goto retry_check;
    }

    if (log_waits > 0) {
        srv_stats.log_waits.add(log_waits);
    }
}


// Writes to the log the string given.
// It is assumed that the caller holds the log mutex.
inline uint64 log_buffer_write(uint64 start_lsn, byte *str, uint32 str_len)
{
    uint32       buf_free;
    uint32       len;
    uint32       data_len;
    byte*        log_block;

    ut_ad(!recv_no_log_write);

    // We have to wait, when we don't have enough space in the log buffer
    log_wait_for_write(start_lsn, str_len);

    buf_free = (start_lsn - log_sys->buf_base_lsn) % log_sys->buf_size;

part_loop:

    /* Calculate a part length */
    data_len = (buf_free % OS_FILE_LOG_BLOCK_SIZE) + str_len;
    if (data_len <= OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
        /* The string fits within the current log block */
        len = str_len;
    } else {
        data_len = OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE;
        len = OS_FILE_LOG_BLOCK_SIZE - (buf_free % OS_FILE_LOG_BLOCK_SIZE) - LOG_BLOCK_TRL_SIZE;
    }

    memcpy(log_sys->buf + buf_free, str, len);
    str_len -= len;
    str = str + len;

    if (data_len == OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
        /* This block became full */
        log_block = (byte*)ut_align_down(log_sys->buf + buf_free, OS_FILE_LOG_BLOCK_SIZE);
        log_block_set_data_len(log_block, OS_FILE_LOG_BLOCK_SIZE);

        //log_block_set_checkpoint_no(log_block, log_sys->next_checkpoint_no);
        len += LOG_BLOCK_HDR_SIZE + LOG_BLOCK_TRL_SIZE;
        start_lsn += len;
        /* Initialize the next block header */
        log_block_init(log_block + OS_FILE_LOG_BLOCK_SIZE, start_lsn);
    } else {
        start_lsn += len;
    }

    buf_free += len;
    if (buf_free >= log_sys->buf_size) {
        buf_free = 0;
    }

    if (str_len > 0) {
        goto part_loop;
    }

    srv_stats.log_write_requests.inc();

    return start_lsn;
}

inline void log_write_complete(log_buf_lsn_t *log_lsn)
{
    uint64 wait_slot_count = 0;

    // wait free slot
    ut_a(log_lsn->val.slot_index >= log_sys->slot_write_pos);
    while (log_lsn->val.slot_index - log_sys->slot_write_pos >= LOG_SLOT_MAX_COUNT) {
        wait_slot_count++;
        // wake up writer
        os_event_set(log_sys->writer_event);
        os_thread_sleep(20);
    }

    // set STATUS_COPIED
    log_slot_t *slot = &log_sys->slots[log_lsn->val.slot_index % LOG_SLOT_MAX_COUNT];
    ut_a(slot->status != LogSlotStatus::COPIED);
    slot->lsn = log_lsn->val.lsn;
    slot->data_len = log_lsn->data_len;
    slot->status = LogSlotStatus::COPIED;

    if (wait_slot_count > 0) {
        srv_stats.log_slot_waits.add(wait_slot_count);
    }
}

// Initializes the log
bool32 log_group_add(char *name, uint64 file_size)
{
    log_group_t *group = NULL;

    ut_a(log_sys);

    mutex_enter(&log_sys->mutex, NULL);

    if ((log_sys->group_count + 1) >= LOG_GROUP_MAX_COUNT) {
        mutex_exit(&log_sys->mutex);
        LOGGER_ERROR(LOGGER, "Error, REDO file has reached the maximum limit");
        return FALSE;
    }

    for (uint32 i = 0; i < log_sys->group_count; i++) {
        if (strlen(log_sys->groups[i].name) == strlen(name) &&
            strncmp(log_sys->groups[i].name, name, strlen(name)) == 0) {
            mutex_exit(&log_sys->mutex);
            LOGGER_ERROR(LOGGER, "redo file exists, name = %s", name);
            return FALSE;
        }
    }

    group = &log_sys->groups[log_sys->group_count];
    log_sys->group_count++;
    UT_LIST_ADD_LAST(list_node, log_sys->log_groups, group);

    mutex_exit(&log_sys->mutex);

    group->id = log_sys->group_count;
    group->status = LogGroupStatus::INACTIVE;
    group->file_size = file_size;
    group->write_offset = LOG_BUF_WRITE_MARGIN;
    log_sys->file_size += group->file_size - LOG_BUF_WRITE_MARGIN;
    group->base_lsn = 0;
    group->name = (char*)ut_malloc_zero(strlen(name) + 1);
    sprintf_s(group->name, strlen(name) + 1, "%s", name);
    group->name[strlen(name)] = '\0';

    bool32 ret = os_open_file(group->name, OS_FILE_OPEN, OS_FILE_AIO, &group->handle);
    if (!ret) {
        LOGGER_ERROR(LOGGER, "failed to open redo file, name = %s", group->name);
        return FALSE;
    }

    return TRUE;
}

// Initializes the log
status_t log_init(uint32 log_buffer_size)
{
    ut_a(log_buffer_size >= 16 * OS_FILE_LOG_BLOCK_SIZE);
    ut_a(log_buffer_size >= 4 * UNIV_PAGE_SIZE);

    log_sys = (log_t*)ut_malloc_zero(sizeof(log_t));

    mutex_create(&log_sys->mutex);
    mutex_create(&log_sys->log_flush_order_mutex);

    log_sys->buf_ptr = (byte*)ut_malloc(log_buffer_size + OS_FILE_LOG_BLOCK_SIZE);
    log_sys->buf = (byte*)ut_align(log_sys->buf_ptr, OS_FILE_LOG_BLOCK_SIZE);
    log_sys->buf_size = (uint32)log_buffer_size;
    log_sys->buf_free = LOG_BLOCK_HDR_SIZE;

    log_sys->buf_lsn.val.lsn = LOG_BLOCK_HDR_SIZE;
    log_sys->buf_lsn.val.slot_index = 0;
    log_sys->buf_lsn.data_len = 0;
    log_sys->writer_writed_lsn = 0;
    log_sys->flusher_flushed_lsn = 0;
    log_sys->buf_base_lsn = 0;

    //rw_lock_create(&log_sys->checkpoint_lock);
    log_sys->checkpoint_buf = (byte*)ut_malloc(4 * OS_FILE_LOG_BLOCK_SIZE);
    log_sys->last_checkpoint_lsn = 0;


    log_block_init(log_sys->buf, 0);
    //log_block_set_first_rec_group(log_sys->buf, LOG_BLOCK_HDR_SIZE);

    log_sys->writer_event = os_event_create(NULL);
    os_event_set(log_sys->writer_event);

    log_sys->flusher_event = os_event_create(NULL);
    os_event_set(log_sys->flusher_event);

    for (uint32 i = 0; i < LOG_SESSION_WAIT_EVENT_COUNT; i++) {
        log_sys->session_wait_event[i] = os_event_create(NULL);
        os_event_set(log_sys->session_wait_event[i]);
    }

    log_sys->slot_write_pos = 0;
    log_sys->slots = (log_slot_t *)ut_malloc(sizeof(log_slot_t) * LOG_SLOT_MAX_COUNT);
    for (uint32 i = 0; i < LOG_SLOT_MAX_COUNT; i++) {
        log_slot_t *slot = &log_sys->slots[i];
        slot->status = LogSlotStatus::INIT;
        slot->lsn = 0;
        slot->data_len = 0;
    }

    log_sys->writer_thread = os_thread_create(log_writer_thread_entry, NULL, NULL);
    if (!os_thread_is_valid(log_sys->writer_thread)) {
        LOGGER_ERROR(LOGGER, "log_init: failed to create thread of log_writer");
        return CM_ERROR;
    }
    log_sys->flusher_thread = os_thread_create(log_flusher_thread_entry, NULL, NULL);
    if (!os_thread_is_valid(log_sys->flusher_thread)) {
        LOGGER_ERROR(LOGGER, "log_init: failed to create thread of log_flusher");
        return CM_ERROR;
    }

    uint32 io_pending_count = 8;
    uint32 io_context_count = 2;
    log_sys->aio_array = os_aio_array_create(io_pending_count, io_context_count);
    if (log_sys->aio_array == NULL) {
        LOGGER_ERROR(LOGGER, "log_init: failed to create aio array");
        return CM_ERROR;
    }
    log_sys->aio_ctx_log_write = os_aio_array_alloc_context(log_sys->aio_array);
    log_sys->aio_ctx_checkpoint = os_aio_array_alloc_context(log_sys->aio_array);

    log_sys->current_write_group = 0;
    log_sys->current_flush_group = 0;
    log_sys->group_count = 0;
    log_sys->file_size = 0;

    return CM_SUCCESS;
}

static uint32 log_group_calc_group_id_by_lsn(lsn_t lsn)
{
    log_group_t* group;
    uint32 group_id = LOG_GROUP_MAX_COUNT;

    mutex_enter(&log_sys->mutex);
    for (uint32 i = 0; i < log_sys->group_count; i++) {
        group = &log_sys->groups[i];
        if (group->base_lsn < lsn && group->base_lsn + group->file_size >= lsn) {
            group_id = i;
            break;
        }
    }
    mutex_exit(&log_sys->mutex);

    return group_id;
}

static uint32 log_group_calc_group_offset_by_lsn(lsn_t lsn)
{
    log_group_t* group;
    uint32 group_offset = 0;

    mutex_enter(&log_sys->mutex);
    for (uint32 i = 0; i < log_sys->group_count; i++) {
        group = &log_sys->groups[i];
        if (group->base_lsn < lsn && group->base_lsn + group->file_size >= lsn) {
            group_offset = lsn - group->base_lsn + LOG_BUF_WRITE_MARGIN;
            break;
        }
    }
    mutex_exit(&log_sys->mutex);

    return group_offset;
}

// Writes the checkpoint info
static void log_checkpoint_write()
{
    uint64      lsn_offset;
    uint32      write_offset;
    uint64      fold;
    byte        buf[OS_FILE_LOG_BLOCK_SIZE];

    ut_ad(!srv_read_only_mode);

    mach_write_to_8(buf + LOG_CHECKPOINT_NO, log_sys->next_checkpoint_no);
    mach_write_to_8(buf + LOG_CHECKPOINT_LSN, log_sys->next_checkpoint_lsn);

    uint32 group_id = log_group_calc_group_id_by_lsn(log_sys->next_checkpoint_lsn);
    mach_write_to_4(buf + LOG_CHECKPOINT_OFFSET_LOW32, group_id);
    uint32 group_offset = log_group_calc_group_offset_by_lsn(log_sys->next_checkpoint_lsn);
    mach_write_to_4(buf + LOG_CHECKPOINT_OFFSET_HIGH32, group_offset);

    mach_write_to_8(buf + LOG_CHECKPOINT_ARCHIVED_LSN, UINT64_MAX);
    for (uint32 i = 0; i < LOG_GROUPS_MAX_COUNT; i++) {
        mach_write_to_4(buf + LOG_CHECKPOINT_GROUP_ARRAY + 8 * i + LOG_CHECKPOINT_ARCHIVED_FILE_NO, 0);
        mach_write_to_4(buf + LOG_CHECKPOINT_GROUP_ARRAY + 8 * i + LOG_CHECKPOINT_ARCHIVED_OFFSET, 0);
    }

    fold = ut_fold_binary(buf, LOG_CHECKPOINT_CHECKSUM_1);
    mach_write_to_4(buf + LOG_CHECKPOINT_CHECKSUM_1, fold);

    fold = ut_fold_binary(buf + LOG_CHECKPOINT_LSN, LOG_CHECKPOINT_CHECKSUM_2 - LOG_CHECKPOINT_LSN);
    mach_write_to_4(buf + LOG_CHECKPOINT_CHECKSUM_2, fold);

    /* We alternate the physical place of the checkpoint info in the first log file */
    if ((log_sys->next_checkpoint_no & 1) == 0) {
        write_offset = LOG_CHECKPOINT_1;
    } else {
        write_offset = LOG_CHECKPOINT_2;
    }

    //
    uint32 timeout_seconds = 300;
    log_group_t* group = &log_sys->groups[0];
    os_aio_slot_t* aio_slot = os_file_aio_submit(log_sys->aio_ctx_checkpoint, OS_FILE_WRITE,
        group->name, group->handle, (void *)buf, OS_FILE_LOG_BLOCK_SIZE, write_offset);
    if (aio_slot == NULL) {
        char errinfo[CM_ERR_MSG_MAX_LEN];
        os_file_get_last_error_desc(errinfo, CM_ERR_MSG_MAX_LEN);
        LOGGER_ERROR(LOGGER,
            "log_checkpoint_write: fail to write redo file, name %s error %s",
            group->name, errinfo);
        goto err_exit;
    }

    int32 ret = os_file_aio_context_wait(log_sys->aio_ctx_checkpoint, &aio_slot, timeout_seconds * 1000000);
    switch (ret) {
    case OS_FILE_IO_COMPLETION:
        break;
    case OS_FILE_IO_TIMEOUT:
        LOGGER_ERROR(LOGGER,
            "log_checkpoint_write: IO timeout for writing redo file, timeout : %u seconds",
            timeout_seconds);
        // fall through
    default:
        goto err_exit;
    }

    //
    if (!os_fsync_file(group->handle)) {
        char errinfo[CM_ERR_MSG_MAX_LEN];
        os_file_get_last_error_desc(errinfo, CM_ERR_MSG_MAX_LEN);
        LOGGER_ERROR(LOGGER,
            "log_checkpoint_write: fail to flush redo file, name %s error %s",
            group->name, errinfo);
        goto err_exit;
    }

    LOGGER_DEBUG(LOGGER,
        "log_checkpoint_write: checkpoint_no = %llu checkpoint_lsn = %llu",
        log_sys->next_checkpoint_no, log_sys->next_checkpoint_lsn);

    return;

err_exit:

    LOGGER_FATAL(LOGGER, "log_checkpoint_write: A fatal error occurred, service exited");
    ut_error;
}

void log_checkpoint(lsn_t checkpoint_lsn)
{
    if (log_sys->last_checkpoint_lsn >= checkpoint_lsn) {
        return;
    }

    //
    log_sys->next_checkpoint_lsn = checkpoint_lsn;
    log_checkpoint_write();

    //
    log_sys->next_checkpoint_no++;
    log_sys->last_checkpoint_lsn = log_sys->next_checkpoint_lsn;
}


// Makes a checkpoint at a given lsn or later
// lsn: make a checkpoint at this or a later lsn,
//      if ut_dulint_max, makes a checkpoint at the latest lsn
void log_make_checkpoint_at(duint64 lsn)
{
    bool32  success = FALSE;

    //while (!success) {
    //    success = log_preflush_pool_modified_pages(lsn, TRUE);
    //}

    success = FALSE;
    //while (!success) {
    //    success = log_checkpoint();
    //}
}


// Calculates the data capacity of a log group,
// when the log file headers are not included.
// return capacity in bytes
static lsn_t log_group_get_capacity(const log_group_t* group)
{
    ut_ad(mutex_own(&(log_sys->mutex)));

    //return (group->file_size - LOG_FILE_HDR_SIZE) * group->n_files;
    return group->file_size;
}

