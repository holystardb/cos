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
    ut_ad(n > 0);
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
    return lsn / OS_FILE_LOG_BLOCK_SIZE;
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

    ut_ad(mutex_own(&(log_sys->mutex)));

    no = log_block_convert_lsn_to_no(lsn);

    log_block_set_hdr_no(log_block, no);

    log_block_set_data_len(log_block, LOG_BLOCK_HDR_SIZE);
    log_block_set_first_rec_group(log_block, 0);
}

// This function is called, e.g., when a transaction wants to commit.
void log_write_up_to(lsn_t lsn)
{
    uint64   trx_sync_log_waits = 0, signal_count = 0;
    uint32   idx;
    uint32   timeout_microseconds = 1000;

    ut_ad(!srv_read_only_mode);

    idx = (lsn / OS_FILE_LOG_BLOCK_SIZE) & (LOG_SESSION_WAIT_EVENT_COUNT - 1);

    while (log_sys->flusher_flushed_lsn < lsn) {
        trx_sync_log_waits++;
        os_event_wait_time(log_sys->session_wait_event[idx], timeout_microseconds, signal_count);
        signal_count = os_event_reset(log_sys->session_wait_event[idx]);
    }

    srv_stats.trx_sync_log_waits.add(trx_sync_log_waits);
}

static bool32 log_writer_write_to_file(log_group_t *group, uint64 start_pos, uint64 end_pos)
{
    uint64 log_checkpoint_waits = 0;
    uint32 timeout_seconds = 300; // 300 seconds
    uint64 offset = group->write_offset;

retry_loop:

    if (group->status == LogGroupStatus::INACTIVE) {
        // modify log file status
    } else if (group->status == LogGroupStatus::ACTIVE) {
        log_checkpoint_waits++;
        //os_event_set(checkpoint_event);
        os_thread_sleep(1000); // 0.1 second
        goto retry_loop;
    }

    srv_stats.log_checkpoint_waits.add(log_checkpoint_waits);

    if (start_pos < end_pos) {
        if (!os_file_aio_submit(group->aio_ctx, OS_FILE_WRITE, group->name, group->handle,
            (void *)(log_sys->buf + start_pos), (uint32)(end_pos - start_pos), offset,
            NULL, NULL)) {
            char errinfo[1024];
            os_file_get_last_error_desc(errinfo, 1024);
            LOGGER_FATAL(LOGGER, "fail to write redo file, name %s error %s", group->name, errinfo);
            goto err_exit;
        }
    } else {
        if (!os_file_aio_submit(group->aio_ctx, OS_FILE_WRITE, group->name, group->handle,
                                (void *)(log_sys->buf + start_pos), log_sys->buf_size - (uint32)start_pos, offset,
                                NULL, NULL)) {
            char errinfo[1024];
            os_file_get_last_error_desc(errinfo, 1024);
            LOGGER_FATAL(LOGGER, "fail to write redo file, name %s error %s", group->name, errinfo);
            goto err_exit;
        }
        offset += log_sys->buf_size - start_pos;
        if (!os_file_aio_submit(group->aio_ctx, OS_FILE_WRITE, group->name, group->handle,
                                (void *)log_sys->buf, (uint32)end_pos, offset, NULL, NULL)) {
            char errinfo[1024];
            os_file_get_last_error_desc(errinfo, 1024);
            LOGGER_FATAL(LOGGER, "fail to write redo file, name %s error %s", group->name, errinfo);
            goto err_exit;
        }
    }

    int ret = os_file_aio_wait(group->aio_ctx, timeout_seconds * 1000000);
    switch (ret) {
    case OS_FILE_IO_COMPLETION:
        break;
    case OS_FILE_IO_TIMEOUT:
        LOGGER_FATAL(LOGGER, "IO timeout for writing redo file, timeout : %u seconds", timeout_seconds);
        goto err_exit;

    default:
        char err_info[1024];
        os_file_get_last_error_desc(err_info, 1024);
        LOGGER_FATAL(LOGGER, "fail to flush redo file, name %s error %s", group->name, err_info);
        goto err_exit;
    }

    group->write_offset += (end_pos - start_pos);

    return ret;

err_exit:

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

    while (write_up_lsn >= checkpoint_lsn) {
        log_checkpoint_waits++;

        //os_event_set(log_sys->checkpoint_event);

        // wake up by checkpoint thread
        os_event_wait_time(log_sys->writer_event, timeout_microseconds, signal_count);
        signal_count = os_event_reset(log_sys->writer_event);
    }

    srv_stats.log_checkpoint_waits.add(log_checkpoint_waits);
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
}

static void* log_writer_thread_entry(void *arg)
{
    uint64 slot_write_pos;
    uint32 slot_sleep_microseconds = 1000;

    while (TRUE) {

        slot_write_pos = log_sys->slot_write_pos;
        while (log_sys->slots[log_sys->slot_write_pos].status == LogSlotStatus::COPIED) {
            log_sys->slot_write_pos++;
        }
        if (slot_write_pos == log_sys->slot_write_pos) {
            os_thread_sleep(slot_sleep_microseconds);
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
            bool32 ret = log_writer_write_to_file(group, buf_begin_pos, write_size);
            if (ret == FALSE) {
                goto err_exit;
            }

            data_len -= write_size;
            start_lsn += write_size;
        }

        //
        for (uint64 i = slot_write_pos; i < log_sys->slot_write_pos; i++) {
            log_sys->slots[i % LOG_SLOT_MAX_COUNT].status = LogSlotStatus::INIT;
        }

        log_sys->writer_writed_lsn = end_lsn;

        // wake up flusher
        os_event_set(log_sys->flusher_event);
    }

    /* We count the number of threads in os_thread_exit().
    A created thread should always use that to exit and not use return() to exit. */
    os_thread_exit(NULL);
    OS_THREAD_DUMMY_RETURN;

err_exit:

    LOGGER_FATAL(LOGGER, "log_writer: A fatal error occurred, service exited.");
    exit(1);
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
        uint8 flush_group = log_sys->current_flush_group;
        uint8 write_group = log_sys->current_write_group;
        for (;;) {
            log_group_t *group = &log_sys->groups[flush_group];
            if (!os_fsync_file(group->handle)) {
                char errinfo[1024];
                os_file_get_last_error_desc(errinfo, 1024);
                LOGGER_FATAL(LOGGER, "fail to flush redo file, name %s error %s", group->name, errinfo);
                goto err_exit;
            }

            if (flush_group == write_group) {
                break;
            }
            flush_group = (flush_group + 1) % log_sys->group_count;
        }

        log_sys->flusher_flushed_lsn = flush_up_to_lsn;
        if (write_group != flush_group) {
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

    LOGGER_FATAL(LOGGER, "log_flusher: A fatal error occurred, service exited.");
    exit(1);
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
        data_len += left_part_len / LOG_BLOCK_DATA_SIZE * OS_FILE_LOG_BLOCK_SIZE;
        if (left_part_len % LOG_BLOCK_DATA_SIZE > 0) {
            data_len += left_part_len % LOG_BLOCK_DATA_SIZE + LOG_BLOCK_HDR_SIZE;
        }
    }

    return data_len;
}

// Opens the log for log_write_low. The log must be closed with log_close and released with log_release.
// return: start lsn of the log record */
log_buf_lsn_t log_buffer_reserve(uint32 len) /*!< in: length of data to be catenated */
{
    uint64        log_waits = 0;
    log_buf_lsn_t cur_log_lsn;

    ut_a(len < log_sys->buf_size / 2);
    ut_a(log_sys->buf_lsn.val.lsn >= log_sys->writer_writed_lsn);

    while (log_sys->buf_lsn.val.lsn - log_sys->writer_writed_lsn + len > log_sys->buf_size) {
        log_waits++;
        os_thread_sleep(100);
    }

    srv_stats.log_waits.add(log_waits);

#ifdef __WIN__

    mutex_enter(&(log_sys->mutex));
    cur_log_lsn = log_sys->buf_lsn;
    cur_log_lsn.data_len += log_calc_data_size(log_sys->buf_lsn.val.lsn, len);
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

    srv_stats.log_waits.add(log_waits);
}


// Writes to the log the string given.
// It is assumed that the caller holds the log mutex.
uint64 log_buffer_write(uint64 start_lsn, byte *str, uint32 str_len)
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

void log_write_complete(log_buf_lsn_t *log_lsn)
{
    uint64 wait_slot_count = 0;

    // wait free slot
    ut_a(log_lsn->val.slot_index > log_sys->slot_write_pos);
    while (log_lsn->val.slot_index - log_sys->slot_write_pos >= LOG_SLOT_MAX_COUNT) {
        wait_slot_count++;
        os_thread_sleep(100);
    }

    // set STATUS_COPIED
    log_slot_t *slot = &log_sys->slots[log_lsn->val.slot_index % LOG_SLOT_MAX_COUNT];
    ut_a(slot->status != LogSlotStatus::COPIED);
    slot->lsn = log_lsn->val.lsn;
    slot->data_len = log_lsn->data_len;
    slot->status = LogSlotStatus::COPIED;

    srv_stats.log_slot_waits.add(wait_slot_count);
}

// Initializes the log
bool32 log_group_add(char *name, uint64 file_size)
{
    log_group_t *group = NULL;

    ut_a(log_sys);

    mutex_enter(&log_sys->mutex, NULL);

    if (log_sys->group_count >= LOG_GROUP_MAX_COUNT) {
        mutex_exit(&log_sys->mutex);

        LOGGER_ERROR(LOGGER, "log_group_add: Error, REDO file has reached the maximum limit");
        return FALSE;
    }

    group = &log_sys->groups[log_sys->group_count];
    log_sys->group_count++;

    group->id = log_sys->group_count;
    group->status = LogGroupStatus::INACTIVE;
    group->aio_ctx = os_aio_array_alloc_context(log_sys->aio_array);
    group->file_size = file_size;
    group->write_offset = LOG_BUF_WRITE_MARGIN;
    group->base_lsn = 0;
    group->name = (char*)malloc(strlen(name) + 1);
    sprintf_s(group->name, strlen(name) + 1, "%s", name);
    group->name[strlen(name)] = '\0';

    bool32 ret = os_open_file(group->name, OS_FILE_OPEN, OS_FILE_AIO, &group->handle);
    if (!ret) {
        mutex_exit(&log_sys->mutex);

        LOGGER_ERROR(LOGGER, "log_init: failed to open redo file, name = %s", group->name);
        return FALSE;
    }

    mutex_exit(&log_sys->mutex);

    return TRUE;
}

// Initializes the log
bool32 log_init()
{
    ut_a(srv_redo_log_buffer_size >= 16 * OS_FILE_LOG_BLOCK_SIZE);
    ut_a(srv_redo_log_buffer_size >= 4 * UNIV_PAGE_SIZE);

    log_sys = (log_t*)malloc(sizeof(log_t));

    mutex_create(&log_sys->mutex);

    log_sys->buf_ptr = (byte*)malloc(srv_redo_log_buffer_size + OS_FILE_LOG_BLOCK_SIZE);
    log_sys->buf = (byte*)ut_align(log_sys->buf_ptr, OS_FILE_LOG_BLOCK_SIZE);
    log_sys->buf_size = (uint32)srv_redo_log_buffer_size;
    log_sys->buf_free = LOG_BLOCK_HDR_SIZE;

    log_sys->buf_lsn.val.lsn = LOG_START_LSN + LOG_BLOCK_HDR_SIZE;
    log_sys->buf_lsn.val.slot_index = 0;
    log_sys->writer_writed_lsn = 0;
    log_sys->flusher_flushed_lsn = 0;
    log_sys->buf_base_lsn = 0;

    //rw_lock_create(&log_sys->checkpoint_lock);
    //log_sys->checkpoint_buf_ptr = (byte*)malloc(2 * OS_FILE_LOG_BLOCK_SIZE);
    //log_sys->checkpoint_buf = (byte*)ut_align(log_sys->checkpoint_buf_ptr, OS_FILE_LOG_BLOCK_SIZE);

    log_block_init(log_sys->buf, log_sys->buf_lsn.val.lsn);
    log_block_set_first_rec_group(log_sys->buf, LOG_BLOCK_HDR_SIZE);

    log_sys->writer_event = os_event_create(NULL);
    os_event_set(log_sys->writer_event);

    log_sys->flusher_event = os_event_create(NULL);
    os_event_set(log_sys->flusher_event);

    for (uint32 i = 0; i < LOG_SESSION_WAIT_EVENT_COUNT; i++) {
        log_sys->session_wait_event[i] = os_event_create(NULL);
        os_event_set(log_sys->session_wait_event[i]);
    }

    log_sys->slot_write_pos = 0;
    log_sys->slots = (log_slot_t *)malloc(sizeof(log_slot_t) * LOG_SLOT_MAX_COUNT);
    for (uint32 i = 0; i < LOG_SLOT_MAX_COUNT; i++) {
        log_slot_t *slot = &log_sys->slots[i];
        slot->status = LogSlotStatus::INIT;
        slot->lsn = 0;
        slot->data_len = 0;
    }

    log_sys->writer_thread = os_thread_create(log_writer_thread_entry, NULL, NULL);
    if (!os_thread_is_valid(log_sys->writer_thread)) {
        LOGGER_ERROR(LOGGER, "log_init: failed to create thread of log_writer");
        return FALSE;
    }
    log_sys->flusher_thread = os_thread_create(log_flusher_thread_entry, NULL, NULL);
    if (!os_thread_is_valid(log_sys->flusher_thread)) {
        LOGGER_ERROR(LOGGER, "log_init: failed to create thread of log_flusher");
        return FALSE;
    }

    uint32 max_io_operation_count = LOG_GROUP_MAX_COUNT;
    uint32 io_context_count = LOG_GROUP_MAX_COUNT;
    log_sys->aio_array = os_aio_array_create(max_io_operation_count, io_context_count);
    if (log_sys->aio_array == NULL) {
        LOGGER_ERROR(LOGGER, "log_init: failed to create aio array");
        return FALSE;
    }

    log_sys->current_write_group = 0;
    log_sys->current_flush_group = 0;
    log_sys->group_count = 0;

    return TRUE;
}




// Writes the checkpoint info
static void log_checkpoint_write()
{
    uint64      lsn_offset;
    uint32      write_offset;
    uint64      fold;
    byte        buf[OS_FILE_LOG_BLOCK_SIZE];

    ut_ad(!srv_read_only_mode);

    ut_ad(mutex_own(&(log_sys->mutex)));

    mach_write_to_8(buf + LOG_CHECKPOINT_NO, log_sys->next_checkpoint_no);
    mach_write_to_8(buf + LOG_CHECKPOINT_LSN, log_sys->next_checkpoint_lsn);

    //lsn_offset = log_group_calc_lsn_offset(log_sys->next_checkpoint_lsn, group);
    lsn_offset = 0;
    mach_write_to_4(buf + LOG_CHECKPOINT_OFFSET_LOW32, lsn_offset & 0xFFFFFFFFUL);
    mach_write_to_4(buf + LOG_CHECKPOINT_OFFSET_HIGH32, lsn_offset >> 32);
    mach_write_to_4(buf + LOG_CHECKPOINT_LOG_BUF_SIZE, log_sys->buf_size);
    mach_write_to_8(buf + LOG_CHECKPOINT_ARCHIVED_LSN, UINT64_MAX);

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

    //log_sys->n_log_ios++;
    uint32      timeout_seconds = 300;
    log_group_t *group = &log_sys->groups[0];
    if (!os_file_aio_submit(group->aio_ctx, OS_FILE_WRITE, group->name, group->handle,
                            (void *)buf, OS_FILE_LOG_BLOCK_SIZE, write_offset, NULL, NULL)) {
        char errinfo[1024];
        os_file_get_last_error_desc(errinfo, 1024);
        LOGGER_FATAL(LOGGER, "checkpoint: fail to write redo file, name %s error %s", group->name, errinfo);
        goto err_exit;
    }

    int ret = os_file_aio_wait(group->aio_ctx, timeout_seconds * 1000000);
    switch (ret) {
    case OS_FILE_IO_COMPLETION:
        break;
    case OS_FILE_IO_TIMEOUT:
    default:
        LOGGER_FATAL(LOGGER, "checkpoint: IO timeout for writing redo file, timeout : %u seconds", timeout_seconds);
        goto err_exit;
    }

    return;

err_exit:

    LOGGER_FATAL(LOGGER, "checkpoint: A fatal error occurred, service exited.");
    exit(1);

}

void log_io_complete_checkpoint(void)
{
    mutex_enter(&(log_sys->mutex), NULL);

    ut_ad(log_sys->n_pending_checkpoint_writes > 0);

    log_sys->n_pending_checkpoint_writes--;

    if (log_sys->n_pending_checkpoint_writes == 0) {
        log_sys->next_checkpoint_no++;
        log_sys->last_checkpoint_lsn = log_sys->next_checkpoint_lsn;
    }

    mutex_exit(&(log_sys->mutex));
}


void log_checkpoint()
{
    uint64 oldest_lsn;

    ut_ad(!recv_no_log_write);

    mutex_enter(&(log_sys->mutex));

    oldest_lsn = buf_pool_get_oldest_modification();

    mutex_exit(&(log_sys->mutex));

    log_write_up_to(oldest_lsn);

    log_io_complete_checkpoint();
    log_checkpoint_write();
}


