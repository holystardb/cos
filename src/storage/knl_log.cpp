#include "knl_log.h"
#include "cm_dbug.h"

/* A margin for free space in the log buffer before a log entry is catenated */
#define LOG_BUF_WRITE_MARGIN    (4 * OS_FILE_LOG_BLOCK_SIZE)

/* Margins for free space in the log buffer after a log entry is catenated */
#define LOG_BUF_FLUSH_RATIO     2
#define LOG_BUF_FLUSH_MARGIN    (LOG_BUF_WRITE_MARGIN + 4 * UNIV_PAGE_SIZE)



log_t*    log_sys = NULL;



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
    return mach_read_from_4(log_block + LOG_BLOCK_HDR_NO);
}

void log_block_set_hdr_no(byte* log_block, uint64 n)
{
    ut_ad(n > 0);
    mach_write_to_4(log_block + LOG_BLOCK_HDR_NO, n);
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
void log_block_init(byte* log_block, lsn_t lsn)
{
    uint32 no;

    ut_ad(mutex_own(&(log_sys->mutex)));

    no = log_block_convert_lsn_to_no(lsn);

    log_block_set_hdr_no(log_block, no);

    log_block_set_data_len(log_block, LOG_BLOCK_HDR_SIZE);
    log_block_set_first_rec_group(log_block, 0);
}

// Calculates the data capacity of a log group,
// when the log file headers are not included.
lsn_t log_group_get_capacity(const log_group_t* group)
{
    ut_ad(mutex_own(&(log_sys->mutex)));
    return((group->file_size - LOG_FILE_HDR_SIZE) * group->n_files);
}

//Calculates the offset within a log group,
//when the log file headers are not included.
lsn_t log_group_calc_size_offset(
    lsn_t offset, //real offset within the log group
    const log_group_t* group)
{
    ut_ad(mutex_own(&(log_sys->mutex)));
    return(offset - LOG_FILE_HDR_SIZE * (1 + offset / group->file_size));
}

//Calculates the offset within a log group,
//when the log file headers are included.
lsn_t log_group_calc_real_offset(
    lsn_t offset, //size offset within the log group
    const log_group_t* group)
{
    ut_ad(mutex_own(&(log_sys->mutex)));
    return(offset + LOG_FILE_HDR_SIZE * (1 + offset / (group->file_size - LOG_FILE_HDR_SIZE)));
}

//Calculates the offset of an lsn within a log group.
static lsn_t log_group_calc_lsn_offset(lsn_t lsn, const log_group_t* group)
{
    lsn_t gr_lsn;
    lsn_t gr_lsn_size_offset;
    lsn_t difference;
    lsn_t group_size;
    lsn_t offset;

    ut_ad(mutex_own(&(log_sys->mutex)));

    gr_lsn = group->lsn;
    gr_lsn_size_offset = log_group_calc_size_offset(group->lsn_offset, group);
    group_size = log_group_get_capacity(group);

    if (lsn >= gr_lsn) {
        difference = lsn - gr_lsn;
    } else {
        difference = gr_lsn - lsn;
        difference = difference % group_size;
        difference = group_size - difference;
    }

    offset = (gr_lsn_size_offset + difference) % group_size;

    /* fprintf(stderr, "Offset is " LSN_PF " gr_lsn_offset is " LSN_PF
    " difference is " LSN_PF "\n", offset, gr_lsn_size_offset, difference);
    */

    return(log_group_calc_real_offset(offset, group));
}

//Writes a log file header to a log file space.
static void log_group_file_header_flush(
    log_group_t* group, //in: log group
    uint32 nth_file, //in: header to the nth file in the log file space
    lsn_t start_lsn) //in: log file data starts at this lsn
{
    byte* buf;
    lsn_t dest_offset;

    ut_ad(mutex_own(&(log_sys->mutex)));
    ut_ad(!recv_no_log_write);
    ut_a(nth_file < group->n_files);

    buf = *(group->file_header_bufs + nth_file);
    mach_write_to_4(buf + LOG_GROUP_ID, group->id);
    mach_write_to_8(buf + LOG_FILE_START_LSN, start_lsn);
    /* Wipe over possible label of ibbackup --restore */
    memcpy(buf + LOG_FILE_WAS_CREATED_BY_HOT_BACKUP, "    ", 4);
    dest_offset = nth_file * group->file_size;

#ifdef UNIV_DEBUG
    fprintf(stderr,
        "Writing log file header to group %lu file %lu\n",
        (ulong) group->id, (ulong) nth_file);
#endif /* UNIV_DEBUG */

    log_sys->n_log_ios++;
    srv_stats.os_log_pending_writes.inc();
    fil_io(OS_FILE_WRITE | OS_FILE_LOG, true, group->space_id, 0,
           (uint32) (dest_offset / UNIV_PAGE_SIZE),
           (uint32) (dest_offset % UNIV_PAGE_SIZE),
           OS_FILE_LOG_BLOCK_SIZE,
           buf, group);
    srv_stats.os_log_pending_writes.dec();
}


//Writes a buffer to a log file group.
void log_group_write_buf(
    log_group_t* group,  //in: log group */
    byte*        buf,    //in: buffer */
    uint32       len,    //in: buffer len; must be divisible by OS_FILE_LOG_BLOCK_SIZE
    lsn_t        start_lsn,  //in: start lsn of the buffer; must be divisible by OS_FILE_LOG_BLOCK_SIZE
    uint32       new_data_offset) //in: start offset of new data in buf:
                                  //    this parameter is used to decide
                                  //    if we have to write a new log file header
{
    uint32      write_len;
    bool32      write_header;
    lsn_t       next_offset;
    uint32      i;

    ut_ad(mutex_own(&(log_sys->mutex)));
    ut_ad(!recv_no_log_write);
    ut_a(len % OS_FILE_LOG_BLOCK_SIZE == 0);
    ut_a(start_lsn % OS_FILE_LOG_BLOCK_SIZE == 0);

    if (new_data_offset == 0) {
        write_header = TRUE;
    } else {
        write_header = FALSE;
    }

loop:

    if (len == 0) {
        return;
    }

    next_offset = log_group_calc_lsn_offset(start_lsn, group);
    if ((next_offset % group->file_size == LOG_FILE_HDR_SIZE) && write_header) {
        /* We start to write a new log file instance in the group */
        ut_a(next_offset / group->file_size <= UINT32_MAX);

        log_group_file_header_flush(group,
                        (uint32)(next_offset / group->file_size),
                        start_lsn);
        srv_stats.os_log_written.add(OS_FILE_LOG_BLOCK_SIZE);
        srv_stats.log_writes.inc();
    }

    if ((next_offset % group->file_size) + len > group->file_size) {
        /* if the above condition holds, then the below expression
        is < len which is ulint, so the typecast is ok */
        write_len = (uint32)(group->file_size - (next_offset % group->file_size));
    } else {
        write_len = len;
    }

#ifdef UNIV_DEBUG
    fprintf(stderr,
        "Writing log file segment to group %lu"
        " offset " LSN_PF " len %lu\n"
        "start lsn " LSN_PF "\n"
        "First block n:o %lu last block n:o %lu\n",
        (ulong) group->id, next_offset,
        write_len,
        start_lsn,
        (ulong) log_block_get_hdr_no(buf),
        (ulong) log_block_get_hdr_no(
            buf + write_len - OS_FILE_LOG_BLOCK_SIZE));
    ut_a(log_block_get_hdr_no(buf) == log_block_convert_lsn_to_no(start_lsn));

    for (i = 0; i < write_len / OS_FILE_LOG_BLOCK_SIZE; i++) {
        ut_a(log_block_get_hdr_no(buf) + i
             == log_block_get_hdr_no(buf + i * OS_FILE_LOG_BLOCK_SIZE));
    }
#endif /* UNIV_DEBUG */

    /* Calculate the checksums for each log block and write them to
    the trailer fields of the log blocks */

    for (i = 0; i < write_len / OS_FILE_LOG_BLOCK_SIZE; i++) {
        log_block_store_checksum(buf + i * OS_FILE_LOG_BLOCK_SIZE);
    }

    log_sys->n_log_ios++;
    srv_stats.os_log_pending_writes.inc();

    ut_a(next_offset / UNIV_PAGE_SIZE <= ULINT_MAX);

    fil_io(OS_FILE_WRITE | OS_FILE_LOG, true, group->space_id, 0,
           (uint32) (next_offset / UNIV_PAGE_SIZE),
           (uint32) (next_offset % UNIV_PAGE_SIZE), write_len, buf,
           group);

    srv_stats.os_log_pending_writes.dec();
    srv_stats.os_log_written.add(write_len);
    srv_stats.log_writes.inc();

    if (write_len < len) {
        start_lsn += write_len;
        len -= write_len;
        buf += write_len;
        write_header = TRUE;

        goto loop;
    }
}

// Sets the field values in group to correspond to a given lsn.
// For this function to work,
// the values must already be correctly initialized to correspond to some lsn,
// for instance, a checkpoint lsn.
void log_group_set_fields(
    log_group_t* group, //in/out: group
    lsn_t lsn) //in: lsn for which the values should be set
{
    group->lsn_offset = log_group_calc_lsn_offset(lsn, group);
    group->lsn = lsn;
}


/******************************************************//**
This function is called, e.g., when a transaction wants to commit.
It checks that the log has been written to the log file up to the last log entry written
by the transaction. If there is a flush running, it waits and checks if the
flush flushed enough. If not, starts a new flush. */
void log_write_up_to(lsn_t lsn,
    uint32 wait,// LOG_NO_WAIT, LOG_WAIT_ONE_GROUP, LOG_WAIT_ALL_GROUPS
    bool32 flush_to_disk)
{
    log_group_t* group;
    uint32       start_offset;
    uint32       end_offset;
    uint32       area_start;
    uint32       area_end;
    uint32       unlock;

    ut_ad(!srv_read_only_mode);

    if (recv_no_ibuf_operations) {
        /* Recovery is running and no operations on the log files are
        allowed yet (the variable name .._no_ibuf_.. is misleading) */
        return;
    }

loop:

    mutex_enter(&(log_sys->mutex));
    ut_ad(!recv_no_log_write);

    if (flush_to_disk && log_sys->flushed_to_disk_lsn >= lsn) {
        mutex_exit(&(log_sys->mutex));
        return;
    }

    if (!flush_to_disk &&
        (log_sys->written_to_all_lsn >= lsn || (log_sys->written_to_some_lsn >= lsn
         && wait != LOG_WAIT_ALL_GROUPS))) {
        mutex_exit(&(log_sys->mutex));
        return;
    }

    if (log_sys->n_pending_writes > 0) {
        /* A write (+ possibly flush to disk) is running */

        if (flush_to_disk && log_sys->current_flush_lsn >= lsn) {
            /* The write + flush will write enough: wait for it to complete */
            goto do_waits;
        }

        if (!flush_to_disk && log_sys->write_lsn >= lsn) {
            /* The write will write enough: wait for it to complete */
            goto do_waits;
        }

        mutex_exit(&(log_sys->mutex));

        /* Wait for the write to complete and try to start a new write */
        os_event_wait(log_sys->no_flush_event);

        goto loop;
    }

    if (!flush_to_disk && log_sys->buf_free == log_sys->buf_next_to_write) {
        /* Nothing to write and no flush to disk requested */
        mutex_exit(&(log_sys->mutex));
        return;
    }

    //fprintf(stderr,
    //        "Writing log from " LSN_PF " up to lsn " LSN_PF "\n",
    //        log_sys->written_to_all_lsn, log_sys->lsn);

    log_sys->n_pending_writes++;

    group = UT_LIST_GET_FIRST(log_sys->log_groups);
    group->n_pending_writes++;  //We assume here that we have only one log group!

    os_event_reset(log_sys->no_flush_event);
    os_event_reset(log_sys->one_flushed_event);

    start_offset = log_sys->buf_next_to_write;
    end_offset = log_sys->buf_free;

    area_start = ut_calc_align_down(start_offset, OS_FILE_LOG_BLOCK_SIZE);
    area_end = ut_calc_align(end_offset, OS_FILE_LOG_BLOCK_SIZE);

    ut_ad(area_end - area_start > 0);

    log_sys->write_lsn = log_sys->lsn;

    if (flush_to_disk) {
        log_sys->current_flush_lsn = log_sys->lsn;
    }

    log_sys->one_flushed = FALSE;

    log_block_set_flush_bit(log_sys->buf + area_start, TRUE);
    //log_block_set_checkpoint_no(
    //    log_sys->buf + area_end - OS_FILE_LOG_BLOCK_SIZE,
    //    log_sys->next_checkpoint_no);

    /* Copy the last, incompletely written, log block a log block length up,
       so that when the flush operation writes from the log buffer,
       the segment to write will not be changed by writers to the log */
    _memcpy(log_sys->buf + area_end,
          log_sys->buf + area_end - OS_FILE_LOG_BLOCK_SIZE,
          OS_FILE_LOG_BLOCK_SIZE);
    log_sys->buf_free += OS_FILE_LOG_BLOCK_SIZE;
    log_sys->write_end_offset = log_sys->buf_free;

    group = UT_LIST_GET_FIRST(log_sys->log_groups);
    /* Do the write to the log files */
    while (group) {
        log_group_write_buf(
            group, log_sys->buf + area_start,
            area_end - area_start,
            ut_uint64_align_down(log_sys->written_to_all_lsn, OS_FILE_LOG_BLOCK_SIZE),
            start_offset - area_start);

        log_group_set_fields(group, log_sys->write_lsn);

        group = UT_LIST_GET_NEXT(log_groups, group);
    }

    mutex_exit(&(log_sys->mutex));

    if (srv_unix_file_flush_method == SRV_UNIX_O_DSYNC) {
        /* O_DSYNC means the OS did not buffer the log file at all:
        so we have also flushed to disk what we have written */
        log_sys->flushed_to_disk_lsn = log_sys->write_lsn;
    } else if (flush_to_disk) {
        group = UT_LIST_GET_FIRST(log_sys->log_groups);
        fil_flush(group->space_id);
        log_sys->flushed_to_disk_lsn = log_sys->write_lsn;
    }

    mutex_enter(&(log_sys->mutex));

    group = UT_LIST_GET_FIRST(log_sys->log_groups);

    ut_a(group->n_pending_writes == 1);
    ut_a(log_sys->n_pending_writes == 1);

    group->n_pending_writes--;
    log_sys->n_pending_writes--;

    unlock = log_group_check_flush_completion(group);
    unlock = unlock | log_sys_check_flush_completion();

    log_flush_do_unlocks(unlock);

    mutex_exit(&(log_sys->mutex));

    return;

do_waits:
    mutex_exit(&(log_sys->mutex));

    switch (wait) {
    case LOG_WAIT_ONE_GROUP:
        os_event_wait(log_sys->one_flushed_event);
        break;
    case LOG_WAIT_ALL_GROUPS:
        os_event_wait(log_sys->no_flush_event);
        break;
    }

}

//Does a syncronous flush of the log buffer to disk. */
void log_buffer_flush_to_disk(void)
{
    lsn_t lsn;

    ut_ad(!srv_read_only_mode);

    mutex_enter(&(log_sys->mutex));
    lsn = log_sys->lsn;
    mutex_exit(&(log_sys->mutex));

    log_write_up_to(lsn, LOG_WAIT_ALL_GROUPS, TRUE);
}






//Inits a log group to the log system.
void log_group_init(
    uint32 id,          /*!< in: group id */
    uint32 n_files,     /*!< in: number of log files */
    lsn_t  file_size,   /*!< in: log file size in bytes */
    uint32 space_id,    /*!< in: space id of the file space
        which contains the log files of this group */
    uint32 archive_space_id __attribute__((unused)))
        /*!< in: space id of the file space
        which contains some archived log
        files for this group; currently, only
        for the first log group this is used */
{
    uint32 i;
    log_group_t* group;

    group = (log_group_t*)malloc(sizeof(log_group_t));
    group->id = id;
    group->n_files = n_files;
    group->file_size = file_size;
    group->space_id = space_id;
    group->state = LOG_GROUP_OK;
    group->lsn = LOG_START_LSN;
    group->lsn_offset = LOG_FILE_HDR_SIZE;
    group->n_pending_writes = 0;

    group->file_header_bufs_ptr = (byte**)malloc(sizeof(byte*) * n_files);
    group->file_header_bufs = (byte**)malloc(sizeof(byte**) * n_files);

#ifdef UNIV_LOG_ARCHIVE
    group->archive_file_header_bufs_ptr = (byte*)malloc(sizeof(byte*) * n_files);
    group->archive_file_header_bufs = (byte*)malloc(sizeof(byte*) * n_files);
#endif /* UNIV_LOG_ARCHIVE */

    for (i = 0; i < n_files; i++) {
        group->file_header_bufs_ptr[i] = (byte*)malloc(LOG_FILE_HDR_SIZE + OS_FILE_LOG_BLOCK_SIZE);
        group->file_header_bufs[i] = (byte*)ut_align(group->file_header_bufs_ptr[i], OS_FILE_LOG_BLOCK_SIZE);
#ifdef UNIV_LOG_ARCHIVE
        group->archive_file_header_bufs_ptr[i] = (byte*)malloc(LOG_FILE_HDR_SIZE + OS_FILE_LOG_BLOCK_SIZE);
        group->archive_file_header_bufs[i] = (byte*)ut_align(group->archive_file_header_bufs_ptr[i], OS_FILE_LOG_BLOCK_SIZE);
#endif /* UNIV_LOG_ARCHIVE */
    }

#ifdef UNIV_LOG_ARCHIVE
    group->archive_space_id = archive_space_id;
    group->archived_file_no = 0;
    group->archived_offset = 0;
#endif /* UNIV_LOG_ARCHIVE */

    group->checkpoint_buf_ptr = (byte*)malloc(2 * OS_FILE_LOG_BLOCK_SIZE);
    group->checkpoint_buf = (byte*)ut_align(group->checkpoint_buf_ptr,OS_FILE_LOG_BLOCK_SIZE);

    UT_LIST_ADD_LAST(log_groups, log_sys->log_groups, group);

    //ut_a(log_calc_max_ages());
}



// Initializes the log
void log_init(void)
{
    log_sys = (log_t*)malloc(sizeof(log_t));

    mutex_create(&log_sys->mutex);
    mutex_create(&log_sys->log_flush_order_mutex);

    mutex_enter(&log_sys->mutex, NULL);

    /* Start the lsn from one log block from zero: this way every
    log record has a start lsn != zero, a fact which we will use */

    log_sys->lsn = LOG_START_LSN;

    ut_a(LOG_BUFFER_SIZE >= 16 * OS_FILE_LOG_BLOCK_SIZE);
    ut_a(LOG_BUFFER_SIZE >= 4 * UNIV_PAGE_SIZE);

    log_sys->buf_ptr = (byte*)malloc(LOG_BUFFER_SIZE + OS_FILE_LOG_BLOCK_SIZE);
    log_sys->buf = (byte*)ut_align(log_sys->buf_ptr, OS_FILE_LOG_BLOCK_SIZE);
    log_sys->buf_size = LOG_BUFFER_SIZE;

    log_sys->max_buf_free = log_sys->buf_size / LOG_BUF_FLUSH_RATIO - LOG_BUF_FLUSH_MARGIN;
    log_sys->check_flush_or_checkpoint = TRUE;
    UT_LIST_INIT(log_sys->log_groups);

    log_sys->n_log_ios = 0;

    log_sys->n_log_ios_old = log_sys->n_log_ios;
    log_sys->last_printout_time = time(NULL);
    /*----------------------------*/

    log_sys->buf_next_to_write = 0;

    log_sys->write_lsn = 0;
    log_sys->current_flush_lsn = 0;
    log_sys->flushed_to_disk_lsn = 0;

    log_sys->written_to_some_lsn = log_sys->lsn;
    log_sys->written_to_all_lsn = log_sys->lsn;

    log_sys->n_pending_writes = 0;

    log_sys->no_flush_event = os_event_create();
    os_event_set(log_sys->no_flush_event);

    log_sys->one_flushed_event = os_event_create();
    os_event_set(log_sys->one_flushed_event);

    /*----------------------------*/

    log_sys->next_checkpoint_no = 0;
    log_sys->last_checkpoint_lsn = log_sys->lsn;
    log_sys->n_pending_checkpoint_writes = 0;


    rw_lock_create(&log_sys->checkpoint_lock);

    log_sys->checkpoint_buf_ptr = (byte*)malloc(2 * OS_FILE_LOG_BLOCK_SIZE);
    log_sys->checkpoint_buf = (byte*)ut_align(log_sys->checkpoint_buf_ptr, OS_FILE_LOG_BLOCK_SIZE);

    log_block_init(log_sys->buf, log_sys->lsn);
    log_block_set_first_rec_group(log_sys->buf, LOG_BLOCK_HDR_SIZE);

    log_sys->buf_free = LOG_BLOCK_HDR_SIZE;
    log_sys->lsn = LOG_START_LSN + LOG_BLOCK_HDR_SIZE;

    mutex_exit(&(log_sys->mutex));

}


// Writes to the log the string given. The log must be released with log_release.
// return: end lsn of the log record, zero if did not succeed
uint64 log_reserve_and_write_fast(const void *str, /*!< in: string */
    uint32 len, /*!< in: string length */
    uint64 *start_lsn) /*!< out: start lsn of the log record */
{
    uint32 data_len;

    mutex_enter(&log_sys->mutex);

    data_len = len + log_sys->buf_free % OS_FILE_LOG_BLOCK_SIZE;
    if (data_len >= OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
        /* The string does not fit within the current log block or the log block would become full */
        mutex_exit(&log_sys->mutex);
        return(0);
    }

    *start_lsn = log_sys->lsn;
    memcpy(log_sys->buf + log_sys->buf_free, str, len);

    log_block_set_data_len((byte*)ut_align_down(log_sys->buf + log_sys->buf_free, OS_FILE_LOG_BLOCK_SIZE),
                           data_len);
    log_sys->buf_free += len;
    ut_ad(log_sys->buf_free <= log_sys->buf_size);
    log_sys->lsn += len;

    return log_sys->lsn;
}



// Opens the log for log_write_low. The log must be closed with log_close and released with log_release.
// return: start lsn of the log record */
uint64 log_reserve_and_open(uint32 len) /*!< in: length of data to be catenated */
{
    log_t*  log = log_sys;
    uint32  len_upper_limit;
#ifdef UNIV_LOG_ARCHIVE
    uint32   archived_lsn_age;
    uint32   dummy;
#endif /* UNIV_LOG_ARCHIVE */

    ut_a(len < log->buf_size / 2);

loop:
    mutex_enter(&(log->mutex));
    ut_ad(!recv_no_log_write);

    /* Calculate an upper limit for the space the string may take in the log buffer */
    len_upper_limit = LOG_BUF_WRITE_MARGIN + (5 * len) / 4;
    if (log->buf_free + len_upper_limit > log->buf_size) {
        mutex_exit(&(log->mutex));

        /* Not enough free space, do a syncronous flush of the log buffer */
        log_buffer_flush_to_disk();
        srv_stats.log_waits.inc();

        goto loop;
    }

#ifdef UNIV_LOG_ARCHIVE
    if (log->archiving_state != LOG_ARCH_OFF) {

        archived_lsn_age = log->lsn - log->archived_lsn;
        if (archived_lsn_age + len_upper_limit > log->max_archived_lsn_age) {
            /* Not enough free archived space in log groups: do a
            synchronous archive write batch: */

            mutex_exit(&(log->mutex));

            ut_ad(len_upper_limit <= log->max_archived_lsn_age);

            log_archive_do(TRUE, &dummy);

            ut_ad(++count < 50);

            goto loop;
        }
    }
#endif /* UNIV_LOG_ARCHIVE */

    return log->lsn;
}

// Releases the log mutex
void log_release(void)
{
    mutex_exit(&(log_sys->mutex));
}

// Writes to the log the string given.
// It is assumed that the caller holds the log mutex.
void log_write_low(byte *str, uint32 str_len)
{
    log_t*   log = log_sys;
    uint32   len;
    uint32   data_len;
    byte*    log_block;

    ut_ad(mutex_own(&(log->mutex)));

part_loop:
    ut_ad(!recv_no_log_write);
    /* Calculate a part length */
    data_len = (log->buf_free % OS_FILE_LOG_BLOCK_SIZE) + str_len;
    if (data_len <= OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
        /* The string fits within the current log block */
        len = str_len;
    } else {
        data_len = OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE;
        len = OS_FILE_LOG_BLOCK_SIZE - (log->buf_free % OS_FILE_LOG_BLOCK_SIZE) - LOG_BLOCK_TRL_SIZE;
    }

    memcpy(log->buf + log->buf_free, str, len);
    str_len -= len;
    str = str + len;
    log_block = (byte*)ut_align_down(log->buf + log->buf_free, OS_FILE_LOG_BLOCK_SIZE);
    log_block_set_data_len(log_block, data_len);

    if (data_len == OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
        /* This block became full */
        log_block_set_data_len(log_block, OS_FILE_LOG_BLOCK_SIZE);
        //log_block_set_checkpoint_no(log_block, log_sys->next_checkpoint_no);
        len += LOG_BLOCK_HDR_SIZE + LOG_BLOCK_TRL_SIZE;
        log->lsn += len;
        /* Initialize the next block header */
        log_block_init(log_block + OS_FILE_LOG_BLOCK_SIZE, log->lsn);
    } else {
        log->lsn += len;
    }

    log->buf_free += len;
    ut_ad(log->buf_free <= log->buf_size);

    if (str_len > 0) {
        goto part_loop;
    }

    srv_stats.log_write_requests.inc();
}

// Closes the log
uint64 log_close(void)
{
    byte*       log_block;
    uint32      first_rec_group;
    lsn_t       oldest_lsn;
    lsn_t       lsn;
    log_t*      log = log_sys;
    lsn_t       checkpoint_age;

    ut_ad(mutex_own(&(log->mutex)));
    ut_ad(!recv_no_log_write);

    lsn = log->lsn;

    log_block = (byte*)ut_align_down(log->buf + log->buf_free, OS_FILE_LOG_BLOCK_SIZE);
    first_rec_group = log_block_get_first_rec_group(log_block);

    if (first_rec_group == 0) {
        /* We initialized a new log block which was not written
        full by the current mtr: the next mtr log record group
        will start within this block at the offset data_len */
        log_block_set_first_rec_group(log_block, log_block_get_data_len(log_block));
    }

    if (log->buf_free > log->max_buf_free) {
        log->check_flush_or_checkpoint = TRUE;
    }

    checkpoint_age = lsn - log->last_checkpoint_lsn;
    if (checkpoint_age >= log->log_group_capacity) {
        /* TODO: split btr_store_big_rec_extern_fields() into small
        steps so that we can release all latches in the middle, and
        call log_free_check() to ensure we never write over log written
        after the latest checkpoint. In principle, we should split all
        big_rec operations, but other operations are smaller. */

        if (!log_has_printed_chkp_warning
            || difftime(time(NULL), log_last_warning_time) > 15) {

            log_has_printed_chkp_warning = TRUE;
            log_last_warning_time = time(NULL);

            ut_print_timestamp(stderr);
            fprintf(stderr,
                " InnoDB: ERROR: the age of the last"
                " checkpoint is " LSN_PF ",\n"
                "InnoDB: which exceeds the log group"
                " capacity " LSN_PF ".\n"
                "InnoDB: If you are using big"
                " BLOB or TEXT rows, you must set the\n"
                "InnoDB: combined size of log files"
                " at least 10 times bigger than the\n"
                "InnoDB: largest such row.\n",
                checkpoint_age,
                log->log_group_capacity);
        }
    }

    if (checkpoint_age <= log->max_modified_age_sync) {
        goto function_exit;
    }

    oldest_lsn = buf_pool_get_oldest_modification();

    if (!oldest_lsn
        || lsn - oldest_lsn > log->max_modified_age_sync
        || checkpoint_age > log->max_checkpoint_age_async) {
        log->check_flush_or_checkpoint = TRUE;
    }

function_exit:

    return lsn;
}


