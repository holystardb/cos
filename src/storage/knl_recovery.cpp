#include "knl_recovery.h"

#include "cm_log.h"

#include "knl_file_system.h"

recovery_sys_t*      recovery_sys = NULL;



static const mlog_dispatch_t g_mlog_dispatch_table[MLOG_MAX_ID] = {
    { mlog_replay_nbytes, mlog_replay_check, MLOG_1BYTE, 0, 0 },
    { mlog_replay_nbytes, mlog_replay_check, MLOG_2BYTES, 0, 0 },
    { mlog_replay_nbytes, mlog_replay_check, MLOG_4BYTES, 0, 0 },
    { mlog_replay_nbytes, mlog_replay_check, MLOG_8BYTES, 0, 0 },
    { NULL, NULL, MLOG_BIGGEST_TYPE, 0, 0 },
};


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

// Reads a checkpoint info from a log group header to log_sys->checkpoint_buf
void log_group_read_checkpoint_info(
    log_group_t* group, // in: log group
    uint32 field)  // in: LOG_CHECKPOINT_1 or LOG_CHECKPOINT_2
{
    ut_ad(mutex_own(&(log_sys->mutex)));

    //log_sys->n_log_ios++;

    const page_id_t page_id(FIL_REDO_SPACE_ID, field / UNIV_PAGE_SIZE);
    const page_size_t page_size(0);

    fil_io(OS_FILE_READ, TRUE, page_id, page_size, field % UNIV_PAGE_SIZE,
        OS_FILE_LOG_BLOCK_SIZE, log_sys->checkpoint_buf);
}


// Looks for the maximum consistent checkpoint from the log groups.
static status_t recv_find_max_checkpoint(
    log_group_t** max_group, // out: max group
    uint32* max_field) // out: LOG_CHECKPOINT_1 or LOG_CHECKPOINT_2
{
	log_group_t*    group;
	uint64          max_no = 0;
	uint64          checkpoint_no;
	uint32          field;
	uint32          fold;
	byte*           buf;
	
	group = UT_LIST_GET_FIRST(log_sys->log_groups);

	*max_group = NULL;
	
	buf = log_sys->checkpoint_buf;
	
	while (group) {
		group->state = LOG_GROUP_CORRUPTED;
	
		for (field = LOG_CHECKPOINT_1; field <= LOG_CHECKPOINT_2;
				field += LOG_CHECKPOINT_2 - LOG_CHECKPOINT_1) {
	
			log_group_read_checkpoint_info(group, field);

			// Check the consistency of the checkpoint info
			fold = ut_fold_binary(buf, LOG_CHECKPOINT_CHECKSUM_1);
			if ((fold & 0xFFFFFFFF) != mach_read_from_4(buf + LOG_CHECKPOINT_CHECKSUM_1)) {
				LOGGER_ERROR(LOGGER, 
                    "Checkpoint in group %lu at %lu invalid, %lu, %lu\n",
					group->id, field, fold & 0xFFFFFFFF,
                    mach_read_from_4(buf + LOG_CHECKPOINT_CHECKSUM_1));
				goto not_consistent;
			}

			fold = ut_fold_binary(buf + LOG_CHECKPOINT_LSN,
				LOG_CHECKPOINT_CHECKSUM_2 - LOG_CHECKPOINT_LSN);
			if ((fold & 0xFFFFFFFF) != mach_read_from_4(buf + LOG_CHECKPOINT_CHECKSUM_2)) {
				LOGGER_ERROR(LOGGER, 
	                "Checkpoint in group %lu at %lu invalid, %lu, %lu\n",
					group->id, field, fold & 0xFFFFFFFF,
                    mach_read_from_4(buf + LOG_CHECKPOINT_CHECKSUM_2));
				goto not_consistent;
			}

			group->state = LOG_GROUP_OK;
			group->lsn = mach_read_from_8(buf + LOG_CHECKPOINT_LSN);
			group->lsn_offset = mach_read_from_4(buf + LOG_CHECKPOINT_OFFSET_LOW32);
			group->lsn_offset |= ((lsn_t) mach_read_from_4(buf + LOG_CHECKPOINT_OFFSET_HIGH32)) << 32;
			checkpoint_no = mach_read_from_8(buf + LOG_CHECKPOINT_NO);

			LOGGER_INFO(LOGGER, 
		        "Checkpoint number %llu found in group %lu\n",
			    checkpoint_no, group->id);

			if (checkpoint_no >= max_no) {
				*max_group = group;
				*max_field = field;
				max_no = checkpoint_no;
			}

		not_consistent:
			;
		}

		group = UT_LIST_GET_NEXT(list_node, group);
	}

	if (*max_group == NULL) {
		LOGGER_ERROR(LOGGER, "No valid checkpoint found");
		return CM_ERROR;
	}

	return CM_SUCCESS;
}


// Reads a specified log segment to a buffer
static void log_group_read_log_seg(
    uint32       type,		/*!< in: LOG_ARCHIVE or LOG_RECOVER */
    byte*        buf,		/*!< in: buffer where to read */
    log_group_t* group,		/*!< in: log group */
    lsn_t        start_lsn,	/*!< in: read area start */
    lsn_t        end_lsn)	/*!< in: read area end */
{
    uint32  len;
    lsn_t   source_offset = 0;
    bool32  sync = (type == LOG_RECOVER);

    ut_ad(mutex_own(&(log_sys->mutex)));

loop:

    ut_a(end_lsn - start_lsn <= UINT_MAX32);
    len = (uint32) (end_lsn - start_lsn);
    ut_ad(len != 0);

    //source_offset = log_group_calc_lsn_offset(start_lsn, group);
    if ((source_offset % group->file_size) + len > group->file_size) {
        /* If the above condition is true then len (which is ulint)
        is > the expression below, so the typecast is ok */
        len = (uint32) (group->file_size - (source_offset % group->file_size));
    }
    ut_a(source_offset / UNIV_PAGE_SIZE <= UINT_MAX32);

    //log_sys->n_log_ios++;

    const page_id_t page_id(FIL_REDO_SPACE_ID, source_offset / UNIV_PAGE_SIZE);
    const page_size_t page_size(0);
    fil_io(OS_FILE_READ, sync, page_id, page_size,
           (uint32) (source_offset % UNIV_PAGE_SIZE), len, buf);

    start_lsn += len;
    buf += len;

    if (start_lsn != end_lsn) {
        goto loop;
    }
}


// Scans log from a buffer and stores new log data to the parsing buffer.
// Parses and hashes the log records if new data found.
// Unless UNIV_HOTBACKUP is defined, this function will apply log records
// automatically when the hash table becomes full.
// return TRUE if limit_lsn has been reached,
// or not able to scan any more in this log group
static bool32 recv_scan_log_recs(
	uint32		available_memory,/*!< in: we let the hash table of recs
					to grow to this size, at the maximum */
	bool32		store_to_hash,	/*!< in: TRUE if the records should be
					stored to the hash table; this is set
					to FALSE if just debug checking is
					needed */
	const byte*	buf,		/*!< in: buffer containing a log
					segment or garbage */
	uint32		len,		/*!< in: buffer length */
	lsn_t		start_lsn,	/*!< in: buffer start lsn */
	lsn_t*		contiguous_lsn,	/*!< in/out: it is known that all log
					groups contain contiguous log data up
					to this lsn */
	lsn_t*		group_scanned_lsn)/*!< out: scanning succeeded up to
					this lsn */
{
	const byte*	log_block;
	uint32		no;
	lsn_t		scanned_lsn;
	bool32		finished;
	uint32		data_len;
	bool32		more_data;

#if 0

	ut_ad(start_lsn % OS_FILE_LOG_BLOCK_SIZE == 0);
	ut_ad(len % OS_FILE_LOG_BLOCK_SIZE == 0);
	ut_ad(len >= OS_FILE_LOG_BLOCK_SIZE);
	ut_a(store_to_hash <= TRUE);

	finished = FALSE;
	log_block = buf;
	scanned_lsn = start_lsn;
	more_data = FALSE;

	do {
		no = log_block_get_hdr_no(log_block);
		if (no != log_block_convert_lsn_to_no(scanned_lsn)
		    || !log_block_checksum_is_ok_or_old_format(log_block)) {

			if (no == log_block_convert_lsn_to_no(scanned_lsn)
			    && !log_block_checksum_is_ok_or_old_format(log_block)) {
				fprintf(stderr,
					"Log block no %lu at lsn %llu has ok header, but checksum field"
					" contains %lu, should be %lu\n",
					(ulong)no, scanned_lsn,
					(ulong) log_block_get_checksum(log_block),
					(ulong) log_block_calc_checksum(log_block));
			}

			// Garbage or an incompletely written log block
			finished = TRUE;
			break;
		}

		if (log_block_get_flush_bit(log_block)) {
			/* This block was a start of a log flush operation:
			we know that the previous flush operation must have
			been completed for all log groups before this block
			can have been flushed to any of the groups. Therefore,
			we know that log data is contiguous up to scanned_lsn
			in all non-corrupt log groups. */
			if (scanned_lsn > *contiguous_lsn) {
				*contiguous_lsn = scanned_lsn;
			}
		}

		data_len = log_block_get_data_len(log_block);
		if ((store_to_hash || (data_len == OS_FILE_LOG_BLOCK_SIZE))
		    && scanned_lsn + data_len > recv_sys->scanned_lsn
		    && (recv_sys->scanned_checkpoint_no > 0)
		    && (log_block_get_checkpoint_no(log_block)
			< recv_sys->scanned_checkpoint_no)
		    && (recv_sys->scanned_checkpoint_no
			- log_block_get_checkpoint_no(log_block)
			> 0x80000000UL)) {
			/* Garbage from a log buffer flush which was made
			before the most recent database recovery */
			finished = TRUE;
			break;
		}

		if (!recv_sys->parse_start_lsn && (log_block_get_first_rec_group(log_block) > 0)) {
			// We found a point from which to start the parsing of log records
			recv_sys->parse_start_lsn = scanned_lsn + log_block_get_first_rec_group(log_block);
			recv_sys->scanned_lsn = recv_sys->parse_start_lsn;
			recv_sys->recovered_lsn = recv_sys->parse_start_lsn;
		}

		scanned_lsn += data_len;
		if (scanned_lsn > recv_sys->scanned_lsn) {
			/* We have found more entries. If this scan is
 			of startup type, we must initiate crash recovery
			environment before parsing these log records. */

#ifndef UNIV_HOTBACKUP
			if (recv_log_scan_is_startup_type && !recv_needed_recovery) {
				if (!srv_read_only_mode) {
					ib_logf(IB_LOG_LEVEL_INFO,
						"Log scan progressed past the checkpoint lsn %llu",
						recv_sys->scanned_lsn);
					recv_init_crash_recovery();
				} else {
					ib_logf(IB_LOG_LEVEL_WARN, "Recovery skipped, --innodb-read-only set!");
					return(TRUE);
				}
			}
#endif /* !UNIV_HOTBACKUP */

			/* We were able to find more log data: add it to the
			parsing buffer if parse_start_lsn is already non-zero */
			if (recv_sys->len + 4 * OS_FILE_LOG_BLOCK_SIZE >= RECV_PARSING_BUF_SIZE) {
				fprintf(stderr,
					"InnoDB: Error: log parsing buffer overflow. Recovery may have failed!\n");
				recv_sys->found_corrupt_log = TRUE;
				ut_error;
			} else if (!recv_sys->found_corrupt_log) {
				more_data = recv_sys_add_to_parsing_buf(
					log_block, scanned_lsn);
			}

			recv_sys->scanned_lsn = scanned_lsn;
			recv_sys->scanned_checkpoint_no = log_block_get_checkpoint_no(log_block);
		}

		if (data_len < OS_FILE_LOG_BLOCK_SIZE) {
			/* Log data for this group ends here */
			finished = TRUE;
			break;
		} else {
			log_block += OS_FILE_LOG_BLOCK_SIZE;
		}
	} while (log_block < buf + len && !finished);

	*group_scanned_lsn = scanned_lsn;

	if (recv_needed_recovery || (recv_is_from_backup && !recv_is_making_a_backup)) {
		recv_scan_print_counter++;
		if (finished || (recv_scan_print_counter % 80 == 0)) {
			fprintf(stderr,
				"Doing recovery: scanned up to log sequence number %llu \n",
				*group_scanned_lsn);
		}
	}

	if (more_data && !recv_sys->found_corrupt_log) {
		// Try to parse more log records
		recv_parse_log_recs(store_to_hash);

#ifndef UNIV_HOTBACKUP
		if (store_to_hash && mem_heap_get_size(recv_sys->heap) > available_memory) {
			/* Hash table of log records has grown too big:
			empty it; FALSE means no ibuf operations
			allowed, as we cannot add new records to the
			log yet: they would be produced by ibuf
			operations */
			recv_apply_hashed_log_recs(FALSE);
		}
#endif /* !UNIV_HOTBACKUP */

		if (recv_sys->recovered_offset > RECV_PARSING_BUF_SIZE / 4) {
			// Move parsing buffer data to the buffer start
			recv_sys_justify_left_parsing_buf();
		}
	}

#endif

    return(finished);
}


// Scans log from a buffer and stores new log data to the parsing buffer.
// Parses and hashes the log records if new data found.
static void recv_group_scan_log_recs(
    log_group_t* group, // in: log group
    lsn_t*       contiguous_lsn, // in/out: it is known that all log groups contain contiguous log data up to this lsn
    lsn_t*       group_scanned_lsn) // out: scanning succeeded up to this lsn
{
    bool32 finished = FALSE;
    lsn_t  start_lsn = *contiguous_lsn;
    lsn_t  end_lsn;

    while (!finished) {
        end_lsn = start_lsn + 4 * UNIV_PAGE_SIZE;

        log_group_read_log_seg(LOG_RECOVER, log_sys->buf, group, start_lsn, end_lsn);

        //finished = recv_scan_log_recs(TRUE, log_sys->buf,
        //    4 * UNIV_PAGE_SIZE, start_lsn, contiguous_lsn, group_scanned_lsn);
        start_lsn = end_lsn;
    }

    LOGGER_INFO(LOGGER,
        "Scanned group %lu up to log sequence number %llu\n",
        group->id, *group_scanned_lsn);
}



// Recovers from a checkpoint.
// When this function returns, the database is able
// to start processing of new user transactions, but the function
// recv_recovery_from_checkpoint_finish should be called later to complete
// the recovery and free the resources used in it.
status_t recovery_from_checkpoint_start(
    uint32   type,       // in: LOG_CHECKPOINT or LOG_ARCHIVE
    duint64  limit_lsn,  // in: recover up to this lsn if possible
    duint64  min_flushed_lsn,// in: min flushed lsn from data files
    duint64  max_flushed_lsn)// in: max flushed lsn from data files
{
    status_t        err;
    log_group_t*    group;
    log_group_t*    max_cp_group;
    log_group_t*    up_to_date_group;
    uint32          max_cp_field;
    byte*           buf;

    lsn_t           checkpoint_lsn;
    uint64          checkpoint_no;
    lsn_t           archived_lsn;

    lsn_t           old_scanned_lsn;
    lsn_t           group_scanned_lsn;
    lsn_t           contiguous_lsn;

    uint32          capacity;


    mutex_enter(&(log_sys->mutex));

    // 1 Look for the latest checkpoint from any of the log groups
    err = recv_find_max_checkpoint(&max_cp_group, &max_cp_field);
    if (err != CM_SUCCESS) {
        mutex_exit(&(log_sys->mutex));
        return(err);
    }

    // 2
    log_group_read_checkpoint_info(max_cp_group, max_cp_field);
    buf = log_sys->checkpoint_buf;
    checkpoint_lsn = mach_read_from_8(buf + LOG_CHECKPOINT_LSN);
    checkpoint_no = mach_read_from_8(buf + LOG_CHECKPOINT_NO);
    archived_lsn = mach_read_from_8(buf + LOG_CHECKPOINT_ARCHIVED_LSN);

    // Read the first log file header to print a note
    // if this is a recovery from a restored InnoDB Hot Backup

    const page_id_t page_id(FIL_REDO_SPACE_ID, 0);
    const page_size_t page_size(0);

    fil_io(OS_FILE_READ, TRUE, page_id, page_size, 0,
        LOG_FILE_HDR_SIZE, log_sys->checkpoint_buf);

    group = UT_LIST_GET_FIRST(log_sys->log_groups);
    while (group) {
        old_scanned_lsn = recovery_sys->scanned_lsn;
        recv_group_scan_log_recs(group, &contiguous_lsn, &group_scanned_lsn);
        group->scanned_lsn = group_scanned_lsn;
        if (old_scanned_lsn < group_scanned_lsn) {
            /* We found a more up-to-date group */
            up_to_date_group = group;
        }
        group = UT_LIST_GET_NEXT(list_node, group);
    }


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

    return CM_SUCCESS;
}

