#ifndef _KNL_RECOVERY_H
#define _KNL_RECOVERY_H

#include "cm_type.h"

#include "knl_mtr.h"





// Recovery system data structure
typedef struct st_recovery_sys recovery_sys_t;
struct st_recovery_sys{
	mutex_t		mutex;	/* mutex protecting the fields apply_log_recs,
				n_addrs, and the state field in each recv_addr
				struct */
	bool32		apply_log_recs;
				/* this is TRUE when log rec application to
				pages is allowed; this flag tells the
				i/o-handler if it should do log record
				application */
	bool32		apply_batch_on;
				/* this is TRUE when a log rec application
				batch is running */
	duint64		lsn;	/* log sequence number */
	uint32		last_log_buf_size;
				/* size of the log buffer when the database
				last time wrote to the log */
	byte*		last_block;
				/* possible incomplete last recovered log
				block */
	byte*		last_block_buf_start;
				/* the nonaligned start address of the
				preceding buffer */
	byte*		buf;	/* buffer for parsing log records */
	uint32		len;	/* amount of data in buf */
    lsn_t		parse_start_lsn;
				/* this is the lsn from which we were able to
				start parsing log records and adding them to
				the hash table; ut_dulint_zero if a suitable
				start point not found yet */
	lsn_t		scanned_lsn;
				/* the log data has been scanned up to this
				lsn */
	uint32		scanned_checkpoint_no;
				/* the log data has been scanned up to this
				checkpoint number (lowest 4 bytes) */
	uint32		recovered_offset;
				/* start offset of non-parsed log records in
				buf */
    lsn_t		recovered_lsn;
				/* the log records have been parsed up to
				this lsn */
    lsn_t		limit_lsn;/* recovery should be made at most up to this
				lsn */
	log_group_t*	archive_group;
				/* in archive recovery: the log group whose
				archive is read */
	memory_pool_t*	mem_pool;	/* memory heap of log records and file addresses*/
	//hash_table_t*	addr_hash;/* hash table of file addresses of pages */
	uint32		n_addrs;/* number of not processed hashed file
				addresses in the hash table */
};

extern recovery_sys_t*      recovery_sys;










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




extern status_t recovery_from_checkpoint_start(
    uint32   type,       // in: LOG_CHECKPOINT or LOG_ARCHIVE
    duint64  limit_lsn,  // in: recover up to this lsn if possible
    duint64  min_flushed_lsn,  // in: min flushed lsn from data files
    duint64  max_flushed_lsn); // in: max flushed lsn from data files

extern status_t recovery_from_checkpoint_finish();

#endif  /* _KNL_RECOVERY_H */
