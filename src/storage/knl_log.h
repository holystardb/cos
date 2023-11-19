#ifndef _KNL_LOG_H_
#define _KNL_LOG_H_

#include "cm_type.h"
#include "cm_list.h"
#include "cm_memory.h"
#include "cm_rwlock.h"

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/* Type used for all log sequence number storage and arithmetics */
typedef uint64          lsn_t;
#define LSN_MAX         uint64
#define LSN_PF          UINT64PF


#define OS_FILE_LOG_BLOCK_SIZE    512

/* The counting of lsn's starts from this value: this must be non-zero */
#define LOG_START_LSN		((lsn_t) (16 * OS_FILE_LOG_BLOCK_SIZE))

#define LOG_BUFFER_SIZE		(srv_log_buffer_size * UNIV_PAGE_SIZE)
#define LOG_ARCHIVE_BUF_SIZE	(srv_log_buffer_size * UNIV_PAGE_SIZE / 4)



/* Offsets of a log block header */
#define LOG_BLOCK_HDR_NO          0  /* block number which must be > 0 */
#define LOG_BLOCK_HDR_DATA_LEN    8  /* number of bytes of log written to this block */
#define LOG_BLOCK_FIRST_REC_GROUP 10 /* offset of the first start of an mtr log record group in this log block */
#define LOG_BLOCK_HDR_SIZE        12 /* size of the log block header in bytes */

#define LOG_BLOCK_FLUSH_BIT_MASK  0x80000000UL  /* mask used to get the highest bit in the preceding field */

/* Offsets of a log block trailer from the end of the block */
#define LOG_BLOCK_CHECKSUM        4  /* 4 byte checksum of the log block contents */
#define LOG_BLOCK_TRL_SIZE        4  /* trailer size in bytes */


/* Offsets of a log file header */
#define LOG_GROUP_ID                0  /* log group number */
#define LOG_FILE_START_LSN          4  /* lsn of the start of data in this log file */
#define LOG_FILE_NO                 12 /* 4-byte archived log file number;
					this field is only defined in an archived log file */
#define LOG_FILE_WAS_CREATED_BY_HOT_BACKUP 16
					/* a 32-byte field which contains
					the string 'ibbackup' and the
					creation time if the log file was
					created by ibbackup --restore;
					when mysqld is first time started
					on the restored database, it can
					print helpful info for the user */
#define	LOG_FILE_ARCH_COMPLETED     OS_FILE_LOG_BLOCK_SIZE
					/* this 4-byte field is TRUE when
					the writing of an archived log file
					has been completed; this field is
					only defined in an archived log file */
#define LOG_FILE_END_LSN            (OS_FILE_LOG_BLOCK_SIZE + 4)
					/* lsn where the archived log file
					at least extends: actually the
					archived log file may extend to a
					later lsn, as long as it is within the
					same log block as this lsn; this field
					is defined only when an archived log
					file has been completely written */
#define LOG_CHECKPOINT_1            OS_FILE_LOG_BLOCK_SIZE
					/* first checkpoint field in the log
					header; we write alternately to the
					checkpoint fields when we make new
					checkpoints; this field is only defined
					in the first log file of a log group */
#define LOG_CHECKPOINT_2            (3 * OS_FILE_LOG_BLOCK_SIZE)
					/* second checkpoint field in the log header */
#define LOG_FILE_HDR_SIZE           (4 * OS_FILE_LOG_BLOCK_SIZE)

#define LOG_GROUP_OK                301
#define LOG_GROUP_CORRUPTED         302



/** Log group consists of a number of log files, each of the same size;
    a log group is implemented as a space in the sense of the module fil0fil. */
typedef struct st_log_group {
	/* The following fields are protected by log_sys->mutex */
	uint32		id;		/*!< log group id */
	uint32		n_files;	/*!< number of files in the group */
	uint64		file_size;	/*!< individual log file size in bytes, including the log file header */
	uint32		space_id;	/*!< file space which implements the log group */
	uint32		state;		/*!< LOG_GROUP_OK or LOG_GROUP_CORRUPTED */
    uint64		lsn;		/*!< lsn used to fix coordinates within the log group */
    uint64		lsn_offset;	/*!< the offset of the above lsn */
	uint32		n_pending_writes;/*!< number of currently pending flush writes for this log group */
	byte**		file_header_bufs_ptr;/*!< unaligned buffers */
	byte**		file_header_bufs;/*!< buffers for each file header in the group */
#ifdef UNIV_LOG_ARCHIVE
	/*-----------------------------*/
	byte**		archive_file_header_bufs_ptr;/*!< unaligned buffers */
	byte**		archive_file_header_bufs;/*!< buffers for each file header in the group */
	uint32		archive_space_id;/*!< file space which implements the log group archive */
	uint32		archived_file_no;/*!< file number corresponding to log_sys->archived_lsn */
	uint32		archived_offset;/*!< file offset corresponding to log_sys->archived_lsn,
                             0 if we have not yet written to the archive file number archived_file_no */
	uint32		next_archived_file_no;/*!< during an archive write,
					until the write is completed, we store the next value for archived_file_no here:
                     the write completion function then sets the new value to ..._file_no */
	uint32		next_archived_offset; /*!< like the preceding field */
#endif /* UNIV_LOG_ARCHIVE */
	/*-----------------------------*/
    uint64		scanned_lsn;	/*!< used only in recovery: recovery scan succeeded up to this lsn in this log group */
	byte*		checkpoint_buf_ptr;/*!< unaligned checkpoint header */
	byte*		checkpoint_buf;	/*!< checkpoint header is written from this buffer to the group */
	UT_LIST_NODE_T(struct st_log_group) log_groups;	/*!< list of log groups */
} log_group_t;


/** Redo log buffer */
typedef struct st_log {
	byte		pad[64]; /*!< padding to prevent other memory
					update hotspots from residing on the same memory cache line */
    uint64		lsn; /*!< log sequence number */
	uint32		buf_free; /*!< first free offset within the log buffer */
#ifndef UNIV_HOTBACKUP
	mutex_t		mutex; /*!< mutex protecting the log */

	mutex_t		log_flush_order_mutex;/*!< mutex to serialize access to
					the flush list when we are putting dirty blocks in the list.
 The idea behind this mutex is to be able to release log_sys->mutex
 during mtr_commit and still ensure that insertions in the flush_list happen in the LSN order. */
#endif /* !UNIV_HOTBACKUP */
	byte*		buf_ptr; /* unaligned log buffer */
	byte*		buf; /*!< log buffer */
	uint32		buf_size; /*!< log buffer size in bytes */
	uint32		max_buf_free; /*!< recommended maximum value of buf_free, after which the buffer is flushed */
 #ifdef UNIV_LOG_DEBUG
	uint32		old_buf_free;	/*!< value of buf free when log was last time opened; only in the debug version */
	uint64      old_lsn;	/*!< value of lsn when log was last time opened; only in the debug version */
#endif /* UNIV_LOG_DEBUG */
	bool32		check_flush_or_checkpoint;
					/*!< this is set to TRUE when there may
					be need to flush the log buffer, or
					preflush buffer pool pages, or make
					a checkpoint; this MUST be TRUE when
					lsn - last_checkpoint_lsn >
					max_checkpoint_age; this flag is
					peeked at by log_free_check(), which
					does not reserve the log mutex */
	UT_LIST_BASE_NODE_T(log_group_t) log_groups;	/*!< log groups */

#ifndef UNIV_HOTBACKUP
	/** The fields involved in the log buffer flush @{ */

	uint32		buf_next_to_write;/*!< first offset in the log buffer
					where the byte content may not exist
					written to file, e.g., the start
					offset of a log record catenated
					later; this is advanced when a flush
					operation is completed to all the log
					groups */
    uint64		written_to_some_lsn;
					/*!< first log sequence number not yet
					written to any log group; for this to
					be advanced, it is enough that the
					write i/o has been completed for any
					one log group */
    uint64		written_to_all_lsn;
					/*!< first log sequence number not yet
					written to some log group; for this to
					be advanced, it is enough that the
					write i/o has been completed for all
					log groups.
					Note that since InnoDB currently
					has only one log group therefore
					this value is redundant. Also it
					is possible that this value
					falls behind the
					flushed_to_disk_lsn transiently.
					It is appropriate to use either
					flushed_to_disk_lsn or
					write_lsn which are always
					up-to-date and accurate. */
    uint64		write_lsn;	/*!< end lsn for the current running
					write */
	uint32		write_end_offset;/*!< the data in buffer has
					been written up to this offset
					when the current write ends:
					this field will then be copied
					to buf_next_to_write */
    uint64		current_flush_lsn;/*!< end lsn for the current running
					write + flush operation */
    uint64		flushed_to_disk_lsn;
					/*!< how far we have written the log
					AND flushed to disk */
	uint32		n_pending_writes;/*!< number of currently
					pending flushes or writes */
	/* NOTE on the 'flush' in names of the fields below: starting from
	4.0.14, we separate the write of the log file and the actual fsync()
	or other method to flush it to disk. The names below shhould really
	be 'flush_or_write'! */
	os_event_t	no_flush_event;	/*!< this event is in the reset state
					when a flush or a write is running;
					a thread should wait for this without
					owning the log mutex, but NOTE that
					to set or reset this event, the
					thread MUST own the log mutex! */
	bool32		one_flushed;	/*!< during a flush, this is
					first FALSE and becomes TRUE
					when one log group has been
					written or flushed */
	os_event_t	one_flushed_event;/*!< this event is reset when the
					flush or write has not yet completed
					for any log group; e.g., this means
					that a transaction has been committed
					when this is set; a thread should wait
					for this without owning the log mutex,
					but NOTE that to set or reset this
					event, the thread MUST own the log
					mutex! */
	uint32		n_log_ios;	/*!< number of log i/os initiated thus far */
	uint32		n_log_ios_old;	/*!< number of log i/o's at the previous printout */
	time_t		last_printout_time;/*!< when log_print was last time called */

	/** Fields involved in checkpoints @{ */
    uint64		log_group_capacity; /*!< capacity of the log group; if
					the checkpoint age exceeds this, it is
					a serious error because it is possible
					we will then overwrite log and spoil
					crash recovery */
    uint64		max_modified_age_async;
					/*!< when this recommended value for lsn - buf_pool_get_oldest_modification()
					is exceeded, we start an asynchronous preflush of pool pages */
    uint64		max_modified_age_sync;
					/*!< when this recommended value for lsn - buf_pool_get_oldest_modification()
					is exceeded, we start a synchronous preflush of pool pages */
    uint64		max_checkpoint_age_async;
					/*!< when this checkpoint age is exceeded we start an asynchronous writing of a new checkpoint */
    uint64		max_checkpoint_age;
					/*!< this is the maximum allowed value for lsn - last_checkpoint_lsn when a new query step is started */
	uint64	next_checkpoint_no;
					/*!< next checkpoint number */
    uint64		last_checkpoint_lsn;
					/*!< latest checkpoint lsn */
    uint64		next_checkpoint_lsn;
					/*!< next checkpoint lsn */
	uint32		n_pending_checkpoint_writes;
					/*!< number of currently pending checkpoint writes */
	rw_lock_t	checkpoint_lock;/*!< this latch is x-locked when a checkpoint write is running; a thread should wait for this without owning the log mutex */
#endif /* !UNIV_HOTBACKUP */
	byte*		checkpoint_buf_ptr;/* unaligned checkpoint header */
	byte*		checkpoint_buf;	/*!< checkpoint header is read to this buffer */

#ifdef UNIV_LOG_ARCHIVE
	/** Fields involved in archiving @{ */
	uint32		archiving_state;/*!< LOG_ARCH_ON, LOG_ARCH_STOPPING LOG_ARCH_STOPPED, LOG_ARCH_OFF */
    uint64		archived_lsn;	/*!< archiving has advanced to this lsn */
    uint64		max_archived_lsn_age_async;
					/*!< recommended maximum age of archived_lsn, before we start asynchronous copying to the archive */
    uint64		max_archived_lsn_age;
					/*!< maximum allowed age for archived_lsn */
    uint64		next_archived_lsn;/*!< during an archive write,
					until the write is completed, we store the next value for archived_lsn here:
 the write completion function then sets the new value to archived_lsn */
	uint32		archiving_phase;/*!< LOG_ARCHIVE_READ or LOG_ARCHIVE_WRITE */
	uint32		n_pending_archive_ios;
					/*!< number of currently pending reads or writes in archiving */
	rw_lock_t	archive_lock;	/*!< this latch is x-locked when an archive write is running;
 a thread should wait for this without owning the log mutex */
	uint32		archive_buf_size;/*!< size of archive_buf */
	byte*		archive_buf;	/*!< log segment is written to the archive from this buffer */
	os_event_t	archiving_on;	/*!< if archiving has been stopped,
					a thread can wait for this event to become signaled */
#endif /* UNIV_LOG_ARCHIVE */
} log_t;


/** Test if flush order mutex is owned. */
#define log_flush_order_mutex_own() \
    mutex_own(&log_sys->log_flush_order_mutex)

/** Acquire the flush order mutex. */
#define log_flush_order_mutex_enter() do {              \
        mutex_enter(&log_sys->log_flush_order_mutex);   \
    } while (0)

/** Release the flush order mutex. */
#define log_flush_order_mutex_exit() do {           \
        mutex_exit(&log_sys->log_flush_order_mutex);\
    } while (0)




/*-----------------------------------------------------------------------*/

extern void log_init(void);

extern uint64 log_reserve_and_write_fast(const void *str, /*!< in: string */
    uint32 len, /*!< in: string length */
    uint64 *start_lsn); /*!< out: start lsn of the log record */

extern uint64 log_reserve_and_open(uint32 len);
extern void log_release(void);
extern void log_write_low(byte *str, uint32 str_len);
extern uint64 log_close(void);


/*-----------------------------------------------------------------------*/

extern log_t*    log_sys;

#ifdef __cplusplus
}
#endif // __cplusplus

#endif // _KNL_LOG_H_
