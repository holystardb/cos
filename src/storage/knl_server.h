#ifndef _KNL_SERVER_H
#define _KNL_SERVER_H

#include "cm_type.h"
#include "cm_error.h"
#include "cm_counter.h"



const unsigned int UNIV_PAGE_SIZE = 16384; /* 16KB */

typedef uint32              space_id_t;
typedef uint32              page_no_t;

typedef uint64              ib_uint64_t;
typedef uint64              ib_id_t;
typedef uint64              trx_id_t;

typedef byte                page_t;
typedef uint64              lsn_t;
typedef uint16              page_type_t;

typedef byte                buf_frame_t;


extern char *srv_data_home;

/*------------------------- UNDO ------------------------ */


extern char *srv_undo_dir;

/*------------------------- REDO ------------------------ */

extern char *srv_log_group_home_dir;
extern uint32 srv_n_log_files;

/*------------------------- BUF POOL -------------------- */

extern uint64 srv_buf_pool_size;
extern uint64 srv_buf_pool_min_size;
extern uint32 srv_buf_pool_instances;
extern uint32 srv_buf_pool_chunk_unit;
extern bool32 buf_pool_should_madvise;


/*------------------------- LOG ------------------------- */

extern uint64 srv_log_file_size;
extern uint32 srv_log_buffer_size;



/* The number of purge threads to use.*/
extern uint32 srv_n_purge_threads;



/** Types of threads existing in the system. */
enum srv_thread_type {
	SRV_NONE,			/*!< None */
	SRV_WORKER,			/*!< threads serving parallelized
					queries and queries released from
					lock wait */
	SRV_PURGE,			/*!< Purge coordinator thread */
	SRV_MASTER			/*!< the master thread, (whose type
					number must be biggest) */
};

/** Thread slot in the thread table.  */
struct srv_slot_t{
	srv_thread_type type;			/*!< thread type: user,
						utility etc. */
	bool32		in_use;			/*!< TRUE if this slot
						is in use */
	bool32		suspended;		/*!< TRUE if the thread is
						waiting for the event of this
						slot */
	time_t	suspend_time;		/*!< time when the thread was
						suspended. Initialized by
						lock_wait_table_reserve_slot()
						for lock wait */
	ulong		wait_timeout;		/*!< wait time that if exceeded
						the thread will be timed out.
						Initialized by
						lock_wait_table_reserve_slot()
						for lock wait */
	//os_event_t	event;  /*!< event used in suspending the thread when it has nothing to do */
	//que_thr_t*	thr;			/*!< suspended query thread(only used for user threads) */
};

/* Global counters */
typedef struct st_srv_stats {
    typedef counter_t<uint64, 64> uint64_ctr_64_t;
    typedef counter_t<uint64, 1> uint64_ctr_1_t;
    typedef counter_t<lsn_t, 1> lsn_ctr_1_t;

    /** Count the amount of data written in total (in bytes) */
    uint64_ctr_1_t      data_written;

    /** Number of the log write requests done */
    uint64_ctr_1_t      log_write_requests;

    /** Number of physical writes to the log performed */
    uint64_ctr_1_t      log_writes;

    /** Amount of data padded for log write ahead */
    uint64_ctr_1_t      log_padded;

    /** Amount of data written to the log files in bytes */
    lsn_ctr_1_t         os_log_written;

    /** Number of writes being done to the log files */
    uint64_ctr_1_t      os_log_pending_writes;

    /** We increase this counter, when we don't have enough
    space in the log buffer and have to flush it */
    uint64_ctr_1_t      log_waits;

    /** Count the number of times the doublewrite buffer was flushed */
    uint64_ctr_1_t      dblwr_writes;

    /** Store the number of pages that have been flushed to the doublewrite buffer */
    uint64_ctr_1_t      dblwr_pages_written;

    /** Store the number of write requests issued */
    uint64_ctr_1_t      buf_pool_write_requests;

    /** Store the number of times when we had to wait for a free page in the buffer pool.
    It happens when the buffer pool is full and we need to make a flush,
    in order to be able to read or create a page. */
    uint64_ctr_1_t      buf_pool_wait_free;

    /** Count the number of pages that were written from buffer pool to the disk */
    uint64_ctr_1_t      buf_pool_flushed;

    /** Number of buffer pool reads that led to the reading of a disk page */
    uint64_ctr_1_t      buf_pool_reads;

    /** Number of data read in total (in bytes) */
    uint64_ctr_1_t      data_read;

    /** Wait time of database locks */
    uint64_ctr_1_t      n_lock_wait_time;

    /** Number of database lock waits */
    uint64_ctr_1_t      n_lock_wait_count;

    /** Number of threads currently waiting on database locks */
    uint64_ctr_1_t      n_lock_wait_current_count;

    /** Number of rows read. */
    uint64_ctr_64_t     n_rows_read;

    /** Number of rows updated */
    uint64_ctr_64_t     n_rows_updated;

    /** Number of rows deleted */
    uint64_ctr_64_t     n_rows_deleted;

    /** Number of rows inserted */
    uint64_ctr_64_t     n_rows_inserted;
} srv_stats_t;


dberr_t srv_start(bool32 create_new_db);


//------------------------------------------------------------------

extern srv_stats_t  srv_stats;


#endif  /* _KNL_SERVER_H */
