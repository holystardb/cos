#ifndef _KNL_SERVER_H
#define _KNL_SERVER_H

#include "cm_type.h"
#include "cm_error.h"
#include "cm_counter.h"
#include "m_ctype.h"
#include "cm_file.h"
#include "cm_memory.h"
#include "cm_virtual_mem.h"


const unsigned int UNIV_PAGE_SIZE = 16384; /* 16KB */


#define INVALID_PAGE_NO     0xFFFFFFFF

#define ROW_MAX_COLUMN_COUNT     (uint32)1000
#define ROW_RECORD_MAX_SIZE      (uint32)8000






typedef uint32              space_id_t;
typedef uint32              page_no_t;

typedef uint64              ib_uint64_t;
typedef uint64              ib_id_t;

typedef byte                page_t;
typedef uint16              page_type_t;

typedef uint64              roll_ptr_t;

typedef byte                buf_frame_t;

/* Type used for all log sequence number storage and arithmetics */
typedef uint64              lsn_t;

typedef uint64              scn_t;
#define SCN_MAX             UINT_MAX64
#define INVALID_SCN         UINT_MAX64

typedef ib_id_t             table_id_t;
typedef ib_id_t             index_id_t;

typedef uint32              command_id_t;

#define FIRST_COMMAND_ID    0
#define INVALID_COMMAND_ID  (~(command_id_t)0)



/*------------------------- global config ------------------------ */




extern bool32 buf_pool_should_madvise;


/* The number of purge threads to use.*/
extern uint32 srv_purge_threads;
/* Use srv_n_io_[read|write]_threads instead. */
extern uint32 srv_n_file_io_threads;
extern uint32 srv_read_io_threads;
extern uint32 srv_write_io_threads;
extern uint32 srv_sync_io_contexts;

extern uint32 srv_read_io_timeout_seconds;
extern uint32 srv_write_io_timeout_seconds;


// Move blocks to "new" LRU list only if the first access was at least this many milliseconds ago.
// Not protected by any mutex or latch.
extern uint32 srv_buf_LRU_old_threshold_ms;

extern os_aio_array_t* srv_os_aio_async_read_array;
extern os_aio_array_t* srv_os_aio_async_write_array;
extern os_aio_array_t* srv_os_aio_sync_array;


/*-------------------------------------------------- */

#define DB_CTRL_FILE_MAX_COUNT          3
#define DB_CTRL_FILE_VERSION            1

#define DB_REDO_FILE_MAX_COUNT          16
#define DB_UNDO_FILE_MAX_COUNT          16
#define DB_TEMP_FILE_MAX_COUNT          16

#define DB_SPACE_DATA_FILE_MAX_COUNT    65536

#define DB_SYSTEM_SPACE_ID              0
#define DB_SYSAUX_SPACE_ID              1
#define DB_REDO_SPACE_ID                2
#define DB_UNDO_SPACE_ID                3
#define DB_TEMP_SPACE_ID                4
#define DB_DICT_SPACE_ID                5
#define DB_DBWR_SPACE_ID                6
#define DB_SYSTEM_SPACE_MAX_COUNT       16

#define DB_USER_SPACE_FIRST_ID          16
#define DB_USER_SPACE_MAX_COUNT         256

#define DB_SPACE_INALID_ID              0xFFFFFFFF

#define DB_SYSTEM_FILNODE_ID            0
#define DB_DBWR_FILNODE_ID              1


#define DB_SYSTEM_DATA_FILE_MAX_COUNT   128
#define DB_USER_DATA_FILE_FIRST_NODE_ID 128

#define DB_DATA_FILNODE_INALID_ID       0xFFFFFFFF


#define DB_DATA_FILE_NAME_MAX_LEN       256
#define DB_OBJECT_NAME_MAX_LEN          64

#define USER_SPACE_MAX_COUNT            10240
#define FILE_NODE_COUNT_PER_USER_SPACE  1024



typedef struct st_db_data_file {
    uint32    node_id;
    uint32    space_id;
    char      name[DB_DATA_FILE_NAME_MAX_LEN];
    uint64    size;
    uint64    max_size;
    bool32    autoextend;
    uint32    status;
} db_data_file_t;

typedef struct st_db_space {
    char      name[DB_OBJECT_NAME_MAX_LEN];
    uint32    space_id;
    uint32    purpose;
} db_space_t;

typedef struct st_db_ctrl {
    uint64          version;
    uint64          ver_num;
    char            database_name[DB_OBJECT_NAME_MAX_LEN];
    char            charset_name[DB_OBJECT_NAME_MAX_LEN];
    CHARSET_INFO*   charset_info;
    uint32          redo_count;
    uint32          undo_count;
    uint32          temp_count;
    uint32          system_space_count;
    uint32          user_space_count;
    uint32          user_space_data_file_count;
    uint32          user_space_data_file_array_size;
    db_data_file_t  system;
    db_data_file_t  dbwr;
    db_data_file_t  redo_group[DB_REDO_FILE_MAX_COUNT];
    db_data_file_t  undo_group[DB_UNDO_FILE_MAX_COUNT];
    db_data_file_t  temp_group[DB_TEMP_FILE_MAX_COUNT];
    db_space_t      system_spaces[DB_SYSTEM_SPACE_MAX_COUNT];
    db_space_t      user_spaces[DB_USER_SPACE_MAX_COUNT];
    db_data_file_t* user_space_data_files;
} db_ctrl_t;

typedef struct st_db_charset_info {
    const char* name;
    CHARSET_INFO *charset_info;
} db_charset_info_t;


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
    typedef counter_t<uint64, 1> lsn_ctr_1_t;


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
    /** We increase this counter, when we don't have enough log_slot for log_buffer */
    uint64_ctr_1_t      log_slot_waits;
    /** We increase this counter, when we don't have enough space in the log file and have to checkpoint */
    uint64_ctr_1_t      log_checkpoint_waits;
    /** We increase this counter, when we have to wait for log sync to file */
    uint64_ctr_1_t      trx_sync_log_waits;
    uint64_ctr_1_t      trx_commits;
    uint64_ctr_1_t      trx_rollbacks;
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
    // Wait time for log_sys->log_flush_order_mutex lock
    spinlock_stats_t    buf_pool_insert_flush_list;
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
    uint64_ctr_1_t      dict_alloc_wait_count;

    uint64_ctr_1_t      filnode_close_wait_count;
} srv_stats_t;

//typedef struct st_thread
//{
//    uint32     id;
//
//    uint64     cid; // command id
//    uint64     query_scn;
//} thread_t;


status_t srv_start(bool32 create_new_db);


//------------------------------------------------------------------
extern bool32 srv_create_ctrl_files();
extern status_t srv_create_redo_log_files();
extern status_t srv_create_undo_log_files();
extern status_t srv_create_temp_files();
extern status_t srv_create_system_file();
extern status_t srv_create_double_write_file();
extern status_t srv_create_user_data_files();


extern bool32 db_ctrl_createdatabase(char *database_name, char *charset_name);
extern bool32 db_ctrl_add_system(char *data_file_name, uint64 size, uint64 max_size, bool32 autoextend);
extern bool32 db_ctrl_add_redo(char *data_file_name, uint64 size, uint64 max_size, bool32 autoextend);
extern bool32 db_ctrl_add_dbwr(char* data_file_name, uint64 size);
extern bool32 db_ctrl_add_undo(char *data_file_name, uint64 size, uint64 max_size, bool32 autoextend);
extern bool32 db_ctrl_add_temp(char *data_file_name, uint64 size, uint64 max_size, bool32 autoextend);
extern bool32 db_ctrl_add_user_space(char* space_name);
extern bool32 db_ctrl_add_space_file(char* space_name, char* data_file_name, uint64 size, uint64 max_size, bool32 autoextend);

extern status_t read_ctrl_file(char *name, db_ctrl_t *ctrl);

extern void* write_io_handler_thread(void *arg);
extern void* read_io_handler_thread(void *arg);

//------------------------------------------------------------------

extern db_charset_info_t srv_db_charset_info[];

extern srv_stats_t  srv_stats;

extern bool32 recv_no_log_write;

extern bool32 srv_read_only_mode;
extern bool32 srv_archive_recovery;

extern db_ctrl_t        srv_ctrl_file;
extern char             srv_data_home[1024];
extern const uint32     srv_data_home_len;

/** At a shutdown this value climbs from SRV_SHUTDOWN_NONE to
SRV_SHUTDOWN_CLEANUP and then to SRV_SHUTDOWN_LAST_PHASE, and so on */
extern shutdown_state_enum_t srv_shutdown_state;

extern memory_area_t*  srv_memory_sga;

extern vm_pool_t*      srv_temp_mem_pool;
extern memory_pool_t*  srv_common_mpool;
extern memory_pool_t*  srv_plan_mem_pool;
extern memory_pool_t*  srv_mtr_memory_pool;
extern memory_pool_t*  srv_dictionary_mem_pool;

#define MY_ALL_CHARSETS_SIZE 2048

extern CHARSET_INFO all_charsets[MY_ALL_CHARSETS_SIZE];

#endif  /* _KNL_SERVER_H */
