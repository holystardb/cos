#ifndef _KNL_LOG_H
#define _KNL_LOG_H

#include "cm_type.h"
#include "cm_list.h"
#include "cm_memory.h"
#include "cm_rwlock.h"
#include "cm_file.h"

#include "knl_server.h"


#ifdef __cplusplus
extern "C" {
#endif // __cplusplus




#define OS_FILE_LOG_BLOCK_SIZE    512

// A margin for free space in the log buffer before a log entry is catenated
#define LOG_BUF_WRITE_MARGIN      (4 * OS_FILE_LOG_BLOCK_SIZE)



/* The counting of lsn's starts from this value: this must be non-zero */
#define LOG_START_LSN           ((uint64)(16 * OS_FILE_LOG_BLOCK_SIZE))

/* Offsets of a log block header */
#define LOG_BLOCK_HDR_NO          0  /* block number which must be > 0 */
#define LOG_BLOCK_HDR_DATA_LEN    8  /* number of bytes of log written to this block */
#define LOG_BLOCK_FIRST_REC_GROUP 10 /* offset of the first start of an mtr log record group in this log block */
#define LOG_BLOCK_CHECKPOINT_NO   12
#define LOG_BLOCK_HDR_SIZE        20 /* size of the log block header in bytes */

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



// Maximum number of log groups
#define LOG_GROUPS_MAX_COUNT            32


/* Offsets for a checkpoint field */
#define LOG_CHECKPOINT_NO               0
#define LOG_CHECKPOINT_LSN              8
#define LOG_CHECKPOINT_OFFSET_LOW32     16  // group file index for LOG_CHECKPOINT_LSN
#define LOG_CHECKPOINT_OFFSET_HIGH32    20  // offset of group file for LOG_CHECKPOINT_LSN
#define LOG_CHECKPOINT_ARCHIVED_LSN     24
#define LOG_CHECKPOINT_GROUP_ARRAY      32

// For each value smaller than LOG_MAX_N_GROUPS the following 8 bytes
#define LOG_CHECKPOINT_ARCHIVED_FILE_NO 0
#define LOG_CHECKPOINT_ARCHIVED_OFFSET  4

#define LOG_CHECKPOINT_ARRAY_END        (LOG_CHECKPOINT_GROUP_ARRAY + LOG_GROUPS_MAX_COUNT * 8)
#define LOG_CHECKPOINT_CHECKSUM_1       LOG_CHECKPOINT_ARRAY_END
#define LOG_CHECKPOINT_CHECKSUM_2       (4 + LOG_CHECKPOINT_ARRAY_END)

#define LOG_CHECKPOINT_SIZE             (8 + LOG_CHECKPOINT_ARRAY_END)


constexpr uint32 LOG_SESSION_WAIT_EVENT_COUNT = 2048;
constexpr uint32 LOG_SLOT_MAX_COUNT = 0xFFFF;

typedef enum {
    INIT     = 0,
    COPIED   = 1,
} LogSlotStatus;

typedef struct st_log_slot {
    volatile uint64  lsn;
    volatile uint32  data_len;
    volatile uint32  status;
} log_slot_t;

typedef struct st_log_buf_lsn {
    uint32  data_len;
    union {
#ifndef __WIN__
        atomic128_t value;
#endif
        struct {
            volatile uint64  lsn;   // log sequence number
            volatile uint32  slot_index;
            volatile uint32  block_count;
        };
    } val;
} log_buf_lsn_t;

typedef enum {
    INACTIVE = 0,
    ACTIVE   = 1,
    CURRENT  = 2,
} LogGroupStatus;

#define LOG_GROUP_MAX_COUNT       16
#define LOG_GROUP_INVALID_ID      UINT_MAX32
typedef struct st_log_group {
    char*             name;
    uint32            id;
    uint32            status;
    os_file_t         handle;
    uint64            file_size; // individual log file size in bytes, including the log file header
    uint64            base_lsn;  // starting lsn of group, the starting postion of third block
    UT_LIST_NODE_T(struct st_log_group) list_node;
} log_group_t;

/** Redo log buffer */
typedef struct st_log {
    // padding to prevent other memory update hotspots from residing on the same memory cache line
    byte              pad[64];
    mutex_t           mutex; // mutex protecting the log
    // mutex to serialize access to the flush list when we are putting dirty blocks in the list
    mutex_t           log_flush_order_mutex;

    // log buffer
    byte*             buf_ptr; // unaligned log buffer
    byte*             buf; // log buffer
    uint32            buf_size; // log buffer size in bytes
    uint64            buf_base_lsn; // lsn for buf[0] while service started

    //
    byte*             checkpoint_buf;

    //
    uint8             group_count; // number of log file
    uint8             current_write_group;
    uint8             current_flush_group;
    log_group_t       groups[LOG_GROUP_MAX_COUNT];
    UT_LIST_BASE_NODE_T(log_group_t) log_groups;  // base list of groups

    uint64            log_files_total_size;  // total size of all redo files

    os_aio_array_t*   aio_array;
    os_aio_context_t* aio_ctx_log_write;
    os_aio_context_t* aio_ctx_checkpoint;

    //
    log_slot_t*       slots;
    volatile uint64   slot_write_pos;
    log_buf_lsn_t     buf_lsn;
    volatile uint64   writer_writed_lsn;
    volatile uint64   flusher_flushed_lsn;

    //
    os_event_t        session_wait_events[LOG_SESSION_WAIT_EVENT_COUNT];

    // writer thread and flusher thread
    volatile bool32   writer_event_is_waitting;
    os_event_t        writer_event;
    os_event_t        flusher_event;
    os_thread_t       writer_thread;
    os_thread_t       flusher_thread;


    //checkpoint
    volatile uint64   next_checkpoint_no; // next checkpoint number
    volatile uint64   last_checkpoint_lsn; // latest checkpoint lsn
    uint64            next_checkpoint_lsn; // next checkpoint lsn


} log_t;


/*-----------------------------------------------------------------------*/

extern status_t log_init(uint32 log_buffer_size);
extern bool32 log_group_add(char *name, uint64 file_size);
extern inline lsn_t log_group_get_capacity(const log_group_t* group);

extern inline void log_buffer_reserve(log_buf_lsn_t* buf_lsn, uint32 len);
extern inline uint64 log_buffer_write(uint64 start_lsn, byte *str, uint32 str_len);
extern inline void log_write_complete(log_buf_lsn_t *log_lsn);
extern inline void log_write_up_to(lsn_t lsn);

extern inline lsn_t log_get_flushed_to_disk_lsn();
extern inline lsn_t log_get_writed_to_file_lsn();
extern inline lsn_t log_get_writed_to_buffer_lsn();
extern inline lsn_t log_get_last_checkpoint_lsn();

extern inline uint64 log_block_get_checkpoint_no(const byte* log_block);
extern inline uint64 log_block_get_hdr_no(const byte* log_block);
extern inline uint32 log_block_get_data_len(const byte* log_block);
extern inline uint32 log_block_get_first_rec_group(const byte* log_block);

extern void log_checkpoint(lsn_t checkpoint_lsn);
extern void log_make_checkpoint_at(duint64 lsn);

extern void log_checkpoint_read(uint32 field);  // in: LOG_CHECKPOINT_1 or LOG_CHECKPOINT_2

extern status_t log_group_file_read(log_group_t* group, byte* buf, uint32 len, uint32 byte_offset);

// Test if flush order mutex is owned.
#define log_flush_order_mutex_own()   mutex_own(&log_sys->log_flush_order_mutex)

// Acquire the flush order mutex.
#define log_flush_order_mutex_enter()                   \
    do {                                                \
        mutex_enter(&log_sys->log_flush_order_mutex, &srv_stats.buf_pool_insert_flush_list); \
    } while (0)

// Release the flush order mutex
#define log_flush_order_mutex_exit()                 \
    do {                                             \
        mutex_exit(&log_sys->log_flush_order_mutex); \
    } while (0)


/*-----------------------------------------------------------------------*/

extern log_t*    log_sys;

#ifdef __cplusplus
}
#endif // __cplusplus

#endif // _KNL_LOG_H
