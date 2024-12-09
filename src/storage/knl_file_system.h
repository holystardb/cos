#ifndef _KNL_FILE_SYSTEM_H
#define _KNL_FILE_SYSTEM_H

#include "cm_type.h"
#include "cm_list.h"
#include "cm_file.h"
#include "cm_memory.h"
#include "knl_hash_table.h"
#include "knl_mtr.h"
#include "knl_page_size.h"
#include "knl_server.h"
#include "knl_page_id.h"
#include "knl_page_size.h"
#include "knl_defs.h"

#ifdef __cplusplus
extern "C" {
#endif


/* 'null' (undefined) page offset in the context of file spaces */
#define FIL_NULL        0xFFFFFFFF

/* Space address data type; this is intended to be used when
addresses accurate to a byte are stored in file pages. If the page part
of the address is FIL_NULL, the address is considered undefined. */

typedef byte fil_faddr_t; /* 'type' definition in C: an address stored in a file page is a string of bytes */
#define FIL_ADDR_PAGE   0   /* first in address is the page offset */
#define FIL_ADDR_BYTE   4   /* then comes 2-byte byte offset within page*/
#define FIL_ADDR_SIZE   6   /* address size is 6 bytes (page_no, offset) */

/* A struct for storing a space address FIL_ADDR, when it is used in C program data structures. */
typedef struct fil_addr_struct {
    uint32      page;    /* page number within a space */
    uint32      boffset; /* byte offset within the page */
} fil_addr_t;

/* Null file address */
extern fil_addr_t   fil_addr_null;

/* The byte offsets on a file page for various variables */
#define FIL_PAGE_SPACE          0   /* space id the page belongs to */
#define FIL_PAGE_OFFSET         4   /* page offset inside space */
#define FIL_PAGE_PREV           8   /* for table */
#define FIL_PAGE_NEXT           12  /* for table */
#define FIL_PAGE_LSN            16  /* lsn of the end of the newest modification log record to the page */
#define FIL_PAGE_TYPE           24  /* file page type: FIL_PAGE_INDEX,..., 2 bytes */
#define FIL_PAGE_FILE_FLUSH_LSN 26  /* this is only defined for the first page in a data file:
                                       the file has been flushed to disk at least up to this lsn */
#define FIL_PAGE_ARCH_LOG_NO    34  /* this is only defined for the first page in a data file:
                                       the latest archived log file number when the flush lsn above was written */
#define FIL_PAGE_DATA           38  /* start of the data on the page */

/* File page trailer */
#define FIL_PAGE_END_LSN        8 /* this should be same as FIL_PAGE_LSN */
#define FIL_PAGE_DATA_END       8


/*-----------------------------------------------------------------*/

/** File page types (values of FIL_PAGE_TYPE) */
#define FIL_PAGE_TYPE_ALLOCATED      0    // Freshly allocated page
#define FIL_PAGE_TYPE_FSP_HDR        1    // File space header
#define FIL_PAGE_TYPE_XDES           2    // Extent descriptor page
#define FIL_PAGE_TYPE_INODE          3    // Index node
#define FIL_PAGE_TYPE_TRX_SYS        4    // Transaction system data
#define FIL_PAGE_TYPE_TRX_SLOT       5    // Transaction slot
#define FIL_PAGE_TYPE_SYSAUX         6    // 
#define FIL_PAGE_TYPE_UNDO_FSM       7    // Undo log page
#define FIL_PAGE_TYPE_UNDO_LOG       8    // Undo log page
#define FIL_PAGE_TYPE_DICT_HDR       9    // Dictionary header
#define FIL_PAGE_TYPE_DICT_DATA      10   // Dictionary header
#define FIL_PAGE_TYPE_HEAP_FSM       11   // Heap Map root page
#define FIL_PAGE_TYPE_HEAP           12   // Heap data page
#define FIL_PAGE_TYPE_BTREE_NONLEAF  13   // B-tree non-leaf node page
#define FIL_PAGE_TYPE_BTREE_LEAF     14   // B-tree node
#define FIL_PAGE_TYPE_HASH_HDR       15   // B-tree non-leaf node page
#define FIL_PAGE_TYPE_HASH_DATA      17   // B-tree node
#define FIL_PAGE_TYPE_TOAST          18   // Toast page

#define FIL_PAGE_TYPE_MASK           0xFF //

#define FIL_PAGE_TYPE_RESIDENT_FLAG  0x8000



//#define FIL_PAGE_IBUF_FREE_LIST     9       // Insert buffer free list
//#define FIL_PAGE_IBUF_BITMAP        10      // Insert buffer bitmap
//#define FIL_PAGE_DOUBLE_WRITE       11      // Double write page
//#define FIL_PAGE_HEAP_MAP_DATA      18      // Heap Map data page
//#define FIL_PAGE_HASH_INDEX_ROOT    19      // Hash root page
//#define FIL_PAGE_HASH_INDEX_DATA    20      // Hash data page



/*-----------------------------------------------------------------*/

#define M_FIL_NODE_NAME_LEN          64
#define M_FIL_NODE_MAGIC_N           89389

#define FIL_NODE_IS_IN_READ_WRITE(node)    (node->n_pending > 0)
#define FIL_NODE_IS_IN_SYNC(node)          (node->n_pending_flushes > 0)
#define FIL_NODE_IS_IN_OPEN_CLOSE(node)    (node->is_io_progress)


typedef struct st_fil_space fil_space_t;

typedef struct st_fil_node {
    char*        name;
    mutex_t      mutex;
    os_file_t    handle;
    uint32       id;
    uint32       page_max_count;
    uint32       page_size;
    uint32       magic_n;
    uint32       is_open : 1;   /* TRUE if file open */
    uint32       is_extend : 1;
    uint32       is_io_progress : 1;  // io progress for open or close
    uint32       is_in_unflushed_list : 1; // only for checkpoint thread, unprotected by mutex
    uint32       reserved : 28;

    // count of pending i/o's on this file;
    // closing of the file is not allowed if this is > 0
    uint32       n_pending;
    // count of pending flushes on this file;
    // closing of the file is not allowed if this is > 0
    uint32       n_pending_flushes;



    // when we write to the file we increment this by one
    uint64       modification_counter;
    // up to what modification_counter value we have flushed the modifications to disk
    uint64       flush_counter;

    fil_space_t *space;

    UT_LIST_NODE_T(struct st_fil_node) chain_list_node;
    UT_LIST_NODE_T(struct st_fil_node) lru_list_node;
    UT_LIST_NODE_T(struct st_fil_node) unflushed_list_node;
} fil_node_t;

/** Space types */
#define FIL_TABLESPACE                501  // tablespace
#define FIL_LOG                       502  // redo log

/* Space types */
#define FIL_SYSTEM_SPACE_ID           DB_SYSTEM_SPACE_ID
#define FIL_SYSTRANS_SPACE_ID         DB_SYSTRANS_SPACE_ID
#define FIL_SYSAUX_SPACE_ID           DB_SYSAUX_SPACE_ID
#define FIL_DBWR_SPACE_ID             DB_DBWR_SPACE_ID
#define FIL_TEMP_SPACE_ID             DB_TEMP_SPACE_ID
#define FIL_UNDO_START_SPACE_ID       DB_UNDO_START_SPACE_ID
#define FIL_UNDO_END_SPACE_ID         DB_UNDO_END_SPACE_ID

#define FIL_USER_SPACE_ID             DB_USER_SPACE_FIRST_ID

typedef struct st_fil_page {
    uint32       file;  /* file id */
    uint32       page;  /* page no */
    UT_LIST_NODE_T(struct st_fil_page) list_node;
} fil_page_t;

struct st_fil_space {
    char*        name;  // tablespace name
    uint32       id;    // tablespace id
    uint32       flags; // tablespace type: FSP_FLAG_SYSTEM, etc
    uint32       size_in_header; // FSP_SIZE in the tablespace header; 0 if not known yet
    uint32       free_limit; // contents of FSP_FREE_LIMIT
    bool32       is_autoextend;
    uint32       autoextend_size;
    uint32       page_size;  // space size in pages
    // number of reserved free extents for ongoing operations like B-tree page split
    uint32       n_reserved_extents;
    uint32       magic_n;
    mutex_t      mutex;
    bool32       io_in_progress;
    rw_lock_t    rw_lock;
    HASH_NODE_T  hash;   // hash chain node

    atomic32_t   refcount;

    UT_LIST_BASE_NODE_T(fil_node_t) fil_nodes;
    UT_LIST_NODE_T(struct st_fil_space) list_node;
};

#define M_FIL_SPACE_MAGIC_N         89472
#define M_FIL_SYSTEM_HASH_LOCKS     4096

typedef struct st_fil_system {
    uint32              max_n_open; /* maximum allowed open files */
    atomic32_t          open_pending_num; /* current number of open files with pending i/o-ops on them */
    uint32              fil_node_max_count;
    uint32              fil_node_num;
    fil_node_t        **fil_nodes;
    uint32              space_max_count;

    memory_area_t      *mem_area;
    memory_pool_t      *mem_pool;
    memory_context_t   *mem_context;

    uint32              aio_pending_count_per_context;
    uint32              aio_context_count;
    os_aio_array_t     *aio_array;

    rw_lock_t           rw_lock[M_FIL_SYSTEM_HASH_LOCKS];
    HASH_TABLE         *space_id_hash; // hash table based on space id
    HASH_TABLE         *name_hash; // hash table based on space name

    mutex_t             mutex;
    UT_LIST_BASE_NODE_T(fil_space_t) fil_spaces;

    mutex_t             lru_mutex;
    UT_LIST_BASE_NODE_T(fil_node_t) fil_node_lru;

    // fil_node_unflushed: only checkpoint thread, no need for locking protection
    UT_LIST_BASE_NODE_T(fil_node_t) fil_node_unflushed;
} fil_system_t;


// Check if the space_id is for a system-tablespace (shared + temp).
inline bool32 is_system_tablespace(uint32 id)
{
    return(id == FIL_SYSTEM_SPACE_ID || id == FIL_TEMP_SPACE_ID);
}

inline void fil_system_pin_space(fil_space_t *space)
{
    atomic32_inc(&space->refcount);
}

inline void fil_system_unpin_space(fil_space_t *space)
{
    ut_ad(space->refcount > 0);
    atomic32_dec(&space->refcount);
}

inline bool32 fil_space_is_pinned(fil_space_t *space)
{
    return space->refcount > 0;
}

//-----------------------------------------------------------------------------------------------------

extern bool32 fil_system_init(memory_pool_t *mem_pool, uint32 max_n_open);
extern inline rw_lock_t* fil_system_get_hash_lock(uint32 space_id);
extern inline fil_space_t* fil_system_get_space_by_id(uint32 space_id);
extern inline void fil_system_insert_space_to_hash_table(fil_space_t* space);
extern bool32 fil_system_flush_filnodes();


extern fil_space_t* fil_space_create(char* name, uint32 space_id, uint32 flags);
extern void fil_space_destroy(uint32 space_id);
extern fil_node_t* fil_node_create(fil_space_t *space, uint32 node_id, char *name,
                                         uint32 page_max_count, uint32 page_size, bool32 is_extend);
extern bool32 fil_node_destroy(fil_space_t* space, fil_node_t* node, bool32 need_space_rwlock = TRUE);
extern bool32 fil_node_open(fil_space_t *space, fil_node_t *node);
extern bool32 fil_node_close(fil_space_t *space, fil_node_t *node);

extern inline fil_node_t* fil_node_get_by_page_id(fil_space_t* space, const page_id_t &page_id);
extern inline void fil_node_complete_io(fil_node_t* node, uint32 type);


extern bool32 fil_addr_is_null(fil_addr_t addr);

extern inline status_t fil_io(uint32 type, bool32 sync, const page_id_t &page_id,
    const page_size_t &page_size, uint32 byte_offset, uint32 len, void* buf,
    aio_slot_func slot_func = NULL, void* message = NULL);
extern inline status_t fil_write(bool32 sync, const page_id_t &page_id,
    const page_size_t &page_size, uint32 len, void* buf, aio_slot_func slot_func, void* message);
extern inline status_t fil_read(bool32 sync, const page_id_t &page_id,
    const page_size_t &page_size, uint32 len, void* buf, aio_slot_func slot_func, void* message);
extern inline void fil_aio_reader_and_writer_wait(os_aio_context_t* context);

extern bool32 fil_space_extend(fil_space_t* space, uint32 size_after_extend, uint32 *actual_size);
extern uint32 fil_space_get_size(uint32 space_id);


//-----------------------------------------------------------------------------------------------------

extern fil_system_t     *fil_system;

#ifdef __cplusplus
}
#endif

#endif  /* _KNL_FILE_SYSTEM_H */

