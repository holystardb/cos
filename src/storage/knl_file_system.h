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
#define FIL_PAGE_ALLOCATED          0       /*!< Freshly allocated page */
#define FIL_PAGE_FSP_HDR            1       /*!< File space header */
#define FIL_PAGE_XDES               2       /*!< Extent descriptor page */
#define FIL_PAGE_INODE              3       /*!< Index node */
#define FIL_PAGE_TRX_SYS            4       /*!< Transaction system data */
#define FIL_PAGE_TRX_SLOT           5       /*!< Transaction system data */
#define FIL_PAGE_UNDO_LOG           6       /*!< Undo log page */
#define FIL_PAGE_DICT_HDR           7       /*!< Dictionary header */
#define FIL_PAGE_DICT_DATA          8       /*!< Dictionary data */
#define FIL_PAGE_IBUF_FREE_LIST     9       /*!< Insert buffer free list */
#define FIL_PAGE_IBUF_BITMAP        10      /*!< Insert buffer bitmap */
#define FIL_PAGE_DOUBLE_WRITE       11      /*!< Double write page */
#define FIL_PAGE_SYS                12      /*!< System page */
#define FIL_PAGE_BTREE_ROOT         13      /*!< B-tree root node */
#define FIL_PAGE_BTREE_DATA         14      /*!< B-tree node */
#define FIL_PAGE_HEAP_ROOT          15      /*!< Heap root page */
#define FIL_PAGE_HEAP_DATA          16      /*!< Heap data page */
#define FIL_PAGE_HEAP_FSM           17      /*!< Heap Map root page */
#define FIL_PAGE_HEAP_MAP_DATA      18      /*!< Heap Map data page */
#define FIL_PAGE_HASH_INDEX_ROOT    19      /*!< Hash root page */
#define FIL_PAGE_HASH_INDEX_DATA    20      /*!< Hash data page */
#define FIL_PAGE_TOAST              21      /*!< Toast page */
#define FIL_PAGE_TYPE_BLOB          22      /*!< Uncompressed BLOB page */
#define FIL_PAGE_TYPE_ZBLOB         23      /*!< First compressed BLOB page */
#define FIL_PAGE_TYPE_ZBLOB2        24      /*!< Subsequent compressed BLOB page */
#define FIL_PAGE_TYPE_LAST          FIL_PAGE_TYPE_ZBLOB2    /*!< Last page type */



/*-----------------------------------------------------------------*/

#define M_FIL_NODE_NAME_LEN          64
#define M_FIL_NODE_MAGIC_N           89389

typedef struct st_fil_space fil_space_t;

typedef struct st_fil_node {
    char        *name;
    mutex_t      mutex;
    os_file_t    handle;
    uint32       id;
    uint32       page_max_count;
    uint32       page_size;
    uint32       magic_n;
    uint32       is_open : 1;   /* TRUE if file open */
    uint32       is_extend : 1;
    uint32       is_io_progress : 1;  // io progress for open or close
    uint32       n_pending : 16; /* count of pending i/o-ops on this file */
    uint32       reserved : 13;
    fil_space_t *space;

    UT_LIST_NODE_T(struct st_fil_node) chain_list_node;
    UT_LIST_NODE_T(struct st_fil_node) lru_list_node; /* link field for the LRU list */
} fil_node_t;

/** Space types */
#define FIL_TABLESPACE   501  // tablespace
#define FIL_LOG          502  // redo log

/* Space types */
#define FIL_SYSTEM_SPACE_ID           0
#define FIL_REDO_SPACE_ID             1
#define FIL_ARCH_LOG_SPACE_ID         2
#define FIL_UNDO_SPACE_ID             3
#define FIL_TEMP_SPACE_ID             4
#define FIL_DICT_SPACE_ID             5
#define FIL_USER_SPACE_ID             100

/** Use maximum UINT value to indicate invalid space ID. */
#define FIL_INVALID_SPACE_ID          0xFFFFFFFF




#define SRV_INVALID_SPACE_ID          0xFFFFFFFF
#define SRV_DICT_SPACE_ID             0xFFFFFFFE
#define SRV_TEMP_SPACE_ID             0xFFFFFFFD
#define SRV_REDO_SPACE_ID             0xFFFFFFFC
#define SRV_UNDO_SPACE_ID             0xFFFFFFFB
#define SRV_SYSTEM_SPACE_ID           0x00000000


#define M_FIL_SPACE_NAME_LEN            64

typedef struct st_fil_page {
    uint32       file;  /* file id */
    uint32       page;  /* page no */
    UT_LIST_NODE_T(struct st_fil_page) list_node;
} fil_page_t;

struct st_fil_space {
    char        *name;  /* space name */
    uint32       id;  /* space id */
    uint32       purpose;  // Space types
    uint32       size_in_header; /* FSP_SIZE in the tablespace header; 0 if not known yet */
    uint32       free_limit; /*!< contents of FSP_FREE_LIMIT */
    uint32       flags; /*!< tablespace flags; see fsp_flags_is_valid(), page_size_t(ulint) (constructor) */
    bool32       is_autoextend;
    uint32       autoextend_size;
    uint32       page_size;  /* space size in pages */
    /* number of reserved free extents for ongoing operations like B-tree page split */
    uint32       n_reserved_extents;
    uint32       magic_n;
    mutex_t      mutex;
    bool32       io_in_progress;
    rw_lock_t    latch;
    HASH_NODE_T  hash;   /*!< hash chain node */

    atomic32_t   refcount;


    UT_LIST_BASE_NODE_T(fil_node_t) fil_nodes;
    UT_LIST_BASE_NODE_T(fil_page_t) free_pages;
    UT_LIST_NODE_T(struct st_fil_space) list_node;
};

#define M_FIL_SPACE_MAGIC_N         89472

typedef struct st_fil_system {
    uint32              max_n_open; /* maximum allowed open files */
    atomic32_t          open_pending_num; /* current number of open files with pending i/o-ops on them */
    uint32              fil_node_max_count;
    uint32              fil_node_num;
    fil_node_t        **fil_nodes;
    uint32              space_max_count;
    mutex_t             mutex;
    mutex_t             lru_mutex;

    memory_area_t      *mem_area;
    memory_pool_t      *mem_pool;
    memory_context_t   *mem_context;

    uint32              aio_pending_count_per_context;
    uint32              aio_context_count;
    os_aio_array_t     *aio_array;

    HASH_TABLE         *spaces; /*!< The hash table of spaces in the system; they are hashed on the space id */
    HASH_TABLE         *name_hash; /*!< hash table based on the space name */

    UT_LIST_BASE_NODE_T(fil_space_t) fil_spaces;
    UT_LIST_BASE_NODE_T(fil_node_t) fil_node_lru;
} fil_system_t;


// Check if the space_id is for a system-tablespace (shared + temp).
inline bool32 is_system_tablespace(uint32 id)
{
    return(id == FIL_SYSTEM_SPACE_ID || id == FIL_TEMP_SPACE_ID);
}

// Check if shared-system or undo tablespace.
inline bool32 is_system_or_undo_tablespace(uint32     id)
{
    return(id == FIL_SYSTEM_SPACE_ID || id <= FIL_UNDO_SPACE_ID);
}

inline void fil_system_pin_space(fil_space_t *space)
{
    //ut_ad(mutex_own(&fil_system->lock));
    space->refcount++;
}

inline void fil_system_unpin_space(fil_space_t *space)
{
    //ut_ad(mutex_own(&fil_system->lock));
    space->refcount--;
}

//-----------------------------------------------------------------------------------------------------

extern bool32 fil_system_init(memory_pool_t *pool, uint32 max_n_open,
                                    uint32 space_max_count, uint32 fil_node_max_count);
extern fil_space_t* fil_space_create(char *name, uint32 space_id, uint32 purpose);
extern void fil_space_destroy(uint32 space_id);
extern fil_node_t* fil_node_create(fil_space_t *space, char *name,
                                         uint32 page_max_count, uint32 page_size, bool32 is_extend);
extern bool32 fil_node_destroy(fil_space_t *space, fil_node_t *node);
extern bool32 fil_node_open(fil_space_t *space, fil_node_t *node);
extern bool32 fil_node_close(fil_space_t *space, fil_node_t *node);
extern fil_space_t* fil_get_space_by_id(uint32 space_id);
extern void fil_release_space(fil_space_t* space);
rw_lock_t* fil_space_get_latch(uint32 space_id, uint32 *flags = NULL);

extern bool fil_addr_is_null(fil_addr_t addr);


extern  dberr_t fil_write(bool32 sync, const page_id_t &page_id, const page_size_t &page_size,
                              uint32 len, void*  buf, void*  message);
extern dberr_t fil_read(bool32 sync, const page_id_t &page_id, const page_size_t &page_size,
                           uint32 len, void*  buf, void*  message);

extern bool32 fil_space_reserve_free_extents(uint32 id, uint32 n_free_now, uint32 n_to_reserve);
extern void fil_space_release_free_extents(uint32 id, uint32 n_reserved);
extern bool32 fil_space_extend(fil_space_t* space, uint32 size_after_extend, uint32 *actual_size);
extern uint32 fil_space_get_size(uint32 space_id);


//-----------------------------------------------------------------------------------------------------

extern fil_system_t     *fil_system;

#ifdef __cplusplus
}
#endif

#endif  /* _KNL_FILE_SYSTEM_H */

