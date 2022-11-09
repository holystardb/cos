#ifndef _KNL_FSP_H
#define _KNL_FSP_H

#include "cm_type.h"
#include "cm_list.h"
#include "cm_file.h"
#include "cm_memory.h"
#include "knl_mtr.h"

#ifdef __cplusplus
extern "C" {
#endif


/* File space extent size in pages */
#define	FSP_EXTENT_SIZE		64

/* On a page of any file segment, data may be put starting from this offset: */
#define FSEG_PAGE_DATA		FIL_PAGE_DATA

/* File segment header which points to the inode describing the file segment */
typedef	byte	fseg_header_t;

#define FSEG_HDR_SPACE		0	/* space id of the inode */
#define FSEG_HDR_PAGE_NO	4	/* page number of the inode */
#define FSEG_HDR_OFFSET		8	/* byte offset of the inode */

#define FSEG_HEADER_SIZE	10





/* 'null' (undefined) page offset in the context of file spaces */
#define	FIL_NULL        0xFFFFFFFF

/* Space address data type; this is intended to be used when
addresses accurate to a byte are stored in file pages. If the page part
of the address is FIL_NULL, the address is considered undefined. */

typedef byte fil_faddr_t; /* 'type' definition in C: an address stored in a file page is a string of bytes */
#define FIL_ADDR_PAGE   0   /* first in address is the page offset */
#define	FIL_ADDR_BYTE   4   /* then comes 2-byte byte offset within page*/
#define	FIL_ADDR_SIZE   6   /* address size is 6 bytes */

/* A struct for storing a space address FIL_ADDR, when it is used in C program data structures. */
typedef struct fil_addr_struct fil_addr_t;
struct fil_addr_struct{
    uint32 page;    /* page number within a space */
    uint32 boffset; /* byte offset within the page */
};

/* Null file address */
extern fil_addr_t   fil_addr_null;

/* The byte offsets on a file page for various variables */
#define FIL_PAGE_SPACE          0   /* space id the page belongs to */
#define FIL_PAGE_OFFSET         4   /* page offset inside space */
#define FIL_PAGE_PREV           8   /* if there is a 'natural' predecessor of the page, its offset */
#define FIL_PAGE_NEXT           12  /* if there is a 'natural' successor of the page, its offset */
#define FIL_PAGE_LSN            16  /* lsn of the end of the newest modification log record to the page */
#define	FIL_PAGE_TYPE           24  /* file page type: FIL_PAGE_INDEX,..., 2 bytes */
#define FIL_PAGE_FILE_FLUSH_LSN 26  /* this is only defined for the first page in a data file:
                                       the file has been flushed to disk at least up to this lsn */
#define FIL_PAGE_ARCH_LOG_NO    34  /* this is only defined for the first page in a data file:
                                       the latest archived log file number when the flush lsn above was written */
#define FIL_PAGE_DATA           38  /* start of the data on the page */

/* File page trailer */
#define FIL_PAGE_END_LSN        8 /* this should be same as FIL_PAGE_LSN */
#define FIL_PAGE_DATA_END       8


/*-----------------------------------------------------------------*/

#define M_FIL_NODE_NAME_LEN          64
#define M_FIL_NODE_MAGIC_N           89389

typedef struct st_fil_node {
    char        *name;
    uint32       id;
    os_file_t    handle;
    uint32       page_max_count;
    uint32       page_hwm;
    uint32       page_size;
    uint32       magic_n;
    uint32       is_open : 1;   /* TRUE if file open */
    uint32       is_extend : 1;
    uint32       n_pending : 1; /* count of pending i/o-ops on this file */
    uint32       reserved : 29;
    UT_LIST_NODE_T(struct st_fil_node) chain_list_node;
    UT_LIST_NODE_T(struct st_fil_node) lru_list_node; /* link field for the LRU list */
} fil_node_t;

/* Space types */
#define FIL_SYSTEM_SPACE_ID           0
#define FIL_REDO_SPACE_ID             1
#define FIL_ARCH_LOG_SPACE_ID         2
#define FIL_UNDO_SPACE_ID             3
#define FIL_TEMP_SPACE_ID             4
#define FIL_USER_SPACE_ID             100

#define M_FIL_SPACE_NAME_LEN            64

typedef struct st_fil_page {
    uint32       file;  /* file id */
    uint32       page;  /* page no */
    UT_LIST_NODE_T(struct st_fil_page) list_node;
} fil_page_t;

typedef struct st_fil_space {
    char        *name;  /* space name */
    uint32       id;  /* space id */
    uint32       purpose;  // Space types
    uint32       page_size;  /* space size in pages */
    /* number of reserved free extents for ongoing operations like B-tree page split */
    uint32       n_reserved_extents;
    uint32       magic_n;
    spinlock_t   lock;
    bool32       io_in_progress;
    UT_LIST_BASE_NODE_T(fil_node_t) fil_nodes;
    UT_LIST_BASE_NODE_T(fil_page_t) free_pages;
    UT_LIST_NODE_T(struct st_fil_space) list_node;
} fil_space_t;

#define M_FIL_SPACE_MAGIC_N         89472

typedef struct st_fil_system {
    uint32              max_n_open; /* maximum allowed open files */
    uint32              open_pending_num; /* current number of open files with pending i/o-ops on them */
    uint32              fil_node_max_count;
    uint32              fil_node_num;
    fil_node_t        **fil_nodes;
    spinlock_t          lock;
    memory_context_t   *mem_context;
    UT_LIST_BASE_NODE_T(fil_space_t) fil_spaces;
    UT_LIST_BASE_NODE_T(fil_node_t) fil_node_lru;
} fil_system_t;

bool32 fil_system_init(memory_pool_t *pool, uint32 max_n_open, uint32 fil_node_max_count);
fil_space_t* fil_space_create(char *name, uint32 space_id, uint32 purpose);
void fil_space_destroy(uint32 space_id);
fil_node_t* fil_node_create(fil_space_t *space, char *name, uint32 page_max_count, uint32 page_size, bool32 is_extend);
bool32 fil_node_destroy(fil_space_t *space, fil_node_t *node);
bool32 fil_node_open(fil_space_t *space, fil_node_t *node);
bool32 fil_node_close(fil_space_t *space, fil_node_t *node);



bool32 fsp_is_system_temporary(space_id_t space_id);


#ifdef __cplusplus
}
#endif

#endif  /* _KNL_FSP_H */

