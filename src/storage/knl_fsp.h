#ifndef _KNL_FSP_H
#define _KNL_FSP_H

#include "cm_type.h"
#include "cm_list.h"
#include "cm_file.h"
#include "cm_memory.h"
#include "knl_hash_table.h"
#include "knl_mtr.h"
#include "knl_page_size.h"
#include "knl_server.h"


#ifdef __cplusplus
extern "C" {
#endif

/* The data structures in files are defined just as byte strings in C */
typedef byte    fsp_header_t;
typedef byte    xdes_t;
typedef byte    page_t;

/* SPACE HEADER
============

File space header data structure: this data structure is contained in the first page of a space.
The space for this header is reserved in every extent descriptor page, but used only in the first. */

#define FSP_HEADER_OFFSET   FIL_PAGE_DATA   /* Offset of the space header within a file page */
/*-------------------------------------*/
#define FSP_SPACE_ID        0 /* space id */

#define FSP_NOT_USED        0   /* this field contained a value up to
which we know that the modifications in the database have been flushed to the file space; not used now */
#define	FSP_SIZE            8   /* Current size of the space in pages */
#define	FSP_FREE_LIMIT      12  /* Minimum page number for which the free list has not been initialized:
the pages >= this limit are, by definition free */
#define	FSP_SPACE_FLAGS		16	/* fsp_space_t.flags, similar to dict_table_t::flags */
#define	FSP_FRAG_N_USED     20  /* number of used pages in the FSP_FREE_FRAG list */
#define	FSP_FREE            24  /* list of free extents */
#define	FSP_FREE_FRAG       (24 + FLST_BASE_NODE_SIZE)
/* list of partially free extents not belonging to any segment */
#define	FSP_FULL_FRAG       (24 + 2 * FLST_BASE_NODE_SIZE)
/* list of full extents not belonging to any segment */
#define FSP_SEG_ID          (24 + 3 * FLST_BASE_NODE_SIZE)
/* 8 bytes which give the first unused segment id */
#define FSP_SEG_INODES_FULL (32 + 3 * FLST_BASE_NODE_SIZE)
/* list of pages containing segment headers, where all the segment inode slots are reserved */
#define FSP_SEG_INODES_FREE (32 + 4 * FLST_BASE_NODE_SIZE)
/* list of pages containing segment headers, where not all the segment header slots are reserved */
/*-------------------------------------*/
/* File space header size */
#define	FSP_HEADER_SIZE     (32 + 5 * FLST_BASE_NODE_SIZE)

#define	FSP_FREE_ADD        4   /* this many free extents are added to the free list from above FSP_FREE_LIMIT at a time */


/* FILE SEGMENT INODE
==================

Segment inode which is created for each segment in a tablespace. NOTE: in
purge we assume that a segment having only one currently used page can be
freed in a few steps, so that the freeing cannot fill the file buffer with
bufferfixed file pages. */

typedef	byte	fseg_inode_t;

#define FSEG_INODE_PAGE_NODE	FSEG_PAGE_DATA
					/* the list node for linking
					segment inode pages */

#define FSEG_ARR_OFFSET		(FSEG_PAGE_DATA + FLST_NODE_SIZE)
/*-------------------------------------*/
#define	FSEG_ID			0	/* 8 bytes of segment id: if this is
					ut_dulint_zero, it means that the
					header is unused */
#define FSEG_NOT_FULL_N_USED	8
					/* number of used segment pages in
					the FSEG_NOT_FULL list */
#define	FSEG_FREE		12
					/* list of free extents of this
					segment */
#define	FSEG_NOT_FULL		(12 + FLST_BASE_NODE_SIZE)
					/* list of partially free extents */
#define	FSEG_FULL		(12 + 2 * FLST_BASE_NODE_SIZE)
					/* list of full extents */
#define	FSEG_MAGIC_N		(12 + 3 * FLST_BASE_NODE_SIZE)
					/* magic number used in debugging */
#define	FSEG_FRAG_ARR		(16 + 3 * FLST_BASE_NODE_SIZE)
					/* array of individual pages
					belonging to this segment in fsp
					fragment extent lists */
#define FSEG_FRAG_ARR_N_SLOTS	(FSP_EXTENT_SIZE / 2)
					/* number of slots in the array for
					the fragment pages */
#define	FSEG_FRAG_SLOT_SIZE	4	/* a fragment page slot contains its
					page number within space, FIL_NULL
					means that the slot is not in use */
/*-------------------------------------*/
#define FSEG_INODE_SIZE	(16 + 3 * FLST_BASE_NODE_SIZE + FSEG_FRAG_ARR_N_SLOTS * FSEG_FRAG_SLOT_SIZE)

#define FSP_SEG_INODES_PER_PAGE	((UNIV_PAGE_SIZE - FSEG_ARR_OFFSET - 10) / FSEG_INODE_SIZE)
				/* Number of segment inodes which fit on a
				single page */

#define FSEG_MAGIC_N_VALUE	97937874
					
#define	FSEG_FILLFACTOR		8	/* If this value is x, then if
					the number of unused but reserved
					pages in a segment is less than
					reserved pages * 1/x, and there are
					at least FSEG_FRAG_LIMIT used pages,
					then we allow a new empty extent to
					be added to the segment in
					fseg_alloc_free_page. Otherwise, we
					use unused pages of the segment. */
					
#define FSEG_FRAG_LIMIT		FSEG_FRAG_ARR_N_SLOTS
					/* If the segment has >= this many
					used pages, it may be expanded by
					allocating extents to the segment;
					until that only individual fragment
					pages are allocated from the space */

#define	FSEG_FREE_LIST_LIMIT	40	/* If the reserved size of a segment
					is at least this many extents, we
					allow extents to be put to the free
					list of the extent: at most
					FSEG_FREE_LIST_MAX_LEN many */
#define	FSEG_FREE_LIST_MAX_LEN	4
					

/* EXTENT DESCRIPTOR
=================

File extent descriptor data structure: contains bits to tell which pages in
the extent are free and which contain old tuple version to clean. */

/*-------------------------------------*/
#define	XDES_ID			0	/* The identifier of the segment
					to which this extent belongs */
#define XDES_FLST_NODE		8	/* The list node data structure
					for the descriptors */
#define	XDES_STATE		(FLST_NODE_SIZE + 8)
					/* contains state information
					of the extent */
#define	XDES_BITMAP		(FLST_NODE_SIZE + 12)
					/* Descriptor bitmap of the pages
					in the extent */
/*-------------------------------------*/
					
#define	XDES_BITS_PER_PAGE	2	/* How many bits are there per page */
#define	XDES_FREE_BIT		0	/* Index of the bit which tells if the page is free */
#define	XDES_CLEAN_BIT		1	/* NOTE: currently not used!
					Index of the bit which tells if there are old versions of tuples on the page */

/* States of a descriptor */
#define XDES_FREE           1   /* extent is in free list of space */
#define XDES_FREE_FRAG      2   /* extent is in free fragment list of space */
#define XDES_FULL_FRAG      3   /* extent is in full fragment list of space */
#define XDES_FSEG           4   /* extent belongs to a segment */

/* File extent data structure size in bytes. The "+ 7 ) / 8" part in the
definition rounds the number of bytes upward. */
#define	XDES_SIZE	(XDES_BITMAP + (FSP_EXTENT_SIZE * XDES_BITS_PER_PAGE + 7) / 8)

/* Offset of the descriptor array on a descriptor page */
#define	XDES_ARR_OFFSET		(FSP_HEADER_OFFSET + FSP_HEADER_SIZE)



















/* File space extent size in pages */
#define FSP_EXTENT_SIZE 64

/* On a page of any file segment, data may be put starting from this offset: */
#define FSEG_PAGE_DATA FIL_PAGE_DATA

/* File segment header which points to the inode describing the file segment */
typedef byte fseg_header_t;


#define FSEG_HDR_SPACE		0	/* space id of the inode */
#define FSEG_HDR_PAGE_NO	4	/* page number of the inode */
#define FSEG_HDR_OFFSET		8	/* byte offset of the inode */

#define FSEG_HEADER_SIZE	10


#define FSP_XDES_OFFSET             0   /* !< extent descriptor */
#define FSP_IBUF_BITMAP_OFFSET      1   /* !< insert buffer bitmap */
                                        /* The ibuf bitmap pages are the ones whose page number is the number
                                           above plus a multiple of XDES_DESCRIBED_PER_PAGE */
#define FSP_FIRST_INODE_PAGE_NO     2   /*!< in every tablespace */
                                        /* The following pages exist in the system tablespace (space 0). */
#define FSP_IBUF_HEADER_PAGE_NO     3   /*!< insert buffer header page, in tablespace 0 */
#define FSP_IBUF_TREE_ROOT_PAGE_NO  4   /*!< insert buffer B-tree root page in tablespace 0 */
                                        /* The ibuf tree root page number in tablespace 0;
                                           its fseg inode is on the page number FSP_FIRST_INODE_PAGE_NO */
#define FSP_TRX_SYS_PAGE_NO         5   /*!< transaction system header, in tablespace 0 */
#define	FSP_FIRST_RSEG_PAGE_NO      6   /*!< first rollback segment page, in tablespace 0 */
#define FSP_DICT_HDR_PAGE_NO        7   /*!< data dictionary header page, in tablespace 0 */



/* 'null' (undefined) page offset in the context of file spaces */
#define FIL_NULL        0xFFFFFFFF

/* Space address data type; this is intended to be used when
addresses accurate to a byte are stored in file pages. If the page part
of the address is FIL_NULL, the address is considered undefined. */

typedef byte fil_faddr_t; /* 'type' definition in C: an address stored in a file page is a string of bytes */
#define FIL_ADDR_PAGE   0   /* first in address is the page offset */
#define FIL_ADDR_BYTE   4   /* then comes 2-byte byte offset within page*/
#define FIL_ADDR_SIZE   6   /* address size is 6 bytes */

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
#define FIL_PAGE_PREV           8   /* if there is a 'natural' predecessor of the page, its offset */
#define FIL_PAGE_NEXT           12  /* if there is a 'natural' successor of the page, its offset */
#define FIL_PAGE_LSN            16  /* lsn of the end of the newest modification log record to the page */
#define	FIL_PAGE_TYPE           24  /* file page type: FIL_PAGE_INDEX,..., 2 bytes */
#define FIL_PAGE_FILE_FLUSH_LSN 26  /* this is only defined for the first page in a data file:
                                       the file has been flushed to disk at least up to this lsn */
#define FIL_PAGE_ARCH_LOG_NO    34  /* this is only defined for the first page in a data file:
                                       the latest archived log file number when the flush lsn above was written */
#define FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID  34 /*!< starting from 4.1.x this contains the space id of the page */
#define FIL_PAGE_DATA           38  /* start of the data on the page */

#define FIL_PAGE_SPACE_ID       34



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

typedef struct st_fil_space {
    char        *name;  /* space name */
    uint32       id;  /* space id */
    uint32       purpose;  // Space types
    uint32       flags;

    uint32       size_in_header; /* FSP_SIZE in the tablespace header; 0 if not known yet */
    uint32       free_limit; /*!< contents of FSP_FREE_LIMIT */
    uint32       flags; /*!< tablespace flags; see fsp_flags_is_valid(), page_size_t(ulint) (constructor) */

    uint32       page_size;  /* space size in pages */
    /* number of reserved free extents for ongoing operations like B-tree page split */
    uint32       n_reserved_extents;
    uint32       magic_n;
    spinlock_t   lock;
    bool32       io_in_progress;
    rw_lock_t    latch;
    HASH_NODE_T  hash;   /*!< hash chain node */


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
    uint32              space_max_count;
    spinlock_t          lock;

    memory_context_t   *mem_context;


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


// Check if tablespace is dd tablespace.
inline bool32 fsp_is_dd_tablespace(space_id_t space_id)
{
    //return (space_id == dict_sys_t::s_space_id);
    return FALSE;
}

/* Check whether a space id is an undo tablespace ID Undo tablespaces
   have space_id's starting 1 less than the redo logs.
   They are numbered down from this.  Since rseg_id=0 always refers to the system tablespace,
   undo_space_num values start at 1.  The current limit is 127.
   The translation from an undo_space_num is: undo space_id = log_first_space_id - undo_space_num
*/
inline bool32 fsp_is_undo_tablespace(space_id_t space_id)
{
  /* Starting with v8, undo space_ids have a unique range. */
  if (space_id >= UNDO_SPACE_MIN_ID && space_id <= UNDO_SPACE_MAX_ID) {
    return (true);
  }

  /* If upgrading from 5.7, there may be a list of old-style
  undo tablespaces.  Search them. */
  //if (trx_sys_undo_spaces != nullptr) {
  //  return (trx_sys_undo_spaces->contains(space_id));
  //}

  return (false);
}

// Check if tablespace is global temporary.
inline bool32 fsp_is_global_temporary(space_id_t space_id)
{
    //return (space_id == srv_tmp_space.space_id());
    return false;
}

// Check if the tablespace is session temporary.
inline bool32 fsp_is_session_temporary(space_id_t space_id)
{
    return (space_id > TEMP_SPACE_MIN_ID && space_id <= TEMP_SPACE_MAX_ID);
}

// Check if tablespace is system temporary.
inline bool32 fsp_is_system_temporary(space_id_t space_id)
{
    return (fsp_is_global_temporary(space_id) || fsp_is_session_temporary(space_id));
}


bool32 fil_system_init(memory_pool_t *pool, uint32 max_n_open, uint32 space_max_count, uint32 fil_node_max_count);
fil_space_t* fil_space_create(char *name, uint32 space_id, uint32 purpose);
void fil_space_destroy(uint32 space_id);
fil_node_t* fil_node_create(fil_space_t *space, char *name, uint32 page_max_count, uint32 page_size, bool32 is_extend);
bool32 fil_node_destroy(fil_space_t *space, fil_node_t *node);
bool32 fil_node_open(fil_space_t *space, fil_node_t *node);
bool32 fil_node_close(fil_space_t *space, fil_node_t *node);
fil_space_t* fil_space_get_by_id(uint32 space_id);
rw_lock_t* fil_space_get_latch(uint32 space_id, uint32 *flags = NULL);



bool32 fsp_is_system_temporary(space_id_t space_id);
fsp_header_t* fsp_get_space_header(uint32 id, const page_size_t& page_size, mtr_t* mtr);
void fsp_header_init(uint32 space_id, uint32 size, mtr_t *mtr);

bool fil_addr_is_null(fil_addr_t addr);

#ifdef __cplusplus
}
#endif

#endif  /* _KNL_FSP_H */

