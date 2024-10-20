#ifndef _KNL_FSP_H
#define _KNL_FSP_H

#include "cm_type.h"
#include "cm_list.h"
#include "cm_file.h"
#include "cm_memory.h"
#include "knl_file_system.h"
#include "knl_hash_table.h"
#include "knl_mtr.h"
#include "knl_page_size.h"
#include "knl_server.h"
//#include "knl_dict.h"
#include "knl_page_id.h"
#include "knl_page_size.h"
#include "knl_buf.h"

#ifdef __cplusplus
extern "C" {
#endif

/* The data structures in files are defined just as byte strings in C */
typedef byte    fsp_header_t;
typedef byte    xdes_t;
//typedef byte    page_t;

/* SPACE HEADER
============

File space header data structure: this data structure is contained in the first page of a space.
The space for this header is reserved in every extent descriptor page, but used only in the first. */

#define FSP_HEADER_OFFSET   FIL_PAGE_DATA   /* Offset of the space header within a file page */
/*-------------------------------------*/
#define FSP_SPACE_ID        0 /* space id */

#define FSP_NOT_USED        0   /* this field contained a value up to
                                   which we know that the modifications
                                   in the database have been flushed to the file space;
                                   not used now */
#define FSP_SIZE            8   /* Current size of the space in pages */
#define FSP_FREE_LIMIT      12  /* Minimum page number for
                                   which the free list has not been initialized:
                                   the pages >= this limit are, by definition free */
#define FSP_SPACE_FLAGS     16  /* fsp_space_t.flags, similar to dict_table_t::flags */
#define FSP_FRAG_N_USED     20  /* number of used pages in the FSP_FREE_FRAG list */
#define FSP_FREE            24  /* list of free extents */
#define FSP_FREE_FRAG       (24 + FLST_BASE_NODE_SIZE)  /* list of partially free extents
                                                           not belonging to any segment */
#define FSP_FULL_FRAG       (24 + 2 * FLST_BASE_NODE_SIZE)  /* list of full extents not belonging to any segment */
#define FSP_SEG_ID          (24 + 3 * FLST_BASE_NODE_SIZE)  /* 8 bytes which give the first unused segment id */
#define FSP_SEG_INODES_FULL (32 + 3 * FLST_BASE_NODE_SIZE)  /* list of pages containing segment headers,
                                                               where all the segment inode slots are reserved */
#define FSP_SEG_INODES_FREE (32 + 4 * FLST_BASE_NODE_SIZE)  /* list of pages containing segment headers,
                                                               where not all the segment header slots are reserved */
/*-------------------------------------*/
/* File space header size */
#define FSP_HEADER_SIZE     (32 + 5 * FLST_BASE_NODE_SIZE)
#define FSP_FREE_ADD        4   /* this many free extents are added to the free list
                                   from above FSP_FREE_LIMIT at a time */


/* FILE SEGMENT INODE
==================

Segment inode which is created for each segment in a tablespace. NOTE: in
purge we assume that a segment having only one currently used page can be
freed in a few steps, so that the freeing cannot fill the file buffer with
bufferfixed file pages. */

typedef byte fseg_inode_t;

#define FSEG_INODE_PAGE_NODE    FSEG_PAGE_DATA /* the list node for linking segment inode pages */
#define FSEG_ARR_OFFSET         (FSEG_PAGE_DATA + FLST_NODE_SIZE)

/*-------------------------------------*/
#define FSEG_ID                 0   /* 8 bytes of segment id: if this is ut_dulint_zero,
                                       it means that the header is unused */
#define FSEG_NOT_FULL_N_USED    8   /* number of used segment pages in the FSEG_NOT_FULL list */
#define FSEG_FREE               12  /* list of free extents of this segment */
#define FSEG_NOT_FULL           (12 + FLST_BASE_NODE_SIZE)  /* list of partially free extents */
#define FSEG_FULL               (12 + 2 * FLST_BASE_NODE_SIZE)  /* list of full extents */
#define FSEG_MAGIC_N            (12 + 3 * FLST_BASE_NODE_SIZE)  /* magic number used in debugging */
#define FSEG_FRAG_ARR           (16 + 3 * FLST_BASE_NODE_SIZE)  /* array of individual pages
                                                    belonging to this segment in fsp fragment extent lists */
#define FSEG_FRAG_ARR_N_SLOTS   (FSP_EXTENT_SIZE / 2) /* number of slots in the array for the fragment pages */
#define FSEG_FRAG_SLOT_SIZE     4   /* a fragment page slot contains its page number within space,
                                       FIL_NULL means that the slot is not in use */
/*-------------------------------------*/
#define FSEG_INODE_SIZE (16 + 3 * FLST_BASE_NODE_SIZE + FSEG_FRAG_ARR_N_SLOTS * FSEG_FRAG_SLOT_SIZE)

#define FSP_SEG_INODES_PER_PAGE ((UNIV_PAGE_SIZE - FSEG_ARR_OFFSET - 10) / FSEG_INODE_SIZE)
                    /* Number of segment inodes which fit on a single page */

#define FSEG_MAGIC_N_VALUE      97937874

#define FSEG_FILLFACTOR         8   /* If this value is x, then if
                    the number of unused but reserved pages in a segment is less than reserved pages * 1/x,
                    and there are at least FSEG_FRAG_LIMIT used pages,
                    then we allow a new empty extent to be added to the segment in fseg_alloc_free_page.
                    Otherwise, we use unused pages of the segment. */

#define FSEG_FRAG_LIMIT         FSEG_FRAG_ARR_N_SLOTS
                    /* If the segment has >= this many used pages,
                       it may be expanded by allocating extents to the segment;
                       until that only individual fragment pages are allocated from the space */

#define FSEG_FREE_LIST_LIMIT    40  /* If the reserved size of a segment is at least this many extents,
                    we allow extents to be put to the free list of the extent:
                    at most FSEG_FREE_LIST_MAX_LEN many */
#define FSEG_FREE_LIST_MAX_LEN  4


/* EXTENT DESCRIPTOR
=================

File extent descriptor data structure: contains bits to tell which pages in
the extent are free and which contain old tuple version to clean. */

/*-------------------------------------*/
#define XDES_ID             0   /* The identifier of the segment to which this extent belongs */
#define XDES_FLST_NODE      8   /* The list node data structure for the descriptors */
#define XDES_STATE          (FLST_NODE_SIZE + 8) /* contains state information of the extent */
#define XDES_BITMAP         (FLST_NODE_SIZE + 12) /* Descriptor bitmap of the pages in the extent */
/*-------------------------------------*/
#define XDES_BITS_PER_PAGE  2   /* How many bits are there per page */
#define XDES_FREE_BIT       0   /* Index of the bit which tells if the page is free */
#define XDES_CLEAN_BIT      1   /* NOTE: currently not used */

/* States of a descriptor */
#define XDES_FREE           1   /* extent is in free list of space */
#define XDES_FREE_FRAG      2   /* extent is in free fragment list of space */
#define XDES_FULL_FRAG      3   /* extent is in full fragment list of space */
#define XDES_FSEG           4   /* extent belongs to a segment */

/* File extent data structure size in bytes. The "+ 7 ) / 8" part in the
definition rounds the number of bytes upward. */
#define XDES_SIZE           (XDES_BITMAP + (FSP_EXTENT_SIZE * XDES_BITS_PER_PAGE + 7) / 8)

/* Offset of the descriptor array on a descriptor page */
#define XDES_ARR_OFFSET     (FSP_HEADER_OFFSET + FSP_HEADER_SIZE)












#define FSP_UP              ((byte)111) /*!< alphabetically upwards */
#define FSP_DOWN            ((byte)112) /*!< alphabetically downwards */
#define FSP_NO_DIR          ((byte)113) /*!< no order */

/** Flags for fsp_reserve_free_extents */
enum fsp_reserve_t {
    FSP_NORMAL,	/* reservation during normal B-tree operations */
    FSP_UNDO,	/* reservation done for undo logging */
    FSP_CLEANING,	/* reservation done during purge operations */
    FSP_BLOB	/* reservation being done for BLOB insertion */
};


/* File space extent size in pages */
#define FSP_EXTENT_SIZE     64

/* On a page of any file segment, data may be put starting from this offset: */
#define FSEG_PAGE_DATA      FIL_PAGE_DATA

/* File segment header which points to the inode describing the file segment */
typedef byte fseg_header_t;

#define FSEG_HDR_SPACE      0   /* space id of the inode */
#define FSEG_HDR_PAGE_NO    4   /* page number of the inode */
#define FSEG_HDR_OFFSET     8   /* byte offset of the inode */

#define FSEG_HEADER_SIZE    10

/* FSP PAGE TYPE
=================
*/

#define FSP_XDES_OFFSET             0   // extent descriptor
#define FSP_FIRST_INODE_PAGE_NO     1   // in every tablespace,
                                        // The following pages exist in the system tablespace (space 0).
#define FSP_TRX_SYS_PAGE_NO         2   // transaction system header, in tablespace 0
#define FSP_DICT_HDR_PAGE_NO        3   // data dictionary header page, in tablespace 0
#define FSP_IBUF_BITMAP_OFFSET      4   // insert buffer bitmap
                                        // The ibuf bitmap pages are the ones whose page number is the number
                                        // above plus a multiple of XDES_DESCRIBED_PER_PAGE
#define FSP_IBUF_HEADER_PAGE_NO     5   // insert buffer header page, in tablespace 0
#define FSP_IBUF_TREE_ROOT_PAGE_NO  6   // insert buffer B-tree root page in tablespace 0
                                        // The ibuf tree root page number in tablespace 0;
                                        // its fseg inode is on the page number FSP_FIRST_INODE_PAGE_NO
#define FSP_FIRST_RSEG_PAGE_NO      32  // first rollback segment page, in tablespace 0
                                        // total = TRX_RSEG_MAX_COUNT(96) * TRX_SLOT_PAGE_COUNT_PER_RSEG(4)
#define FSP_DYNAMIC_FIRST_PAGE_NO   320

//-----------------------------------------------

#define FSP_INODE_ALLOC_PAGE_COUNT  8


extern inline uint32 xdes_get_offset(const xdes_t* descr);
extern inline void xdes_set_bit(
    xdes_t  *descr,  /*!< in: descriptor */
    uint32   bit,    /*!< in: XDES_FREE_BIT or XDES_CLEAN_BIT */
    uint32   offset, /*!< in: page offset within extent: 0 ... FSP_EXTENT_SIZE - 1 */
    bool32   val,    /*!< in: bit value */
    mtr_t   *mtr);   /*!< in/out: mini-transaction */








// Check if the space_id is for a system-tablespace (shared + temp).
//inline bool32 is_system_tablespace(uint32 id)
//{
//    return(id == FIL_SYSTEM_SPACE_ID || id == FIL_TEMP_SPACE_ID);
//}

// Check if shared-system or undo tablespace.
//inline bool32 is_system_or_undo_tablespace(uint32     id)
//{
//    return(id == FIL_SYSTEM_SPACE_ID || id <= FIL_UNDO_SPACE_ID);
//}


// Check if tablespace is dd tablespace.
extern inline bool32 fsp_is_dd_tablespace(space_id_t space_id)
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
extern inline bool32 fsp_is_undo_tablespace(space_id_t space_id)
{
  /* Starting with v8, undo space_ids have a unique range. */
  //if (space_id >= UNDO_SPACE_MIN_ID && space_id <= UNDO_SPACE_MAX_ID) {
  //  return (true);
  //}

  /* If upgrading from 5.7, there may be a list of old-style
  undo tablespaces.  Search them. */
  //if (trx_sys_undo_spaces != nullptr) {
  //  return (trx_sys_undo_spaces->contains(space_id));
  //}

  return (false);
}

// Check if tablespace is global temporary.
extern inline bool32 fsp_is_global_temporary(space_id_t space_id)
{
    //return (space_id == srv_tmp_space.space_id());
    return false;
}

// Check if the tablespace is session temporary.
extern inline bool32 fsp_is_session_temporary(space_id_t space_id)
{
    //return (space_id > TEMP_SPACE_MIN_ID && space_id <= TEMP_SPACE_MAX_ID);
    return FALSE;
}

// Check if tablespace is system temporary.
extern inline bool32 fsp_is_system_temporary(space_id_t space_id)
{
    return (fsp_is_global_temporary(space_id) || fsp_is_session_temporary(space_id));
}


//-----------------------------------------------------------------------------------------------------

extern byte* fut_get_ptr(space_id_t space, const page_size_t &page_size,
                  fil_addr_t addr, rw_lock_type_t rw_latch, mtr_t *mtr,
                  buf_block_t **ptr_block);

extern buf_block_t* fsp_page_create(uint32 space_id, uint32 page_no, mtr_t* mtr, mtr_t* init_mtr);


extern bool32 fsp_is_system_temporary(space_id_t space_id);
extern fsp_header_t* fsp_get_space_header(uint32 space_id, const page_size_t& page_size, mtr_t* mtr);

extern status_t fsp_init_space(uint32 space_id, uint32 size);
extern status_t fsp_system_space_reserve_pages(uint32 reserved_max_page_no);

extern void fsp_free_page(const page_id_t& page_id, const page_size_t& page_size, mtr_t* mtr);
extern buf_block_t* fsp_alloc_free_page(uint32 space_id, const page_size_t& page_size, mtr_t* mtr);

extern void fsp_free_extent(    uint32 space_id,   xdes_t* xdes, mtr_t* mtr);
extern xdes_t* fsp_alloc_free_extent(uint32 space_id, const page_size_t&  page_size, mtr_t* mtr);


extern void fseg_init(fseg_inode_t* inode, uint64 seg_id, mtr_t* mtr);
// Allocates a single free page from a segment.
extern buf_block_t* fseg_alloc_free_page(
    uint32          space_id,  /*!< in: space */
    fseg_inode_t*   inode, /*!< in/out: segment inode */
    mtr_t*          mtr,        /*!< in/out: mini-transaction */
    mtr_t*          init_mtr);  /*!< in/out: mtr or another mini-transaction
                        in which the page should be initialized.
                        If init_mtr!=mtr, but the page is already
                        latched in mtr, do not initialize the page. */

// Frees a single page of a segment
extern void fseg_free_page(uint32 space_id, fseg_inode_t* inode, uint32 page_no, mtr_t* mtr);

extern xdes_t* fseg_alloc_free_extent(uint32 space_id, fseg_inode_t* inode, mtr_t* mtr);

// Allocates free extents from table space
extern xdes_t* fseg_reserve_free_extents(uint32 space_id, fseg_inode_t* inode, uint32 count, mtr_t* mtr);


#ifdef __cplusplus
}
#endif

#endif  /* _KNL_FSP_H */

