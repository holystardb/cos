#ifndef _KNL_PAGE_H
#define _KNL_PAGE_H

#include "cm_type.h"
#include "cm_util.h"
#include "cm_dbug.h"
#include "knl_server.h"


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


#define LOG_BLOCK_FLUSH_BIT_MASK 0x80000000 /* mask used to get the highest bit in the preceding field */

/* Offsets of a log block header */
#define	LOG_BLOCK_HDR_NO            0   /* block number which must be > 0 and is allowed to wrap around at 2G;
                                       the highest bit is set to 1 if this is the first log block in a log flush write segment */
#define	LOG_BLOCK_HDR_DATA_LEN      4   /* number of bytes of log written to this block */
#define	LOG_BLOCK_FIRST_REC_GROUP   6
#define LOG_BLOCK_CHECKPOINT_NO     8
#define LOG_BLOCK_HDR_SIZE          12  /* size of the log block header in bytes */

/* Offsets of a log block trailer from the end of the block */
#define	LOG_BLOCK_TRL_NO            4   /* log block number */
#define	LOG_BLOCK_TRL_SIZE          4   /* trailer size in bytes */


typedef byte        page_t;


/************************************************************//**
Gets the start of a page.
@return start of the page */
inline page_t* page_align(void* ptr) /*!< in: pointer to page frame */
{
    return((page_t*) ut_align_down(ptr, UNIV_PAGE_SIZE));
}

/************************************************************//**
Gets the offset within a page.
@return offset from the start of the page */
inline uint32 page_offset(void* ptr) /*!< in: pointer to page frame */
{
    return(ut_align_offset(ptr, UNIV_PAGE_SIZE));
}

/** Gets the page number.
 @return page number */
inline page_no_t page_get_page_no(const page_t *page) /*!< in: page */
{
  ut_ad(page == page_align((page_t *)page));
  return (mach_read_from_4(page + FIL_PAGE_OFFSET));
}


#endif  /* _KNL_PAGE_H */
