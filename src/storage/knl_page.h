#ifndef _KNL_PAGE_H
#define _KNL_PAGE_H

#include "cm_type.h"
#include "cm_util.h"
#include "cm_dbug.h"
#include "knl_server.h"
#include "knl_fsp.h"

//typedef byte        page_t;




typedef byte        page_header_t;

#define PAGE_HEADER         FSEG_PAGE_DATA  /* index page header starts at this offset */

/* file segment header for the leaf pages in a B-tree:
   defined only on the root page of a B-tree,
   but not in the root of an ibuf tree */
#define PAGE_BTR_SEG_LEAF   36

/* file segment header for the non-leaf pages in a B-tree:
   defined only on the root page of a B-tree,
   but not in the root of an ibuf tree */
#define PAGE_BTR_SEG_TOP    (36 + FSEG_HEADER_SIZE)

/* start of data on the page */
#define PAGE_DATA           (PAGE_HEADER + 36 + 2 * FSEG_HEADER_SIZE)



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

// Gets the page number
inline page_no_t page_get_page_no(const page_t *page) /*!< in: page */
{
  ut_ad(page == page_align((page_t *)page));
  return (mach_read_from_4(page + FIL_PAGE_OFFSET));
}

// Gets the tablespace identifier.
inline space_id_t page_get_space_id(const page_t* page) /*!< in: page */
{
    ut_ad(page == page_align((page_t*) page));
    return(mach_read_from_4(page + FIL_PAGE_SPACE));
}

extern page_t* page_create(
    buf_block_t* block,  /*!< in: a buffer block where the page is created */
    mtr_t* mtr);


#endif  /* _KNL_PAGE_H */
