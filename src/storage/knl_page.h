#ifndef _KNL_PAGE_H
#define _KNL_PAGE_H

#include "cm_type.h"
#include "cm_util.h"
#include "cm_dbug.h"
#include "knl_server.h"
#include "knl_fsp.h"

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
    return(mach_read_from_4(page + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID));
}


#endif  /* _KNL_PAGE_H */
