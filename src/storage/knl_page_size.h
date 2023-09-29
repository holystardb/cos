#ifndef _KNL_PAGE_SIZE_H
#define _KNL_PAGE_SIZE_H

#include "cm_type.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Define the Min, Max, Default page sizes. */
/** Minimum Page Size Shift (power of 2) */
#define UNIV_PAGE_SIZE_SHIFT_MIN            12
/** Maximum Page Size Shift (power of 2) */
#define UNIV_PAGE_SIZE_SHIFT_MAX            16
/** Default Page Size Shift (power of 2) */
#define UNIV_PAGE_SIZE_SHIFT_DEF            14

/** Minimum page size InnoDB currently supports. */
#define UNIV_PAGE_SIZE_MIN                  (1 << UNIV_PAGE_SIZE_SHIFT_MIN)
/** Maximum page size InnoDB currently supports. */
#define UNIV_PAGE_SIZE_MAX                  (1 << UNIV_PAGE_SIZE_SHIFT_MAX)
/** Default page size for InnoDB tablespaces. */
#define UNIV_PAGE_SIZE_DEF                  (1 << UNIV_PAGE_SIZE_SHIFT_DEF)

#define PAGE_SIZE_T_SIZE_BITS 17

/** Number of flag bits used to indicate the tablespace page size */
#define FSP_FLAGS_WIDTH_PAGE_SSIZE 4

/** Return the value of the PAGE_SSIZE field */
#define FSP_FLAGS_GET_PAGE_SSIZE(flags)    UNIV_PAGE_SIZE_DEF

class page_size_t {
public:
     /** Constructor from (physical, logical, is_compressed).
     @param[in]    physical    physical (on-disk/zipped) page size
     @param[in]    logical     logical (in-memory/unzipped) page size
     @param[in]    is_compressed   whether the page is compressed */
     page_size_t(uint32 physical, uint32 logical, bool is_compressed) {
       if (physical == 0) {
         physical = UNIV_PAGE_SIZE_DEF;
       }
       if (logical == 0) {
         logical = UNIV_PAGE_SIZE_DEF;
       }
    
       m_physical = static_cast<unsigned>(physical);
       m_logical = static_cast<unsigned>(logical);
       m_is_compressed = static_cast<unsigned>(is_compressed);
    
       ut_ad(physical <= (1 << PAGE_SIZE_T_SIZE_BITS));
       ut_ad(logical <= (1 << PAGE_SIZE_T_SIZE_BITS));
    
       ut_ad(ut_is_2pow(physical));
       ut_ad(ut_is_2pow(logical));
    
       ut_ad(logical <= UNIV_PAGE_SIZE_MAX);
       ut_ad(logical >= physical);
     }

    
    /** Constructor from (fsp_flags).
    @param[in]    fsp_flags   filespace flags */
    explicit page_size_t(uint32_t fsp_flags) {
      uint32 ssize = FSP_FLAGS_GET_PAGE_SSIZE(fsp_flags);
    
      /* If the logical page size is zero in fsp_flags, then use the legacy 16k page size. */
      ssize = (0 == ssize) ? UNIV_PAGE_SIZE_DEF : ssize;
      m_logical = ssize;
      m_is_compressed = false;
      m_physical = m_logical;
    }

    inline void copy_from(const page_size_t &src) {
      m_physical = src.physical();
      m_logical = src.logical();
      m_is_compressed = src.is_compressed();
    }

	inline uint32 physical() const
	{
		ut_ad(m_physical > 0);

		return(m_physical);
	}

	inline uint32 logical() const
	{
		ut_ad(m_logical > 0);
		return(m_logical);
	}

    /** Check whether the page is compressed on disk. */
    inline bool is_compressed() const { return (m_is_compressed); }

private:
     /* For non compressed tablespaces, physical page size is equal to
     the logical page size and the data is stored in buf_page_t::frame
     (and is also always equal to univ_page_size (--innodb-page-size=)).
    
     For compressed tablespaces, physical page size is the compressed
     page size as stored on disk and in buf_page_t::zip::data. The logical
     page size is the uncompressed page size in memory - the size of
     buf_page_t::frame (currently also always equal to univ_page_size
     (--innodb-page-size=)). */
    
     /** Physical page size. */
     unsigned m_physical : PAGE_SIZE_T_SIZE_BITS;
    
     /** Logical page size. */
     unsigned m_logical : PAGE_SIZE_T_SIZE_BITS;
    
     /** Flag designating whether the physical page is compressed, which is
     true IFF the whole tablespace where the page belongs is compressed. */
     unsigned m_is_compressed : 1;

};


#ifdef __cplusplus
}
#endif


#endif  /* _KNL_PAGE_SIZE_H */
