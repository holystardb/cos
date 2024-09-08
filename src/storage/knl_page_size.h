#ifndef _KNL_PAGE_SIZE_H
#define _KNL_PAGE_SIZE_H

#include "cm_type.h"
#include "cm_dbug.h"
#include "cm_util.h"


#ifdef __cplusplus
extern "C" {
#endif


// 16KB: Default Page Size Shift (power of 2)
#define UNIV_PAGE_SIZE_SHIFT_DEF            14
// 128KB: Default Page Size Shift (power of 2)
#define UNIV_PAGE_SIZE_TEMP_SHIFT           17
// 512:
#define UNIV_PAGE_SIZE_REDO_SHIFT           9

#define UNIV_PAGE_SIZE_DEF       (1 << UNIV_PAGE_SIZE_SHIFT_DEF)
#define UNIV_PAGE_SIZE_TEMP      (1 << UNIV_PAGE_SIZE_TEMP_SHIFT)
#define UNIV_PAGE_SIZE_REDO      (1 << UNIV_PAGE_SIZE_REDO_SHIFT)


#define FSP_FLAG_DEFAULT         0
#define FSP_FLAG_SYSTEM          1
#define FSP_FLAG_REDO            2
#define FSP_FLAG_UNDO            3
#define FSP_FLAG_TEMPORARY       4
#define FSP_FLAG_USER_DATA       5


class page_size_t {
public:
     page_size_t(uint32 physical, uint32 logical, bool32 is_compressed) {
       if (physical == 0) {
         physical = UNIV_PAGE_SIZE_DEF;
       }
       if (logical == 0) {
         logical = UNIV_PAGE_SIZE_DEF;
       }

       m_physical = physical;
       m_logical = logical;
       m_is_compressed = is_compressed;

       ut_ad(ut_is_2pow(physical));
       ut_ad(ut_is_2pow(logical));
       ut_ad(logical >= physical);
     }

    explicit page_size_t(uint32 fsp_flags) {
        switch (fsp_flags) {
        case FSP_FLAG_REDO:
            m_logical = UNIV_PAGE_SIZE_REDO;
            m_physical = UNIV_PAGE_SIZE_REDO;
            m_is_compressed = false;
            break;

        case FSP_FLAG_TEMPORARY:
            m_logical = UNIV_PAGE_SIZE_TEMP;
            m_physical = UNIV_PAGE_SIZE_TEMP;
            m_is_compressed = false;
            break;

        case FSP_FLAG_SYSTEM:
        case FSP_FLAG_UNDO:
        case FSP_FLAG_USER_DATA:
        case FSP_FLAG_DEFAULT:
        default:
            m_logical = UNIV_PAGE_SIZE_DEF;
            m_physical = UNIV_PAGE_SIZE_DEF;
            m_is_compressed = false;
            break;
        }
    }

    void copy_from(const page_size_t &src) {
        m_physical = src.physical();
        m_logical = src.logical();
        m_is_compressed = src.is_compressed();
    }

	uint32 physical() const
	{
		ut_ad(m_physical > 0);
		return(m_physical);
	}

	uint32 logical() const
	{
		ut_ad(m_logical > 0);
		return(m_logical);
	}

    // Check whether the page is compressed on disk
    inline bool32 is_compressed() const { return (m_is_compressed); }

private:
     // For non compressed tablespaces:
     //   physical page size is equal to the logical page size

     // For compressed tablespaces:
     //   physical page size is the compressed page size as stored on disk
     //   logical page size is the uncompressed page size in memory

     uint32 m_physical;
     uint32 m_logical;

     // Flag designating whether the physical page is compressed,
     // which is true IFF the whole tablespace where the page belongs is compressed.
     bool32 m_is_compressed;

};


#ifdef __cplusplus
}
#endif


#endif  /* _KNL_PAGE_SIZE_H */
