#ifndef _KNL_PAGE_ID_H
#define _KNL_PAGE_ID_H

#include "cm_type.h"
#include "knl_server.h"

#ifdef __cplusplus
extern "C" {
#endif

class page_id_t {
public:
    page_id_t() : m_space(), m_page_no(), m_fold() {}

    page_id_t(space_id_t space, page_no_t page_no)
        : m_space(space), m_page_no(page_no), m_fold(UINT32_UNDEFINED) {}

    page_id_t(const page_id_t &) = default;

    /** Retrieve the tablespace id.
    @return tablespace id */
    inline space_id_t space_id() const { return (m_space); }

    /** Retrieve the page number.
    @return page number */
    inline page_no_t page_no() const { return (m_page_no); }

    /** Retrieve the fold value.
    @return fold value */
    inline uint32 fold() const {
        /* Initialize m_fold if it has not been initialized yet. */
        if (m_fold == UINT32_UNDEFINED) {
            m_fold = (m_space << 20) + m_space + m_page_no;
            ut_ad(m_fold != UINT32_UNDEFINED);
        }

        return (m_fold);
    }

    /** Copy the values from a given page_id_t object.
    @param[in]    src page id object whose values to fetch */
    inline void copy_from(const page_id_t &src) {
        m_space = src.space_id();
        m_page_no = src.page_no();
        m_fold = src.fold();
    }

    /** Reset the values from a (space, page_no).
    @param[in]    space   tablespace id
    @param[in]    page_no page number */
    inline void reset(space_id_t space, page_no_t page_no) {
        m_space = space;
        m_page_no = page_no;
        m_fold = UINT32_UNDEFINED;
    }

    /** Reset the page number only.
    @param[in]    page_no page number */
    void set_page_no(page_no_t page_no) {
        m_page_no = page_no;
        m_fold = UINT32_UNDEFINED;
    }

    /** Check if a given page_id_t object is equal to the current one.
    @param[in]    a   page_id_t object to compare
    @return true if equal */
    inline bool equals_to(const page_id_t &a) const {
        return (a.space_id() == m_space && a.page_no() == m_page_no);
    }

private:
    /** Tablespace id. */
    space_id_t m_space;

    /** Page number. */
    page_no_t m_page_no;

    /** A fold value derived from m_space and m_page_no,
    used in hashing. */
    mutable uint32 m_fold;

    /* Disable implicit copying. */
    void operator=(const page_id_t &) = delete;
};



#ifdef __cplusplus
}
#endif


#endif  /* _KNL_PAGE_ID_H */
