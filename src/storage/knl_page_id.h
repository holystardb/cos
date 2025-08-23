#ifndef _KNL_PAGE_ID_H
#define _KNL_PAGE_ID_H

#include "cm_type.h"
#include "knl_server.h"

#ifdef __cplusplus
extern "C" {
#endif

class page_id_t {
public:
    page_id_t() : m_space_id(), m_page_no(), m_fold() {}

    page_id_t(space_id_t space_id, page_no_t page_no)
        : m_space_id(space_id), m_page_no(page_no), m_fold(UINT32_UNDEFINED) {}

    page_id_t(const page_id_t &) = default;

    uint32 get_space_id() const { return (m_space_id); }
    uint32 get_page_no() const { return (m_page_no); }

    uint32 fold() const {
        /* Initialize m_fold if it has not been initialized yet. */
        if (m_fold == UINT32_UNDEFINED) {
            m_fold = (m_space_id << 20) + m_space_id + m_page_no;
            ut_ad(m_fold != UINT32_UNDEFINED);
        }

        return (m_fold);
    }

    // Copy the values from a given page_id_t object
    void copy_from(const page_id_t &src) {
        m_space_id = src.get_space_id();
        m_page_no = src.get_page_no();
        m_fold = src.fold();
    }

    // Reset the values from a (space_id, page_no)
    void reset(space_id_t space_id, page_no_t page_no) {
        m_space_id = space_id;
        m_page_no = page_no;
        m_fold = UINT32_UNDEFINED;
    }

    // Reset the page number only
    void set_page_no(page_no_t page_no) {
        m_page_no = page_no;
        m_fold = UINT32_UNDEFINED;
    }

    // Check if a given page_id_t object is equal to the current one
    bool32 equals_to(const page_id_t& a) const {
        return (a.get_space_id() == m_space_id && a.get_page_no() == m_page_no);
    }

private:
    uint32 m_space_id;
    uint32 m_page_no;

    /** A fold value derived from m_space and m_page_no, used in hashing. */
    mutable uint32 m_fold;

    /* Disable implicit copying. */
    void operator=(const page_id_t &) = delete;
};



#ifdef __cplusplus
}
#endif


#endif  /* _KNL_PAGE_ID_H */
