#ifndef _CM_STRING_H
#define _CM_STRING_H

#include "cm_type.h"
#include "cm_memory.h"
#include "m_ctype.h"

#ifdef __cplusplus
extern "C" {
#endif

class string_t {
private:
    const CHARSET_INFO *m_charset;
    char               *m_ptr;
    uint32              m_length;
    uint32              m_alloced_length;  // should be size_t, but kept uint32 for size reasons
    bool32              m_is_alloced;

public:

    string_t()
        : m_charset(&my_charset_bin), m_ptr(NULL), m_length(0), m_alloced_length(0), m_is_alloced(FALSE)
    {}

    string_t(char *str, uint32 len, const CHARSET_INFO *cs)
        : m_charset(cs), m_ptr(str), m_length(len), m_alloced_length(0), m_is_alloced(FALSE)
    {}

    ~string_t() {
        if (m_is_alloced) {
            m_is_alloced = false;
            m_alloced_length = 0;
            my_free(m_ptr);
            m_ptr = NULL;
            m_length = 0; /* Safety */
        }
    }

    bool32 realloc_memory(uint32 alloc_length);

    uint32 length() const { return m_length; }

    bool is_empty() const { return (m_length == 0); }

    const char *ptr() const { return m_ptr; }
    char *ptr() { return m_ptr; }

    bool32 append(const string_t &s);
    bool32 append(const char *s, uint32 len);
    bool32 append(const char *s, uint32 len, const CHARSET_INFO *cs);
    bool32 append(char chr);
    bool32 append_uint64(uint64 val);
    bool32 append_int64(int64 val);

    int strstr(const string_t &search, uint32 offset = 0) const;
    int strrstr(const string_t &search, uint32 offset = 0) const;

private:
    bool32 needs_conversion(uint32 arg_length, const CHARSET_INFO *from_cs, const CHARSET_INFO *to_cs, uint32 *offset);
    bool32 my_charset_same(const CHARSET_INFO *cs1, const CHARSET_INFO *cs2);

    bool32 uses_buffer_owned_by(const string_t *s) const
    {
        return (s->m_is_alloced && m_ptr >= s->m_ptr && m_ptr < s->m_ptr + s->m_length);
    }

};


#ifdef __cplusplus
}
#endif

#endif  /* _CM_STRING_H */

