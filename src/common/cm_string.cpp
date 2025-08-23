#include "cm_string.h"
#include "cm_util.h"
#include "m_string.h"

bool32 string_t::realloc_memory(uint32 alloc_length)
{
    uint32 len = ut_align8(alloc_length + 1);

    if (len <= alloc_length)
        return FALSE; /* Overflow */

    if (m_alloced_length < len) {  // Available bytes are not enough.
        char *new_ptr;
        if (m_is_alloced) {
            if (!(new_ptr = (char *)my_realloc(m_ptr, len)))
                return FALSE;  // Signal error

        } else if ((new_ptr = (char *)ut_malloc(len))) {
            if (m_length > len - 1) {
                m_length = 0;
            }
            if (m_length > 0) {
                memcpy(new_ptr, m_ptr, m_length);
            }
            new_ptr[m_length] = 0;
            m_is_alloced = true;
        } else {
            return FALSE;  // Signal error
        }
        m_ptr = new_ptr;
        m_alloced_length = (uint32)(len);
    }
    m_ptr[alloc_length] = 0;  // This make other funcs shorter

    return TRUE;
}

bool32 string_t::append(const string_t &s)
{
    uint32 alloc_length = s.length();
    if (alloc_length) {
        DBUG_ASSERT(!this->uses_buffer_owned_by(&s));
        DBUG_ASSERT(!s.uses_buffer_owned_by(this));

        alloc_length = ut_align8(alloc_length + 1);
        alloc_length = (m_is_alloced && m_alloced_length < alloc_length)
            ? alloc_length + (m_length / 4) : alloc_length;

        if (!realloc_memory(alloc_length)) {
            return FALSE;
        }

        memcpy(m_ptr + m_length, s.ptr(), s.length());
        m_length += s.length();
    }
    return TRUE;
}

bool32 string_t::append(char chr)
{
    if (m_length < m_alloced_length) {
        m_ptr[m_length++] = chr;
    } else {
        if (!realloc_memory(m_length + 1))
            return FALSE;

        m_ptr[m_length++] = chr;
    }
    return TRUE;
}

//Append an ASCII string to the a string of the current character set
bool32 string_t::append(const char *s, uint32 arg_length)
{
    if (!arg_length)
        return FALSE;

    //For an ASCII incompatible string, e.g. UCS-2, we need to convert
    if (m_charset->mbminlen > 1) {
        uint32 add_length = arg_length * m_charset->mbmaxlen;
        uint dummy_errors;

        if (realloc_memory(m_length + add_length))
            return FALSE;

        m_length += my_convert(m_ptr + m_length, add_length, m_charset, s,
            arg_length, &my_charset_latin1, &dummy_errors);

        return TRUE;
    }

    //For an ASCII compatinble string we can just append.
    if (realloc_memory(m_length + arg_length))
        return FALSE;

    memcpy(m_ptr + m_length, s, arg_length);
    m_length += arg_length;

    return TRUE;
}

bool32 string_t::my_charset_same(const CHARSET_INFO *cs1, const CHARSET_INFO *cs2)
{
    return ((cs1 == cs2) || !strcmp(cs1->csname, cs2->csname));
}

/*
  Checks that the source string can be just copied to the destination string without conversion.

  SYNPOSIS

  needs_conversion()
  arg_length		Length of string to copy.
  from_cs		Character set to copy from
  to_cs			Character set to copy to
  uint32 *offset	Returns number of unaligned characters.

  RETURN
   0  No conversion needed
   1  Either character set conversion or adding leading  zeros
      (e.g. for UCS-2) must be done

  NOTE
  to_cs may be NULL for "no conversion" if the system variable character_set_results is NULL.
*/
bool32 string_t::needs_conversion(uint32 arg_length, const CHARSET_INFO *from_cs,
                              const CHARSET_INFO *to_cs, uint32 *offset)
{
    *offset = 0;
    if (!to_cs || (to_cs == &my_charset_bin) || (to_cs == from_cs) || my_charset_same(from_cs, to_cs)
        || (from_cs == &my_charset_bin &&  (!(*offset = (arg_length % to_cs->mbminlen))))) {
        return FALSE;
    }

    return TRUE;
}

bool32 string_t::append(const char *s, uint32 arg_length, const CHARSET_INFO *cs)
{
    uint32 offset;

    if (needs_conversion(arg_length, cs, m_charset, &offset)) {
        size_t add_length;
        if ((cs == &my_charset_bin) && offset) {
            DBUG_ASSERT(m_charset->mbminlen > offset);
            offset = m_charset->mbminlen - offset;  // How many characters to pad
            add_length = arg_length + offset;

            if (!realloc_memory(m_length + add_length))
                return FALSE;

            memset(m_ptr + m_length, 0, offset);
            memcpy(m_ptr + m_length + offset, s, arg_length);
            m_length += add_length;

            return TRUE;
        }

        add_length = arg_length / cs->mbminlen * m_charset->mbmaxlen;
        uint dummy_errors;

        if (!realloc_memory(m_length + add_length))
            return FALSE;

        m_length += my_convert(m_ptr + m_length, add_length, m_charset, s, arg_length, cs, &dummy_errors);
    } else {
        if (!realloc_memory(m_length + arg_length))
            return FALSE;

        memcpy(m_ptr + m_length, s, arg_length);
        m_length += arg_length;
    }

    return TRUE;
}

bool32 string_t::append_uint64(uint64 val)
{
    if (!realloc_memory(m_length + MAX_BIGINT_WIDTH + 2))
        return FALSE;

    char *end = longlong10_to_str(val, m_ptr + m_length, 10);
    m_length = end - m_ptr;

    return TRUE;
}

bool32 string_t::append_int64(int64 val)
{
    if (!realloc_memory(m_length + MAX_BIGINT_WIDTH + 2))
        return FALSE; /* purecov: inspected */

    char *end = longlong10_to_str(val, m_ptr + m_length, -10);
    m_length = end - m_ptr;

    return TRUE;
}

int string_t::strstr(const string_t &s, uint32 offset) const
{
    if (s.length() + offset <= m_length) {
        if (!s.length())
            return ((int)offset);  // Empty string is always found

        const char *str = m_ptr + offset;
        const char *search = s.ptr();
        const char *end = m_ptr + m_length - s.length() + 1;
        const char *search_end = s.ptr() + s.length();
skip:
        while (str != end) {
            if (*str++ == *search) {
                const char *i = str;
                const char *j = search + 1;
                while (j != search_end) {
                    if (*i++ != *j++) {
                        goto skip;
                    }
                }
                return (int)(str - m_ptr) - 1;
            }
        }
    }

    return -1;
}

// Search string from end. Offset is offset to the end of string
int string_t::strrstr(const string_t &s, uint32 offset) const
{
    if (s.length() <= offset && offset <= m_length) {
        if (!s.length()) {
            return static_cast<int>(offset);  // Empty string is always found
        }

        const char *str = m_ptr + offset - 1;
        const char *search = s.ptr() + s.length() - 1;

        const char *end = m_ptr + s.length() - 2;
        const char *search_end = s.ptr() - 1;

skip:
        while (str != end) {
            if (*str-- == *search) {
                const char *i = str;
                const char *j = search - 1;

                while (j != search_end) {
                    if (*i-- != *j--) {
                        goto skip;
                    }
                }
                return (int)(i - m_ptr) + 1;
            }
        }
    }

    return -1;
}


