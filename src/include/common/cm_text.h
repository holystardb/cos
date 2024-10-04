#ifndef _CM_TEXT_H
#define _CM_TEXT_H

#include "cm_type.h"

#ifdef __cplusplus
extern "C" {
#endif


typedef struct st_text {
    char*  str;
    uint32 len;
} text_t;

/* The Error codes when parsing numeric text */
typedef enum en_num_errno {
    NERR_SUCCESS = 0,  // CM_SUCCESS
    NERR_ERROR,        /* error without concrete reason */
    NERR_INVALID_LEN,
    NERR_NO_DIGIT,
    NERR_UNEXPECTED_CHAR,
    NERR_NO_EXPN_DIGIT,
    NERR_EXPN_WITH_NCHAR,
    NERR_EXPN_TOO_LONG,
    NERR_EXPN_OVERFLOW,
    NERR_OVERFLOW,
    NERR_UNALLOWED_NEG,
    NERR_UNALLOWED_DOT,
    NERR_UNALLOWED_EXPN,
    NERR_MULTIPLE_DOTS,
    NERR_EXPECTED_INTEGER,
    NERR_EXPECTED_POS_INT,
    NERR__NOT_USED__ /* for safely accessing the error information */
} num_errno_t;







#define CM_TEXT_BEGIN(text)  ((text)->str[0])
#define CM_TEXT_FIRST(text)  ((text)->str[0])
#define CM_TEXT_SECOND(text) ((text)->str[1])
#define CM_TEXT_END(text)    ((text)->str[(text)->len - 1])
#define CM_TEXT_SECONDTOLAST(text)      (((text)->len >= 2) ? ((text)->str[(text)->len - 2]) : '\0')
#define CM_NULL_TERM(text)               \
    {                                    \
        (text)->str[(text)->len] = '\0'; \
    }

/** Append a char at the end of text */
#define CM_TEXT_APPEND(text, c) (text)->str[(text)->len++] = (c)


#define CM_IS_EMPTY(text) (((text)->str == NULL) || ((text)->len == 0))
#define CM_IS_QUOTE_CHAR(c1) ((c1)== '\'' || (c1) == '"' || (c1) == '`')
#define CM_IS_QUOTE_STRING(c1, c2) ((c1) == (c2) && CM_IS_QUOTE_CHAR(c1))

#define CM_REMOVE_FIRST(text) \
        do {                      \
            --((text)->len);      \
            ++((text)->str);      \
        } while (0)

    /* n must be less than text->len */
#define CM_REMOVE_FIRST_N(text, n) \
        do {                           \
            uint32 _nn = (uint32)(n);  \
            (text)->len -= _nn;        \
            (text)->str += _nn;        \
        } while (0)

#define CM_REMOVE_LAST(text) \
        {                        \
            --((text)->len);     \
        }
#define CM_IS_EMPTY_STR(str)     (((str) == NULL) || ((str)[0] == 0))
#define CM_STR_REMOVE_FIRST(str) \
        {                            \
            (str)++;                 \
        }
#define CM_STR_REMOVE_FIRST_N(str, cnt) \
        do {                         \
            uint32 _cnt = cnt;       \
            while (_cnt > 0) {        \
                (str)++;             \
                _cnt--;              \
            }                        \
        } while (0)
#define CM_STR_GET_FIRST(str, out) \
        {                              \
            (out) = (str)[0];          \
        }
#define CM_STR_POP_FIRST(str, out)      \
        do {                                \
            CM_STR_GET_FIRST((str), (out)); \
            CM_STR_REMOVE_FIRST((str));     \
        } while (0)



    /** Clear all characters of the text */
#define CM_TEXT_CLEAR(text) (text)->len = 0

#define UPPER(c) (((c) >= 'a' && (c) <= 'z') ? ((c) - 32) : (c))
#define LOWER(c) (((c) >= 'A' && (c) <= 'Z') ? ((c) + 32) : (c))

#define CM_IS_DIGIT(c)         ((c) >= '0' && ((c) <= '9'))
#define CM_IS_NONZERO_DIGIT(c) ((c) > '0' && ((c) <= '9'))
#define CM_IS_EXPN_CHAR(c)     ((c) == 'e' || ((c) == 'E'))
#define CM_IS_SIGN_CHAR(c)     ((c) == '-' || ((c) == '+'))

/** Hex chars are any of: 0 1 2 3 4 5 6 7 8 9 a b c d e f A B C D E F */
#define CM_IS_HEX(c)   ((bool32)isxdigit(c))
#define CM_IS_ASCII(c) ((c) >= 0)

/** An enclosed char must be an ASCII char */
#define CM_IS_VALID_ENCLOSED_CHAR(c) CM_IS_ASCII(((char)(c)))

#define CM_IS_ZERO(c)    ((c) == '0')
#define CM_IS_DOT(c)     ((c) == '.')
#define CM_IS_NONZERO(c) ((c) != '0')
#define CM_IS_COLON(c)   ((c) == ':')

/** Convert a digital char into numerical digit */
#define CM_C2D(c) ((c) - '0')

#define CM_DEFAULT_DIGIT_RADIX  10
#define CM_HEX_DIGIT_RADIX      16
#define SIGNED_LLONG_MAX  "9223372036854775807"
#define SIGNED_LLONG_MIN  "-9223372036854775808"
#define UNSIGNED_LLONG_MAX  "18446744073709551615"






static inline void cm_rtrim_text(text_t* text)
{
    int32 index;

    if (text->str == NULL) {
        text->len = 0;
        return;
    } else if (text->len == 0) {
        return;
    }

    index = (int32)text->len - 1;
    while (index >= 0) {
        if ((uchar)text->str[index] > (uchar)' ') {
            text->len = (uint32)(index + 1);
            return;
        }

        --index;
    }
}

static inline void cm_ltrim_text(text_t* text)
{
    if (text->str == NULL) {
        text->len = 0;
        return;
    } else if (text->len == 0) {
        return;
    }

    while (text->len > 0) {
        if ((uchar)*text->str > ' ') {
            break;
        }
        text->str++;
        text->len--;
    }
}

static inline void cm_trim_text(text_t* text)
{
    cm_ltrim_text(text);
    cm_rtrim_text(text);
}

static inline void cm_str2text(char* str, text_t* text)
{
    text->str = str;
    text->len = (str == NULL) ? 0 : (uint32)strlen(str);
}

/* Locate first occurrence of character in string
 * Returns the first position of the character in the text.
 * If the character is not found return -1
 * */
static inline int32 cm_text_chr(const text_t *text, char c)
{
    for (uint32 i = 0; i < text->len; i++) {
        if (text->str[i] == c) {
            return (int32)i;
        }
    }

    return -1;
}


// case sensitive
static inline bool32 cm_text_equal(const text_t *text1, const text_t *text2)
{
    uint32 i;

    if (text1->len != text2->len) {
        return FALSE;
    }

    for (i = 0; i < text1->len; i++) {
        if (text1->str[i] != text2->str[i]) {
            return FALSE;
        }
    }

    return TRUE;
}

// case insensitive, all text uses upper mode to compare.
static inline bool32 cm_text_ncase_equal(const text_t *text1, const text_t *text2)
{
    uint32 i;

    if (text1->len != text2->len) {
        return FALSE;
    }

    for (i = 0; i < text1->len; i++) {
        if (UPPER(text1->str[i]) != UPPER(text2->str[i])) {
            return FALSE;
        }
    }

    return TRUE;
}

static inline bool32 cm_text_str_equal(const text_t *text, const char *str)
{
    uint32 i;

    for (i = 0; i < text->len; i++) {
        if (text->str[i] != str[i] || str[i] == '\0') {
            return FALSE;
        }
    }

    return (str[text->len] == '\0');
}

static inline bool32 cm_text_str_contain_ncase_equal(const text_t *text, const char *str, const uint32 str_len)
{
    uint32 i;

    if (text->len < str_len) {
        return FALSE;
    }

    for (i = 0; i < str_len; i++) {
        if (UPPER(text->str[i]) != UPPER(str[i])) {
            return FALSE;
        }
    }

    return TRUE;
}

static inline bool32 cm_char_in_text(char c, const text_t *set)
{
    return memchr((void *)set->str, c, set->len) != NULL;
}





#ifdef __cplusplus
}
#endif

#endif
