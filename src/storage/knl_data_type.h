#ifndef _KNL_DATA_TYPE_H
#define _KNL_DATA_TYPE_H

#include "cm_type.h"
#include "cm_dbug.h"
#include "m_ctype.h"
#include "knl_server.h"

// The 'MAIN TYPE' of a column
typedef enum en_data_type {
    DATA_MISSING = 0,  // missing column

    // STRING
    DATA_STRING = 1,
    DATA_VARCHAR = 2,
    DATA_CHAR = 3,
    DATA_BINARY = 4,  // fix binary
    DATA_VARBINARY = 5,
    DATA_RAW = 6,
    //
    DATA_CLOB = 7,
    DATA_BLOB1 = 8,

    // NUMBER
    DATA_NUMBER = 10,
    DATA_INTEGER = 11,
    DATA_BIGINT = 12,
    DATA_SMALLINT = 13,
    DATA_TINYINT = 14,
    DATA_DECIMAL = 15, // decimal number stored as an ASCII string
    DATA_FLOAT = 16,
    DATA_DOUBLE = 17,
    DATA_REAL = 18,
    DATA_BOOLEAN = 19,

    // TIMESTAMP
    DATA_TIMESTAMP = 20,
    DATA_TIMESTAMP_TZ = 21,
    DATA_TIMESTAMP_LTZ = 22,  // with local time zone
    DATA_DATE = 23,
    DATA_DATETIME = 24,
    DATA_TIME = 25,
    DATA_INTERVAL = 26,
    DATA_INTERVAL_YM = 27, //interval YEAR TO MONTH
    DATA_INTERVAL_DS = 28, // interval DAY TO SECOND

    DATA_CURSOR = 31,  // resultset, for stored procedure
    DATA_COLUMN = 32,  // column type, internal used
    DATA_RECORD = 33,
    DATA_COLLECTION = 34,
    DATA_OBJECT = 35,
    DATA_ARRAY = 36,

    DATA_TYPE_MAX = 63,
} data_type_t;


/* Precise data types for system columns and the length of those columns;
NOTE: the values must run from 0 up in the order given! All codes must be less than 256 */
#define DATA_ROW_ID         0   /* row id: a 64-bit integer */
#define DATA_ROW_ID_LEN     8   /* stored length for row id */

#define DATA_TRX_ID         1   /* transaction id: 6 bytes */
#define DATA_TRX_ID_LEN     6

#define DATA_ROLL_PTR       2   /* rollback data pointer: 7 bytes */
#define DATA_ROLL_PTR_LEN   7

#define	DATA_N_SYS_COLS     3   /* number of system columns defined above */

#define DATA_FTS_DOC_ID     3   /* Used as FTS DOC ID column */

#define DATA_SYS_PRTYPE_MASK 0xF /* mask to extract the above from prtype */

/* Flags ORed to the precise data type */
#define DATA_NOT_NULL       256     /* column is declared as NOT NULL */
#define DATA_UNSIGNED       512     /* an unsigned integer type */
#define DATA_BINARY_TYPE    1024    /* if the data type is a binary character string */

/* the column is true VARCHAR uses 2 bytes to store the data len;
   shorter VARCHARs uses only 1 byte */
#define DATA_LONG_TRUE_VARCHAR 4096




/* We now support 15 bits (up to 32767) collation number */
#define MAX_CHAR_COLL_NUM   32767

/* Mask to get the Charset Collation number (0x7fff) */
#define CHAR_COLL_MASK      MAX_CHAR_COLL_NUM

/* Maximum multi-byte character length in bytes, plus 1 */
#define DATA_MBMAX          5

/* Pack mbminlen, mbmaxlen to mbminmaxlen. */
#define DATA_MBMINMAXLEN(mbminlen, mbmaxlen)    ((mbmaxlen) * DATA_MBMAX + (mbminlen))

// Get mbminlen from mbminmaxlen.
// Cast the result of UNIV_EXPECT to uint32 because in GCC it returns a long.
#define DATA_MBMINLEN(mbminmaxlen)              ((uint32)EXPECT(((mbminmaxlen) % DATA_MBMAX),  1))

/* Get mbmaxlen from mbminmaxlen. */
#define DATA_MBMAXLEN(mbminmaxlen)              ((uint32) ((mbminmaxlen) / DATA_MBMAX))


// Structure for an SQL data type
typedef struct st_dtype {
    uint8  mtype         : 6;  // main data type
    uint8  is_null       : 1;  // null or not null
    uint8  is_compressed : 1;

    union {
        struct {
            uint8  precision;
            uint8  scale;
            uint8  aligned1;
        };
        struct {
            uint16 len;
            uint8  mbminmaxlen : 5; // minimum and maximum length of a character, in bytes
            uint8  aligned2    : 3;
        };
    };
} dtype_t;




extern inline bool32 dtype_is_string_type(uint32 mtype)
{
    if (mtype >= DATA_STRING && mtype <= DATA_RAW) {
        return TRUE;
    }

    return FALSE;
}

extern inline bool32 dtype_is_binary_string_type(uint32 mtype, uint32 prtype)
{
    if (mtype == DATA_VARBINARY || mtype == DATA_BINARY
        || (mtype == DATA_BLOB1 && (prtype & DATA_BINARY_TYPE))) {
        return TRUE;
    }

    return FALSE;
}

// Gets the MySQL charset-collation code for MySQL string types.
// return MySQL charset-collation code
extern inline uint32 dtype_get_charset_coll(uint32 prtype)
{
    return((prtype >> 16) & CHAR_COLL_MASK);
}

// Get the variable length bounds of the given character set.
extern inline void innobase_get_cset_width(
    uint32  cset,       /*!< in: MySQL charset-collation code */
    uint32* mbminlen,   /*!< out: minimum length of a char (in bytes) */
    uint32* mbmaxlen)   /*!< out: maximum length of a char (in bytes) */
{
    CHARSET_INFO* cs;
    ut_ad(cset <= MAX_CHAR_COLL_NUM);

    cs = &all_charsets[cset];
    if (cs) {
        *mbminlen = cs->mbminlen;
        *mbmaxlen = cs->mbmaxlen;
        ut_ad(*mbminlen < DATA_MBMAX);
        ut_ad(*mbmaxlen < DATA_MBMAX);
    } else {
        *mbminlen = *mbmaxlen = 0;
    }
}

extern inline void dtype_get_mblen(
    uint32  mtype,   /*!< in: main type */
    uint32  prtype,  /*!< in: precise type (and collation) */
    uint32* mbminlen,/*!< out: minimum length of a multi-byte character */
    uint32* mbmaxlen)/*!< out: maximum length of a multi-byte character */
{
    if (dtype_is_string_type(mtype)) {
        innobase_get_cset_width(dtype_get_charset_coll(prtype), mbminlen, mbmaxlen);
        ut_ad(*mbminlen <= *mbmaxlen);
        ut_ad(*mbminlen < DATA_MBMAX);
        ut_ad(*mbmaxlen < DATA_MBMAX);
    } else {
        *mbminlen = *mbmaxlen = 0;
    }
}

extern inline void dtype_set_mbminmaxlen(
    dtype_t* type,
    uint32   mbminlen,  /*!< in: minimum length of a char, in bytes, or 0 if this is not a character type */
    uint32   mbmaxlen)  /*!< in: maximum length of a char, in bytes, or 0 if this is not a character type */
{
    ut_ad(mbminlen < DATA_MBMAX);
    ut_ad(mbmaxlen < DATA_MBMAX);
    ut_ad(mbminlen <= mbmaxlen);

    type->mbminmaxlen = DATA_MBMINMAXLEN(mbminlen, mbmaxlen);
}

extern inline uint32 dtype_get_mbminlen(const dtype_t* type)
{
    return(DATA_MBMINLEN(type->mbminmaxlen));
}

extern inline uint32 dtype_get_mbmaxlen(const dtype_t* type)
{
    return(DATA_MBMAXLEN(type->mbminmaxlen));
}

extern inline void dtype_set_mblen(dtype_t* type)
{
    uint32 mbminlen;
    uint32 mbmaxlen;

    dtype_get_mblen(type->mtype, type->precision, &mbminlen, &mbmaxlen);
    dtype_set_mbminmaxlen(type, mbminlen, mbmaxlen);

    ut_ad(dtype_get_mbminlen(type) <= dtype_get_mbmaxlen(type));
}

extern inline void dtype_set(dtype_t* type, uint32 mtype, uint32 prtype, uint32 len)
{
    type->mtype = mtype;
    type->precision = prtype;
    type->len = len;

    dtype_set_mblen(type);
}

extern inline uint32 dtype_get_mtype(const dtype_t* type)
{
    return(type->mtype);
}

extern inline uint32 dtype_get_prtype(const dtype_t* type)
{
    return(type->precision);
}

extern inline uint32 dtype_get_len(const dtype_t* type)
{
    return(type->len);
}




#endif  /* _KNL_DATA_TYPE_H */
