#ifndef _KNL_DATA_TYPE_H
#define _KNL_DATA_TYPE_H

#include "cm_type.h"
#include "cm_space.h"
#include "cm_page.h"

/*-------------------------------------------*/
/* The 'MAIN TYPE' of a column */
#define DATA_MISSING        0   /* missing column */
#define DATA_VARCHAR        1   /* character varying */
#define DATA_CHAR           2   /* fixed length character */
#define DATA_INT            3   /* integer: can be any size 1 - 8 bytes */
#define DATA_FLOAT          4
#define DATA_DOUBLE         5
#define DATA_DECIMAL        6   /* decimal number stored as an ASCII string */
#define DATA_FIXBINARY      7   /* binary string of fixed length */
#define DATA_BINARY         8   /* binary string */
#define DATA_BLOB           9   /* binary large object, or a TEXT type */
#define DATA_SYS_CHILD      10  /* address of the child page in node pointer */
#define DATA_SYS            11  /* system column */
#define DATA_MTYPE_MAX      63  /* dtype_store_for_order_and_null_size() requires the values are <= 63 */



/* Precise data types for system columns and the length of those columns;
NOTE: the values must run from 0 up in the order given! All codes must
be less than 256 */
#define DATA_ROW_ID         0   /* row id: a 64-bit integer */
#define DATA_ROW_ID_LEN     8   /* stored length for row id */

#define DATA_TRX_ID         1   /* transaction id: 6 bytes */
#define DATA_TRX_ID_LEN     6

#define DATA_ROLL_PTR       2   /* rollback data pointer: 7 bytes */
#define DATA_ROLL_PTR_LEN   7

#define	DATA_N_SYS_COLS 3	/* number of system columns defined above */

#define DATA_FTS_DOC_ID	3	/* Used as FTS DOC ID column */

#define DATA_SYS_PRTYPE_MASK 0xF /* mask to extract the above from prtype */

/* Flags ORed to the precise data type */
#define DATA_NOT_NULL	256	/* this is ORed to the precise type when
				the column is declared as NOT NULL */
#define DATA_UNSIGNED	512	/* this id ORed to the precise type when
				we have an unsigned integer type */
#define	DATA_BINARY_TYPE 1024	/* if the data type is a binary character
				string, this is ORed to the precise type:
				this only holds for tables created with
				>= MySQL-4.0.14 */
/* #define	DATA_NONLATIN1	2048 This is a relic from < 4.1.2 and < 5.0.1.
				In earlier versions this was set for some
				BLOB columns.
*/
#define	DATA_LONG_TRUE_VARCHAR 4096	/* this is ORed to the precise data
				type when the column is true VARCHAR where
				MySQL uses 2 bytes to store the data len;
				for shorter VARCHARs MySQL uses only 1 byte */



/* Structure for an SQL data type.
If you add fields to this structure, be sure to initialize them everywhere.
This structure is initialized in the following functions:
dtype_set()
dtype_read_for_order_and_null_size()
dtype_new_read_for_order_and_null_size()
sym_tab_add_null_lit() */

typedef struct st_dtype {
	unsigned    prtype:32; /*!< precise type; MySQL data
					type, charset code, flags to indicate nullability,
					signedness, whether this is a binary string, whether this is
					a true VARCHAR where MySQL uses 2 bytes to store the length */
	unsigned    mtype:8; /*!< main data type */

	/* the remaining fields do not affect alphabetical ordering: */

	unsigned    len:16; /*!< length; for MySQL data this
					is field->pack_length(),
					except that for a >= 5.0.3
					type true VARCHAR this is the maximum byte length of the string data (in addition to
					the string, MySQL uses 1 or 2 bytes to store the string length) */
	unsigned	mbminmaxlen:5; /*!< minimum and maximum length of a character, in bytes;
					DATA_MBMINMAXLEN(mbminlen,mbmaxlen);
					mbminlen=DATA_MBMINLEN(mbminmaxlen);
					mbmaxlen=DATA_MBMINLEN(mbminmaxlen) */
} dtype_t;





#endif  /* _KNL_DATA_TYPE_H */
