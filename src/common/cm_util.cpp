#include "cm_util.h"
#include "cm_dbug.h"



duint64 ut_duint64_zero = {0, 0};
duint64 ut_duint64_max = {0xFFFFFFFF, 0xFFFFFFFF};



/***********************************************************************************************
*                                      math or compare                                         *
***********************************************************************************************/

/* out: logarithm in the base 2, rounded upward */
inline uint32 ut_2_log(uint32 n) /* in: number != 0 */
{
    uint32 res;
    res = 0;
    n = n - 1;

    for (;;) {
        n = n / 2;
        if (n == 0) {
            break;
        }
        res++;
    }

    return(res + 1);
}

inline uint32 ut_2_exp(uint32 n)
{
    return(1 << n);
}

/** Calculates fast the number rounded up to the nearest power of 2.
 @return first power of 2 which is >= n */
inline uint64 ut_2_power_up(uint64 n) /*!< in: number != 0 */
{
    uint64 res = 1;

    ut_ad(n > 0);

    while (res < n) {
        res = res * 2;
    }

    return (res);
}


/***********************************************************************************************
*                                      align                                                   *
***********************************************************************************************/


/************************************************************
The following function calculates the value of an integer n rounded
to the least product of align_no which is >= n. align_no has to be a power of 2. */
inline uint32 ut_uint32_align_up(
    uint32 n,            /* in: number to be rounded */
    uint32 align_no)      /* in: align by this number */
{
    ut_ad(align_no > 0);
    ut_ad(ut_is_2pow(align_no));

    return((n + align_no - 1) & ~(align_no - 1));
}

/************************************************************
The following function calculates the value of an integer n rounded
to the biggest product of align_no which is <= n. align_no has to be a power of 2. */
inline uint32 ut_uint32_align_down(
    uint32 n,              /* in: number to be rounded */
    uint32 align_no)       /* in: align by this number */
{
    ut_ad(align_no > 0);
    ut_ad(ut_is_2pow(align_no));

    return(n & ~(align_no - 1));
}


/*************************************************************
The following function rounds up a pointer to the nearest aligned address. */
inline void* ut_align_up(
    void* ptr,            /* in: pointer */
    uint32 align_no)          /* in: align by this number */
{
    ut_ad(align_no > 0);
    ut_ad(ut_is_2pow(align_no));
    ut_ad(ptr);

    return((void*)((((uint64)ptr) + align_no - 1) & ~((uint64)(align_no - 1))));
}

inline uint32 ut_align_offset(
    const void* ptr,       /*!< in: pointer */
    uint32      align_no)  /*!< in: align by this number */
{
    ut_ad(align_no > 0);
    ut_ad(ut_is_2pow(align_no));
    ut_ad(ptr);

    return (((uint64)ptr) & (align_no - 1));
}

/*************************************************************
The following function rounds down a pointer to the nearest aligned address. */
inline void* ut_align_down(
    void* ptr,            /* in: pointer */
    uint32 align_no)          /* in: align by this number */
{
    ut_ad(align_no > 0);
    ut_ad(ut_is_2pow(align_no));
    ut_ad(ptr);

    return((void*)((((uint64)ptr)) & ~((uint64)(align_no - 1))));
}

//Rounds a 64-bit integer downward to a multiple of a power of 2.
//return: rounded value
inline uint64 ut_uint64_align_down(
    uint64 n, //in: number to be rounded
    uint32 align_no) //in: align by this number which must be a power of 2
{
    ut_ad(align_no > 0);
    ut_ad(ut_is_2pow(align_no));

    return(n & ~((uint64) align_no - 1));
}

//Rounds ib_uint64_t upward to a multiple of a power of 2.
//return: rounded value
inline uint64 ut_uint64_align_up(
    uint64 n, //in: number to be rounded
    uint32 align_no) //in: align by this number which must be a power of 2
{
    uint64 align_1 = (uint64) align_no - 1;

    ut_ad(align_no > 0);
    ut_ad(ut_is_2pow(align_no));

    return((n + align_1) & ~align_1);
}

// Gets the nth bit of a uint8. */
// out: TRUE if nth bit is 1; 0th bit is defined to be the least significant
inline bool32 ut_bit8_get_nth(
    uint8 a,    /* in: uint8 */
    uint32 n)    /* in: nth bit requested */
{
    ut_ad(n < 8 * sizeof(uint8));
    ut_ad(TRUE == 1);

    return(1 & (a >> n));
}

// Sets the nth bit of a uint8. */
// out: the uint32 with the bit set as requested */
inline uint8 ut_bit8_set_nth(
    uint8 a,    /* in: uint8 */
    uint32 n,    /* in: nth bit requested */
    bool32 val)    /* in: value for the bit to set */
{
    ut_ad(n < 8 * sizeof(uint8));
    ut_ad(TRUE == 1);

    if (val) {
        return((1 << n) | a);
    } else {
        return(~(1 << n) & a);
    }
}

// Gets the nth bit of a uint32. */
// out: TRUE if nth bit is 1; 0th bit is defined to be the least significant
inline bool32 ut_bit32_get_nth(
    uint32 a,    /* in: uint32 */
    uint32 n)    /* in: nth bit requested */
{
    ut_ad(n < 8 * sizeof(uint32));
    ut_ad(TRUE == 1);

    return(1 & (a >> n));
}

// Sets the nth bit of a uint32. */
// out: the uint32 with the bit set as requested */
inline uint32 ut_bit32_set_nth(
    uint32 a,    /* in: uint32 */
    uint32 n,    /* in: nth bit requested */
    bool32 val)    /* in: value for the bit to set */
{
    ut_ad(n < 8 * sizeof(uint32));
    ut_ad(TRUE == 1);

    if (val) {
        return((1 << n) | a);
    } else {
        return(~(1 << n) & a);
    }
}

// Gets the nth bit of a uint64. */
// out: TRUE if nth bit is 1; 0th bit is defined to be the least significant
inline bool32 ut_bit64_get_nth(
    uint64 a,    /* in: uint64 */
    uint32 n)    /* in: nth bit requested */
{
    ut_ad(n < 8 * sizeof(uint64));
    ut_ad(TRUE == 1);

    return(1 & (a >> n));
}

// Sets the nth bit of a uint64. */
// out: the uint32 with the bit set as requested */
inline uint64 ut_bit64_set_nth(
    uint64 a,    /* in: uint64 */
    uint32 n,    /* in: nth bit requested */
    bool32 val)    /* in: value for the bit to set */
{
    ut_ad(n < 8 * sizeof(uint64));
    ut_ad(TRUE == 1);

    if (val) {
        return(((uint64)1 << n) | a);
    } else {
        return(~(1 << n) & a);
    }
}

inline uint32 ut_raw_to_hex(
    const void* raw,        /*!< in: raw data */
    uint32      raw_size,   /*!< in: "raw" length in bytes */
    char*       hex,        /*!< out: hex string */
    uint32      hex_size)   /*!< in: "hex" size in bytes */
{
#ifdef WORDS_BIGENDIAN
#define MK_UINT16(a, b) (((uint16) (a)) << 8 | (uint16) (b))
#define UINT16_GET_A(u) ((unsigned char) ((u) >> 8))
#define UINT16_GET_B(u) ((unsigned char) ((u) & 0xFF))
#else /* WORDS_BIGENDIAN */
#define MK_UINT16(a, b) (((uint16) (b)) << 8 | (uint16) (a))
#define UINT16_GET_A(u) ((unsigned char) ((u) & 0xFF))
#define UINT16_GET_B(u) ((unsigned char) ((u) >> 8))
#endif /* WORDS_BIGENDIAN */

#define MK_ALL_UINT16_WITH_A(a) \
    MK_UINT16(a, '0'),          \
    MK_UINT16(a, '1'),          \
    MK_UINT16(a, '2'),          \
    MK_UINT16(a, '3'),          \
    MK_UINT16(a, '4'),          \
    MK_UINT16(a, '5'),          \
    MK_UINT16(a, '6'),          \
    MK_UINT16(a, '7'),          \
    MK_UINT16(a, '8'),          \
    MK_UINT16(a, '9'),          \
    MK_UINT16(a, 'A'),          \
    MK_UINT16(a, 'B'),          \
    MK_UINT16(a, 'C'),          \
    MK_UINT16(a, 'D'),          \
    MK_UINT16(a, 'E'),          \
    MK_UINT16(a, 'F')

    static const uint16 hex_map[256] = {
        MK_ALL_UINT16_WITH_A('0'),
        MK_ALL_UINT16_WITH_A('1'),
        MK_ALL_UINT16_WITH_A('2'),
        MK_ALL_UINT16_WITH_A('3'),
        MK_ALL_UINT16_WITH_A('4'),
        MK_ALL_UINT16_WITH_A('5'),
        MK_ALL_UINT16_WITH_A('6'),
        MK_ALL_UINT16_WITH_A('7'),
        MK_ALL_UINT16_WITH_A('8'),
        MK_ALL_UINT16_WITH_A('9'),
        MK_ALL_UINT16_WITH_A('A'),
        MK_ALL_UINT16_WITH_A('B'),
        MK_ALL_UINT16_WITH_A('C'),
        MK_ALL_UINT16_WITH_A('D'),
        MK_ALL_UINT16_WITH_A('E'),
        MK_ALL_UINT16_WITH_A('F')
    };
    const unsigned char* rawc;
    uint32  read_bytes;
    uint32  write_bytes;
    uint32  i;

    rawc = (const unsigned char*)raw;
    if (hex_size == 0) {
        return(0);
    }

    if (hex_size <= 2 * raw_size) {
        read_bytes = hex_size / 2;
        write_bytes = hex_size;
    } else {
        read_bytes = raw_size;
        write_bytes = 2 * raw_size + 1;
    }

#define LOOP_READ_BYTES(ASSIGN)         \
    for (i = 0; i < read_bytes; i++) {  \
        ASSIGN;                         \
        hex += 2;                       \
        rawc++;                         \
    }

    if (ut_align_offset(hex, 2) == 0) {
        LOOP_READ_BYTES(*(uint16*)hex = hex_map[*rawc]);
    } else {
        LOOP_READ_BYTES(*hex = UINT16_GET_A(hex_map[*rawc]);
        *(hex + 1) = UINT16_GET_B(hex_map[*rawc])
            );
    }

    if (hex_size <= 2 * raw_size && hex_size % 2 == 0) {
        hex--;
    }
    *hex = '\0';

    return write_bytes;
}




/***********************************************************************************************
*                                      load / store data                                       *
***********************************************************************************************/

inline void mach_write_to_1(unsigned char* b, uint32 n)
{
    b[0] = (unsigned char) n;
}

inline uint32 mach_read_from_1(const unsigned char* b)
{
    return ((uint32)(b[0]));
}

inline void mach_write_to_2(unsigned char* b, uint32 n)
{
    b[0] = (unsigned char)(n >> 8);
    b[1] = (unsigned char)(n);
}

inline uint32 mach_read_from_2(const unsigned char* b)
{
    return(((uint32)(b[0]) << 8) | (uint32)(b[1]));
}

inline void mach_write_to_3(unsigned char* b, uint32 n)
{
    b[0] = (unsigned char)(n >> 16);
    b[1] = (unsigned char)(n >> 8);
    b[2] = (unsigned char)(n);
}

inline uint32 mach_read_from_3(const unsigned char* b)
{
    return (((uint32)(b[0]) << 16) | ((uint32)(b[1]) << 8) | (uint32)(b[2]));
}

inline void mach_write_to_4(unsigned char* b, uint32 n)
{
    b[0] = (unsigned char)(n >> 24);
    b[1] = (unsigned char)(n >> 16);
    b[2] = (unsigned char)(n >> 8);
    b[3] = (unsigned char) n;
}

inline uint32 mach_read_from_4(const unsigned char* b)
{
    return (((uint32)(b[0]) << 24) | ((uint32)(b[1]) << 16) | ((uint32)(b[2]) << 8) | (uint32)(b[3]));
}

inline uint32 mach_write_compressed(unsigned char* b, uint32 n)
{
    if (n < 0x80UL) {
        mach_write_to_1(b, n);
        return(1);
    } else if (n < 0x4000UL) {
        mach_write_to_2(b, n | 0x8000UL);
        return(2);
    } else if (n < 0x200000UL) {
        mach_write_to_3(b, n | 0xC00000UL);
        return(3);
    } else if (n < 0x10000000UL) {
        mach_write_to_4(b, n | 0xE0000000UL);
        return(4);
    } else {
        mach_write_to_1(b, 0xF0UL);
        mach_write_to_4(b + 1, n);
        return(5);
    }
}

inline uint32 mach_get_compressed_size(uint32 n)
{
    if (n < 0x80UL) {
        return(1);
    } else if (n < 0x4000UL) {
        return(2);
    } else if (n < 0x200000UL) {
        return(3);
    } else if (n < 0x10000000UL) {
        return(4);
    } else {
        return(5);
    }
}

inline uint32 mach_read_compressed(const unsigned char* b)
{
    uint32 flag = mach_read_from_1(b);
    if (flag < 0x80UL) {
        return (flag);
    } else if (flag < 0xC0UL) {
        return (mach_read_from_2(b) & 0x7FFFUL);
    } else if (flag < 0xE0UL) {
        return (mach_read_from_3(b) & 0x3FFFFFUL);
    } else if (flag < 0xF0UL) {
        return (mach_read_from_4(b) & 0x1FFFFFFFUL);
    } else {
        return (mach_read_from_4(b + 1));
    }
}

inline void mach_write_to_8(void* b, uint64 n)
{
    mach_write_to_4((unsigned char *)b, (uint32) (n >> 32));
    mach_write_to_4((unsigned char *)b + 4, (uint32) n);
}

inline uint64 mach_read_from_8(const unsigned char* b)
{
    uint64 ull;
    ull = ((uint64) mach_read_from_4(b)) << 32;
    ull |= (uint64) mach_read_from_4(b + 4);
    return (ull);
}

inline void mach_write_to_7(unsigned char* b, uint64 n)
{
    mach_write_to_3(b, (uint32) (n >> 32));
    mach_write_to_4(b + 3, (uint32) n);
}

inline uint64 mach_read_from_7(const unsigned char* b)
{
    return ((uint64)mach_read_from_3(b)) << 32 | mach_read_from_4(b + 3);
}

inline void mach_write_to_6(unsigned char* b, uint64 n)
{
    mach_write_to_2(b, (uint32) (n >> 32));
    mach_write_to_4(b + 2, (uint32) n);
}

inline uint64 mach_read_from_6(const unsigned char* b)
{
    return ((uint64)mach_read_from_2(b)) << 32 | mach_read_from_4(b + 2);
}

inline uint32 mach_ull_write_compressed(unsigned char* b, uint64 n)
{
    uint32	size;
    size = mach_write_compressed(b, (uint32)(n >> 32));
    mach_write_to_4(b + size, (uint32) n);
    return (size + 4);
}

inline uint32 mach_ull_get_compressed_size(uint64 n)
{
    return (4 + mach_get_compressed_size((uint32) (n >> 32)));
}

inline uint64 mach_ull_read_compressed(const unsigned char* b)
{
    uint64 n;
    uint32 size;
    n = (uint64) mach_read_compressed(b);
    size = mach_get_compressed_size((uint32)n);
    n <<= 32;
    n |= (uint64) mach_read_from_4(b + size);
    return (n);
}

inline uint32 mach_ull_write_much_compressed(unsigned char* b, uint64 n)
{
    uint32 size;
    if (!(n >> 32)) {
        return(mach_write_compressed(b, (uint32) n));
    }
    *b = (unsigned char)0xFF;
    size = 1 + mach_write_compressed(b + 1, (uint32) (n >> 32));
    size += mach_write_compressed(b + size, (uint32) n & 0xFFFFFFFF);
    return (size);
}

inline uint32 mach_ull_get_much_compressed_size(uint64 n)
{
    if (!(n >> 32)) {
        return(mach_get_compressed_size((uint32) n));
    }
    return(1 + mach_get_compressed_size((uint32) (n >> 32))
       + mach_get_compressed_size((uint32) n & 0xFFFFFFFF));
}

inline uint64 mach_ull_read_much_compressed(const unsigned char* b)
{
    uint64 n;
    uint32 size;
    if (*b != (unsigned char)0xFF) {
        n = 0;
        size = 0;
    } else {
        n = (uint64) mach_read_compressed(b + 1);
        size = 1 + mach_get_compressed_size((uint32)n);
        n <<= 32;
    }
    n |= mach_read_compressed(b + size);
    return(n);
}

/*********************************************************//**
Reads a ulint in a compressed form if the log record fully contains it.
@return	pointer to end of the stored field, NULL if not complete */
inline byte* mach_parse_compressed(
	byte* ptr,	/*!< in: pointer to buffer from where to read */
	byte* end_ptr,/*!< in: pointer to end of the buffer */
	uint32* val)	/*!< out: read value (< 2^32) */
{
	uint32 flag;

	ut_ad(ptr && end_ptr && val);

	if (ptr >= end_ptr) {
		return(NULL);
	}

	flag = mach_read_from_1(ptr);

	if (flag < 0x80UL) {
		*val = flag;
		return(ptr + 1);

	} else if (flag < 0xC0UL) {
		if (end_ptr < ptr + 2) {
			return(NULL);
		}
		*val = mach_read_from_2(ptr) & 0x7FFFUL;
		return(ptr + 2);

	} else if (flag < 0xE0UL) {
		if (end_ptr < ptr + 3) {
			return(NULL);
		}
		*val = mach_read_from_3(ptr) & 0x3FFFFFUL;
		return(ptr + 3);
	} else if (flag < 0xF0UL) {
		if (end_ptr < ptr + 4) {
			return(NULL);
		}

		*val = mach_read_from_4(ptr) & 0x1FFFFFFFUL;
		return(ptr + 4);
	} else {
		ut_ad(flag == 0xF0UL);

		if (end_ptr < ptr + 5) {
			return(NULL);
		}

		*val = mach_read_from_4(ptr + 1);
		return(ptr + 5);
	}
}

/*********************************************************//**
Reads a 64-bit integer in a compressed form
if the log record fully contains it.
@return pointer to end of the stored field, NULL if not complete */
inline unsigned char* mach_ull_parse_compressed(
	unsigned char* ptr, /* in: pointer to buffer from where to read */
	unsigned char* end_ptr,/* in: pointer to end of the buffer */
	uint64* val) /* out: read value */
{
	uint32		size;
	if (end_ptr < ptr + 5) {
		return(NULL);
	}
	*val = mach_read_compressed(ptr);
	size = mach_get_compressed_size((uint32) *val);
	ptr += size;
	if (end_ptr < ptr + 4) {
		return(NULL);
	}
	*val <<= 32;
	*val |= mach_read_from_4(ptr);
	return(ptr + 4);
}

/*********************************************************//**
Reads a double. It is stored in a little-endian format.
@return	double read */
inline double mach_double_read(
	const unsigned char*	b)	/*!< in: pointer to memory from where to read */
{
	double	d;
	uint32	i;
	unsigned char*	ptr;
	ptr = (unsigned char*) &d;
	for (i = 0; i < sizeof(double); i++) {
#ifdef BIG_ENDIAN
		ptr[sizeof(double) - i - 1] = b[i];
#else
		ptr[i] = b[i];
#endif
	}
	return(d);
}

/*********************************************************//**
Writes a double. It is stored in a little-endian format. */
inline void mach_double_write(
	unsigned char*	b,	/*!< in: pointer to memory where to write */
	double	d)	/*!< in: double */
{
	uint32	i;
	unsigned char*	ptr;
	ptr = (unsigned char*) &d;
	for (i = 0; i < sizeof(double); i++) {
#ifdef BIG_ENDIAN
		b[i] = ptr[sizeof(double) - i - 1];
#else
		b[i] = ptr[i];
#endif
	}
}

/*********************************************************//**
Reads a float. It is stored in a little-endian format.
@return	float read */
inline float mach_float_read(
	const unsigned char*	b)	/*!< in: pointer to memory from where to read */
{
	float	d;
	uint32	i;
	unsigned char*	ptr;
	ptr = (unsigned char*) &d;
	for (i = 0; i < sizeof(float); i++) {
#ifdef BIG_ENDIAN
		ptr[sizeof(float) - i - 1] = b[i];
#else
		ptr[i] = b[i];
#endif
	}
	return(d);
}

/*********************************************************//**
Writes a float. It is stored in a little-endian format. */
inline void mach_float_write(
	unsigned char*	b,	/*!< in: pointer to memory where to write */
	float	d)	/*!< in: float */
{
	uint32	i;
	unsigned char*	ptr;
	ptr = (unsigned char*) &d;
	for (i = 0; i < sizeof(float); i++) {
#ifdef BIG_ENDIAN
		b[i] = ptr[sizeof(float) - i - 1];
#else
		b[i] = ptr[i];
#endif
	}
}

/*********************************************************//**
Reads a uint32 stored in the little-endian format.
@return	unsigned long int */
inline uint32 mach_read_from_n_little_endian(
/*===========================*/
	const unsigned char*	buf,		/*!< in: from where to read */
	uint32		buf_size)	/*!< in: from how many bytes to read */
{
	uint32	n	= 0;
	const unsigned char*	ptr;
	ptr = buf + buf_size;
	for (;;) {
		ptr--;
		n = n << 8;
		n += (uint32)(*ptr);
		if (ptr == buf) {
			break;
		}
	}
	return(n);
}

/*********************************************************//**
Writes a uint32 in the little-endian format. */
inline void mach_write_to_n_little_endian(
	unsigned char*	dest,		/*!< in: where to write */
	uint32	dest_size,	/*!< in: into how many bytes to write */
	uint32	n)		/*!< in: unsigned long int to write */
{
	unsigned char*	end;
	end = dest + dest_size;
	for (;;) {
		*dest = (unsigned char)(n & 0xFF);
		n = n >> 8;
		dest++;
		if (dest == end) {
			break;
		}
	}
}

/*********************************************************//**
Reads a uint32 stored in the little-endian format.
@return	unsigned long int */
inline uint32 mach_read_from_2_little_endian(
	const unsigned char*	buf)		/*!< in: from where to read */
{
	return((uint32)(buf[0]) | ((uint32)(buf[1]) << 8));
}

/*********************************************************//**
Writes a uint32 in the little-endian format. */
inline void mach_write_to_2_little_endian(
	unsigned char*	dest,		/*!< in: where to write */
	uint32	n)		/*!< in: unsigned long int to write */
{
	*dest = (unsigned char)(n & 0xFFUL);
	n = n >> 8;
	dest++;
	*dest = (unsigned char)(n & 0xFFUL);
}

/*********************************************************//**
Swap unsigned char ordering. */
inline void mach_swap_byte_order(
        unsigned char*           dest,           /*!< out: where to write */
        const unsigned char*     from,           /*!< in: where to read from */
        uint32           len)            /*!< in: length of src */
{
        dest += len;
        switch (len & 0x7) {
        case 0: *--dest = *from++;
        case 7: *--dest = *from++;
        case 6: *--dest = *from++;
        case 5: *--dest = *from++;
        case 4: *--dest = *from++;
        case 3: *--dest = *from++;
        case 2: *--dest = *from++;
        case 1: *--dest = *from;
        }
}

inline duint64 ut_ull_create(
    uint32 high, // in: high-order 32 bits
    uint32 low)  // in: low-order 32 bits
{
    duint64 res;

    ut_ad(high <= 0xFFFFFFFF);
    ut_ad(low <= 0xFFFFFFFF);

    res.high = high;
    res.low  = low;

    return res;
}

// out: -1 if a < b, 0 if a == b, 1 if a > b
inline int32 ut_duint64_cmp(duint64 a, duint64 b)
{
    if (a.high > b.high) {
        return(1);
    } else if (a.high < b.high) {
        return(-1);
    } else if (a.low > b.low) {
        return(1);
    } else if (a.low < b.low) {
        return(-1);
    } else {
        return(0);
    }
}

// Calculates the max of two dulints
// out: max(a, b)
inline duint64 ut_duint64_get_max(duint64 a, duint64 b)
{
    if (ut_duint64_cmp(a, b) > 0) {
        return(a);
    }
    return(b);
}

// Calculates the min of two dulints
// out: min(a, b)
inline duint64 ut_duint64_get_min(duint64 a, duint64 b)
{
    if (ut_duint64_cmp(a, b) > 0) {
        return(b);
    }

    return(a);
}

// Adds a uint32 to a duint64
// out: sum a + b
inline duint64 ut_duint64_add(duint64 a, uint32 b)
{
    if (0xFFFFFFFF - b >= a.low) {
        a.low += b;
        return(a);
    }

    a.low = a.low - (0xFFFFFFFF - b) - 1;
    a.high++;

    return(a);
}

// Subtracts a uint32 from a duint64
// out: a - b
inline duint64 ut_duint64_subtract(duint64 a, uint32 b)
{
    if (a.low >= b) {
        a.low -= b;
        return(a);
    }

    b -= a.low + 1;
    a.low = 0xFFFFFFFF - b;
    ut_ad(a.high > 0);
    a.high--;

    return(a);
}

