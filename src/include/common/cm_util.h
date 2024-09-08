#ifndef _CM_UTIL_H
#define _CM_UTIL_H

#include "cm_type.h"

/* Bug in developerstudio: use the C version */
#if defined(__cplusplus) && !defined(__SUNPRO_CC)
template <class T, size_t N>
constexpr size_t array_elements(T(&)[N]) noexcept
{
    return N;
}
#else
// Less type-safe version that e.g. allows sending in pointers or STL containers without an error.
#define array_elements(A) ((size_t)(sizeof(A) / sizeof(A[0])))
#endif


//Offset of a structure/union field within that structure/union
#ifndef offsetof
#define offsetof(type, field) ((long)&((type *)0)->field)
#endif



#ifdef __cplusplus
extern "C" {
#endif

/***********************************************************************************************
*                                      math or compare                                         *
***********************************************************************************************/

#define ut_min(a, b)    ((a) < (b) ? (a) : (b))
#define ut_max(a, b)    ((a) > (b) ? (a) : (b))
#define ut_test(a)      ((a) ? 1 : 0)

#define swap_variables(t, a, b) { t dummy; dummy= a; a= b; b= dummy; }

#define MY_TEST(a)      ((a) ? 1 : 0)
#define MY_MAX(a, b)    ((a) > (b) ? (a) : (b))
#define MY_MIN(a, b)    ((a) < (b) ? (a) : (b))


#define set_if_bigger(a, b)   \
  do {                        \
    if ((a) < (b)) (a) = (b); \
  } while (0)

#define set_if_smaller(a, b)  \
  do {                        \
    if ((a) > (b)) (a) = (b); \
  } while (0)

#define test_all_bits(a, b) (((a) & (b)) == (b))

#define BIT_RESET(bits, mask)    ((bits) &= ~(mask))

#ifndef __WIN__
#define _atoi64(val)        strtoll(val, NULL, 10)
#endif

extern uint32 ut_raw_to_hex(
    const void* raw,        /*!< in: raw data */
    uint32      raw_size,   /*!< in: "raw" length in bytes */
    char*       hex,        /*!< out: hex string */
    uint32      hex_size);  /*!< in: "hex" size in bytes */

extern uint32 ut_2_log(uint32 n);
extern uint32 ut_2_exp(uint32 n);


/** Determines if a number is zero or a power of two.
@param[in] n number
@return nonzero if n is zero or a power of two; zero otherwise */
#define ut_is_2pow(n) (!((n) & ((n)-1)))

/** Calculates the biggest multiple of m that is not bigger than n
 when m is a power of two.  In other words, rounds n down to m * k.
 @param n in: number to round down
 @param m in: alignment, must be a power of two
 @return n rounded down to the biggest possible integer multiple of m */
#define ut_2pow_round(n, m) ((n) & ~((m)-1))

/** Calculates fast the number rounded up to the nearest power of 2.
 @return first power of 2 which is >= n */
uint64 ut_2_power_up(uint64 n); /*!< in: number != 0 */

/** Calculates fast the remainder of n/m when m is a power of two.
    @param n in: numerator
    @param m in: denominator, must be a power of two
    @return the remainder of n/m */
#define ut_2pow_remainder(n, m) ((n) & ((m)-1))


/***********************************************************************************************
*                                      align                                                   *
***********************************************************************************************/

extern uint32 ut_calc_align(
    uint32 n,            /* in: number to be rounded */
    uint32 align_no);    /* in: align by this number */

extern void* ut_align(
    void* ptr,            /* in: pointer */
    uint32 align_no);     /* in: align by this number */

extern uint32 ut_align_offset(
    const void* ptr,       /*!< in: pointer */
    uint32      align_no); /*!< in: align by this number */

extern uint32 ut_calc_align_down(
    uint32 n,              /* in: number to be rounded */
    uint32 align_no);       /* in: align by this number */

extern void* ut_align_down(
    void* ptr,            /* in: pointer */
    uint32 align_no);     /* in: align by this number */

extern uint64 ut_uint64_align_down(
    uint64 n, //in: number to be rounded
    uint32 align_no); //in: align by this number which must be a power of 2

extern uint64 ut_uint64_align_up(
    uint64 n, //in: number to be rounded
    uint32 align_no); //in: align by this number which must be a power of 2


extern bool32 ut_bit8_get_nth(
    uint8 a,    /* in: uint8 */
    uint32 n);   /* in: nth bit requested */

extern uint8 ut_bit8_set_nth(
    uint8 a,    /* in: uint8 */
    uint32 n,    /* in: nth bit requested */
    bool32 val); /* in: value for the bit to set */

extern bool32 ut_bit32_get_nth(
    uint32 a,    /* in: uint32 */
    uint32 n);   /* in: nth bit requested */

extern uint32 ut_bit32_set_nth(
    uint32 a,    /* in: uint32 */
    uint32 n,    /* in: nth bit requested */
    bool32 val); /* in: value for the bit to set */

extern bool32 ut_bit64_get_nth(
    uint64 a,    /* in: uint64 */
    uint32 n);   /* in: nth bit requested */

extern uint64 ut_bit64_set_nth(
    uint64 a,    /* in: uint64 */
    uint32 n,    /* in: nth bit requested */
    bool32 val); /* in: value for the bit to set */


#define ut_align4(size)         (((size) & 0x03) == 0 ? (size) : (size) + 0x04 - ((size) & 0x03))
#define ut_align8(size)         (((size) & 0x07) == 0 ? (size) : (size) + 0x08 - ((size) & 0x07))
#define ut_align16(size)        (((size) & 0x0F) == 0 ? (size) : (size) + 0x10 - ((size) & 0x0F))
#define ut_is_align4(size)      (((size) & 0x03) == 0)
#define ut_is_align8(size)      (((size) & 0x07) == 0)
#define ut_is_align16(size)     (((size) & 0x0F) == 0)

#define MY_ALIGN(A, L)          (((A) + (L)-1) & ~((L)-1))

/***********************************************************************************************
*                                      load / store data                                       *
***********************************************************************************************/


/*******************************************************//**
The following function is used to store data in one char. */
extern inline void mach_write_to_1(
    unsigned char*  b,  /*!< in: pointer to char where to store */
    uint32  n);  /*!< in: uint32 integer to be stored, >= 0, < 256 */

/********************************************************//**
The following function is used to fetch data from one char.
@return uint32 integer, >= 0, < 256 */
extern inline uint32 mach_read_from_1(
    const unsigned char*    b); /*!< in: pointer to char */

/*******************************************************//**
The following function is used to store data in two consecutive
bytes. We store the most significant char to the lower address. */
extern inline void mach_write_to_2(
    unsigned char*  b,  /*!< in: pointer to two bytes where to store */
    uint32  n);  /*!< in: uint32 integer to be stored, >= 0, < 64k */

/********************************************************//**
The following function is used to fetch data from two consecutive
bytes. The most significant char is at the lowest address.
@return uint32 integer, >= 0, < 64k */
extern inline uint32 mach_read_from_2(
    const unsigned char*    b); /*!< in: pointer to two bytes */

/*******************************************************//**
The following function is used to store data in 3 consecutive
bytes. We store the most significant char to the lowest address. */
extern inline void mach_write_to_3(
    unsigned char*  b,  /*!< in: pointer to 3 bytes where to store */
    uint32  n);  /*!< in: uint32 integer to be stored */

/********************************************************//**
The following function is used to fetch data from 3 consecutive
bytes. The most significant char is at the lowest address.
@return uint32 integer */
extern inline uint32 mach_read_from_3(
    const unsigned char*    b); /*!< in: pointer to 3 bytes */

/*******************************************************//**
The following function is used to store data in four consecutive
bytes. We store the most significant char to the lowest address. */
extern inline void mach_write_to_4(
    unsigned char*  b,  /*!< in: pointer to four bytes where to store */
    uint32  n);  /*!< in: uint32 integer to be stored */

/********************************************************//**
The following function is used to fetch data from 4 consecutive
bytes. The most significant char is at the lowest address.
@return uint32 integer */
extern inline uint32 mach_read_from_4(
    const unsigned char*    b); /*!< in: pointer to four bytes */

/*********************************************************//**
Writes a uint32 in a compressed form (1..5 bytes).
@return stored size in bytes */
extern inline uint32 mach_write_compressed(
    unsigned char*  b,  /*!< in: pointer to memory where to store */
    uint32  n); /*!< in: uint32 integer to be stored */

/*********************************************************//**
Returns the size of an uint32 when written in the compressed form.
@return compressed size in bytes */
extern inline uint32 mach_get_compressed_size(
    uint32  n); /*!< in: uint32 integer to be stored */

/*********************************************************//**
Reads a uint32 in a compressed form.
@return read integer */
extern inline uint32 mach_read_compressed(
    const unsigned char*    b); /*!< in: pointer to memory from where to read */

/*******************************************************//**
The following function is used to store data in 6 consecutive
bytes. We store the most significant char to the lowest address. */
extern inline void mach_write_to_6(
    unsigned char*  b,  /*!< in: pointer to 6 bytes where to store */
    uint64  id);    /*!< in: 48-bit integer */

/********************************************************//**
The following function is used to fetch data from 6 consecutive
bytes. The most significant char is at the lowest address.
@return 48-bit integer */
extern inline uint64 mach_read_from_6(
    const unsigned char*    b); /*!< in: pointer to 6 bytes */

/*******************************************************//**
The following function is used to store data in 7 consecutive
bytes. We store the most significant char to the lowest address. */
extern inline void mach_write_to_7(
    unsigned char*      b,  /*!< in: pointer to 7 bytes where to store */
    uint64  n); /*!< in: 56-bit integer */

/********************************************************//**
The following function is used to fetch data from 7 consecutive
bytes. The most significant char is at the lowest address.
@return 56-bit integer */
extern inline uint64 mach_read_from_7(
    const unsigned char*    b); /*!< in: pointer to 7 bytes */

/*******************************************************//**
The following function is used to store data in 8 consecutive
bytes. We store the most significant char to the lowest address. */
extern inline void mach_write_to_8(
    void*       b,  /*!< in: pointer to 8 bytes where to store */
    uint64  n); /*!< in: 64-bit integer to be stored */

/********************************************************//**
The following function is used to fetch data from 8 consecutive
bytes. The most significant char is at the lowest address.
@return 64-bit integer */
extern inline uint64 mach_read_from_8(
    const unsigned char*    b); /*!< in: pointer to 8 bytes */

/*********************************************************//**
Writes a 64-bit integer in a compressed form (5..9 bytes).
@return size in bytes */
extern inline uint32 mach_ull_write_compressed(
    unsigned char*      b,  /*!< in: pointer to memory where to store */
    uint64  n); /*!< in: 64-bit integer to be stored */

/*********************************************************//**
Returns the size of a 64-bit integer when written in the compressed form.
@return compressed size in bytes */
extern inline uint32 mach_ull_get_compressed_size(
    uint64  n); /*!< in: 64-bit integer to be stored */

/*********************************************************//**
Reads a 64-bit integer in a compressed form.
@return the value read */
extern inline uint64 mach_ull_read_compressed(
    const unsigned char*    b); /*!< in: pointer to memory from where to read */

/*********************************************************//**
Writes a 64-bit integer in a compressed form (1..11 bytes).
@return size in bytes */
extern inline uint32 mach_ull_write_much_compressed(
    unsigned char*  b,  /*!< in: pointer to memory where to store */
    uint64  n); /*!< in: 64-bit integer to be stored */

/*********************************************************//**
Returns the size of a 64-bit integer when written in the compressed form.
@return compressed size in bytes */
extern inline uint32 mach_ull_get_much_compressed_size(
    uint64  n); /*!< in: 64-bit integer to be stored */

/*********************************************************//**
Reads a 64-bit integer in a compressed form.
@return the value read */
extern inline uint64 mach_ull_read_much_compressed(
    const unsigned char*    b); /*!< in: pointer to memory from where to read */

/*********************************************************//**
Reads a uint32 in a compressed form if the log record fully contains it.
@return pointer to end of the stored field, NULL if not complete */
extern inline unsigned char* mach_parse_compressed(
    unsigned char*  ptr,    /*!< in: pointer to buffer from where to read */
    unsigned char*  end_ptr,/*!< in: pointer to end of the buffer */
    uint32* val);   /*!< out: read value */

/*********************************************************//**
Reads a 64-bit integer in a compressed form
if the log record fully contains it.
@return pointer to end of the stored field, NULL if not complete */
extern inline unsigned char* mach_ull_parse_compressed(
    unsigned char*      ptr,    /*!< in: pointer to buffer from where to read */
    unsigned char*      end_ptr,/*!< in: pointer to end of the buffer */
    uint64* val);   /*!< out: read value */

/*********************************************************//**
Reads a double. It is stored in a little-endian format.
@return double read */
extern inline double mach_double_read(
    const unsigned char*    b); /*!< in: pointer to memory from where to read */

/*********************************************************//**
Writes a double. It is stored in a little-endian format. */
extern inline void mach_double_write(
    unsigned char*  b,  /*!< in: pointer to memory where to write */
    double  d); /*!< in: double */

/*********************************************************//**
Reads a float. It is stored in a little-endian format.
@return float read */
extern inline float mach_float_read(
    const unsigned char*    b); /*!< in: pointer to memory from where to read */

/*********************************************************//**
Writes a float. It is stored in a little-endian format. */
extern inline void mach_float_write(
    unsigned char*  b,  /*!< in: pointer to memory where to write */
    float   d); /*!< in: float */
    
/*********************************************************//**
Reads a uint32 stored in the little-endian format.
@return unsigned long int */
extern inline uint32 mach_read_from_n_little_endian(
    const unsigned char*    buf,        /*!< in: from where to read */
    uint32      buf_size);  /*!< in: from how many bytes to read */

/*********************************************************//**
Writes a uint32 in the little-endian format. */
extern inline void mach_write_to_n_little_endian(
/*==========================*/
    unsigned char*  dest,       /*!< in: where to write */
    uint32  dest_size,  /*!< in: into how many bytes to write */
    uint32  n);     /*!< in: unsigned long int to write */

/*********************************************************//**
Reads a uint32 stored in the little-endian format.
@return unsigned long int */
extern inline uint32 mach_read_from_2_little_endian(
    const unsigned char*    buf);       /*!< in: from where to read */
    
/*********************************************************//**
Writes a uint32 in the little-endian format. */
extern inline void mach_write_to_2_little_endian(
    unsigned char*  dest,       /*!< in: where to write */
    uint32  n);     /*!< in: unsigned long int to write */


// Type definition for a 64-bit unsigned integer
typedef struct st_duint64 duint64;
struct st_duint64{
    uint32  high;  // most significant 32 bits
    uint32  low;   // least significant 32 bits
};

/* Zero value for a duint64 */
extern duint64      ut_duint64_zero;

/* Maximum value for a duint64 */
extern duint64      ut_duint64_max;


extern inline duint64 ut_ull_create(
  uint32  high, /*!< in: high-order 32 bits */
  uint32  low);  /*!< in: low-order 32 bits */

// Compares two dulints
// out: -1 if a < b, 0 if a == b, 1 if a > b
extern inline int ut_duint64_cmp(duint64 a, duint64 b);

// Calculates the max of two dulints
// out: max(a, b)
extern inline duint64 ut_duint64_get_max(duint64 a, duint64 b);

// Calculates the min of two dulints
// out: min(a, b)
extern inline duint64 ut_duint64_get_min(duint64 a, duint64 b);

// Adds a uint32 to a duint64
// out: sum a + b
extern inline duint64 ut_duint64_add(duint64 a, uint32 b);

// Subtracts a uint32 from a duint64
// out: a - b
extern inline duint64 ut_duint64_subtract(duint64 a, uint32 b);

extern inline uint32 ut_duint64_get_high(duint64 d)
{
    return d.high;
}

extern inline uint32 ut_duint64_get_low(duint64 d)
{
    return d.low;
}


/*
 Functions for big-endian loads and stores. These are safe to use
 no matter what the compiler, CPU or alignment, and also with -fstrict-aliasing.
 The stores return a pointer just past the value that was written.
*/
extern inline uint16 load16be(const char *ptr) {
  uint16 val;
  memcpy(&val, ptr, sizeof(val));
  return ntohs(val);
}

extern inline uint32 load32be(const char *ptr) {
  uint32 val;
  memcpy(&val, ptr, sizeof(val));
  return ntohl(val);
}

extern inline char *store16be(char *ptr, uint16 val) {
#if defined(_MSC_VER)
  // _byteswap_ushort is an intrinsic on MSVC, but htons is not.
  val = _byteswap_ushort(val);
#else
  val = htons(val);
#endif
  memcpy(ptr, &val, sizeof(val));
  return ptr + sizeof(val);
}

extern inline char *store32be(char *ptr, uint32 val) {
  val = htonl(val);
  memcpy(ptr, &val, sizeof(val));
  return ptr + sizeof(val);
}



extern inline int8 sint1korr(const uchar *A) { return *A; }

extern inline uint8 uint1korr(const uchar *A) { return *A; }

extern inline int16 sint2korr(const uchar *A)
{
    return (int16)((uint32)(A[1]) + ((uint32)(A[0]) << 8));
}

extern inline int32 sint3korr(const uchar *A)
{
    return (int32)((A[0] & 128) ? ((255U << 24) | ((uint32)(A[0]) << 16) |
        ((uint32)(A[1]) << 8) | ((uint32)A[2]))
        : (((uint32)(A[0]) << 16) |
        ((uint32)(A[1]) << 8) | ((uint32)(A[2]))));
}

extern inline int32 sint4korr(const uchar *A)
{
    return (int32)((uint32)(A[3]) + ((uint32)(A[2]) << 8) +
        ((uint32)(A[1]) << 16) + ((uint32)(A[0]) << 24));
}

extern inline uint16 uint2korr(const uchar *A)
{
    return (uint16)((uint16)A[1]) + ((uint16)A[0] << 8);
}

extern inline uint32 uint3korr(const uchar *A)
{
    return (uint32)((uint32)A[2] + ((uint32)A[1] << 8) + ((uint32)A[0] << 16));
}

extern inline uint32 uint4korr(const uchar *A)
{
    return (uint32)((uint32)A[3] + ((uint32)A[2] << 8) + ((uint32)A[1] << 16) +
        ((uint32)A[0] << 24));
}

extern inline ulonglong uint5korr(const uchar *A)
{
    return (ulonglong)((uint32)A[4] + ((uint32)A[3] << 8) + ((uint32)A[2] << 16) +
        ((uint32)A[1] << 24)) +
        ((ulonglong)A[0] << 32);
}

extern inline ulonglong uint6korr(const uchar *A)
{
    return (ulonglong)((uint32)A[5] + ((uint32)A[4] << 8) + ((uint32)A[3] << 16) +
        ((uint32)A[2] << 24)) +
        (((ulonglong)((uint32)A[1] + ((uint32)A[0] << 8))) << 32);
}

extern inline ulonglong uint7korr(const uchar *A)
{
    return (ulonglong)((uint32)A[6] + ((uint32)A[5] << 8) + ((uint32)A[4] << 16) +
        ((uint32)A[3] << 24)) +
        (((ulonglong)((uint32)A[2] + ((uint32)A[1] << 8) +
        ((uint32)A[0] << 16)))
            << 32);
}

extern inline ulonglong uint8korr(const uchar *A)
{
    return (ulonglong)((uint32)A[7] + ((uint32)A[6] << 8) + ((uint32)A[5] << 16) +
        ((uint32)A[4] << 24)) +
        (((ulonglong)((uint32)A[3] + ((uint32)A[2] << 8) +
        ((uint32)A[1] << 16) + ((uint32)A[0] << 24)))
            << 32);
}

extern inline longlong sint8korr(const uchar *A)
{
    return (longlong)uint8korr(A);
}

/* This one is for uniformity */
#define int1store(T, A) *((uchar *)(T)) = (uchar)(A)

#define int2store(T, A)                         \
  {                                             \
    uint def_temp = (uint)(A);                  \
    ((uchar *)(T))[1] = (uchar)(def_temp);      \
    ((uchar *)(T))[0] = (uchar)(def_temp >> 8); \
  }

#define int3store(T, A)                          \
  {                                              \
    ulong def_temp = (ulong)(A);                 \
    ((uchar *)(T))[2] = (uchar)(def_temp);       \
    ((uchar *)(T))[1] = (uchar)(def_temp >> 8);  \
    ((uchar *)(T))[0] = (uchar)(def_temp >> 16); \
  }

#define int4store(T, A)                          \
  {                                              \
    ulong def_temp = (ulong)(A);                 \
    ((uchar *)(T))[3] = (uchar)(def_temp);       \
    ((uchar *)(T))[2] = (uchar)(def_temp >> 8);  \
    ((uchar *)(T))[1] = (uchar)(def_temp >> 16); \
    ((uchar *)(T))[0] = (uchar)(def_temp >> 24); \
  }

#define int5store(T, A)                                       \
  {                                                              \
    ulong def_temp = (ulong)(A), def_temp2 = (ulong)((A) >> 32); \
    ((uchar *)(T))[4] = (uchar)(def_temp);                       \
    ((uchar *)(T))[3] = (uchar)(def_temp >> 8);                  \
    ((uchar *)(T))[2] = (uchar)(def_temp >> 16);                 \
    ((uchar *)(T))[1] = (uchar)(def_temp >> 24);                 \
    ((uchar *)(T))[0] = (uchar)(def_temp2);                      \
  }

#define int6store(T, A)                                       \
  {                                                              \
    ulong def_temp = (ulong)(A), def_temp2 = (ulong)((A) >> 32); \
    ((uchar *)(T))[5] = (uchar)(def_temp);                       \
    ((uchar *)(T))[4] = (uchar)(def_temp >> 8);                  \
    ((uchar *)(T))[3] = (uchar)(def_temp >> 16);                 \
    ((uchar *)(T))[2] = (uchar)(def_temp >> 24);                 \
    ((uchar *)(T))[1] = (uchar)(def_temp2);                      \
    ((uchar *)(T))[0] = (uchar)(def_temp2 >> 8);                 \
  }

#define int7store(T, A)                                       \
  {                                                              \
    ulong def_temp = (ulong)(A), def_temp2 = (ulong)((A) >> 32); \
    ((uchar *)(T))[6] = (uchar)(def_temp);                       \
    ((uchar *)(T))[5] = (uchar)(def_temp >> 8);                  \
    ((uchar *)(T))[4] = (uchar)(def_temp >> 16);                 \
    ((uchar *)(T))[3] = (uchar)(def_temp >> 24);                 \
    ((uchar *)(T))[2] = (uchar)(def_temp2);                      \
    ((uchar *)(T))[1] = (uchar)(def_temp2 >> 8);                 \
    ((uchar *)(T))[0] = (uchar)(def_temp2 >> 16);                \
  }

#define int8store(T, A)                                        \
  {                                                               \
    ulong def_temp3 = (ulong)(A), def_temp4 = (ulong)((A) >> 32); \
    mi_int4store((uchar *)(T) + 0, def_temp4);                    \
    mi_int4store((uchar *)(T) + 4, def_temp3);                    \
  }


/***********************************************************************************************
*                                      string                                                  *
***********************************************************************************************/

#ifdef __WIN__
#define strcasecmp      _stricmp
#define strncasecmp     _strnicmp
#endif

#ifdef __WIN__

#define strtok_r strtok_s

#endif

#ifdef __cplusplus
}
#endif

#endif  /* _CM_UTIL_H */

