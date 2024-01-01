#ifndef _CM_TYPE_H
#define _CM_TYPE_H

#if !defined(__WIN__) && defined(_WIN32)
#define __WIN__
#endif

#include <stdarg.h>
#include <time.h>
#include <ctype.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <malloc.h>
#include <assert.h>

#ifdef __WIN__
#define WIN32_LEAN_ADD_MEAN
#include <winsock2.h>
#include <windows.h>    
#include <tlhelp32.h>
#include <stddef.h>
#else
#include <unistd.h>
#include <pthread.h>
#include <sys/queue.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#endif


#ifdef __cplusplus
extern "C" {
#endif


/***********************************************************************************************
*                                      compiler                                                *
***********************************************************************************************/

/* If supported, give compiler hints for branch prediction. */
#if !defined(__builtin_expect) && (!defined(__GNUC__) || (__GNUC__ == 2 && __GNUC_MINOR__ < 96))
#define __builtin_expect(x, expected_value)     (x)
#endif

#define likely(x)           __builtin_expect((x),1)
#define unlikely(x)         __builtin_expect((x),0)
#define LIKELY(x)           __builtin_expect((x),1)
#define UNLIKELY(x)         __builtin_expect((x),0)


/* Compile-time constant of the given array's size. */
#define UT_ARR_SIZE(a)      (sizeof(a) / sizeof((a)[0]))

#if defined(_MSC_VER)
#define ALWAYS_INLINE       __forceinline
#else
#define ALWAYS_INLINE       __attribute__((always_inline)) inline
#endif

/*
Disable MY_ATTRIBUTE for Sun Studio and Visual Studio.
Note that Sun Studio supports some __attribute__ variants,
but not format or unused which we use quite a lot.
*/
#ifndef MY_ATTRIBUTE
#if defined(__GNUC__) || defined(__clang__)
#define MY_ATTRIBUTE(A)     __attribute__(A)
#else
#define MY_ATTRIBUTE(A)
#endif
#endif

#if defined(_MSC_VER)
#define THREAD_LOCAL        __declspec(thread)
#else
#define THREAD_LOCAL        __thread
#endif


/** barrier definitions for memory ordering */
#ifdef __WIN__
#include <mmintrin.h>
#define os_rmb          _mm_lfence()
#define os_wmb          _mm_sfence()
#define os_mb           _mm_mfence()
#elif defined(HAVE_IB_GCC_ATOMIC_THREAD_FENCE)
#define os_rmb          __atomic_thread_fence(__ATOMIC_ACQUIRE)
#define os_wmb          __atomic_thread_fence(__ATOMIC_RELEASE)
#define os_mb           __atomic_thread_fence()
#elif defined(HAVE_IB_GCC_SYNC_SYNCHRONISE)
#define os_rmb          __sync_synchronize()
#define os_wmb          __sync_synchronize()
#define os_mmb          __sync_synchronize()
#else
#define os_rmb
#define os_wmb
#define os_mb
#endif



/***********************************************************************************************
*                                      types                                                   *
***********************************************************************************************/

typedef unsigned char           UCHAR;
typedef unsigned char           uchar;
typedef unsigned char           BYTE;
typedef unsigned char           byte;
typedef unsigned char           BYTE8;
typedef unsigned char           byte8;
typedef unsigned char           UINT8;
typedef unsigned char           uint8;
typedef signed   char           INT8;
typedef signed   char           int8;
typedef signed   char           bool8;

typedef char                    my_bool;

#if defined(__GNUC__)
typedef char                    pchar;      /* Mixed prototypes can take char */
typedef char                    pbool;      /* Mixed prototypes can take char */
#else
typedef int                     pchar;      /* Mixed prototypes can't take char */
typedef int                     pbool;      /* Mixed prototypes can't take char */
#endif

typedef unsigned short int      WORD;
typedef unsigned short int      WORD16;
typedef unsigned short int      UINT16;
typedef unsigned short int      uint16;
typedef signed   short int      INT16;
typedef signed   short int      int16;

#ifndef __WIN__
typedef unsigned int            DWORD;
typedef char                    CHAR;
#endif

typedef unsigned int            UINT;
typedef unsigned int            uint;
typedef unsigned int            WORD32;
typedef unsigned int            UINT32;
typedef unsigned int            uint32;
typedef signed   int            INT32;
typedef signed   int            int32;

typedef unsigned int            BOOL32;
typedef unsigned int            bool32;

typedef long                    LONG;
typedef unsigned long           ULONG;
typedef unsigned long           ulong;

#ifndef __WIN__
typedef unsigned long long      WORD64;
typedef unsigned long long      UINT64;
typedef unsigned long long      uint64;
typedef signed   long long      INT64;
typedef signed   long long      int64;
#else
typedef unsigned __int64        WORD64;
typedef unsigned __int64        UINT64;
typedef unsigned __int64        uint64;
typedef __int64                 INT64;
typedef __int64                 int64;
#endif

#ifdef __WIN__
typedef struct st_int128 {
    int64   val1;
    int64   val2;
} int128;
typedef struct st_uint128 {
    uint64  val1;
    uint64  val2;
} uint128;
#else
typedef unsigned __int128       UINT128;
typedef unsigned __int128       uint128;
typedef __int128                INT128;
typedef __int128                int128;
#endif

#ifdef __WIN__
typedef __int64                 longlong;
typedef unsigned __int64        ULONGLONG;
typedef unsigned __int64        ulonglong;
#else
typedef long long               longlong;
typedef unsigned long long      ULONGLONG;
typedef unsigned long long      ulonglong;
#endif


#ifndef __WIN__
typedef void*                   HANDLE;
typedef void                    VOID;
typedef unsigned char*          LPSTR;
#endif
typedef unsigned char*          PUCHAR;

#define OS_ASSERT(c)            assert(c)
#define OS_ERROR                assert(0)


/* Define some general constants */
#ifndef TRUE
#define TRUE                    (1) /* Logical true */
#define FALSE                   (0) /* Logical false */
#endif

#ifndef SIZE_T_MAX
#define SIZE_T_MAX              ((size_t)-1)
#endif
#define SIZEOF_CHARP            sizeof(char *)


/** The 'undefined' value for a ulint */
#define UINT32_UNDEFINED        ((uint32)(-1))

#define INT_MIN64               (~0x7FFFFFFFFFFFFFFFLL)
#define INT_MAX64               0x7FFFFFFFFFFFFFFFLL
#define INT_MIN32               (~0x7FFFFFFFL)
#define INT_MAX32               0x7FFFFFFFL
#define UINT_MAX32              0xFFFFFFFFL
#define INT_MIN24               (~0x007FFFFF)
#define INT_MAX24               0x007FFFFF
#define UINT_MAX24              0x00FFFFFF
#define INT_MIN16               (~0x7FFF)
#define INT_MAX16               0x7FFF
#define UINT_MAX16              0xFFFF
#define INT_MIN8                (~0x7F)
#define INT_MAX8                0x7F
#define UINT_MAX8               0xFF

#define MAX_TINYINT_WIDTH       3     /**< Max width for a TINY w.o. sign */
#define MAX_SMALLINT_WIDTH      5     /**< Max width for a SHORT w.o. sign */
#define MAX_MEDIUMINT_WIDTH     8     /**< Max width for a INT24 w.o. sign */
#define MAX_INT_WIDTH           10    /**< Max width for a LONG w.o. sign */
#define MAX_BIGINT_WIDTH        20    /**< Max width for a LONGLONG */
#define MAX_CHAR_WIDTH          255   /**< Max length for a CHAR colum */


// Definition of the null string (a null pointer of type char *), used in some of our string handling code.
// New code should use nullptr instead.
#define NullS                  (char *)0

#ifdef __WIN__
#define SRV_PATH_SEPARATOR     '\\'
#else
#define SRV_PATH_SEPARATOR     '/'
#endif

#ifdef __WIN__
#define os_file_t                           HANDLE
#define OS_FILE_INVALID_HANDLE              INVALID_HANDLE_VALUE
#else
typedef int                                 os_file_t;
#define OS_FILE_INVALID_HANDLE              -1
#endif

typedef enum
{
    LOG_UNKOWN   = 0,
    LOG_FATAL    = 1,
    LOG_ERROR    = 2,
    LOG_WARN     = 3,
    LOG_NOTICE   = 4,
    LOG_INFO     = 5,
    LOG_DEBUG    = 6,
    LOG_TRACE    = 7,
} log_level_t;


/***********************************************************************************************
*                                      callback function                                       *
***********************************************************************************************/

typedef struct st_callback_data
{
    union
    {
        void             *ptr;
        int               i32;
        uint32            u32;
        uint64            u64;
    };
    uint32                len;
} callback_data_t;

typedef void (*callback_func) (callback_data_t *data);



/***********************************************************************************************
*                                      return status                                           *
***********************************************************************************************/


typedef enum status_stuct
{
    M_ERROR = -1,
    M_SUCCESS = 0,
    M_TIMEOUT = 1,
} status_t;

#define M_RETURN_IF_ERROR(ret)            \
    do {                                  \
        status_t _status_ = (ret);        \
        if (_status_ != M_SUCCESS) {      \
            return _status_;              \
        }                                 \
    } while (0)

#define M_RETURN_IF_FALSE(ret)            \
    do {                                  \
        if ((ret) == FALSE) {             \
            return FALSE;                 \
        }                                 \
    } while (0)

#define M_RETURN_IF_TRUE(ret)             \
    do {                                  \
        if ((ret) == TRUE ) {             \
            return TRUE;                  \
        }                                 \
    } while (0)



/***********************************************************************************************
*                                      DEBUG OUTPUT                                            *
***********************************************************************************************/

#if 0
#define UNIV_DEBUG              /*  */
#define UNIV_MEMORY_DEBUG       /* detect memory leaks etc */
#define UNIV_MEMROY_VALGRIND    /* Enable extra Valgrind instrumentation */
#define UNIV_DEBUG_OUTPUT       /* DBUG_*, ut_ad, ut_d */
#define UNIV_MUTEX_DEBUG        /* mutex and latch */


#endif





#ifdef __cplusplus
}
#endif

#endif  /*_CM_TYPE_H*/
