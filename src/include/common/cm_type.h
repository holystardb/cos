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
#else  // __WIN__
#include <unistd.h>
#include <pthread.h>
#include <sys/queue.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#endif  // __WIN__


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

#define likely(x)               __builtin_expect((x), 1)
#define unlikely(x)             __builtin_expect((x), 0)
#define LIKELY(x)               __builtin_expect((x), 1)
#define UNLIKELY(x)             __builtin_expect((x), 0)
#define expect(expr, constant)  __builtin_expect(expr, constant)
#define EXPECT(expr, constant)  __builtin_expect(expr, constant)


/* Compile-time constant of the given array's size. */
#define UT_ARR_SIZE(a)      (sizeof(a) / sizeof((a)[0]))

#ifdef __WIN__
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

#ifdef __WIN__
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
    int64   high;
    int64   low;
} int128;
typedef struct st_uint128 {
    uint64  high;
    uint64  low;
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

#define SIZE_K(n)               ((n) * 1024)
#define SIZE_M(n)               (1024 * SIZE_K(n))
#define SIZE_G(n)               (1024 * (uint64)SIZE_M(n))
#define SIZE_T(n)               (1024 * (uint64)SIZE_G(n))


/* Define some general constants */
#ifndef TRUE
#define TRUE                    (1) /* Logical true */
#define FALSE                   (0) /* Logical false */
#endif

#ifndef SIZE_T_MAX
#define SIZE_T_MAX              ((size_t)-1)
#endif
#define SIZEOF_CHARP            sizeof(char *)


/** The 'undefined' value for a uint32 */
#define UINT32_UNDEFINED        ((uint32)(-1))

#define INT_MIN64               (~0x7FFFFFFFFFFFFFFFLL)
#define INT_MAX64               0x7FFFFFFFFFFFFFFFLL
#define UINT_MAX64              0xFFFFFFFFFFFFFFFFLL
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

#define VAR_OFFSET(type, member)    ((unsigned long long)(&((type *)0)->member))
#define OFFSET_OF                   offsetof

#ifdef __WIN__
#define os_file_t                           HANDLE
#define OS_FILE_INVALID_HANDLE              INVALID_HANDLE_VALUE
#else
typedef int                                 os_file_t;
#define OS_FILE_INVALID_HANDLE              -1
#endif


/***********************************************************************************************
*                                      define                                                  *
***********************************************************************************************/

#define CM_FILE_NAME_BUF_SIZE               256
#define CM_FILE_NAME_MAX_LEN                255
#define CM_FILE_PATH_BUF_SIZE               1024
#define CM_FILE_PATH_MAX_LEN                1023

#define CM_ERR_MSG_MAX_LEN                  1023


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

/** Shutdown state */
typedef enum shutdown_state_enum {
    SHUTDOWN_NONE = 0,	/*!< running normally */
    SHUTDOWN_CLEANUP,	/*!< Cleaning up in logs_empty_and_mark_files_at_shutdown() */
    SHUTDOWN_FLUSH_PHASE,/*!< At this phase the master and the
                             purge threads must have completed their
                             work. Once we enter this phase the
                             page_cleaner can clean up the buffer
                             pool and exit */
    SHUTDOWN_LAST_PHASE,/*!< Last phase after ensuring that
                            the buffer pool can be freed: flush
                            all file spaces and close all files */
    SHUTDOWN_EXIT_THREADS/*!< Exit all threads */
} shutdown_state_enum_t;



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

#ifndef EOK
#define EOK (0)
#endif

typedef enum st_status {
    CM_EAGAIN   = -2,
    CM_ERROR    = -1,
    CM_SUCCESS  = 0,
    CM_TIMEDOUT = 1,
} status_t;


#define M_RETURN_IF_ERROR(ret)            \
    do {                                  \
        status_t _status_ = (ret);        \
        if (_status_ != CM_SUCCESS) {      \
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

// securec memory function check
#define MEMS_RETURN_IFERR(func)                        \
    do {                                               \
        int32 __code__ = (func);                       \
        if (UNLIKELY(__code__ != EOK)) {               \
            CM_SET_ERROR(ERR_SYSTEM_CALL, __code__);   \
            return CM_ERROR;                           \
        }                                              \
    } while (0)


// securec memory function check
#define MEMS_RETVOID_IFERR(func)                       \
    do {                                               \
        int32 __code__ = (func);                       \
        if (UNLIKELY(__code__ != EOK)) {               \
            CM_SET_ERROR(ERR_SYSTEM_CALL, __code__);   \
            return;                                    \
        }                                              \
    } while (0)

// for snprintf_s/sprintf_s..., return CM_ERROR if error
#define SPRINTF_RETURN_IFERR(func)                        \
    do {                                               \
        int32 __code__ = (func);                       \
        if (UNLIKELY(__code__ == -1)) {                \
            CM_SET_ERROR(ERR_SYSTEM_CALL, __code__);   \
            return CM_ERROR;                           \
        }                                              \
    } while (0)


/***********************************************************************************************
*                                      DEBUG OUTPUT                                            *
***********************************************************************************************/

#ifndef UNIV_DEBUG
#define UNIV_DEBUG              /* ut_ad, ut_d */
#endif

#ifndef UNIV_MEMORY_DEBUG
#define UNIV_MEMORY_DEBUG       /* detect memory leaks etc */
#endif

#ifndef UNIV_MEMROY_VALGRIND
//#define UNIV_MEMROY_VALGRIND    /* Enable extra Valgrind instrumentation */
#endif

#ifndef UNIV_DEBUG_OUTPUT
#define UNIV_DEBUG_OUTPUT       /* DBUG_* */
#endif

#ifndef UNIV_MUTEX_DEBUG
#define UNIV_MUTEX_DEBUG        /* mutex and latch */
#endif


#ifdef __cplusplus
}
#endif

#endif  /*_CM_TYPE_H*/
