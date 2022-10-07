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

#if defined(__WIN__)
#define likely(x) (x)
#define unlikely(x) (x)
#else
#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)
#endif


/***********************************************************************************************
*                                      types                                                   *
***********************************************************************************************/

typedef unsigned char	        UCHAR;
typedef unsigned char	        uchar;
typedef unsigned char           BYTE;
typedef unsigned char           byte;
typedef unsigned char           BYTE8;
typedef unsigned char           byte8;
typedef unsigned char           UINT8;
typedef unsigned char           uint8;
typedef signed   char           INT8;
typedef signed   char           int8;
typedef signed   char           bool8;

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
typedef unsigned int            BOOL32;
typedef unsigned int            bool32;
typedef signed   int            INT32;
typedef signed   int            int32;

typedef long                    LONG;
typedef unsigned long           ULONG;
typedef unsigned long           ulong;

#ifndef __WIN__
typedef unsigned long long      WORD64;
#ifndef HP_UINT64
typedef unsigned long long      UINT64;
typedef unsigned long long      uint64;
#endif
typedef signed   long long      INT64;
typedef signed   long long      int64;
#else
typedef unsigned __int64        WORD64;
typedef unsigned __int64        uint64;
typedef __int64                 int64;
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

#ifndef SIZE_T_MAX
#define SIZE_T_MAX	((size_t)-1)
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
#define TRUE                    (1)	/* Logical true */
#define FALSE                   (0)	/* Logical false */
#endif

/** The 'undefined' value for a ulint */
#define ULINT_UNDEFINED		    ((uint32)(-1))

#define INT_MIN64 (~0x7FFFFFFFFFFFFFFFLL)
#define INT_MAX64 0x7FFFFFFFFFFFFFFFLL
#define INT_MIN32 (~0x7FFFFFFFL)
#define INT_MAX32 0x7FFFFFFFL
#define UINT_MAX32 0xFFFFFFFFL
#define INT_MIN24 (~0x007FFFFF)
#define INT_MAX24 0x007FFFFF
#define UINT_MAX24 0x00FFFFFF
#define INT_MIN16 (~0x7FFF)
#define INT_MAX16 0x7FFF
#define UINT_MAX16 0xFFFF
#define INT_MIN8 (~0x7F)
#define INT_MAX8 0x7F
#define UINT_MAX8 0xFF



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

typedef void (*os_callback_func) (callback_data_t *data);



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


#ifdef __cplusplus
}
#endif

#endif  /*_CM_TYPE_H*/
