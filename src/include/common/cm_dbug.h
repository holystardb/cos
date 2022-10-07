#ifndef _CM_DBUG_H
#define _CM_DBUG_H

#include <assert.h>
#include <stdlib.h>
#include "cm_thread.h"

extern uint64 ut_dbg_zero; /* This is used to eliminate compiler warnings */
extern bool32 ut_dbg_stop_threads;
extern uint64 *ut_dbg_null_ptr;

#define ut_a(EXPR) {\
    uint64 dbg_i;\
    if (!((uint64)(EXPR) + ut_dbg_zero)) {\
        fprintf(stderr, "Assertion failure in thread %lu in file %s line %lu\n",\
            os_thread_get_curr_id(), __FILE__, (uint32)__LINE__);\
        fprintf(stderr, "We intentionally generate a memory trap.\n");\
        ut_dbg_stop_threads = TRUE;\
        dbg_i = *(ut_dbg_null_ptr);\
        if (dbg_i) {\
            ut_dbg_null_ptr = NULL;\
        }\
    }\
    if (ut_dbg_stop_threads) {\
        fprintf(stderr, "Thread %lu stopped in file %s line %lu\n",\
            os_thread_get_curr_id(), __FILE__, (uint32)__LINE__);\
            os_thread_sleep(1000000000);\
    }\
}

#define ut_error {\
    uint64 dbg_i;\
    fprintf(stderr, "Assertion failure in thread %lu in file %s line %lu\n",\
        os_thread_get_curr_id(), __FILE__, (uint32)__LINE__);\
    fprintf(stderr, "We intentionally generate a memory trap.\n");\
    ut_dbg_stop_threads = TRUE;\
    dbg_i = *(ut_dbg_null_ptr);\
    printf("%lld", dbg_i);\
}

#ifdef UNIV_DEBUG
#define ut_ad(EXPR) ut_a(EXPR)
#define ut_d(EXPR)  {EXPR;}
#else
#define ut_ad(EXPR)
#define ut_d(EXPR)
#endif

#define UT_NOT_USED(A) A = A

/*----------------------------------------------------------------------*/
/*
#define DBUG_ENTER(a1)
#define DBUG_LEAVE
#define DBUG_RETURN(a1)                 do { return(a1); } while(0)
#define DBUG_VOID_RETURN                do { return; } while(0)
#define DBUG_EXECUTE(keyword,a1)        do { } while(0)
#define DBUG_EXECUTE_IF(keyword,a1)     do { } while(0)
#define DBUG_EVALUATE(keyword,a1,a2) (a2)
#define DBUG_EVALUATE_IF(keyword,a1,a2) (a2)
#define DBUG_PRINT(keyword,arglist)     do { } while(0)
#define DBUG_PUSH(a1)                   do { } while(0)
#define DBUG_SET(a1)                    do { } while(0)
#define DBUG_SET_INITIAL(a1)            do { } while(0)
#define DBUG_POP()                      do { } while(0)
#define DBUG_PROCESS(a1)                do { } while(0)
#define DBUG_SETJMP(a1) setjmp(a1)
#define DBUG_LONGJMP(a1) longjmp(a1)
#define DBUG_DUMP(keyword,a1,a2)        do { } while(0)
#define DBUG_END()                      do { } while(0)
#define DBUG_ASSERT(A)                  do { } while(0)
#define DBUG_LOCK_FILE                  do { } while(0)
#define DBUG_FILE (stderr)
#define DBUG_UNLOCK_FILE                do { } while(0)
#define DBUG_EXPLAIN(buf,len)
#define DBUG_EXPLAIN_INITIAL(buf,len)
#define DEBUGGER_OFF                    do { } while(0)
#define DEBUGGER_ON                     do { } while(0)
#define DBUG_ABORT()                    do { } while(0)
#define DBUG_CRASH_ENTER(func)
#define DBUG_CRASH_RETURN(val)          do { return(val); } while(0)
#define DBUG_CRASH_VOID_RETURN          do { return; } while(0)
#define DBUG_SUICIDE()                  do { } while(0)
*/

typedef struct _dbug_stack_frame_ {
    const char *func;   /* function name */
} _dbug_stack_frame_;

#ifdef DEBUG_OUTPUT

extern int do_debug;

/* There's no if(do_debug) check on DBUG_INIT or DBUG_ASSERT */
#define DBUG_INIT(OUTFILE, LEVEL) dbug_init(OUTFILE, LEVEL)
#define DBUG_END() if (do_debug) dbug_end()
#define DBUG_ABORT()
#define DBUG_EXIT()

#define DBUG_ASSERT(x) assert(x)

#define DBUG_PRINT(a, ...)             \
    if (do_debug) dbug_print(__FILE__, __LINE__, &_db_stack_frame_, a, __VA_ARGS__)

#define DBUG_ENTER(a)                                  \
    struct _debug_stack_frame_ _db_stack_frame_;        \
    if (do_debug) dbug_enter(__FILE__, __LINE__, &_db_stack_frame_, a)

#define DBUG_LEAVE                     \
    if (do_debug) dbug_leave(__FILE__, __LINE__, &_db_stack_frame_)

#define DBUG_RETURN(a)         \
    do {                        \
        DBUG_LEAVE;            \
        return (a);             \
    } while (0)

#define DBUG_VOID_RETURN        \
    do {                        \
        DBUG_LEAVE;            \
        return;                 \
    } while (0)

#else

#define DBUG_INIT(OUTFILE, LEVEL)
#define DBUG_END()
#define DBUG_ABORT()
#define DBUG_EXIT()

#define DBUG_ASSERT(x)

#define DBUG_PRINT(a, ...)
#define DBUG_ENTER(a)
#define DBUG_RETURN(a)                 do { return(a); } while(0)
#define DBUG_VOID_RETURN               do { return; } while(0)

#endif

bool32 dbug_init(const char *file, int level);
void dbug_print(char *_file_, uint _line_, _dbug_stack_frame_ *_stack_frame_, const char *format, ...);
void dbug_enter(char *_file_, uint _line_, _dbug_stack_frame_ *_stack_frame_, char *func);
void dbug_leave(char *_file_, uint _line_, _dbug_stack_frame_ *_stack_frame_);
void dbug_end();

#endif

