#ifndef _CM_DBUG_H
#define _CM_DBUG_H

#include <assert.h>
#include <stdlib.h>
#include "cm_thread.h"

extern uint64  ut_dbg_zero; /* This is used to eliminate compiler warnings */
extern bool32  ut_dbg_stop_threads;
extern uint64 *ut_dbg_null_ptr;

#define ut_a(EXPR) {                                                            \
    uint64 dbg_i;                                                               \
    if (!((uint64)(EXPR) + ut_dbg_zero)) {                                      \
        fprintf(stderr, "\nAssertion failure in thread %lu in file %s line %lu\n",\
            os_thread_get_curr_id(), __FILE__, (uint32)__LINE__);               \
        fprintf(stderr, "We intentionally generate a memory trap.\n");          \
        ut_dbg_stop_threads = TRUE;                                             \
        dbug_print_stacktrace();                                                \
        dbg_i = *(ut_dbg_null_ptr);                                             \
        if (dbg_i) {                                                            \
            ut_dbg_null_ptr = NULL;                                             \
        }                                                                       \
    }                                                                           \
    if (ut_dbg_stop_threads) {                                                  \
        fprintf(stderr, "Thread %lu stopped in file %s line %lu\n",             \
            os_thread_get_curr_id(), __FILE__, (uint32)__LINE__);               \
            os_thread_sleep(1000000000);                                        \
    }                                                                           \
}

#define ut_error {                                                              \
    uint64 dbg_i;                                                               \
    fprintf(stderr, "\nAssertion failure in thread %lu in file %s line %lu\n",    \
        os_thread_get_curr_id(), __FILE__, (uint32)__LINE__);                   \
    fprintf(stderr, "We intentionally generate a memory trap.\n");              \
    ut_dbg_stop_threads = TRUE;                                                 \
    dbug_print_stacktrace();                                                    \
    dbg_i = *(ut_dbg_null_ptr);                                                 \
    printf("%lld", dbg_i);                                                      \
}




#ifdef UNIV_DEBUG_OUTPUT
#define ut_ad(EXPR) ut_a(EXPR)
#define ut_d(EXPR)  {EXPR;}
#else
#define ut_ad(EXPR)
#define ut_d(EXPR)
#endif

#define UT_NOT_USED(A)

typedef struct _st_dbug_stack_frame_ {
    const char *func;   /* function name */
} _dbug_stack_frame_;

#ifdef UNIV_DEBUG_OUTPUT

extern int do_debug;

/* There's no if(do_debug) check on DBUG_INIT or DBUG_ASSERT */
#define DBUG_INIT(LOG_PATH, FILE_NAME, LEVEL) dbug_init(LOG_PATH, FILE_NAME, LEVEL)
#define DBUG_END() if (do_debug) dbug_end()
#define DBUG_ABORT()
#define DBUG_EXIT()

#define DBUG_ASSERT(x) assert(x)

#define DBUG_PRINT(a, ...)             \
    if (do_debug) dbug_print(__FILE__, __LINE__, &_db_stack_frame_, a, __VA_ARGS__)

#define DBUG_ENTER(a)                                                   \
    _dbug_stack_frame_ _db_stack_frame_;                               \
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

#define DBUG_INIT(LOG_PATH, FILE_NAME, LEVEL)
#define DBUG_END()
#define DBUG_ABORT()
#define DBUG_EXIT()

#define DBUG_ASSERT(x)

#define DBUG_PRINT(a, ...)
#define DBUG_ENTER(a)
#define DBUG_RETURN(a)                 do { return(a); } while(0)
#define DBUG_VOID_RETURN               do { return; } while(0)
#define DBUG_TRACE
#endif

extern bool32 dbug_init(char *log_path, char *file_name, int level);
extern void dbug_print(char *_file_, uint _line_, _dbug_stack_frame_ *_stack_frame_, const char *format, ...);
extern void dbug_leave(char *_file_, uint _line_, _dbug_stack_frame_ *_stack_frame_);
extern void dbug_enter(char *_file_, uint _line_, _dbug_stack_frame_ *_stack_frame_, char *func);
extern void dbug_end();

extern void dbug_print_stacktrace();


#endif

