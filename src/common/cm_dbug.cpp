#include "cm_dbug.h"
#include "cm_datetime.h"
#include "cm_log.h"

#include <signal.h>       /* for signal */  
#ifndef __WIN__
#include <execinfo.h>     /* for backtrace() */  
#endif

#define MAX_BACK_TRACE_DEPTH    100

/* This is used to eliminate compiler warnings */
uint64 ut_dbg_zero = 0;

/* If this is set to TRUE all threads will stop into the next assertion and assert */
bool32 ut_dbg_stop_threads = FALSE;

/* Null pointer used to generate memory trap */
uint64* ut_dbg_null_ptr = NULL;


#define DEBUG_MSG_BUF 256

int         do_debug = 0;
log_info    DBUG_LOGGER;


bool32 dbug_init(char *log_path, char *file_name, int level)
{
    do_debug = level;
    if (do_debug == 0) {
        return FALSE;
    }

    DBUG_LOGGER.init(LOG_LEVEL_TRACE, log_path, file_name);

    return TRUE;
}

void dbug_end()
{
    DBUG_LOGGER.log_file_flush();
}

void dbug_print(const char *_file_, uint32 _line_, _dbug_stack_frame_ *_stack_frame_, const char *format, ...)
{
    va_list       args;
    char          buf[DEBUG_MSG_BUF];
    int           len;
    const char   *filename;

#ifdef __WIN__
    filename = strrchr(_file_, '\\');
#else
    filename = strrchr(_file_, '/');
#endif
    if (filename) {
        filename++;
    }

    len = snprintf(buf, DEBUG_MSG_BUF, "[%s : %d] [%s]", filename, _line_, _stack_frame_->func);

    va_start(args, format);
    len += vsnprintf(buf + len, DEBUG_MSG_BUF - len, format, args);
    va_end(args);

    LOGGER_DEBUG(DBUG_LOGGER, LOG_MODULE_DBUG, "%s", buf);
}

void dbug_enter(const char *_file_, uint32 _line_, _dbug_stack_frame_ *_stack_frame_, const char *func)
{
    char          buf[DEBUG_MSG_BUF];
    const char   *filename;

#ifdef __WIN__
    filename = strrchr(_file_, '\\');
#else
    filename = strrchr(_file_, '/');
#endif
    if (filename) {
        filename++;
    }

    _stack_frame_->func = func;
    snprintf(buf, DEBUG_MSG_BUF, "[%s : %d] enter %s", filename, _line_, func);

    LOGGER_DEBUG(DBUG_LOGGER, LOG_MODULE_DBUG, "%s", buf);
}

void dbug_leave(const char *_file_, uint32 _line_, _dbug_stack_frame_ *_stack_frame_)
{
    char          buf[DEBUG_MSG_BUF];
    const char   *filename;

#ifdef __WIN__
    filename = strrchr(_file_, '\\');
#else
    filename = strrchr(_file_, '/');
#endif
    if (filename) {
        filename++;
    }

    snprintf(buf, DEBUG_MSG_BUF, "[%s : %d] leave %s", filename, _line_, _stack_frame_->func);

    LOGGER_DEBUG(DBUG_LOGGER, LOG_MODULE_DBUG, "%s", buf);
}

void dbug_print_stacktrace()
{
    fprintf(stderr, "dbug_print_stacktrace:");
#ifdef __WIN__
#else
    void  *buffer[MAX_BACK_TRACE_DEPTH];
    char **symbol_strings;

    int len_symbols = backtrace(buffer, MAX_BACK_TRACE_DEPTH);
    symbol_strings = backtrace_symbols(buffer, len_symbols);
    if (symbol_strings != NULL) {
        //
        LOGGER.coredump_to_file(symbol_strings, len_symbols);

        //
        for (int i = 0; i < len_symbols; i++) {
            fprintf(stderr, "%s", symbol_strings[i]);
        }
        fflush(stderr);

        free(symbol_strings);
    }
#endif
}

