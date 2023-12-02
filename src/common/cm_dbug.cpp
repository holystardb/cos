#include "cm_dbug.h"
#include "cm_datetime.h"

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


#define DEBUG_MSG_BUF 1022

FILE *debug_outfile = NULL;
int do_debug = 0;

bool32 dbug_init(const char *filename, int level)
{
    errno_t err;

    do_debug = level;
    if (do_debug == 0) {
        return FALSE;
    }

    if(filename) {
        err = fopen_s(&debug_outfile, filename, "w");
        if (err != 0) {
            return FALSE;
        }
    }

    return TRUE;
}

void dbug_end()
{
    if (debug_outfile) {
        fflush(debug_outfile);
    }
}

void dbug_print(char *_file_, uint _line_, _dbug_stack_frame_ *_stack_frame_, const char *format, ...)
{
    va_list args;
    char buf[DEBUG_MSG_BUF];
    int len;
    date_clock_t clock;

    char *filename = strrchr(_file_, '/');
    if (filename == NULL) {
        filename = strrchr(_file_, '\\');
    }
    if (filename) {
        filename++;
    }

    current_clock(&clock);
    len = snprintf(buf, DEBUG_MSG_BUF, "%d-%02d-%02d %02d:%02d:%02d.%03d [%lu] [%s : %d] [%s] ",
        clock.year, clock.month, clock.day, clock.hour, clock.minute, clock.second, clock.milliseconds,
        os_thread_get_curr_id(), filename, _line_, _stack_frame_->func);

    va_start(args, format);
    len += vsnprintf(buf + len, DEBUG_MSG_BUF - len, format, args);
    va_end(args);

    if (debug_outfile) {
        fprintf(debug_outfile, "%s\n", buf);
    } else {
        fprintf(stderr, "%s\n", buf);
        fflush(stderr);
    }
}

void dbug_enter(char *_file_, uint _line_, _dbug_stack_frame_ *_stack_frame_, char *func)
{
    date_clock_t clock;
    char buf[DEBUG_MSG_BUF];

    char *filename = strrchr(_file_, '/');
    if (filename == NULL) {
        filename = strrchr(_file_, '\\');
    }
    if (filename) {
        filename++;
    }

    _stack_frame_->func = func;

    current_clock(&clock);
    snprintf(buf, DEBUG_MSG_BUF, "%d-%02d-%02d %02d:%02d:%02d.%03d [%lu] [%s : %d] enter %s",
        clock.year, clock.month, clock.day, clock.hour, clock.minute, clock.second, clock.milliseconds,
        os_thread_get_curr_id(), filename, _line_, func);

    if (debug_outfile) {
        fprintf(debug_outfile, "%s\n", buf);
    } else {
        fprintf(stderr, "%s\n", buf);
        fflush(stderr);
    }
}

void dbug_leave(char *_file_, uint _line_, _dbug_stack_frame_ *_stack_frame_)
{
    date_clock_t clock;
    char buf[DEBUG_MSG_BUF];

    char *filename = strrchr(_file_, '/');
    if (filename == NULL) {
        filename = strrchr(_file_, '\\');
    }
    if (filename) {
        filename++;
    }

    current_clock(&clock);
    snprintf(buf, DEBUG_MSG_BUF, "%d-%02d-%02d %02d:%02d:%02d.%03d [%lu] [%s : %d] leave %s",
       clock.year, clock.month, clock.day, clock.hour, clock.minute, clock.second, clock.milliseconds,
       os_thread_get_curr_id(), filename, _line_, _stack_frame_->func);

    if (debug_outfile) {
        fprintf(debug_outfile, "%s\n", buf);
    } else {
        fprintf(stderr, "%s\n", buf);
        fflush(stderr);
    }
}

void dbug_print_stacktrace()
{
    fprintf(stderr, "dbug_print_stacktrace:\n");
#ifdef __WIN__
#else
    void  *buffer[MAX_BACK_TRACE_DEPTH];
    char **symbol_strings;

    int len_symbols = backtrace(buffer, MAX_BACK_TRACE_DEPTH);
    symbol_strings = backtrace_symbols(buffer, len_symbols);
    if (symbol_strings != NULL) {
        for (int i = 0; i < len_symbols; i++) {
            log_stderr(LOG_FATAL, "%s", symbol_strings[i]);
        }
        free(symbol_strings);
    }
#endif
}

