#include "cm_dbug.h"
#include "cm_date.h"

/* This is used to eliminate compiler warnings */
uint64 ut_dbg_zero = 0;

/* If this is set to TRUE all threads will stop into the next assertion and assert */
bool32 ut_dbg_stop_threads = FALSE;

/* Null pointer used to generate memory trap */
uint64* ut_dbg_null_ptr = NULL;


#define DEBUG_MSG_BUF 1022

FILE *debug_outfile = NULL;
int do_debug = 0;

bool32 debug_init(const char *filename, int level)
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
    else {
        //debug_outfile = fdopen(STDERR_FILENO, "a");
    }

    return TRUE;
}

void debug_end()
{
    fflush(debug_outfile);
}

void debug_print(char *_file_, uint _line_, _dbug_stack_frame_ *_stack_frame_, const char *format, ...)
{
    va_list args;
    char buf[DEBUG_MSG_BUF + 2];
    int len;
    date_clock_t clock;

    current_clock(&clock);
    len = snprintf(buf, DEBUG_MSG_BUF, "%d-%02d-%02d %02d:%02d:%02d.%03d [%lu %s()] [%s : %d] ",
        clock.year, clock.month, clock.day, clock.hour, clock.minute, clock.second, clock.milliseconds,
        os_thread_get_curr_id(), _stack_frame_->func, _file_, _line_);

    va_start(args, format);
    len += vsnprintf(buf + len, DEBUG_MSG_BUF - len, format, args);
    va_end(args);

    snprintf(buf + len, DEBUG_MSG_BUF + 2 - len, "\n");
    fputs(buf, debug_outfile);
}

void debug_enter(char *_file_, uint _line_, _dbug_stack_frame_ *_stack_frame_, char *func)
{
    date_clock_t clock;
    char buf[DEBUG_MSG_BUF];

    _stack_frame_->func = func;

    current_clock(&clock);
    snprintf(buf, DEBUG_MSG_BUF, "%d-%02d-%02d %02d:%02d:%02d.%03d [%lu] [%s : %d] enter %s\n",
        clock.year, clock.month, clock.day, clock.hour, clock.minute, clock.second, clock.milliseconds,
        os_thread_get_curr_id(), _file_, _line_, func);
    fprintf(debug_outfile, "%s\n", buf);
}

void debug_leave(char *_file_, uint _line_, _dbug_stack_frame_ *_stack_frame_)
{
    date_clock_t clock;
    char buf[DEBUG_MSG_BUF];

    current_clock(&clock);
    snprintf(buf, DEBUG_MSG_BUF, "%d-%02d-%02d %02d:%02d:%02d.%03d [%lu] [%s : %d] leave %s\n",
       clock.year, clock.month, clock.day, clock.hour, clock.minute, clock.second, clock.milliseconds,
       os_thread_get_curr_id(), _file_, _line_, _stack_frame_->func);
    fprintf(debug_outfile, "%s\n", buf);
}

