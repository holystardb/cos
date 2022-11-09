#include "cm_log.h"
#include "cm_datetime.h"
#include "cm_mutex.h"
#include "cm_thread.h"

const char* g_log_level_desc[] = {
    "UNKOWN",
    "FATAL",
    "ERROR",
    "WARN",
    "NOTICE",
    "INFO",
    "DEBUG",
    "TRACE"
};

#define LOG_FILE_NAME_MAX_LEN      255
#define LOG_BUF_SIZE               512

char             g_log_file[LOG_FILE_NAME_MAX_LEN];
FILE            *g_log_file_handle = NULL;

spinlock_t       g_log_lock;
log_level_t      g_log_level = LOG_INFO;


static bool32 log_create_or_open_file()
{
    errno_t err;

    err = fopen_s(&g_log_file_handle, g_log_file, "at");
    if (err != 0) {
        return FALSE;
    }
    return TRUE;
}

bool32 log_init(log_level_t level, char *log_path, char *file_name)
{
    g_log_level = level;

    if (log_path) {
        int len;
        date_clock_t clock;

        current_clock(&clock);
        len = snprintf(g_log_file, LOG_FILE_NAME_MAX_LEN, "%s/%s_%d-%02d-%02d-%02d-%02d-%02d.log",
            log_path, file_name, clock.year, clock.month, clock.day, clock.hour, clock.minute, clock.second);
        g_log_file[len] = '\0';

        if (!log_create_or_open_file()) {
            return FALSE;
        }
    }

    spin_lock_init(&g_log_lock);

    return TRUE;
}

void log_to_file(log_level_t log_level, const char *fmt,...)
{
    int             len;
    va_list         ap;
    date_clock_t    clock;
    char            errbuf[LOG_BUF_SIZE];
    char            str[LOG_BUF_SIZE];

    current_clock(&clock);

    va_start(ap, fmt);
    len = vsnprintf(errbuf, sizeof(errbuf), fmt, ap);
    errbuf[len] = '\0';
    len = snprintf(str, LOG_BUF_SIZE, "%d-%02d-%02d %02d:%02d:%02d.%03d [%s] [thread %lu] %s\n",
        clock.year, clock.month, clock.day,
        clock.hour, clock.minute, clock.second,
        clock.milliseconds, g_log_level_desc[log_level], os_thread_get_curr_id(), errbuf);
    str[len] = '\0';
    va_end(ap);

    spin_lock(&g_log_lock, NULL);
    if (g_log_file_handle) {
        fwrite(str, len, 1, g_log_file_handle);
    }
    fwrite(str, len, 1, stderr);
    spin_unlock(&g_log_lock);

    if (g_log_file_handle) {
        fflush(g_log_file_handle);
    }
    fflush(stderr);
}

void log_to_stderr(log_level_t log_level, const char *fmt,...)
{
    int             len;
    va_list         ap;
    date_clock_t    clock;
    char            errbuf[LOG_BUF_SIZE];

    current_clock(&clock);

    va_start(ap, fmt);
#ifdef __WIN__
    len = vsnprintf(errbuf, sizeof(errbuf), fmt, ap);
    errbuf[len] = '\0';
    fprintf(stderr, "%d-%02d-%02d %02d:%02d:%02d.%03d [%s] [thread %lu] %s\n",
        clock.year, clock.month, clock.day,
        clock.hour, clock.minute, clock.second,
        clock.milliseconds, g_log_level_desc[log_level], os_thread_get_curr_id(), errbuf);
    fflush(stderr);
#else
    len = vsnprintf(errbuf, sizeof(errbuf), fmt, ap);
    errbuf[len] = '\0';
    fprintf(stderr, "%d-%02d-%02d %02d:%02d:%02d.%03d [%s] [thread %lu] %s\n",
        clock.year, clock.month, clock.day,
        clock.hour, clock.minute, clock.second,
        clock.milliseconds, g_log_level_desc[log_level], os_thread_get_curr_id(), errbuf);
    fflush(stderr);
#endif
    va_end(ap);
}

