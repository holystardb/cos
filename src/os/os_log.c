#include "os_log.h"
#include "os_date.h"
#include "os_mutex.h"


uint32      g_log_level = 7;
log_type_t  g_log_type = 1;
char        g_log_file[256] = { 0 };
char*       g_log_buff = NULL;
uint32      g_log_buff_pos = 0;
spinlock_t  g_lock = 0;


const char* g_log_level_desc[] = {
    [LOG_FATAL]  = "FATAL",
    [LOG_ERROR]  = "ERROR",
    [LOG_WARN]   = "WARN",
    [LOG_INFO]   = "INFO",
    [LOG_NOTICE] = "NOTICE",
    [LOG_DEBUG]  = "DEBUG",
};

bool32 log_init(uint32 log_level, log_type_t log_type, char* log_file)
{
    if (log_type == LOG_TO_FILE && log_file == NULL && g_log_file[0] == '\0') {
        return FALSE;
    }
    
    if (log_type == LOG_TO_FILE) {
        if (log_file != NULL) {
            strcpy(g_log_file, log_file);
            g_log_file[255] = '\0';
        }

        if (g_log_buff == NULL) {
            g_log_buff = (char *)malloc(1024 * 1024);
            g_log_buff_pos = 0;
            g_lock = 0;
        }
    }
    
    g_log_level = log_level;
    g_log_type = log_type;

    return TRUE;
}

void log_stderr(log_level_t log_level, const char *fmt,...)
{
    int             len;
    va_list         ap;
    date_clock_t    clock;
    char            errbuf[M_LOG_BUF_SIZE];

    current_clock(&clock);

    va_start(ap, fmt);
#ifdef WIN32
    len = vsnprintf(errbuf, sizeof(errbuf), fmt, ap);
    errbuf[len] = '\0';
    fprintf(stderr, "%d-%02d-%02d %02d:%02d:%02d.%03d [%s] %s\n",
        clock.year, clock.month, clock.day,
        clock.hour, clock.minute, clock.second,
        clock.milliseconds, g_log_level_desc[log_level], errbuf);
    fflush(stderr);
#else
    len = vsnprintf(errbuf, sizeof(errbuf), fmt, ap);
    errbuf[len] = '\0';
    fprintf(stderr, "%d-%02d-%02d %02d:%02d:%02d.%03d [%s] %s\n",
        clock.year, clock.month, clock.day,
        clock.hour, clock.minute, clock.second,
        clock.milliseconds, g_log_level_desc[log_level], errbuf);
    fflush(stderr);
#endif
    va_end(ap);
}

void log_file(log_level_t log_level, const char *fmt,...)
{
    int             len;
    va_list         ap;
    date_clock_t    clock;
    char            errbuf[M_LOG_BUF_SIZE+1];
    char            str[M_LOG_BUF_SIZE+1];
    FILE*           fp;

    if (g_log_file[0] == '\0') {
        return;
    }

    current_clock(&clock);

    va_start(ap, fmt);

    len = vsnprintf(errbuf, sizeof(errbuf), fmt, ap);
    errbuf[len] = '\0';
    len = snprintf(str, M_LOG_BUF_SIZE, "%d-%02d-%02d %02d:%02d:%02d.%03d [%s] %s\n",
        clock.year, clock.month, clock.day,
        clock.hour, clock.minute, clock.second,
        clock.milliseconds, g_log_level_desc[log_level], errbuf);
    str[len] = '\0';
    
    va_end(ap);

    spin_lock(&g_lock, NULL);
    
    if (g_log_buff_pos + len >= 1024 * 1024) {
        fp = fopen(g_log_file, "at");
        if (fp != NULL) {
            fwrite(g_log_buff, g_log_buff_pos, 1, fp);
            fclose(fp);
        }
        g_log_buff_pos = 0;
    }
    
    strncpy(g_log_buff + g_log_buff_pos, str, len);
    g_log_buff_pos += len;
    
    spin_unlock(&g_lock);

    log_file_flush();
}

void log_file_flush()
{
    FILE*           fp;
    
    spin_lock(&g_lock, NULL);
    
    if (g_log_buff_pos == 0) {
        spin_unlock(&g_lock);
        return;
    }
    fp = fopen(g_log_file, "at");
    if (fp != NULL) {
        fwrite(g_log_buff, g_log_buff_pos, 1, fp);
        fclose(fp);
    }
    g_log_buff_pos = 0;
    
    spin_unlock(&g_lock);
}
