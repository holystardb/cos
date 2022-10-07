#ifndef _CM_LOG_H
#define _CM_LOG_H

#include "cm_type.h"


#ifdef __cplusplus
extern "C" {
#endif

#define M_LOG_BUF_SIZE              1024

typedef enum
{
    LOG_TO_STDERR   = 0,
    LOG_TO_FILE     = 1,
    LOG_TO_SYSLOG   = 2,
} log_type_t;

typedef enum
{
    LOG_FATAL    = 1,
    LOG_ERROR    = 2,
    LOG_WARN     = 4,
    LOG_NOTICE   = 8,
    LOG_INFO     = 16,
    LOG_DEBUG    = 32,
    LOG_TRACE    = 64,
} log_level_t;

bool32 log_init(uint32 log_level, log_type_t log_type, char* log_file);
void log_file_flush();

void log_stderr(log_level_t log_level, const char *fmt,...);
void log_file(log_level_t log_level, const char *fmt, ...);

#define LOG_PRINT(log_level, format, ...)                       \
    do {                                                        \
        if (!(log_level & g_log_level)) {                       \
            break;                                              \
        }                                                       \
        if (g_log_type == LOG_TO_STDERR) {                      \
            log_stderr(log_level, format, ##__VA_ARGS__);       \
        } else if (g_log_type == LOG_TO_FILE) {                 \
            log_file(log_level, format, ##__VA_ARGS__);         \
        } else {                                                \
        }                                                       \
    } while (0);

#define LOG_PRINT_STDERR(log_level, format, ...)                \
    do {                                                        \
        if (log_level & g_log_level) {                          \
            log_stderr(log_level, format, ##__VA_ARGS__);       \
        }                                                       \
    } while (0);

#define LOG_PRINT_FILE(log_level, format, ...)                  \
    do {                                                        \
        if (log_level & g_log_level) {                          \
            log_file(log_level, format, ##__VA_ARGS__);         \
        }                                                       \
    } while (0);

extern uint32      g_log_level;
extern log_type_t  g_log_type;

void sql_print_error(const char *format,...);

#ifdef __cplusplus
}
#endif

#endif  /* _CM_LOG_H */

