#ifndef _CM_LOG_H
#define _CM_LOG_H

#include "cm_type.h"


#ifdef __cplusplus
extern "C" {
#endif

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

bool32 log_init(log_level_t log_level, char *log_path, char *file_name);
void log_to_stderr(log_level_t log_level, const char *fmt,...);
void log_to_file(log_level_t log_level, const char *fmt, ...);

#define LOG_PRINT_TRACE(format, ...)                            \
    do {                                                        \
        if (g_log_level < LOG_TRACE) {                          \
            break;                                              \
        }                                                       \
        log_to_file(LOG_TRACE, format, ##__VA_ARGS__);          \
    } while (0);


#define LOG_PRINT_DEBUG(format, ...)                            \
    do {                                                        \
        if (g_log_level < LOG_DEBUG) {                          \
            break;                                              \
        }                                                       \
        log_to_file(LOG_DEBUG, format, ##__VA_ARGS__);          \
    } while (0);

#define LOG_PRINT_NOTICE(format, ...)                           \
    do {                                                        \
        if (g_log_level < LOG_NOTICE) {                         \
            break;                                              \
        }                                                       \
        log_to_file(LOG_NOTICE, format, ##__VA_ARGS__);         \
    } while (0);

#define LOG_PRINT_INFO(format, ...)                             \
    do {                                                        \
        if (g_log_level < LOG_INFO) {                           \
            break;                                              \
        }                                                       \
        log_to_file(LOG_INFO, format, ##__VA_ARGS__);           \
    } while (0);

#define LOG_PRINT_WARN(format, ...)                             \
    do {                                                        \
        if (g_log_level < LOG_WARN) {                           \
            break;                                              \
        }                                                       \
        log_to_file(LOG_WARN, format, ##__VA_ARGS__);           \
    } while (0);

#define LOG_PRINT_ERROR(format, ...)                            \
    do {                                                        \
        if (g_log_level < LOG_ERROR) {                          \
            break;                                              \
        }                                                       \
        log_to_file(LOG_ERROR, format, ##__VA_ARGS__);          \
    } while (0);

#define LOG_PRINT_FATAL(format, ...)                            \
    do {                                                        \
        if (g_log_level < LOG_FATAL) {                          \
            break;                                              \
        }                                                       \
        log_to_file(LOG_FATAL, format, ##__VA_ARGS__);          \
    } while (0);


extern log_level_t      g_log_level;

#ifdef __cplusplus
}
#endif

#endif  /* _CM_LOG_H */

