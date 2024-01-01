#ifndef _CM_LOG_H
#define _CM_LOG_H

#include "cm_type.h"
#include "cm_mutex.h"

#ifdef __cplusplus
extern "C" {
#endif

class log_info {
public:
    log_info();

    bool32 log_init(log_level_t log_level, char *log_path, char *file_name, bool32 batch_flush = FALSE);
    void log_to_stderr(log_level_t log_level, const char *fmt,...);
    void log_to_file(log_level_t log_level, const char *fmt, ...);
    void log_file_flush();
    void coredump_to_file(char **symbol_strings, int len_symbols);

public:
    log_level_t basic_level;

private:
    bool32 create_log_file();

private:
    char      write_buffer[512];
    uint32    write_pos;
    bool32    write_to_buffer_flag;
    uint64    log_file_size;
    uint32    log_file_create_time;
    char      log_file_path[256];
    char      log_file_name[63];
    os_file_t log_handle;
    bool32    batch_flush_flag;
    uint64    batch_flush_pos;
    mutex_t   mutex;
};




#define LOGGER_TRACE(LOGINFO, format, ...)                      \
    do {                                                        \
        if (LOGINFO.basic_level < LOG_TRACE) {                  \
            break;                                              \
        }                                                       \
        LOGINFO.log_to_file(LOG_TRACE, format, ##__VA_ARGS__);  \
    } while (0);

#define LOGGER_DEBUG(LOGINFO, format, ...)                      \
    do {                                                        \
        if (LOGINFO.basic_level < LOG_DEBUG) {                  \
            break;                                              \
        }                                                       \
        LOGINFO.log_to_file(LOG_DEBUG, format, ##__VA_ARGS__);  \
    } while (0);

#define LOGGER_NOTICE(LOGINFO, format, ...)                     \
    do {                                                        \
        if (LOGINFO.basic_level < LOG_NOTICE) {                 \
            break;                                              \
        }                                                       \
        LOGINFO.log_to_file(LOG_NOTICE, format, ##__VA_ARGS__); \
    } while (0);

#define LOGGER_INFO(LOGINFO, format, ...)                       \
    do {                                                        \
        if (LOGINFO.basic_level < LOG_INFO) {                   \
            break;                                              \
        }                                                       \
        LOGINFO.log_to_file(LOG_INFO, format, ##__VA_ARGS__);   \
    } while (0);

#define LOGGER_WARN(LOGINFO, format, ...)                       \
    do {                                                        \
        if (LOGINFO.basic_level < LOG_WARN) {                   \
            break;                                              \
        }                                                       \
        LOGINFO.log_to_file(LOG_WARN, format, ##__VA_ARGS__);   \
    } while (0);

#define LOGGER_ERROR(LOGINFO, format, ...)                      \
    do {                                                        \
        if (LOGINFO.basic_level < LOG_ERROR) {                  \
            break;                                              \
        }                                                       \
        LOGINFO.log_to_file(LOG_ERROR, format, ##__VA_ARGS__);  \
    } while (0);

#define LOGGER_FATAL(LOGINFO, format, ...)                      \
    do {                                                        \
        if (LOGINFO.basic_level < LOG_FATAL) {                  \
            break;                                              \
        }                                                       \
        LOGINFO.log_to_file(LOG_FATAL, format, ##__VA_ARGS__);  \
    } while (0);




extern log_info    LOGGER;


#ifdef __cplusplus
}
#endif

#endif  /* _CM_LOG_H */

