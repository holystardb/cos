#ifndef _CM_LOG_H
#define _CM_LOG_H

#include "cm_type.h"
#include "cm_mutex.h"

#ifdef __cplusplus
extern "C" {
#endif

#define LOG_LEVEL_FATAL           1
#define LOG_LEVEL_ERROR           2
#define LOG_LEVEL_WARN            4
#define LOG_LEVEL_NOTICE          8
#define LOG_LEVEL_INFO            16
#define LOG_LEVEL_DEBUG           32
#define LOG_LEVEL_TRACE           64

#define LOG_LEVEL_NONE            0
#define LOG_LEVEL_ALL             127
#define LOG_LEVEL_CRITICAL        15  // include fatal, error, warn, notice

#define LOG_WRITE_BUFFER_SIZE      1023

class log_info {
public:
    log_info();

    bool32 log_init(uint32 level, char *log_path, char *file_name, bool32 batch_flush = FALSE);
    void log_to_stderr(char* log_level_desc, const char *file, uint32 line, const char *fmt, ...);
    void log_to_file(char* log_level_desc, const char *file, uint32 line, const char *fmt, ...);
    void log_file_flush();
    void coredump_to_file(char **symbol_strings, int len_symbols);

    void set_print_stderr(bool32 val) {
        is_print_stderr = val;
    }

    bool32 get_is_print_stderr() {
        return is_print_stderr;
    }

public:
    uint32    log_level;

private:
    bool32 create_log_file();

private:
    char      write_buffer[LOG_WRITE_BUFFER_SIZE+1];
    uint32    write_pos;
    bool32    write_to_buffer_flag;
    uint64    log_file_size;
    uint32    log_file_create_time;
    bool32    is_print_stderr;
    char      log_file_path[CM_FILE_NAME_BUF_SIZE];
    char      log_file_name[CM_FILE_NAME_BUF_SIZE];
    os_file_t log_handle;
    bool32    batch_flush_flag;
    uint64    batch_flush_pos;
    mutex_t   mutex;
};




#define LOGGER_TRACE(LOGINFO, format, ...)                      \
    do {                                                        \
        if (!(LOGINFO.log_level & LOG_LEVEL_TRACE)) {           \
            break;                                              \
        }                                                       \
        LOGINFO.log_to_file("[TRACE] ", (char *)__FILE__, (uint32)__LINE__, format, ##__VA_ARGS__);  \
    } while (0);

#define LOGGER_DEBUG(LOGINFO, format, ...)                      \
    do {                                                        \
        if (!(LOGINFO.log_level & LOG_LEVEL_DEBUG)) {           \
            break;                                              \
        }                                                       \
        LOGINFO.log_to_file("[DEBUG] ", (char *)__FILE__, (uint32)__LINE__, format, ##__VA_ARGS__);  \
    } while (0);

#define LOGGER_NOTICE(LOGINFO, format, ...)                     \
    do {                                                        \
        if (!(LOGINFO.log_level & LOG_LEVEL_NOTICE)) {          \
            break;                                              \
        }                                                       \
        LOGINFO.log_to_file("[NOTICE]", (char *)__FILE__, (uint32)__LINE__, format, ##__VA_ARGS__); \
    } while (0);

#define LOGGER_INFO(LOGINFO, format, ...)                       \
    do {                                                        \
        if (!(LOGINFO.log_level & LOG_LEVEL_INFO)) {            \
            break;                                              \
        }                                                       \
        LOGINFO.log_to_file("[INFO]  ", (char *)__FILE__, (uint32)__LINE__, format, ##__VA_ARGS__);   \
    } while (0);

#define LOGGER_WARN(LOGINFO, format, ...)                       \
    do {                                                        \
        if (!(LOGINFO.log_level & LOG_LEVEL_WARN)) {            \
            break;                                              \
        }                                                       \
        LOGINFO.log_to_file("[WARN]  ", (char *)__FILE__, (uint32)__LINE__, format, ##__VA_ARGS__);   \
    } while (0);

#define LOGGER_ERROR(LOGINFO, format, ...)                      \
    do {                                                        \
        if (!(LOGINFO.log_level & LOG_LEVEL_ERROR)) {           \
            break;                                              \
        }                                                       \
        LOGINFO.log_to_file("[ERROR] ", (char *)__FILE__, (uint32)__LINE__, format, ##__VA_ARGS__);  \
    } while (0);

#define LOGGER_FATAL(LOGINFO, format, ...)                      \
    do {                                                        \
        if (!(LOGINFO.log_level & LOG_LEVEL_FATAL)) {           \
            break;                                              \
        }                                                       \
        LOGINFO.log_to_file("[FATAL] ", (char *)__FILE__, (uint32)__LINE__, format, ##__VA_ARGS__);  \
    } while (0);


#define LOGGER_PANIC_CHECK(LOGINFO, condition, format, ...)                                             \
    do {                                                                                                \
        if (UNLIKELY(!(condition))) {                                                                   \
            LOGINFO.log_to_file("[FATAL] ", (char *)__FILE__, (uint32)__LINE__, format, ##__VA_ARGS__);  \
            ut_error;                                                                                   \
        }                                                                                               \
    } while (0);



// ---------------------------------------------------------------------------------------------------

extern log_info    LOGGER;


#ifdef __cplusplus
}
#endif

#endif  /* _CM_LOG_H */

