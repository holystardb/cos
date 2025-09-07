#ifndef _CM_LOG_H
#define _CM_LOG_H

#include "cm_type.h"
#include "cm_mutex.h"

#ifdef __cplusplus
extern "C" {
#endif

// log level

#define LOG_LEVEL_FATAL           1
#define LOG_LEVEL_ERROR           2
#define LOG_LEVEL_WARN            4
#define LOG_LEVEL_NOTICE          8
#define LOG_LEVEL_INFO            16
#define LOG_LEVEL_DEBUG           32
#define LOG_LEVEL_TRACE           64
#define LOG_LEVEL_DETAIL          128

#define LOG_LEVEL_NONE            0
#define LOG_LEVEL_CRITICAL        7   // include fatal, error, warn
#define LOG_LEVEL_DEFAULT         31  // include fatal, error, warn, notice
#define LOG_LEVEL_ALL             255


// log module
enum log_module_id_t {
    LOG_MODULE_ALL = 0,
    LOG_MODULE_COMMON,
    LOG_MODULE_DBUG,
    LOG_MODULE_GUC,
    LOG_MODULE_SECUREC,

    /* 10 - 29*/
    LOG_MODULE_RWLOCK = 10,
    LOG_MODULE_VIRTUAL_MEM,
    LOG_MODULE_MEMORY,
    LOG_MODULE_STACK,

    /* vio: 30 - 39 */
    LOG_MODULE_VIO = 30,
    LOG_MODULE_VIO_SOCKET,
    LOG_MODULE_VIO_EPOLL,
    LOG_MODULE_VIO_REACTOR,
    LOG_MODULE_VIO_PAT,

    /* thread: 40 - 49 */
    LOG_MODULE_THREAD = 40,
    LOG_MODULE_THREAD_GROUP,
    LOG_MODULE_THREAD_POOL,

    /* time: 50 - 59 */
    LOG_MODULE_TIMER = 50,
    LOG_MODULE_TIMEWHEEL,
    LOG_MODULE_TIMEZONE,


    /* charset: 60 - 69 */
    LOG_MODULE_CHARSET = 60,
    LOG_MODULE_STRINGS,

    /* charset: 70 - 79 */
    LOG_MODULE_STARTUP = 70,
    LOG_MODULE_SHUTDOWN,

    /* sql type: 100 - 199 */
    LOG_MODULE_PARSER = 100,
    LOG_MODULE_OPTIMIZER,
    LOG_MODULE_REWRITE,
    LOG_MODULE_EXECUTOR,
    LOG_MODULE_CATALOG,
    LOG_MODULE_STATISTICS,
    LOG_MODULE_PLAN,
    LOG_MODULE_THREADS,


    LOG_MODULE_DATETIME,
    LOG_MODULE_STRING,
    LOG_MODULE_TEXT,

    /* storage: 200 - 299 */
    LOG_MODULE_RECOVERY = 200,
    LOG_MODULE_ARCHIVE,
    LOG_MODULE_BACKUP,
    LOG_MODULE_REPLICATION,
    LOG_MODULE_LOGICAL_DECODE,
    LOG_MODULE_CHECKPOINT,
    LOG_MODULE_DICTIONARY,
    LOG_MODULE_IBUF,
    LOG_MODULE_BUFFERPOOL,
    LOG_MODULE_TABLESPACE,
    LOG_MODULE_FSP,
    LOG_MODULE_SESSION,
    LOG_MODULE_TRX,
    LOG_MODULE_ROLLSEGMENT,
    LOG_MODULE_UNDO,
    LOG_MODULE_REDO,
    LOG_MODULE_CTRLFILE,
    LOG_MODULE_MTR,

    LOG_MODULE_PAGE,
    LOG_MODULE_HEAP,
    LOG_MODULE_HEAP_FSM,
    LOG_MODULE_ROW,
    LOG_MODULE_TOAST,
    LOG_MODULE_INDEX_BTREE,
    LOG_MODULE_INDEX_HASH,

    //
    LOG_MODULE_END = 1024
};


//
#define LOG_BUFFER_SIZE            SIZE_M(4)  //must be power of 2
#define LOG_DATA_PREFIX_SIZE       51
#define LOG_FILE_MAX_SIZE          SIZE_G(1)

#define LOG_WRITE_BUFFER_SIZE      1023
#define LOGINFO_SLOT_COUNT         SIZE_K(8) //must be power of 2

#define LOGINFO_SLOT_STATUS_INIT   0
#define LOGINFO_SLOT_STATUS_COPIED 1


struct loginfo_slot_t {
    volatile uint32  start_pos;
    volatile uint16  data_len;
    volatile uint16  status;
};

class log_info {
public:
    log_info();

    bool32 init(uint32 log_level, char *log_path, char *file_name);
    void destroy();

    void log_to_stderr(const char* log_level_desc, uint32 module_id, const char *fmt, ...);
    void log_to_file(const char* log_level_desc, uint32 module_id, const char *fmt, ...);
    void log_file_flush();
    void coredump_to_file(char **symbol_strings, int len_symbols);

    uint32 get_log_level() {
        return m_log_level;
    }

    void set_log_level(uint32 log_level) {
        m_log_level = log_level;
    }

    void set_log_thread_exited() {
        m_is_exited = TRUE;
    }
    bool32 get_log_thread_exited()
    {
        return m_is_exited;
    }

    void set_module_log_level(uint32 module_id, uint32 log_level) {
        ut_ad(module_id > 0 && module_id < LOG_MODULE_END);
        m_modules_log_level[module_id] = log_level;
    }

    uint32 get_module_log_level(uint32 module_id) {
        ut_ad(module_id >=0 && module_id < LOG_MODULE_END);
        return m_modules_log_level[module_id];
    }

    bool32 is_print_module_log(uint32 module_id, uint32 log_level) {
        return m_modules_log_level[module_id] & log_level;
    }

    os_thread_ret_t log_write_file_thread_run();

private:
    bool32 open_or_create_log_file();
    bool32 log_rotate_file_if_needed();
    bool32 rename_log_file();
    bool32 get_reserve_buf(uint32 len, uint32 &slot_index);
    uint32 get_buf_slots(uint32 &slot_start_index);


private:
    char*            m_buffer;
    volatile uint64  m_buffer_write_offset;
    volatile uint64  m_buffer_sync_offset;

    volatile uint32  m_file_slot_index;
    volatile uint32  m_buffer_slot_index;
    loginfo_slot_t   m_slot[LOGINFO_SLOT_COUNT];

    uint64           m_file_offset;
    os_file_t        m_log_handle;
    os_thread_t      m_write_file_thread;
    volatile bool32  m_is_exited;
    uint32           m_log_file_create_time;
    uint32           m_log_file_index;
    char             m_log_file_path[CM_FILE_NAME_BUF_SIZE];
    char             m_log_file_name[CM_FILE_NAME_BUF_SIZE];
    char             m_log_file_path_name[CM_FILE_NAME_BUF_SIZE];

    uint32           m_log_level;
    mutex_t          m_mutex;
    uint32           m_modules_log_level[LOG_MODULE_END];
};




#define LOGGER_TRACE(LOGINFO, module_id, format, ...)                       \
    do {                                                                    \
        if (!(LOGINFO.is_print_module_log(module_id, LOG_LEVEL_TRACE))) {   \
            break;                                                          \
        }                                                                   \
        LOGINFO.log_to_file("[TRACE] ", module_id, format, ##__VA_ARGS__);  \
    } while (0);

#define LOGGER_DEBUG(LOGINFO, module_id, format, ...)                       \
    do {                                                                    \
        if (!(LOGINFO.is_print_module_log(module_id, LOG_LEVEL_DEBUG))) {   \
            break;                                                          \
        }                                                                   \
        LOGINFO.log_to_file("[DEBUG] ", module_id, format, ##__VA_ARGS__);  \
    } while (0);

#define LOGGER_NOTICE(LOGINFO, module_id, format, ...)                      \
    do {                                                                    \
        if (!(LOGINFO.is_print_module_log(module_id, LOG_LEVEL_NOTICE))) {  \
            break;                                                          \
        }                                                                   \
        LOGINFO.log_to_file("[NOTICE]", module_id, format, ##__VA_ARGS__);  \
    } while (0);

#define LOGGER_INFO(LOGINFO, module_id, format, ...)                        \
    do {                                                                    \
        if (!(LOGINFO.is_print_module_log(module_id, LOG_LEVEL_INFO))) {    \
            break;                                                          \
        }                                                                   \
        LOGINFO.log_to_file("[INFO]  ", module_id, format, ##__VA_ARGS__);  \
    } while (0);

#define LOGGER_WARN(LOGINFO, module_id, format, ...)                        \
    do {                                                                    \
        if (!(LOGINFO.is_print_module_log(module_id, LOG_LEVEL_WARN))) {    \
            break;                                                          \
        }                                                                   \
        LOGINFO.log_to_file("[WARN]  ", module_id, format, ##__VA_ARGS__);  \
    } while (0);

#define LOGGER_ERROR(LOGINFO, module_id, format, ...)                       \
    do {                                                                    \
        if (!(LOGINFO.is_print_module_log(module_id, LOG_LEVEL_ERROR))) {   \
            break;                                                          \
        }                                                                   \
        LOGINFO.log_to_file("[ERROR] ", module_id, format, ##__VA_ARGS__);  \
    } while (0);

#define LOGGER_FATAL(LOGINFO, module_id, format, ...)                       \
    do {                                                                    \
        if (!(LOGINFO.is_print_module_log(module_id, LOG_LEVEL_FATAL))) {   \
            break;                                                          \
        }                                                                   \
        LOGINFO.log_to_file("[FATAL] ", module_id, format, ##__VA_ARGS__);  \
    } while (0);


#define LOGGER_PANIC_CHECK(LOGINFO, module_id, condition, format, ...)          \
    do {                                                                        \
        if (UNLIKELY(!(condition))) {                                           \
            LOGINFO.log_to_file("[FATAL] ", module_id, format, ##__VA_ARGS__);  \
            ut_error;                                                           \
        }                                                                       \
    } while (0);



// ---------------------------------------------------------------------------------------------------

extern log_info    LOGGER;


#ifdef __cplusplus
}
#endif

#endif  /* _CM_LOG_H */

