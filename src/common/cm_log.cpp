#include "cm_log.h"
#include "cm_datetime.h"
#include "cm_mutex.h"
#include "cm_memory.h"
#include "cm_thread.h"
#include "cm_file.h"
#include "securec.h"

#ifdef __WIN__
#include "io.h"
#else
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dlfcn.h>
#endif // __WIN__

log_info    LOGGER;


static os_thread_ret_t log_write_file_thread_main(log_info* info)
{
    return info->log_write_file_thread_run();
}

log_info::log_info()
{
    m_buffer = NULL;
    m_buffer_write_offset = 0;
    m_buffer_sync_offset = 0;
    m_file_slot_index = 0;
    m_buffer_slot_index = 0;
    for (uint32 i = 0; i < LOGINFO_SLOT_COUNT; i++) {
        m_slot[i].status = LOGINFO_SLOT_STATUS_INIT;
        m_slot[i].start_pos = 0;
        m_slot[i].data_len = 0;
    }

    m_log_file_name[0] = '\0';
    m_log_file_path[0] = '\0';
    m_file_offset = 0;
    m_log_handle = OS_FILE_INVALID_HANDLE;
    m_log_file_create_time = 0;
    m_log_file_index = 0;

    m_is_exited = FALSE;
    mutex_create(&m_mutex);
    m_write_file_thread = INVALID_OS_THREAD;

    m_log_level = LOG_LEVEL_DEFAULT;
    m_modules_log_level[0] = LOG_LEVEL_ALL;
    for (uint32 i = 1; i < LOG_MODULE_END; i++) {
        m_modules_log_level[i] = LOG_LEVEL_DEFAULT;
    }
}

bool32 log_info::init(uint32 log_level, char *log_path, char *file_name)
{
    m_log_level = log_level;

    if (!log_path || !file_name) {
        return FALSE;
    }

    strncpy_s(m_log_file_path, CM_FILE_NAME_MAX_LEN, log_path, strlen(log_path));
    strncpy_s(m_log_file_name, CM_FILE_NAME_MAX_LEN, file_name, strlen(file_name));

    m_buffer = (char *)ut_malloc(LOG_BUFFER_SIZE);
    if (m_buffer == NULL) {
        return FALSE;
    }

    os_file_init();
    if (!open_or_create_log_file()) {
        ut_free(m_buffer);
        m_buffer = NULL;
        return FALSE;
    }

    m_write_file_thread = thread_start(log_write_file_thread_main, this);
    if (!os_thread_is_valid(m_write_file_thread)) {
        os_close_file(m_log_handle);
        m_log_handle = OS_FILE_INVALID_HANDLE;
        ut_free(m_buffer);
        m_buffer = NULL;
        return FALSE;
    }

    return TRUE;
}

void log_info::destroy()
{
    m_is_exited = TRUE;
    if (m_write_file_thread != INVALID_OS_THREAD) {
        os_thread_join(m_write_file_thread);
        m_write_file_thread = INVALID_OS_THREAD;
    }
    if (m_log_handle != OS_FILE_INVALID_HANDLE) {
        os_close_file(m_log_handle);
        m_log_handle = OS_FILE_INVALID_HANDLE;
    }
    if (m_buffer != NULL) {
        ut_free(m_buffer);
        m_buffer = NULL;
    }
}

os_thread_ret_t log_info::log_write_file_thread_run()
{
    uint32 slot_start_index, slot_count, offset, data_len;
    uint32 skip_flush_count = 0, need_flush_bytes = 0;
    const uint32 flush_min_bytes = SIZE_K(512), flush_max_skip_count = 1000;

    while (!m_is_exited) {
        //
        if (m_log_handle == OS_FILE_INVALID_HANDLE) {
            os_thread_sleep(1000);
            continue;
        }

        slot_count = get_buf_slots(slot_start_index);
        if (slot_count == 0) {
            if (log_rotate_file_if_needed()) {
                need_flush_bytes = 0;
                skip_flush_count = 0;
                continue;
            }

            if (need_flush_bytes) {
                if (need_flush_bytes >= flush_min_bytes || skip_flush_count >= flush_max_skip_count) {
                    os_fsync_file(m_log_handle);
                    need_flush_bytes = 0;
                    skip_flush_count = 0;
                } else {
                    skip_flush_count++;
                    os_thread_sleep(1000);
                }
            } else {
                os_thread_sleep(1000);
            }
            continue;
        }

        ut_ad(slot_start_index < LOGINFO_SLOT_COUNT);
        offset = m_slot[slot_start_index].start_pos;
        data_len = m_slot[slot_start_index].data_len;
        for (uint32 i = 1; i < slot_count; i++) {
            data_len += m_slot[((slot_start_index + i) & (LOGINFO_SLOT_COUNT - 1))].data_len;
        }
        if (offset + data_len <= LOG_BUFFER_SIZE) {
            if (!os_pwrite_file(m_log_handle, m_file_offset, m_buffer + offset, data_len)) {
                char err_info[CM_ERR_MSG_MAX_LEN];
                os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
                printf("os_pwrite_file: %s\n", err_info);
                ut_error;
            }
        } else {
            if (!os_pwrite_file(m_log_handle, m_file_offset, m_buffer + offset, LOG_BUFFER_SIZE - offset)) {
                char err_info[CM_ERR_MSG_MAX_LEN];
                os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
                printf("os_pwrite_file: %s\n", err_info);
                ut_error;
            }
            if (!os_pwrite_file(m_log_handle, m_file_offset, m_buffer, data_len - (LOG_BUFFER_SIZE - offset))) {
                char err_info[CM_ERR_MSG_MAX_LEN];
                os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
                printf("os_pwrite_file: %s\n", err_info);
                ut_error;
            }
        }
        m_file_offset += data_len;
        m_buffer_sync_offset += data_len;

        // 
        //printf("slot reset: count %d, [%d - %d)\n", slot_count, slot_start_index, slot_start_index + slot_count);
        ut_ad(slot_count > 0 && slot_count < LOGINFO_SLOT_COUNT);
        for (uint32 i = 0; i < slot_count; i++) {
            m_slot[((slot_start_index + i) & (LOGINFO_SLOT_COUNT - 1))].status = LOGINFO_SLOT_STATUS_INIT;
        }

        need_flush_bytes += data_len;
        if (need_flush_bytes >= flush_min_bytes) {
            os_fsync_file(m_log_handle);
            need_flush_bytes = 0;
            skip_flush_count = 0;
        } else {
            skip_flush_count++;
        }

        if (log_rotate_file_if_needed()) {
            need_flush_bytes = 0;
            skip_flush_count = 0;
        }
    }

    os_thread_exit(NULL);
    OS_THREAD_DUMMY_RETURN;
}

bool32 log_info::rename_log_file()
{
    char new_name[CM_FILE_NAME_BUF_SIZE];
    uint32 len = snprintf(new_name, CM_FILE_NAME_MAX_LEN, "%s.%d", m_log_file_path_name, m_log_file_index);
    if (len == strlen(m_log_file_path_name)) {
        return FALSE;
    }
    return os_file_rename(m_log_file_path_name, new_name);
}

bool32 log_info::open_or_create_log_file()
{
    int          len;
    date_clock_t clock;

    current_clock(&clock);
    if (m_log_file_path[strlen(m_log_file_path) - 1] == SRV_PATH_SEPARATOR) {
        len = snprintf(m_log_file_path_name, CM_FILE_NAME_MAX_LEN, "%s%s_%d-%02d-%02d.log",
            m_log_file_path, m_log_file_name, clock.year, clock.month, clock.day);
    } else {
        len = snprintf(m_log_file_path_name, CM_FILE_NAME_MAX_LEN, "%s%c%s_%d-%02d-%02d.log",
            m_log_file_path, SRV_PATH_SEPARATOR, m_log_file_name, clock.year, clock.month, clock.day);
    }
    m_log_file_path_name[len] = '\0';

    m_log_file_create_time = clock.year * 10000 + clock.month * 100 + clock.day;

    os_file_type_t type;
    bool32 exists;

    if (!os_file_status(m_log_file_path_name, &exists, &type)) {
        return FALSE;
    }

    os_open_file(m_log_file_path_name, exists ? OS_FILE_OPEN : OS_FILE_CREATE, 0, &m_log_handle);
    if (m_log_handle == OS_FILE_INVALID_HANDLE) {
        char err_info[CM_ERR_MSG_MAX_LEN];
        os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
        fprintf(stderr, "%d-%02d-%02d %02d:%02d:%02d.%03d [%s] [%u] %s, error %s\n",
            clock.year, clock.month, clock.day,
            clock.hour, clock.minute, clock.second,
            clock.milliseconds, "[ERROR] ", os_thread_get_curr_id(),
            "can not create log file", err_info);
        fflush(stderr);
        return FALSE;
    }

    uint64 file_size;
    if (!os_file_get_size(m_log_handle, &file_size)) {
        return FALSE;
    }
    m_file_offset = file_size;

    return TRUE;
}

//rotate and create a new log file
bool32 log_info::log_rotate_file_if_needed()
{
    date_clock_t clock;

    current_clock(&clock);
    if (m_log_file_create_time == clock.year * 10000 + clock.month * 100 + clock.day) {
        if (m_file_offset < LOG_FILE_MAX_SIZE) {
            return FALSE;
        }
        m_log_file_index++;
    } else {
        m_log_file_index = 0;
    }

    if (m_log_handle == OS_FILE_INVALID_HANDLE) {
        return FALSE;
    }

    os_fsync_file(m_log_handle);
    if (!os_close_file(m_log_handle)) {
        ut_error;
    }

    //
    if (m_log_file_index > 0) {
        rename_log_file();
    }

    if (!open_or_create_log_file()) {
        ut_error;
    }
    m_file_offset = 0;

    return TRUE;
}

uint32 log_info::get_buf_slots(uint32 &slot_start_index)
{
    uint32 count = 0;
    const uint32 slot_count_per_write = LOGINFO_SLOT_COUNT - SIZE_K(1);

    slot_start_index = m_file_slot_index;
    ut_ad(slot_start_index < LOGINFO_SLOT_COUNT);
    while (count < slot_count_per_write && m_slot[m_file_slot_index].status == LOGINFO_SLOT_STATUS_COPIED) {
        count++;
        ut_ad(count < LOGINFO_SLOT_COUNT);
        m_file_slot_index = ((m_file_slot_index+1) & (LOGINFO_SLOT_COUNT - 1));
    }

    return count;
}

bool32 log_info::get_reserve_buf(uint32 len, uint32 &slot_index)
{
    uint64 start_pos;

    mutex_enter(&m_mutex, NULL);
    slot_index = m_buffer_slot_index;
    start_pos = m_buffer_write_offset;
    m_buffer_slot_index = ((m_buffer_slot_index+1) & (LOGINFO_SLOT_COUNT - 1));
    m_buffer_write_offset += len;
    mutex_exit(&m_mutex);
    ut_ad(slot_index < LOGINFO_SLOT_COUNT);

    while (!m_is_exited) {
        if (start_pos + len - m_buffer_sync_offset > LOG_BUFFER_SIZE) {
            os_thread_sleep(100);
            continue;
        }

        //
        if (m_slot[slot_index].status != LOGINFO_SLOT_STATUS_INIT) {
            os_thread_sleep(100);
            continue;
        }

        m_slot[slot_index].start_pos = (start_pos & (LOG_BUFFER_SIZE - 1));
        m_slot[slot_index].data_len = len;
        return TRUE;
    }

    return FALSE;
}

void log_info::log_to_file(const char* log_level_desc, uint32 module_id, const char *fmt, ...)
{
    int32           len;
    va_list         ap;
    date_clock_t    clock;
    char            buf[LOG_WRITE_BUFFER_SIZE + LOG_DATA_PREFIX_SIZE + 1 /* \n */ + 1 /* \0 */];
    uint32          slot_index;

    va_start(ap, fmt);
    len = vsnprintf(buf + LOG_DATA_PREFIX_SIZE, LOG_WRITE_BUFFER_SIZE, fmt, ap);
    buf[len + LOG_DATA_PREFIX_SIZE] = '\n';
    buf[len + LOG_DATA_PREFIX_SIZE + 1] = '\0';
    va_end(ap);

    len += LOG_DATA_PREFIX_SIZE + 1;
    if (!get_reserve_buf(len, slot_index)) {
        return;
    }
    ut_ad(slot_index < LOGINFO_SLOT_COUNT);

    current_clock(&clock);
    snprintf(buf, LOG_DATA_PREFIX_SIZE, "%d-%02d-%02d %02d:%02d:%02d.%03d %s [%04u] [%08u]",
        clock.year, clock.month, clock.day, clock.hour, clock.minute, clock.second,
        clock.milliseconds, log_level_desc, module_id, os_thread_get_curr_id());
    buf[LOG_DATA_PREFIX_SIZE - 1] = ' ';

    if (m_slot[slot_index].start_pos + len <= LOG_BUFFER_SIZE) {
        memcpy_s(m_buffer + m_slot[slot_index].start_pos, len, buf, len);
    } else {
        memcpy_s(m_buffer + m_slot[slot_index].start_pos, LOG_BUFFER_SIZE - m_slot[slot_index].start_pos,
                 buf, LOG_BUFFER_SIZE - m_slot[slot_index].start_pos);
        memcpy_s(m_buffer, len - (LOG_BUFFER_SIZE - m_slot[slot_index].start_pos),
                 buf + (LOG_BUFFER_SIZE - m_slot[slot_index].start_pos),
                 len - (LOG_BUFFER_SIZE - m_slot[slot_index].start_pos));
    }

    // write complete
    m_slot[slot_index].status = LOGINFO_SLOT_STATUS_COPIED;
}

void log_info::log_file_flush()
{
    if (m_log_handle != OS_FILE_INVALID_HANDLE) {
        os_fsync_file(m_log_handle);
    }
}

void log_info::coredump_to_file(char **symbol_strings, int len_symbols)
{
    int          len;
    char         name[CM_FILE_NAME_BUF_SIZE];
    date_clock_t clock;
    os_file_t    handle;
    uint32       pos = 0;

    if (m_log_file_path[0] == '\0') {
        return;
    }

    current_clock(&clock);
    if (m_log_file_path[strlen(m_log_file_path) - 1] == SRV_PATH_SEPARATOR) {
        len = snprintf(name, CM_FILE_NAME_MAX_LEN, "%scoredump_%d-%02d-%02d-%02d-%02d-%02d.log",
            m_log_file_path, clock.year, clock.month, clock.day,
            clock.hour, clock.minute, clock.second);
    } else {
        len = snprintf(name, CM_FILE_NAME_MAX_LEN, "%s%ccoredump_%d-%02d-%02d-%02d-%02d-%02d.log",
            m_log_file_path, SRV_PATH_SEPARATOR, clock.year, clock.month, clock.day,
            clock.hour, clock.minute, clock.second);
    }
    name[len] = '\0';

    os_open_file(name, OS_FILE_CREATE, 0, &handle);
    if (handle == OS_FILE_INVALID_HANDLE) {
        for (int i = 0; i < len_symbols; i++) {
            fprintf(stderr, "%s", symbol_strings[i]);
        }
        fflush(stderr);
        return;
    }

    for (int i = 0; i < len_symbols; i++) {
#ifdef __WIN__
        DWORD write_size = 0;
        bool32 ret;
        ret = WriteFile(handle, symbol_strings[i], (DWORD)strlen(symbol_strings[i]), &write_size, NULL);
        if (ret == 0 || strlen(symbol_strings[i]) != write_size) {
            
        }
#else
        ssize_t ret = pwrite64(handle, (char *)symbol_strings[i], pos, strlen(symbol_strings[i]));
        if ((uint32)ret == strlen(symbol_strings[i])) {
            pos += strlen(symbol_strings[i]);
        }
#endif
    }
    os_fsync_file(handle);
}


void log_info::log_to_stderr(const char* log_level_desc, uint32 module_id, const char *fmt, ...)
{
    int             len;
    va_list         ap;
    date_clock_t    clock;
    char            errbuf[LOG_WRITE_BUFFER_SIZE];

    current_clock(&clock);

    va_start(ap, fmt);
#ifdef __WIN__
    len = vsnprintf(errbuf, LOG_WRITE_BUFFER_SIZE, fmt, ap);
    errbuf[len] = '\0';
    fprintf(stderr, "%d-%02d-%02d %02d:%02d:%02d.%03d [%s] [%04u] [%08u] %s\n",
        clock.year, clock.month, clock.day,
        clock.hour, clock.minute, clock.second,
        clock.milliseconds, log_level_desc, module_id, os_thread_get_curr_id(), errbuf);
    fflush(stderr);
#else
    len = vsnprintf(errbuf, LOG_WRITE_BUFFER_SIZE, fmt, ap);
    errbuf[len] = '\0';
    fprintf(stderr, "%d-%02d-%02d %02d:%02d:%02d.%03d [%s] [%04u] [%08u] %s\n",
        clock.year, clock.month, clock.day,
        clock.hour, clock.minute, clock.second,
        clock.milliseconds, log_level_desc, module_id, os_thread_get_curr_id(), errbuf);
    fflush(stderr);
#endif
    va_end(ap);
}

