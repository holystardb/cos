#include "cm_log.h"
#include "cm_datetime.h"
#include "cm_mutex.h"
#include "cm_thread.h"
#include "cm_file.h"


log_info    LOGGER;

log_info::log_info()
    : log_handle(OS_FILE_INVALID_HANDLE), log_level(LOG_LEVEL_CRITICAL),
      write_pos(0), write_to_buffer_flag(FALSE), log_file_size(0),
      log_file_create_time(0), batch_flush_flag(FALSE), batch_flush_pos(0)
{
    write_buffer[0] = '\0';
    log_file_name[0] = '\0';
    log_file_path[0] = '\0';

    is_print_stderr = FALSE;
    mutex_create(&mutex);
}

bool32 log_info::log_init(uint32 level, char *log_path, char *file_name, bool32 batch_flush)

{
    log_level = level;

    if (!log_path || !file_name) {
        return TRUE;
    }

    batch_flush_flag = batch_flush;
    strncpy_s(log_file_path, 256, log_path, strlen(log_path));
    strncpy_s(log_file_name, 63, file_name, strlen(file_name));

    if (!create_log_file()) {
        return FALSE;
    }

    return TRUE;
}

bool32 log_info::create_log_file()
{
    int          len;
    char         name[CM_FILE_NAME_BUF_SIZE];
    date_clock_t clock;

    current_clock(&clock);

    if (log_file_path[strlen(log_file_path) - 1] == SRV_PATH_SEPARATOR) {
        len = snprintf(name, CM_FILE_NAME_MAX_LEN, "%s%s_%d-%02d-%02d.log",
            log_file_path, log_file_name, clock.year, clock.month, clock.day);
    } else {
        len = snprintf(name, CM_FILE_NAME_MAX_LEN, "%s%c%s_%d-%02d-%02d.log",
            log_file_path, SRV_PATH_SEPARATOR, log_file_name, clock.year, clock.month, clock.day);
    }
    name[len] = '\0';

    log_file_create_time = clock.year * 10000 + clock.month * 100 + clock.day;

    os_file_type_t type;
    bool32 exists;

    /* New path must not exist. */
    if (!os_file_status(name, &exists, &type)) {
        return FALSE;
    }

#ifdef __WIN__
    log_handle = CreateFile(name,
        GENERIC_READ | GENERIC_WRITE, /* read and write access */
        FILE_SHARE_READ | FILE_SHARE_WRITE,/* file can be read by other processes */
        NULL,    /* default security attributes */
        exists ? OPEN_EXISTING : CREATE_NEW,
        FILE_ATTRIBUTE_NORMAL,//FILE_FLAG_OVERLAPPED,
        NULL);    /* no template file */
#else
    if (!exists) {
        log_handle = open(name, O_RDWR | O_CREAT | O_EXCL,
            S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
    } else {
        log_handle = open(name, O_RDWR);
    }
#endif

    if (log_handle == OS_FILE_INVALID_HANDLE) {
        char err_info[CM_ERR_MSG_MAX_LEN];
        os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);
        fprintf(stderr, "%d-%02d-%02d %02d:%02d:%02d.%03d [%s] [%lu] %s, error %s\n",
            clock.year, clock.month, clock.day,
            clock.hour, clock.minute, clock.second,
            clock.milliseconds, "[ERROR] ", os_thread_get_curr_id(),
            "can not create log file", err_info);
        fflush(stderr);
        return FALSE;
    }

    log_file_size = 0;

    if (!exists) {
        return TRUE;
    }

    if (!os_file_get_size(log_handle, &log_file_size)) {
        return FALSE;
    }

    DWORD           offset_low; /* least significant 32 bits of file offset where to read */
    LONG            offset_high; /* most significant 32 bits of offset */
    DWORD           ret2;

    offset_low = (log_file_size & 0xFFFFFFFF);
    offset_high = (log_file_size >> 32);

    ret2 = SetFilePointer(log_handle, offset_low, &offset_high, FILE_BEGIN);
    if (ret2 == 0xFFFFFFFF && GetLastError() != NO_ERROR) {
        return FALSE;
    }

    return TRUE;
}

void log_info::log_to_file(char* log_level_desc, const char *file, uint32 line, const char *fmt, ...)
{
    int             len, write_size = 0;
    va_list         ap;
    date_clock_t    clock;
    char            errbuf[LOG_WRITE_BUFFER_SIZE+1];
    bool32          need_retry = TRUE, need_flush = FALSE;

    va_start(ap, fmt);
    len = vsnprintf(errbuf, LOG_WRITE_BUFFER_SIZE, fmt, ap);
    errbuf[len] = '\0';
    va_end(ap);

retry:

    mutex_enter(&mutex, NULL);
    if (write_to_buffer_flag) {
        mutex_exit(&mutex);

        os_thread_sleep(100);
        goto retry;
    }
    write_to_buffer_flag = TRUE;
    mutex_exit(&mutex);

    current_clock(&clock);
    write_pos = snprintf(write_buffer, LOG_WRITE_BUFFER_SIZE,
        "%d-%02d-%02d %02d:%02d:%02d.%03d %s [%08lu] %s\n",
        clock.year, clock.month, clock.day, clock.hour, clock.minute, clock.second,
        clock.milliseconds, log_level_desc, os_thread_get_curr_id(), errbuf);
    write_buffer[write_pos] = '\0';

    if (log_handle != OS_FILE_INVALID_HANDLE) {
#ifdef __WIN__
        DWORD write_size = 0;
        bool32 ret;

        if (log_file_create_time != clock.year * 10000 + clock.month * 100 + clock.day) {
            if (os_close_file(log_handle)) {
                goto err_exit;
            }
            if (!create_log_file()) {
                goto err_exit;
            }
            log_file_size = 0;
        }

        //
        if (is_print_stderr) {
            fprintf(stderr, "%s", write_buffer);
            fflush(stderr);
        }

        //
        ret = WriteFile(log_handle, write_buffer, (DWORD)write_pos, &write_size, NULL);
        if (ret == 0 || write_pos != write_size) {
            char err_info[CM_ERR_MSG_MAX_LEN];
            os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);

            fprintf(stderr, "%d-%02d-%02d %02d:%02d:%02d.%03d [%s] [%lu] %s, error %s\n",
                clock.year, clock.month, clock.day,
                clock.hour, clock.minute, clock.second,
                clock.milliseconds, "[ERROR] ", os_thread_get_curr_id(),
                "error, can not write data to log file", err_info);
            fflush(stderr);
            goto err_exit;
        }
#else
        ssize_t ret = pwrite64(log_handle, (char *)write_buffer, write_pos, log_file_size);
        if ((uint32)ret != size) {
            char err_info[CM_ERR_MSG_MAX_LEN];
            os_file_get_last_error_desc(err_info, CM_ERR_MSG_MAX_LEN);

            fprintf(stderr, "%d-%02d-%02d %02d:%02d:%02d.%03d [%s] [%lu] %s, error %s\n",
                clock.year, clock.month, clock.day,
                clock.hour, clock.minute, clock.second,
                clock.milliseconds, "[ERROR] ", os_thread_get_curr_id(),
                "error, can not write data to log file", err_info);
            fflush(stderr);
            goto err_exit;
        }
#endif
    } else {
        fwrite(write_buffer, write_pos, 1, stderr);
        fflush(stderr);
    }

    mutex_enter(&mutex, NULL);
    log_file_size += write_pos;
    if (log_file_size - batch_flush_pos >= 1024 * 1024) {
        batch_flush_pos = log_file_size;
        need_flush = TRUE;
    }
    write_to_buffer_flag = FALSE;
    mutex_exit(&mutex);

    if (log_handle != OS_FILE_INVALID_HANDLE && (!batch_flush_flag || need_flush )) {
        os_fsync_file(log_handle);
    }

    return;

err_exit:

    mutex_enter(&mutex, NULL);
    write_to_buffer_flag = FALSE;
    mutex_exit(&mutex);
}

void log_info::log_file_flush()
{
    if (log_handle != OS_FILE_INVALID_HANDLE) {
        os_fsync_file(log_handle);
    }
}

void log_info::coredump_to_file(char **symbol_strings, int len_symbols)
{
    int          len;
    char         name[CM_FILE_NAME_BUF_SIZE];
    date_clock_t clock;
    os_file_t    handle;
    uint32       pos = 0;

    if (log_file_path[0] == '\0') {
        return;
    }

    current_clock(&clock);
    if (log_file_path[strlen(log_file_path) - 1] == SRV_PATH_SEPARATOR) {
        len = snprintf(name, CM_FILE_NAME_MAX_LEN, "%scoredump_%d-%02d-%02d-%02d-%02d-%02d.log",
            log_file_path, clock.year, clock.month, clock.day,
            clock.hour, clock.minute, clock.second);
    } else {
        len = snprintf(name, CM_FILE_NAME_MAX_LEN, "%s%ccoredump_%d-%02d-%02d-%02d-%02d-%02d.log",
            log_file_path, SRV_PATH_SEPARATOR, clock.year, clock.month, clock.day,
            clock.hour, clock.minute, clock.second);
    }
    name[len] = '\0';

#ifdef __WIN__
    handle = CreateFile(name,
        GENERIC_READ | GENERIC_WRITE, /* read and write access */
        FILE_SHARE_READ | FILE_SHARE_WRITE,/* file can be read by other processes */
        NULL,    /* default security attributes */
        CREATE_NEW,
        FILE_ATTRIBUTE_NORMAL,//FILE_FLAG_OVERLAPPED,
        NULL);    /* no template file */
#else
    handle = open(name, O_RDWR | O_CREAT | O_EXCL,
        S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
#endif

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


void log_info::log_to_stderr(char* log_level_desc, const char *file, uint32 line, const char *fmt, ...)
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
    fprintf(stderr, "%d-%02d-%02d %02d:%02d:%02d.%03d [%s] [%08lu] %s\n",
        clock.year, clock.month, clock.day,
        clock.hour, clock.minute, clock.second,
        clock.milliseconds, log_level_desc, os_thread_get_curr_id(), errbuf);
    fflush(stderr);
#else
    len = vsnprintf(errbuf, LOG_WRITE_BUFFER_SIZE, fmt, ap);
    errbuf[len] = '\0';
    fprintf(stderr, "%d-%02d-%02d %02d:%02d:%02d.%03d [%s] [%08lu] %s\n",
        clock.year, clock.month, clock.day,
        clock.hour, clock.minute, clock.second,
        clock.milliseconds, log_level_desc, os_thread_get_curr_id(), errbuf);
    fflush(stderr);
#endif
    va_end(ap);
}

