#include "cm_error.h"
#include "cm_dbug.h"
#include "cm_memory.h"
#include "cm_file.h"
#include "cm_log.h"
#include "m_ctype.h"

THREAD_LOCAL error_info_t       g_tls_error = { 0 };
THREAD_LOCAL errinfo_stack_t    g_tls_errinfo_stack = { 0 };


const char *g_error_desc[ERR_CODE_CEIL] = { 0 };

static char* remove_annotation_or_space(CHARSET_INFO *cs, char* src, char* end, char* errmsg_file)
{
retry:

    for (; src < end && my_isspace(cs, *src); src++);

    ut_a(my_mbcharlen(cs, *src) == 1 && my_mbcharlen(cs, *(src + 1)) == 1);
    if (*src == '/' && (*(src + 1) == '*' || *(src + 1) == '/')) {
        char* ret_ptr = my_strchr(cs, src, end, '\r');
        if (ret_ptr && *src == '/' && *(src + 1) == '*') {
            char* ptr = ret_ptr - 1;
            // remote space at the end
            for (; ptr > src && my_isspace(cs, *ptr); ptr--);
            if (!(*ptr == '/' && *(ptr - 1) == '*')) {
                *ret_ptr = '\0';
                LOGGER_FATAL(LOGGER, LOG_MODULE_COMMON, "Invalid errmsg file: %s, context = %s", errmsg_file, src);
                ut_error;
            }
        }
        //return ret_ptr == NULL ? NULL : ret_ptr + 1;
        if (ret_ptr == NULL || (ret_ptr + 1) == NULL) {
            return NULL;
        }
        src = ret_ptr + 1;
        goto retry;
    }

    return src;
}

static bool32 error_message_append(CHARSET_INFO *cs, char* err_msg, char* end, char* errmsg_file)
{
    if (err_msg == end) {
        return TRUE;
    }

    char* err_code = remove_annotation_or_space(cs, err_msg, end, errmsg_file);
    char* ptr = err_code;
    while (ptr < end && *ptr != ',') {
        ut_a(my_mbcharlen(cs, *ptr) == 1 && my_mbcharlen(cs, *(ptr+1)) == 1);
        ptr++;
    }

    if (err_code == ptr || *ptr != ',') {
        return FALSE;
    }

    if (*ptr == ',') {
        *ptr = '\0';

        int32 err_no = atoi(err_code);
        if (err_no >= ERR_CODE_CEIL) {
            *ptr = ',';
            return FALSE;
        }

        ptr = remove_annotation_or_space(cs, ptr+1, end, errmsg_file);
        g_error_desc[err_no] = ptr;
        LOGGER_DEBUG(LOGGER, LOG_MODULE_COMMON, "error_message_append: err code = %d err desc = %s", err_no, g_error_desc[err_no]);
    }

    return TRUE;
}

bool32 error_message_init(char* errmsg_file)
{
    bool32    ret;
    os_file_t file;

#ifdef __WIN__
    ret = os_open_file(errmsg_file, OS_FILE_OPEN, OS_FILE_SYNC, &file);
#else
    ret = os_open_file(errmsg_file, OS_FILE_OPEN, OS_FILE_SYNC, &file);
#endif

    if (ret == FALSE) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_COMMON, "Failed to open errmsg file: %s", errmsg_file);
        return FALSE;
    }

    uint32 read_bytes;
    uint32 size = 1024 * 64;
    char* buf = (char *)ut_malloc(size);
    ret = os_pread_file(file, 0, buf, size, &read_bytes);
    if (!ret || read_bytes == 0) {
        os_close_file(file);
        LOGGER_FATAL(LOGGER, LOG_MODULE_COMMON, "Invalid errmsg file: %s", errmsg_file);
        return FALSE;
    }

    CHARSET_INFO *cs = &my_charset_latin1;
    char* ptr = remove_annotation_or_space(cs, buf, buf + read_bytes, errmsg_file);
    char* ret_ptr;
    while (ptr && ptr - buf < read_bytes && (ret_ptr = my_strchr(cs, ptr, buf + read_bytes, '\r'))) {
        *ret_ptr = '\0';
        if (error_message_append(cs, ptr, ret_ptr - 1, errmsg_file) == FALSE) {
            os_close_file(file);
            LOGGER_FATAL(LOGGER, LOG_MODULE_COMMON, "Invalid errmsg file: %s, context = %s", errmsg_file, ptr);
            return FALSE;
        }
        ptr = remove_annotation_or_space(cs, ret_ptr + 1, buf + read_bytes, errmsg_file);
    }
    if (ptr && ptr - buf < read_bytes) {
        if (error_message_append(cs, ptr, buf + read_bytes, errmsg_file) == FALSE) {
            os_close_file(file);
            LOGGER_FATAL(LOGGER, LOG_MODULE_COMMON, "Invalid errmsg file: %s, context = %s", errmsg_file, ptr);
            return FALSE;
        }
    }

    os_close_file(file);

    return TRUE;
}


