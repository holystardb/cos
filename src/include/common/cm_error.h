#ifndef _CM_ERROR_H
#define _CM_ERROR_H

#include "cm_type.h"

typedef struct st_source_location {
    uint16 line;
    uint16 column;
} source_location_t;

#define ERROR_INFO_MSG_LENGTH         256
#define ERROR_INFO_STACK_MAX_LEVEL    16

typedef struct st_error_info_t {
    int32       code;
    uint32      line;
    char*       file;
    source_location_t loc;
    char        message[ERROR_INFO_MSG_LENGTH];
    bool8       is_ignored;
    bool8       is_ignore_log;
    bool8       is_full;
    bool8       reserved;
} error_info_t;

typedef struct st_errinfo_stack_t {
    uint32       level;
    error_info_t err_info[ERROR_INFO_STACK_MAX_LEVEL];
} errinfo_stack_t;

// -----------------------------------------------------------------------------------------------

extern THREAD_LOCAL error_info_t       g_tls_error;
extern THREAD_LOCAL errinfo_stack_t    g_tls_errinfo_stack;


#define CM_THROW_ERROR(err_no, module_id, ...)                                   \
    do {                                                                         \
        g_tls_error.code = err_no;                                               \
        g_tls_error.loc.line = 0;                                                \
        g_tls_error.loc.column = 0;                                              \
        LOGGER_ERROR(LOGGER, module_id, g_error_desc[err_no], ##__VA_ARGS__);    \
    } while (0)

extern inline void set_stack_error_message(const char *fmt, ...)
{
    int32 len;
    va_list ap;

    va_start(ap, fmt);
    len = vsnprintf(g_tls_errinfo_stack.err_info[g_tls_errinfo_stack.level].message, ERROR_INFO_MSG_LENGTH, fmt, ap);
    g_tls_errinfo_stack.err_info[g_tls_errinfo_stack.level].message[len] = '\0';
    va_end(ap);
}

#define CM_SET_ERROR(err_no, ...)                                         \
    do {                                                                             \
        if (g_tls_errinfo_stack.level < ERROR_INFO_STACK_MAX_LEVEL) {                \
            g_tls_errinfo_stack.err_info[g_tls_errinfo_stack.level].code = err_no;   \
            g_tls_errinfo_stack.err_info[g_tls_errinfo_stack.level].file = __FILE__; \
            g_tls_errinfo_stack.err_info[g_tls_errinfo_stack.level].line = __LINE__; \
            set_stack_error_message(g_error_desc[err_no], ##__VA_ARGS__);            \
            g_tls_errinfo_stack.level++;                                             \
        }                                                                            \
    } while (0)

#define CM_RESET_ERROR_INFO_STACK                          \
    do {                                                   \
        g_tls_errinfo_stack.level = 0;                     \
    } while (0)


extern bool32 error_message_init(char* errmsg_file);

// ------------------------------------------------------------------------------------------------

extern const char *g_error_desc[ERR_CODE_CEIL];



#endif   // _CM_ERROR_H

