#ifndef _CM_ERROR_H
#define _CM_ERROR_H

#include "cm_type.h"

typedef enum en_err_no {
    ERR_UNSET = 0,

    /* The following are error codes */
    /* internal errors or common errors: 11 - 99 */
    ERR_READ_ONLY, /* Update operation attempted in a read-only transaction */    
    ERR_INTERRUPTED,
    ERR_QUE_THR_SUSPENDED,
    ERR_CORRUPTION,  /* data structure corruption noticed */

    /* os errors: 100 - 199 */
    ERR_ALLOC_MEMORY = 100,
    ERR_ALLOC_MEMORY_REACH_LIMIT = 101,
    ERR_OUT_OF_MEMORY = 102,
    ERR_STACK_OVERFLOW = 103,
    ERR_ALLOC_MEMORY_CONTEXT = 104,



    ERR_VM_NOT_OPEN = 110,
    ERR_VM_OPEN_LIMIT_EXCEED  = 111,

    ERR_IO_ERROR = 120, /* Generic IO error */
    ERR_SYSTEM_CALL = 121,


    /* invalid configuration errors: 200 - 299 */

    /* network errors: 300 - 399 */

    /* privilege error: 400 - 499 */

    /* client errors: 500 - 599 */

    /* transaction & XA & lock Errors: 600 - 699 */
    ERR_TOO_MANY_CONCURRENT_TRXS = 600,
    ERR_NO_FREE_UNDO_PAGE,
    ERR_LOCK_WAIT = 601,
    ERR_LOCK_WAIT_TIMEOUT,
    ERR_DEADLOCK,
    ERR_ROLLBACK,
    ERR_MISSING_HISTORY,  /* required history data has been deleted due to lack of space in rollback segment */


    /* resource manager error: 700 - 799 */
    ERR_OUT_OF_FILE_SPACE,
    ERR_MUST_GET_MORE_FILE_SPACE,  /* the database has to be stopped and restarted with more file space */

    /* meta data error: 800 - 899 */
    ERR_TABLESPACE_EXISTS = 800,  /* file of the same name already exists */
    ERR_TABLESPACE_DELETED,       /* tablespace was deleted or is being dropped right now */
    ERR_TABLESPACE_NOT_FOUND,     /* Attempt to delete a tablespace instance that was not found in the tablespace hash table */

    ERR_TABLE_EXISTS,
    ERR_TABLE_OR_VIEW_NOT_FOUND,
    ERR_TABLE_IS_BEING_USED,
    ERR_TABLE_CHANGED = 806,        /* Some part of table dictionary has changed. Such as index dropped or foreign key dropped */

    ERR_COLUMN_COUNT_REACH_LIMIT,
    ERR_COLUMN_NOT_EXIST,
    ERR_COLUMN_EXISTS,
    ERR_COLUMN_INDEXED,

    ERR_DUPLICATE_KEY = 811,
    ERR_FOREIGN_DUPLICATE_KEY,  /* foreign key constraints activated by the operation would lead to a duplicate key in some table */
    ERR_CLUSTER_NOT_FOUND,
    ERR_CANNOT_ADD_CONSTRAINT,  /* adding a foreign key constraint to a table failed */
    ERR_CANNOT_DROP_CONSTRAINT, /* dropping a foreign key constraint from a table failed */
    ERR_NO_REFERENCED_ROW,  /* referenced key value not found for a foreign key in an insert or update of a row */
    ERR_ROW_IS_REFERENCED,  /* cannot delete or update a row because it contains a key value which is referenced */


    ERR_USER_NOT_EXIST = 820,
    ERR_ROLE_NOT_EXIST,
    ERR_FUNCTION_NOT_EXIST,




    /* sql engine: 1000 - 1199*/
    ERR_UNSUPPORTED = 1000,
    ERR_TYPE_NOT_FOUND,            /*!< Generic error code for "Not found" type of errors */

    // JSON
    // sql engine parallel


    /* PL/SQL Error: 1200 - 1299 */
    ERR_TYPE_DATETIME_OVERFLOW = 1250,
    ERR_TYPE_TIMESTAMP_OVERFLOW,
    ERR_TEXT_FORMAT_ERROR,
    ERR_UNRECOGNIZED_FORMAT_ERROR,
    ERR_TYPE_OVERFLOW,
    ERR_MUTIPLE_FORMAT_ERROR,

    /* job error , 1300 - 1399 */

    /* SPM: 1400 - 1499 */

    /* storage engine: 1500 - 1699 */
    ERR_ROW_RECORD_TOO_BIG  = 1500,
    ERR_UNDO_RECORD_TOO_BIG = 1501,
    ERR_VARIANT_DATA_TOO_BIG = 1502,
    ERR_RECORD_NOT_FOUND,
    ERR_END_OF_INDEX,

    /* partition error: 1700 - 1799 */

    /* replication error: 1800 - 1899 */

    /* archive error: 2000 - 2099 */

    /* backup error: 2100 - 2199 */
    /* Tools: 2200 - 2299 */
    /* dblink: 2300 - 2399 */

    /* tenant error: 3000 - 3299 */
    /* sharding error: 3300 - 3499 */
    // re-balance error

    /* CMS:  3500 - 3599 */

    // The max error number defined in g_error_desc[]
    ERR_ERRNO_CEIL = 3999,

    /* user define errors */
    ERR_MIN_USER_DEFINE_ERROR = 5000,


    // The max error number can be used in raise exception, it not need to defined in g_error_desc[]
    ERR_CODE_CEIL = 10000,
} err_no_t;

typedef struct st_source_location {
    uint16 line;
    uint16 column;
} source_location_t;

#define ERROR_INFO_MSG_LENGTH         256
#define ERROR_INFO_STACK_MAX_LEVEL    16

typedef struct st_error_info_t {
    int32  code;
    uint32 line;
    char*  file;
    source_location_t loc;
    char   message[ERROR_INFO_MSG_LENGTH];
    bool8 is_ignored;
    bool8 is_ignore_log;
    bool8 is_full;
    bool8 reserved;
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

#define CM_SET_ERROR(err_no, ...)                                                \
    do {                                                                         \
        if (g_tls_errinfo_stack.level >= ERROR_INFO_STACK_MAX_LEVEL) {           \
            break;                                                               \
        }                                                                        \
        g_tls_errinfo_stack.err_info[g_tls_errinfo_stack.level].code = err_no;   \
        g_tls_errinfo_stack.err_info[g_tls_errinfo_stack.level].file = __FILE__; \
        g_tls_errinfo_stack.err_info[g_tls_errinfo_stack.level].line = __LINE__; \
        set_stack_error_message(g_error_desc[err_no], ##__VA_ARGS__);            \
        g_tls_errinfo_stack.level++;                                             \
    } while (0)

#define CM_RESET_ERR_INFO_STACK                            \
    do {                                                   \
        g_tls_errinfo_stack.level = 0;                     \
    } while (0)


extern bool32 error_message_init(char* errmsg_file);

// ------------------------------------------------------------------------------------------------

extern const char *g_error_desc[ERR_CODE_CEIL];



#endif   // _CM_ERROR_H

