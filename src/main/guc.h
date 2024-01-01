#ifndef _GUC_H
#define _GUC_H

#include "cm_type.h"
#include "cm_error.h"
#include "cm_mutex.h"

#define GUC_TYPE_BOOL        1
#define GUC_TYPE_INT         2
#define GUC_TYPE_REAL        3
#define GUC_TYPE_STRING      4
#define GUC_TYPE_ENUM        5

#define GUC_CONTEXT_INTERNAL     1
#define GUC_CONTEXT_POSTMASTER   2
#define GUC_CONTEXT_SIGHUP       3
#define GUC_CONTEXT_USERSET      4


#define GUC_UNIT_GB         0x0001  /* value is in kilobytes */
#define GUC_UNIT_MB         0x0002  /* value is in megabytes */
#define GUC_UNIT_KB         0x0004  /* value is in kilobytes */
#define GUC_UNIT_BYTE       0x0008  /* value is in bytes */
#define GUC_UNIT_MEMORY     0x1000  /* mask for size-related units */

#define GUC_UNIT_MS         0x0001  /* value is in milliseconds */
#define GUC_UNIT_S          0x0002  /* value is in seconds */
#define GUC_UNIT_MIN        0x0003  /* value is in minutes */
#define GUC_UNIT_HOUR       0x0004  /* value is in hours */
#define GUC_UNIT_DAY        0x0005  /* value is in days */
#define GUC_UNIT_TIME       0x2000  /* mask for time-related units */

typedef bool32 (*GucBoolCheckHook) (char *newval, void *config);
typedef bool32 (*GucIntCheckHook) (char *newval, void *config);
typedef bool32 (*GucRealCheckHook) (char *newval, void *config);
typedef bool32 (*GucStringCheckHook) (char *newval, void *config);
typedef bool32 (*GucEnumCheckHook) (char *newval, void *config);

typedef void (*GucBoolAssignHook) (char *newval, void *extra);
typedef void (*GucIntAssignHook) (char *newval, void *extra);
typedef void (*GucRealAssignHook) (char *newval, void *extra);
typedef void (*GucStringAssignHook) (char *newval, void **extra);
typedef void (*GucEnumAssignHook) (char *newval, void *extra);

typedef const char *(*GucShowHook) (void);

typedef struct {
    char             *name; /* name of variable - MUST BE FIRST */
    char             *desc; /* short desc. of this variable's purpose */
    int               context;
    int               type; /* type of variable (set only at startup) */
    int               flags;
    void             *extra; /* "extra" pointer for current actual value */
} config_generic;

typedef struct
{
    config_generic        gen;
    bool32               *variable;
    bool32                boot_val;
    GucBoolCheckHook      check_hook;
    GucBoolAssignHook     assign_hook;
    GucShowHook           show_hook;
} config_bool;

typedef struct
{
    config_generic        gen;
    int                  *variable;
    int                   boot_val;
    int                   min;
    int                   max;
    GucIntCheckHook       check_hook;
    GucIntAssignHook      assign_hook;
    GucShowHook           show_hook;
} config_int;

typedef struct
{
    config_generic        gen;
    double               *variable;
    double                boot_val;
    double                min;
    double                max;
    GucRealCheckHook      check_hook;
    GucRealAssignHook     assign_hook;
    GucShowHook           show_hook;
} config_real;

typedef struct
{
    config_generic        gen;
    char                **variable;
    char                 *boot_val;
    GucStringCheckHook    check_hook;
    GucStringAssignHook   assign_hook;
    GucShowHook           show_hook;
    char                 *reset_val;
} config_string;

typedef struct
{
    config_generic        gen;
    int                  *variable;
    int                   boot_val;
    GucEnumCheckHook      check_hook;
    GucEnumAssignHook     assign_hook;
    GucShowHook           show_hook;
} config_enum;


extern void build_guc_variables(void);
extern config_generic* find_guc_variable(char *key_name);
extern bool32 set_guc_option_value(config_generic *gconfig, char* value);

extern bool32      g_read_only;
extern bool32      g_flush_log_at_commit;

extern char       *g_host_address;
extern char       *g_base_directory;
extern char       *g_character_set_client;
extern char       *g_character_set_server;
extern char       *g_flush_method;
extern char       *g_transaction_isolation;

extern int         g_port;
extern int         g_server_id;
extern int         g_max_connections;
extern int         g_thread_pool_size;
extern int         g_thread_stack_size;
extern int         g_thread_stack_depth;
extern int         g_max_allowed_package;
extern int         g_open_files_limit;
extern int         g_table_open_cache;
extern int         g_max_dirty_pages_pct;
extern int         g_io_capacity;
extern int         g_io_capacity_max;
extern int         g_read_io_threads;
extern int         g_write_io_threads;
extern int         g_purge_threads;
extern int         g_lru_scan_depth;
extern int         g_buffer_pool_size;
extern int         g_buffer_pool_instances;
extern int         g_temp_buffer_size;
extern int         g_plan_buffer_size;
extern int         g_query_cache_size;
extern int         g_query_cache_limit;
extern int         g_undo_buffer_size;
extern int         g_redo_log_buffer_size;
extern int         g_redo_log_file_size;
extern int         g_redo_log_files;
extern int         g_lock_wait_timeout;
extern int         g_session_wait_timeout;

extern double      g_sample_rate;

//------------------------------------------------------------------------------

extern void initialize_guc_options(char *config_file);

#endif  /* _GUC_H */
