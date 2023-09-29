#include "guc.h"


bool32      g_read_only;
bool32      g_flush_log_at_commit;

char       *g_host_address;
char       *g_base_directory;
char       *g_character_set_client;
char       *g_character_set_server;
char       *g_flush_method;
char       *g_transaction_isolation;

int         g_port;
int         g_server_id;
int         g_max_connections;
int         g_thread_pool_size;
int         g_thread_stack_size;
int         g_thread_stack_depth;
int         g_max_allowed_package;
int         g_open_files_limit;
int         g_table_open_cache;
int         g_max_dirty_pages_pct;
int         g_io_capacity;
int         g_io_capacity_max;
int         g_read_io_threads;
int         g_write_io_threads;
int         g_purge_threads;
int         g_lru_scan_depth;
int         g_buffer_pool_size;
int         g_buffer_pool_instances;
int         g_temp_buffer_size;
int         g_plan_buffer_size;
int         g_query_cache_size;
int         g_query_cache_limit;
int         g_undo_buffer_size;
int         g_redo_log_buffer_size;
int         g_redo_log_file_size;
int         g_redo_log_files;
int         g_lock_wait_timeout;
int         g_session_wait_timeout;

double      g_sample_rate;

/*
 * Actual lookup of variables is done through this single, sorted array.
 */
static config_generic **guc_variables;

/* Current number of variables contained in the vector */
static int num_guc_variables;

/* Vector capacity */
static int size_guc_variables;




static config_bool ConfigureNamesBool[] =
{
   {
        {"read_only",
         "Enables read only for server.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_BOOL
        },
        &g_read_only,
        FALSE,
        NULL, NULL, NULL
    },
    {
        {"flush_log_at_commit",
         "flush_log_at_commit.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_BOOL
        },
        &g_flush_log_at_commit,
        TRUE,
        NULL, NULL, NULL
    },

    /* End-of-list marker */
    {
        {NULL, NULL, 0, 0, 0, NULL}, NULL, false, NULL, NULL, NULL
    }
};

static config_int ConfigureNamesInt[] =
{
    {
        {"server_id",
         "server id.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT
        },
        &g_server_id, 0, 0, UINT_MAX16,
        NULL, NULL, NULL
    },
    {
        {"port",
         "port.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT
        },
        &g_port, 5500, 1, UINT_MAX16,
        NULL, NULL, NULL
    },
    {
        {"max_connections",
         "max connections.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT
        },
        &g_max_connections, 256, 1, UINT_MAX16,
        NULL, NULL, NULL
    },
    {
        {"thread_pool_size",
         "thread count of thread pool.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT
        },
        &g_thread_pool_size, 16, 1, 1024,
        NULL, NULL, NULL
    },
    {
        {"thread_stack_size",
         "thread stack size of thread pool.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, GUC_UNIT_KB
        },
        &g_thread_stack_size, 512, 32, 8196,
        NULL, NULL, NULL
    },
    {
        {"thread_stack_depth",
         "thread stack depth of thread pool.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, 0
        },
        &g_thread_stack_depth, 64, 16, 256,
        NULL, NULL, NULL
    },
    {
        {"max_allowed_package",
         "max allowed package of connection.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, GUC_UNIT_MB
        },
        &g_max_allowed_package, 1, 1, 64,
        NULL, NULL, NULL
    },
    {
        {"open_files_limit",
         "max opened file count of server.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, 0
        },
        &g_open_files_limit, 1024, 128, UINT_MAX16,
        NULL, NULL, NULL
    },
    {
        {"table_open_cache",
         "table open cache.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, 0
        },
        &g_table_open_cache, 128, 0, 4096,
        NULL, NULL, NULL
    },
    {
        {"max_dirty_pages_pct",
         "max_dirty_pages_pct.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, 0
        },
        &g_max_dirty_pages_pct, 50, 0, 100,
        NULL, NULL, NULL
    },
    {
        {"io_capacity",
         "io_capacity.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, 0
        },
        &g_io_capacity, 2000, 1, UINT_MAX16,
        NULL, NULL, NULL
    },
    {
        {"io_capacity_max",
         "io_capacity_max.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, 0
        },
        &g_io_capacity_max, 10000, 1, UINT_MAX16,
        NULL, NULL, NULL
    },
    {
        {"read_io_threads",
         "read_io_threads.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, 0
        },
        &g_read_io_threads, 4, 1, 32,
        NULL, NULL, NULL
    },
    {
        {"write_io_threads",
         "write_io_threads.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, 0
        },
        &g_write_io_threads, 4, 1, 32,
        NULL, NULL, NULL
    },
    {
        {"purge_threads",
         "purge_threads.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, 0
        },
        &g_purge_threads, 4, 1, 32,
        NULL, NULL, NULL
    },
    {
        {"lru_scan_depth",
         "lru_scan_depth.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, 0
        },
        &g_lru_scan_depth, 4000, 1, UINT_MAX16,
        NULL, NULL, NULL
    },
    {
        {"buffer_pool_size",
         "totoal memory size of data buffer pool.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, GUC_UNIT_MB
        },
        &g_buffer_pool_size, 16, 1, INT_MAX32,
        NULL, NULL, NULL
    },
    {
        {"buffer_pool_instances",
         "count of data buffer pool.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, 0
        },
        &g_buffer_pool_instances, 4, 1, 128,
        NULL, NULL, NULL
    },
    {
        {"temp_buffer_size",
         "memory size of temp buffer.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, GUC_UNIT_MB
        },
        &g_temp_buffer_size, 16, 1, INT_MAX32,
        NULL, NULL, NULL
    },
    {
        {"plan_buffer_size",
         "memory size of plan buffer.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, GUC_UNIT_MB
        },
        &g_plan_buffer_size, 16, 1, INT_MAX32,
        NULL, NULL, NULL
    },
    {
        {"query_cache_size",
         "memory size of query cache.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, GUC_UNIT_MB
        },
        &g_query_cache_size, 16, 1, INT_MAX32,
        NULL, NULL, NULL
    },
    {
        {"query_cache_limit",
         "memory limit of query cache.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, GUC_UNIT_MB
        },
        &g_query_cache_limit, 1, 1, 64,
        NULL, NULL, NULL
    },
    {
        {"undo_buffer_size",
         "memory size of undo buffer.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, GUC_UNIT_MB
        },
        &g_undo_buffer_size, 16, 1, INT_MAX32,
        NULL, NULL, NULL
    },
    {
        {"redo_log_buffer_size",
         "memory size of redo buffer.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, GUC_UNIT_MB
        },
        &g_redo_log_buffer_size, 4, 1, 1024,
        NULL, NULL, NULL
    },
    {
        {"redo_log_file_size",
         "size of redo log file.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, GUC_UNIT_MB
        },
        &g_redo_log_file_size, 1, 1, UINT_MAX16,
        NULL, NULL, NULL
    },
    {
        {"redo_log_files",
         "count of redo log file.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, GUC_UNIT_MB
        },
        &g_redo_log_files, 3, 3, 16,
        NULL, NULL, NULL
    },
    {
        {"lock_wait_timeout",
         "wait seconds of lock.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, GUC_UNIT_S
        },
        &g_lock_wait_timeout, 10, 0, UINT_MAX16,
        NULL, NULL, NULL
    },
    {
        {"sessionwait_timeout",
         "wait seconds of session.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT, GUC_UNIT_S
        },
        &g_session_wait_timeout, 60, 0, UINT_MAX16,
        NULL, NULL, NULL
    },

    /* End-of-list marker */
    {
        { NULL, NULL, 0, 0, 0, NULL }, NULL, 0, 0, 0, NULL, NULL, NULL
    }
};

static config_real ConfigureNamesReal[] =
{
    {
        {"sample_rate",
         "rate .",
         GUC_CONTEXT_SIGHUP, GUC_TYPE_REAL
        },
        &g_sample_rate, 0, 0, 100,
        NULL, NULL, NULL
    },

    /* End-of-list marker */
    {
        { NULL, NULL, 0, 0, 0, NULL }, NULL, 0, 0, 0, NULL, NULL, NULL
    }
};

static config_string ConfigureNamesString[] =
{
    {
        {"host_address",
         "host address.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_STRING
        },
        &g_host_address,
        "*",
        NULL, NULL, NULL
    },
    {
        {"base_directory",
         "base directory for server.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_STRING
        },
        &g_base_directory,
        "",
        NULL, NULL, NULL
    },
    {
        {"character_set_client",
         "character set for client.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_STRING
        },
        &g_character_set_client,
        "utf8mb4",
        NULL, NULL, NULL
    },
    {
        {"character_set_server",
         "character set for server.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_STRING
        },
        &g_character_set_server,
        "utf8mb4",
        NULL, NULL, NULL
    },
    {
        {"flush_method",
         "flush_method.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_STRING
        },
        &g_flush_method,
        "O_DIRECT",
        NULL, NULL, NULL
    },
    {
        {"transaction_isolation",
         "transaction_isolation.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_STRING
        },
        &g_transaction_isolation,
        "REPEATABLE-READ",
        NULL, NULL, NULL
    },

    /* End-of-list marker */
    {
        {NULL, NULL, 0, 0, 0, NULL}, NULL, NULL, NULL, NULL, NULL
    }
};

static config_enum ConfigureNamesEnum[] =
{

    /* End-of-list marker */
    {
        {NULL, NULL, 0, 0, 0, NULL}, NULL, 0, NULL, NULL, NULL
    }
};

static bool32 guc_bool_check_hook(char *newval, void *config)
{
    config_bool *conf = (config_bool *)config;

    if (newval == NULL) {
        return FALSE;
    }

    if (_stricmp(newval, "on") != 0 && _stricmp(newval, "1") != 0 &&
        _stricmp(newval, "off") != 0 && _stricmp(newval, "0") != 0) {
        return FALSE;
    }

    return TRUE;
}

static void guc_bool_assign_hook(char *newval, void *extra)
{
    if (_stricmp(newval, "on") == 0 || _stricmp(newval, "1") == 0) {
        *(bool32 *)extra = TRUE;
    } else if (_stricmp(newval, "off") == 0 || _stricmp(newval, "0") == 0) {
        *(bool32 *)extra = FALSE;
    }
}

static bool32 guc_int_check_hook(char *newval, void *config)
{
    config_int *conf = (config_int *)config;

    if (newval == NULL) {
        return FALSE;
    }

    int val = atoi(newval);
    if (conf->min > val || conf->max < val) {
        return FALSE;
    }

    return TRUE;
}

static void guc_int_assign_hook(char *newval, void *extra)
{
    *(int *)extra = atoi(newval);
}

static bool32 guc_real_check_hook(char *newval, void *config)
{
    config_real *conf = (config_real *)config;

    if (newval == NULL) {
        return FALSE;
    }

    double val = atof(newval);
    if (conf->min > val || conf->max < val) {
        return FALSE;
    }

    return TRUE;
}

static void guc_real_assign_hook(char *newval, void *extra)
{
    *(double *)extra = atof(newval);
}

static bool32 guc_string_check_hook(char *newval, void *config)
{
    config_string *conf = (config_string *)config;

    if (newval == NULL) {
        return FALSE;
    }

    return TRUE;
}

static void guc_string_assign_hook(char *newval, void **extra)
{
    if (*extra != NULL) {
        free(*extra);
    }
    *extra = (char *)malloc(strlen(newval) + 1);
    memcpy((char *)*extra, newval, strlen(newval) + 1);
}

/*
 * the bare comparison function for GUC names
 */
static int guc_name_compare(const char *namea, const char *nameb)
{
    /*
     * The temptation to use strcasecmp() here must be resisted, because the
     * array ordering has to remain stable across setlocale() calls. So, build
     * our own with a simple ASCII-only downcasing.
     */
    while (*namea && *nameb)
    {
        char cha = *namea++;
        char chb = *nameb++;

        if (cha >= 'A' && cha <= 'Z')
            cha += 'a' - 'A';
        if (chb >= 'A' && chb <= 'Z')
            chb += 'a' - 'A';
        if (cha != chb)
            return cha - chb;
    }
    if (*namea)
        return 1; /* a is longer */
    if (*nameb)
        return -1; /* b is longer */
    return 0;
}

static int guc_var_compare(const void *a, const void *b)
{
    const config_generic *confa = *(config_generic *const *) a;
    const config_generic *confb = *(config_generic *const *) b;

    return guc_name_compare(confa->name, confb->name);
}

void build_guc_variables(void)
{
    uint32 i;
    int size_vars;
    int num_vars = 0;
    config_generic **guc_vars;

    for (i = 0; ConfigureNamesBool[i].gen.name; i++) {
        config_bool *conf = &ConfigureNamesBool[i];
        num_vars++;
    }
    for (i = 0; ConfigureNamesInt[i].gen.name; i++) {
        config_int *conf = &ConfigureNamesInt[i];
        num_vars++;
    }
    for (i = 0; ConfigureNamesReal[i].gen.name; i++) {
        config_real *conf = &ConfigureNamesReal[i];
        num_vars++;
    }
    for (i = 0; ConfigureNamesString[i].gen.name; i++) {
        config_string *conf = &ConfigureNamesString[i];
        num_vars++;
    }
    for (i = 0; ConfigureNamesEnum[i].gen.name; i++) {
        config_enum *conf = &ConfigureNamesEnum[i];
        num_vars++;
    }

    size_vars = num_vars + num_vars / 4;
    guc_vars = (config_generic **)malloc(size_vars * sizeof(config_generic *));

    num_vars = 0;

    for (i = 0; ConfigureNamesBool[i].gen.name; i++) {
        if (!ConfigureNamesBool[i].check_hook) {
            ConfigureNamesBool[i].check_hook = guc_bool_check_hook;
        }
        if (!ConfigureNamesBool[i].assign_hook) {
            ConfigureNamesBool[i].assign_hook = guc_bool_assign_hook;
        }
        *ConfigureNamesBool[i].variable = ConfigureNamesBool[i].boot_val;
        guc_vars[num_vars++] = &ConfigureNamesBool[i].gen;
    }
    for (i = 0; ConfigureNamesInt[i].gen.name; i++) {
        if (!ConfigureNamesInt[i].check_hook) {
            ConfigureNamesInt[i].check_hook = guc_int_check_hook;
        }
        if (!ConfigureNamesInt[i].assign_hook) {
            ConfigureNamesInt[i].assign_hook = guc_int_assign_hook;
        }
        *ConfigureNamesInt[i].variable = ConfigureNamesInt[i].boot_val;
        guc_vars[num_vars++] = &ConfigureNamesInt[i].gen;
    }
    for (i = 0; ConfigureNamesReal[i].gen.name; i++) {
        if (!ConfigureNamesReal[i].check_hook) {
            ConfigureNamesReal[i].check_hook = guc_real_check_hook;
        }
        if (!ConfigureNamesReal[i].assign_hook) {
            ConfigureNamesReal[i].assign_hook = guc_real_assign_hook;
        }
        *ConfigureNamesReal[i].variable = ConfigureNamesReal[i].boot_val;
        guc_vars[num_vars++] = &ConfigureNamesReal[i].gen;
    }
    for (i = 0; ConfigureNamesString[i].gen.name; i++) {
        if (!ConfigureNamesString[i].check_hook) {
            ConfigureNamesString[i].check_hook = guc_string_check_hook;
        }
        if (!ConfigureNamesString[i].assign_hook) {
            ConfigureNamesString[i].assign_hook = guc_string_assign_hook;
        }
        *ConfigureNamesString[i].variable = ConfigureNamesString[i].boot_val;
        guc_vars[num_vars++] = &ConfigureNamesString[i].gen;
    }
    for (i = 0; ConfigureNamesEnum[i].gen.name; i++) {
        guc_vars[num_vars++] = &ConfigureNamesEnum[i].gen;
        *ConfigureNamesEnum[i].variable = ConfigureNamesEnum[i].boot_val;
    }
    if (guc_variables) {
        free(guc_variables);
    }
    guc_variables = guc_vars;
    num_guc_variables = num_vars;
    size_guc_variables = size_vars;
    qsort((void **)guc_variables, num_guc_variables, sizeof(config_generic *), guc_var_compare);
}

config_generic* find_guc_variable(char *key_name)
{
    config_generic **conf;
    char **key = &key_name;

    conf = (config_generic **)bsearch((void *)(&key),
        (void **) guc_variables, num_guc_variables,
        sizeof(config_generic *), guc_var_compare);

    return conf ? *conf : NULL;
}

bool32 set_guc_option_value(config_generic *gconfig, char* value)
{
    switch (gconfig->type)
    {
    case GUC_TYPE_BOOL:
    {
        config_bool *conf = (config_bool *)gconfig;

        if (conf->check_hook && !conf->check_hook(value, conf)) {
            return FALSE;
        }

        if (conf->assign_hook) {
            conf->assign_hook(value, (void *)conf->variable);
        }
        break;
    }
    case GUC_TYPE_INT:
    {
        config_int *conf = (config_int *)gconfig;

        if (conf->check_hook && !conf->check_hook(value, conf)) {
            return FALSE;
        }

        if (conf->assign_hook) {
            conf->assign_hook(value, (void *)conf->variable);
        }
        break;
    }
    case GUC_TYPE_REAL:
    {
        config_real *conf = (config_real *)gconfig;

        if (conf->check_hook && !conf->check_hook(value, conf)) {
            return FALSE;
        }

        if (conf->assign_hook) {
            conf->assign_hook(value, (void *)conf->variable);
        }
        break;
    }
    case GUC_TYPE_STRING:
    {
        config_string *conf = (config_string *)gconfig;

        if (conf->check_hook && !conf->check_hook(value, conf)) {
            return FALSE;
        }

        if (conf->reset_val) {
            free(conf->reset_val);
        }
        conf->assign_hook(value, (void **)&conf->reset_val);
        *conf->variable = conf->reset_val;
        break;
    }
    case GUC_TYPE_ENUM:
    {
        break;
    }
    }

    return TRUE;
}

