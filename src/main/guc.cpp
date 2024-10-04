#include "guc.h"
#include "cm_config.h"
#include "cm_util.h"

attribute_t   g_guc_options = {0};

// Actual lookup of variables is done through this single, sorted array.
static config_generic **guc_variables;

// Current number of variables contained in the vector
static int num_guc_variables;

// Vector capacity
static int size_guc_variables;

// =======================================================================


static bool32 guc_memory_check_hook(char* newval, void* extra);
static void guc_memory_assign_hook(char* newval, void* extra);
static void guc_memory_init_hook(int64 newval, void* config);
static bool32 guc_time_check_hook(char* newval, void* extra);
static void guc_time_assign_hook(char* newval, void* extra);
static void guc_time_init_hook(int64 newval, void* config);

// =======================================================================


static config_bool ConfigureNamesBool[] =
{
   {
        {"read_only",
         "Enables read only for server.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_BOOL, 0, NULL
        },
        &g_guc_options.attr_common.read_db_only,
        FALSE,
        NULL, NULL, NULL, NULL
    },
    {
        {"flush_log_at_commit",
         "flush_log_at_commit.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_BOOL
        },
        &g_guc_options.attr_storage.flush_log_at_commit,
        TRUE,
        NULL, NULL, NULL, NULL
    },

    /* End-of-list marker */
    {
        {NULL, NULL, 0, 0, 0, NULL}, NULL, false, NULL, NULL, NULL, NULL
    }
};

static config_int32 ConfigureNamesInt32[] =
{
    {
        {"server_id",
         "server id.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT32
        },
        &g_guc_options.attr_common.server_id, 1, 1, INT_MAX32,
        NULL, NULL, NULL, NULL
    },
    {
        {"port",
         "port.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT32
        },
        &g_guc_options.attr_network.port, 5500, 1, UINT_MAX16,
        NULL, NULL, NULL, NULL
    },
    {
        {"max_connections",
         "max connections.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT32
        },
        &g_guc_options.attr_network.max_connections, 256, 1, UINT_MAX16,
        NULL, NULL, NULL, NULL
    },
    {
        {"thread_pool_size",
         "thread count of thread pool.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT32
        },
        &g_guc_options.attr_common.thread_pool_size, 16, 1, 4096,
        NULL, NULL, NULL, NULL
    },
    {
        {"thread_stack_depth",
         "thread stack depth of thread pool.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT32, 0
        },
        &g_guc_options.attr_common.thread_stack_depth, 64, 16, 256,
        NULL, NULL, NULL, NULL
    },
    {
        {"open_files_limit",
         "max opened file count of server.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT32, 0
        },
        &g_guc_options.attr_common.open_files_limit, 1024, 128, INT_MAX32,
        NULL, NULL, NULL, NULL
    },
    {
        {"table_open_cache",
         "table open cache.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT32, 0
        },
        &g_guc_options.attr_sql.table_open_cache, 128, 128, UINT_MAX16,
        NULL, NULL, NULL, NULL
    },
    {
        {"table_hash_array_size",
         "hash array size of table cache.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT32, 0
        },
        &g_guc_options.attr_memory.table_hash_array_size, 10000, 100, 1000000,
        NULL, NULL, NULL, NULL
    },

    {
        {"max_dirty_pages_pct",
         "max_dirty_pages_pct.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT32, 0
        },
        &g_guc_options.attr_storage.max_dirty_pages_pct, 50, 0, 100,
        NULL, NULL, NULL, NULL
    },
    {
        {"io_capacity",
         "io_capacity.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT32, 0
        },
        &g_guc_options.attr_storage.io_capacity, 2000, 1, INT_MAX32,
        NULL, NULL, NULL, NULL
    },
    {
        {"io_capacity_max",
         "io_capacity_max.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT32, 0
        },
        &g_guc_options.attr_storage.io_capacity_max, 10000, 1, INT_MAX32,
        NULL, NULL, NULL, NULL
    },
    {
        {"read_io_threads",
         "read_io_threads.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT32, 0
        },
        &g_guc_options.attr_storage.read_io_threads, 4, 1, 128,
        NULL, NULL, NULL, NULL
    },
    {
        {"write_io_threads",
         "write_io_threads.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT32, 0
        },
        &g_guc_options.attr_storage.write_io_threads, 4, 1, 128,
        NULL, NULL, NULL, NULL
    },
    {
        {"purge_threads",
         "purge_threads.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT32, 0
        },
        &g_guc_options.attr_storage.purge_threads, 4, 1, 128,
        NULL, NULL, NULL, NULL
    },
    {
        {"lru_scan_depth",
         "lru_scan_depth.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT32, 0
        },
        &g_guc_options.attr_storage.lru_scan_depth, 4000, 1, UINT_MAX16,
        NULL, NULL, NULL, NULL
    },
    {
        {"buffer_pool_instances",
         "count of data buffer pool.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT32, 0
        },
        &g_guc_options.attr_storage.buffer_pool_instances, 1, 1, 1024,
        NULL, NULL, NULL, NULL
    },
    {
        {"undo_segment_count",
         "count of undo segment.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT32, 0
        },
        &g_guc_options.attr_storage.undo_segment_count, 32, 4, 96,
        NULL, NULL, NULL, NULL
    },


    /* End-of-list marker */
    {
        { NULL, NULL, 0, 0, 0 }, NULL, 0, 0, 0, NULL, NULL, NULL, NULL
    }
};

static config_int64 ConfigureNamesInt64[] =
{
    {
        {"lock_wait_timeout",
         "wait seconds of lock.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT64, GUC_UNIT_S
        },
        &g_guc_options.attr_storage.lock_wait_timeout, 10, 0, UINT_MAX16,
        guc_time_check_hook, guc_time_assign_hook, guc_time_init_hook, NULL
    },
    {
        {"sessionwait_timeout",
         "wait seconds of session.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT64, GUC_UNIT_S
        },
        &g_guc_options.attr_network.session_wait_timeout, 60, 0, UINT_MAX16,
        guc_time_check_hook, guc_time_assign_hook, guc_time_init_hook, NULL
    },
    {
        {"thread_stack_size",
         "thread stack size of thread pool.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT64, GUC_UNIT_KB
        },
        &g_guc_options.attr_common.thread_stack_size, 512, 512, 8196,
        guc_memory_check_hook, guc_memory_assign_hook, guc_memory_init_hook, NULL
    },
    {
        {"max_allowed_package",
         "max allowed package of connection.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT64, GUC_UNIT_MB
        },
        &g_guc_options.attr_network.max_allowed_package, 1, 1, 64,
        guc_memory_check_hook, guc_memory_assign_hook, guc_memory_init_hook, NULL
    },
    {
        {"buffer_pool_size",
         "totoal memory size of data buffer pool.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT64, GUC_UNIT_MB
        },
        &g_guc_options.attr_storage.buffer_pool_size, 16 , 16, INT_MAX32,
        guc_memory_check_hook, guc_memory_assign_hook, guc_memory_init_hook, NULL
    },
    {
        {"dictionary_cache_size",
         "memory size of dictionary cache.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT64, GUC_UNIT_MB
        },
        &g_guc_options.attr_memory.dictionary_memory_cache_size, 16 , 1, INT_MAX32,
        guc_memory_check_hook, guc_memory_assign_hook, guc_memory_init_hook, NULL
    },
    {
        {"mtr_cache_size",
         "memory size of min transaction cache.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT64, GUC_UNIT_MB
        },
        &g_guc_options.attr_memory.mtr_memory_cache_size, 16 , 1, 1024,
        guc_memory_check_hook, guc_memory_assign_hook, guc_memory_init_hook, NULL
    },

    {
        {"common_cache_size",
         "memory size of common cache.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT64, GUC_UNIT_MB
        },
        &g_guc_options.attr_memory.common_memory_cache_size, 16 , 1, INT_MAX32,
        guc_memory_check_hook, guc_memory_assign_hook, guc_memory_init_hook, NULL
    },
    {
        {"temporary_cache_size",
         "memory size of temporary cache.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT64, GUC_UNIT_MB
        },
        &g_guc_options.attr_memory.temp_memory_cache_size, 16 , 1, INT_MAX32,
        guc_memory_check_hook, guc_memory_assign_hook, guc_memory_init_hook, NULL
    },
    {
        {"plan_cache_size",
         "memory size of plan cache.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT64, GUC_UNIT_MB
        },
        &g_guc_options.attr_memory.plan_memory_cache_size, 16 , 1, INT_MAX32,
        guc_memory_check_hook, guc_memory_assign_hook, guc_memory_init_hook, NULL
    },
    {
        {"query_cache_size",
         "memory size of query cache.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT64, GUC_UNIT_MB
        },
        &g_guc_options.attr_memory.query_memory_cache_size, 16 , 1, INT_MAX32,
        guc_memory_check_hook, guc_memory_assign_hook, guc_memory_init_hook, NULL
    },
    {
        {"query_cache_limit",
         "memory limit of query cache.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT64, GUC_UNIT_MB
        },
        &g_guc_options.attr_memory.query_memory_cache_limit, 16 , 1, INT_MAX32,
        guc_memory_check_hook, guc_memory_assign_hook, guc_memory_init_hook, NULL
    },
    {
        {"undo_cache_size",
         "memory size of undo cache.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT64, GUC_UNIT_MB
        },
        &g_guc_options.attr_storage.undo_cache_size, 16, 16, 512 * 1024,
        guc_memory_check_hook, guc_memory_assign_hook, guc_memory_init_hook, NULL
    },
    {
        {"redo_log_buffer_size",
         "memory size of redo buffer.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_INT64, GUC_UNIT_MB
        },
        &g_guc_options.attr_storage.redo_log_buffer_size, 16, 1, 1024,
        guc_memory_check_hook, guc_memory_assign_hook, guc_memory_init_hook, NULL
    },

    /* End-of-list marker */
    {
        { NULL, NULL, 0, 0, 0, NULL }, NULL, 0, 0, 0, NULL, NULL, NULL, NULL
    }
};

static config_real ConfigureNamesReal[] =
{
    //{
    //    {"sample_rate",
    //     "rate .",
    //     GUC_CONTEXT_SIGHUP, GUC_TYPE_REAL
    //    },
    //    &g_guc_options.attr_sql.sample_rate, 0, 0, 100,
    //    NULL, NULL, NULL, NULL
    //},

    /* End-of-list marker */
    {
        { NULL, NULL, 0, 0, 0, NULL }, NULL, 0, 0, 0, NULL, NULL, NULL, NULL
    }
};

static config_string ConfigureNamesString[] =
{
    {
        {"host_address",
         "host address.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_STRING
        },
        &g_guc_options.attr_network.host_address,
        "*",
        NULL, NULL, NULL, NULL
    },
    {
        {"character_set_client",
         "character set for client.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_STRING
        },
        &g_guc_options.attr_common.character_set_client,
        "utf8mb4",
        NULL, NULL, NULL, NULL
    },
    {
        {"character_set_server",
         "character set for server.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_STRING
        },
        &g_guc_options.attr_common.character_set_server,
        "utf8mb4",
        NULL, NULL, NULL, NULL
    },
    {
        {"flush_method",
         "flush_method.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_STRING
        },
        &g_guc_options.attr_storage.flush_method,
        "O_DIRECT",
        NULL, NULL, NULL, NULL
    },
    {
        {"transaction_isolation",
         "transaction_isolation.",
         GUC_CONTEXT_POSTMASTER, GUC_TYPE_STRING
        },
        &g_guc_options.attr_storage.transaction_isolation,
        "REPEATABLE-READ", NULL, NULL, NULL, NULL
    },

    /* End-of-list marker */
    {
        {NULL, NULL, 0, 0, 0, NULL}, NULL, NULL, NULL, NULL, NULL, NULL
    }
};

static config_enum ConfigureNamesEnum[] =
{
    {
        {"wal_level", "wal_level", GUC_CONTEXT_POSTMASTER, GUC_TYPE_ENUM},
        &g_guc_options.attr_storage.wal_level,
        WAL_LEVEL_MINIMAL, NULL, NULL, NULL
    },

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

static uint32 guc_memory_get_unit(char *newval)
{
    char* ptr = newval;
    while (ptr != '\0' && (*ptr == ' ' || (*ptr >= '0' && *ptr <= '9'))) {
        ptr++;
    }

    if (ptr == '\0') {
        return GUC_UNIT_BYTE;
    }

    if (strcasecmp(ptr, "K") == 0 || strcasecmp(ptr, "KB") == 0) {
        return GUC_UNIT_KB;
    } else if (strcasecmp(ptr, "M") == 0 || strcasecmp(ptr, "MB") == 0) {
        return GUC_UNIT_MB;
    } else if (strcasecmp(ptr, "G") == 0 || strcasecmp(ptr, "GB") == 0) {
        return GUC_UNIT_GB;
    } 

    return GUC_UNIT_INVALID;
}

static int64 guc_memory_get_value_by_uint(int64 val, uint32 unit)
{
    switch (unit) {
    case GUC_UNIT_KB:
        val = val * 1024;
        break;
    case GUC_UNIT_MB:
        val = val * 1024 * 1024;
        break;
    case GUC_UNIT_GB:
        val = val * 1024 * 1024 * 1024;
        break;
    case GUC_UNIT_BYTE:
    default:
        break;
    }

    return val;
}

static bool32 guc_memory_check_hook(char* newval, void* config)
{
    if (newval == NULL) {
        return FALSE;
    }

    uint32 unit = guc_memory_get_unit(newval);
    if (unit == GUC_UNIT_INVALID) {
        return FALSE;
    }

    config_int64 *conf = (config_int64 *)config;
    int64 val = atoll(newval);
    if (guc_memory_get_value_by_uint(conf->min, unit) > guc_memory_get_value_by_uint(val, unit) ||
        guc_memory_get_value_by_uint(conf->max, unit) < guc_memory_get_value_by_uint(val, unit)) {
        return FALSE;
    }

    return TRUE;
}

static void guc_memory_assign_hook(char* newval, void* extra)
{
    uint32 unit = guc_memory_get_unit(newval);

    switch (unit) {
    case GUC_UNIT_KB:
        *(int64 *)extra = atoll(newval) * 1024;
        break;
    case GUC_UNIT_MB:
        *(int64 *)extra = atoll(newval) * 1024 * 1024;
        break;
    case GUC_UNIT_GB:
        *(int64 *)extra = atoll(newval) * 1024 * 1024 * 1024;
        break;
    case GUC_UNIT_BYTE:
    default:
        *(int64 *)extra = atoll(newval);
        break;
    }
}

static void guc_memory_init_hook(int64 newval, void* config)
{
    config_int64 *conf = (config_int64 *)config;
    *conf->variable = guc_memory_get_value_by_uint(newval, conf->gen.flags);
}

static uint32 guc_time_get_unit(char *newval)
{
    char* ptr = newval;
    while (ptr != '\0' && (*ptr == ' ' || (*ptr >= '0' && *ptr <= '9'))) {
        ptr++;
    }

    if (ptr == '\0') {
        return GUC_UNIT_MS;
    }

    if (strcasecmp(ptr, "ms") == 0) {
        return GUC_UNIT_MS;
    } else if (strcasecmp(ptr, "s") == 0) {
        return GUC_UNIT_S;
    } else if (strcasecmp(ptr, "min") == 0) {
        return GUC_UNIT_MIN;
    } else if (strcasecmp(ptr, "hour") == 0) {
        return GUC_UNIT_HOUR;
    } else if (strcasecmp(ptr, "day") == 0) {
        return GUC_UNIT_DAY;
    }

    return GUC_UNIT_INVALID;
}

static int64 guc_time_get_value_by_unit(int64 val, uint32 unit)
{
    switch (unit) {
    case GUC_UNIT_S:
        val = val * 1000;
        break;
    case GUC_UNIT_MIN:
        val = val * 1000 * 60;
        break;
    case GUC_UNIT_HOUR:
        val = val * 1000 * 3600;
        break;
    case GUC_UNIT_DAY:
        val = val * 1000 * 3600 * 24;
        break;
    case GUC_UNIT_MS:
    default:
        break;
    }

    return val;
}

static bool32 guc_time_check_hook(char* newval, void* config)
{
    if (newval == NULL) {
        return FALSE;
    }

    uint32 unit = guc_time_get_unit(newval);
    if (unit == GUC_UNIT_INVALID) {
        return FALSE;
    }

    config_int64 *conf = (config_int64 *)config;
    int64 val = atoll(newval);
    if (guc_time_get_value_by_unit(conf->min, unit) > guc_time_get_value_by_unit(val, unit) ||
        guc_time_get_value_by_unit(conf->max, unit) < guc_time_get_value_by_unit(val, unit)) {
        return FALSE;
    }

    return TRUE;
}

static void guc_time_assign_hook(char* newval, void* extra)
{
    uint32 unit = guc_time_get_unit(newval);
    switch (unit) {
    case GUC_UNIT_S:
        *(int64 *)extra = atoll(newval) * 1000;
        break;
    case GUC_UNIT_MIN:
        *(int64 *)extra = atoll(newval) * 1000 * 60;
        break;
    case GUC_UNIT_HOUR:
        *(int64 *)extra = atoll(newval) * 1000 * 3600;
        break;
    case GUC_UNIT_DAY:
        *(int64 *)extra = atoll(newval) * 1000 * 3600 * 24;
        break;
    case GUC_UNIT_MS:
    default:
        *(int64 *)extra = atoll(newval);
        break;
    }
}

static void guc_time_init_hook(int64 newval, void* config)
{
    config_int64 *conf = (config_int64 *)config;
    *conf->variable = guc_time_get_value_by_unit(newval, conf->gen.flags);
}

static bool32 guc_int32_check_hook(char *newval, void *config)
{
    config_int32 *conf = (config_int32 *)config;

    if (newval == NULL) {
        return FALSE;
    }

    int32 val = atoi(newval);
    if (conf->min > val || conf->max < val) {
        return FALSE;
    }

    return TRUE;
}

static void guc_int32_assign_hook(char *newval, void *extra)
{
    *(int32 *)extra = atoi(newval);
}

static bool32 guc_int64_check_hook(char *newval, void *config)
{
    config_int64 *conf = (config_int64 *)config;

    if (newval == NULL) {
        return FALSE;
    }

    int64 val = atoll(newval);
    if (conf->min > val || conf->max < val) {
        return FALSE;
    }

    return TRUE;
}

static void guc_int64_assign_hook(char *newval, void *extra)
{
    *(int64 *)extra = atoll(newval);
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
    for (i = 0; ConfigureNamesInt32[i].gen.name; i++) {
        config_int32 *conf = &ConfigureNamesInt32[i];
        num_vars++;
    }
    for (i = 0; ConfigureNamesInt64[i].gen.name; i++) {
        config_int64 *conf = &ConfigureNamesInt64[i];
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
    for (i = 0; ConfigureNamesInt32[i].gen.name; i++) {
        if (!ConfigureNamesInt32[i].check_hook) {
            ConfigureNamesInt32[i].check_hook = guc_int32_check_hook;
        }
        if (!ConfigureNamesInt32[i].assign_hook) {
            ConfigureNamesInt32[i].assign_hook = guc_int32_assign_hook;
        }
        if (ConfigureNamesInt32[i].init_hook) {
            ConfigureNamesInt32[i].init_hook(ConfigureNamesInt32[i].boot_val, &ConfigureNamesInt32[i]);
        } else {
            *ConfigureNamesInt32[i].variable = ConfigureNamesInt32[i].boot_val;
        }
        guc_vars[num_vars++] = &ConfigureNamesInt32[i].gen;
    }
    for (i = 0; ConfigureNamesInt64[i].gen.name; i++) {
        if (!ConfigureNamesInt64[i].check_hook) {
            ConfigureNamesInt64[i].check_hook = guc_int64_check_hook;
        }
        if (!ConfigureNamesInt64[i].assign_hook) {
            ConfigureNamesInt64[i].assign_hook = guc_int64_assign_hook;
        }
        if (ConfigureNamesInt64[i].init_hook) {
            ConfigureNamesInt64[i].init_hook(ConfigureNamesInt64[i].boot_val, &ConfigureNamesInt64[i]);
        } else {
            *ConfigureNamesInt64[i].variable = ConfigureNamesInt64[i].boot_val;
        }
        guc_vars[num_vars++] = &ConfigureNamesInt64[i].gen;
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

config_generic* find_guc_variable(char* key_name)
{
    config_generic **conf;
    char **key = &key_name;

    conf = (config_generic **)bsearch((void *)(&key),
        (void **) guc_variables, num_guc_variables,
        sizeof(config_generic *), guc_var_compare);

    return conf ? *conf : NULL;
}

bool32 set_guc_option_value(config_generic* gconfig, char* value)
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
    case GUC_TYPE_INT32:
    {
        config_int32 *conf = (config_int32 *)gconfig;

        if (conf->check_hook && !conf->check_hook(value, conf)) {
            return FALSE;
        }

        if (conf->assign_hook) {
            conf->assign_hook(value, (void *)conf->variable);
        }
        break;
    }
    case GUC_TYPE_INT64:
    {
        config_int64 *conf = (config_int64 *)gconfig;

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





status_t initialize_guc_options(char* config_file, attribute_t* attr)
{
    build_guc_variables();

    char *section = NULL, *key = NULL, *value = NULL;
    config_lines* lines = read_lines_from_config_file(config_file);
    for (uint32 i = 0; i < lines->num_lines; i++) {
        if (!parse_key_value_from_config_line(lines->lines[i], &section, &key, &value)) {
            printf("Invalid config: %s\n", key);
            return CM_ERROR;
        }
        if (key == NULL) {
            continue;
        }
        //printf("config: key %s value %s\n", key, value);
        config_generic* conf = find_guc_variable(key);
        if (conf == NULL) {
            printf("Invalid config, not found key = %s, value %s\n", key, value);
            continue;
        }
        set_guc_option_value(conf, value);
    }

    *attr = g_guc_options;

    return CM_SUCCESS;
}
