#ifndef _KNL_SESSION_H
#define _KNL_SESSION_H

#include "cm_type.h"
#include "cm_mutex.h"
#include "knl_trx_rseg.h"

/* WAL levels */
typedef enum en_wal_level {
    WAL_LEVEL_MINIMAL = 0,
    WAL_LEVEL_ARCHIVE,
    WAL_LEVEL_HOT_STANDBY,
    WAL_LEVEL_LOGICAL
} wal_level_t;



typedef struct st_attr_storage {
    uint32      wal_level;
    bool32      flush_log_at_commit;
    char*       flush_method;
    char*       transaction_isolation;
    int32       lru_scan_depth;
    int32       buffer_pool_size;
    int32       buffer_pool_instances;
    int32       temp_buffer_size;
    int32       max_dirty_pages_pct;
    int32       io_capacity;
    int32       io_capacity_max;
    int32       read_io_threads;
    int32       write_io_threads;
    int32       undo_buffer_size;
    int32       redo_log_buffer_size;
    int32       redo_log_file_size;
    int32       redo_log_files;
    int32       lock_wait_timeout;
} attr_storage_t;

typedef struct st_attr_replication {
    int         enable_logical_rep;

} attr_replication_t;


typedef struct st_attr_sql {
    int         query_cache_size;
    int         query_cache_limit;
    int         plan_buffer_size;

    int         table_open_cache;

    double      sample_rate;
} attr_sql_t;

typedef struct st_attr_network {
    char*       host_address;
    int         port;
    int         max_connections;
    int         max_allowed_package;
    
    int         session_wait_timeout;

} attr_network_t;


typedef struct st_attr_common {
    uint32      session_count;
    uint32      session_stack_size;

    char*       character_set_client;
    char*       character_set_server;

    uint32      thread_pool_size;
    uint32      thread_stack_size;
    uint32      thread_stack_depth;

    char*       base_directory;

    bool32      read_db_only;
    int         server_id;

    int         open_files_limit;

} attr_common_t;


typedef struct st_attribute {
    attr_sql_t          attr_sql;
    attr_storage_t      attr_storage;
    attr_replication_t  attr_rep;
    attr_network_t      attr_network;
    attr_common_t       attr_common;
} attribute_t;


typedef struct st_que_sess que_sess_t;
struct st_que_sess {
    mutex_t        mutex;
    uint32         id;  // session id
    command_id_t   cid;
    trx_slot_id_t  slot;
    cm_stack_t     stack;

    trx_t*         trx;

    attribute_t*   attr;

    memory_stack_context_t* stack_context;
    UT_LIST_NODE_T(que_sess_t) list_node;
};


typedef struct st_session_pool {
    mutex_t        mutex;
    que_sess_t*    sessions;
    UT_LIST_BASE_NODE_T(que_sess_t)  used_sess_list;
    UT_LIST_BASE_NODE_T(que_sess_t)  free_sess_list;
} session_pool_t;


extern status_t sess_pool_create(uint32 sess_count);

extern inline void que_sess_init(que_sess_t* sess, uint32 stack_size);
extern inline void que_sess_destroy(que_sess_t* sess);

extern attribute_t       g_attribute;
extern session_pool_t*   g_sess_pool;


#endif  /* _KNL_SESSION_H */
