#ifndef _CM_ATTRIUBTE_H
#define _CM_ATTRIUBTE_H

#include "cm_type.h"


#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/* WAL levels */
typedef enum en_wal_level {
    WAL_LEVEL_MINIMAL = 0,
    WAL_LEVEL_ARCHIVE,
    WAL_LEVEL_HOT_STANDBY,
    WAL_LEVEL_LOGICAL
} wal_level_t;


typedef struct st_attr_storage {
    int32       wal_level;
    bool32      flush_log_at_commit;
    char*       flush_method;
    char*       transaction_isolation;
    int32       lru_scan_depth;
    int64       buffer_pool_size;
    int32       buffer_pool_instances;

    int32       max_dirty_pages_pct;
    int32       io_capacity;
    int32       io_capacity_max;
    int32       read_io_threads;
    int32       write_io_threads;
    int32       purge_threads;

    int64       undo_cache_size;
    int32       undo_segment_count;

    int64       redo_log_buffer_size;
    int32       redo_log_file_size;
    int32       redo_log_files;

    int64       lock_wait_timeout;



} attr_storage_t;

typedef struct st_attr_replication {
    int32       enable_logical_rep;

} attr_replication_t;

typedef struct st_attr_memory {
    int64      query_memory_cache_size;
    int64      query_memory_cache_limit;
    int64      plan_memory_cache_size;
    int64      dictionary_memory_cache_size;
    int64      common_memory_cache_size;
    int644     common_ext_memory_cache_size;
    int64      mtr_memory_cache_size;
    int64      temp_memory_cache_size;

    int32      table_hash_array_size;
} attr_memory_t;


typedef struct st_attr_sql {

    int32       table_open_cache;

    //double      sample_rate;
} attr_sql_t;

typedef struct st_attr_network {
    char*       host_address;
    int32       port;
    int32       max_connections;
    int64       max_allowed_package;
    
    int64       session_wait_timeout;

} attr_network_t;


typedef struct st_attr_common {
    uint32      session_count;
    uint32      session_stack_size;

    char*       character_set_client;
    char*       character_set_server;

    int32       thread_pool_size;
    int64       thread_stack_size;
    int32       thread_stack_depth;
        
    bool32      read_db_only;
    int32       server_id;

    int32       open_files_limit;

} attr_common_t;


typedef struct st_attribute {
    attr_sql_t          attr_sql;
    attr_storage_t      attr_storage;
    attr_replication_t  attr_rep;
    attr_network_t      attr_network;
    attr_common_t       attr_common;
    attr_memory_t       attr_memory;
} attribute_t;


#ifdef __cplusplus
}
#endif // __cplusplus

#endif  /* _CM_ATTRIUBTE_H */
