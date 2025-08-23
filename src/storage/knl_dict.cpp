#include "knl_dict.h"
#include "cm_memory.h"
#include "cm_log.h"
#include "cm_list.h"
#include "cm_timer.h"
#include "knl_buf.h"
#include "knl_btree.h"
#include "knl_fsp.h"
#include "knl_heap.h"
#include "knl_trx.h"
#include "knl_handler.h"

dict_sys_t*     dict_sys = NULL;

/** dummy index for ROW_FORMAT=REDUNDANT supremum and infimum records */
dict_index_t*   dict_ind_redundant;

/**********************************************************************//**
Gets a pointer to the dictionary header and x-latches its page.
@return pointer to the dictionary header, page x-latched */
dict_hdr_t* dict_hdr_get(mtr_t* mtr)
{
    buf_block_t* block;
    dict_hdr_t* header;
    const page_id_t page_id(DICT_HDR_SPACE_ID, DICT_HDR_PAGE_NO);
    const page_size_t page_size(DICT_HDR_SPACE_ID);

    block = buf_page_get(page_id, page_size, RW_X_LATCH, mtr);
    header = DICT_HDR + buf_block_get_frame(block);

    //buf_block_dbg_add_level(block, SYNC_DICT_HEADER);

    return header;
}

// Creates and initializes the data dictionary at the server bootstrap.
// Creates the file page for the dictionary header.
// This function is called only at the database creation.
status_t dict_create()
{
    status_t err = CM_ERROR;
    mtr_t init_mtr, *mtr = &init_mtr;

    LOGGER_INFO(LOGGER, LOG_MODULE_DICTIONARY, "create dictionary header");

    mtr_start(mtr);

    const page_id_t page_id(DICT_HDR_SPACE_ID, FSP_DICT_HDR_PAGE_NO);
    const page_size_t page_size(DICT_HDR_SPACE_ID);
    buf_block_t* block = buf_page_create(page_id, page_size, RW_X_LATCH, Page_fetch::RESIDENT, mtr);
    ut_a(block->get_page_no() == FSP_DICT_HDR_PAGE_NO);
    //buf_block_dbg_add_level(block, SYNC_DICT_HEADER);

    page_t* page = buf_block_get_frame(block);
    mlog_write_uint32(page + FIL_PAGE_TYPE, FIL_PAGE_TYPE_DICT_HDR | FIL_PAGE_TYPE_RESIDENT_FLAG, MLOG_2BYTES, mtr);

    dict_hdr_t* dict_header = DICT_HDR + buf_block_get_frame(block);

    // Start counting row, table, index, and tree ids from DICT_HDR_FIRST_ID
    mlog_write_uint64(dict_header + DICT_HDR_ROW_ID, DICT_HDR_FIRST_ID, mtr);
    mlog_write_uint64(dict_header + DICT_HDR_TABLE_ID, DICT_HDR_FIRST_ID, mtr);
    mlog_write_uint64(dict_header + DICT_HDR_INDEX_ID, DICT_HDR_FIRST_ID, mtr);
    mlog_write_uint32(dict_header + DICT_HDR_MAX_SPACE_ID, 0, MLOG_4BYTES, mtr);
    // Obsolete, but we must initialize it anyway.
    mlog_write_uint32(dict_header + DICT_HDR_MIX_ID_LOW, DICT_HDR_FIRST_ID, MLOG_4BYTES, mtr);

    // Create heap entry and B-tree root for system tables

    // sys_tables
    uint32 root_page_no = heap_create_entry(FIL_SYSTEM_SPACE_ID);
    if (root_page_no == FIL_NULL) {
        goto err_exit;
    }
    mlog_write_uint32(dict_header + DICT_HDR_TABLES, root_page_no, MLOG_4BYTES, mtr);

    root_page_no = btr_create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE_ID, page_size, DICT_TABLES_CLUST_ID, dict_ind_redundant, NULL, mtr);
    if (root_page_no == FIL_NULL) {
        goto err_exit;
    }
    mlog_write_uint32(dict_header + DICT_HDR_TABLES_CLUST, root_page_no, MLOG_4BYTES, mtr);

    root_page_no = btr_create(DICT_UNIQUE, DICT_HDR_SPACE_ID, page_size, DICT_TABLES_INDEX_ID_ID, dict_ind_redundant, NULL, mtr);
    if (root_page_no == FIL_NULL) {
        goto err_exit;
    }
    mlog_write_uint32(dict_header + DICT_HDR_TABLES_IDS, root_page_no, MLOG_4BYTES, mtr);

    // sys_columns
    root_page_no = heap_create_entry(FIL_SYSTEM_SPACE_ID);
    if (root_page_no == FIL_NULL) {
        goto err_exit;
    }
    mlog_write_uint32(dict_header + DICT_HDR_COLUMNS, root_page_no, MLOG_4BYTES, mtr);

    root_page_no = btr_create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE_ID, page_size, DICT_COLUMNS_CLUST_ID, dict_ind_redundant, NULL, mtr);
    if (root_page_no == FIL_NULL) {
        goto err_exit;
    }
    mlog_write_uint32(dict_header + DICT_HDR_COLUMNS_CLUST, root_page_no, MLOG_4BYTES, mtr);

    // sys_indexes
    root_page_no = heap_create_entry(FIL_SYSTEM_SPACE_ID);
    if (root_page_no == FIL_NULL) {
        goto err_exit;
    }
    mlog_write_uint32(dict_header + DICT_HDR_INDEXES, root_page_no, MLOG_4BYTES, mtr);

    root_page_no = btr_create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE_ID, page_size, DICT_INDEXES_CLUST_ID, dict_ind_redundant, NULL, mtr);
    if (root_page_no == FIL_NULL) {
        goto err_exit;
    }
    mlog_write_uint32(dict_header + DICT_HDR_INDEXES_CLUST, root_page_no, MLOG_4BYTES, mtr);

    // sys_fields
    root_page_no = heap_create_entry(FIL_SYSTEM_SPACE_ID);
    if (root_page_no == FIL_NULL) {
        goto err_exit;
    }
    mlog_write_uint32(dict_header + DICT_HDR_FIELDS, root_page_no, MLOG_4BYTES, mtr);

    root_page_no = btr_create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE_ID, page_size, DICT_FIELDS_CLUST_ID, dict_ind_redundant, NULL, mtr);
    if (root_page_no == FIL_NULL) {
        goto err_exit;
    }
    mlog_write_uint32(dict_header + DICT_HDR_FIELDS_CLUST, root_page_no, MLOG_4BYTES, mtr);

    // sys_users
    root_page_no = heap_create_entry(FIL_SYSTEM_SPACE_ID);
    if (root_page_no == FIL_NULL) {
        goto err_exit;
    }
    mlog_write_uint32(dict_header + DICT_HDR_USERS, root_page_no, MLOG_4BYTES, mtr);

    root_page_no = btr_create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE_ID, page_size, DICT_USERS_CLUST_ID, dict_ind_redundant, NULL, mtr);
    if (root_page_no == FIL_NULL) {
        goto err_exit;
    }
    mlog_write_uint32(dict_header + DICT_HDR_USERS_CLUST, root_page_no, MLOG_4BYTES, mtr);

    root_page_no = btr_create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE_ID, page_size, DICT_USERS_INDEX_ID_ID, dict_ind_redundant, NULL, mtr);
    if (root_page_no == FIL_NULL) {
        goto err_exit;
    }
    mlog_write_uint32(dict_header + DICT_HDR_USERS_IDS, root_page_no, MLOG_4BYTES, mtr);

    err = CM_SUCCESS;

err_exit:

    mtr_commit(mtr);

    return err;
}

static status_t dict_init(memory_pool_t* mem_pool, uint64 memory_cache_size, uint32 table_hash_array_size)
{
    dict_sys = (dict_sys_t*)ut_malloc_zero(sizeof(dict_sys_t));
    if (dict_sys == NULL) {
        return CM_ERROR;
    }

    mutex_create(&dict_sys->mutex);
    dict_sys->mem_pool = mem_pool;
    dict_sys->memory_cache_size = 0;
    dict_sys->memory_cache_max_size = memory_cache_size;
    for (uint32 i = 0; i < DICT_TABLE_LRU_LIST_COUNT; i++) {
        mutex_create(&dict_sys->table_LRU_list_mutex[i]);
        UT_LIST_INIT(dict_sys->table_LRU_list[i]);
    }

    dict_sys->table_hash = HASH_TABLE_CREATE(table_hash_array_size, HASH_TABLE_SYNC_RW_LOCK, 4096);
    if (dict_sys->table_hash == NULL) {
        return CM_ERROR;
    }

    dict_sys->table_id_hash = HASH_TABLE_CREATE(table_hash_array_size, HASH_TABLE_SYNC_RW_LOCK, 4096);
    if (dict_sys->table_id_hash == NULL) {
        return CM_ERROR;
    }

    return CM_SUCCESS;
}


// Initializes the data dictionary memory structures when the database is started.
// This function is also called when the data dictionary is created.
status_t dict_boot(memory_pool_t* mem_pool, uint64 memory_cache_size, uint32 table_hash_array_size)
{
    status_t        err;
    dict_table_t*   table;
    dict_index_t*   index;
    dict_hdr_t*     dict_hdr;
    mtr_t           mtr;

    // Create dict_sys and hash tables etc
    err = dict_init(mem_pool, memory_cache_size, table_hash_array_size);
    CM_RETURN_IF_ERROR(err);

    // load basic system table
    mtr_start(&mtr);

    // Get the dictionary header
    dict_hdr = dict_hdr_get(&mtr);

    dict_sys->row_id = mach_read_from_8(dict_hdr + DICT_HDR_ROW_ID);
    dict_sys->table_id = mach_read_from_8(dict_hdr + DICT_HDR_TABLE_ID);
    dict_sys->index_id = mach_read_from_8(dict_hdr + DICT_HDR_INDEX_ID);
    dict_sys->max_space_id = mach_read_from_8(dict_hdr + DICT_HDR_MAX_SPACE_ID);
    dict_sys->mix_id_low = mach_read_from_8(dict_hdr + DICT_HDR_MIX_ID_LOW);

    // 1. SYS_TABLES
    table = dict_mem_table_create("SYS_TABLES", DICT_TABLES_ID, DICT_SYS_USER_ID, DICT_HDR_SPACE_ID, 22);
    dict_mem_table_add_col(table, "USER_ID", DATA_BIGINT, 0, 8);
    dict_mem_table_add_col(table, "ID", DATA_BIGINT, 0, 8);
    dict_mem_table_add_col(table, "NAME", DATA_VARCHAR, 0, DB_OBJECT_NAME_MAX_LEN);
    dict_mem_table_add_col(table, "TYPE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "COLS", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "INDEXES", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "PARTITIONED", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "INITRANS", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "PCTFREE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "CRMODE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "RECYCLED", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "APPEND_ONLY", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "NUM_ROWS", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "BLOCKS", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "EMPTY_BLOCKS", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "AVG_ROW_LEN", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "SIMPLE_SIZE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "ANALYZE_TIME", DATA_TIMESTAMP, 0, 8);
    dict_mem_table_add_col(table, "OPTIONS", DATA_RAW, 0, 1024);
    dict_mem_table_add_col(table, "SPACE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "ENTRY", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "FLAGS", DATA_INTEGER, 0, 4);

    table->entry_page_no = mach_read_from_4(dict_hdr + DICT_HDR_TABLES);
    dict_add_table_to_cache(table, FALSE);
    dict_sys->sys_tables = table;

    index = dict_mem_index_create(table, "CLUST_IND", DICT_TABLES_CLUST_ID, DICT_HDR_SPACE_ID, DICT_UNIQUE | DICT_CLUSTERED, 2);
    dict_mem_index_add_field(index, "USER_ID", 0);
    dict_mem_index_add_field(index, "NAME", 0);
    index->entry_page_no = mach_read_from_4(dict_hdr + DICT_HDR_TABLES_CLUST);

    index = dict_mem_index_create(table, "ID_IND", DICT_TABLES_INDEX_ID_ID, DICT_HDR_SPACE_ID, DICT_UNIQUE, 1);
    dict_mem_index_add_field(index, "ID", 0);
    index->entry_page_no = mach_read_from_4(dict_hdr + DICT_HDR_TABLES_IDS);

    // 2. SYS_COLUMNS
    table = dict_mem_table_create("SYS_COLUMNS", DICT_COLUMNS_ID, DICT_SYS_USER_ID, DICT_HDR_SPACE_ID, 18);
    dict_mem_table_add_col(table, "USER_ID", DATA_BIGINT, 0, 8);
    dict_mem_table_add_col(table, "TABLE_ID", DATA_BIGINT, 0, 8);
    dict_mem_table_add_col(table, "ID", DATA_BIGINT, 0, 8);
    dict_mem_table_add_col(table, "POS", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "NAME", DATA_VARCHAR, 0, DB_OBJECT_NAME_MAX_LEN);
    dict_mem_table_add_col(table, "MTYPE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "PRTYPE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "LEN", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "PREC", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "NULLABLE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "DEFAULT_TEXT", DATA_VARCHAR, 0, 1024);
    dict_mem_table_add_col(table, "DEFAULT_DATA", DATA_RAW, 0, 4096);
    dict_mem_table_add_col(table, "NUM_DISTINCT", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "LOW_VALUE", DATA_VARCHAR, 0, 64);
    dict_mem_table_add_col(table, "HIGH_VALUE", DATA_VARCHAR, 0, 64);
    dict_mem_table_add_col(table, "HISTOGRAM", DATA_VARCHAR, 0, 64);
    dict_mem_table_add_col(table, "OPTIONS", DATA_RAW, 0, 1024);
    dict_mem_table_add_col(table, "FLAGS", DATA_INTEGER, 0, 4);

    table->entry_page_no = mach_read_from_4(dict_hdr + DICT_HDR_COLUMNS);
    dict_add_table_to_cache(table, FALSE);
    dict_sys->sys_columns = table;

    index = dict_mem_index_create(table, "CLUST_IND", DICT_COLUMNS_CLUST_ID, DICT_HDR_SPACE_ID, DICT_UNIQUE | DICT_CLUSTERED, 2);
    dict_mem_index_add_field(index, "TABLE_ID", 0);
    dict_mem_index_add_field(index, "POS", 0);
    index->entry_page_no = mach_read_from_4(dict_hdr + DICT_HDR_COLUMNS_CLUST);

    // 3. SYS_INDEXES
    table = dict_mem_table_create("SYS_INDEXES", DICT_INDEXES_ID, DICT_SYS_USER_ID, DICT_HDR_SPACE_ID, 22);
    dict_mem_table_add_col(table, "USER_ID", DATA_BIGINT, 0, 8);
    dict_mem_table_add_col(table, "TABLE_ID", DATA_BIGINT, 0, 8);
    dict_mem_table_add_col(table, "ID", DATA_BIGINT, 0, 8);
    dict_mem_table_add_col(table, "NAME", DATA_VARCHAR, 0, DB_OBJECT_NAME_MAX_LEN);
    dict_mem_table_add_col(table, "IS_PRIMARY", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "IS_UNIQUE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "TYPE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "COLS", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "PARTITIONED", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "INITRANS", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "PCTFREE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "CRMODE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "DISTINCT_KEYS", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "LEVEL", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "LEVEL_BLOCKS", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "EMPTY_LEAF_BLOCKS", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "CLUFAC", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "SIMPLE_SIZE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "ANALYZE_TIME", DATA_TIMESTAMP, 0, 8);
    dict_mem_table_add_col(table, "OPTIONS", DATA_RAW, 0, 1024);
    dict_mem_table_add_col(table, "SPACE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "ENTRY", DATA_INTEGER, 0, 4);

    table->entry_page_no = mach_read_from_4(dict_hdr + DICT_HDR_INDEXES);
    dict_add_table_to_cache(table, FALSE);
    dict_sys->sys_indexes = table;

    index = dict_mem_index_create(table, "CLUST_IND", DICT_INDEXES_CLUST_ID, DICT_HDR_SPACE_ID, DICT_UNIQUE | DICT_CLUSTERED, 2);
    dict_mem_index_add_field(index, "TABLE_ID", 0);
    dict_mem_index_add_field(index, "ID", 0);
    index->entry_page_no = mach_read_from_4(dict_hdr + DICT_HDR_INDEXES_CLUST);

    // 4. SYS_FIELDS
    table = dict_mem_table_create("SYS_FIELDS", DICT_FIELDS_ID, DICT_SYS_USER_ID, DICT_HDR_SPACE_ID, 3);
    dict_mem_table_add_col(table, "INDEX_ID", DATA_BIGINT, 0, 8);
    dict_mem_table_add_col(table, "POS", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "COL_NAME", DATA_VARCHAR, 0, DB_OBJECT_NAME_MAX_LEN);

    table->entry_page_no = mach_read_from_4(dict_hdr + DICT_HDR_FIELDS);
    dict_add_table_to_cache(table, FALSE);
    dict_sys->sys_fields = table;

    index = dict_mem_index_create(table, "CLUST_IND", DICT_FIELDS_CLUST_ID, DICT_HDR_SPACE_ID, DICT_UNIQUE | DICT_CLUSTERED, 2);
    dict_mem_index_add_field(index, "INDEX_ID", 0);
    dict_mem_index_add_field(index, "POS", 0);
    index->entry_page_no = mach_read_from_4(dict_hdr + DICT_HDR_FIELDS_CLUST);

    // 5. SYS_USER
    table = dict_mem_table_create("SYS_USERS", DICT_FIELDS_ID, DICT_SYS_USER_ID, DICT_HDR_SPACE_ID, 10);
    dict_mem_table_add_col(table, "ID", DATA_BIGINT, 0, 8);
    dict_mem_table_add_col(table, "NAME", DATA_VARCHAR, 0, DB_OBJECT_NAME_MAX_LEN);
    dict_mem_table_add_col(table, "PASSWORD", DATA_RAW, 0, DB_OBJECT_NAME_MAX_LEN);
    dict_mem_table_add_col(table, "CREATE_TIME", DATA_DATE, 0, 8);
    dict_mem_table_add_col(table, "EXPIRED_TIME", DATA_DATE, 0, 8);
    dict_mem_table_add_col(table, "LOCKED_TIME", DATA_DATE, 0, 8);
    dict_mem_table_add_col(table, "OPTIONS", DATA_RAW, 0, 1024);
    dict_mem_table_add_col(table, "DATA_SPACE", DATA_INTEGER, 0, 4);  // default tablespace
    dict_mem_table_add_col(table, "TEMP_SPACE", DATA_INTEGER, 0, 4);  // temporary tablespace
    dict_mem_table_add_col(table, "STATUS", DATA_INTEGER, 0, 4);  // 0 = Open, 1 = Locked, 2 = Expired

    table->entry_page_no = mach_read_from_4(dict_hdr + DICT_HDR_USERS);
    dict_add_table_to_cache(table, FALSE);
    dict_sys->sys_tables = table;

    index = dict_mem_index_create(table, "CLUST_IND", DICT_USERS_CLUST_ID, DICT_HDR_SPACE_ID, DICT_UNIQUE | DICT_CLUSTERED, 1);
    dict_mem_index_add_field(index, "NAME", 0);
    index->entry_page_no = mach_read_from_4(dict_hdr + DICT_HDR_USERS_CLUST);

    index = dict_mem_index_create(table, "ID_IND", DICT_USERS_INDEX_ID_ID, DICT_HDR_SPACE_ID, DICT_UNIQUE, 1);
    dict_mem_index_add_field(index, "ID", 0);
    index->entry_page_no = mach_read_from_4(dict_hdr + DICT_HDR_USERS_IDS);

    mtr_commit(&mtr);

    return CM_SUCCESS;
}







static inline uint64 dict_cache_clean_memory_size()
{
    uint64 size = (uint64)(dict_sys->memory_cache_max_size * 0.01);
    if (size < 1024 * 512) {
        size = 1024 * 512;
    }

    return size;
}

static inline void dict_cache_lru_remove_tables_low(uint32 lru_list_id)
{
    uint64 size = dict_cache_clean_memory_size();
    uint64 skip_table_id = DICT_INVALID_OBJECT_ID;
    uint32 loop = 0, loop_max_count;
    uint32 need_skip_count = 0;

    mutex_enter(&dict_sys->table_LRU_list_mutex[lru_list_id]);
    loop_max_count = UT_LIST_GET_LEN(dict_sys->table_LRU_list[lru_list_id]);
    mutex_exit(&dict_sys->table_LRU_list_mutex[lru_list_id]);

    while (loop < loop_max_count &&
           dict_sys->memory_cache_max_size < dict_sys->memory_cache_size + size) {

        dict_table_t* table;
        char table_name[DB_OBJECT_NAME_MAX_LEN];
        uint64 user_id;

        mutex_enter(&dict_sys->table_LRU_list_mutex[lru_list_id]);
        table = UT_LIST_GET_LAST(dict_sys->table_LRU_list[lru_list_id]);
        while (table) {
            if (DICT_IN_USE(table) || DICT_IS_HOT(table) || table->id == skip_table_id) {
                // 
                table->touch_number = table->touch_number / 2;
                // shift to first
                UT_LIST_REMOVE(table_LRU, dict_sys->table_LRU_list[lru_list_id], table);
                UT_LIST_ADD_FIRST(table_LRU, dict_sys->table_LRU_list[lru_list_id], table);
                //
                table = UT_LIST_GET_LAST(dict_sys->table_LRU_list[lru_list_id]);
                continue;
            }

            // found a evicter
            break;
        }

        if (table) {
            skip_table_id = table->id;
            memcpy(table_name, table->name, strlen(table->name) + 1);
            user_id = table->user_id;
        }
        mutex_exit(&dict_sys->table_LRU_list_mutex[lru_list_id]);

        //
        if (dict_remove_table_from_cache(user_id, table_name)) {
            skip_table_id = DICT_INVALID_OBJECT_ID;
        }

        loop++;
    }
}

static inline void dict_cache_lru_remove_tables(table_id_t table_id)
{
    uint64 size = dict_cache_clean_memory_size();

    for (uint32 i = 0; i < DICT_TABLE_LRU_LIST_COUNT; i++) {
        if (dict_sys->memory_cache_max_size >= dict_sys->memory_cache_size + size) {
            break;
        }

        dict_cache_lru_remove_tables_low((i + table_id) % DICT_TABLE_LRU_LIST_COUNT);
    }
}


static memory_stack_context_t* dict_cache_alloc_mem_stack_context()
{
    return mcontext_stack_create(dict_sys->mem_pool);
}

static void* dict_cache_alloc_memory(table_id_t table_id, memory_stack_context_t* mem_stack_ctx, uint32 size)
{
    void*        mem_ptr;
    const uint32 max_loop_count = 1000;
    uint32       loop = 0;

    ut_ad(mem_stack_ctx);
    ut_ad(size > 0);

retry:

    mem_ptr = mcontext_stack_push(mem_stack_ctx, size);
    if (mem_ptr == NULL && loop < max_loop_count) {
        if (loop > 0) {
            os_thread_sleep(1000);
        }
        dict_cache_lru_remove_tables(table_id);
        loop++;
        goto retry;
    }

    if (loop > 0) {
        srv_stats.dict_alloc_wait_count.add(loop);
    }

    if (UNLIKELY(mem_ptr == NULL)) {
        CM_SET_ERROR(ERR_ALLOC_MEMORY_REACH_LIMIT, "dictionary cache", dict_sys->memory_cache_max_size);
    }

    return mem_ptr;
}

static void dict_cache_free_memory(memory_stack_context_t* mcontext_stack, void* ptr, uint32 size)
{
    mcontext_stack_pop(mcontext_stack, ptr, size);
}

inline void dict_mem_table_destroy(dict_table_t* table)
{
    ut_ad(table->to_be_cache_removed);
    ut_ad(table->ref_count == 1);

    mutex_destroy(&table->mutex);
    mcontext_stack_destroy(table->mcontext_stack);
}

dict_table_t* dict_mem_table_create(
    const char* name,
    table_id_t  table_id,
    uint64      user_id,
    uint32      space_id,
    uint16      column_count)
{
    dict_table_t* table;
    uint32 name_len = (uint32)strlen(name) + 1;

    if (name_len <= 1 || name_len >= DB_OBJECT_NAME_MAX_LEN) {
        return NULL;
    }

    if (column_count == 0 || column_count > DICT_TABLE_COLUMN_MAX_COUNT) {
        return NULL;
    }

    memory_stack_context_t* mem_stack_ctx = dict_cache_alloc_mem_stack_context();
    if (mem_stack_ctx == NULL) {
        CM_SET_ERROR(ERR_CREATE_MEMORY_CONTEXT, "creating table");
        return NULL;
    }
    table = (dict_table_t *)dict_cache_alloc_memory(table_id, mem_stack_ctx, sizeof(dict_table_t));
    if (table == NULL) {
        CM_SET_ERROR(ERR_ALLOC_MEMORY, sizeof(dict_table_t), "creating table");
        goto err_exit;
    }
    memset((char *)table, 0x00, sizeof(dict_table_t));

    table->mcontext_stack = mem_stack_ctx;
    table->ref_count = 0;
    table->id = table_id;
    table->user_id = user_id;
    table->space_id = space_id;
    table->entry_page_no = FIL_NULL;
    table->init_trans = DICT_INI_TRANS;
    table->pctfree = DICT_PCT_FREE;
    table->cr_mode = DICT_CR_ROW;
    table->column_count = column_count;
    table->column_index = 0;
    table->status = 0;
    mutex_create(&table->mutex);
    UT_LIST_INIT(table->indexes);

    table->name = (char *)dict_cache_alloc_memory(table_id, table->mcontext_stack, name_len);
    if (table->name == NULL) {
        CM_SET_ERROR(ERR_ALLOC_MEMORY, name_len, "creating table");
        goto err_exit;
    }
    memcpy(table->name, name, name_len);

    table->columns = (dict_col_t**)dict_cache_alloc_memory(table_id, table->mcontext_stack,
                                                           column_count * sizeof(dict_col_t*));
    if (table->columns == NULL) {
        CM_SET_ERROR(ERR_ALLOC_MEMORY, column_count * sizeof(dict_col_t*), "creating table");
        goto err_exit;
    }
    memset((char *)table->columns, 0x00, column_count * sizeof(dict_col_t*));

    return table;

err_exit:

    mcontext_stack_destroy(mem_stack_ctx);

    return NULL;
}

status_t dict_mem_table_add_col(
    dict_table_t*   table,
    const char*     name,
    data_type_t     mtype,  // in: main datatype
    uint32          precision, // in: precise type
    uint32          scale_or_len)
{
    if (table->column_index >= table->column_count) {
        CM_SET_ERROR(ERR_COLUMN_COUNT_REACH_LIMIT, table->column_count);
        return CM_ERROR;
    }

    for (uint32 i = 0; i < table->column_index; i++) {
        if (strncasecmp(name, table->columns[i]->name, strlen(name) + 1) == 0) {
            CM_SET_ERROR(ERR_COLUMN_EXISTS, name);
            return CM_ERROR;
        }
    }

    void* save_ptr = mcontext_stack_save(table->mcontext_stack);

    dict_col_t* col = (dict_col_t*)dict_cache_alloc_memory(table->id, table->mcontext_stack, sizeof(dict_col_t));
    if (col == NULL) {
        CM_SET_ERROR(ERR_ALLOC_MEMORY, sizeof(dict_col_t), "add column");
        goto err_exit;
    }
    memset((char *)col, 0x00, sizeof(dict_col_t));
    col->mtype = mtype;

    col->name = (char *)dict_cache_alloc_memory(table->id, table->mcontext_stack, (uint32)strlen(name) + 1);
    if (col->name == NULL) {
        CM_SET_ERROR(ERR_ALLOC_MEMORY, (uint32)strlen(name) + 1, "add column");
        return CM_ERROR;
    }
    memcpy(col->name, name, strlen(name) + 1);

    switch (col->mtype) {
    case DATA_VARCHAR:
    case DATA_STRING:
    case DATA_CHAR:
    case DATA_BINARY:
    case DATA_VARBINARY:
    case DATA_RAW:
    case DATA_CLOB:
    case DATA_BLOB1:
        col->len = scale_or_len;
        break;
    case DATA_NUMBER:
    case DATA_DECIMAL:
        col->precision = precision;
        col->scale = scale_or_len;
        break;

    case DATA_INTEGER:
    case DATA_BIGINT:
    case DATA_SMALLINT:
    case DATA_TINYINT:
    case DATA_BOOLEAN:
        break;
    case DATA_FLOAT:
    case DATA_DOUBLE:
    case DATA_REAL:
        break;
    case DATA_TIMESTAMP:
    case DATA_TIMESTAMP_TZ:
    case DATA_TIMESTAMP_LTZ:
    case DATA_DATE:
    case DATA_DATETIME:
    case DATA_TIME:
    case DATA_INTERVAL:
    case DATA_INTERVAL_YM:
    case DATA_INTERVAL_DS:
        break;
    default:
        break;
    }

    //
    col->index = table->column_index;
    table->columns[col->index] = col;
    table->column_index++;

    return CM_SUCCESS;

err_exit:

    mcontext_stack_restore(table->mcontext_stack, save_ptr);

    return CM_ERROR;
}

// Adds a field definition to an index
status_t dict_mem_index_add_field(
    dict_index_t* index,
    const char*   name,
    uint32        prefix_len)
{
    if (index->field_index >= index->field_count) {
        return CM_ERROR;
    }

    uint32 ind = DICT_TABLE_COLUMN_MAX_COUNT;
    for (uint32 i = 0; i < index->table->column_index; i++) {
        if (strncasecmp(name, index->table->columns[i]->name, strlen(name)+1) == 0) {
            ind = i;
            break;
        }
    }
    if (ind == DICT_TABLE_COLUMN_MAX_COUNT) {
        return CM_ERROR;
    }

    dict_field_t* field = dict_index_get_nth_field(index, index->field_index);
    field->col_ind = ind;
    field->prefix_len = prefix_len;

    index->field_index++;

    return CM_SUCCESS;
}

// Creates an index memory object
dict_index_t* dict_mem_index_create(
    dict_table_t*   table,
    const char*     index_name,
    index_id_t      index_id,
    uint32          space_id,
    uint32          type,       // in: DICT_UNIQUE, DICT_CLUSTERED, ... ORed
    uint32          field_count)   // in: number of fields
{
    dict_index_t*   index;
    uint32 index_name_len = (uint32)strlen(index_name) + 1;

    if (index_name_len <= 1 || index_name_len >= DB_OBJECT_NAME_MAX_LEN) {
        return NULL;
    }

    index = UT_LIST_GET_FIRST(table->indexes);
    while (index) {
        if (strncasecmp(index_name, index->name, index_name_len) == 0) {
            return NULL;
        }
        index = UT_LIST_GET_NEXT(list_node, index);
    }

    void* save_ptr = mcontext_stack_save(table->mcontext_stack);

    index = (dict_index_t *)dict_cache_alloc_memory(table->id, table->mcontext_stack, sizeof(dict_index_t));
    if (index == NULL) {
        goto err_exit;
    }
    memset((char*)index, 0x00, sizeof(dict_index_t));

    index->name = (const char*)dict_cache_alloc_memory(table->id, table->mcontext_stack, index_name_len);
    if (index->name == NULL) {
        goto err_exit;
    }
    memcpy((void*)index->name, index_name, index_name_len);

    index->fields = (dict_field_t*)dict_cache_alloc_memory(table->id,
        table->mcontext_stack, 1 + field_count * sizeof(dict_field_t));
    if (index->fields == NULL) {
        goto err_exit;
    }
    memset((char*)index->fields, 0x00, sizeof(dict_field_t));
    index->table = table;
    index->type = type;
    index->id = index_id;
    index->space_id = space_id;
    index->entry_page_no = FIL_NULL;
    index->field_count = field_count;

    UT_LIST_ADD_LAST(list_node, table->indexes, index);

    return index;

err_exit:

    mcontext_stack_restore(table->mcontext_stack, save_ptr);

    return NULL;
}


static inline uint64 dict_table_memory_size(dict_table_t* table)
{
    return sizeof(memory_stack_context_t) + mcontext_stack_get_size(table->mcontext_stack);
}


dict_index_t* dict_table_get_index_by_index_name(dict_table_t* table, const char* index_name)
{
    if (!index_name) {
        return(NULL);
    }

    //dict_index_t* index = dict_table_get_first_index(table);
    //while (index != NULL) {
    //    if (strcasecmp(index->name, name) == 0) {
    //        return(index);
    //    }
    //    index = dict_table_get_next_index(index);
    //}

    return(NULL);
}



inline bool32 dict_add_table_to_cache(dict_table_t* table, bool32 can_be_evicted)
{
    rw_lock_t *name_hash_lock;
    uint32  fold;

    table->cached = TRUE;
    table->can_be_evicted = can_be_evicted;

    fold = ut_fold_string(table->name);
    //id_fold = ut_fold_uint64(table->id);
    name_hash_lock = hash_get_lock(dict_sys->table_hash, fold);
    //id_hash_lock = hash_get_lock(dict_sys->table_id_hash, id_fold);

    rw_lock_x_lock(name_hash_lock);
    //rw_lock_x_lock(id_hash_lock);

    // Look for a table with the same name: error if such exists
    {
        dict_table_t* table2;
        HASH_SEARCH(name_hash, dict_sys->table_hash, fold,
            dict_table_t*, table2, ut_ad(table2->cached),
            table2->user_id == table->user_id && strcmp(table2->name, table->name) == 0);
        if (table2) {
            goto err_exit;
        }
    }
    // Look for a table with the same id: error if such exists
    //{
    //    dict_table_t*   table2;
    //    HASH_SEARCH(id_hash, dict_sys->table_id_hash, id_fold,
    //        dict_table_t*, table2, ut_ad(table2->cached),
    //        table2->id == table->id);
    //    if (table2) {
    //        goto err_exit;
    //    }
    //}

    // Add table to hash table of tables
    HASH_INSERT(dict_table_t, name_hash, dict_sys->table_hash, fold, table);
    // Add table to hash table of tables based on table id
    //HASH_INSERT(dict_table_t, id_hash, dict_sys->table_id_hash, id_fold, table);

    rw_lock_x_unlock(name_hash_lock);
    //rw_lock_x_unlock(id_hash_lock);

    // add memory
    uint64 size = dict_table_memory_size(table);
    atomic64_add(&dict_sys->memory_cache_size, size);

    // lru
    if (table->can_be_evicted) {
        uint32 lru_list_id = DICT_TABLE_GET_LRU_LIST_ID(table);
        mutex_enter(&dict_sys->table_LRU_list_mutex[lru_list_id]);
        UT_LIST_ADD_FIRST(table_LRU, dict_sys->table_LRU_list[lru_list_id], table);
        table->in_lru_list = TRUE;
        mutex_exit(&dict_sys->table_LRU_list_mutex[lru_list_id]);
    }

    return TRUE;

err_exit:

    rw_lock_x_unlock(name_hash_lock);
    //rw_lock_x_unlock(id_hash_lock);

    return FALSE;
}

inline uint32 dict_get_table_from_cache_by_name(uint64 user_id, char* table_name, dict_table_t** table)
{
    dict_table_t* find_table;
    rw_lock_t* hash_lock;
    uint32 fold;

    fold = ut_fold_string(table_name);
    hash_lock = hash_get_lock(dict_sys->table_hash, fold);

    rw_lock_s_lock(hash_lock);

    // Look for a table with the same name
    HASH_SEARCH(name_hash, dict_sys->table_hash, fold,
        dict_table_t*, find_table, ut_ad(find_table->cached),
        (find_table->user_id == user_id && !find_table->to_be_cache_removed &&
         strcmp(find_table->name, table_name) == 0));
    if (find_table) {
        atomic32_inc(&find_table->ref_count);
    }

    rw_lock_s_unlock(hash_lock);

    if (find_table == NULL) {
        *table = NULL;
        return DICT_TABLE_NOT_FOUND;
    }

    *table = find_table;
    if (!find_table->can_be_evicted) {
        return find_table->status;
    }

    // lru
    date_t now = g_timer()->now_us;
    if (now >= find_table->access_time + DICT_LRU_INTERVAL_WINDOW_US) {
        find_table->touch_number++;
        find_table->access_time = now;
    }

    return find_table->status;
}

inline void dict_release_table(dict_table_t* table)
{
    ut_ad(table->ref_count > 0);
    atomic32_dec(&table->ref_count);
}

inline bool32 dict_remove_table_from_cache(uint64 user_id, char* table_name)
{
    dict_table_t* table;
    rw_lock_t* hash_lock;
    uint32 fold = ut_fold_string(table_name);

    hash_lock = hash_get_lock(dict_sys->table_hash, fold);

retry:

    rw_lock_x_lock(hash_lock);

    // Look for a table with the same name
    HASH_SEARCH(name_hash, dict_sys->table_hash, fold,
        dict_table_t*, table, ut_ad(table->cached),
        (table->user_id == user_id && strcmp(table->name, table_name) == 0));
    if (table == NULL) {
        rw_lock_x_unlock(hash_lock);
        return TRUE;
    }

    if (table->to_be_cache_removed) {
        rw_lock_x_unlock(hash_lock);
        os_thread_sleep(100);
        goto retry;
    }

    table->to_be_cache_removed = TRUE;
    atomic32_inc(&table->ref_count);

    rw_lock_x_unlock(hash_lock);

    // wait for free table
    while (table->ref_count != 1) {
        os_thread_sleep(100);
    }

    //
    rw_lock_x_lock(hash_lock);
    HASH_DELETE(dict_table_t, name_hash, dict_sys->table_hash,
        ut_fold_string(table->name), table);
    rw_lock_x_unlock(hash_lock);

    //fold = ut_fold_uint64(table->id);
    //hash_lock = hash_get_lock(dict_sys->table_id_hash, fold);
    //rw_lock_s_lock(hash_lock);
    //HASH_DELETE(dict_table_t, id_hash, dict_sys->table_id_hash,
    //    ut_fold_uint64(table->id), table);
    //rw_lock_s_unlock(hash_lock);

    // Remove the foreign constraints from the cache
    //for (dict_foreign_t* foreign = UT_LIST_GET_LAST(table->foreign_list);
    //     foreign != NULL;
    //     foreign = UT_LIST_GET_LAST(table->foreign_list)) {
    //    dict_foreign_remove_from_cache(foreign);
    //}

    // Reset table field in referencing constraints
    //for (dict_foreign_t* foreign = UT_LIST_GET_FIRST(table->referenced_list);
    //     foreign != NULL;
    //     foreign = UT_LIST_GET_NEXT(referenced_list, foreign)) {
    //    foreign->referenced_table = NULL;
    //    foreign->referenced_index = NULL;
    //}

    // Remove the indexes from the cache
    for (dict_index_t* index = UT_LIST_GET_LAST(table->indexes);
         index != NULL;
         index = UT_LIST_GET_LAST(table->indexes)) {
        //dict_index_remove_from_cache_low(table, index, lru_evict);
    }

    // Remove table from LRU or non-LRU list
    if (table->can_be_evicted) {
        uint32 lru_list_id = DICT_TABLE_GET_LRU_LIST_ID(table);
        mutex_enter(&dict_sys->table_LRU_list_mutex[lru_list_id]);
        UT_LIST_REMOVE(table_LRU, dict_sys->table_LRU_list[lru_list_id], table);
        table->in_lru_list = FALSE;
        mutex_exit(&dict_sys->table_LRU_list_mutex[lru_list_id]);
    }

    uint64 size = dict_table_memory_size(table);
    ut_ad(dict_sys->memory_cache_size >= (int64)size);

    //
    dict_mem_table_destroy(table);

    atomic64_add(&dict_sys->memory_cache_size, -1 * size);

    return TRUE;
}



// Loads a table definition from a SYS_TABLES record to dict_table_t.
// Does not load any columns or indexes.
static char* dict_load_create_mem_table(const char* table_name, rec_t* rec, dict_table_t** table)
{
    status_t err;
    uint16 size, col_count, offsets[DICT_NUM_COLS_SYS_TABLES], lens[DICT_NUM_COLS_SYS_TABLES];

    err = rec_get_columns_offset(rec, &size, &col_count, offsets, lens);
    if (err != CM_SUCCESS) {
        return "invalid record in SYS_TABLES";
    }

    if (col_count != DICT_NUM_COLS_SYS_TABLES) {
        return("wrong number of columns in SYS_TABLES record");
    }

    ut_a(lens[DICT_COL_SYS_TABLES_USER_ID] == 8);
    uint64 user_id = mach_read_from_8(rec + offsets[DICT_COL_SYS_TABLES_USER_ID]);

    ut_a(lens[DICT_COL_SYS_TABLES_ID] == 8);
    uint64 table_id = mach_read_from_8(rec + offsets[DICT_COL_SYS_TABLES_ID]);

    ut_a(lens[DICT_COL_SYS_TABLES_COLS] == 2);
    uint16 column_count = mach_read_from_2(rec + offsets[DICT_COL_SYS_TABLES_COLS]);

    ut_a(lens[DICT_COL_SYS_TABLES_SPACE] == 4);
    uint32 space_id = mach_read_from_4(rec + offsets[DICT_COL_SYS_TABLES_SPACE]);

    ut_a(lens[DICT_COL_SYS_TABLES_ENTRY] == 4);
    uint32 entry_page_no = mach_read_from_4(rec + offsets[DICT_COL_SYS_TABLES_ENTRY]);

    *table = dict_mem_table_create(table_name, table_id, user_id, space_id, column_count);
    if (*table == NULL) {
        return("cannot alloc memory for table");
    }

    (*table)->entry_page_no = entry_page_no;

    return NULL;

err_len:

    return("incorrect column length in SYS_TABLES");
}

static dict_table_t* dict_load_table(que_sess_t* sess, uint64 user_id, char* table_name, bool32 cached)
{
    status_t  err;
    dict_table_t* table;
    dict_index_t* clust_index = UT_LIST_GET_FIRST(dict_sys->sys_tables->indexes);
    scan_cursor_t scan(dict_sys->sys_tables->mcontext_stack);
    knl_handler handler;
    byte* rec_buf;
    scan_key_t keys[2];
    uint32 keycount = 0;

    scan_key_init(&keys[keycount++], DICT_COL_SYS_TABLES_ID, BTREE_FETCH_STRATEGY_EQUAL, UInt64GetDatum(user_id));
    scan_key_init(&keys[keycount++], DICT_COL_SYS_TABLES_NAME, BTREE_FETCH_STRATEGY_EQUAL, StringGetDatum(table_name));
    scan_cursor_begin(&scan, clust_index, keys, keycount);

    CM_SAVE_STACK(&sess->stack);

    rec_buf = (byte *)cm_stack_push(&sess->stack, ROW_RECORD_MAX_SIZE);
    handler.index_fetch(&scan, rec_buf, FETCH_ROW_KEY_EXACT);

    const char* err_desc = dict_load_create_mem_table(table_name, rec_buf, &table);
    if (err_desc) {
        return NULL;
    }
    table->user_id = user_id;

    scan_cursor_end(&scan);

    //err = dict_load_columns(table, heap, index_load_err);

    //dict_table_add_to_cache(table, TRUE, heap);

    //err = dict_load_indexes(table, heap, index_load_err);

    CM_RESTORE_STACK(&sess->stack);

    return table;
}








void heap_fetch()
{
    dict_table_t* table;
    tuple_slot_t *slot;
    memory_context_t mem_ctx;
    snapshot_t* snapshot;
    scan_cursor_t* scan;
    row_fetch_direction_t direction;

    scan = heap_begin_scan(mem_ctx, table, snapshot, 0, NULL);
    slot = create_tuple_table_slot(mem_ctx, table);

    while (heap_get_next_slot(scan, direction, slot)) {
        /* Deconstruct the tuple ... */
        heap_slot_get_attrs(slot);
        
    }

    heap_end_scan(scan);
    destroy_tuple_table_slot(slot);

}




/*
 * slot_getattr - fetch one attribute of the slot's contents.
 */
static inline Datum slot_get_attr(tuple_slot_t* slot, int col_ind, bool32* isnull)
{
    *isnull = slot->tts_isnull[col_ind - 1];
    return slot->values[col_ind - 1];
}


/* ----------------
 *		FormIndexDatum
 *			Construct values[] and isnull[] arrays for a new index tuple.
 */
void FormIndexDatum(dict_index_t* index, tuple_slot_t *slot, Datum *values, bool32 *isnull)
{
    for (uint32 i = 0; i < index->field_count; i++)
    {
        dict_field_t* field = index->fields[i];
        values[i] = slot_getattr(slot, field->col_ind, &isnull[i]);
    }
}


void index_fetch()
{
    IndexScanDesc index_scan;
	ScanKeyData scankeys[INDEX_MAX_KEYS];

	index_scan = index_beginscan(heap, index, &DirtySnapshot, indnkeyatts, 0);
	index_rescan(index_scan, scankeys, indnkeyatts, NULL, 0);
    TupleTableSlot* slot = table_slot_create(heap, NULL);

	while (index_getnext_slot(index_scan, ForwardScanDirection, slot))
	{
	}
	index_endscan(index_scan);
	ExecDropSingleTupleTableSlot(existing_slot);

}

inline status_t dict_get_table(que_sess_t* sess, uint64 user_id, char* table_name, dict_table_t** table)
{
    uint32 status;

    status = dict_get_table_from_cache_by_name(user_id, table_name, table);
    if (status & DICT_TABLE_TO_BE_DROPED) {
        ut_ad(*table);
        dict_release_table(*table);
        *table = NULL;
        CM_SET_ERROR(ERR_TABLE_OR_VIEW_NOT_FOUND, table_name);
        return CM_ERROR;
    }
    if (*table) {
        return CM_SUCCESS;
    }

    // table is not found from cache, now load it from sys_table

    *table = dict_load_table(sess, user_id, table_name, TRUE);
    if (*table == NULL) {
        return CM_ERROR;
    }
    atomic32_inc(&(*table)->ref_count);

    //
    if (!dict_add_table_to_cache(*table, TRUE)) {
        dict_mem_table_destroy(*table);
    }

    return CM_SUCCESS;
}

status_t dict_drop_table(que_sess_t* sess, uint64 user_id, char* table_name)
{
    status_t err;
    dict_table_t* table;

retry:

    table = NULL;
    err = dict_get_table(sess, user_id, table_name, &table);
    if (table == NULL) {
        if (err == ERR_TABLE_OR_VIEW_NOT_FOUND) {
            return CM_SUCCESS;
        }
        return err;
    }

    mutex_enter(&table->mutex);
    if (table->to_be_dropped) {
        mutex_exit(&table->mutex);
        os_thread_sleep(100);
        goto retry;
    }
    table->to_be_dropped = TRUE;
    mutex_exit(&table->mutex);

    // delete from sys_tables
    mutex_enter(&table->mutex);
    if (table->heap_io_in_progress) {
        mutex_exit(&table->mutex);
        os_thread_sleep(100);
        goto retry;
    }
    table->heap_io_in_progress = TRUE;
    mutex_exit(&table->mutex);

    

    //
    atomic32_dec(&table->ref_count);

    // remove from cache
    dict_remove_table_from_cache(user_id, table_name);

    return CM_SUCCESS;
}





