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
    const page_id_t page_id(DICT_HDR_SPACE, DICT_HDR_PAGE_NO);
    const page_size_t page_size(0);

    block = buf_page_get(page_id, page_size, RW_X_LATCH, mtr);
    header = DICT_HDR + buf_block_get_frame(block);

    //buf_block_dbg_add_level(block, SYNC_DICT_HEADER);

    return header;
}

// Creates the file page for the dictionary header.
// This function is called only at the database creation.
static status_t dict_hdr_create(mtr_t* mtr)
{
    const page_id_t page_id(DICT_HDR_SPACE, TRX_SYS_PAGE_NO);
    const page_size_t page_size(0);
    buf_block_t* block = buf_page_get(page_id, page_size, RW_X_LATCH, mtr);
    ut_a(block->get_page_no() == TRX_SYS_PAGE_NO);

    //buf_block_dbg_add_level(block, SYNC_DICT_HEADER);

    page_t* page = buf_block_get_frame(block);
    mlog_write_uint32(page + FIL_PAGE_TYPE, FIL_PAGE_DICT_HDR, MLOG_2BYTES, mtr);

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
        return CM_ERROR;
    }
    mlog_write_uint32(dict_header + DICT_HDR_TABLES, root_page_no, MLOG_4BYTES, mtr);

    root_page_no = btr_create(DICT_UNIQUE, DICT_HDR_SPACE, page_size, DICT_TABLES_INDEX_ID_ID, dict_ind_redundant, NULL, mtr);
    if (root_page_no == FIL_NULL) {
        return CM_ERROR;
    }
    mlog_write_uint32(dict_header + DICT_HDR_TABLE_IDS, root_page_no, MLOG_4BYTES, mtr);

    // sys_columns
    root_page_no = heap_create_entry(FIL_SYSTEM_SPACE_ID);
    if (root_page_no == FIL_NULL) {
        return CM_ERROR;
    }
    mlog_write_uint32(dict_header + DICT_HDR_COLUMNS, root_page_no, MLOG_4BYTES, mtr);

    root_page_no = btr_create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE, page_size, DICT_COLUMNS_CLUST_ID, dict_ind_redundant, NULL, mtr);
    if (root_page_no == FIL_NULL) {
        return CM_ERROR;
    }
    mlog_write_uint32(dict_header + DICT_HDR_COLUMNS_CLUST, root_page_no, MLOG_4BYTES, mtr);

    // sys_indexes
    root_page_no = heap_create_entry(FIL_SYSTEM_SPACE_ID);
    if (root_page_no == FIL_NULL) {
        return CM_ERROR;
    }
    mlog_write_uint32(dict_header + DICT_HDR_INDEXES, root_page_no, MLOG_4BYTES, mtr);

    root_page_no = btr_create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE, page_size, DICT_INDEXES_CLUST_ID, dict_ind_redundant, NULL, mtr);
    if (root_page_no == FIL_NULL) {
        return CM_ERROR;
    }
    mlog_write_uint32(dict_header + DICT_HDR_INDEXES_CLUST, root_page_no, MLOG_4BYTES, mtr);

    // sys_fields
    root_page_no = heap_create_entry(FIL_SYSTEM_SPACE_ID);
    if (root_page_no == FIL_NULL) {
        return CM_ERROR;
    }
    mlog_write_uint32(dict_header + DICT_HDR_FIELDS, root_page_no, MLOG_4BYTES, mtr);

    root_page_no = btr_create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE, page_size, DICT_FIELDS_CLUST_ID, dict_ind_redundant, NULL, mtr);
    if (root_page_no == FIL_NULL) {
        return CM_ERROR;
    }
    mlog_write_uint32(dict_header + DICT_HDR_FIELDS_CLUST, root_page_no, MLOG_4BYTES, mtr);

    return CM_SUCCESS;
}

status_t dict_init(memory_pool_t* mem_pool, uint64 memory_cache_size, uint32 table_hash_array_size)
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
status_t dict_boot()
{
    dict_table_t*   table;
    dict_index_t*   index;
    dict_hdr_t*     dict_hdr;
    mtr_t           mtr;
    status_t        error;

    mtr_start(&mtr);

    // Get the dictionary header
    dict_hdr = dict_hdr_get(&mtr);

    // 1.
    dict_sys->row_id = mach_read_from_8(dict_hdr + DICT_HDR_ROW_ID);

    // 2. SYS_TABLES
    table = dict_mem_table_create("SYS_TABLES", DICT_TABLES_ID, DICT_HDR_SPACE, 8, 0, 0);
    dict_mem_table_add_col(table, "ID", DATA_BINARY, 0, 0);
    dict_mem_table_add_col(table, "NAME", DATA_BINARY, 0, 0);
    // ROW_FORMAT = (N_COLS >> 31) ? COMPACT : REDUNDANT
    dict_mem_table_add_col(table, "COLUMN_COUNT", DATA_INTEGER, 0, 4);
    // The low order bit of TYPE is always set to 1.  If the format
    // is UNIV_FORMAT_B or higher, this field matches table->flags.
    dict_mem_table_add_col(table, "TYPE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "MIX_ID", DATA_BINARY, 0, 0);
    // MIX_LEN may contain additional table flags when
    // ROW_FORMAT!=REDUNDANT.  Currently, these flags include
    // DICT_TF2_TEMPORARY. 
    dict_mem_table_add_col(table, "MIX_LEN", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "CLUSTER_NAME", DATA_BINARY, 0, 0);
    dict_mem_table_add_col(table, "SPACE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "ENTRY", DATA_INTEGER, 0, 4);

    dict_add_table_to_cache(table, FALSE);
    dict_sys->sys_tables = table;

    index = dict_mem_index_create(table, "CLUST_IND", DICT_TABLES_CLUST_ID, DICT_HDR_SPACE, DICT_UNIQUE | DICT_CLUSTERED, 1);
    dict_mem_index_add_field(index, "NAME", 0);

    //error = dict_index_add_to_cache(table, index,
    //    mtr_read_uint32(dict_hdr + DICT_HDR_TABLES, MLOG_4BYTES, &mtr),
    //    FALSE);
    //ut_a(error == CM_SUCCESS);

    //
    index = dict_mem_index_create(table, "ID_IND", DICT_TABLES_INDEX_ID_ID, DICT_HDR_SPACE, DICT_UNIQUE, 1);
    dict_mem_index_add_field(index, "ID", 0);

    // 3. SYS_COLUMNS
    table = dict_mem_table_create("SYS_COLUMNS", DICT_COLUMNS_ID, DICT_HDR_SPACE, 7, 0, 0);
    dict_mem_table_add_col(table, "TABLE_ID", DATA_BINARY, 0, 0);
    dict_mem_table_add_col(table, "POS", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "NAME", DATA_BINARY, 0, 0);
    dict_mem_table_add_col(table, "MTYPE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "PRTYPE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "LEN", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "PREC", DATA_INTEGER, 0, 4);

    dict_add_table_to_cache(table, FALSE);
    dict_sys->sys_columns = table;

    index = dict_mem_index_create(table, "CLUST_IND", DICT_COLUMNS_CLUST_ID, DICT_HDR_SPACE, DICT_UNIQUE | DICT_CLUSTERED, 2);
    dict_mem_index_add_field(index, "TABLE_ID", 0);
    dict_mem_index_add_field(index, "POS", 0);

    // 4. SYS_INDEXES
    table = dict_mem_table_create("SYS_INDEXES", DICT_INDEXES_ID, DICT_HDR_SPACE, 7, 0, 0);
    dict_mem_table_add_col(table, "TABLE_ID", DATA_BINARY, 0, 0);
    dict_mem_table_add_col(table, "ID", DATA_BINARY, 0, 0);
    dict_mem_table_add_col(table, "NAME", DATA_BINARY, 0, 0);
    dict_mem_table_add_col(table, "N_FIELDS", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "TYPE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "SPACE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "PAGE_NO", DATA_INTEGER, 0, 4);

    dict_add_table_to_cache(table, FALSE);
    dict_sys->sys_indexes = table;

    index = dict_mem_index_create(table, "CLUST_IND", DICT_INDEXES_CLUST_ID, DICT_HDR_SPACE, DICT_UNIQUE | DICT_CLUSTERED, 2);
    dict_mem_index_add_field(index, "TABLE_ID", 0);
    dict_mem_index_add_field(index, "ID", 0);

    // 5. SYS_FIELDS
    table = dict_mem_table_create("SYS_FIELDS", DICT_FIELDS_ID, DICT_HDR_SPACE, 3, 0, 0);
    dict_mem_table_add_col(table, "INDEX_ID", DATA_BINARY, 0, 0);
    dict_mem_table_add_col(table, "POS", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "COL_NAME", DATA_BINARY, 0, 0);

    dict_add_table_to_cache(table, FALSE);
    dict_sys->sys_fields = table;

    index = dict_mem_index_create(table, "CLUST_IND", DICT_FIELDS_CLUST_ID, DICT_HDR_SPACE, DICT_UNIQUE | DICT_CLUSTERED, 2);
    dict_mem_index_add_field(index, "INDEX_ID", 0);
    dict_mem_index_add_field(index, "POS", 0);

    mtr_commit(&mtr);

    return CM_SUCCESS;
}

// Creates and initializes the data dictionary at the server bootstrap.
status_t dict_create()
{
    status_t err;
    mtr_t mtr;

    LOGGER_INFO(LOGGER, LOG_MODULE_DICTIONARY, "create dictionary");

    mtr_start(&mtr);
    err = dict_hdr_create(&mtr);
    mtr_commit(&mtr);

    return err;
}

static inline uint64 dict_cache_clean_memory_size()
{
    uint64 size = dict_sys->memory_cache_max_size * 0.01;
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
        }
        mutex_exit(&dict_sys->table_LRU_list_mutex[lru_list_id]);

        //
        if (dict_remove_table_from_cache(table_name)) {
            skip_table_id = DICT_INVALID_OBJECT_ID;
        }

        loop++;
    }
}

static inline void dict_cache_lru_remove_tables(uint32 table_id)
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

    if (UNLIKELY(mem_ptr)) {
        CM_SET_ERROR(ERR_ALLOC_MEMORY_REACH_LIMIT, "dictionary cache", dict_sys->memory_cache_max_size);
    }

    return mem_ptr;
}

static void dict_cache_free_memory(memory_stack_context_t* mem_stack_ctx, void* ptr, uint32 size)
{
    mcontext_stack_pop(mem_stack_ctx, ptr, size);
}

void dict_mem_table_destroy(dict_table_t* table)
{
    ut_ad(table->to_be_cache_removed);
    ut_ad(table->ref_count == 1);

    mutex_destroy(&table->mutex);
    mcontext_stack_destroy(table->mem_stack_ctx);
}

dict_table_t* dict_mem_table_create(
    const char* name,
    table_id_t  table_id,
    uint32      space_id,
    uint32      column_count,
    uint32      flags,
    uint32      flags2)
{
    dict_table_t* table;
    memory_stack_context_t* mem_stack_ctx;

    if (column_count > DICT_TABLE_COLUMN_MAX_COUNT) {
        return NULL;
    }

    mem_stack_ctx = dict_cache_alloc_mem_stack_context();
    if (mem_stack_ctx == NULL) {
        CM_SET_ERROR(ERR_ALLOC_MEMORY_CONTEXT, "creating table");
        return NULL;
    }
    table = (dict_table_t *)dict_cache_alloc_memory(table_id, mem_stack_ctx, sizeof(dict_table_t));
    if (table == NULL) {
        CM_SET_ERROR(ERR_ALLOC_MEMORY, sizeof(dict_table_t), "creating table");
        goto err_exit;
    }
    memset((char *)table, 0x00, sizeof(dict_table_t));

    table->mem_stack_ctx = mem_stack_ctx;
    table->ref_count = 0;
    table->id = table_id;
    table->space_id = (unsigned int)space_id;
    table->column_count = column_count;
    table->column_index = 0;

    mutex_create(&table->mutex);
    UT_LIST_INIT(table->indexes);

    //table->flags = (unsigned int) flags;
    //table->flags2 = (unsigned int) flags2;

    if (column_count > 0) {
        table->columns = (dict_col_t*)dict_cache_alloc_memory(table_id, table->mem_stack_ctx,
                                                              column_count * sizeof(dict_col_t));
        if (table->columns == NULL) {
            CM_SET_ERROR(ERR_ALLOC_MEMORY, column_count * sizeof(dict_col_t), "creating table");
            goto err_exit;
        }
    }

    table->name = (char *)dict_cache_alloc_memory(table_id, table->mem_stack_ctx, strlen(name) + 1);
    if (table->name == NULL) {
        CM_SET_ERROR(ERR_ALLOC_MEMORY, strlen(name) + 1, "creating table");
        goto err_exit;
    }
    memcpy(table->name, name, strlen(name) + 1);

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
        if (strncasecmp(name, table->columns[i].name, strlen(name)) == 0) {
            CM_SET_ERROR(ERR_COLUMN_EXISTS, name);
            return CM_ERROR;
        }
    }

    dict_col_t* col = dict_table_get_nth_col(table, table->column_index);
    col->ind = table->column_index;
    table->column_index++;

    col->name = (char *)dict_cache_alloc_memory(table->id, table->mem_stack_ctx, strlen(name) + 1);
    if (col->name == NULL) {
        CM_SET_ERROR(ERR_ALLOC_MEMORY, sizeof(dict_table_t), "creating table");
        return CM_ERROR;
    }
    memcpy(col->name, name, strlen(name) + 1);
    col->mtype = mtype;

    switch (mtype) {
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

    return CM_SUCCESS;
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
        if (strncasecmp(name, index->table->columns[i].name, strlen(name)) == 0) {
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

    index = UT_LIST_GET_FIRST(table->indexes);
    while (index) {
        if (strncasecmp(index_name, index->name, strlen(index_name)) == 0) {
            return NULL;
        }
        index = UT_LIST_GET_NEXT(indexes, index);
    }

    void* save_ptr = mcontext_stack_save(table->mem_stack_ctx);

    index = (dict_index_t *)dict_cache_alloc_memory(table->id, table->mem_stack_ctx, sizeof(dict_index_t));
    if (index == NULL) {
        return NULL;
    }
    memset((char*)index, 0x00, sizeof(dict_index_t));

    index->name = (const char*)dict_cache_alloc_memory(table->id, table->mem_stack_ctx, strlen(index_name) + 1);
    if (index->name == NULL) {
        goto err_exit;
    }
    memcpy((void*)index->name, index_name, strlen(index_name) + 1);

    index->fields = (dict_field_t*)dict_cache_alloc_memory(table->id,
        table->mem_stack_ctx, 1 + field_count * sizeof(dict_field_t));
    if (index->fields == NULL) {
        goto err_exit;
    }

    index->table = table;
    index->type = type;
    index->id = index_id;
    index->space_id = space_id;
    index->page_no = FIL_NULL;
    index->field_count = field_count;

    UT_LIST_ADD_LAST(indexes, table->indexes, index);

    return index;

err_exit:

    mcontext_stack_restore(table->mem_stack_ctx, save_ptr);

    return NULL;
}


static inline uint64 dict_table_memory_size(dict_table_t* table)
{
    return sizeof(memory_stack_context_t) + mcontext_stack_get_size(table->mem_stack_ctx);
}

bool32 dict_add_table_to_cache(
    dict_table_t*   table,
    bool32          can_be_evicted) /*!< in: TRUE if can be evicted */
{
    rw_lock_t *name_hash_lock, *id_hash_lock;
    uint32  fold, id_fold;

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
        dict_table_t*   table2;
        HASH_SEARCH(name_hash, dict_sys->table_hash, fold,
            dict_table_t*, table2, ut_ad(table2->cached),
            strcmp(table2->name, table->name) == 0);
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

uint32 dict_get_table_from_cache_by_name(char* table_name, dict_table_t** table)
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
        (strcmp(find_table->name, table_name) == 0 && !find_table->to_be_cache_removed));
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
    date_t now = g_timer()->now;
    if (now >= find_table->access_time + DICT_LRU_INTERVAL_WINDOW_US) {
        find_table->touch_number++;
        find_table->access_time = now;
    }

    return find_table->status;
}

void dict_release_table(dict_table_t* table)
{
    ut_ad(table->ref_count > 0);
    atomic32_dec(&table->ref_count);
}

bool32 dict_remove_table_from_cache(char* table_name)
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
        (strcmp(table->name, table_name) == 0));
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
    ut_ad(dict_sys->memory_cache_size >= size);

    //
    dict_mem_table_destroy(table);

    atomic64_add(&dict_sys->memory_cache_size, -1 * size);

    return TRUE;
}

static dict_table_t* dict_load_table(char* table_name)
{
    return NULL;
}

status_t dict_get_table(char* table_name, dict_table_t** table)
{
    uint32 status;

    status = dict_get_table_from_cache_by_name(table_name, table);
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

    *table = dict_load_table(table_name);
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

status_t dict_drop_table(char* table_name)
{
    status_t err;
    dict_table_t* table;

retry:

    table = NULL;
    err = dict_get_table(table_name, &table);
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
    dict_remove_table_from_cache(table_name);

    return CM_SUCCESS;
}





