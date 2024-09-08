#include "knl_dict.h"
#include "cm_memory.h"
#include "cm_log.h"
#include "cm_list.h"
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
static bool32 dict_hdr_create(mtr_t* mtr)
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

    // Create the B-tree roots for the clustered indexes of the basic system tables
    //uint32 root_page_no = btr_create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE,
    //    page_size, DICT_TABLES_ID, dict_ind_redundant, NULL, mtr);
    uint32 root_page_no = heap_create_entry(SRV_SYSTEM_SPACE_ID);
    if (root_page_no == FIL_NULL) {
        return(FALSE);
    }
    mlog_write_uint32(dict_header + DICT_HDR_TABLES, root_page_no, MLOG_4BYTES, mtr);

    root_page_no = btr_create(DICT_UNIQUE, DICT_HDR_SPACE, page_size, DICT_TABLE_IDS_ID, dict_ind_redundant, NULL, mtr);
    if (root_page_no == FIL_NULL) {
        return(FALSE);
    }
    mlog_write_uint32(dict_header + DICT_HDR_TABLE_IDS, root_page_no, MLOG_4BYTES, mtr);

    root_page_no = btr_create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE, page_size, DICT_COLUMNS_ID, dict_ind_redundant, NULL, mtr);
    if (root_page_no == FIL_NULL) {
    	return(FALSE);
    }
    mlog_write_uint32(dict_header + DICT_HDR_COLUMNS, root_page_no, MLOG_4BYTES, mtr);

    root_page_no = btr_create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE, page_size, DICT_INDEXES_ID, dict_ind_redundant, NULL, mtr);
    if (root_page_no == FIL_NULL) {
        return(FALSE);
    }
    mlog_write_uint32(dict_header + DICT_HDR_INDEXES, root_page_no, MLOG_4BYTES, mtr);

    root_page_no = btr_create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE, page_size, DICT_FIELDS_ID, dict_ind_redundant, NULL, mtr);
    if (root_page_no == FIL_NULL) {
        return(FALSE);
    }
    mlog_write_uint32(dict_header + DICT_HDR_FIELDS, root_page_no, MLOG_4BYTES, mtr);

    return(TRUE);
}

static void dict_init(memory_pool_t* mem_pool, uint32 table_hash_array_size)
{
    dict_sys = (dict_sys_t*)malloc(sizeof(dict_sys_t));

    mutex_create(&dict_sys->mutex);

    dict_sys->table_hash = HASH_TABLE_CREATE(table_hash_array_size);
    dict_sys->table_id_hash = HASH_TABLE_CREATE(table_hash_array_size);
    dict_sys->mem_pool = mem_pool;

    UT_LIST_INIT(dict_sys->table_LRU);
    UT_LIST_INIT(dict_sys->table_non_LRU);
}


// Initializes the data dictionary memory structures when the database is started.
// This function is also called when the data dictionary is created.
status_t dict_boot(memory_pool_t* mem_pool, uint32 table_hash_array_size)
{
    dict_table_t*   table;
    dict_index_t*   index;
    dict_hdr_t*     dict_hdr;
    mtr_t           mtr;
    status_t        error;

    mtr_start(&mtr);

    // Create the hash tables etc.
    dict_init(mem_pool, table_hash_array_size);

    mutex_enter(&(dict_sys->mutex));

    // Get the dictionary header
    dict_hdr = dict_hdr_get(&mtr);

    // 1.
    dict_sys->row_id = mach_read_from_8(dict_hdr + DICT_HDR_ROW_ID);

    // 2. SYS_TABLES
    table = dict_mem_table_create("SYS_TABLES", DICT_HDR_SPACE, 8, 0, 0);
    dict_mem_table_add_col(table, "NAME", DATA_BINARY, 0, 0);
    dict_mem_table_add_col(table, "ID", DATA_BINARY, 0, 0);
    // ROW_FORMAT = (N_COLS >> 31) ? COMPACT : REDUNDANT
    dict_mem_table_add_col(table, "N_COLS", DATA_INTEGER, 0, 4);
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

    table->id = DICT_TABLES_ID;

    dict_table_add_to_cache(table, FALSE);
    dict_sys->sys_tables = table;

    index = dict_mem_index_create(table, "CLUST_IND", DICT_HDR_SPACE, DICT_UNIQUE | DICT_CLUSTERED, 1);
    dict_mem_index_add_field(index, "NAME", 0);
    index->id = DICT_TABLES_ID;

    //error = dict_index_add_to_cache(table, index,
    //    mtr_read_uint32(dict_hdr + DICT_HDR_TABLES, MLOG_4BYTES, &mtr),
    //    FALSE);
    //ut_a(error == CM_SUCCESS);


    //
    index = dict_mem_index_create(table, "ID_IND", DICT_HDR_SPACE, DICT_UNIQUE, 1);
    dict_mem_index_add_field(index, "ID", 0);
    index->id = DICT_TABLE_IDS_ID;

    // 3. SYS_COLUMNS
    table = dict_mem_table_create("SYS_COLUMNS", DICT_HDR_SPACE, 7, 0, 0);
    dict_mem_table_add_col(table, "TABLE_ID", DATA_BINARY, 0, 0);
    dict_mem_table_add_col(table, "POS", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "NAME", DATA_BINARY, 0, 0);
    dict_mem_table_add_col(table, "MTYPE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "PRTYPE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "LEN", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "PREC", DATA_INTEGER, 0, 4);

    table->id = DICT_COLUMNS_ID;

    dict_table_add_to_cache(table, FALSE);
    dict_sys->sys_columns = table;

    index = dict_mem_index_create(table, "CLUST_IND", DICT_HDR_SPACE, DICT_UNIQUE | DICT_CLUSTERED, 2);
    dict_mem_index_add_field(index, "TABLE_ID", 0);
    dict_mem_index_add_field(index, "POS", 0);
    index->id = DICT_COLUMNS_ID;

    // 4. SYS_INDEXES
    table = dict_mem_table_create("SYS_INDEXES", DICT_HDR_SPACE, 7, 0, 0);
    dict_mem_table_add_col(table, "TABLE_ID", DATA_BINARY, 0, 0);
    dict_mem_table_add_col(table, "ID", DATA_BINARY, 0, 0);
    dict_mem_table_add_col(table, "NAME", DATA_BINARY, 0, 0);
    dict_mem_table_add_col(table, "N_FIELDS", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "TYPE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "SPACE", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "PAGE_NO", DATA_INTEGER, 0, 4);

    table->id = DICT_INDEXES_ID;

    dict_table_add_to_cache(table, FALSE);
    dict_sys->sys_indexes = table;

    index = dict_mem_index_create(table, "CLUST_IND", DICT_HDR_SPACE, DICT_UNIQUE | DICT_CLUSTERED, 2);
    dict_mem_index_add_field(index, "TABLE_ID", 0);
    dict_mem_index_add_field(index, "ID", 0);
    index->id = DICT_INDEXES_ID;

    // 5. SYS_FIELDS
    table = dict_mem_table_create("SYS_FIELDS", DICT_HDR_SPACE, 3, 0, 0);
    dict_mem_table_add_col(table, "INDEX_ID", DATA_BINARY, 0, 0);
    dict_mem_table_add_col(table, "POS", DATA_INTEGER, 0, 4);
    dict_mem_table_add_col(table, "COL_NAME", DATA_BINARY, 0, 0);

    table->id = DICT_FIELDS_ID;

    dict_table_add_to_cache(table, FALSE);
    dict_sys->sys_fields = table;

    index = dict_mem_index_create(table, "CLUST_IND", DICT_HDR_SPACE, DICT_UNIQUE | DICT_CLUSTERED, 2);
    dict_mem_index_add_field(index, "INDEX_ID", 0);
    dict_mem_index_add_field(index, "POS", 0);

    index->id = DICT_FIELDS_ID;

    mtr_commit(&mtr);

    //
    //dict_load_sys_table(dict_sys->sys_tables);
    //dict_load_sys_table(dict_sys->sys_columns);
    //dict_load_sys_table(dict_sys->sys_indexes);
    //dict_load_sys_table(dict_sys->sys_fields);

    return CM_SUCCESS;
}

// Creates and initializes the data dictionary at the server bootstrap.
status_t dict_create(memory_pool_t* mem_pool, uint32 table_hash_array_size)
{
    mtr_t mtr;

    LOGGER_INFO(LOGGER, "create dictionary");

    mtr_start(&mtr);
    dict_hdr_create(&mtr);
    mtr_commit(&mtr);

    status_t err = dict_boot(mem_pool, table_hash_array_size);

    return(err);
}

void dict_cache_lru_remove_tables()
{

}

memory_context_t* dict_cache_alloc_mem_context()
{
    memory_context_t* mem_ctx;
    const uint32      max_loop_count = 10;
    uint32            loop = 0;

retry:

    mem_ctx = mcontext_create(dict_sys->mem_pool);
    if (mem_ctx == NULL && loop < max_loop_count) {
        dict_cache_lru_remove_tables();
        loop++;
        goto retry;
    }

    return mem_ctx;
}

void* dict_cache_alloc_memory(memory_context_t* mem_ctx, uint32 size)
{
    void*        mem_ptr;
    const uint32 max_loop_count = 10;
    uint32       loop = 0;

    ut_ad(mem_ctx);
    ut_ad(size > 0);

retry:

    mem_ptr = mem_alloc(mem_ctx, size);
    if (mem_ptr == NULL && loop < max_loop_count) {
        dict_cache_lru_remove_tables();
        loop++;
        goto retry;
    }

    return mem_ptr;
}

void dict_cache_free_memory(void* ptr)
{
    mem_free(ptr);
}

void dict_mem_table_destroy(dict_table_t* table)
{
    if (table->to_be_dropped) {

    }

    mutex_enter(&table->mutex);
    if (table->ref_count != 0) {
        mutex_exit(&table->mutex);
        return;
    }
    mutex_exit(&table->mutex);

    mutex_destroy(&table->mutex);
    mcontext_destroy(table->mem_ctx);
}

dict_table_t* dict_mem_table_create(
    const char* name,
    uint32      space_id,
    uint32      column_count,
    uint32      flags,
    uint32      flags2)
{
    dict_table_t*     table;
    memory_context_t* mem_ctx;
    uint32            loop = 0;
    
    mem_ctx = dict_cache_alloc_mem_context();
    if (mem_ctx == NULL) {
        return NULL;
    }
    table = (dict_table_t *)dict_cache_alloc_memory(mem_ctx, sizeof(dict_table_t));
    if (table == NULL) {
        goto err_exit;
    }
    memset((char *)table, 0x00, sizeof(dict_table_t));

    table->mem_ctx = mem_ctx;
    table->ref_count = 0;
    table->space_id = (unsigned int)space_id;
    table->column_count = column_count;
    mutex_create(&table->mutex);
    UT_LIST_INIT(table->indexes);
    
    //table->flags = (unsigned int) flags;
    //table->flags2 = (unsigned int) flags2;
    table->name = (char *)dict_cache_alloc_memory(table->mem_ctx, strlen(name) + 1);
    if (table->name == NULL) {
        goto err_exit;
    }
    memcpy(table->name, name, strlen(name) + 1);

    table->cols = (dict_col_t*)dict_cache_alloc_memory(table->mem_ctx, column_count * sizeof(dict_col_t));
    if (table->cols == NULL) {
        goto err_exit;
    }

    return table;

err_exit:

    mcontext_destroy(mem_ctx);

    return NULL;
}

void dict_mem_table_add_col(
    dict_table_t*   table,
    const char*     name,
    data_type_t     mtype,  // in: main datatype
    uint32          precision, // in: precise type
    uint32          scale_or_len)
{
    dict_col_t* col;
    uint32      col_pos;

    //ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

    col_pos = table->col_count;
    table->col_count++;

    col = dict_table_get_nth_col(table, col_pos);
    col->ind = col_pos;
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
}


// Adds system columns to a table object. */
static void dict_table_add_system_columns(dict_table_t* table)
{
    ut_ad(table);
    //ut_ad(table->n_def == table->n_cols - DATA_N_SYS_COLS);
    //ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);
    ut_ad(!table->cached);

    /* NOTE: the system columns MUST be added in the following order
    (so that they can be indexed by the numerical value of DATA_ROW_ID,
    etc.) and as the last columns of the table memory object.
    The clustered index will not always physically contain all
    system columns. */

    //dict_mem_table_add_col(table, "DB_ROW_ID", DATA_SYS,
    //   DATA_ROW_ID | DATA_NOT_NULL, DATA_ROW_ID_LEN);
    //dict_mem_table_add_col(table, "DB_TRX_ID", DATA_SYS,
    //   DATA_TRX_ID | DATA_NOT_NULL, DATA_TRX_ID_LEN);
    //dict_mem_table_add_col(table, "DB_ROLL_PTR", DATA_SYS,
    //   DATA_ROLL_PTR | DATA_NOT_NULL, DATA_ROLL_PTR_LEN);
}


void dict_table_add_to_cache(
    dict_table_t*   table,
    bool32          can_be_evicted) /*!< in: TRUE if can be evicted */
{
    uint32  fold;
    uint32  id_fold;

    ut_ad(mutex_own(&(dict_sys->mutex)));

    dict_table_add_system_columns(table);

    table->cached = TRUE;
    table->can_be_evicted = can_be_evicted;

    fold = ut_fold_binary((const byte*)table->name, strlen(table->name));
    id_fold = ut_fold_uint64(table->id);

    // Look for a table with the same name: error if such exists
    {
        dict_table_t*   table2;
        HASH_SEARCH(name_hash, dict_sys->table_hash, fold,
            dict_table_t*, table2, ut_ad(table2->cached),
            strcmp(table2->name, table->name) == 0);
        ut_a(table2 == NULL);
    }

    // Look for a table with the same id: error if such exists
    {
        dict_table_t*   table2;
        HASH_SEARCH(id_hash, dict_sys->table_id_hash, id_fold,
            dict_table_t*, table2, ut_ad(table2->cached),
            table2->id == table->id);
        ut_a(table2 == NULL);
    }

    /* Add table to hash table of tables */
    HASH_INSERT(dict_table_t, name_hash, dict_sys->table_hash, fold, table);

    /* Add table to hash table of tables based on table id */
    HASH_INSERT(dict_table_t, id_hash, dict_sys->table_id_hash, id_fold, table);

    if (table->can_be_evicted) {
        UT_LIST_ADD_FIRST(table_LRU, dict_sys->table_LRU, table);
    } else {
        UT_LIST_ADD_FIRST(table_LRU, dict_sys->table_non_LRU, table);
    }

    //dict_sys->size += memory_context_get_size(table->mem_ctx);
}



// Adds a field definition to an index
void dict_mem_index_add_field(
    dict_index_t* index,
    const char*   name,
    uint32        prefix_len)
{
    dict_field_t* field;

    index->n_def++;

    field = dict_index_get_nth_field(index, index->n_def - 1);
    field->name = name;
    field->prefix_len = (unsigned int) prefix_len;
}

// Creates an index memory object
dict_index_t* dict_mem_index_create(
    dict_table_t*   table,
    const char*     index_name,
    uint32          space_id,
    uint32          type,       /*!< in: DICT_UNIQUE, DICT_CLUSTERED, ... ORed */
    uint32          n_fields)   /*!< in: number of fields */
{
    dict_index_t*   index;

    index = (dict_index_t *)dict_cache_alloc_memory(table->mem_ctx, sizeof(dict_index_t));
    if (index == NULL) {
        return NULL;
    }
    memset((char*)index, 0x00, sizeof(dict_index_t));

    index->name = (const char*)dict_cache_alloc_memory(table->mem_ctx, strlen(index_name) + 1);
    if (index->name == NULL) {
        goto err_exit;
    }
    memcpy((void*)index->name, index_name, strlen(index_name) + 1);

    index->fields = (dict_field_t*)dict_cache_alloc_memory(table->mem_ctx, 1 + n_fields * sizeof(dict_field_t));
    if (index->fields == NULL) {
        goto err_exit;
    }

    index->type = type;
    index->space = space_id;
    index->page = FIL_NULL;
    index->n_fields = n_fields;

    UT_LIST_ADD_LAST(indexes, table->indexes, index);

    return index;

err_exit:

    if (index->fields) {
        dict_cache_free_memory((void*)index->fields);
    }
    if (index->name) {
        dict_cache_free_memory((void*)index->name);
    }
    dict_cache_free_memory((void*)index);

    return NULL;
}

