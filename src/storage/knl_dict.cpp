#include "knl_dict.h"
#include "knl_buf.h"



// Creates the file page for the dictionary header. This function is called only at the database creation.
static bool32 dict_hdr_create(mtr_t* mtr)
{
    /*
	buf_block_t* block;
	dict_hdr_t*  dict_header;
	uint32       root_page_no;

	ut_ad(mtr);

	// Create the dictionary header file block in a new, allocated file segment in the system tablespace
	block = fseg_create(DICT_HDR_SPACE, 0, DICT_HDR + DICT_HDR_FSEG_HEADER, mtr);

	ut_a(DICT_HDR_PAGE_NO == buf_block_get_page_no(block));

	dict_header = dict_hdr_get(mtr);

	/* Start counting row, table, index, and tree ids from DICT_HDR_FIRST_ID
	mlog_write_uint64(dict_header + DICT_HDR_ROW_ID, DICT_HDR_FIRST_ID, mtr);
	mlog_write_uint64(dict_header + DICT_HDR_TABLE_ID, DICT_HDR_FIRST_ID, mtr);
	mlog_write_uint64(dict_header + DICT_HDR_INDEX_ID, DICT_HDR_FIRST_ID, mtr);
	mlog_write_uint32(dict_header + DICT_HDR_MAX_SPACE_ID, 0, MLOG_4BYTES, mtr);
	// Obsolete, but we must initialize it anyway.
	mlog_write_uint32(dict_header + DICT_HDR_MIX_ID_LOW, DICT_HDR_FIRST_ID, MLOG_4BYTES, mtr);

	// Create the B-tree roots for the clustered indexes of the basic system tables
	root_page_no = btr_create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE, 0, DICT_TABLES_ID, dict_ind_redundant, mtr);
	if (root_page_no == FIL_NULL) {
		return(FALSE);
	}
	mlog_write_uint32(dict_header + DICT_HDR_TABLES, root_page_no, MLOG_4BYTES, mtr);

	root_page_no = btr_create(DICT_UNIQUE, DICT_HDR_SPACE, 0, DICT_TABLE_IDS_ID, dict_ind_redundant, mtr);
	if (root_page_no == FIL_NULL) {
		return(FALSE);
	}
	mlog_write_uint32(dict_header + DICT_HDR_TABLE_IDS, root_page_no, MLOG_4BYTES, mtr);

	root_page_no = btr_create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE, 0, DICT_COLUMNS_ID, dict_ind_redundant, mtr);
	if (root_page_no == FIL_NULL) {
		return(FALSE);
	}
	mlog_write_uint32(dict_header + DICT_HDR_COLUMNS, root_page_no, MLOG_4BYTES, mtr);

	root_page_no = btr_create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE, 0, DICT_INDEXES_ID, dict_ind_redundant, mtr);
	if (root_page_no == FIL_NULL) {
		return(FALSE);
	}
	mlog_write_uint32(dict_header + DICT_HDR_INDEXES, root_page_no, MLOG_4BYTES, mtr);

	root_page_no = btr_create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE, 0, DICT_FIELDS_ID, dict_ind_redundant, mtr);
	if (root_page_no == FIL_NULL) {
		return(FALSE);
	}
	mlog_write_uint32(dict_header + DICT_HDR_FIELDS, root_page_no, MLOG_4BYTES, mtr);
    */
	return(TRUE);
}

// Initializes the data dictionary memory structures when the database is started.
// This function is also called when the data dictionary is created.
dberr_t dict_boot(void)
{
    /*
	dict_table_t*	table;
	dict_index_t*	index;
	dict_hdr_t*	dict_hdr;
	mem_heap_t*	heap;
	mtr_t		mtr;
	dberr_t		error;

	// Be sure these constants do not ever change.  To avoid bloat,
	// only check the *NUM_FIELDS* in each table

	ut_ad(DICT_NUM_COLS__SYS_TABLES == 8);
	ut_ad(DICT_NUM_FIELDS__SYS_TABLES == 10);
	ut_ad(DICT_NUM_FIELDS__SYS_TABLE_IDS == 2);
	ut_ad(DICT_NUM_COLS__SYS_COLUMNS == 7);
	ut_ad(DICT_NUM_FIELDS__SYS_COLUMNS == 9);
	ut_ad(DICT_NUM_COLS__SYS_INDEXES == 7);
	ut_ad(DICT_NUM_FIELDS__SYS_INDEXES == 9);
	ut_ad(DICT_NUM_COLS__SYS_FIELDS == 3);
	ut_ad(DICT_NUM_FIELDS__SYS_FIELDS == 5);
	ut_ad(DICT_NUM_COLS__SYS_FOREIGN == 4);
	ut_ad(DICT_NUM_FIELDS__SYS_FOREIGN == 6);
	ut_ad(DICT_NUM_FIELDS__SYS_FOREIGN_FOR_NAME == 2);
	ut_ad(DICT_NUM_COLS__SYS_FOREIGN_COLS == 4);
	ut_ad(DICT_NUM_FIELDS__SYS_FOREIGN_COLS == 6);

	mtr_start(&mtr);

	// Create the hash tables etc.
	dict_init();

	heap = mem_heap_create(450);

	mutex_enter(&(dict_sys->mutex));

	// Get the dictionary header
	dict_hdr = dict_hdr_get(&mtr);

	// Because we only write new row ids to disk-based data structure
	//(dictionary header) when it is divisible by
	//DICT_HDR_ROW_ID_WRITE_MARGIN, in recovery we will not recover
	//the latest value of the row id counter. Therefore we advance
	//the counter at the database startup to avoid overlapping values.
	//Note that when a user after database startup first time asks for
	//a new row id, then because the counter is now divisible by
	//..._MARGIN, it will immediately be updated to the disk-based header.

	dict_sys->row_id = DICT_HDR_ROW_ID_WRITE_MARGIN
		+ ut_uint64_align_up(mach_read_from_8(dict_hdr + DICT_HDR_ROW_ID),
				     DICT_HDR_ROW_ID_WRITE_MARGIN);

	// Insert into the dictionary cache the descriptions of the basic system tables
	//
	table = dict_mem_table_create("SYS_TABLES", DICT_HDR_SPACE, 8, 0, 0);

	dict_mem_table_add_col(table, heap, "NAME", DATA_BINARY, 0, 0);
	dict_mem_table_add_col(table, heap, "ID", DATA_BINARY, 0, 0);
	// ROW_FORMAT = (N_COLS >> 31) ? COMPACT : REDUNDANT
	dict_mem_table_add_col(table, heap, "N_COLS", DATA_INT, 0, 4);
	// The low order bit of TYPE is always set to 1.  If the format
	// is UNIV_FORMAT_B or higher, this field matches table->flags.
	dict_mem_table_add_col(table, heap, "TYPE", DATA_INT, 0, 4);
	dict_mem_table_add_col(table, heap, "MIX_ID", DATA_BINARY, 0, 0);
	// MIX_LEN may contain additional table flags when
	// ROW_FORMAT!=REDUNDANT.  Currently, these flags include
	// DICT_TF2_TEMPORARY. 
	dict_mem_table_add_col(table, heap, "MIX_LEN", DATA_INT, 0, 4);
	dict_mem_table_add_col(table, heap, "CLUSTER_NAME", DATA_BINARY, 0, 0);
	dict_mem_table_add_col(table, heap, "SPACE", DATA_INT, 0, 4);

	table->id = DICT_TABLES_ID;

	dict_table_add_to_cache(table, FALSE, heap);
	dict_sys->sys_tables = table;
	mem_heap_empty(heap);

	index = dict_mem_index_create("SYS_TABLES", "CLUST_IND",
				      DICT_HDR_SPACE,
				      DICT_UNIQUE | DICT_CLUSTERED, 1);

	dict_mem_index_add_field(index, "NAME", 0);

	index->id = DICT_TABLES_ID;

	error = dict_index_add_to_cache(table, index,
					mtr_read_ulint(dict_hdr
						       + DICT_HDR_TABLES,
						       MLOG_4BYTES, &mtr),
					FALSE);
	ut_a(error == DB_SUCCESS);

	//
	index = dict_mem_index_create("SYS_TABLES", "ID_IND",
				      DICT_HDR_SPACE, DICT_UNIQUE, 1);
	dict_mem_index_add_field(index, "ID", 0);

	index->id = DICT_TABLE_IDS_ID;
	error = dict_index_add_to_cache(table, index,
					mtr_read_ulint(dict_hdr
						       + DICT_HDR_TABLE_IDS,
						       MLOG_4BYTES, &mtr),
					FALSE);
	ut_a(error == DB_SUCCESS);

	//
	table = dict_mem_table_create("SYS_COLUMNS", DICT_HDR_SPACE, 7, 0, 0);

	dict_mem_table_add_col(table, heap, "TABLE_ID", DATA_BINARY, 0, 0);
	dict_mem_table_add_col(table, heap, "POS", DATA_INT, 0, 4);
	dict_mem_table_add_col(table, heap, "NAME", DATA_BINARY, 0, 0);
	dict_mem_table_add_col(table, heap, "MTYPE", DATA_INT, 0, 4);
	dict_mem_table_add_col(table, heap, "PRTYPE", DATA_INT, 0, 4);
	dict_mem_table_add_col(table, heap, "LEN", DATA_INT, 0, 4);
	dict_mem_table_add_col(table, heap, "PREC", DATA_INT, 0, 4);

	table->id = DICT_COLUMNS_ID;

	dict_table_add_to_cache(table, FALSE, heap);
	dict_sys->sys_columns = table;
	mem_heap_empty(heap);

	index = dict_mem_index_create("SYS_COLUMNS", "CLUST_IND",
				      DICT_HDR_SPACE,
				      DICT_UNIQUE | DICT_CLUSTERED, 2);

	dict_mem_index_add_field(index, "TABLE_ID", 0);
	dict_mem_index_add_field(index, "POS", 0);

	index->id = DICT_COLUMNS_ID;
	error = dict_index_add_to_cache(table, index,
					mtr_read_ulint(dict_hdr
						       + DICT_HDR_COLUMNS,
						       MLOG_4BYTES, &mtr),
					FALSE);
	ut_a(error == DB_SUCCESS);

	//
	table = dict_mem_table_create("SYS_INDEXES", DICT_HDR_SPACE, 7, 0, 0);

	dict_mem_table_add_col(table, heap, "TABLE_ID", DATA_BINARY, 0, 0);
	dict_mem_table_add_col(table, heap, "ID", DATA_BINARY, 0, 0);
	dict_mem_table_add_col(table, heap, "NAME", DATA_BINARY, 0, 0);
	dict_mem_table_add_col(table, heap, "N_FIELDS", DATA_INT, 0, 4);
	dict_mem_table_add_col(table, heap, "TYPE", DATA_INT, 0, 4);
	dict_mem_table_add_col(table, heap, "SPACE", DATA_INT, 0, 4);
	dict_mem_table_add_col(table, heap, "PAGE_NO", DATA_INT, 0, 4);

	table->id = DICT_INDEXES_ID;

	dict_table_add_to_cache(table, FALSE, heap);
	dict_sys->sys_indexes = table;
	mem_heap_empty(heap);

	index = dict_mem_index_create("SYS_INDEXES", "CLUST_IND",
				      DICT_HDR_SPACE,
				      DICT_UNIQUE | DICT_CLUSTERED, 2);

	dict_mem_index_add_field(index, "TABLE_ID", 0);
	dict_mem_index_add_field(index, "ID", 0);

	index->id = DICT_INDEXES_ID;
	error = dict_index_add_to_cache(table, index,
					mtr_read_ulint(dict_hdr
						       + DICT_HDR_INDEXES,
						       MLOG_4BYTES, &mtr),
					FALSE);
	ut_a(error == DB_SUCCESS);

	//
	table = dict_mem_table_create("SYS_FIELDS", DICT_HDR_SPACE, 3, 0, 0);

	dict_mem_table_add_col(table, heap, "INDEX_ID", DATA_BINARY, 0, 0);
	dict_mem_table_add_col(table, heap, "POS", DATA_INT, 0, 4);
	dict_mem_table_add_col(table, heap, "COL_NAME", DATA_BINARY, 0, 0);

	table->id = DICT_FIELDS_ID;

	dict_table_add_to_cache(table, FALSE, heap);
	dict_sys->sys_fields = table;
	mem_heap_free(heap);

	index = dict_mem_index_create("SYS_FIELDS", "CLUST_IND",
				      DICT_HDR_SPACE,
				      DICT_UNIQUE | DICT_CLUSTERED, 2);

	dict_mem_index_add_field(index, "INDEX_ID", 0);
	dict_mem_index_add_field(index, "POS", 0);

	index->id = DICT_FIELDS_ID;
	error = dict_index_add_to_cache(table, index,
					mtr_read_ulint(dict_hdr
						       + DICT_HDR_FIELDS,
						       MLOG_4BYTES, &mtr),
					FALSE);
	ut_a(error == DB_SUCCESS);

	mtr_commit(&mtr);

	//

	// Initialize the insert buffer table and index for each tablespace

	ibuf_init_at_db_start();

	dberr_t	err = DB_SUCCESS;

	if (srv_read_only_mode && !ibuf_is_empty()) {

		ib_logf(IB_LOG_LEVEL_ERROR,
			"Change buffer must be empty when --innodb-read-only "
			"is set!");

		err = DB_ERROR;
	} else {
		// Load definitions of other indexes on system tables

		dict_load_sys_table(dict_sys->sys_tables);
		dict_load_sys_table(dict_sys->sys_columns);
		dict_load_sys_table(dict_sys->sys_indexes);
		dict_load_sys_table(dict_sys->sys_fields);
	}

	mutex_exit(&(dict_sys->mutex));

	return(err);
    */
return DB_ERROR;
}


// Creates and initializes the data dictionary at the server bootstrap.
dberr_t dict_create(void)
{
    mtr_t mtr;

    mtr_start(&mtr);

    dict_hdr_create(&mtr);

    mtr_commit(&mtr);

    dberr_t err = dict_boot();

    return(err);
}


