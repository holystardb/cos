#ifndef _KNL_DICT_H
#define _KNL_DICT_H

#include "cm_type.h"
#include "cm_error.h"
#include "cm_mutex.h"
#include "knl_hash_table.h"
#include "knl_server.h"
#include "knl_trx.h"

/* Space id and page no where the dictionary header resides */
#define DICT_HDR_SPACE      0   /* the SYSTEM tablespace */
#define DICT_HDR_PAGE_NO    FSP_DICT_HDR_PAGE_NO

/* The ids for the basic system tables and their indexes */
#define DICT_TABLES_ID          1
#define DICT_COLUMNS_ID         2
#define DICT_INDEXES_ID         3
#define DICT_FIELDS_ID          4
/* The following is a secondary index on SYS_TABLES */
#define DICT_TABLE_IDS_ID       5

/* the ids for tables etc.
   start from this number, except for basic system tables and their above defined indexes;
   ibuf tables and indexes are assigned as the id the number DICT_IBUF_ID_MIN plus the space id */
#define DICT_HDR_FIRST_ID       256

/* The offset of the dictionary header on the page */
#define DICT_HDR                FSEG_PAGE_DATA

/*-------------------------------------------------------------*/
/* Dictionary header offsets */
#define DICT_HDR_ROW_ID         0   /* The latest assigned row id */
#define DICT_HDR_TABLE_ID       8   /* The latest assigned table id */
#define DICT_HDR_INDEX_ID       16  /* The latest assigned index id */
#define DICT_HDR_MAX_SPACE_ID   24  /* The latest assigned space id,or 0*/
#define DICT_HDR_MIX_ID_LOW     28  /* Obsolete,always DICT_HDR_FIRST_ID*/
#define DICT_HDR_TABLES         32  /* Root of SYS_TABLES clust index */
#define DICT_HDR_TABLE_IDS      36  /* Root of SYS_TABLE_IDS sec index */
#define DICT_HDR_COLUMNS        40  /* Root of SYS_COLUMNS clust index */
#define DICT_HDR_INDEXES        44  /* Root of SYS_INDEXES clust index */
#define DICT_HDR_FIELDS         48  /* Root of SYS_FIELDS clust index */
#define DICT_HDR_FSEG_HEADER    1024  /* Segment header for the tablespace segment
                                       into which the dictionary header is created */
/*-------------------------------------------------------------*/


/** Type flags of an index:
    OR'ing of the flags is allowed to define a combination of types */
#define DICT_CLUSTERED      1   /*!< clustered index */
#define DICT_UNIQUE         2   /*!< unique index */
#define DICT_UNIVERSAL      4   /*!< index which can contain records from any other index */
#define DICT_IBUF           8   /*!< insert buffer tree */
#define DICT_CORRUPT        16  /*!< bit to store the corrupted flag in SYS_INDEXES.TYPE */
#define DICT_FTS            32  /* FTS index; can't be combined with the other flags */

#define DICT_IT_BITS        6   /*!< number of bits used for SYS_INDEXES.TYPE */


/** Width of the COMPACT flag */
#define DICT_TF_WIDTH_COMPACT		1
/** Width of the ZIP_SSIZE flag */
#define DICT_TF_WIDTH_ZIP_SSIZE		4
/** Width of the ATOMIC_BLOBS flag.  The Antelope file formats broke up
BLOB and TEXT fields, storing the first 768 bytes in the clustered index.
Brracuda row formats store the whole blob or text field off-page atomically.
Secondary indexes are created from this external data using row_ext_t
to cache the BLOB prefixes. */
#define DICT_TF_WIDTH_ATOMIC_BLOBS	1
/** If a table is created with the MYSQL option DATA DIRECTORY and
innodb-file-per-table, an older engine will not be able to find that table.
This flag prevents older engines from attempting to open the table and
allows InnoDB to update_create_info() accordingly. */
#define DICT_TF_WIDTH_DATA_DIR		1

/** Width of all the currently known table flags */
#define DICT_TF_BITS	(DICT_TF_WIDTH_COMPACT		\
			+ DICT_TF_WIDTH_ZIP_SSIZE	\
			+ DICT_TF_WIDTH_ATOMIC_BLOBS	\
			+ DICT_TF_WIDTH_DATA_DIR)

#define DICT_TF2_BITS			6








/* The columns in SYS_TABLES */
enum dict_col_sys_tables_enum {
    DICT_COL__SYS_TABLES__NAME          = 0,
    DICT_COL__SYS_TABLES__ID            = 1,
    DICT_COL__SYS_TABLES__N_COLS        = 2,
    DICT_COL__SYS_TABLES__TYPE          = 3,
    DICT_COL__SYS_TABLES__MIX_ID        = 4,
    DICT_COL__SYS_TABLES__MIX_LEN       = 5,
    DICT_COL__SYS_TABLES__CLUSTER_ID    = 6,
    DICT_COL__SYS_TABLES__SPACE         = 7,
    DICT_NUM_COLS__SYS_TABLES           = 8
};
/* The field numbers in the SYS_TABLES clustered index */
enum dict_fld_sys_tables_enum {
	DICT_FLD__SYS_TABLES__NAME		= 0,
	DICT_FLD__SYS_TABLES__DB_TRX_ID		= 1,
	DICT_FLD__SYS_TABLES__DB_ROLL_PTR	= 2,
	DICT_FLD__SYS_TABLES__ID		= 3,
	DICT_FLD__SYS_TABLES__N_COLS		= 4,
	DICT_FLD__SYS_TABLES__TYPE		= 5,
	DICT_FLD__SYS_TABLES__MIX_ID		= 6,
	DICT_FLD__SYS_TABLES__MIX_LEN		= 7,
	DICT_FLD__SYS_TABLES__CLUSTER_ID	= 8,
	DICT_FLD__SYS_TABLES__SPACE		= 9,
	DICT_NUM_FIELDS__SYS_TABLES		= 10
};
/* The field numbers in the SYS_TABLE_IDS index */
enum dict_fld_sys_table_ids_enum {
	DICT_FLD__SYS_TABLE_IDS__ID		= 0,
	DICT_FLD__SYS_TABLE_IDS__NAME		= 1,
	DICT_NUM_FIELDS__SYS_TABLE_IDS		= 2
};
/* The columns in SYS_COLUMNS */
enum dict_col_sys_columns_enum {
	DICT_COL__SYS_COLUMNS__TABLE_ID		= 0,
	DICT_COL__SYS_COLUMNS__POS		= 1,
	DICT_COL__SYS_COLUMNS__NAME		= 2,
	DICT_COL__SYS_COLUMNS__MTYPE		= 3,
	DICT_COL__SYS_COLUMNS__PRTYPE		= 4,
	DICT_COL__SYS_COLUMNS__LEN		= 5,
	DICT_COL__SYS_COLUMNS__PREC		= 6,
	DICT_NUM_COLS__SYS_COLUMNS		= 7
};
/* The field numbers in the SYS_COLUMNS clustered index */
enum dict_fld_sys_columns_enum {
	DICT_FLD__SYS_COLUMNS__TABLE_ID		= 0,
	DICT_FLD__SYS_COLUMNS__POS		= 1,
	DICT_FLD__SYS_COLUMNS__DB_TRX_ID	= 2,
	DICT_FLD__SYS_COLUMNS__DB_ROLL_PTR	= 3,
	DICT_FLD__SYS_COLUMNS__NAME		= 4,
	DICT_FLD__SYS_COLUMNS__MTYPE		= 5,
	DICT_FLD__SYS_COLUMNS__PRTYPE		= 6,
	DICT_FLD__SYS_COLUMNS__LEN		= 7,
	DICT_FLD__SYS_COLUMNS__PREC		= 8,
	DICT_NUM_FIELDS__SYS_COLUMNS		= 9
};
/* The columns in SYS_INDEXES */
enum dict_col_sys_indexes_enum {
	DICT_COL__SYS_INDEXES__TABLE_ID		= 0,
	DICT_COL__SYS_INDEXES__ID		= 1,
	DICT_COL__SYS_INDEXES__NAME		= 2,
	DICT_COL__SYS_INDEXES__N_FIELDS		= 3,
	DICT_COL__SYS_INDEXES__TYPE		= 4,
	DICT_COL__SYS_INDEXES__SPACE		= 5,
	DICT_COL__SYS_INDEXES__PAGE_NO		= 6,
	DICT_NUM_COLS__SYS_INDEXES		= 7
};
/* The field numbers in the SYS_INDEXES clustered index */
enum dict_fld_sys_indexes_enum {
	DICT_FLD__SYS_INDEXES__TABLE_ID		= 0,
	DICT_FLD__SYS_INDEXES__ID		= 1,
	DICT_FLD__SYS_INDEXES__DB_TRX_ID	= 2,
	DICT_FLD__SYS_INDEXES__DB_ROLL_PTR	= 3,
	DICT_FLD__SYS_INDEXES__NAME		= 4,
	DICT_FLD__SYS_INDEXES__N_FIELDS		= 5,
	DICT_FLD__SYS_INDEXES__TYPE		= 6,
	DICT_FLD__SYS_INDEXES__SPACE		= 7,
	DICT_FLD__SYS_INDEXES__PAGE_NO		= 8,
	DICT_NUM_FIELDS__SYS_INDEXES		= 9
};
/* The columns in SYS_FIELDS */
enum dict_col_sys_fields_enum {
	DICT_COL__SYS_FIELDS__INDEX_ID		= 0,
	DICT_COL__SYS_FIELDS__POS		= 1,
	DICT_COL__SYS_FIELDS__COL_NAME		= 2,
	DICT_NUM_COLS__SYS_FIELDS		= 3
};
/* The field numbers in the SYS_FIELDS clustered index */
enum dict_fld_sys_fields_enum {
	DICT_FLD__SYS_FIELDS__INDEX_ID		= 0,
	DICT_FLD__SYS_FIELDS__POS		= 1,
	DICT_FLD__SYS_FIELDS__DB_TRX_ID		= 2,
	DICT_FLD__SYS_FIELDS__DB_ROLL_PTR	= 3,
	DICT_FLD__SYS_FIELDS__COL_NAME		= 4,
	DICT_NUM_FIELDS__SYS_FIELDS		= 5
};
/* The columns in SYS_FOREIGN */
enum dict_col_sys_foreign_enum {
	DICT_COL__SYS_FOREIGN__ID		= 0,
	DICT_COL__SYS_FOREIGN__FOR_NAME		= 1,
	DICT_COL__SYS_FOREIGN__REF_NAME		= 2,
	DICT_COL__SYS_FOREIGN__N_COLS		= 3,
	DICT_NUM_COLS__SYS_FOREIGN		= 4
};
/* The field numbers in the SYS_FOREIGN clustered index */
enum dict_fld_sys_foreign_enum {
	DICT_FLD__SYS_FOREIGN__ID		= 0,
	DICT_FLD__SYS_FOREIGN__DB_TRX_ID	= 1,
	DICT_FLD__SYS_FOREIGN__DB_ROLL_PTR	= 2,
	DICT_FLD__SYS_FOREIGN__FOR_NAME		= 3,
	DICT_FLD__SYS_FOREIGN__REF_NAME		= 4,
	DICT_FLD__SYS_FOREIGN__N_COLS		= 5,
	DICT_NUM_FIELDS__SYS_FOREIGN		= 6
};
/* The field numbers in the SYS_FOREIGN_FOR_NAME secondary index */
enum dict_fld_sys_foreign_for_name_enum {
	DICT_FLD__SYS_FOREIGN_FOR_NAME__NAME	= 0,
	DICT_FLD__SYS_FOREIGN_FOR_NAME__ID	= 1,
	DICT_NUM_FIELDS__SYS_FOREIGN_FOR_NAME	= 2
};
/* The columns in SYS_FOREIGN_COLS */
enum dict_col_sys_foreign_cols_enum {
	DICT_COL__SYS_FOREIGN_COLS__ID			= 0,
	DICT_COL__SYS_FOREIGN_COLS__POS			= 1,
	DICT_COL__SYS_FOREIGN_COLS__FOR_COL_NAME	= 2,
	DICT_COL__SYS_FOREIGN_COLS__REF_COL_NAME	= 3,
	DICT_NUM_COLS__SYS_FOREIGN_COLS			= 4
};
/* The field numbers in the SYS_FOREIGN_COLS clustered index */
enum dict_fld_sys_foreign_cols_enum {
	DICT_FLD__SYS_FOREIGN_COLS__ID			= 0,
	DICT_FLD__SYS_FOREIGN_COLS__POS			= 1,
	DICT_FLD__SYS_FOREIGN_COLS__DB_TRX_ID		= 2,
	DICT_FLD__SYS_FOREIGN_COLS__DB_ROLL_PTR		= 3,
	DICT_FLD__SYS_FOREIGN_COLS__FOR_COL_NAME	= 4,
	DICT_FLD__SYS_FOREIGN_COLS__REF_COL_NAME	= 5,
	DICT_NUM_FIELDS__SYS_FOREIGN_COLS		= 6
};
/* The columns in SYS_TABLESPACES */
enum dict_col_sys_tablespaces_enum {
	DICT_COL__SYS_TABLESPACES__SPACE		= 0,
	DICT_COL__SYS_TABLESPACES__NAME			= 1,
	DICT_COL__SYS_TABLESPACES__FLAGS		= 2,
	DICT_NUM_COLS__SYS_TABLESPACES			= 3
};
/* The field numbers in the SYS_TABLESPACES clustered index */
enum dict_fld_sys_tablespaces_enum {
	DICT_FLD__SYS_TABLESPACES__SPACE		= 0,
	DICT_FLD__SYS_TABLESPACES__DB_TRX_ID		= 1,
	DICT_FLD__SYS_TABLESPACES__DB_ROLL_PTR		= 2,
	DICT_FLD__SYS_TABLESPACES__NAME			= 3,
	DICT_FLD__SYS_TABLESPACES__FLAGS		= 4,
	DICT_NUM_FIELDS__SYS_TABLESPACES		= 5
};
/* The columns in SYS_DATAFILES */
enum dict_col_sys_datafiles_enum {
	DICT_COL__SYS_DATAFILES__SPACE			= 0,
	DICT_COL__SYS_DATAFILES__PATH			= 1,
	DICT_NUM_COLS__SYS_DATAFILES			= 2
};
/* The field numbers in the SYS_DATAFILES clustered index */
enum dict_fld_sys_datafiles_enum {
	DICT_FLD__SYS_DATAFILES__SPACE			= 0,
	DICT_FLD__SYS_DATAFILES__DB_TRX_ID		= 1,
	DICT_FLD__SYS_DATAFILES__DB_ROLL_PTR		= 2,
	DICT_FLD__SYS_DATAFILES__PATH			= 3,
	DICT_NUM_FIELDS__SYS_DATAFILES			= 4
};




/** Quiescing states for flushing tables to disk. */
enum ib_quiesce_t {
	QUIESCE_NONE,
	QUIESCE_START,			/*!< Initialise, prepare to start */
	QUIESCE_COMPLETE		/*!< All done */
};

typedef	byte    dict_hdr_t;

typedef struct st_dict_table dict_table_t;
typedef struct st_dict_index dict_index_t;


#define M_MIN_NUM_SCALE         (int32)(-84)
#define M_MAX_NUM_SCALE         (int32)127

#define M_MIN_NUM_PRECISION     (int32)1
#define M_MAX_NUM_PRECISION     (int32)38



/** Data structure for a column in a table */
typedef struct st_dict_col {
    uint8  mtype;  // main data type
    union {
        struct {
            uint8  precision;
            uint8  scale;
            uint8  aligned1;
        };
        struct {
            uint16 len;
            uint8  mbminmaxlen : 5; // minimum and maximum length of a character, in bytes
            uint8  aligned2    : 3;
        };
    };

    uint32 ind           : 10; // table column position (starting from 0)
    uint32 nullable      : 1;  // null or not null
    uint32 is_compressed : 1;
    uint32 is_droped     : 1;
    uint32 is_hidden     : 1;
    uint32 reserved      : 18;

    void*  default_expr;  // deserialized default expr
} dict_col_t;


/** Data structure for a field in an index */
typedef struct st_dict_field{
	dict_col_t*	col;		/*!< pointer to the table column */
	const char*	name;		/*!< name of the column */
	unsigned	prefix_len:12;	/*!< 0 or the length of the column
					prefix in bytes in a MySQL index of
					type, e.g., INDEX (textcol(25));
					must be smaller than
					DICT_MAX_FIELD_LEN_BY_FORMAT;
					NOTE that in the UTF-8 charset, MySQL
					sets this to (mbmaxlen * the prefix len)
					in UTF-8 chars */
	unsigned	fixed_len:10;	/*!< 0 or the fixed length of the
					column if smaller than
					DICT_ANTELOPE_MAX_INDEX_COL_LEN */
} dict_field_t;

struct st_dict_heap {
    dict_table_t*   table;
    uint32          space_id;
    uint32          map_root_page;  // map root page number */

};

/** Data structure for an index.  Most fields will be
initialized to 0, NULL or FALSE in dict_mem_index_create(). */
struct st_dict_index{
	index_id_t	id;	/*!< id of the index */
	const char*	name;	/*!< index name */
	const char*	table_name;/*!< table name */
	dict_table_t*	table;	/*!< back pointer to table */
	unsigned	space:32; /*!< space where the index tree is placed */
	unsigned	page:32; /*!< index tree root page number */
	unsigned	type:DICT_IT_BITS;
				/*!< index type (DICT_CLUSTERED, DICT_UNIQUE,
				DICT_UNIVERSAL, DICT_IBUF, DICT_CORRUPT) */
#define MAX_KEY_LENGTH_BITS 12
	unsigned	trx_id_offset:MAX_KEY_LENGTH_BITS;
				/*!< position of the trx id column
				in a clustered index record, if the fields
				before it are known to be of a fixed size,
				0 otherwise */
#if (1<<MAX_KEY_LENGTH_BITS) < MAX_KEY_LENGTH
# error (1<<MAX_KEY_LENGTH_BITS) < MAX_KEY_LENGTH
#endif
	unsigned	n_user_defined_cols:10;
				/*!< number of columns the user defined to
				be in the index: in the internal
				representation we add more columns */
	unsigned	n_uniq:10;/*!< number of fields from the beginning
				which are enough to determine an index
				entry uniquely */
	unsigned	n_def:10;/*!< number of fields defined so far */
	unsigned	n_fields:10;/*!< number of fields in the index */
	unsigned	n_nullable:10;/*!< number of nullable fields */
	unsigned	cached:1;/*!< TRUE if the index object is in the
				dictionary cache */
	unsigned	to_be_dropped:1;
				/*!< TRUE if the index is to be dropped;
				protected by dict_operation_lock */
	unsigned	online_status:2;
				/*!< enum online_index_status.
				Transitions from ONLINE_INDEX_COMPLETE (to
				ONLINE_INDEX_CREATION) are protected
				by dict_operation_lock and
				dict_sys->mutex. Other changes are
				protected by index->lock. */
	dict_field_t*	fields;	/*!< array of field descriptions */

    HASH_NODE_T	name_hash; /*!< hash chain node */
    HASH_NODE_T	id_hash; /*!< hash chain node */

	UT_LIST_NODE_T(dict_index_t) indexes; /*!< list of indexes of the table */

	/** Statistics for query optimization */
	uint64*	stat_n_diff_key_vals;
				/*!< approximate number of different
				key values for this index, for each
				n-column prefix where 1 <= n <=
				dict_get_n_unique(index) (the array is
				indexed from 0 to n_uniq-1); we
				periodically calculate new
				estimates */
	uint64*	stat_n_sample_sizes;
				/*!< number of pages that were sampled
				to calculate each of stat_n_diff_key_vals[],
				e.g. stat_n_sample_sizes[3] pages were sampled
				to get the number stat_n_diff_key_vals[3]. */
	uint64*	stat_n_non_null_key_vals;
				/* approximate number of non-null key values
				for this index, for each column where
				1 <= n <= dict_get_n_unique(index) (the array
				is indexed from 0 to n_uniq-1); This
				is used when innodb_stats_method is
				"nulls_ignored". */
	uint32		stat_index_size;  /*!< approximate index size in database pages */
    uint32		stat_n_leaf_pages;
				/*!< approximate number of leaf pages in the index tree */

	rw_lock_t	lock;	/*!< read-write lock protecting the
				upper levels of the index tree */
	trx_id_t	trx_id; /*!< id of the transaction that created this
				index, or 0 if the index existed
				when InnoDB was started up */

};


/** Data structure for a database table.  Most fields will be
initialized to 0, NULL or FALSE in dict_mem_table_create(). */
struct st_dict_table{
	table_id_t	id;	/*!< id of the table */
    memory_context_t* mem_ctx;
	char*		name;	/*!< table name */
	const char*	dir_path_of_temp_table;/*!< NULL or the directory path
				where a TEMPORARY table that was explicitly
				created by a user should be placed if
				innodb_file_per_table is defined in my.cnf;
				in Unix this is usually /tmp/..., in Windows
				temp\... */
	char*		data_dir_path; /*!< NULL or the directory path
				specified by DATA DIRECTORY */
	uint32      space_id;
    uint32      entry_page_no;
    bool32      heap_io_in_progress;
    uint8       init_trans;
    uint8       pctfree;
    mutex_t     mutex;

	unsigned	flags:DICT_TF_BITS;	/*!< DICT_TF_... */
	unsigned	flags2:DICT_TF2_BITS;	/*!< DICT_TF2_... */
	unsigned	ibd_file_missing:1;
				/*!< TRUE if this is in a single-table
				tablespace and the .ibd file is missing; then
				we must return in ha_innodb.cc an error if the
				user tries to query such an orphaned table */
	unsigned	cached:1;/*!< TRUE if the table object has been added
				to the dictionary cache */
	unsigned	to_be_dropped:1;
				/*!< TRUE if the table is to be dropped, but
				not yet actually dropped (could in the bk
				drop list); It is turned on at the beginning
				of row_drop_table_for_mysql() and turned off
				just before we start to update system tables
				for the drop. It is protected by
				dict_operation_lock */
	unsigned	n_def:10;/*!< number of columns defined so far */
	unsigned	n_cols:10;/*!< number of columns */
	unsigned	can_be_evicted:1;
				/*!< TRUE if it's not an InnoDB system table
				or a table that has no FK relationships */
	unsigned	corrupted:1;
				/*!< TRUE if table is corrupted */
	unsigned	drop_aborted:1;
				/*!< TRUE if some indexes should be dropped
				after ONLINE_INDEX_ABORTED
				or ONLINE_INDEX_ABORTED_DROPPED */
	dict_col_t*	cols;	/*!< array of column descriptions */
	const char*	col_names;
				/*!< Column names packed in a character string
				"name1\0name2\0...nameN\0".  Until
				the string contains n_cols, it will be
				allocated from a temporary heap.  The final
				string will be allocated from table->heap. */
#ifndef UNIV_HOTBACKUP
	HASH_NODE_T name_hash; /*!< hash chain node */
	HASH_NODE_T id_hash; /*!< hash chain node */
	UT_LIST_BASE_NODE_T(dict_index_t)
			indexes; /*!< list of indexes of the table */
	//UT_LIST_BASE_NODE_T(dict_foreign_t)
	//		foreign_list;/*!< list of foreign key constraints
	//			in the table; these refer to columns
	//			in other tables */
	//UT_LIST_BASE_NODE_T(dict_foreign_t)
	//		referenced_list;/*!< list of foreign key constraints
	//			which refer to this table */
	UT_LIST_NODE_T(dict_table_t)
			table_LRU; /*!< node of the LRU list of tables */
	unsigned	fk_max_recusive_level:8;
				/*!< maximum recursive level we support when
				loading tables chained together with FK
				constraints. If exceeds this level, we will
				stop loading child table into memory along with
				its parent table */
	uint32		n_foreign_key_checks_running;
				/*!< count of how many foreign key check
				operations are currently being performed
				on the table: we cannot drop the table while
				there are foreign key checks running on
				it! */
	trx_id_t	def_trx_id;
				/*!< transaction id that last touched
				the table definition, either when
				loading the definition or CREATE
				TABLE, or ALTER TABLE (prepare,
				commit, and rollback phases) */
	trx_id_t	query_cache_inv_trx_id;
				/*!< transactions whose trx id is
				smaller than this number are not
				allowed to store to the MySQL query
				cache or retrieve from it; when a trx
				with undo logs commits, it sets this
				to the value of the trx id counter for
				the tables it had an IX lock on */
#ifdef UNIV_DEBUG
	/*----------------------*/
	bool32		does_not_fit_in_memory;
				/*!< this field is used to specify in
				simulations tables which are so big
				that disk should be accessed: disk
				access is simulated by putting the
				thread to sleep for a while; NOTE that
				this flag is not stored to the data
				dictionary on disk, and the database
				will forget about value TRUE if it has
				to reload the table definition from
				disk */
#endif /* UNIV_DEBUG */
	/*----------------------*/
	unsigned	big_rows:1;
				/*!< flag: TRUE if the maximum length of
				a single row exceeds BIG_ROW_SIZE;
				initialized in dict_table_add_to_cache() */
				/** Statistics for query optimization */
				/* @{ */
	unsigned	stat_initialized:1; /*!< TRUE if statistics have
				been calculated the first time
				after database startup or table creation */
    time_t	stats_last_recalc;
				/*!< Timestamp of last recalc of the stats */
	uint32	stat_persistent;
				/*!< The two bits below are set in the
				::stat_persistent member and have the following
				meaning:
				1. _ON=0, _OFF=0, no explicit persistent stats
				setting for this table, the value of the global
				srv_stats_persistent is used to determine
				whether the table has persistent stats enabled
				or not
				2. _ON=0, _OFF=1, persistent stats are
				explicitly disabled for this table, regardless
				of the value of the global srv_stats_persistent
				3. _ON=1, _OFF=0, persistent stats are
				explicitly enabled for this table, regardless
				of the value of the global srv_stats_persistent
				4. _ON=1, _OFF=1, not allowed, we assert if
				this ever happens. */
#define DICT_STATS_PERSISTENT_ON	(1 << 1)
#define DICT_STATS_PERSISTENT_OFF	(1 << 2)
	uint32	stats_auto_recalc;
				/*!< The two bits below are set in the
				::stats_auto_recalc member and have
				the following meaning:
				1. _ON=0, _OFF=0, no explicit auto recalc
				setting for this table, the value of the global
				srv_stats_persistent_auto_recalc is used to
				determine whether the table has auto recalc
				enabled or not
				2. _ON=0, _OFF=1, auto recalc is explicitly
				disabled for this table, regardless of the
				value of the global
				srv_stats_persistent_auto_recalc
				3. _ON=1, _OFF=0, auto recalc is explicitly
				enabled for this table, regardless of the
				value of the global
				srv_stats_persistent_auto_recalc
				4. _ON=1, _OFF=1, not allowed, we assert if
				this ever happens. */
#define DICT_STATS_AUTO_RECALC_ON	(1 << 1)
#define DICT_STATS_AUTO_RECALC_OFF	(1 << 2)
	uint32		stats_sample_pages;
				/*!< the number of pages to sample for this
				table during persistent stats estimation;
				if this is 0, then the value of the global
				srv_stats_persistent_sample_pages will be
				used instead. */
	uint64	stat_n_rows;
				/*!< approximate number of rows in the table;
				we periodically calculate new estimates */
    uint32		stat_clustered_index_size;
				/*!< approximate clustered index size in
				database pages */
    uint32		stat_sum_of_other_index_sizes;
				/*!< other indexes in database pages */
	uint64	stat_modified_counter;
				/*!< when a row is inserted, updated,
				or deleted,
				we add 1 to this number; we calculate new
				estimates for the stat_... values for the
				table and the indexes when about 1 / 16 of
				table has been modified;
				also when the estimate operation is
				called for MySQL SHOW TABLE STATUS; the
				counter is reset to zero at statistics
				calculation; this counter is not protected by
				any latch, because this is only used for
				heuristics */
#define BG_STAT_NONE		0
#define BG_STAT_IN_PROGRESS	(1 << 0)
				/*!< BG_STAT_IN_PROGRESS is set in
				stats_bg_flag when the background
				stats code is working on this table. The DROP
				TABLE code waits for this to be cleared
				before proceeding. */
#define BG_STAT_SHOULD_QUIT	(1 << 1)
				/*!< BG_STAT_SHOULD_QUIT is set in
				stats_bg_flag when DROP TABLE starts
				waiting on BG_STAT_IN_PROGRESS to be cleared,
				the background stats thread will detect this
				and will eventually quit sooner */
	byte		stats_bg_flag;
				/*!< see BG_STAT_* above.
				Writes are covered by dict_sys->mutex.
				Dirty reads are possible. */
				/* @} */
	/*----------------------*/
				/**!< The following fields are used by the
				AUTOINC code.  The actual collection of
				tables locked during AUTOINC read/write is
				kept in trx_t. In order to quickly determine
				whether a transaction has locked the AUTOINC
				lock we keep a pointer to the transaction
				here in the autoinc_trx variable. This is to
				avoid acquiring the lock_sys_t::mutex and
				scanning the vector in trx_t.

				When an AUTOINC lock has to wait, the
				corresponding lock instance is created on
				the trx lock heap rather than use the
				pre-allocated instance in autoinc_lock below.*/
				/* @{ */
	//lock_t*		autoinc_lock;
				/*!< a buffer for an AUTOINC lock
				for this table: we allocate the memory here
				so that individual transactions can get it
				and release it without a need to allocate
				space from the lock heap of the trx:
				otherwise the lock heap would grow rapidly
				if we do a large insert from a select */
	mutex_t		autoinc_mutex;
				/*!< mutex protecting the autoincrement
				counter */
	uint64	autoinc;/*!< autoinc counter value to give to the
				next inserted row */
	ulong		n_waiting_or_granted_auto_inc_locks;
				/*!< This counter is used to track the number
				of granted and pending autoinc locks on this
				table. This value is set after acquiring the
				lock_sys_t::mutex but we peek the contents to
				determine whether other transactions have
				acquired the AUTOINC lock or not. Of course
				only one transaction can be granted the
				lock but there can be multiple waiters. */
	const trx_t*	autoinc_trx;
				/*!< The transaction that currently holds the
				the AUTOINC lock on this table.
				Protected by lock_sys->mutex. */
	//fts_t*		fts;	/* FTS specific state variables */

	/*----------------------*/

	ib_quiesce_t	 quiesce;/*!< Quiescing states, protected by the
				dict_index_t::lock. ie. we can only change
				the state if we acquire all the latches
				(dict_index_t::lock) in X mode of this table's
				indexes. */

	/*----------------------*/
	uint32		n_rec_locks;
				/*!< Count of the number of record locks on
				this table. We use this to determine whether
				we can evict the table from the dictionary
				cache. It is protected by lock_sys->mutex. */
    uint32		n_ref_count;
				/*!< count of how many handles are opened
				to this table; dropping of the table is
				NOT allowed until this count gets to zero;
				MySQL does NOT itself check the number of
				open handles at drop */
	//UT_LIST_BASE_NODE_T(lock_t)
	//		locks;	/*!< list of locks on the table; protected by lock_sys->mutex */
#endif /* !UNIV_HOTBACKUP */

#ifdef UNIV_DEBUG
	uint32		magic_n;/*!< magic number */
/** Value of dict_table_t::magic_n */
# define DICT_TABLE_MAGIC_N	76333786
#endif /* UNIV_DEBUG */
};



/* Dictionary system struct */
struct dict_sys_t {
    mutex_t             mutex;
    uint64              row_id;
    /*!< hash table of the tables, based on name */
    HASH_TABLE*         table_hash;
    /*!< hash table of the tables, based on id */
    HASH_TABLE*         table_id_hash;
    /*!< varying space in bytes occupied by the data dictionary table and index objects */
    uint32              size;

    /** Handler to sys_* tables, they're only for upgrade */
    dict_table_t*       sys_tables;  /*!< SYS_TABLES table */
    dict_table_t*       sys_columns; /*!< SYS_COLUMNS table */
    dict_table_t*       sys_indexes; /*!< SYS_INDEXES table */
    dict_table_t*       sys_fields;  /*!< SYS_FIELDS table */
    dict_table_t*       sys_virtual; /*!< SYS_VIRTUAL table */

    /** Permanent handle to mysql.innodb_table_stats */
    dict_table_t*       table_stats;
    /** Permanent handle to mysql.innodb_index_stats */
    dict_table_t*       index_stats;
    /** Permanent handle to mysql.innodb_ddl_log */
    dict_table_t*       ddl_log;
    /** Permanent handle to mysql.innodb_dynamic_metadata */
    dict_table_t*       dynamic_metadata;
    memory_pool_t*      mem_pool;

    UT_LIST_BASE_NODE_T(dict_table_t) table_LRU;
    UT_LIST_BASE_NODE_T(dict_table_t) table_non_LRU;







  /** Iterate each table.
  @tparam Functor visitor
  @param[in,out]  functor to be invoked on each table */
  template <typename Functor>
  void for_each_table(Functor &functor) {
    mutex_enter(&mutex);

    HASH_TABLE *hash = table_id_hash;

    for (uint32 i = 0; i < hash->n_cells; i++) {
      for (dict_table_t *table =
               static_cast<dict_table_t *>(HASH_GET_FIRST(hash, i));
           table;
           table = static_cast<dict_table_t *>(HASH_GET_NEXT(id_hash, table))) {
        functor(table);
      }
    }

    mutex_exit(&mutex);
  }

  /** Check if a tablespace id is a reserved one
  @param[in]	space	tablespace id to check
  @return true if a reserved tablespace id, otherwise false */
  //static bool is_reserved(space_id_t space) {
  //  return (space >= dict_sys_t::s_reserved_space_id || fsp_is_session_temporary(space));
  //}

  /** Set of ids of DD tables */
  //static std::set<dd::Object_id> s_dd_table_ids;

  /** Check if a table is hardcoded. it only includes the dd tables
  @param[in]	id	table ID
  @retval true	if the table is a persistent hard-coded table
                  (dict_table_t::is_temporary() will not hold)
  @retval false	if the table is not hard-coded
                  (it can be persistent or temporary) */
  //static bool is_dd_table_id(uint64 id) {
  //  return (s_dd_table_ids.find(id) != s_dd_table_ids.end());
  //}

  /** The first ID of the redo log pseudo-tablespace */
#define REDO_SPACE_FIRST_ID       0xFFFFFFF0UL
  //static constexpr space_id_t s_log_space_first_id = 0xFFFFFFF0UL;

  /** Use maximum UINT value to indicate invalid space ID. */
#define INVALID_SPACE_ID          0xFFFFFFFF
  //static constexpr space_id_t s_invalid_space_id = 0xFFFFFFFF;

  /** The data dictionary tablespace ID. */
#define DICT_SPACE_ID             0xFFFFFFFE
  //static constexpr space_id_t s_space_id = 0xFFFFFFFE;

  /** The innodb_temporary tablespace ID. */
#define TEMP_SPACE_ID             0xFFFFFFFD
  //static constexpr space_id_t s_temp_space_id = 0xFFFFFFFD;

  /** The number of space IDs dedicated to each undo tablespace */
#define UNDO_SPACE_ID_RANGE       512
  //static constexpr space_id_t undo_space_id_range = 512;

  /** The lowest undo tablespace ID. */
#define UNDO_SPACE_MIN_ID         (REDO_SPACE_FIRST_ID - (FSP_MAX_UNDO_TABLESPACES * UNDO_SPACE_ID_RANGE))
  //static constexpr space_id_t s_min_undo_space_id = s_log_space_first_id - (FSP_MAX_UNDO_TABLESPACES * undo_space_id_range);

  /** The highest undo  tablespace ID. */
#define UNDO_SPACE_MAX_ID         (REDO_SPACE_FIRST_ID - 1)
  //static constexpr space_id_t s_max_undo_space_id = s_log_space_first_id - 1;

  /** The first reserved tablespace ID */
#define RESERVED_SPACE_ID         UNDO_SPACE_MIN_ID
  //static constexpr space_id_t s_reserved_space_id = s_min_undo_space_id;

  /** Leave 1K space_ids and start space_ids for temporary
  general tablespaces (total 400K space_ids)*/
#define TEMP_SPACE_MAX_ID         (RESERVED_SPACE_ID - 1000)
  //static constexpr space_id_t s_max_temp_space_id = s_reserved_space_id - 1000;

  /** Lowest temporary general space id */
#define TEMP_SPACE_MIN_ID         (RESERVED_SPACE_ID - 1000 - 400000)
  //static constexpr space_id_t s_min_temp_space_id = s_reserved_space_id - 1000 - 400000;

  /** The dd::Tablespace::id of the dictionary tablespace. */
  //static constexpr dd::Object_id s_dd_space_id = 1;

  /** The dd::Tablespace::id of innodb_system. */
  //static constexpr dd::Object_id s_dd_sys_space_id = 2;

  /** The dd::Tablespace::id of innodb_temporary. */
  //static constexpr dd::Object_id s_dd_temp_space_id = 3;

  /** The name of the data dictionary tablespace. */
  static const char *s_dd_space_name;

  /** The file name of the data dictionary tablespace. */
  static const char *s_dd_space_file_name;

  /** The name of the hard-coded system tablespace. */
  static const char *s_sys_space_name;

  /** The name of the predefined temporary tablespace. */
  static const char *s_temp_space_name;

  /** The file name of the predefined temporary tablespace. */
  static const char *s_temp_space_file_name;

  /** The hard-coded tablespace name innodb_file_per_table. */
  static const char *s_file_per_table_name;

  /** These two undo tablespaces cannot be dropped. */
  static const char *s_default_undo_space_name_1;
  static const char *s_default_undo_space_name_2;

  /** The table ID of mysql.innodb_dynamic_metadata */
  //static constexpr table_id_t s_dynamic_meta_table_id = 2;

  /** The clustered index ID of mysql.innodb_dynamic_metadata */
  //static constexpr space_index_t s_dynamic_meta_index_id = 2;


};


//------------------------------------------------------

#define dict_table_get_nth_col(table, pos) ((table)->cols + (pos))
#define dict_table_get_sys_col(table, sys) \
    ((table)->cols + (table)->n_cols + (sys) - DATA_N_SYS_COLS)
#define dict_index_get_nth_field(index, pos) ((index)->fields + (pos))


extern dberr_t dict_boot(void);
extern dberr_t dict_create(void);

extern dict_table_t* dict_mem_table_create(const char* name, uint32 space, uint32 n_cols, uint32 flags, uint32 flags2);
extern void dict_mem_table_add_col(dict_table_t* table, const char* name, uint32 mtype, uint32 prtype, uint32 len);
extern dict_index_t* dict_mem_index_create(dict_table_t* table, const char* index_name, uint32 space_id, uint32 type, uint32 n_fields);
extern void dict_mem_index_add_field(dict_index_t* index, const char* name, uint32 prefix_len);
extern void dict_table_add_to_cache(dict_table_t* table, bool32 can_be_evicted);


//------------------------------------------------------

/** the dictionary system */
extern dict_sys_t*    dict_sys;


#endif  /* _KNL_DICT_H */
