#ifndef _KNL_DICT_H
#define _KNL_DICT_H

#include "cm_type.h"
#include "cm_error.h"
#include "cm_mutex.h"
#include "cm_memory.h"
#include "cm_date.h"
#include "knl_hash_table.h"
#include "knl_server.h"
#include "knl_trx_types.h"
#include "knl_data_type.h"

/* Space id and page no where the dictionary header resides */
#define DICT_HDR_SPACE      0   /* the SYSTEM tablespace */
#define DICT_HDR_PAGE_NO    FSP_DICT_HDR_PAGE_NO

#define DICT_INVALID_OBJECT_ID        UINT_MAX64
#define DICT_TABLE_COLUMN_MAX_COUNT   1000
#define DICT_INDEX_COLUMN_MAX_COUNT   30


/* The ids for the basic system tables and their indexes */
#define DICT_TABLES_ID              1
#define DICT_COLUMNS_ID             2
#define DICT_INDEXES_ID             3
#define DICT_FIELDS_ID              4

/* The following is a secondary index on SYS_TABLES */
#define DICT_TABLES_CLUST_ID        5
#define DICT_TABLES_INDEX_ID_ID     6
#define DICT_COLUMNS_CLUST_ID       7
#define DICT_INDEXES_CLUST_ID       8
#define DICT_FIELDS_CLUST_ID        9


/* the ids for tables etc.
   start from this number, except for basic system tables and their above defined indexes;
   ibuf tables and indexes are assigned as the id the number DICT_IBUF_ID_MIN plus the space id */
#define DICT_HDR_FIRST_ID           256

/* The offset of the dictionary header on the page */
#define DICT_HDR                    FSEG_PAGE_DATA

/*-------------------------------------------------------------*/
/* Dictionary header offsets */
#define DICT_HDR_ROW_ID         0   /* The latest assigned row id */
#define DICT_HDR_TABLE_ID       8   /* The latest assigned table id */
#define DICT_HDR_INDEX_ID       16  /* The latest assigned index id */
#define DICT_HDR_MAX_SPACE_ID   24  /* The latest assigned space id,or 0*/
#define DICT_HDR_MIX_ID_LOW     28  /* Obsolete,always DICT_HDR_FIRST_ID*/
#define DICT_HDR_TABLES         32  /* Root of SYS_TABLES heap */
#define DICT_HDR_TABLE_CLUST    36  /* Root of SYS_TABLE_CLUST clust index */
#define DICT_HDR_TABLE_IDS      40  /* Root of SYS_TABLE_IDS sec index */
#define DICT_HDR_COLUMNS        44  /* Root of SYS_COLUMNS heap */
#define DICT_HDR_COLUMNS_CLUST  48  /* Root of SYS_COLUMNS clust index */
#define DICT_HDR_INDEXES        52  /* Root of SYS_INDEXES heap */
#define DICT_HDR_INDEXES_CLUST  56  /* Root of SYS_INDEXES clust index */
#define DICT_HDR_FIELDS         60  /* Root of SYS_FIELDS heap */
#define DICT_HDR_FIELDS_CLUST   64  /* Root of SYS_FIELDS clust index */
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

typedef struct st_dict_table    dict_table_t;
typedef struct st_dict_index    dict_index_t;


#define M_MIN_NUM_SCALE         (int32)(-84)
#define M_MAX_NUM_SCALE         (int32)127

#define M_MIN_NUM_PRECISION     (int32)1
#define M_MAX_NUM_PRECISION     (int32)38

// Data structure for a column in a table
// total 16 Bytes
typedef struct st_dict_col {
    char*  name;
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
} dict_col_t;

// indexed field length (or indexed prefix length) for indexes on tables
#define DICT_INDEXE_MAX_COL_LEN     768


// Data structure for a field in an index
typedef struct st_dict_field{
    uint32      col_ind:10;  // table column position (starting from 0)
    // 0 or the length of the column prefix in bytes in a MySQL index of type, e.g., INDEX (textcol(25));
    // must be smaller than DICT_INDEXE_MAX_COL_LEN;
    // NOTE that in the UTF-8 charset, MySQL sets this to (mbmaxlen * the prefix len) in UTF-8 chars
    uint32      prefix_len:10;
    uint32      fixed_len:10; // 0 or the fixed length of the column if smaller than DICT_INDEXE_MAX_COL_LEN
    uint32      reserved:2;
} dict_field_t;

struct st_dict_heap {
    dict_table_t*   table;
    uint32          space_id;
    uint32          map_root_page;  // map root page number */

};

/** Data structure for an index.  Most fields will be
initialized to 0, NULL or FALSE in dict_mem_index_create(). */
struct st_dict_index{
    index_id_t      id;
    const char*     name; // index name
    dict_table_t*   table;
    uint32          space_id; // space where the index tree is placed
    uint32          page_no; // index tree root page number
    // index type (DICT_CLUSTERED, DICT_UNIQUE, DICT_UNIVERSAL, DICT_IBUF, DICT_CORRUPT)
    uint32          type:DICT_IT_BITS;
    uint32          field_index:5;
    uint32          field_count:5;
    uint32          to_be_dropped:1;
    uint32          online_status:2;  // enum online_index_status
    uint32          reserved:14;
    dict_field_t*   fields; // array of field descriptions

    HASH_NODE_T     name_hash; // hash chain node
    HASH_NODE_T     id_hash; // hash chain node

    UT_LIST_NODE_T(dict_index_t) indexes; // list of indexes of the table

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
};

#define DICT_TYPE_TABLE_NOLOGGING       1
#define DICT_TYPE_TEMP_TABLE_SESSION    2

#define DICT_NEED_REDO(table) (!((table)->type == DICT_TYPE_TABLE_NOLOGGING || (table)->type == DICT_TYPE_TEMP_TABLE_SESSION))

#define DICT_TABLE_NOT_FOUND       0
#define DICT_TABLE_CACHED          1
#define DICT_TABLE_TO_BE_DROPED    2
#define DICT_TABLE_TO_BE_LOADED    4
#define DICT_TABLE_TO_BE_REMOVED   8

#define DICT_TOUCH_AGE             3
#define DICT_IN_USE(object)          ((object)->ref_count > 0)
#define DICT_IS_HOT(object)          ((object)->touch_number >= DICT_TOUCH_AGE)
#define DICT_LRU_INTERVAL_WINDOW_US  3000000  // 3 seconds


struct st_dict_table {
    table_id_t      id;
    char*           name; // table name
    space_id_t      space_id;
    page_no_t       entry_page_no;
    uint32          type;

    // array of column descriptions
    uint16          column_index;
    uint16          column_count;
    dict_col_t*     columns;
    uchar*          default_values;

    bool32          heap_io_in_progress;
    uint8           init_trans;
    uint8           pctfree;
    mutex_t         mutex;

    union {
        uint32  status;
        struct {
            // TRUE if the table object has been added to the dictionary cache
            uint32  cached : 1;
            // TRUE if the table is to be dropped,
            // but not yet actually dropped (could in the bk drop list)
            // It is protected by dict_operation_lock
            uint32  to_be_dropped : 1;
            uint32  to_be_loaded : 1;
            uint32  to_be_cache_removed : 1;

            // TRUE if it's not system table or a table that has no FK relationships
            uint32  can_be_evicted : 1;
            // TRUE if table is corrupted
            uint32  corrupted : 1;
            // TRUE if some indexes should be dropped
            // after ONLINE_INDEX_ABORTED or ONLINE_INDEX_ABORTED_DROPPED
            uint32  drop_aborted : 1;
            uint32  in_lru_list : 1;
            uint32  reserved : 24;
        };
    };

    // count of how many handles are opened to this table
    atomic32_t      ref_count;
    volatile uint16 touch_number;
    volatile date_t access_time;

    UT_LIST_BASE_NODE_T(dict_index_t) indexes;
    //UT_LIST_BASE_NODE_T(dict_foreign_t) foreign_list;
    //UT_LIST_BASE_NODE_T(dict_foreign_t) referenced_list;


    UT_LIST_NODE_T(dict_table_t) table_LRU; // node of the LRU list of tables
    HASH_NODE_T                  name_hash; // hash chain node
    HASH_NODE_T                  id_hash;   // hash chain node
    memory_stack_context_t*      mem_stack_ctx;
};

#define DICT_TABLE_LRU_LIST_COUNT         16
#define DICT_TABLE_GET_LRU_LIST_ID(table) (table->id % DICT_TABLE_LRU_LIST_COUNT)

/* Dictionary system struct */
struct dict_sys_t {
    mutex_t             mutex;
    uint64              row_id;
    // hash table of the tables, based on name
    HASH_TABLE*         table_hash;
    // hash table of the tables, based on id
    HASH_TABLE*         table_id_hash;

    // varying space in bytes occupied by the data dictionary table and index objects
    atomic64_t          memory_cache_size;
    uint64              memory_cache_max_size;

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

    mutex_t             table_LRU_list_mutex[DICT_TABLE_LRU_LIST_COUNT];
    UT_LIST_BASE_NODE_T(dict_table_t) table_LRU_list[DICT_TABLE_LRU_LIST_COUNT];

    
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

#define dict_table_get_nth_col(table, pos) ((table)->columns + (pos))
#define dict_index_get_nth_field(index, pos) ((index)->fields + (pos))

extern status_t dict_init(memory_pool_t* mem_pool, uint64 memory_cache_size, uint32 table_hash_array_size);
extern status_t dict_boot();
extern status_t dict_create();

extern dict_table_t* dict_mem_table_create(const char* name, table_id_t  table_id,
    uint32 space_id, uint32 column_count, uint32 flags, uint32 flags2);
extern status_t dict_mem_table_add_col(dict_table_t* table, const char* name,
    data_type_t mtype, uint32 precision, uint32 scale_or_len);
extern dict_index_t* dict_mem_index_create(dict_table_t* table, const char* index_name,
    index_id_t index_id, uint32 space_id, uint32 type, uint32 field_count);
extern status_t dict_mem_index_add_field(dict_index_t* index, const char* name, uint32 prefix_len);

extern bool32 dict_add_table_to_cache(dict_table_t* table, bool32 can_be_evicted);
extern uint32 dict_get_table_from_cache_by_name(char* table_name, dict_table_t** table);
extern void dict_release_table(dict_table_t* table);
extern bool32 dict_remove_table_from_cache(char* table_name);

extern status_t dict_get_table(char* table_name, dict_table_t** table);
extern status_t dict_drop_table(char* table_name);
//------------------------------------------------------

/** the dictionary system */
extern dict_sys_t*    dict_sys;


#endif  /* _KNL_DICT_H */
