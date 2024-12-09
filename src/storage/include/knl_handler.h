#ifndef _KNL_HANDLER_H
#define _KNL_HANDLER_H

#include "cm_type.h"
#include "cm_memory.h"
#include "cm_attribute.h"
#include "knl_session.h"
#include "knl_heap.h"
#include "knl_trx.h"

enum ha_rkey_function {
  HA_READ_KEY_EXACT,			/* Find first record else error */
  HA_READ_KEY_OR_NEXT,			/* Record or next record */
  HA_READ_KEY_OR_PREV,			/* Record or previous */
  HA_READ_AFTER_KEY,			/* Find next rec. after key-record */
  HA_READ_BEFORE_KEY,			/* Find next rec. before key-record */
  HA_READ_PREFIX,			/* Key which as same prefix */
  HA_READ_PREFIX_LAST			/* Last key with the same prefix */			
};

typedef struct st_ha_create_information
{
  ulong table_options;
  //enum db_type db_type;
  //enum row_type row_type;
  ulong avg_row_length;
  ulonglong max_rows,min_rows;
  ulonglong auto_increment_value;
  char *comment,*password;
  char *create_statement;
  uint options;					/* OR of HA_CREATE_ options */
  uint raid_type,raid_chunks;
  ulong raid_chunksize;
  bool32 if_not_exists;
  ulong used_fields;
  //SQL_LIST merge_list;
} HA_CREATE_INFO;




// Search direction for the interface
enum row_fetch_direction {
    FETCH_ROW_NEXT = 0, // ascending direction
    FETCH_ROW_PREV = 1  // descending direction
};

// Match mode for the interface
enum row_fetch_match_mode {
    // search using a complete key value
    FETCH_ROW_KEY_EXACT = 0,
    // search using a key prefix which must match rows:
    //     the prefix may contain an incomplete field
    //     (the last field in prefix may be just a prefix of a fixed length column)
    FETCH_ROW_EXACT_PREFIX = 2
};

// Strategy numbers for B-tree indexes
#define BTREE_FETCH_STRATEGY_LESS            1
#define BTREE_FETCH_STRATEGY_LESS_EQUAL      2
#define BTREE_FETCH_STRATEGY_EQUAL           3
#define BTREE_FETCH_STRATEGY_GREATER_EQUAL   4
#define BTREE_FETCH_STRATEGY_GREATER         5

#define BTREE_FETCH_STRATEGY_COUNT           5

typedef struct st_scan_key
{
    uint32    attr_no; // table or index column number
    uint32    strategy;
    Datum     argument; // data to compare
} scan_key_t;

class scan_cursor_t {
public:
    scan_cursor_t(memory_stack_context_t* context)
    {
        reset_memory_stack_context(context);
    }

    ~scan_cursor_t()
    {
        restore_mcontext_stack();
    }

    void reset_memory_stack_context(memory_stack_context_t* context)
    {
        restore_mcontext_stack();
        //
        mcontext_stack = context;
        mcontext_stack_save_ptr = mcontext_stack_save(mcontext_stack);
    }

    void restore_mcontext_stack()
    {
        if (mcontext_stack) {
            mcontext_stack_restore(mcontext_stack, mcontext_stack_save_ptr);
        }
    }


public:
    scan_key_t*     keys;
    uint32          key_count;
    dict_table_t*   table;
    dict_index_t*   index; // current index for a search, if any
    trx_t*          trx;   // current transaction handle

    insert_node_t*  insert_node;
    byte*           insert_upd_rec_buff;// buffer for storing data converted to the Innobase format from the MySQL format
    const byte*     default_rec; // the default values of all columns (a "default row") in MySQL format
    //update_node_t*  update_node; // SQL update node used to perform updates and deletes

    /*----------------------*/
    void*           idx_cond;  // In ICP, NULL if index condition pushdown is not used
    uint32          idx_cond_n_cols; // Number of fields in idx_cond_cols. 0 if and only if idx_cond == NULL.

    uint32          n_rows_fetched; // number of rows fetched after positioning the current cursor
    uint32          fetch_direction;// ROW_SEL_NEXT or ROW_SEL_PREV

    // a cache for fetched rows if we fetch many rows from the same cursor:
    //    it saves CPU time to fetch them in a batch;
#define FETCH_CACHE_SIZE     8
    byte*           fetch_cache[FETCH_CACHE_SIZE];
    uint32          fetch_cache_first; // position of the first not yet fetched row in fetch_cache
    uint32          n_fetch_cached; // number of not yet fetched rows in fetch_cache

    uint32          select_lock_type;// LOCK_NONE, LOCK_S, or LOCK_X

    void*           mcontext_stack_save_ptr;
    memory_stack_context_t* mcontext_stack;
};

inline void scan_key_init(scan_key_t* entry, uint32 attr_no, uint32 strategy, Datum argument)
{
    entry->attr_no = attr_no;
    entry->strategy = strategy;
    entry->argument = argument;
}

inline void scan_cursor_begin(scan_cursor_t* scan, dict_index_t* index, scan_key_t* keys, uint32 key_count)
{
    scan->index = index;
    scan->key_count = key_count;
    scan->keys = keys;
}

inline void scan_cursor_end(scan_cursor_t* scan)
{

}


class knl_handler//: public handler
{
private:
    scan_cursor_t*  cursor;

public:
   knl_handler()
   {
   }

   ~knl_handler() {}

    /*----------------------*/
    void initialize(void);
    int open(const char* table_name);
    int close(void);

    /*----------------------*/
    // Estimates calculation 
    double scan_time();
    // The cost of reading a set of ranges from the table using an index to access it
    double read_time();

    void savepoint(que_sess_t* sess, trx_savepoint_t* savepoint);
    void rollback(que_sess_t* sess, trx_savepoint_t* savepoint = NULL);
    void commit(que_sess_t* sess);


    /*----------------------*/
    status_t insert_row(que_sess_t* sess, scan_cursor_t* cursor);
    status_t update_row(que_sess_t* sess, scan_cursor_t* cursor);
    status_t delete_row(que_sess_t* sess, scan_cursor_t* cursor);
    int unlock_row(const byte* record);


    /*----------------------*/
    int index_init(uint32 index);
    int index_end();
    int index_read(byte * buf, const byte * key, uint key_len, enum ha_rkey_function find_flag);
    int index_read_idx(byte * buf, uint index, const byte * key, uint key_len, enum ha_rkey_function find_flag);

    int32 general_fetch(    scan_cursor_t* scan, byte* buf,
        row_fetch_direction direction, row_fetch_match_mode match_mode);
    int index_next(scan_cursor_t* scan, byte* buf);
    int32 index_prev(scan_cursor_t* scan, byte* buf);

    int32 index_first(scan_cursor_t* scan, byte* buf);
    int32 index_last(scan_cursor_t* scan, byte* buf);
    int32 index_fetch(scan_cursor_t* scan,
        byte* buf, // in/out: buffer for the row
        row_fetch_match_mode match_mode);

    status_t fetch_by_rowid(scan_cursor_t* scan, byte* buf, bool32* is_found);

    /*----------------------*/
    int create_table(const char* table_name, HA_CREATE_INFO* create_info);
    int delete_table(const char* table_name);
    int32 truncate_table(const char* table_name);
    int rename_table(const char* from, const char* to);
    char* update_table_comment(const char* comment);

};


extern status_t knl_server_init(char* base_dir, attribute_t* attr);
extern status_t knl_server_end();


#endif  /* _KNL_HANDLER_H */
