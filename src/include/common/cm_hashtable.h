#ifndef _CM_HASH_TABLE_H
#define _CM_HASH_TABLE_H

#include "cm_type.h"
#include "cm_memory.h"

#define HASH_CELLS_LOCK_COUNT           64

#define HASH_TABLE_MEM              1

#define HASH_TABLE_DUPLICATE_KEY              1
#define HASH_TABLE_KEY_NOT_FOUND              1

typedef uint32 (*hash_calc_value)(void* key, uint32 key_len);
typedef int (*hash_cmp)(void* data, uint32 data_len, void* key, uint32 key_len);

typedef struct st_hash_cell_data{
    struct st_hash_cell_data* next;
    void* data;  /* data for current cell */
    uint32 data_len;
} hash_cell_data_t;

typedef struct st_hash_cell{
    hash_cell_data_t *cell_data;
} hash_cell_t;

/* The hash table structure */
typedef struct st_hash_table {
    hash_cell_t*       array; // pointer to cell array
    uint32             n_cells; // number of cells in the hash table
    mutex_t            mutex;
    bool32             alloc_in_process;
    uint32             next_start_cell_index;

    hash_cell_data_t*  cell_data_list[HASH_CELLS_LOCK_COUNT];
    mutex_t            cell_data_list_mutexs[HASH_CELLS_LOCK_COUNT];

    memory_pool_t*     pool;
    UT_LIST_BASE_NODE_T(memory_page_t) used_pages; // proected by mutex

    hash_calc_value    calc_hash;
    hash_cmp           cmp;
} hash_table_t;

hash_table_t* hash_table_create(memory_pool_t* pool, uint32 cell_count, hash_calc_value calc_hash, hash_cmp cmp);
void hash_table_destroy(hash_table_t* table);

status_t hash_table_insert(hash_table_t* table, void* data, uint32 data_len);
status_t hash_table_delete(hash_table_t* table, void* key, uint32 key_len);
void* hash_table_search(hash_table_t* table, void* key, uint32 key_len);
void* hash_table_search_or_insert(hash_table_t* table, void* key, uint32 key_len);



#endif  /* _CM_HASH_TABLE_H */
