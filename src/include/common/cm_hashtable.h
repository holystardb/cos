#ifndef _CM_HASH_TABLE_H
#define _CM_HASH_TABLE_H

#include "cm_type.h"
#include "cm_memory.h"

#define HASH_CELLS_LOCK_COUNT     16

typedef uint32 (*hash_calc_value)(void *key, uint32 key_len);
typedef int (*hash_cmp)(void *data, void *key, uint32 key_len);

typedef struct st_hash_cell_data{
    struct st_hash_cell_data *next;
    void *data;  /* data for current cell */
} hash_cell_data_t;

typedef struct st_hash_cell{
    hash_cell_data_t *cell_data;
} hash_cell_t;

/* The hash table structure */
typedef struct st_hash_table {
    hash_cell_t       *array; /* pointer to cell array */
    uint32             n_cells; /* number of cells in the hash table */

    hash_cell_data_t  *cell_data_list[HASH_CELLS_LOCK_COUNT];
    spinlock_t         locks[HASH_CELLS_LOCK_COUNT]; /* NULL, or an array of mutexes used to protect segments of the hash table */
    memory_context_t  *contexts[HASH_CELLS_LOCK_COUNT]; /* if this is non-NULL, hash chain nodes for external chaining
                                    can be allocated from these memory heaps; there are then n_mutexes many of these heaps */
    memory_pool_t     *pool;

    hash_calc_value    calc_hash;
    hash_cmp           cmp;
} hash_table_t;

hash_table_t* hash_table_create(memory_pool_t *pool, uint32 cell_count, hash_calc_value calc_hash, hash_cmp cmp);
void hash_table_destroy(hash_table_t *table);

bool32 hash_table_insert(hash_table_t *table, void *data, uint32 data_len);
bool32 hash_table_delete(hash_table_t *table, void *key, uint32 key_len);
void* hash_table_search(hash_table_t *table, void *key, uint32 key_len);



#endif  /* _CM_HASH_TABLE_H */
