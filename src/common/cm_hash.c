#include "cm_hash.h"
#include "cm_util.h"

hash_table_t* hash_table_create(memory_pool_t *pool, uint32 cell_count, hash_calc_value calc_hash, hash_cmp cmp)
{
    hash_cell_t *cell;
    hash_table_t *table;

    table = (hash_table_t *)malloc(ut_align8(sizeof(hash_table_t)) + ut_align8(sizeof(hash_cell_t *)) * cell_count);
    if (table) {
        table->calc_hash = calc_hash;
        table->cmp = cmp;

        table->array = (hash_cell_t **)((char *)table + ut_align8(sizeof(hash_table_t)));
        table->n_cells = cell_count;
        /* Initialize the cell array */
        for (uint32 i = 0; i < table->n_cells; i++) {
            cell = table->array[i];
            cell->next = NULL;
        }

        table->pool = pool;
        for (uint32 i = 0; i < HASH_CELL_MUTEX_COUNT; i++) {
            M_INIT_SPIN_LOCK(table->locks[i]);
            table->contexts[i] = mcontext_create(pool);
        }
    }

    return table;
}

void hash_table_destroy(hash_table_t *table)
{
    for (uint32 i = 0; i < HASH_CELL_MUTEX_COUNT; i++) {
        mcontext_destroy(table->contexts[i]);
    }

    free(table);
}

bool32 hash_insert(hash_table_t *table, void *data, uint32 data_len)
{
    hash_cell_t *cell;
    uint32 val, idx;

    val = table->calc_hash(data, data_len) % table->n_cells;
    idx = val % HASH_CELL_MUTEX_COUNT;

    cell = mcontext_alloc(table->contexts[idx], sizeof(hash_cell_t));
    if (!cell) {
        return FALSE;
    }
    cell->data = data;
    cell->next = NULL;

    spin_lock(&table->locks[idx], NULL);
    if (table->array + val != NULL) {
        cell->next = table->array[val];
    }
    table->array[val] = cell;
    spin_unlock(&table->locks[idx]);

    return TRUE;
}

bool32 hash_delete(hash_table_t *table, void *key, uint32 key_len)
{
    bool32 ret = FALSE;
    hash_cell_t *cell, *prev = NULL;
    uint32 val, idx;

    val = table->calc_hash(key, key_len) % table->n_cells;
    idx = val % HASH_CELL_MUTEX_COUNT;

    spin_lock(&table->locks[idx], NULL);
    cell = table->array[val];
    while (cell) {
        if (table->cmp(cell, key, key_len) == 0) {
            if (prev) {
                prev->next = cell->next;
            } else {
                table->array[val] = cell->next;
            }
            mcontext_free(table->contexts[idx], cell);
            ret = TRUE;
            break;
        }
        prev = cell;
        cell = cell->next;
    }
    spin_unlock(&table->locks[idx]);

    return ret;
}

void* hash_search(hash_table_t *table, void *key, uint32 key_len)
{
    hash_cell_t *cell;
    uint32 val, idx;
    void *data = NULL;

    val = table->calc_hash(key, key_len) % table->n_cells;
    idx = val % HASH_CELL_MUTEX_COUNT;

    spin_lock(&table->locks[idx], NULL);
    cell = table->array[val];
    while (cell) {
        if (table->cmp(cell, key, key_len) == 0) {
            data = cell->data;
            break;
        }
        cell = cell->next;
    }
    spin_unlock(&table->locks[idx]);

    return data;
}

