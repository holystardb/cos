#include "cm_hashtable.h"
#include "cm_util.h"
#include "cm_dbug.h"

hash_table_t* hash_table_create(memory_pool_t *pool, uint32 cell_count, hash_calc_value calc_hash, hash_cmp cmp)
{
    hash_cell_t *cell;
    hash_table_t *table;

    table = (hash_table_t *)malloc(ut_align8(sizeof(hash_table_t)) + ut_align8(sizeof(hash_cell_t)) * cell_count);
    if (table) {
        table->calc_hash = calc_hash;
        table->cmp = cmp;

        table->array = (hash_cell_t *)((char *)table + ut_align8(sizeof(hash_table_t)));
        table->n_cells = cell_count;
        /* Initialize the cell array */
        for (uint32 i = 0; i < table->n_cells; i++) {
            cell = table->array + i;
            cell->cell_data = NULL;
        }

        table->pool = pool;
        for (uint32 i = 0; i < HASH_CELLS_LOCK_COUNT; i++) {
            table->cell_data_list[i] = NULL;
            spin_lock_init(&table->locks[i]);
            table->contexts[i] = mcontext_create(pool);
        }
    }

    return table;
}

void hash_table_destroy(hash_table_t *table)
{
    for (uint32 i = 0; i < HASH_CELLS_LOCK_COUNT; i++) {
        mcontext_destroy(table->contexts[i]);
    }

    free(table);
}

static hash_cell_t* hash_table_get_nth_cell(hash_table_t *table, uint32 n)
{
    ut_ad(n < table->n_cells);
    return(table->array + n);
}

static hash_cell_data_t* hash_table_get_free_cell_data(hash_table_t *table, uint32 cell_index)
{
    hash_cell_data_t *data = NULL;
    uint32 index;

    for (uint32 i = 0; i < HASH_CELLS_LOCK_COUNT; i++) {
        index = (cell_index + i) % HASH_CELLS_LOCK_COUNT;

        spin_lock(&table->locks[index], NULL);
        if (table->cell_data_list[index]) {
            data  = table->cell_data_list[index];
            table->cell_data_list[index] = data->next;
        }
        spin_unlock(&table->locks[index]);

        if (data) {
            break;
        }
    }

    if (data == NULL) {
        data = (hash_cell_data_t *)my_malloc(table->contexts[cell_index % HASH_CELLS_LOCK_COUNT],
            ut_align8(sizeof(hash_cell_data_t)));
    }

    return data;
}

static void hash_table_set_free_cell_data(hash_table_t *table, hash_cell_data_t *cell_data, uint32 cell_index)
{
    uint32 index = cell_index % HASH_CELLS_LOCK_COUNT;

    spin_lock(&table->locks[index], NULL);
    cell_data->next = table->cell_data_list[index];
    table->cell_data_list[index] = cell_data;
    spin_unlock(&table->locks[index]);
}

bool32 hash_table_insert(hash_table_t *table, void *data, uint32 data_len)
{
    hash_cell_data_t *cell_data;
    hash_cell_t  *cell;
    uint32 idx;

    idx = table->calc_hash(data, data_len) % table->n_cells;
    cell = hash_table_get_nth_cell(table, idx);

    cell_data = hash_table_get_free_cell_data(table, idx);
    cell_data->data = data;
    cell_data->next = NULL;

    spin_lock(&table->locks[idx % HASH_CELLS_LOCK_COUNT], NULL);
    if (cell->cell_data == NULL) {
        cell->cell_data = cell_data;
    } else {
        cell_data->next = cell->cell_data;
        cell->cell_data = cell_data;
    }
    spin_unlock(&table->locks[idx % HASH_CELLS_LOCK_COUNT]);

    return TRUE;
}

bool32 hash_table_delete(hash_table_t *table, void *key, uint32 key_len)
{
    hash_cell_data_t *cell_data;
    hash_cell_t  *cell;
    uint32 idx;

    idx = table->calc_hash(key, key_len) % table->n_cells;
    cell = hash_table_get_nth_cell(table, idx);

    spin_lock(&table->locks[idx % HASH_CELLS_LOCK_COUNT], NULL);
    cell_data = cell->cell_data;
    while (cell_data) {
        if (table->cmp(cell_data->data, key, key_len) == 0) {
            if (cell_data == cell->cell_data) {
                cell->cell_data = cell_data->next;
            } else {
                cell_data->next = cell_data->next;
            }
            break;
        }
        cell_data = cell_data->next;
    }
    spin_unlock(&table->locks[idx % HASH_CELLS_LOCK_COUNT]);

    if (cell_data) {
        hash_table_set_free_cell_data(table, cell_data, idx);
        return TRUE;
    }

    return FALSE;
}

void* hash_table_search(hash_table_t *table, void *key, uint32 key_len)
{
    hash_cell_data_t *cell_data;
    hash_cell_t  *cell;
    uint32 idx;
    void *data = NULL;

    idx = table->calc_hash(key, key_len) % table->n_cells;
    cell = hash_table_get_nth_cell(table, idx);

    spin_lock(&table->locks[idx % HASH_CELLS_LOCK_COUNT], NULL);
    cell_data = cell->cell_data;
    while (cell_data) {
        if (table->cmp(cell_data->data, key, key_len) == 0) {
            data = cell_data->data;
            break;
        }
        cell_data = cell_data->next;
    }
    spin_unlock(&table->locks[idx % HASH_CELLS_LOCK_COUNT]);

    return data;
}

