#include "cm_hashtable.h"
#include "cm_util.h"
#include "cm_dbug.h"
#include "cm_error.h"

hash_table_t* hash_table_create(memory_pool_t* pool, uint32 cell_count, hash_calc_value calc_hash, hash_cmp cmp)
{
    hash_cell_t* cell;
    hash_table_t* table;

    table = (hash_table_t *)ut_malloc_zero(ut_align8(sizeof(hash_table_t)) + ut_align8(sizeof(hash_cell_t)) * cell_count);
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

        table->alloc_in_process = FALSE;
        table->next_start_cell_index = 0;
        table->pool = pool;
        UT_LIST_INIT(table->used_pages);
        for (uint32 i = 0; i < HASH_CELLS_LOCK_COUNT; i++) {
            table->cell_data_list[i] = NULL;
            mutex_create(&table->cell_data_list_mutexs[i]);
        }
    }

    return table;
}

void hash_table_destroy(hash_table_t* table)
{
    memory_page_t* page;

    mutex_enter(&table->mutex, NULL);
    page = UT_LIST_GET_FIRST(table->used_pages);
    while (page) {
        UT_LIST_REMOVE(list_node, table->used_pages, page);
        mpool_free_page(table->pool, page);
        page = UT_LIST_GET_FIRST(table->used_pages);
    }
    mutex_exit(&table->mutex);

    ut_free(table);
}

static hash_cell_t* hash_table_get_nth_cell(hash_table_t* table, uint32 n)
{
    ut_ad(n < table->n_cells);
    return(table->array + n);
}

static hash_cell_data_t* hash_table_alloc_cell_data(hash_table_t* table, uint32 cell_index, bool32 need_mutex)
{
    hash_cell_data_t* cell_data = NULL;
    uint32 index;

retry:

    for (uint32 i = 0; i < HASH_CELLS_LOCK_COUNT; i++) {
        index = (cell_index + i) % HASH_CELLS_LOCK_COUNT;

        //
        if (need_mutex || i != cell_index) {
            mutex_enter(&table->cell_data_list_mutexs[index], NULL);
        }

        if (table->cell_data_list[index]) {
            cell_data  = table->cell_data_list[index];
            table->cell_data_list[index] = cell_data->next;
        }

        if (need_mutex || i != cell_index) {
            mutex_exit(&table->cell_data_list_mutexs[index]);
        }

        if (cell_data) {
            break;
        }
    }

    if (cell_data == NULL) {
        //
        mutex_enter(&table->mutex, NULL);
        if (table->alloc_in_process) {
            mutex_exit(&table->mutex);
            os_thread_sleep(100);
            goto retry;
        }
        table->alloc_in_process = TRUE;
        mutex_exit(&table->mutex);

        //
        memory_page_t* page = mpool_alloc_page(table->pool);
        if (page == NULL) {
            mutex_enter(&table->mutex, NULL);
            table->alloc_in_process = FALSE;
            mutex_exit(&table->mutex);
            return NULL;
        }

        mutex_enter(&table->mutex, NULL);
        UT_LIST_ADD_LAST(list_node, table->used_pages, page);
        mutex_exit(&table->mutex);

        hash_cell_data_t* base_cell_data = (hash_cell_data_t *)MEM_PAGE_DATA_PTR(page);
        uint32 cell_data_count = table->pool->page_size / ut_align8(sizeof(hash_cell_data_t));
        uint32 data_count_per_cell = cell_data_count / HASH_CELLS_LOCK_COUNT;
        ut_a(data_count_per_cell > 0);

        for (uint32 i = 0; i < HASH_CELLS_LOCK_COUNT; i++) {
            mutex_enter(&table->cell_data_list_mutexs[table->next_start_cell_index], NULL);
            for (uint32 j = 0; j < data_count_per_cell; j++) {
                base_cell_data->next = table->cell_data_list[table->next_start_cell_index];
                table->cell_data_list[table->next_start_cell_index] = base_cell_data;
                //
                base_cell_data++;
            }
            mutex_exit(&table->cell_data_list_mutexs[table->next_start_cell_index]);
            table->next_start_cell_index = (table->next_start_cell_index++) % HASH_CELLS_LOCK_COUNT;
        }
        // remaining part
        mutex_enter(&table->cell_data_list_mutexs[table->next_start_cell_index], NULL);
        for (uint32 i = data_count_per_cell * HASH_CELLS_LOCK_COUNT; i < cell_data_count; i++) {
            base_cell_data->next = table->cell_data_list[table->next_start_cell_index];
            table->cell_data_list[table->next_start_cell_index] = base_cell_data;
            //
            base_cell_data++;
        }
        mutex_exit(&table->cell_data_list_mutexs[table->next_start_cell_index]);
        table->next_start_cell_index = (table->next_start_cell_index++) % HASH_CELLS_LOCK_COUNT;

        mutex_enter(&table->mutex, NULL);
        table->alloc_in_process = FALSE;
        mutex_exit(&table->mutex);

        goto retry;
    }

    return cell_data;
}

static void hash_table_free_cell_data(hash_table_t* table, hash_cell_data_t* cell_data, uint32 cell_index)
{
    uint32 index = cell_index % HASH_CELLS_LOCK_COUNT;

    mutex_enter(&table->cell_data_list_mutexs[index], NULL);
    cell_data->next = table->cell_data_list[index];
    table->cell_data_list[index] = cell_data;
    mutex_exit(&table->cell_data_list_mutexs[index]);
}

static hash_cell_data_t* hash_table_search_low(hash_table_t* table, hash_cell_t* cell, void* key, uint32 key_len)
{
    hash_cell_data_t* cell_data;

    cell_data = cell->cell_data;
    while (cell_data) {
        if (table->cmp(cell_data->data, cell_data->data_len, key, key_len) == 0) {
            break;
        }
        cell_data = cell_data->next;
    }

    return cell_data;
}


status_t hash_table_insert(hash_table_t* table, void* data, uint32 data_len)
{
    hash_cell_data_t *cell_data;
    hash_cell_t  *cell;
    uint32 cell_index;

    cell_index = table->calc_hash(data, data_len) % table->n_cells;
    cell = hash_table_get_nth_cell(table, cell_index);

    //
    cell_data = hash_table_alloc_cell_data(table, cell_index, TRUE);
    if (cell_data == NULL) {
        CM_SET_ERROR(ERR_ALLOC_MEMORY, table->pool->page_size, "cannot alloc cell_data for hash_table_insert");
        return CM_ERROR;
    }
    cell_data->data = data;
    cell_data->data_len = data_len;
    cell_data->next = NULL;

    mutex_enter(&table->cell_data_list_mutexs[cell_index % HASH_CELLS_LOCK_COUNT], NULL);

    if (hash_table_search_low(table, cell, data, data_len)) {
        mutex_exit(&table->cell_data_list_mutexs[cell_index % HASH_CELLS_LOCK_COUNT]);
        //
        hash_table_free_cell_data(table, cell_data, cell_index);

        CM_SET_ERROR(ERR_DUPLICATE_KEY, "the same KEY already exists for hash_table_insert");
        return CM_ERROR;
    }

    if (cell->cell_data == NULL) {
        cell->cell_data = cell_data;
    } else {
        cell_data->next = cell->cell_data;
        cell->cell_data = cell_data;
    }
    mutex_exit(&table->cell_data_list_mutexs[cell_index % HASH_CELLS_LOCK_COUNT]);

    return CM_SUCCESS;
}

status_t hash_table_delete(hash_table_t* table, void* key, uint32 key_len)
{
    hash_cell_data_t* cell_data;
    hash_cell_t* cell;
    uint32 cell_index;

    cell_index = table->calc_hash(key, key_len) % table->n_cells;
    cell = hash_table_get_nth_cell(table, cell_index);

    mutex_enter(&table->cell_data_list_mutexs[cell_index % HASH_CELLS_LOCK_COUNT], NULL);

    cell_data = cell->cell_data;
    while (cell_data) {
        if (table->cmp(cell_data->data, cell_data->data_len, key, key_len) == 0) {
            if (cell_data == cell->cell_data) {
                cell->cell_data = cell_data->next;
            } else {
                cell_data->next = cell_data->next;
            }
            break;
        }
        cell_data = cell_data->next;
    }

    mutex_exit(&table->cell_data_list_mutexs[cell_index % HASH_CELLS_LOCK_COUNT]);

    if (cell_data) {
        hash_table_free_cell_data(table, cell_data, cell_index);
    }

    return CM_SUCCESS;
}

void* hash_table_search(hash_table_t* table, void* key, uint32 key_len)
{
    hash_cell_data_t* cell_data;
    hash_cell_t* cell;
    uint32 cell_index;
    void* data = NULL;

    cell_index = table->calc_hash(key, key_len) % table->n_cells;
    cell = hash_table_get_nth_cell(table, cell_index);

    mutex_enter(&table->cell_data_list_mutexs[cell_index % HASH_CELLS_LOCK_COUNT], NULL);

    cell_data = hash_table_search_low(table, cell, key, key_len);
    if (cell_data) {
        data = cell_data->data;
    }

    mutex_exit(&table->cell_data_list_mutexs[cell_index % HASH_CELLS_LOCK_COUNT]);

    return data;
}

void* hash_table_search_or_insert(hash_table_t* table, void* key, uint32 key_len)
{
    hash_cell_data_t* cell_data;
    hash_cell_t* cell;
    uint32 cell_index;
    void *data = NULL;

    cell_index = table->calc_hash(key, key_len) % table->n_cells;
    cell = hash_table_get_nth_cell(table, cell_index);

    mutex_enter(&table->cell_data_list_mutexs[cell_index % HASH_CELLS_LOCK_COUNT], NULL);

    cell_data = hash_table_search_low(table, cell, key, key_len);
    if (cell_data) {
        data = cell_data->data;
    } else {
        cell_data = hash_table_alloc_cell_data(table, cell_index, FALSE);
        if (cell_data == NULL) {
            mutex_exit(&table->cell_data_list_mutexs[cell_index % HASH_CELLS_LOCK_COUNT]);
            CM_SET_ERROR(ERR_ALLOC_MEMORY, table->pool->page_size, "cannot alloc cell_data for hash_table_insert");
            return NULL;
        }

        cell_data->data = key;
        cell_data->data_len = key_len;
        cell_data->next = NULL;

        if (cell->cell_data == NULL) {
            cell->cell_data = cell_data;
        } else {
            cell_data->next = cell->cell_data;
            cell->cell_data = cell_data;
        }
        data = key;
    }

    mutex_exit(&table->cell_data_list_mutexs[cell_index % HASH_CELLS_LOCK_COUNT]);

    return data;
}


