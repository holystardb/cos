#include "knl_hash_table.h"
#include "cm_util.h"
#include "cm_dbug.h"

HASH_TABLE* HASH_TABLE_CREATE(uint32 hash_cell_count,
    enum hash_table_sync_t type, // in: HASH_TABLE_SYNC_MUTEX or HASH_TABLE_SYNC_RW_LOCK
    uint32 n_sync_obj) // in: number of sync objects, must be a power of 2
{
    ut_a(n_sync_obj > 0);
    ut_a(ut_is_2pow(n_sync_obj));

    HASH_TABLE* table;

    table = (HASH_TABLE*)ut_malloc_zero(sizeof(HASH_TABLE) + sizeof(HASH_CELL_T) * hash_cell_count);
    if (table) {
        table->array = (HASH_CELL_T*)((char *)table + sizeof(HASH_TABLE));
        table->n_cells = hash_cell_count;
        table->magic_n = HASH_TABLE_MAGIC_N;
        table->n_sync_obj = n_sync_obj;
        table->type = type;

        switch (type) {
        case HASH_TABLE_SYNC_MUTEX:
            table->sync_obj.mutexes = (mutex_t*)ut_malloc_zero(n_sync_obj * sizeof(mutex_t));
            for (uint32 i = 0; i < n_sync_obj; i++) {
                mutex_create(table->sync_obj.mutexes + i);
            }
            break;

        case HASH_TABLE_SYNC_RW_LOCK:
            table->sync_obj.rw_locks = (rw_lock_t*)ut_malloc_zero(n_sync_obj * sizeof(rw_lock_t));
            for (uint32 i = 0; i < n_sync_obj; i++) {
                rw_lock_create(table->sync_obj.rw_locks + i);
            }
            break;

        case HASH_TABLE_SYNC_NONE:
            ut_error;
        }
    }

    return table;
}

void HASH_TABLE_FREE(HASH_TABLE* table)
{
    ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
    ut_free(table);
}

HASH_CELL_T* HASH_GET_NTH_CELL(HASH_TABLE* table, uint32 n)
{
    ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
    ut_ad(n < table->n_cells);

    return (table->array + n);
}

uint32 HASH_CALC_HASH(HASH_TABLE* table, uint32 fold)
{
    ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
#define UT_HASH_RANDOM_MASK2       1653893711
    fold = fold ^ UT_HASH_RANDOM_MASK2;
    return (fold % table->n_cells);
}

/** Gets the sync object index for a fold value in a hash table. */
uint32 hash_get_sync_obj_index(HASH_TABLE* table, uint32 fold)
{
    ut_ad(table);
    ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
    ut_ad(table->type != HASH_TABLE_SYNC_NONE);
    ut_ad(ut_is_2pow(table->n_sync_obj));
    return (ut_2pow_remainder(HASH_CALC_HASH(table, fold), table->n_sync_obj));
}

/** Gets the nth mutex in a hash table. */
mutex_t* hash_get_nth_mutex(HASH_TABLE* table, uint32 i)
{
    ut_ad(table);
    ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
    ut_ad(table->type == HASH_TABLE_SYNC_MUTEX);
    ut_ad(i < table->n_sync_obj);

    return (table->sync_obj.mutexes + i);
}

/** Gets the mutex for a fold value in a hash table. */
mutex_t* HASH_GET_MUTEX(HASH_TABLE* table, uint32 fold)
{
    uint32 i;

    ut_ad(table);
    ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);

    i = hash_get_sync_obj_index(table, fold);

    return (hash_get_nth_mutex(table, i));
}

/** Gets the nth rw_lock in a hash table. */
rw_lock_t* hash_get_nth_lock(HASH_TABLE* table, uint32 i) /*!< in: index of the rw_lock */
{
    ut_ad(table);
    ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
    ut_ad(table->type == HASH_TABLE_SYNC_RW_LOCK);
    ut_ad(i < table->n_sync_obj);

    return (table->sync_obj.rw_locks + i);
}

/** Gets the rw_lock for a fold value in a hash table. */
rw_lock_t* hash_get_lock(HASH_TABLE* table, uint32 fold)
{
    uint32 i;

    ut_ad(table);
    ut_ad(table->type == HASH_TABLE_SYNC_RW_LOCK);
    ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);

    i = hash_get_sync_obj_index(table, fold);

    return (hash_get_nth_lock(table, i));
}

/** If not appropriate rw_lock for a fold value in a hash table,
relock S-lock the another rw_lock until appropriate for a fold value.
@param[in]	hash_lock	latched rw_lock to be confirmed
@param[in]	table		hash table
@param[in]	fold		fold value
@return	latched rw_lock */

rw_lock_t* hash_lock_s_confirm(rw_lock_t* hash_lock, HASH_TABLE* table, uint32 fold)
{
    ut_ad(rw_lock_own(hash_lock, RW_LOCK_SHARED));

    rw_lock_t* hash_lock_tmp = hash_get_lock(table, fold);

    while (hash_lock_tmp != hash_lock) {
        rw_lock_s_unlock(hash_lock);
        hash_lock = hash_lock_tmp;
        rw_lock_s_lock(hash_lock);
        hash_lock_tmp = hash_get_lock(table, fold);
    }

    return (hash_lock);
}

/** If not appropriate rw_lock for a fold value in a hash table,
relock X-lock the another rw_lock until appropriate for a fold value.
@param[in]	hash_lock	latched rw_lock to be confirmed
@param[in]	table		hash table
@param[in]	fold		fold value
@return	latched rw_lock */

rw_lock_t* hash_lock_x_confirm(rw_lock_t* hash_lock, HASH_TABLE* table, uint32 fold)
{
    ut_ad(rw_lock_own(hash_lock, RW_LOCK_WAIT_EXCLUSIVE));

    rw_lock_t *hash_lock_tmp = hash_get_lock(table, fold);

    while (hash_lock_tmp != hash_lock) {
        rw_lock_x_unlock(hash_lock);
        hash_lock = hash_lock_tmp;
        rw_lock_x_lock(hash_lock);
        hash_lock_tmp = hash_get_lock(table, fold);
    }

    return (hash_lock);
}

