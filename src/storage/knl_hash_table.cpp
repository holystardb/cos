#include "knl_hash_table.h"
#include "cm_util.h"
#include "cm_dbug.h"

HASH_TABLE* HASH_TABLE_CREATE(uint32 n) /*!< in: number of array cells */
{
    HASH_TABLE *table;

    table = (HASH_TABLE*)malloc(sizeof(HASH_TABLE) + sizeof(HASH_CELL) * n);
    table->array = (HASH_CELL*)((char *)table + sizeof(HASH_TABLE));
    table->n_cells = n;

    table->magic_n = HASH_TABLE_MAGIC_N;

    return table;
}

void HASH_TABLE_FREE(HASH_TABLE *table)
{
    ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
    free(table);
}

HASH_CELL* hash_get_nth_cell(HASH_TABLE *table, uint32 n)
{
    ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
    ut_ad(n < table->n_cells);

    return(table->array + n);
}

uint32 hash_calc_hash(HASH_TABLE *table, uint32 fold)
{
    ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
#define UT_HASH_RANDOM_MASK2       1653893711
    fold = fold ^ UT_HASH_RANDOM_MASK2;
    return (fold % table->n_cells);
}

/** Gets the sync object index for a fold value in a hash table. */
uint32 hash_get_sync_obj_index(HASH_TABLE *table, uint32 fold)
{
    ut_ad(table);
    ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
    ut_ad(table->type != HASH_TABLE_SYNC_NONE);
    ut_ad(ut_is_2pow(table->n_sync_obj));
    return (ut_2pow_remainder(hash_calc_hash(table, fold), table->n_sync_obj));
}

/** Gets the nth mutex in a hash table. */
mutex_t *hash_get_nth_mutex(HASH_TABLE *table, uint32 i)
{
    ut_ad(table);
    ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
    ut_ad(table->type == HASH_TABLE_SYNC_MUTEX);
    ut_ad(i < table->n_sync_obj);

    return (table->sync_obj.mutexes + i);
}

/** Gets the mutex for a fold value in a hash table. */
mutex_t *hash_get_mutex(HASH_TABLE *table, uint32 fold)
{
    uint32 i;

    ut_ad(table);
    ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);

    i = hash_get_sync_obj_index(table, fold);

    return (hash_get_nth_mutex(table, i));
}

/** Gets the nth rw_lock in a hash table. */
rw_lock_t *hash_get_nth_lock(HASH_TABLE *table, uint32 i) /*!< in: index of the rw_lock */
{
    ut_ad(table);
    ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
    ut_ad(table->type == HASH_TABLE_SYNC_RW_LOCK);
    ut_ad(i < table->n_sync_obj);

    return (table->sync_obj.rw_locks + i);
}

/** Gets the rw_lock for a fold value in a hash table. */
rw_lock_t *hash_get_lock(HASH_TABLE *table, uint32 fold)
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

rw_lock_t *hash_lock_s_confirm(rw_lock_t *hash_lock, HASH_TABLE *table, uint32 fold)
{
    ut_ad(rw_lock_own(hash_lock, RW_LOCK_SHARED));

    rw_lock_t *hash_lock_tmp = hash_get_lock(table, fold);

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

rw_lock_t *hash_lock_x_confirm(rw_lock_t *hash_lock, HASH_TABLE *table, uint32 fold)
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

