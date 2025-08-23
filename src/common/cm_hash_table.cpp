#include "cm_hash_table.h"
#include "cm_util.h"
#include "cm_dbug.h"
#include "cm_error.h"

#define HASH_TABLE_EXPAND_BUCKET_COUNT   4
#define HASH_TABLE_DEFAULT_FACTOR        4

static inline status_t hash_table_expand_segment_if_need(hash_table_t* table, uint32 segment_index)
{
    ut_ad(rw_lock_own(&table->rw_lock, RW_LOCK_EXCLUSIVE) || rw_lock_own(&table->rw_lock, RW_LOCK_SHARED));

    if (likely(segment_index < table->segment_size)) {
        return CM_SUCCESS;
    }

    ut_ad(rw_lock_own(&table->rw_lock, RW_LOCK_EXCLUSIVE));
    while (segment_index >= table->segment_size) {
        memory_page_t* page = mpool_alloc_page(table->pool);
        if (page == NULL) {
            return ERR_ALLOC_MEMORY;
        }

        hash_table_segment_t* segment;
        for (uint32 i = 0; i < table->segment_count_per_page; i++) {
            segment = (hash_table_segment_t*)(MEM_PAGE_DATA_PTR(page) + sizeof(hash_table_segment_t) * i);
            segment->buckets = NULL;
        }
        UT_LIST_ADD_LAST(list_node, table->segment_pages, page);
        table->segment_size += table->segment_count_per_page;
    }

    return CM_SUCCESS;
}

static inline hash_table_segment_t* hash_table_get_segment(hash_table_t* table, uint32 segment_index)
{
    ut_ad(rw_lock_own(&table->rw_lock, RW_LOCK_EXCLUSIVE) || rw_lock_own(&table->rw_lock, RW_LOCK_SHARED));

    // expand segment
    hash_table_expand_segment_if_need(table, segment_index);

    // get segment
    memory_page_t* page = UT_LIST_GET_FIRST(table->segment_pages);
    while (page && segment_index >= table->segment_count_per_page) {
        segment_index -= table->segment_count_per_page;
        page = UT_LIST_GET_NEXT(list_node, page);
    }
    if (page == NULL) {
        return NULL;
    }

    hash_table_segment_t* segment;
    segment = (hash_table_segment_t *)(MEM_PAGE_DATA_PTR(page) + sizeof(hash_table_segment_t) * segment_index);
    if (segment->buckets == NULL) {
        // alloc bucket
        page = mpool_alloc_page(table->pool);
        if (page == NULL) {
            return NULL;
        }
        UT_LIST_ADD_LAST(list_node, table->bucket_pages, page);
        segment->buckets = (hash_table_bucket_t *)MEM_PAGE_DATA_PTR(page);
        for (uint32 i = 0; i < table->bucket_count_per_segment; i++) {
            segment->buckets[i].element = NULL;
        }
        table->segment_count++;
    }

    return segment;
}

static inline hash_table_bucket_t* hash_table_get_bucket(hash_table_t* table, uint32 bucket_index)
{
    ut_ad(rw_lock_own(&table->rw_lock, RW_LOCK_EXCLUSIVE) || rw_lock_own(&table->rw_lock, RW_LOCK_SHARED));

    uint32 segment_index = bucket_index >> table->segment_shift;
    uint32 segment_bucket_index = ut_2pow_remainder(bucket_index, table->segment_size);

    hash_table_segment_t* seg = hash_table_get_segment(table, segment_index);
    if (seg == NULL) {
        return NULL;
    }

    ut_ad(seg->buckets);
    return &seg->buckets[segment_bucket_index];
}

/* Convert a hash value to a bucket number */
static inline uint32 hash_table_calc_bucket(hash_table_t* table, uint32 hash_value)
{
    uint32 bucket_index;

    ut_ad(rw_lock_own(&table->rw_lock, RW_LOCK_EXCLUSIVE) || rw_lock_own(&table->rw_lock, RW_LOCK_SHARED));

    bucket_index = hash_value & table->high_mask;
    if (bucket_index > table->bucket_count) {
        bucket_index = bucket_index & table->low_mask;
    }

    return bucket_index;
}

static inline rw_lock_t* hash_table_get_bucket_rwlock(hash_table_t* table, uint32 bucket_index)
{
    uint32 rwlock_index = bucket_index & (HASH_TABLE_BUCKET_RWLOCK_COUNT - 1);
    uint32 rwlock_offset = rwlock_index % table->rwlock_count_per_page;
    uint32 rwlock_page_index = rwlock_index / table->rwlock_count_per_page;

    // get segment
    memory_page_t* page = UT_LIST_GET_FIRST(table->rwlock_pages);
    while (page && rwlock_page_index > 0) {
        page = UT_LIST_GET_NEXT(list_node, page);
        rwlock_page_index--;
    }
    if (page == NULL) {
        return NULL;
    }

    return (rw_lock_t *)(MEM_PAGE_DATA_PTR(page) + sizeof(rw_lock_t) * rwlock_offset);
}

static inline bool32 hash_table_is_need_expand(hash_table_t* table)
{
    if (!table->enable_expand_bucket) {
        return FALSE;
    }

    uint32 sum = atomic32_get(&table->fill_factor_list[0]) * HASH_TABLE_FILL_FACTOR_LIST_COUNT;
    return (sum >= table->bucket_count * table->fill_factor) ? TRUE : FALSE;
}

static inline void hash_table_expand_bucket_low(hash_table_t* table)
{
    ut_ad(rw_lock_own(&table->rw_lock, RW_LOCK_EXCLUSIVE));

    table->expansion_count++;

    uint32 new_bucket_index = table->bucket_count + 1;
    /* OK, we created a new bucket */
    table->bucket_count++;

    /*
     * *Before* changing masks, find old bucket corresponding to same hash
     * values; values in that bucket may need to be relocated to new bucket.
     * Note that new_bucket is certainly larger than low_mask at this point,
     * so we can skip the first step of the regular hash mask calc.
     */
    uint32 old_bucket_index = (new_bucket_index & table->low_mask);

    // If we crossed a power of 2, readjust masks.
    if (new_bucket_index > table->high_mask) {
        table->low_mask = table->high_mask;
        table->high_mask = (uint32) new_bucket_index | table->low_mask;
    }

    hash_table_bucket_t* old_bucket = hash_table_get_bucket(table, old_bucket_index);
    hash_table_bucket_t* new_bucket = hash_table_get_bucket(table, new_bucket_index);
    rw_lock_t* old_bucket_rwlock = hash_table_get_bucket_rwlock(table, old_bucket_index);
    rw_lock_t* new_bucket_rwlock = hash_table_get_bucket_rwlock(table, new_bucket_index);
    rw_lock_x_lock(old_bucket_rwlock);
    if (likely(old_bucket_rwlock != new_bucket_rwlock)) {
        rw_lock_x_lock(new_bucket_rwlock);
    }

    // clear old bucket
    hash_table_element_t *curr_element, *next_element;
    curr_element = old_bucket->element;
    old_bucket->element = NULL;

    // for elements of old bucket, add it to new bucket or old bucket
    for (; curr_element != NULL; curr_element = next_element) {
        next_element = curr_element->next;
        if (hash_table_calc_bucket(table, curr_element->hash_value) == old_bucket_index) {
            curr_element->next = old_bucket->element;
            old_bucket->element = curr_element;
        } else {
            curr_element->next = new_bucket->element;
            new_bucket->element = curr_element;
        }
    }

    rw_lock_x_unlock(old_bucket_rwlock);
    if (likely(old_bucket_rwlock != new_bucket_rwlock)) {
        rw_lock_x_unlock(new_bucket_rwlock);
    }
    table->expansion_success_count++;
}

static inline void hash_table_expand_bucket(hash_table_t* table)
{
    for (uint32 i = 0; i < HASH_TABLE_EXPAND_BUCKET_COUNT; i++) {
        hash_table_expand_bucket_low(table);
    }
}

static inline hash_table_element_t* hash_table_alloc_element(hash_table_t* table, uint32 hash_value)
{
    hash_table_element_t* element;
    uint32 free_list_id = hash_value & (HASH_TABLE_FREE_ELEMENT_LIST_COUNT - 1);

retry:

    mutex_enter(&table->free_element_mutex[free_list_id], NULL);
    element = table->free_element_list[free_list_id];
    if (likely(element != NULL)) {
        table->free_element_list[free_list_id] = element->next;
    }
    mutex_exit(&table->free_element_mutex[free_list_id]);

    if (unlikely(element == NULL)) {
        mutex_enter(&table->free_element_mutex[free_list_id], NULL);
        if (table->alloc_page_in_process) {
            mutex_exit(&table->free_element_mutex[free_list_id]);
            os_thread_sleep(20);
            goto retry;
        }
        table->alloc_page_in_process = TRUE;
        mutex_exit(&table->free_element_mutex[free_list_id]);

        memory_page_t* page = mpool_alloc_page(table->pool);
        if (unlikely(page == NULL)) {
            mutex_enter(&table->free_element_mutex[free_list_id], NULL);
            table->alloc_page_in_process = FALSE;
            mutex_exit(&table->free_element_mutex[free_list_id]);
            return NULL;
        }

        uint32 element_count = table->pool->page_size / sizeof(hash_table_element_t);
        mutex_enter(&table->free_element_mutex[free_list_id], NULL);
        for (uint32 i = 0; i < element_count; i++) {
            element = (hash_table_element_t *)(MEM_PAGE_DATA_PTR(page) + sizeof(hash_table_element_t) * i);
            element->owner_list_id = free_list_id;
            element->next = table->free_element_list[free_list_id];
            table->free_element_list[free_list_id] = element;
        }
        table->alloc_page_in_process = FALSE;
        mutex_exit(&table->free_element_mutex[free_list_id]);
        goto retry;
    }

    return element;
}

static inline void hash_table_free_element(hash_table_t* table, hash_table_element_t* element)
{
    uint32 free_list_id = element->owner_list_id;
    mutex_enter(&table->free_element_mutex[free_list_id], NULL);
    element->next = table->free_element_list[free_list_id];
    table->free_element_list[free_list_id] = element;
    mutex_exit(&table->free_element_mutex[free_list_id]);
}

inline status_t hash_table_insert(hash_table_t* table, void* key, void* data)
{
    uint32 hash_value;
    bool32 is_need_expand;
    rw_lock_t* bucket_rwlock;
    hash_table_bucket_t* bucket;
    uint32 bucket_index;
    hash_table_element_t* element;

    ut_ad(table->is_initialized);

    hash_value = table->calc(key, table->key_size);
    element = hash_table_alloc_element(table, hash_value);
    if (element == NULL) {
        return ERR_ALLOC_MEMORY;
    }
    element->key = key;
    element->data = data;
    element->hash_value = hash_value;

    // get bucket
    is_need_expand = hash_table_is_need_expand(table);
    if (is_need_expand) {
        rw_lock_x_lock(&table->rw_lock);
        if (hash_table_is_need_expand(table)) {
            hash_table_expand_bucket(table);
        } else {
            rw_lock_x_unlock(&table->rw_lock);
            is_need_expand = FALSE;
            rw_lock_s_lock(&table->rw_lock);
        }
    } else {
        rw_lock_s_lock(&table->rw_lock);
    }
    bucket_index = hash_table_calc_bucket(table, hash_value);
    bucket = hash_table_get_bucket(table, bucket_index);
    bucket_rwlock = hash_table_get_bucket_rwlock(table, bucket_index);
    rw_lock_x_lock(bucket_rwlock);
    if (is_need_expand) {
        rw_lock_x_unlock(&table->rw_lock);
    } else {
        rw_lock_s_unlock(&table->rw_lock);
    }

    // search element
    hash_table_element_t* tmp = bucket->element;
    while (tmp) {
        if (tmp->hash_value == hash_value && table->cmp(tmp->key, key, table->key_size) == 0) {
            rw_lock_x_unlock(bucket_rwlock);
            // found
            return ERR_HASHTABLE_DUPLICATE_KEY;
        }
        tmp = tmp->next;
    }

    // insert into bucket
    if (tmp == NULL) {
        element->next = bucket->element;
        bucket->element = element;
    }

    rw_lock_x_unlock(bucket_rwlock);

    atomic32_inc(&table->fill_factor_list[hash_value & (HASH_TABLE_FILL_FACTOR_LIST_COUNT-1)]);

    return CM_SUCCESS;
}

inline void* hash_table_delete(hash_table_t* table, void* key, bool32* is_found)
{
    hash_table_element_t *element, *prev_element = NULL;
    uint32 hash_value;
    rw_lock_t* bucket_rwlock;
    hash_table_bucket_t* bucket;
    uint32 bucket_index;
    void* data = NULL;

    ut_ad(table->is_initialized);

    // get bucket
    hash_value = table->calc(key, table->key_size);
    rw_lock_s_lock(&table->rw_lock);
    bucket_index = hash_table_calc_bucket(table, hash_value);
    bucket = hash_table_get_bucket(table, bucket_index);
    bucket_rwlock = hash_table_get_bucket_rwlock(table, bucket_index);
    rw_lock_x_lock(bucket_rwlock);
    rw_lock_s_unlock(&table->rw_lock);

    // search element and delete from bucket
    element = bucket->element;
    while (element) {
        if (element->hash_value == hash_value && table->cmp(element->data, key, table->key_size) == 0) {
            break;
        }
        prev_element = element;
        element = element->next;
    }

    if (is_found) {
        *is_found = (element != NULL);
    }

    if (element) {
        if (prev_element) {
            prev_element->next = element->next;
        } else {
            bucket->element = element->next;
        }
        data = element->data;
        //
        hash_table_free_element(table, element);
        atomic32_dec(&table->fill_factor_list[hash_value & (HASH_TABLE_FILL_FACTOR_LIST_COUNT-1)]);
    }

    rw_lock_x_unlock(bucket_rwlock);

    return data;
}

inline void* hash_table_search(hash_table_t* table, const void* key, bool32* is_found)
{
    uint32 hash_value;
    rw_lock_t* bucket_rwlock;
    hash_table_bucket_t* bucket;
    uint32 bucket_index;
    void* data = NULL;

    ut_ad(table->is_initialized);

    // get bucket
    hash_value = table->calc(key, table->key_size);
    rw_lock_s_lock(&table->rw_lock);
    bucket_index = hash_table_calc_bucket(table, hash_value);
    bucket = hash_table_get_bucket(table, bucket_index);
    bucket_rwlock = hash_table_get_bucket_rwlock(table, bucket_index);
    rw_lock_s_lock(bucket_rwlock);
    rw_lock_s_unlock(&table->rw_lock);

    // search element from bucket
    hash_table_element_t* element = bucket->element;
    while (element) {
        if (element->hash_value == hash_value && table->cmp(element->key, key, table->key_size) == 0) {
            break;
        }
        element = element->next;
    }

    if (is_found) {
        *is_found = (element != NULL);
    }
    data = element ? element->data : NULL;

    rw_lock_s_unlock(bucket_rwlock);

    return data;
}

status_t hash_table_create(hash_table_t* table, uint32 bucket_count, hash_table_ctrl_t* info)
{
    bucket_count = (uint32)ut_2_power_up(bucket_count);

    table->expansion_count = 0;
    table->expansion_success_count = 0;
    table->fill_factor = info->factor == 0 ? HASH_TABLE_DEFAULT_FACTOR : info->factor;
    for (uint32 i = 0; i < HASH_TABLE_FILL_FACTOR_LIST_COUNT; i++) {
        table->fill_factor_list[i] = 0;
    }

    table->calc = info->calc;
    table->cmp = info->cmp;
    table->key_size = info->key_size;
    table->enable_expand_bucket = info->enable_expand_bucket;
    table->pool = info->pool;
    table->bucket_count = bucket_count;
    table->low_mask = table->bucket_count - 1;
    table->high_mask = (table->bucket_count << 1) - 1;

    /* Initialize the segment array */
    table->segment_count = 0;
    table->segment_size = 0;
    table->segment_count_per_page = table->pool->page_size / sizeof(hash_table_segment_t);
    table->segment_shift = ut_2_log(table->pool->page_size / sizeof(hash_table_bucket_t));
    table->bucket_count_per_segment = ut_2_exp(table->segment_shift);
    UT_LIST_INIT(table->segment_pages);

    /* Initialize the bucket array of segment */
    rw_lock_create(&table->rw_lock);
    UT_LIST_INIT(table->bucket_pages);

    bool32 is_exist_error = FALSE;
    uint32 max_segment_index = (bucket_count - 1) >> table->segment_shift;
    rw_lock_x_lock(&table->rw_lock);
    for (uint32 segment_index = 0; segment_index <= max_segment_index; segment_index++) {
        if (hash_table_get_segment(table, segment_index) == NULL) {
            is_exist_error = TRUE;
            break;
        }
    }
    rw_lock_x_unlock(&table->rw_lock);
    if (is_exist_error) {
        return CM_ERROR;
    }

    /* Initialize the element freelist */
    table->alloc_page_in_process = FALSE;
    UT_LIST_INIT(table->element_pages);
    for (uint32 i = 0; i < HASH_TABLE_FREE_ELEMENT_LIST_COUNT; i++) {
        mutex_create(&table->free_element_mutex[i]);
        table->free_element_list[i] = NULL;
    }

    /* Initialize the bucket rwlock */
    UT_LIST_INIT(table->rwlock_pages);
    table->rwlock_count_per_page = table->pool->page_size / sizeof(rw_lock_t);
    uint32 rwlock_count = 0;
    while (rwlock_count < HASH_TABLE_BUCKET_RWLOCK_COUNT) {
        memory_page_t* page = mpool_alloc_page(table->pool);
        if (page == NULL) {
            return CM_ERROR;
        }
        UT_LIST_ADD_LAST(list_node, table->rwlock_pages, page);
        for (uint32 i = 0; i < table->rwlock_count_per_page; i++) {
            rw_lock_t* rw_lock = (rw_lock_t*)(MEM_PAGE_DATA_PTR(page) + sizeof(rw_lock_t) * i);
            rw_lock_create(rw_lock);
        }
        rwlock_count += table->rwlock_count_per_page;
    }

    table->is_initialized = TRUE;

    return CM_SUCCESS;
}

void hash_table_destroy(hash_table_t* table)
{
    memory_page_t* page;
    rw_lock_t* rw_lock;

    ut_ad(table->is_initialized);

    page = UT_LIST_GET_FIRST(table->segment_pages);
    while (page) {
        UT_LIST_REMOVE(list_node, table->segment_pages, page);
        mpool_free_page(table->pool, page);
        page = UT_LIST_GET_FIRST(table->segment_pages);
    }

    page = UT_LIST_GET_FIRST(table->bucket_pages);
    while (page) {
        UT_LIST_REMOVE(list_node, table->bucket_pages, page);
        mpool_free_page(table->pool, page);
        page = UT_LIST_GET_FIRST(table->bucket_pages);
    }

    page = UT_LIST_GET_FIRST(table->element_pages);
    while (page) {
        UT_LIST_REMOVE(list_node, table->element_pages, page);
        mpool_free_page(table->pool, page);
        page = UT_LIST_GET_FIRST(table->element_pages);
    }

    for (uint32 i = 0; i < HASH_TABLE_FREE_ELEMENT_LIST_COUNT; i++) {
        mutex_destroy(&table->free_element_mutex[i]);
        table->free_element_list[i] = NULL;
    }

    page = UT_LIST_GET_FIRST(table->rwlock_pages);
    while (page) {
        for (uint32 i = 0; i < table->rwlock_count_per_page; i++) {
            rw_lock = (rw_lock_t*)(MEM_PAGE_DATA_PTR(page) + sizeof(rw_lock_t) * i);
            rw_lock_destroy(rw_lock);
        }
        UT_LIST_REMOVE(list_node, table->rwlock_pages, page);
        mpool_free_page(table->pool, page);
        page = UT_LIST_GET_FIRST(table->rwlock_pages);
    }

    rw_lock_destroy(&table->rw_lock);

    table->is_initialized = FALSE;
}

