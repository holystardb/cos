#include "cm_hashtable.h"
#include "cm_util.h"
#include "cm_dbug.h"
#include "cm_error.h"

hash_table_t* hash_table_create(    uint32 bucket_count, hash_ctrl_t* info)
{
    hash_cell_t* cell;
    hash_table_t* table;

    table = (hash_table_t *)ut_malloc_zero(sizeof(hash_table_t) + sizeof(hash_bucket_t) * bucket_count);
    if (table) {
        table->calc = info->calc;
        table->cmp = info->cmp;
        table->alloc = info->alloc;
        table->key_size = info->key_size;
        table->entry_size = info->entry_size;
        table->pool = info->pool;
        table->bucket_count = bucket_count;
        table->buckets = (hash_bucket_t *)((char *)table + sizeof(hash_table_t));

        /* Initialize the bucket array */
        for (uint32 i = 0; i < bucket_count; i++) {
            table->buckets[i].element = NULL;
            mutex_create(&table->buckets[i].mutex);
        }

        table->memory_page.init(table->pool);
        table->free_elements.init(&table->memory_page);
    }

    return table;
}

void hash_table_destroy(hash_table_t* table)
{
    table->free_elements.destroy();
    table->memory_page.destroy();
    ut_free(table);
}

static inline hash_bucket_t* hash_table_get_nth_bucket(hash_table_t* table, uint32 nth)
{
    ut_ad(nth < table->bucket_count);
    return(table->buckets + nth);
}

inline status_t hash_table_insert(hash_table_t* table, void* key, void* data)
{
    status_t ret = CM_SUCCESS;
    uint32 hash_value = table->calc(key, table->key_size);
    uint32 bucket_index = hash_value % table->bucket_count;

    hash_element_t* element = table->free_elements->alloc_element(bucket_index);
    if (element == NULL) {
        return ERR_ALLOC_MEMORY;
    }
    element->key = key;
    element->data = data;
    element->hash_value = hash_value;
    element->owner_list_id = bucket_index;

    hash_bucket_t* bucket = hash_table_get_nth_bucket(table, bucket_index);

    mutex_enter(&bucket->mutex, NULL);

    // search element
    hash_element_t* tmp = bucket->element;
    while (tmp) {
        if (tmp->hash_value == hash_value && table->cmp(tmp->key, key, table->key_size) == 0) {
            // found
            ret = ERR_HASHTABLE_DUPLICATE_KEY;
            break;
        }
        tmp = tmp->next;
    }

    // insert into bucket
    if (tmp == NULL) {
        element->next = bucket->element;
        bucket->element = element;
    }

    mutex_exit(&bucket->mutex);

    return ret;
}

inline void* hash_table_delete(hash_table_t* table, void* key, bool32* is_found)
{
    uint32 hash_value = table->calc(key, table->key_size);
    uint32 bucket_index = hash_value % table->bucket_count;
    hash_bucket_t* bucket = hash_table_get_nth_bucket(table, bucket_index);
    hash_element_t *element, *prev_element = NULL;
    void* data = NULL;

    mutex_enter(&bucket->mutex, NULL);

    // search element
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
        table->free_elements->free_element(element);
    }

    mutex_exit(&bucket->mutex);

    return data;
}

inline void* hash_table_search(hash_table_t* table, const void* key, bool32* is_found)
{
    uint32 hash_value = table->calc(key, table->key_size);
    uint32 bucket_index = hash_value % table->bucket_count;
    hash_bucket_t* bucket = hash_table_get_nth_bucket(table, bucket_index);
    void* data = NULL;

    mutex_enter(&bucket->mutex, NULL);

    // search element
    hash_element_t* element = bucket->element;
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

    data = element ? element->data : NULL;

    mutex_exit(&bucket->mutex);

    return data;
}

void hash_memory_page_t::init(memory_pool_t* pool)
{
    m_pool = pool;
    UT_LIST_INIT(m_pages);
    mutex_create(&m_mutex);
}

void hash_memory_page_t::destroy()
{
    memory_page_t* page;

    mutex_enter(&m_mutex, NULL);
    page = UT_LIST_GET_FIRST(m_pages);
    while (page) {
        UT_LIST_REMOVE(list_node, m_pages, page);
        mpool_free_page(m_pool, page);
        page = UT_LIST_GET_FIRST(m_pages);
    }
    mutex_exit(&m_mutex);

    mutex_destroy(&m_mutex);
}

memory_page_t* hash_memory_page_t::alloc_page()
{
    memory_page_t* page = mpool_alloc_page(m_pool);
    if (unlikely(page == NULL)) {
        CM_SET_ERROR(ERR_ALLOC_MEMORY, m_pool->page_size, "cannot alloc cell_data for hash_table_insert");
        return NULL;
    }
    
    mutex_enter(&m_mutex, NULL);
    UT_LIST_ADD_LAST(list_node, m_pages, page);
    mutex_exit(&m_mutex);

    return page;
}

void hash_free_elements_t::init(hash_memory_page_t* hash_memory_page)
{
    for (uint32 i = 0; i < HASH_FREE_ELEMENT_LIST_COUNT; i++) {
        mutex_create(&m_mutex[i]);
        m_free_element_list[i] = NULL;
    }
}

void hash_free_elements_t::destory()
{
    for (uint32 i = 0; i < HASH_FREE_ELEMENT_LIST_COUNT; i++) {
        mutex_destroy(&m_mutex[i]);
    }
}

hash_element_t* hash_free_elements_t::alloc_element(uint32 hash_value)
{
    hash_element_t* element;
    uint32 free_list_id = hash_value % HASH_FREE_ELEMENT_LIST_COUNT;

retry:

    mutex_enter(&m_mutex[free_list_id], NULL);
    element = m_free_element_list[free_list_id];
    if (likely(element)) {
        m_free_element_list[free_list_id] = element->next;
    }
    mutex_exit(&m_mutex[free_list_id]);

    if (unlikely(element == NULL)) {
        mutex_enter(&m_mutex[free_list_id], NULL);
        if (m_alloc_page_in_process) {
            mutex_exit(&m_mutex[free_list_id]);
            os_thread_sleep(20);
            goto retry;
        }
        m_alloc_page_in_process = TRUE;
        mutex_exit(&m_mutex[free_list_id]);

        memory_page_t* page = m_hash_memory_page->alloc_page();
        if (unlikely(page == NULL)) {
            mutex_enter(&m_mutex[free_list_id], NULL);
            m_alloc_page_in_process = FALSE;
            mutex_exit(&m_mutex[free_list_id]);
            return NULL;
        }

        uint32 element_count = m_hash_memory_page->get_page_size() / sizeof(hash_element_t);
        mutex_enter(&m_mutex[free_list_id], NULL);
        for (uint32 i = 0; i < element_count; i++) {
            element = (hash_element_t *)(MEM_PAGE_DATA_PTR(page) + sizeof(hash_element_t) * i);
            element->owner_list_id = free_list_id;
            element->next = m_free_element_list[free_list_id];
            m_free_element_list[free_list_id] = element;
        }
        m_alloc_page_in_process = FALSE;
        mutex_exit(&m_mutex[free_list_id]);
        goto retry;
    }

    return element;
}

void hash_free_elements_t::free_element(hash_element_t* element)
{
    uint32 free_list_id = element->owner_list_id;
    mutex_enter(&m_mutex[free_list_id], NULL);
    element->next = m_free_element_list[free_list_id];
    m_free_element_list[free_list_id] = element;
    mutex_exit(&m_mutex[free_list_id]);
}


