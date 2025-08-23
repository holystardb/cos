#ifndef _CM_HASH_TABLE_H
#define _CM_HASH_TABLE_H

#include "cm_type.h"
#include "cm_memory.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef uint32 (*hash_calc_value_func)(void* key, uint32 key_len);
typedef int32 (*hash_compare_func)(void* key1, void* key2, uint32 key_len);
typedef void *(*hash_alloc_func)(memory_pool_t* pool, uint32 size);

/* Parameter data structure for hash_create */
typedef struct st_hash_ctrl
{
    uint32               key_size;    /* hash key length in bytes */
    memory_pool_t*       pool;
    hash_calc_value_func calc;
    hash_compare_func    cmp;
    hash_alloc_func      alloc;
} hash_ctrl_t;

typedef struct st_hash_element {
    struct st_hash_element* next;
    void*  key;
    void*  data;
    uint32 hash_value;
    uint32 owner_list_id;
} hash_element_t;

typedef struct st_hash_bucket{
    hash_element_t* element;
    mutex_t         mutex;
} hash_bucket_t;

class hash_memory_page_t {
public:
    hash_memory_page_t() {    }
    ~hash_memory_page_t() {    }

    void init(memory_pool_t* pool);
    void destroy();
    memory_page_t* alloc_page();
    uint32 get_page_size() {
        return m_pool->page_size;
    }

private:
    mutex_t            m_mutex;
    memory_pool_t*     m_pool;
    UT_LIST_BASE_NODE_T(memory_page_t) m_pages;
};

#define HASH_FREE_ELEMENT_LIST_COUNT    64

class hash_free_elements_t {
public:
    hash_free_elements_t() {}
    ~hash_free_elements_t() {}

    void init(hash_memory_page_t* hash_memory_page);
    void destroy();
    hash_element_t* alloc_element(uint32 bucket_index);
    void free_element(hash_element_t* element);

private:
    hash_element_t*     m_free_element_list[HASH_FREE_ELEMENT_LIST_COUNT];
    mutex_t             m_mutex[HASH_FREE_ELEMENT_LIST_COUNT];
    hash_memory_page_t* m_hash_memory_page = NULL;
    bool32              m_alloc_page_in_process = FALSE;
};

/* The hash table structure */
typedef struct st_hash_table {
    hash_bucket_t*       buckets;
    uint32               bucket_count;
    uint32               key_size;    /* hash key length in bytes */
    memory_pool_t*       pool;
    hash_free_elements_t free_elements;
    hash_memory_page_t   memory_page;

    hash_calc_value_func calc;
    hash_compare_func    cmp;
    hash_alloc_func      alloc;
} hash_table_t;

extern hash_table_t* hash_table_create(uint32 bucket_count, hash_ctrl_t* info);
extern void hash_table_destroy(hash_table_t* table);

extern inline status_t hash_table_insert(hash_table_t* table, void* key, void* data);
extern inline void* hash_table_delete(hash_table_t* table, void* key, bool32* is_found);
extern inline void* hash_table_search(hash_table_t* table, const void* key, bool32* is_found);

#ifdef __cplusplus
}
#endif

#endif  /* _CM_HASH_TABLE_H */
