#ifndef _CM_HASH_TABLE_H
#define _CM_HASH_TABLE_H

#include "cm_type.h"
#include "cm_memory.h"
#include "cm_rwlock.h"

#ifdef __cplusplus
extern "C" {
#endif

#define HASH_TABLE_FREE_ELEMENT_LIST_COUNT    64    // must be power of 2
#define HASH_TABLE_FILL_FACTOR_LIST_COUNT     64    // must be power of 2
#define HASH_TABLE_BUCKET_RWLOCK_COUNT        1024  // must be power of 2

typedef uint32 (*hash_table_calc_func)(const void* key, uint32 key_len);

// < 0: key1 < key2
// = 0: key1 = key2
// > 0: key1 > key2
typedef int32 (*hash_table_compare_func)(const void* key1, const void* key2, uint32 key_len);



/* Parameter data structure for hash_create */
struct hash_table_ctrl_t {
    uint32                  key_size {0};
    bool32                  enable_expand_bucket {0};
    uint32                  factor {0};
    memory_pool_t*          pool {NULL};
    hash_table_calc_func    calc {NULL};
    hash_table_compare_func cmp {NULL};
};

typedef struct st_hash_table_element {
    struct st_hash_table_element* next;
    void*  key;
    void*  data;
    uint32 hash_value;
    uint32 owner_list_id;
} hash_table_element_t;

typedef struct st_hash_table_bucket {
    hash_table_element_t* element;
} hash_table_bucket_t;

typedef struct st_hash_table_segment {
    hash_table_bucket_t* buckets;
} hash_table_segment_t;

/* The hash table structure */
typedef struct st_hash_table {
    memory_pool_t*          pool;
    bool32                  is_initialized {FALSE};
    uint32                  bucket_count;
    uint32                  key_size;
    bool32                  enable_expand_bucket;

    uint32                  high_mask; /* mask to modulo into entire table */
    uint32                  low_mask;  /* mask to modulo into lower half of table */
    uint32                  segment_count_per_page;
    uint32                  bucket_count_per_segment;
    uint32                  rwlock_count_per_page;
    uint32                  segment_count;  // segment count with alloced bucket
    uint32                  segment_size;   // segment total count
    uint32                  segment_shift;  // = log2(bucket_count_per_segment)

    rw_lock_t               rw_lock;
    bool32                  alloc_page_in_process;
    uint32                  expansion_count;
    uint32                  expansion_success_count;
    uint32                  fill_factor;
    atomic32_t              fill_factor_list[HASH_TABLE_FILL_FACTOR_LIST_COUNT];

    hash_table_element_t*   free_element_list[HASH_TABLE_FREE_ELEMENT_LIST_COUNT];
    mutex_t                 free_element_mutex[HASH_TABLE_FREE_ELEMENT_LIST_COUNT];

    hash_table_calc_func    calc;
    hash_table_compare_func cmp;

    UT_LIST_BASE_NODE_T(memory_page_t) segment_pages;
    UT_LIST_BASE_NODE_T(memory_page_t) bucket_pages;
    UT_LIST_BASE_NODE_T(memory_page_t) element_pages;
    UT_LIST_BASE_NODE_T(memory_page_t) rwlock_pages;
} hash_table_t;

extern status_t hash_table_create(hash_table_t* table, uint32 bucket_count, hash_table_ctrl_t* info);
extern void hash_table_destroy(hash_table_t* table);

extern inline status_t hash_table_insert(hash_table_t* table, void* key, void* data);

// Only delete the mapping relationship within HASHTABLE,
// the memory of key and data needs to be freed by the caller themselves.
extern inline void* hash_table_delete(hash_table_t* table, void* key, bool32* is_found);

extern inline void* hash_table_search(hash_table_t* table, const void* key, bool32* is_found);

#ifdef __cplusplus
}
#endif

#endif  /* _CM_HASH_TABLE_H */
