#ifndef _KNL_FAST_CLEAN_H
#define _KNL_FAST_CLEAN_H

#include "cm_type.h"
#include "cm_list.h"
#include "cm_memory.h"

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

struct alignas(4) fast_clean_block_t {
    fast_clean_block_t* next;
    void*  block;
    uint32 space_id;
    uint32 page_no;
    uint32 itl_id: 8;
    uint32 reserved: 24;
};

struct fast_clean_bucket_t {
    fast_clean_block_t* clean_block;
};

class fast_clean_mgr_t {
public:
    fast_clean_mgr_t() {
        UT_LIST_INIT(m_used_pages);
    }

    void init(memory_pool_t* pool);
    void clean();
    void append_clean_block(uint32 space_id, uint32 page_no, void* block, uint8 itl_id);
    fast_clean_block_t* find_clean_block(uint32 index);

    uint32 get_clean_block_count() {
        return m_clean_block_num;
    }

private:
    fast_clean_block_t* alloc_clean_block();
    fast_clean_block_t* find_clean_block(uint32 space_id, uint32 page_no);
    void insert_to_bucket(fast_clean_block_t* clean_block);

    void init_buckets();
    fast_clean_bucket_t* get_bucket(uint32 space_id, uint32 page_no);

private:
    memory_pool_t* m_pool{NULL};
    uint32 m_bucket_count{0};
    uint32 m_clean_block_num{0};
    uint32 m_clean_block_max_count{0};
    uint32 m_clean_block_count_per_page{0};
    UT_LIST_BASE_NODE_T(memory_pool_t) m_used_pages;
};


#ifdef __cplusplus
}
#endif // __cplusplus

#endif  /* _KNL_FAST_CLEAN_H */
