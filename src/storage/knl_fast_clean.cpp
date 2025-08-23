#include "knl_fast_clean.h"
#include "cm_memory.h"
#include "cm_log.h"

#define FAST_CLEAN_BLOCK_PAGE_COUNT        4

void fast_clean_mgr_t::init(memory_pool_t* pool)
{
    m_pool = pool;
    m_bucket_count = m_pool->page_size / sizeof(fast_clean_bucket_t*);
    m_clean_block_num = 0;
    m_clean_block_count_per_page = m_pool->page_size / sizeof(fast_clean_block_t);
    m_clean_block_max_count = m_clean_block_count_per_page * FAST_CLEAN_BLOCK_PAGE_COUNT;
    UT_LIST_INIT(m_used_pages);

    // bucket page
    memory_page_t* page = mpool_alloc_page(m_pool);
    if (page == NULL) {
        goto err_exit;
    }
    UT_LIST_ADD_LAST(list_node, m_used_pages, page);
    init_buckets();

    // fast_clean_block page
    for (uint32 i = 0; i < FAST_CLEAN_BLOCK_PAGE_COUNT; i++) {
        page = mpool_alloc_page(m_pool);
        if (page == NULL) {
            goto err_exit;
        }
        UT_LIST_ADD_LAST(list_node, m_used_pages, page);
    }

    return;

err_exit:

    destroy();
}

void fast_clean_mgr_t::clean()
{
    m_clean_block_num = 0;
    init_buckets();
}

void fast_clean_mgr_t::destroy()
{
    m_bucket_count = 0;
    m_clean_block_num = 0;
    m_clean_block_count_per_page = 0;
    m_clean_block_max_count = 0;

    memory_page_t* page = UT_LIST_GET_FIRST(m_used_pages);
    while (page) {
        UT_LIST_REMOVE(list_node, m_used_pages, page);
        mpool_free_page(m_pool, page);
        memory_page_t* page = UT_LIST_GET_FIRST(m_used_pages);
    }
}

void fast_clean_mgr_t::init_buckets()
{
    if (unlikely(m_bucket_count == 0)) {
        return NULL;
    }

    memory_page_t* page = UT_LIST_GET_FIRST(m_used_pages);
    fast_clean_bucket_t** bucket = (fast_clean_bucket_t *)MEM_PAGE_DATA_PTR(page);
    for (uint32 i = 0; i < m_bucket_count; i++) {
        bucket[i]->clean_block = NULL;
    }
}

fast_clean_bucket_t* fast_clean_mgr_t::get_bucket(uint32 space_id, uint32 page_no)
{
    if (unlikely(m_bucket_count == 0)) {
        return NULL;
    }

    memory_page_t* page = UT_LIST_GET_FIRST(m_used_pages);
    fast_clean_bucket_t* bucket = (fast_clean_bucket_t *)MEM_PAGE_DATA_PTR(page);
    return (bucket + page_no % m_bucket_count);
}

void fast_clean_mgr_t::insert_to_bucket(fast_clean_block_t* clean_block)
{
    fast_clean_bucket_t* bucket = get_bucket(clean_block->space_id, clean_block->page_no);
    if (unlikely(bucket == NULL)) {
        return NULL;
    }

    clean_block->next = bucket->clean_block;
    bucket->clean_block = clean_block;
}

fast_clean_block_t* fast_clean_mgr_t::find_clean_block(uint32 space_id, uint32 page_no)
{
    fast_clean_bucket_t* bucket = get_bucket(space_id, page_no);
    if (unlikely(bucket == NULL)) {
        return NULL;
    }

    fast_clean_block_t* clean_block = bucket->clean_block;
    while (clean_block) {
        if (clean_block->space_id == space_id && clean_block->page_no == page_no) {
            return clean_block;
        }
        clean_block = clean_block->next;
    }

    return NULL;
}

fast_clean_block_t* fast_clean_mgr_t::find_clean_block(uint32 index)
{
    if (index >= m_clean_block_num || m_bucket_count == 0) {
        return NULL;
    }

    memory_page_t* page = UT_LIST_GET_FIRST(m_used_pages);  // bucket page
    page = UT_LIST_GET_NEXT(list_node, page);  // first page of clean_block
    while (index >= m_clean_block_count_per_page) {
        index -= m_clean_block_count_per_page;
        page = UT_LIST_GET_NEXT(list_node, page);
    }

    return (fast_clean_block_t *)(MEM_PAGE_DATA_PTR(page) + sizeof(fast_clean_block_t) * index);
}

fast_clean_block_t* fast_clean_mgr_t::alloc_clean_block()
{
    fast_clean_block_t* clean_block;

    if (m_clean_block_num >= m_clean_block_max_count || m_bucket_count == 0) {
        return NULL;
    }

    uint32 num = m_clean_block_num;
    memory_page_t* page = UT_LIST_GET_FIRST(m_used_pages);  // bucket page
    page = UT_LIST_GET_NEXT(list_node, page);  // first page of clean_block
    while (num >= m_clean_block_count_per_page) {
        num -= m_clean_block_count_per_page;
        page = UT_LIST_GET_NEXT(list_node, page);
    }

    clean_block = (fast_clean_block_t *)(MEM_PAGE_DATA_PTR(page) + sizeof(fast_clean_block_t) * num);
    m_clean_block_num++;

    return clean_block;
}

void fast_clean_mgr_t::append_clean_block(uint32 space_id, uint32 page_no, void* block, uint8 itl_id)
{
    if (find_clean_block(space_id, page_no)) {
        return;
    }

    fast_clean_block_t* clean_block = alloc_clean_block();
    if (clean_block == NULL) {
        return;
    }

    clean_block->block = block;
    clean_block->space_id = space_id;
    clean_block->page_no = page_no;
    clean_block->itl_id = itl_id;
}

