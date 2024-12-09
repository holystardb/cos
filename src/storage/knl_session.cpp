#include "knl_session.h"

#include "cm_memory.h"
#include "cm_log.h"

session_pool_t*   g_sess_pool = NULL;
attribute_t       g_attribute;



status_t sess_pool_create(uint32 sess_count, uint32 session_stack_size)
{
    g_sess_pool = (session_pool_t *)ut_malloc_zero(sizeof(session_pool_t));
    g_sess_pool->sessions = (que_sess_t *)ut_malloc_zero(sizeof(que_sess_t) * sess_count);
    UT_LIST_INIT(g_sess_pool->used_sess_list);
    UT_LIST_INIT(g_sess_pool->free_sess_list);
    mutex_create(&g_sess_pool->mutex);

    for (uint32 i = 0; i < sess_count; i++) {
        que_sess_t* sess = g_sess_pool->sessions + i;
        sess->id = i;
        sess->attr = &g_attribute;
        sess->mem_page_size = session_stack_size;
        sess->is_free = TRUE;

        UT_LIST_ADD_LAST(list_node, g_sess_pool->free_sess_list, sess);
    }

    return CM_SUCCESS;
}

void sess_pool_destroy()
{
    que_sess_t* sess;

    mutex_enter(&g_sess_pool->mutex);
    sess = UT_LIST_GET_FIRST(g_sess_pool->used_sess_list);
    while (sess) {
        marea_free_page(srv_memory_sga, sess->mem_page, sess->mem_page_size);
        UT_LIST_REMOVE(list_node, g_sess_pool->used_sess_list, sess);
        UT_LIST_ADD_LAST(list_node, g_sess_pool->free_sess_list, sess);
        sess = UT_LIST_GET_FIRST(g_sess_pool->used_sess_list);
    }
    mutex_exit(&g_sess_pool->mutex);

    mutex_destroy(&g_sess_pool->mutex);
    ut_free(g_sess_pool->sessions);
    ut_free(g_sess_pool);
    g_sess_pool = NULL;
}

que_sess_t* que_sess_alloc()
{
    que_sess_t* sess;

    mutex_enter(&g_sess_pool->mutex);
    sess = UT_LIST_GET_FIRST(g_sess_pool->free_sess_list);
    if (sess) {
        UT_LIST_REMOVE(list_node, g_sess_pool->free_sess_list, sess);
    }
    mutex_exit(&g_sess_pool->mutex);

    if (sess == NULL) {
        return NULL;
    }

    sess->mem_page = marea_alloc_page(srv_memory_sga, sess->mem_page_size);
    if (sess->mem_page == NULL) {
        mutex_enter(&g_sess_pool->mutex);
        UT_LIST_ADD_LAST(list_node, g_sess_pool->free_sess_list, sess);
        mutex_exit(&g_sess_pool->mutex);

        CM_THROW_ERROR(ERR_ALLOC_MEMORY, (uint64)sess->mem_page_size, "alloc session");
        return NULL;
    }

    sess->mcontext_stack = mcontext_stack_create(srv_common_mpool);
    if (sess->mcontext_stack == NULL) {
        mutex_enter(&g_sess_pool->mutex);
        UT_LIST_ADD_LAST(list_node, g_sess_pool->free_sess_list, sess);
        mutex_exit(&g_sess_pool->mutex);
        marea_free_page(srv_memory_sga, sess->mem_page, sess->mem_page_size);
        return NULL;
    }

    mutex_enter(&g_sess_pool->mutex);
    sess->is_free = FALSE;
    UT_LIST_ADD_LAST(list_node, g_sess_pool->used_sess_list, sess);
    mutex_exit(&g_sess_pool->mutex);

    char* stack_buf = MEM_PAGE_DATA_PTR(sess->mem_page);
    cm_stack_init(&sess->stack, stack_buf, sess->mem_page_size);

    return sess;
}

void que_sess_free(que_sess_t* sess)
{
    ut_a(!sess->is_free);

    marea_free_page(srv_memory_sga, sess->mem_page, sess->mem_page_size);
    sess->mem_page = NULL;

    mcontext_stack_destroy(sess->mcontext_stack);
    sess->mcontext_stack = NULL;

    mutex_enter(&g_sess_pool->mutex);
    sess->is_free = TRUE;
    UT_LIST_REMOVE(list_node, g_sess_pool->used_sess_list, sess);
    UT_LIST_ADD_LAST(list_node, g_sess_pool->free_sess_list, sess);
    mutex_exit(&g_sess_pool->mutex);

    sess->stack.buf = NULL;
}

inline void sess_append_fast_clean_page_list(que_sess_t* sess, buf_block_t* block, uint8 itl_id)
{
    if (sess->fast_clean_pages == NULL) {
        return;
    }

    fast_clean_page_hdr_t* clean_page_hdr = (fast_clean_page_hdr_t *)MEM_PAGE_DATA_PTR(sess->fast_clean_pages);
    uint32 clean_page_count = mach_read_from_2(clean_page_hdr);
    if (clean_page_count >= FAST_CLEAN_PAGE_COUNT_PER_PAGE) {
        return;
    }

    fast_clean_page_t* clean_page = clean_page_hdr + FAST_CLEAN_PAGE_HEADER_SIZE + clean_page_count * FAST_CLEAN_PAGE_SIZE;
    mach_write_to_4(clean_page, block->get_space_id());
    mach_write_to_4(clean_page, block->get_page_no());
    mach_write_to_1(clean_page, itl_id);
    mach_write_to_8(clean_page, PointerGetDatum(block));
    mach_write_to_2(clean_page_hdr, clean_page_count + 1);
}

