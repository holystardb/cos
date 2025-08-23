#include "knl_session.h"

#include "cm_memory.h"
#include "cm_log.h"

session_pool_t*   g_sess_pool = NULL;
attribute_t       g_attribute;



status_t sess_pool_create(uint32 sess_count, uint32 session_stack_size)
{
    g_sess_pool = new session_pool_t();
    if (g_sess_pool == NULL) {
        return ERR_ALLOC_MEMORY;
    }

    g_sess_pool->init(sess_count, session_stack_size, &g_attribute);

    return CM_SUCCESS;
}

void sess_pool_destroy()
{
    delete g_sess_pool;
    g_sess_pool = NULL;
}

que_sess_t* que_sess_alloc()
{
    que_sess_t* sess = g_sess_pool->alloc_session();
    if (sess && sess->init() != CM_SUCCESS) {
        sess->clean();
        g_sess_pool->free_session(sess);
        return NULL;
    }

    return sess;
}

void que_sess_free(que_sess_t* sess)
{
    sess->clean();
    g_sess_pool->free_session(sess);
}

session_pool_t::session_pool_t()
{
    UT_LIST_INIT(used_sess_list);
    UT_LIST_INIT(free_sess_list);
    mutex_create(&mutex);
}

session_pool_t::~session_pool_t()
{
    ut_a(UT_LIST_GET_LEN(used_sess_list) == 0);

    que_sess_t* sess;

    mutex_enter(&mutex);
    sess = UT_LIST_GET_FIRST(free_sess_list);
    while (sess) {
        //sess->destroy();
        sess = UT_LIST_GET_NEXT(list_node, sess);
    }
    UT_LIST_INIT(free_sess_list);
    mutex_exit(&mutex);

    mutex_destroy(&mutex);

    ut_free(sessions);
}

status_t session_pool_t::init(uint32 sess_count, uint32 stack_size, attribute_t* attr)
{
    sessions = (que_sess_t *)ut_malloc_zero(sizeof(que_sess_t) * sess_count);
    if (sessions == NULL) {
        return ERR_ALLOC_MEMORY;
    }

    for (uint32 i = 0; i < sess_count; i++) {
        que_sess_t* sess = sessions + i;
        sess->sess_id = i;
        sess->attr = attr;
        sess->mem_page_size_for_stack = stack_size;
        sess->is_free = TRUE;

        UT_LIST_ADD_LAST(list_node, free_sess_list, sess);
    }

    return CM_SUCCESS;
}

que_sess_t* session_pool_t::alloc_session()
{
    que_sess_t* sess;

    mutex_enter(&mutex);
    sess = UT_LIST_GET_FIRST(free_sess_list);
    if (sess) {
        UT_LIST_REMOVE(list_node, free_sess_list, sess);
        UT_LIST_ADD_LAST(list_node, used_sess_list, sess);
        sess->is_free = FALSE;
    }
    mutex_exit(&mutex);

    return sess;
}

void session_pool_t::free_session(que_sess_t* sess)
{
    mutex_enter(&mutex);
    sess->is_free = TRUE;
    UT_LIST_REMOVE(list_node, used_sess_list, sess);
    UT_LIST_ADD_LAST(list_node, free_sess_list, sess);
    mutex_exit(&mutex);
}


status_t que_sess_t::init()
{
    mem_page_for_stack = marea_alloc_page(srv_memory_sga, mem_page_size_for_stack);
    if (mem_page_for_stack == NULL) {
        CM_SET_ERROR(ERR_ALLOC_MEMORY, (uint64)mem_page_size_for_stack, "initializing session");
        return ERR_ALLOC_MEMORY;
    }

    mcontext_stack = mcontext_stack_create(srv_common_mpool);
    if (mcontext_stack == NULL) {
        return ERR_ALLOC_MEMORY;
    }

    // stack size = 
    cm_stack_init(&stack, MEM_PAGE_DATA_PTR(mem_page_for_stack), mem_page_size_for_stack);
    //
    fast_clean_mgr.init(srv_common_mpool);

    wait_trx_event = g_resource.events[sess_id % CM_RM_EVENT_MAX_COUNT];

    return CM_SUCCESS;
}

void que_sess_t::clean()
{
    ut_a(is_free);

    if (mem_page_for_stack) {
        marea_free_page(srv_memory_sga, mem_page_for_stack, mem_page_size_for_stack);
        mem_page_for_stack = NULL;
    }

    fast_clean_mgr.destroy();

    if (mcontext_stack) {
        mcontext_stack_destroy(mcontext_stack);
        mcontext_stack = NULL;
    }

    cm_stack_init(&stack, NULL, 0);
}

status_t que_sess_t::check_is_valid()
{
    status_t ret = CM_SUCCESS;
/*
    if (session->dead_locked) {
        CT_THROW_ERROR(ERR_DEAD_LOCK, "transaction", session->id);
        CT_LOG_ALARM(WARN_DEADLOCK, "'instance-name':'%s'}", session->kernel->instance_name);
        return CT_ERROR;
    }

    if (session->itl_dead_locked) {
        CT_THROW_ERROR(ERR_DEAD_LOCK, "itl", session->id);
        CT_LOG_ALARM(WARN_DEADLOCK, "'instance-name':'%s'}", session->kernel->instance_name);
        return CT_ERROR;
    }

    if (session->lock_dead_locked) {
        CT_THROW_ERROR(ERR_DEAD_LOCK, "table", session->id);
        CT_LOG_ALARM(WARN_DEADLOCK, "'instance-name':'%s'}", session->kernel->instance_name);
        return CT_ERROR;
    }

    if (session->canceled) {
        CT_THROW_ERROR(ERR_OPERATION_CANCELED);
        return CT_ERROR;
    }
*/
    if (is_killed) {
        //CM_SET_ERROR(ERR_OPERATION_KILLED);
        ret = CM_ERROR;
    }

    return ret;
}


status_t que_sess_t::wait_transaction_end(uint64 timeout_us)
{
    trx_status_t trx_status;
    uint64 signal_count = 0;
    const uint64 timeout_us_per = 1000;
    date_t begin_time_us = g_timer()->now_us;
    status_t ret = CM_SUCCESS;

    for (;;) {
        trx_get_status_by_itl(wait_xid, &trx_status);
        if (trx_status.status == XACT_END) {
            break;
        }

        ut_ad(wait_trx_event);
        os_event_wait_time(wait_trx_event, timeout_us_per, signal_count);
        signal_count = os_event_reset(wait_trx_event);

        if (timeout_us != 0 && g_timer()->now_us >= begin_time_us + timeout_us) {
            //CM_SET_ERROR(ERR_LOCK_WAIT_TIMEOUT);
            ret = ERR_LOCK_WAIT_TIMEOUT;
            break;
        }
        if ((ret = check_is_valid()) != CM_SUCCESS) {
            break;
        }
    }

    return ret;
}


void page_fast_clean_mgr_t::init(memory_page_t* mem_page)
{
    m_mem_page = mem_page;
    m_clean_page_count = 0;
    m_clean_page_limit = UNIV_PAGE_SIZE / FAST_CLEAN_PAGE_NODE_SIZE;
}

void page_fast_clean_mgr_t::append_page(uint32 space_id, uint32 page_no, void* block, uint8 itl_id)
{
    if (m_clean_page_count >= m_clean_page_limit) {
        return;
    }

    fast_clean_page_node_t* node = MEM_PAGE_DATA_PTR(m_mem_page) + m_clean_page_count * FAST_CLEAN_PAGE_NODE_SIZE;
    mach_write_to_4(node, space_id);
    mach_write_to_4(node, page_no);
    mach_write_to_1(node, itl_id);
    mach_write_to_8(node, PointerGetDatum(block));
    m_clean_page_count++;
}

page_fast_clean_t* page_fast_clean_mgr_t::get_page(uint32 i)
{
    if (i >= m_clean_page_count) {
        return NULL;
    }

    fast_clean_page_node_t* page_node = MEM_PAGE_DATA_PTR(m_mem_page) + i * FAST_CLEAN_PAGE_NODE_SIZE;
    m_clean_page.space_id = mach_read_from_4(page_node + FAST_CLEAN_PAGE_NODE_SPACE_ID);
    m_clean_page.page_no = mach_read_from_4(page_node + FAST_CLEAN_PAGE_NODE_PAGE_NO);
    m_clean_page.itl_id = mach_read_from_1(page_node + FAST_CLEAN_PAGE_NODE_ITL_ID);
    m_clean_page.block = (buf_block_t*)DatumGetPointer(mach_read_from_8(page_node + FAST_CLEAN_PAGE_NODE_BLOCK));

    return &m_clean_page;
}

