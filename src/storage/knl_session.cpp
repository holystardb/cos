#include "knl_session.h"

session_pool_t*   g_sess_pool = NULL;
attribute_t       g_attribute;


status_t sess_pool_create(uint32 sess_count)
{
    g_sess_pool = (session_pool_t *)ut_malloc_zero(sizeof(session_pool_t));
    g_sess_pool->sessions = (que_sess_t *)ut_malloc_zero(sizeof(que_sess_t) * sess_count);
    UT_LIST_INIT(g_sess_pool->used_sess_list);
    UT_LIST_INIT(g_sess_pool->free_sess_list);
    mutex_create(g_sess_pool->mutex);

    que_sess_t* sess;
    for (uint32 i = 0; i < sess_count; i++) {
        sess = g_sess_pool->sessions + i;
        que_sess_init(sess, i, );
        UT_LIST_ADD_LAST(list_node, g_sess_pool->free_sess_list, sess);
    }
}

inline void que_sess_init(que_sess_t* sess, uint32 sess_id, uint32 stack_size)
{
    memset((char *)sess, 0x00, sizeof(que_sess_t));

    sess->id = sess_id;
    sess->attr = &g_attribute;

    char* stack_buf = (char *)ut_malloc(sess->attr->attr_common.session_stack_size);
    cm_stack_init(&sess->stack, stack_buf, sess->attr->attr_common.session_stack_size);
}

inline void que_sess_destroy(que_sess_t* sess)
{
    ut_free(sess->stack.buf);
}


