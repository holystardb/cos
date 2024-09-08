#ifndef _KNL_SESSION_H
#define _KNL_SESSION_H

#include "cm_type.h"
#include "cm_mutex.h"
#include "cm_stack.h"
#include "cm_attribute.h"
#include "cm_memory.h"

#include "knl_trx_types.h"

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus



typedef struct st_que_sess {
   // mutex_t        mutex;
    uint32         id;  // session id
    command_id_t   cid;
    cm_stack_t     stack;

    trx_t*         trx;

    attribute_t*   attr;

    char*          buf;
    uint32         buf_len;
    //row_header_t*  row;

    memory_stack_context_t* stack_context;
    UT_LIST_NODE_T(struct st_que_sess) list_node;
} que_sess_t;


typedef struct st_session_pool {
    mutex_t        mutex;
    que_sess_t*    sessions;
    UT_LIST_BASE_NODE_T(que_sess_t)  used_sess_list;
    UT_LIST_BASE_NODE_T(que_sess_t)  free_sess_list;
} session_pool_t;


extern status_t sess_pool_create(uint32 sess_count);

extern inline void que_sess_init(que_sess_t* sess, uint32 sess_id, uint32 stack_size);
extern inline void que_sess_destroy(que_sess_t* sess);

extern attribute_t       g_attribute;
extern session_pool_t*   g_sess_pool;

#ifdef __cplusplus
}
#endif // __cplusplus

#endif  /* _KNL_SESSION_H */
