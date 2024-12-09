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

// ------------------------------------------------------

typedef byte  fast_clean_page_hdr_t;

#define FAST_CLEAN_PAGE_HEADER_SIZE      2

typedef byte  fast_clean_page_t;

#define FAST_CLEAN_PAGE_SPACE_ID         0
#define FAST_CLEAN_PAGE_PAGE_NO          4
#define FAST_CLEAN_PAGE_ITL_ID           8
#define FAST_CLEAN_PAGE_BLOCK            9

#define FAST_CLEAN_PAGE_SIZE             17

#define FAST_CLEAN_PAGE_COUNT_PER_PAGE   ((UNIV_PAGE_SIZE - FAST_CLEAN_PAGE_HEADER_SIZE) / FAST_CLEAN_PAGE_SIZE)


// ------------------------------------------------------



typedef struct st_que_sess {
   // mutex_t          mutex;
    uint32            id;  // session id
    command_id_t      cid;
    cm_stack_t        stack;

    trx_t*            trx;

    attribute_t*      attr;
    bool32            is_free;
    uint32            mem_page_size;
    memory_page_t*    mem_page;
    char*             buf;
    uint32            buf_len;
    //row_header_t*    row;

    //
    memory_page_t*    fast_clean_pages;

    mutex_t           scn_mutex;
    atomic64_t        current_scn;

    memory_stack_context_t* mcontext_stack;
    UT_LIST_NODE_T(struct st_que_sess) list_node;
} que_sess_t;


typedef struct st_session_pool {
    mutex_t        mutex;
    que_sess_t*    sessions;

    UT_LIST_BASE_NODE_T(que_sess_t)  used_sess_list;
    UT_LIST_BASE_NODE_T(que_sess_t)  free_sess_list;
} session_pool_t;


extern status_t sess_pool_create(uint32 sess_count, uint32 session_stack_size);
extern void sess_pool_destroy();

extern que_sess_t* que_sess_alloc();
extern void que_sess_free(que_sess_t* sess);

extern inline void sess_append_fast_clean_page_list(que_sess_t* sess, buf_block_t* block, uint8 itl_id);

extern attribute_t       g_attribute;
extern session_pool_t*   g_sess_pool;

#ifdef __cplusplus
}
#endif // __cplusplus

#endif  /* _KNL_SESSION_H */
