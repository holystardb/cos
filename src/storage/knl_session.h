#ifndef _KNL_SESSION_H
#define _KNL_SESSION_H

#include "cm_type.h"
#include "cm_mutex.h"
#include "cm_stack.h"
#include "cm_attribute.h"
#include "cm_memory.h"

#include "knl_trx_types.h"
#include "knl_fast_clean.h"

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus


// ------------------------------------------------------

typedef struct st_kernel_sess {
    bool32            is_xa_consistency;

} kernel_sess_t;


#define CM_RM_EVENT_MAX_COUNT   1024

struct resource_t {
    os_event_t         events[CM_RM_EVENT_MAX_COUNT];

};

struct resource_owner_t {
    uint32     event_id;
};


struct que_sess_t {
    kernel_sess_t*    kernel{NULL};
   // mutex_t          mutex;
    uint32            sess_id;
    command_id_t      cid;
    cm_stack_t        stack;

    trx_t*            trx{NULL};

    attribute_t*      attr{NULL};
    bool32            is_free;
    uint32            mem_page_size_for_stack;
    memory_page_t*    mem_page_for_stack{NULL};
    char*             buf;
    uint32            buf_len;
    //row_header_t*    row;

    resource_owner_t  resource;

    volatile trx_slot_id_t wait_xid;
    volatile row_id_t wait_row_id;
    os_event_t wait_trx_event;

    // caution !! do not assign killed to true directly, use g_knl_callback.kill_session instead
    volatile bool8    is_killed;
    volatile bool8    is_canceled;
    volatile bool8    force_kill;


    //
    fast_clean_mgr_t  fast_clean_mgr;

    mutex_t           scn_mutex;
    atomic64_t        current_scn;

    memory_stack_context_t* mcontext_stack{NULL};
    UT_LIST_NODE_T(que_sess_t) list_node;

    status_t init();
    void clean();
    status_t wait_transaction_end(uint64 timeout_us = OS_WAIT_INFINITE_TIME);
};


class session_pool_t {
public:
    session_pool_t();
    ~session_pool_t();

    status_t init(uint32 sess_count, uint32 stack_size, attribute_t* attr);
    que_sess_t* alloc_session();
    void free_session(que_sess_t* sess);

private:
    mutex_t        mutex;
    que_sess_t*    sessions;

    UT_LIST_BASE_NODE_T(que_sess_t) used_sess_list;
    UT_LIST_BASE_NODE_T(que_sess_t) free_sess_list;
};


extern status_t sess_pool_create(uint32 sess_count, uint32 session_stack_size);
extern void sess_pool_destroy();

extern que_sess_t* que_sess_alloc();
extern void que_sess_free(que_sess_t* sess);


extern attribute_t       g_attribute;
extern session_pool_t*   g_sess_pool;
extern resource_t        g_resource;

#ifdef __cplusplus
}
#endif // __cplusplus

#endif  /* _KNL_SESSION_H */
