#include "cm_type.h"
#include "cm_memory.h"
#include "cm_encrypt.h"

typedef struct st_vm_xdesc {
    UT_LIST_NODE_T(struct st_vm_xdesc) list_node;
    uint64          id;
    union {
        byte        bits[8];
        uint64      value;
    } bitmap;
} vm_xdesc_t;

int main77(int argc, char *argv[])
{
    memory_area_t* area;
    memory_pool_t* pool;
    memory_context_t* context;
    uint64 size = 1024 * 1024;
    bool32 is_extend = FALSE;
    uint32 local_page_count = 8;
    uint32 max_page_count = 16;
    uint32 page_size = 1024 * 8;

    area = marea_create(size, is_extend);
    pool = mpool_create(area, local_page_count, max_page_count, page_size);
    context = mcontext_create(pool);

    char *buf;
    buf = mcontext_alloc(context, 32);
    mcontext_free(context, buf);

    mcontext_push(context, 44);
    buf = mcontext_push(context, 44);
    mcontext_push(context, 5000);
    mcontext_push(context, 6000);
    mcontext_pop2(context, buf);
    mcontext_clean(context);

    

    printf("\n\n*********************** done ***********************\n");

    return 0;
}
