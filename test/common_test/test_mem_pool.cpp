#include "cm_type.h"
#include "cm_memory.h"
#include "cm_dbug.h"
#include "cm_log.h"
#include "cm_mutex.h"
#include "cm_list.h"

#pragma comment(lib, "ws2_32.lib")

static memory_area_t* g_mem_area = NULL;
static memory_pool_t* g_mem_pool = NULL;

static bool32 create_memory_pool()
{
    uint64 memory_size = SIZE_M(10);
    bool32 is_extend = FALSE;
    uint32 initial_page_count = 8;
    uint32 lower_page_count = 1024;
    uint32 page_size = SIZE_K(8);

    g_mem_area = marea_create(memory_size, is_extend);
    if (g_mem_area == NULL) {
        return FALSE;
    }
    g_mem_pool = mpool_create(g_mem_area, "mem pool", memory_size, page_size, initial_page_count, lower_page_count);
    if (g_mem_pool == NULL) {
        return FALSE;
    }

    return TRUE;
}

static void destroy_memory_pool()
{
    if (g_mem_pool) {
        mpool_destroy(g_mem_pool);
        g_mem_pool = NULL;
    }
    if (g_mem_area) {
        marea_destroy(g_mem_area);
        g_mem_area = NULL;
    }
}

void mem_pool_test_thread(memory_pool_t* pool, uint32 index, status_t* result)
{
    const uint32 page_count = 8;// SIZE_K(1);
    memory_page_t* pages[page_count] = {NULL};
    uint32 loop_count = 100000;
    uint64 thread_index = os_thread_get_internal_id();

    printf("thread %lu, thread id %llu\n", index, thread_index);

    *result = CM_SUCCESS;
    for (uint32 loop = 0; loop < loop_count && *result == CM_SUCCESS; loop++) {
        for (uint32 i = 0; i < page_count; i++) {
            pages[i] = mpool_alloc_page(pool);
            if (pages[i] == NULL) {
                *result = CM_ERROR;
            }
        }
        for (uint32 i = 0; i < page_count; i++) {
            if (pages[i] == NULL) {
                continue;
            }
            mpool_free_page(pool, pages[i]);
            pages[i] = NULL;
        }
    }
}

bool32 test_pool()
{
    bool32 ret = TRUE;
    const uint32 thread_count = 8;
    os_thread_t threads[thread_count];
    status_t results[thread_count];
    uint32 thread_index[thread_count];

    for (uint32 i = 0; i < thread_count; ++i) {
        thread_index[i] = i;
        threads[i] = thread_start(mem_pool_test_thread, g_mem_pool, thread_index[i], &results[i]);
    }

    for (uint32 i = 0; i < thread_count; ++i) {
        if (!os_thread_join(threads[i]) || results[i] != CM_SUCCESS) {
            ret = FALSE;
        }
    }

    // check
    for (uint32 i = 0; i < MPOOL_FREE_PAGE_LIST_COUNT; i++) {
        pool_free_page_list_t* free_page_list = &g_mem_pool->page_list[i];
        printf("free_list %02u: count %u\n", i, UT_LIST_GET_LEN(*free_page_list));
    }

    return ret;
}



#define M_MEM_BUF_NUM      1024
struct mem_context_mgr_t {
    memory_context_t* ctx;
    char*             buf[M_MEM_BUF_NUM];
    atomic32_t        index;
};

void mem_context_test_thread(uint32 index, status_t* result, mem_context_mgr_t* mgr)
{
    uint32 size;

    *result = CM_SUCCESS;

    uint32 idx = atomic32_inc(&mgr->index);
    while (idx < M_MEM_BUF_NUM) {
        size = ut_rnd_interval(32, 1023);
        mgr->buf[idx] = (char *)my_malloc_zero(mgr->ctx, size);
        if (mgr->buf[idx]) {
            sprintf_s(mgr->buf[idx], size, "index: %04d data size: %04d", idx, size);
            idx = atomic32_inc(&mgr->index);
        }
    }
}

bool32 test_context()
{
    bool32 ret = FALSE;
    const uint32 thread_count = 8;
    os_thread_t threads[thread_count];
    status_t results[thread_count];
    uint32 thread_index[thread_count];

    mem_context_mgr_t mgr;
    memset(&mgr, 0x00, sizeof(mem_context_mgr_t));
    mgr.ctx = mcontext_create(g_mem_pool);

    for (uint32 i = 0; i < thread_count; ++i) {
        thread_index[i] = i;
        threads[i] = thread_start(mem_context_test_thread, thread_index[i], &results[i], &mgr);
    }

    for (uint32 i = 0; i < thread_count; ++i) {
        if (!os_thread_join(threads[i]) || results[i] != CM_SUCCESS) {
            goto err_exit;
        }
    }

    // check
    const uint32 temp_buf_size = 48;
    char temp_buf[temp_buf_size];
    for (uint32 i = 1; i < M_MEM_BUF_NUM; i++) {
        if (mgr.buf[i] == NULL) {
            printf("check: buf is NULL, buf index=%d\n", i);
            goto err_exit;
        }

        sprintf_s(temp_buf, temp_buf_size, "index: %04d", i);
        if (strncmp(mgr.buf[i], temp_buf, strlen(temp_buf)) != 0) {
            printf("check: buf is not equal, buf index=%d\n", i);
            goto err_exit;
        }
        //printf("memory: %s\n", mgr.buf[i]);
        my_free(mgr.buf[i]);
    }

    ret = TRUE;

err_exit:

    mcontext_clean(mgr.ctx);
    mcontext_destroy(mgr.ctx);

    return ret;
}

bool32 test_context_ext()
{
    bool32 ret = FALSE;
    memory_context_t* ctx;

    ctx = mcontext_create(g_mem_pool);
    char* buf = (char*)mem_alloc(ctx, SIZE_K(16));
    if (buf) {
        mem_free(buf);
        ret = TRUE;
    }

    mcontext_clean(ctx);
    mcontext_destroy(ctx);

    return ret;
}


bool32 test_stack_context()
{
    bool32 ret = FALSE;
    void* save_point;
    memory_stack_context_t* stack_context;
    char* buf[M_MEM_BUF_NUM];

    stack_context = mcontext_stack_create(g_mem_pool);

    buf[0] = (char *)mcontext_stack_push(stack_context, 44);
    if (buf[0] == NULL) goto err_exit;
    buf[1] = (char *)mcontext_stack_push(stack_context, 55);
    if (buf[1] == NULL) goto err_exit;
    save_point = mcontext_stack_save(stack_context);
    buf[2] = (char *)mcontext_stack_push(stack_context, 5000);
    if (buf[2] == NULL) goto err_exit;
    buf[3] = (char *)mcontext_stack_push(stack_context, 6000);
    if (buf[3] == NULL) goto err_exit;

    if (mcontext_stack_pop(stack_context, buf[1], 55) == CM_SUCCESS) {
        printf("test_stack_context: stack_pop should return failure\n");
        goto err_exit;
    }

    if (mcontext_stack_restore(stack_context, save_point) != CM_SUCCESS) {
        printf("test_stack_context: error for stack_restore\n");
        goto err_exit;
    }

    if (mcontext_stack_pop(stack_context, buf[1], 55) != CM_SUCCESS) {
        printf("test_stack_context: error for buf1 stack_pop\n");
        goto err_exit;
    }

    ret = TRUE;

err_exit:

    mcontext_stack_clean(stack_context);
    mcontext_stack_destroy(stack_context);

    return ret;
}

int main(int argc, char *argv[])
{
    bool32 ret;
    char *log_path = "D:\\MyWork\\cos\\data\\";
    //LOGGER.log_init(LOG_LEVEL_ALL, log_path, "memory_test");
    //LOGGER.log_init(LOG_INFO, NULL, "memory_test");
    //LOGGER_WARN(LOGGER, "WARN: This is memory test");
    //LOGGER_DEBUG(LOGGER, "DEBUG: This is memory test");

    ret = create_memory_pool();
    if (!ret) goto err_exit;

    ret = test_pool();
    if (!ret) goto err_exit;

    ret = test_context();
    if (!ret) goto err_exit;

    ret = test_context_ext();
    if (!ret) goto err_exit;

    ret = test_stack_context();
    if (!ret) goto err_exit;

err_exit:

    destroy_memory_pool();

    return ret;
}


