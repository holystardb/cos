#include "cm_type.h"
#include "cm_memory.h"
#include "cm_vm_pool.h"
#include "cm_dbug.h"
#include "cm_log.h"
#include "cm_mutex.h"
#include "cm_list.h"
#include "cm_rwlock.h"

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

bool32 test_vm_memory()
{
    bool32 ret = FALSE;
    vm_pool_t* pool;
    vm_ctrl_t* ctrl[1024];
    uint64     memory_size = SIZE_M(64);
    uint32     page_size = SIZE_K(128), loop_count = 100;
    char      *name = "D:\\MyWork\\cos\\data\\vm_data1.dat";
    char       temp[1024];

    os_del_file(name);
    pool = vm_pool_create(memory_size, page_size, 10);
    if (pool == NULL) {
        printf("error: cannot create vmpool\n");
        goto err_exit;
    }
    if (vm_pool_add_file(pool, name, SIZE_M(100)) != CM_SUCCESS) {
        printf("error: cannot create file %s\n", name);
        goto err_exit;
    }

    for (uint32 i = 0; i < loop_count; i++) {
        if (i == 21) {
            //printf("flag: %d\n", i);
        }

        ctrl[i] = vm_alloc(pool);
        if (ctrl[i] == NULL) {
            printf("write error: vm_alloc, i=%d\n", i);
            goto err_exit;
        }
        //printf("\nwrite: ctrl %p i=%d ********************\n", ctrl[i], i);

        if (vm_open(pool, ctrl[i]) == FALSE) {
            printf("write error: vm_open, i=%d\n", i);
            goto err_exit;
        }

        char *buf = VM_CTRL_GET_DATA_PTR(ctrl[i]);
        memset(buf, 0x00, page_size);
        sprintf_s(buf, page_size, "data: %08d", i);

        if (!vm_close(pool, ctrl[i])) {
            printf("write error: vm_close, i=%d\n", i);
            goto err_exit;
        }
    }

    for (uint32 i = 0; i < loop_count; i++) {
        //printf("\nread check: ctrl %p i=%d ********************\n", ctrl[i], i);
        if (vm_open(pool, ctrl[i]) == FALSE) {
            printf("read error: vm_open, i=%d\n", i);
            goto err_exit;
        }

        if (i == 6) {
        //    printf("flag\n");
        }

        sprintf_s(temp, 1024, "data: %08d", i);
        if (strncmp(VM_CTRL_GET_DATA_PTR(ctrl[i]), temp, strlen(temp)) == 0) {
            //printf("read check: ok, i=%d\n", i);
        } else {
            printf("read check: fail, i=%d\n", i);
            goto err_exit;
        }

        if (!vm_close(pool, ctrl[i])) {
            printf("read error: vm_close, i=%d\n", i);
            goto err_exit;
        }

    }

    for (uint32 i = 0; i < loop_count; i++) {
        vm_free(pool, ctrl[i]);
    }

    ret = TRUE;

err_exit:

    vm_pool_destroy(pool);

    return ret;
}


int vmpool_main(int argc, char *argv[])
{
    bool32 ret;
    char *log_path = "D:\\MyWork\\cos\\data\\";
    //LOGGER.log_init(LOG_LEVEL_ALL, log_path, "memory_test");
    //LOGGER.log_init(LOG_INFO, NULL, "memory_test");
    //LOGGER_WARN(LOGGER, "WARN: This is memory test");
    //LOGGER_DEBUG(LOGGER, "DEBUG: This is memory test");

    //DBUG_INIT(log_path, "memory_dbug", 1);
    //DBUG_INIT(NULL, "memory_dbug", 1);
    //DBUG_ENTER("main");
    //DBUG_PRINT("%s", "do vm_memory test");
    //DBUG_PRINT("%s", "-------------------------------------------");

    ret = create_memory_pool();
    if (!ret) goto err_exit;

    sync_init(g_mem_pool);

    //
    ret = test_vm_memory();
    if (!ret) goto err_exit;

    //DBUG_END();

err_exit:

    sync_close();
    destroy_memory_pool();

    return ret;
}


