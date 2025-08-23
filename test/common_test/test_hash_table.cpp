#include "cm_type.h"
#include "cm_memory.h"
#include "cm_dbug.h"
#include "cm_log.h"
#include "cm_mutex.h"
#include "cm_hash_table.h"

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


struct hash_key {
    uint32 id;
};

struct hash_data {
    uint32 refcount;
    uint32 data;
};

struct hash_element {
    hash_key key;
    hash_data data;
};


uint32 hash_calc_func(const void* key, uint32 key_len)
{
    hash_key* k = (hash_key*)key;
    return k->id;
}

int32 hash_compare_func(const void* key1, const void* key2, uint32 key_len)
{
    hash_key* k1 = (hash_key*)key1;
    hash_key* k2 = (hash_key*)key2;

    if (k1->id == k2->id) {
        return 0;
    } else if (k1->id > k2->id) {
        return 1;
    }
    return -1;
}


bool32 test_hash_table()
{
    bool32 ret = FALSE;
    status_t err;

    hash_table_t table;
    uint32 bucket_count = 1024;
    hash_table_ctrl_t info;

    info.enable_expand_bucket = TRUE;
    info.pool = g_mem_pool;
    info.key_size = 4;
    info.calc = hash_calc_func;
    info.cmp = hash_compare_func;
    if (hash_table_create(&table, bucket_count, &info) != CM_SUCCESS) {
        goto err_exit;
    }
    
    hash_element ele1, ele2;
    ele1.key.id = 1;
    ele1.data.refcount = 1;
    ele1.data.data = 1000;
    ele2.key.id = 2;
    ele2.data.refcount = 1;
    ele2.data.data = 2000;

    bool32 is_found = FALSE;
    void* data = hash_table_search(&table, &ele1.key, &is_found);
    if (is_found) goto err_exit;

    err = hash_table_insert(&table, &ele1.key, &ele1.data);
    if (err != CM_SUCCESS) goto err_exit;

    data = hash_table_search(&table, &ele1.key, &is_found);
    if (!is_found) goto err_exit;

    err = hash_table_insert(&table, &ele2.key, &ele2.data);
    if (err != CM_SUCCESS) goto err_exit;

    is_found = FALSE;
    data = hash_table_delete(&table, &ele1.key, &is_found);
    if (!is_found) goto err_exit;

    is_found = FALSE;
    data = hash_table_search(&table, &ele1.key, &is_found);
    if (is_found) goto err_exit;

    ret = TRUE;

err_exit:

    hash_table_destroy(&table);

    return ret;
}

bool32 test_hash_table_expand()
{
    bool32 ret = FALSE;
    status_t err;

    hash_table_t table;
    uint32 bucket_count = 1024;
    hash_table_ctrl_t info;

    info.enable_expand_bucket = TRUE;
    info.pool = g_mem_pool;
    info.key_size = 4;
    info.calc = hash_calc_func;
    info.cmp = hash_compare_func;
    if (hash_table_create(&table, bucket_count, &info) != CM_SUCCESS) {
        goto err_exit;
    }

    for (uint32 i = 0; i < bucket_count * 8; i++) {
        hash_element ele1;
        ele1.key.id = i;
        ele1.data.refcount = i;
        ele1.data.data = 1000000 + i;

        err = hash_table_insert(&table, &ele1.key, &ele1.data);
        if (err != CM_SUCCESS) goto err_exit;

        bool32 is_found = FALSE;
        hash_data* data = (hash_data*)hash_table_search(&table, &ele1.key, &is_found);
        if (!is_found) goto err_exit;
        if (data->refcount != i) goto err_exit;
        if (data->data != 1000000 + i) goto err_exit;
    }

    ret = TRUE;

err_exit:

    hash_table_destroy(&table);

    return ret;
}

int main_ht(int argc, char *argv[])
{
    bool32 ret;
    char *log_path = "D:\\MyWork\\cos\\data\\";
    //LOGGER.log_init(LOG_LEVEL_ALL, log_path, "memory_test");
    //LOGGER.log_init(LOG_INFO, NULL, "memory_test");
    //LOGGER_WARN(LOGGER, "WARN: This is memory test");
    //LOGGER_DEBUG(LOGGER, "DEBUG: This is memory test");

    ret = create_memory_pool();
    if (!ret) goto err_exit;

    sync_init(g_mem_pool);

    //test_pool();
    ret = test_hash_table();
    if (!ret) goto err_exit;

    ret = test_hash_table_expand();
    if (!ret) goto err_exit;

err_exit:

    sync_close();
    destroy_memory_pool();

    return ret;
}


