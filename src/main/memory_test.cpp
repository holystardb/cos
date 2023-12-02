#include "cm_type.h"
#include "cm_memory.h"
#include "cm_virtual_mem.h"
#include "cm_dbug.h"
#include "cm_log.h"

typedef struct st_vm_xdesc {
    UT_LIST_NODE_T(struct st_vm_xdesc) list_node;
    uint64          id;
    union {
        byte        bits[8];
        uint64      value;
    } bitmap;
} vm_xdesc_t;

void test_memory()
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
    buf = (char *)mcontext_alloc(context, 32);
    mcontext_free(context, buf);

    mcontext_stack_push(context, 44);
    buf = (char *)mcontext_stack_push(context, 44);
    mcontext_stack_push(context, 5000);
    mcontext_stack_push(context, 6000);
    mcontext_stack_pop2(context, buf);
    mcontext_clean(context);

    printf("\n\n*********************** done ***********************\n");
}

bool32 test_vm_memory()
{
    vm_pool_t* pool;
    vm_ctrl_t* ctrl[1024];
    uint64     memory_size = 1024 * 1024;
    uint32     page_size = 1024 * 128, loop_count = 100;
    char      *name = "D:\\MyWork\\cos\\data\\vm_data1.dat";
    char       temp[1024];

    os_del_file(name);
    pool = vm_pool_create(memory_size, page_size);
    if (!vm_pool_add_file(pool, name, 1024 * 1024 * 100)) {
        printf("error: cannot create file %s\n", name);
        goto err_exit;
    }

    for (uint32 i = 0; i < loop_count; i++) {
        if (i == 94) {
            printf("flag: %d\n", i);
        }

        ctrl[i] = vm_alloc(pool);
        if (ctrl[i] == NULL) {
            printf("write error: vm_alloc, i=%d\n", i);
            goto err_exit;
        }
        printf("\nwrite: ctrl %p i=%d ********************\n", ctrl[i], i);

        if (vm_open(pool, ctrl[i]) == FALSE) {
            printf("write error: vm_open, i=%d\n", i);
            goto err_exit;
        }

        char *buf = ctrl[i]->val.data;
        memset(buf, 0x00, page_size);
        sprintf_s(buf, page_size, "data: %08d", i);

        if (!vm_close(pool, ctrl[i])) {
            printf("write error: vm_close, i=%d\n", i);
            goto err_exit;
        }
    }

    for (uint32 i = 0; i < loop_count; i++) {
        printf("\nread check: ctrl %p i=%d ********************\n", ctrl[i], i);
        if (vm_open(pool, ctrl[i]) == FALSE) {
            printf("read error: vm_open, i=%d\n", i);
            goto err_exit;
        }

        if (i == 6) {
            printf("flag\n");
        }

        sprintf_s(temp, 1024, "data: %08d", i);
        if (strncmp(ctrl[i]->val.data, temp, strlen(temp)) == 0) {
            printf("read check: ok, i=%d\n", i);
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

    vm_pool_destroy(pool);

    printf("\n\n*********************** done ***********************\n");

    return TRUE;

err_exit:

    printf("\n\n*********************** Error ***********************\n");

    return FALSE;
}


void test_free_page(uint32 page_no)
{
    uint64 slot_count_pre_page = 1024 * 128 / sizeof(vm_page_slot_t);
    uint64 page_count_pre_slot = 64;
    uint64 page_count_pre_slot_page = slot_count_pre_page * page_count_pre_slot;

    uint32 page_count, remain_page_count, page_index, slot_index, byte_index, bit_index;

    page_count = page_no + 1;
    page_index = (page_count - 1) / page_count_pre_slot_page;
    remain_page_count = page_count % page_count_pre_slot_page;
    if (remain_page_count == 0) {
        slot_index = slot_count_pre_page;
        byte_index = 7;
        bit_index = 7;
    } else {
        if (remain_page_count % page_count_pre_slot == 0) {
            slot_index = (remain_page_count - 1) / page_count_pre_slot;
            byte_index = 7;
            bit_index = 7;
        } else {
            slot_index = remain_page_count / page_count_pre_slot;
            remain_page_count = remain_page_count % page_count_pre_slot;
            if (remain_page_count % 8 == 0) {
                byte_index = (remain_page_count - 1) / 8;
                bit_index = 7;
            } else {
                byte_index = remain_page_count / 8;
                bit_index = remain_page_count % 8 - 1;
            }
        }
    }
    printf("page no %u : page index %u slot index %u byte index %u bit index %u\n",
        page_no, page_index, slot_index, byte_index, bit_index);
}

void do_test_free_page()
{
    uint64 slot_count_pre_page = 1024 * 128 / sizeof(vm_page_slot_t);
    uint64 page_count_pre_slot = 64;
    uint64 page_count_pre_slot_page = slot_count_pre_page * page_count_pre_slot;
    uint32 page_no;

    page_no = 63;
    test_free_page(page_no - 1);
    test_free_page(page_no);
    test_free_page(page_no + 1);
    printf("\n");

    page_no = 127;
    test_free_page(page_no - 1);
    test_free_page(page_no);
    test_free_page(page_no + 1);
    printf("\n");

    page_no = page_count_pre_slot_page - 1;
    test_free_page(page_no - 1);
    test_free_page(page_no);
    test_free_page(page_no + 1);
    printf("\n");

    page_no = page_count_pre_slot_page * 2 - 1;
    test_free_page(page_no - 1);
    test_free_page(page_no);
    test_free_page(page_no + 1);
}

int main(int argc, char *argv[])
{
    char *log_path = "D:\\MyWork\\cos\\data";
    log_init(LOG_TRACE, log_path, "memory_test.log");

    DBUG_INIT(NULL, 1);
    DBUG_ENTER("main");
    DBUG_PRINT("%s", "do vm_memory test");

    test_vm_memory();

    DBUG_END();

    return 0;
}


