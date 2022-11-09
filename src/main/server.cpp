#include "cm_type.h"
#include "cm_memory.h"
#include "cm_log.h"
#include "knl_handler.h"
#include "cm_dbug.h"
#include "cm_rwlock.h"
#include "cm_thread.h"

rw_lock_t lock1;
rw_lock_t lock2;

void thread1_main(void *arg)
{
    rw_lock_s_lock(&lock1);
    LOG_PRINT_INFO("thread1: lock1 %p\n", &lock1);

    os_thread_sleep(2000000);

    rw_lock_x_lock(&lock2);
}

void thread2_main(void *arg)
{
    rw_lock_s_lock(&lock2);
    LOG_PRINT_INFO("thread2: lock2 %p\n", &lock2);

    os_thread_sleep(1000000);

    rw_lock_x_lock(&lock1);
}

int main(int argc, const char *argv[])
{
    os_thread_t thd1, thd2;

    log_init(LOG_DEBUG, NULL, NULL);
    sync_init();

    rw_lock_create(&lock1);
    rw_lock_create(&lock2);

    thd1 = thread_start(thread1_main, &lock1);
    thd2 = thread_start(thread2_main, &lock2);

    for (uint32 i = 0; ; i++) {
        if (i == 10) {
            //break;
        }

        //sync_print(stderr);
        os_thread_sleep(1000000);
    }

    sync_print(stderr);

    os_thread_join(thd1);
    os_thread_join(thd2);
      

    rw_lock_destroy(&lock1);
    rw_lock_destroy(&lock2);

    sync_close();

    knl_server_init();

    return 0;
}

