#ifndef _CM_MUTEX_H
#define _CM_MUTEX_H

#include "cm_atomic.h"
#include "cm_counter.h"
#include "cm_list.h"
#include "cm_thread.h"
#include "cm_type.h"
#include "cm_dbug.h"

#ifdef __cplusplus
extern "C" {
#endif

/***********************************************************************************************
*                                     os mutex                                                 *
***********************************************************************************************/

#define OS_WAIT_TIME_EXCEEDED   1
#define OS_WAIT_TIME_FAIL       2

/** Denotes an infinite delay for os_event_wait_time() */
#define OS_WAIT_INFINITE_TIME   UINT32_UNDEFINED

#ifdef __WIN__
typedef CRITICAL_SECTION    os_mutex_t;
typedef CONDITION_VARIABLE  os_cond_t;
#define os_event_t          HANDLE
#else
typedef pthread_mutex_t     os_mutex_t;
typedef pthread_cond_t      os_cond_t;
struct os_event_struct {
    os_mutex_t      os_mutex;   /* this mutex protects the next fields */
    bool32          is_set;     /* this is TRUE if the next mutex is not reserved */
    os_cond_t       cond_var;   /* condition variable is used in waiting for the event */
    uint64          signal_count;  /*!< this is incremented each time the event becomes signaled */
};
typedef struct os_event_struct*  os_event_t;
#endif

os_event_t os_event_create(char* name);
void os_event_set(os_event_t event);
void os_event_set_signal(os_event_t event);
uint64 os_event_reset(os_event_t event);
void os_event_destroy(os_event_t event);
void os_event_wait(os_event_t event, uint64 reset_sig_count = 0);
int os_event_wait_time(os_event_t event, uint32 timeout_us, uint64 reset_sig_count = 0);

void os_mutex_create(os_mutex_t *mutex);
void os_mutex_enter(os_mutex_t *mutex);
bool32 os_mutex_tryenter(os_mutex_t *mutex);
void os_mutex_exit(os_mutex_t *mutex);
void os_mutex_destroy(os_mutex_t *mutex);



/***********************************************************************************************
*                                     spin lock                                                *
***********************************************************************************************/

#define mutex_t               spinlock_t
#define mutex_create          spin_lock_init
#define mutex_destroy         spin_lock_destroy
#define mutex_own             spin_lock_own
#define mutex_enter           spin_lock
#define mutex_tryenter        spin_trylock
#define mutex_exit            spin_unlock
#define mutex_stats_t         spinlock_stats_t

#define SPINLOCK_MAGIC_N      979585UL

typedef struct st_spin_lock {
    atomic32_t      lock;
#ifdef UNIV_MUTEX_DEBUG
    uint32          magic_n;
    os_thread_id_t  thread_id; /*!< The thread id of the thread which locked. */
#endif
} spinlock_t;

typedef struct st_spinlock_stats {
    // number of spin thread yield
    uint64 spin_thread_yield_count;
    // number of spin loop rounds
    uint64 spin_round_count;
} spinlock_stats_t;

#define SPIN_ROUND_COUNT          30
#define SPIN_ROUND_WAIT_DELAY     6

inline void spin_lock_init(spinlock_t* lock)
{
    os_wmb;
#ifdef UNIV_MUTEX_DEBUG
    lock->thread_id = 0;
    lock->magic_n = SPINLOCK_MAGIC_N;
#endif
    lock->lock = 0;
}

inline void spin_lock_destroy(spinlock_t* lock)
{
}

inline void spin_lock(spinlock_t* lock, spinlock_stats_t* stats = NULL)
{
    uint32 i = 0;
    uint64 thread_yield_count = 0, spin_round_count = 0;

    if (atomic32_compare_and_swap(&lock->lock, 0, 1)) {
#ifdef UNIV_MUTEX_DEBUG
        ut_ad(lock->thread_id = os_thread_get_curr_id());
#endif
        return;
    }

lock_loop:

    os_rmb;
    while (i < SPIN_ROUND_COUNT && lock->lock != 0) {
        os_thread_delay(ut_rnd_interval(0, SPIN_ROUND_WAIT_DELAY));
        i++;
    }

    if (i == SPIN_ROUND_COUNT) {
        os_thread_yield();
        thread_yield_count++;
    }

    spin_round_count += i;

    /* We try once again to obtain the lock */
    if (atomic32_compare_and_swap(&lock->lock, 0, 1)) {
#ifdef UNIV_MUTEX_DEBUG
        ut_ad(lock->thread_id = os_thread_get_curr_id());
#endif
        if (stats) {
            stats->spin_round_count += spin_round_count;
            stats->spin_thread_yield_count += thread_yield_count;
        }
        return;
    }

    i = 0;
    goto lock_loop;
}

inline bool32 spin_trylock(spinlock_t* lock)
{
    if (lock->lock != 0 || !atomic32_compare_and_swap(&lock->lock, 0, 1)) {
        return FALSE;
    }

    return TRUE;
}

inline void spin_unlock(spinlock_t* lock)
{
    os_mb;
    lock->lock = 0;
}

inline bool32 spin_lock_own(spinlock_t* lock)
{
#ifdef UNIV_MUTEX_DEBUG
    ut_ad(lock->magic_n == SPINLOCK_MAGIC_N);
    return(lock->lock == 1 && os_thread_eq(lock->thread_id, os_thread_get_curr_id()));
#else
    return TRUE;
#endif
}


#ifdef __cplusplus
}
#endif

#endif  /* _CM_MUTEX_H */

