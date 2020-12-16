#ifndef _OS_MUTEX_H
#define _OS_MUTEX_H

#include "os_type.h"



#ifdef __cplusplus
extern "C" {
#endif


#define OS_SYNC_TIME_EXCEEDED   1
#define OS_SYNC_TIME_FAIL       2

/** Denotes an infinite delay for os_event_wait_time() */
#define OS_SYNC_INFINITE_TIME   ULINT_UNDEFINED

#if defined(__GNUC__) && ((__GNUC__ > 3) || (__GNUC__ == 3 && __GNUC_MINOR__ > 3))
#define UNLIKELY(x)     __builtin_expect(!!(x), 1)
#define LIKELY(x)       __builtin_expect(!!(x), 0)
#else
#define UNLIKELY(x)     (x)
#define LIKELY(x)       (x)
#endif


#ifdef __WIN__
/** Native mutex */
typedef CRITICAL_SECTION	os_mutex_t;
/** Native condition variable. */
typedef CONDITION_VARIABLE	os_cond_t;
#else
/** Native mutex */
typedef pthread_mutex_t		os_mutex_t;
/** Native condition variable */
typedef pthread_cond_t		os_cond_t;
#endif

struct os_event_struct {
    os_mutex_t      os_mutex;   /* this mutex protects the next fields */
    bool32          is_set;     /* this is TRUE if the next mutex is not reserved */
    os_cond_t       cond_var;   /* condition variable is used in waiting for the event */
};

typedef struct os_event_struct os_event_struct_t;
typedef os_event_struct_t*     os_event_t;

os_event_t os_event_create(char* name);
void os_event_set(os_event_t event);
void os_event_set_signal(os_event_t event);
void os_event_reset(os_event_t event);
void os_event_free(os_event_t event);
void os_event_wait(os_event_t event);
int os_event_wait_time(os_event_t event,uint32 time);



void os_mutex_init(os_mutex_t*    fast_mutex);
void os_mutex_lock(os_mutex_t*    fast_mutex);
void os_mutex_unlock(os_mutex_t*  fast_mutex);
void os_mutex_free(os_mutex_t* fast_mutex);






/***********************************************************************************************
*                                     spin lock                                                *
***********************************************************************************************/

typedef volatile uint32 spinlock_t;

#if defined(__arm__) || defined(__aarch64__)
#define M_INIT_SPIN_LOCK(lock)                          \
    {                                                   \
        __atomic_store_n(&lock, 0, __ATOMIC_SEQ_CST);   \
    }
#else
#define M_INIT_SPIN_LOCK(lock)                          \
    {                                                   \
        (lock) = 0;                                     \
    }
#endif

#define M_SPIN_COUNT        1000
#define SPIN_STAT_INC(stat, item)                       \
    {                                                   \
        if ((stat) != NULL) {                           \
            ((stat)->item)++;                           \
        }                                               \
    }

typedef struct st_spin_statis
{
    uint64     spins;
    uint64     wait_usecs;
    uint64     fails;
} spin_statis_t;

typedef struct st_recursive_lock
{
    spinlock_t mutex;
    uint16     sid;
    uint16     r_cnt;
} recursive_lock_t;

#if defined(__arm__) || defined(__aarch64__)
#define fas_cpu_pause()                          \
    {                                            \
        __asm__ volatile("nop");                 \
    }
#else
#define fas_cpu_pause()                          \
    {                                            \
        __asm__ volatile("pause");               \
    }
#endif


void spin_sleep_and_stat(spin_statis_t *stat);

#ifdef __WIN__

static inline uint32 spin_set(spinlock_t *ptr, uint32 value)
{
    return (uint32)InterlockedExchange(ptr, value);
}

static inline void spin_sleep()
{
    Sleep(1);
}

static inline void spin_sleep_ex(uint32 tick)
{
    Sleep(tick);
}

#else

#if defined(__arm__) || defined(__aarch64__)
static inline uint32 spin_set(spinlock_t *ptr, uint32 value)
{
    uint32  old_value = 0;
    return !__atomic_compare_exchange_n(ptr, &old_value,  value, FALSE, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
}

static inline void spin_unlock(spinlock_t *lock)
{
    __atomic_store_n(lock, 0, __ATOMIC_SEQ_CST);
}

#else
static inline uint32 spin_set(spinlock_t *ptr, uint32 value)
{
    uint32  old_value = 0;
    return (uint32)__sync_val_compare_and_swap(ptr, old_value,  value);
}
#endif

static inline void spin_sleep()
{
    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 100;
    nanosleep(&ts, NULL);
}

static inline void spin_sleep_ex(uint32 tick)
{
    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = tick;
    nanosleep(&ts, NULL);
}

#endif

static inline void spin_lock(spinlock_t *lock, spin_statis_t *stat)
{
#ifndef __WIN__
    uint32 i;
#endif
    uint32 spin_times = 0;
    uint32 sleep_times = 0;

    if (UNLIKELY(lock == NULL))
        return;

    for (;;)
    {
#if defined(__arm__) || defined(__aarch64__)
        while (__atomic_load_n(lock, __ATOMIC_SEQ_CST) != 0) {
#else
        while (*lock != 0) {
#endif
            SPIN_STAT_INC(stat, spins);
            spin_times++;
            if (UNLIKELY(spin_times == M_SPIN_COUNT))
            {
                spin_sleep_and_stat(stat);
                spin_times = 0;
            }
        }

        if (LIKELY(spin_set(lock, 1) == 0))
        {
            break;
        }

        SPIN_STAT_INC(stat, fails);
        sleep_times++;
#ifndef __WIN__
        for (i = 0; i < sleep_times; i++)
        {
            fas_cpu_pause();
        }
#endif
    }
}

static inline void spin_lock_ex(spinlock_t *lock, spin_statis_t *stat, uint32 spin_count)
{
#ifndef __WIN__
    uint32 i;
#endif
    uint32 spin_times = 0;
    uint32 sleep_times = 0;

    if (UNLIKELY(lock == NULL))
        return;

    for (;;)
    {
#if defined(__arm__) || defined(__aarch64__)
        while (__atomic_load_n(lock, __ATOMIC_SEQ_CST) != 0) {
#else
        while (*lock != 0) {
#endif
            SPIN_STAT_INC(stat, spins);
            spin_times++;

#ifndef __WIN__
            fas_cpu_pause();
#endif

            if (UNLIKELY(spin_times == spin_count))
            {
                spin_sleep_and_stat(stat);
                spin_times = 0;
            }
        }

        if (spin_set(lock, 1) != 0)
        {
            SPIN_STAT_INC(stat, fails);
            sleep_times++;
#ifndef __WIN__
            for (i = 0; i < sleep_times; i++)
            {
                fas_cpu_pause();
            }
#endif            
            continue;
        }
        break;
    }
}

#if !defined(__arm__) || !defined(__aarch64__)
static inline void spin_unlock(spinlock_t *lock)
{
    if (UNLIKELY(lock == NULL))
    {
        return;
    }

    *lock = 0;
}
#endif

static inline bool32 spin_try_lock(spinlock_t *lock)
{
#if defined(__arm__) || defined(__aarch64__)
    if (__atomic_load_n(lock, __ATOMIC_SEQ_CST) != 0) {
#else
    if (*lock != 0) {
#endif
        return FALSE;
    }

    return (spin_set(lock, 1) == 0);
}

static inline bool32 spin_timed_lock(spinlock_t *lock, uint32 timeout_ticks)
{
#ifndef __WIN__
    uint32 i;
#endif
    uint32 spin_times = 0, wait_ticks = 0;
    uint32 sleep_times = 0;

    for (;;)
    {
#if defined(__arm__) || defined(__aarch64__)
        while (__atomic_load_n(lock, __ATOMIC_SEQ_CST) != 0) {
#else
        while (*lock != 0) {
#endif
            if (UNLIKELY(wait_ticks >= timeout_ticks))
            {
                return FALSE;
            }

#ifndef __WIN__
            fas_cpu_pause();
#endif

            spin_times++;
            if (UNLIKELY(spin_times == M_SPIN_COUNT))
            {
                spin_sleep();
                spin_times = 0;
                wait_ticks++;
            }
        }

        if (spin_set(lock, 1) != 0)
        {
            sleep_times++;
#ifndef __WIN__
            for (i = 0; i < sleep_times; i++)
            {
                fas_cpu_pause();
            }
#endif            
            continue;
        }
        break;
    }

    return TRUE;
}


#ifdef __cplusplus
}
#endif

#endif  /* _OS_MUTEX_H */

