#ifndef _SYNC_RWLOCK_H
#define _SYNC_RWLOCK_H

#include "cm_type.h"
#include "cm_list.h"
#include "cm_mutex.h"
#include "cm_atomic.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Codes used to designate lock operations */
#define RW_LOCK_NOT_LOCKED              350
#define RW_LOCK_EXCLUSIVE               351
#define RW_LOCK_SHARED                  352
#define RW_LOCK_WAIT_EXCLUSIVE          353

#define SYNC_MUTEX              354

typedef struct st_mutex {
    os_event_t event; /*!< Used for the wait queue */
    atomic32_t lock_word; /* This ulint is the target of the atomic test-and-set instruction in Win32 */
    os_mutex_t os_fast_mutex; /* In other systems we use this OS mutex in place of lock_word */
    uint32 waiters;  /* This ulint is set to 1 
                        if there are (or may be) threads waiting in the global wait array
                        for this mutex to be released. Otherwise, this is 0. */
    UT_LIST_NODE_T(struct st_mutex) list; /* All allocated mutexes are put into a list.	Pointers to the next and prev. */
    os_thread_id_t thread_id; /* Debug version: The thread id of the thread which locked the mutex. */
    char* file_name; /* Debug version: File name where the mutex was locked */
    uint32 line; /* Debug version: Line where the mutex was locked */
    uint32 level; /* Debug version: level in the global latching order; default SYNC_LEVEL_NONE */
    char* cfile_name; /* File name where mutex created */
    uint32 cline; /* Line where created */
    uint32 magic_n;
    uint64 count_os_wait; /*!< count of os_wait */
} mutex_t;

typedef struct st_rw_lock rw_lock_t;

/** The latch types that use the sync array. */
union sync_object_t {

    /** RW lock instance */
    rw_lock_t   *lock;

    /** Mutex instance */
    //WaitMutex*	mutex;

    /** Block mutex instance */
    //BlockWaitMutex*	bpmutex;
};

typedef struct st_sync_cell {
    sync_object_t   wait_object; /*!< pointer to the object the thread is waiting for; if NULL the cell is free for use */
    uint32          request_type; /*!< lock type requested on the object */
    const char     *file; /*!< in debug version file where requested */
    uint32          line; /*!< in debug version line where requested */
    os_thread_id_t  thread_id; /*!< thread id of this waiting thread */
    bool32          waiting; /*!< TRUE if the thread has already called sync_array_event_wait on this cell */
    int64           signal_count;  /*!< We capture the signal_count of the wait_object when we reset the event.
                            This value is then passed on to os_event_wait and we wait only if the event
                            has not been signalled in the period between the reset and wait call. */
    time_t          reservation_time;/*!< time when the thread reserved the wait cell */
} sync_cell_t;

typedef struct st_sync_array {
    uint32          n_reserved;  /*!< number of currently reserved cells in the wait array */
    uint32          n_cells;  /*!< number of cells in the wait array */
    sync_cell_t    *array;  /*!< pointer to wait array */
    spinlock_t      lock;
    uint32          res_count; /*!< count of cell reservations since creation of the array */
    uint32           next_free_slot; /*!< the next free cell in the array */
    uint32           first_free_slot;/*!< the last slot that was freed */
} sync_array_t;


void mutex_enter_func(mutex_t *mutex, const char *file_name, uint32 line);
bool32 mutex_enter_nowait_func(mutex_t *mutex, const char *file_name, uint32 line);
void mutex_exit_func(mutex_t *mutex);
bool32 mutex_own(const mutex_t *mutex);
void mutex_create_func(mutex_t *mutex, const char *cfile_name, uint32 cline);
void mutex_destroy_func(mutex_t *mutex);

void sync_init(void);
void sync_close(void);



#define MUTEX_MAGIC_N           (uint32)979585

#define mutex_enter(M)            mutex_enter_func((M), __FILE__, __LINE__)
#define mutex_enter_nowait(M)     mutex_enter_nowait_func((M), __FILE__, __LINE__)
#define mutex_exit(M)             mutex_exit_func(M)
#define mutex_create(M) mutex_create_func((M), __FILE__, __LINE__)
#define mutex_destroy(M)          mutex_destroy_func(M)


/**************************************************************************************
*
**************************************************************************************/


/* Latch types; these are used also in btr0btr.h and mtr0mtr.h: keep the
numerical values smaller than 30 (smaller than BTR_MODIFY_TREE and
MTR_MEMO_MODIFY) and the order of the numerical values like below! and they
should be 2pow value to be used also as ORed combination of flag. */
enum rw_lock_type_t {
    RW_S_LATCH = 1,
    RW_X_LATCH = 2,
    RW_SX_LATCH = 4,
    RW_NO_LATCH = 8
};

/* We decrement lock_word by X_LOCK_DECR for each x_lock.
It is also the start value for the lock_word, meaning that it limits the maximum number
of concurrent read locks before the rw_lock breaks. */
/* We decrement lock_word by X_LOCK_HALF_DECR for sx_lock. */
#define X_LOCK_DECR         0x20000000
#define X_LOCK_HALF_DECR    0x10000000


/** The structure for storing debug info of an rw-lock.
All access to this structure must be protected by rw_lock_debug_mutex_enter(). */
struct rw_lock_debug_t
{
    os_thread_id_t thread_id;  /*!< The thread id of the thread which locked the rw-lock */
    uint32         pass;    /*!< Pass value given in the lock operation */
    uint32         lock_type;   /*!< Type of the lock: RW_LOCK_EX, RW_LOCK_SHARED, RW_LOCK_WAIT_EX */
    const char    *file_name;  /*!< File name where the lock was obtained */
    uint32         line;  /*!< Line where the rw-lock was locked */
    UT_LIST_NODE_T(rw_lock_debug_t) list_node; /*!< Debug structs are linked in a two-way list */
};


/** The structure used in the spin lock implementation of a read-write lock.
Several threads may have a shared lock simultaneously in this lock,
but only one writer may have an exclusive lock, in which case no shared locks are allowed.
To prevent starving of a writer blocked by readers, a writer may queue for x-lock by decrementing lock_word:
no new readers will be let in while the thread waits for readers to exit. */

struct st_rw_lock
{
    /** Holds the state of the lock. */
    atomic32_t lock_word;

    /** 1: there are waiters */
    atomic32_t waiters;

    /** Default value FALSE which means the lock is non-recursive.
    The value is typically set to TRUE making normal rw_locks recursive.
    In case of asynchronous IO, when a non-zero value of 'pass' is
    passed then we keep the lock non-recursive.

    This flag also tells us about the state of writer_thread field.
    If this flag is set then writer_thread MUST contain the thread
    id of the current x-holder or wait-x thread.  This flag must be
    reset in x_unlock functions before incrementing the lock_word */
    volatile bool32 recursive;

    /** number of granted SX locks. */
    volatile uint32 sx_recursive;

    /** This is TRUE if the writer field is RW_LOCK_X_WAIT; this field
    is located far from the memory update hotspot fields which are at
    the start of this struct, thus we can peek this field without
    causing much memory bus traffic */
    bool32 writer_is_wait_ex;

    /** Thread id of writer thread. Is only guaranteed to have sane
    and non-stale value iff recursive flag is set. */
    atomic32_t writer_thread;

    /** Used by sync0arr.cc for thread queueing */
    os_event_t event;

    /** Event for next-writer to wait on. A thread must decrement
    lock_word before waiting. */
    os_event_t wait_ex_event;

    /** File name where lock created */
    const char *cfile_name;

    /** last s-lock file/line is not guaranteed to be correct */
    const char *last_s_file_name;

    /** File name where last x-locked */
    const char *last_x_file_name;

    /** Line where created */
    unsigned cline : 13;

    /** If 1 then the rw-lock is a block lock */
    unsigned is_block_lock : 1;

    /** Line number where last time s-locked */
    unsigned last_s_line : 14;

    /** Line number where last time x-locked */
    unsigned last_x_line : 14;

    /** Count of os_waits. May not be accurate */
    uint32_t count_os_wait;

    /** All allocated rw locks are put into a list */
    UT_LIST_NODE_T(rw_lock_t) list_node;

    /** The mutex protecting rw_lock_t */
    mutable mutex_t mutex;

    UT_LIST_BASE_NODE_T(rw_lock_debug_t) debug_list;  /*!< In the debug version: pointer to the debug info list of the lock */
    uint32 level;  /*!< Level in the global latching order. */

#define RW_LOCK_MAGIC_N 22643
    /** For checking memory corruption. */
    uint32 magic_n;

};

void rw_lock_create_func(rw_lock_t *lock, const char *cfile_name, uint32 cline);
void rw_lock_destroy_func(rw_lock_t *lock);
bool rw_lock_validate(const rw_lock_t *lock);

void sync_check_init(size_t max_threads);


#define rw_lock_create(L) rw_lock_create_func((L), __FILE__, __LINE__)
#define rw_lock_destroy(L) rw_lock_destroy_func((L))

#define rw_lock_s_lock(M) rw_lock_s_lock_func((M), 0, __FILE__, __LINE__)
#define rw_lock_s_lock_inline(M, P, F, L) rw_lock_s_lock_func((M), (P), (F), (L))
#define rw_lock_s_lock_gen(M, P) rw_lock_s_lock_func((M), (P), __FILE__, __LINE__)
#define rw_lock_s_lock_nowait(M, F, L) rw_lock_s_lock_low((M), 0, (F), (L))

#define rw_lock_s_unlock_gen(L) rw_lock_s_unlock_func(L)

#define rw_lock_x_lock(M) rw_lock_x_lock_func((M), 0, __FILE__, __LINE__)
#define rw_lock_x_lock_gen(M, P) rw_lock_x_lock_func((M), (P), __FILE__, __LINE__)
#define rw_lock_x_lock_nowait(M) rw_lock_x_lock_func_nowait((M), __FILE__, __LINE__)

#define rw_lock_x_unlock_gen(L) rw_lock_x_unlock_func(L)


#ifdef __cplusplus
}
#endif

#endif  /* _SYNC_RWLOCK_H */

