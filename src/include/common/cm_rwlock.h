#ifndef _CM_RWLOCK_H
#define _CM_RWLOCK_H

#include "cm_type.h"
#include "cm_list.h"
#include "cm_mutex.h"
#include "cm_atomic.h"
#include "cm_counter.h"
#include "cm_memory.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Codes used to designate lock operations */
#define RW_LOCK_NOT_LOCKED              0
#define RW_LOCK_EXCLUSIVE               1
#define RW_LOCK_SHARED                  2
#define RW_LOCK_WAIT_EXCLUSIVE          3


/** Counters for RW locks. */
struct rw_lock_stats_t {
    typedef counter_t<uint64, COUNTER_SLOTS_64> uint64_counter_t;

    /** number of spin waits on rw-latches, resulted during shared (read) locks */
    uint64_counter_t     rw_s_spin_wait_count;

    /** number of spin loop rounds on rw-latches, resulted during shared (read) locks */
    uint64_counter_t     rw_s_spin_round_count;

    /** number of OS waits on rw-latches, resulted during shared (read) locks */
    uint64_counter_t     rw_s_os_wait_count;

    /** number of spin waits on rw-latches, resulted during exclusive (write) locks */
    uint64_counter_t     rw_x_spin_wait_count;

    /** number of spin loop rounds on rw-latches, resulted during exclusive (write) locks */
    uint64_counter_t     rw_x_spin_round_count;

    /** number of OS waits on rw-latches, resulted during exclusive (write) locks */
    uint64_counter_t     rw_x_os_wait_count;
};

/* Latch types */
typedef enum en_rw_lock_type {
    RW_S_LATCH = 1,
    RW_X_LATCH = 2,
    RW_NO_LATCH = 4
} rw_lock_type_t;

/* We decrement lock_word by X_LOCK_DECR for each x_lock. It is also the start value for the lock_word,
   meaning that it limits the maximum number of concurrent read locks before the rw_lock breaks.
   The current value of 0x00100000 allows 1,048,575 concurrent readers and 2047 recursive writers.*/
#define X_LOCK_DECR         0x00100000

struct rw_lock_t;

   /* The structure for storing debug info of an rw-lock. */
struct rw_lock_debug_info_t {
    rw_lock_t     *lock;
    os_thread_id_t thread_id;  /*!< The thread id of the thread which locked the rw-lock */
    uint32         pass;    /*!< Pass value given in the lock operation */
    uint32         lock_type;   /*!< Type of the lock: RW_LOCK_EX, RW_LOCK_SHARED, RW_LOCK_WAIT_EX */
    const char    *file_name;  /*!< File name where the lock was obtained */
    uint32         line;  /*!< Line where the rw-lock was locked */
    UT_LIST_NODE_T(rw_lock_debug_info_t) list_node; /*!< Debug structs are linked in a two-way list */
};

/* The structure used in the spin lock implementation of a read-write lock */
struct rw_lock_t
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

    /** Thread id of writer thread. Is only guaranteed to have sane
    and non-stale value iff recursive flag is set. */
    atomic32_t writer_thread_id;

    /** Used by sync0arr.cc for thread queueing */
    os_event_t event;

    /** Event for next-writer to wait on. A thread must decrement lock_word before waiting. */
    os_event_t wait_ex_event;

    /** File name where lock created */
    const char *cfile_name;
    /** last s-lock file/line is not guaranteed to be correct */
    const char *last_s_file_name;
    /** File name where last x-locked */
    const char *last_x_file_name;

    /** Line where created */
    uint64 cline : 16;
    /** Line number where last time s-locked */
    uint64 last_s_line : 16;
    /** Line number where last time x-locked */
    uint64 last_x_line : 16;
    uint64 reserved : 16;

    /** All allocated rw locks are put into a list */
    UT_LIST_NODE_T(rw_lock_t) list_node;

#ifdef UNIV_DEBUG
    /* In the debug version: pointer to the debug info list of the lock */
    UT_LIST_BASE_NODE_T(rw_lock_debug_info_t) debug_list;
#endif

#define RW_LOCK_MAGIC_N 22643
    /** For checking memory corruption. */
    uint32 magic_n;
};

#define SYNC_WAIT_ARRAY_SIZE         64

/** The latch types that use the sync array. */
union sync_object_t {

    /** RW lock instance */
    rw_lock_t*   lock;

    /** Mutex instance */
    //WaitMutex* mutex;
};

struct sync_cell_t {
    sync_object_t   wait_object; /*!< pointer to the object the thread is waiting for; if NULL the cell is free for use */
    const char     *file; /*!< in debug version file where requested */
    uint32          line; /*!< in debug version line where requested */
    uint32          request_type; /*!< lock type requested on the object */
    os_thread_id_t  thread_id; /*!< thread id of this waiting thread */
    bool32          waiting; /*!< TRUE if the thread has already called sync_array_event_wait on this cell */
    uint64          signal_count;  /*!< We capture the signal_count of the wait_object when we reset the event.
                            This value is then passed on to os_event_wait and we wait only if the event
                            has not been signalled in the period between the reset and wait call. */
    UT_LIST_NODE_T(sync_cell_t) list_node;
};

struct sync_array_t {
    mutex_t       sync_cell_mutex;  // protect sync_cell_t
    mutex_t       free_sync_cell_list_mutex;
    mutex_stats_t free_sync_cell_list_mutex_stats;
    UT_LIST_BASE_NODE_T(sync_cell_t) free_sync_cell_list;

    sync_cell_t* alloc_sync_cell();
    void free_sync_cell(sync_cell_t* sync_cell);
};


class rw_lock_sync_mgr_t {
public:
    rw_lock_sync_mgr_t();
    ~rw_lock_sync_mgr_t();

    status_t init(memory_pool_t* pool);
    void destroy();

    sync_array_t* get_sync_array(uint32 index);
    void print_sync_wait_info(FILE *file);

    sync_cell_t* alloc_sync_cell();

#ifdef UNIV_DEBUG
    void debug_mutex_enter();
    void debug_mutex_exit();
    rw_lock_debug_info_t* alloc_rwlock_debug_info();
    void free_rwlock_debug_info(rw_lock_debug_info_t* info);
    bool32 expand_rwlock_debug_info();
    void remove_rwlock_debug_info(rw_lock_t* lock, uint32 pass, uint32 lock_type);
    void add_rwlock_debug_info(rw_lock_t* lock, uint32 pass, uint32 lock_type, const char *file_name, uint32 line);
#endif

private:
    bool32 expand_sync_cell();

private:
    memory_pool_t*   m_pool;
    sync_array_t     m_sync_wait_array[SYNC_WAIT_ARRAY_SIZE];
    uint32           m_sync_array_size;
    atomic32_t       m_sync_array_index;

    mutex_t          m_used_page_list_mutex;
    mutex_t          m_free_sync_cell_list_mutex;

#ifdef UNIV_DEBUG
    os_mutex_t       m_rw_lock_debug_mutex;
    os_event_t       m_rw_lock_debug_event;
    bool32           m_rw_lock_debug_waiters;
    UT_LIST_BASE_NODE_T(rw_lock_debug_info_t) m_free_rwlock_debug_list;
#endif

    UT_LIST_BASE_NODE_T(sync_cell_t) m_free_sync_cell_list;
    UT_LIST_BASE_NODE_T(memory_page_t) m_used_page_list;
};


extern status_t sync_init(memory_pool_t* pool);
extern void sync_close(void);

extern inline void rw_lock_create_func(rw_lock_t *lock, const char *cfile_name, uint32 cline);
extern inline void rw_lock_destroy_func(rw_lock_t *lock);

extern inline void rw_lock_s_lock_func(rw_lock_t *lock, uint32 pass, const char *file_name, uint32 line);
extern inline void rw_lock_s_unlock_func(uint32 pass, rw_lock_t *lock);
extern inline bool32 rw_lock_s_lock_low(rw_lock_t *lock, uint32 pass, const char *file_name, uint32 line);

extern inline void rw_lock_x_lock_func(rw_lock_t *lock, uint32 pass, const char *file_name, uint32 line);
extern inline void rw_lock_x_unlock_func(uint32 pass, rw_lock_t *lock);
extern inline bool32 rw_lock_x_lock_func_nowait(rw_lock_t *lock, const char* file_name, uint32 line);

extern inline bool32 rw_lock_validate(const rw_lock_t *lock);
#ifdef UNIV_DEBUG
extern inline bool32 rw_lock_own(rw_lock_t *lock, uint32 lock_type);
#endif

extern inline uint32 rw_lock_get_reader_count(const rw_lock_t *lock);
extern inline uint32 rw_lock_get_x_lock_count(const rw_lock_t *lock);


#define rw_lock_create(M) rw_lock_create_func((M), __FILE__, __LINE__)
#define rw_lock_destroy(M) rw_lock_destroy_func((M))

#define rw_lock_s_lock(M) rw_lock_s_lock_func((M), 0, __FILE__, __LINE__)
#define rw_lock_s_lock_gen(M, P) rw_lock_s_lock_func((M), (P), __FILE__, __LINE__)
#define rw_lock_s_lock_nowait(M, F, L) rw_lock_s_lock_low((M), 0, (F), (L))
#define rw_lock_s_unlock(M) rw_lock_s_unlock_func(0, M)
#define rw_lock_s_unlock_gen(M, P) rw_lock_s_unlock_func((P), (M))

#define rw_lock_x_lock(M) rw_lock_x_lock_func((M), 0, __FILE__, __LINE__)
#define rw_lock_x_lock_gen(M, P) rw_lock_x_lock_func((M), (P), __FILE__, __LINE__)
#define rw_lock_x_lock_nowait(M) rw_lock_x_lock_func_nowait((M), __FILE__, __LINE__)
#define rw_lock_x_unlock(M) rw_lock_x_unlock_func(0, M)
#define rw_lock_x_unlock_gen(M, P) rw_lock_x_unlock_func((P), (M))


#ifdef __cplusplus
}
#endif

#endif  /* _CM_RWLOCK_H */

