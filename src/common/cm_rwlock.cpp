#include "cm_rwlock.h"
#include "cm_dbug.h"
#include "cm_memory.h"
#include "cm_random.h"
#include "cm_log.h"

#define SYNC_SPIN_ROUNDS         30
#define SYNC_SPIN_WAIT_DELAY     6

static rw_lock_sync_mgr_t   rw_lock_sync_mgr;
static rw_lock_stats_t      rw_lock_stats;
static bool32               sync_initialized = FALSE;

rw_lock_sync_mgr_t::rw_lock_sync_mgr_t()
{
    m_pool = NULL;
    m_sync_array_index = 0;
    m_sync_array_size = SYNC_WAIT_ARRAY_SIZE;
    for (uint32 i = 0; i < m_sync_array_size; i++) {
        UT_LIST_INIT(m_sync_wait_array[i].free_sync_cell_list);
        mutex_create(&m_sync_wait_array[i].free_sync_cell_list_mutex);
        mutex_create(&m_sync_wait_array[i].sync_cell_mutex);
        memset(&m_sync_wait_array[i].free_sync_cell_list_mutex_stats, 0x00, sizeof(mutex_stats_t));
    }

    os_mutex_create(&m_rw_lock_debug_mutex);
    m_rw_lock_debug_event = os_event_create("rw_lock_debug_event");
    m_rw_lock_debug_waiters = FALSE;
    UT_LIST_INIT(m_free_rwlock_debug_list);

    mutex_create(&m_used_page_list_mutex);
    UT_LIST_INIT(m_used_page_list);
}

rw_lock_sync_mgr_t::~rw_lock_sync_mgr_t()
{
    destroy();

    for (uint32 i = 0; i < m_sync_array_size; i++) {
        mutex_destroy(&m_sync_wait_array[i].free_sync_cell_list_mutex);
        mutex_destroy(&m_sync_wait_array[i].sync_cell_mutex);
    }
    os_mutex_destroy(&m_rw_lock_debug_mutex);
    os_event_destroy(m_rw_lock_debug_event);
    mutex_destroy(&m_used_page_list_mutex);
}

#ifdef UNIV_DEBUG

bool32 rw_lock_sync_mgr_t::expand_rwlock_debug_info()
{
    memory_page_t* page;
    rw_lock_debug_info_t* info;
    uint32 count;

    page = mpool_alloc_page(m_pool);
    if (page == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_RWLOCK,
            "expand_rwlock_debug: failed to alloc page from memory pool %p", m_pool);
        return FALSE;
    }
    count = m_pool->page_size / sizeof(rw_lock_debug_info_t);
    for (uint32 i = 0; i < count; i++) {
        info = (rw_lock_debug_info_t*)(MEM_PAGE_DATA_PTR(page) + sizeof(rw_lock_debug_info_t) * i);
        UT_LIST_ADD_LAST(list_node, m_free_rwlock_debug_list, info);
    }
    return TRUE;
}

rw_lock_debug_info_t* rw_lock_sync_mgr_t::alloc_rwlock_debug_info()
{
    rw_lock_debug_info_t* info;

retry:

    info = UT_LIST_GET_FIRST(m_free_rwlock_debug_list);
    if (info) {
        UT_LIST_REMOVE(list_node, m_free_rwlock_debug_list, info);
        return info;
    }

    if (!expand_rwlock_debug_info()) {
        return NULL;
    }

    goto retry;
}

void rw_lock_sync_mgr_t::free_rwlock_debug_info(rw_lock_debug_info_t* info)
{
    UT_LIST_ADD_LAST(list_node, m_free_rwlock_debug_list, info);
}

void rw_lock_sync_mgr_t::remove_rwlock_debug_info(
    rw_lock_t   *lock,  /*!< in: rw-lock */
    uint32       pass,  /*!< in: pass value */
    uint32       lock_type)  /*!< in: lock type */
{
    rw_lock_debug_info_t* info;

    ut_ad(lock);

    rw_lock_sync_mgr.debug_mutex_enter();

    for (info = UT_LIST_GET_FIRST(lock->debug_list);
         info != 0;
         info = UT_LIST_GET_NEXT(list_node, info)) {

        if (pass == info->pass
            && (pass != 0 || os_thread_eq(info->thread_id, os_thread_get_curr_id()))
            && info->lock_type == lock_type) {

            /* Found! */
            UT_LIST_REMOVE(list_node, lock->debug_list, info);
            rw_lock_sync_mgr.free_rwlock_debug_info(info);

            rw_lock_sync_mgr.debug_mutex_exit();
            return;
        }
    }

    ut_error;
}

void rw_lock_sync_mgr_t::add_rwlock_debug_info(
    rw_lock_t  *lock,   /*!< in: rw-lock */
    uint32      pass,   /*!< in: pass value */
    uint32      lock_type,  /*!< in: lock type */
    const char *file_name,  /*!< in: file where requested */
    uint32      line)   /*!< in: line where requested */
{
    ut_ad(file_name != NULL);

    rw_lock_debug_info_t *info;

    rw_lock_sync_mgr.debug_mutex_enter();

    info = rw_lock_sync_mgr.alloc_rwlock_debug_info();
    info->lock = lock;
    info->pass = pass;
    info->line = line;
    info->lock_type = lock_type;
    info->file_name = file_name;
    info->thread_id = os_thread_get_curr_id();
    UT_LIST_ADD_FIRST(list_node, lock->debug_list, info);

    rw_lock_sync_mgr.debug_mutex_exit();
}

void rw_lock_sync_mgr_t::debug_mutex_enter()
{
    uint64 signal_count;

    for (;;) {
        if (os_mutex_try_enter(&m_rw_lock_debug_mutex)) {
            return;
        }

        signal_count = os_event_reset(m_rw_lock_debug_event);
        m_rw_lock_debug_waiters = TRUE;

        if (os_mutex_try_enter(&m_rw_lock_debug_mutex)) {
            return;
        }

        os_event_wait(m_rw_lock_debug_event, signal_count);
    }
}

void rw_lock_sync_mgr_t::debug_mutex_exit()
{
    os_mutex_exit(&m_rw_lock_debug_mutex);

    if (m_rw_lock_debug_waiters) {
        m_rw_lock_debug_waiters = FALSE;
        os_event_set(m_rw_lock_debug_event);
    }
}

#endif

bool32 rw_lock_sync_mgr_t::expand_sync_cell()
{
    memory_page_t* page;
    sync_cell_t* sync_cell;
    uint32 count;

    page = mpool_alloc_page(m_pool);
    if (page == NULL) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_RWLOCK,
            "expand_sync_cell: failed to alloc page from memory pool %p", m_pool);
        return FALSE;
    }
    count = m_pool->page_size / sizeof(sync_cell_t);
    for (uint32 i = 0; i < count; i++) {
        sync_cell = (sync_cell_t*)(MEM_PAGE_DATA_PTR(page) + sizeof(sync_cell_t) * i);
        UT_LIST_ADD_LAST(list_node, m_free_sync_cell_list, sync_cell);
    }
    return TRUE;
}

sync_cell_t* rw_lock_sync_mgr_t::alloc_sync_cell()
{
    sync_cell_t* sync_cell;

    mutex_enter(&m_free_sync_cell_list_mutex);

retry:

    sync_cell = UT_LIST_GET_FIRST(m_free_sync_cell_list);
    if (sync_cell) {
        UT_LIST_REMOVE(list_node, m_free_sync_cell_list, sync_cell);
        mutex_exit(&m_free_sync_cell_list_mutex);
        return sync_cell;
    }

    if (!expand_sync_cell()) {
        mutex_exit(&m_free_sync_cell_list_mutex);
        return NULL;
    }

    goto retry;
}

sync_array_t* rw_lock_sync_mgr_t::get_sync_array(uint32 index)
{
    return (&m_sync_wait_array[index % m_sync_array_size]);
}

status_t rw_lock_sync_mgr_t::init(memory_pool_t* pool)
{
    m_pool = pool;
    if (!expand_rwlock_debug_info()) {
        return CM_ERROR;
    }

    uint32 cell_count_per_sync_array = 2;
    for (uint32 i = 0; i < m_sync_array_size; i++) {
        sync_array_t* arr = &m_sync_wait_array[i];
        for (uint32 j = 0; j < cell_count_per_sync_array; j++) {
            sync_cell_t* cell = alloc_sync_cell();
            if (cell == NULL) {
                return CM_ERROR;
            }
            arr->free_sync_cell(cell);
        }
    }

    return CM_SUCCESS;
}

void rw_lock_sync_mgr_t::destroy()
{
    for (uint32 i = 0; i < m_sync_array_size; i++) {
        mutex_enter(&m_sync_wait_array[i].free_sync_cell_list_mutex);
        UT_LIST_INIT(m_sync_wait_array[i].free_sync_cell_list);
        mutex_exit(&m_sync_wait_array[i].free_sync_cell_list_mutex);
    }

    rw_lock_sync_mgr.debug_mutex_enter();
    UT_LIST_INIT(m_free_rwlock_debug_list);
    rw_lock_sync_mgr.debug_mutex_exit();

    mutex_enter(&m_used_page_list_mutex);
    memory_page_t* page = UT_LIST_GET_FIRST(m_used_page_list);
    while (page) {
        UT_LIST_REMOVE(list_node, m_used_page_list, page);
        mpool_free_page(m_pool, page);
        page = UT_LIST_GET_FIRST(m_used_page_list);
    }
    mutex_exit(&m_used_page_list_mutex);
}

void rw_lock_sync_mgr_t::print_sync_wait_info(FILE *file)
{
  fprintf(file,
          "RW-shared spins %llu, rounds %llu, OS waits %llu\n"
          "RW-exclusive spins %llu, rounds %llu, OS waits %llu\n",
          (uint64)rw_lock_stats.rw_s_spin_wait_count,
          (uint64)rw_lock_stats.rw_s_spin_round_count,
          (uint64)rw_lock_stats.rw_s_os_wait_count,
          (uint64)rw_lock_stats.rw_x_spin_wait_count,
          (uint64)rw_lock_stats.rw_x_spin_round_count,
          (uint64)rw_lock_stats.rw_x_os_wait_count);

  fprintf(
      file,
      "Spin rounds per wait: %.2f RW-shared, %.2f RW-exclusive\n",
      (double)rw_lock_stats.rw_s_spin_round_count /
          ut_max(uint64(1), (uint64)rw_lock_stats.rw_s_spin_wait_count),
      (double)rw_lock_stats.rw_x_spin_round_count /
          ut_max(uint64(1), (uint64)rw_lock_stats.rw_x_spin_wait_count));
}


sync_cell_t* sync_array_t::alloc_sync_cell()
{
    sync_cell_t* sync_cell;

    mutex_enter(&free_sync_cell_list_mutex, &free_sync_cell_list_mutex_stats);

    sync_cell = UT_LIST_GET_FIRST(free_sync_cell_list);
    if (sync_cell) {
        UT_LIST_REMOVE(list_node, free_sync_cell_list, sync_cell);
        mutex_exit(&free_sync_cell_list_mutex);
        return sync_cell;
    }

    mutex_exit(&free_sync_cell_list_mutex);

    sync_cell = rw_lock_sync_mgr.alloc_sync_cell();
    if (sync_cell == NULL) {
        ut_error;
    }

    return sync_cell;
}

void sync_array_t::free_sync_cell(sync_cell_t* sync_cell)
{
    mutex_enter(&free_sync_cell_list_mutex, &free_sync_cell_list_mutex_stats);
    UT_LIST_ADD_LAST(list_node, free_sync_cell_list, sync_cell);
    mutex_exit(&free_sync_cell_list_mutex);
}


#ifdef UNIV_DEBUG

inline bool32 rw_lock_own(rw_lock_t *lock, uint32 lock_type)
{
    ut_ad(lock);
    ut_ad(rw_lock_validate(lock));

    rw_lock_sync_mgr.debug_mutex_enter();

    for (const rw_lock_debug_info_t* info = UT_LIST_GET_FIRST(lock->debug_list);
         info != NULL;
         info = UT_LIST_GET_NEXT(list_node, info)) {

        if (os_thread_eq(info->thread_id, os_thread_get_curr_id())
            && info->pass == 0
            && info->lock_type == lock_type) {

            rw_lock_sync_mgr.debug_mutex_exit();
            /* Found! */
            return TRUE;
        }
    }
    rw_lock_sync_mgr.debug_mutex_exit();

    return FALSE;
}

#endif

static os_event_t sync_cell_get_event(sync_cell_t *cell) /*!< in: non-empty sync array cell */
{
    uint32 type = cell->request_type;

    if (type == RW_LOCK_WAIT_EXCLUSIVE) {
        return(cell->wait_object.lock->wait_ex_event);
    } else { /* RW_LOCK_S and RW_LOCK_X wait on the same event */
        return(cell->wait_object.lock->event);
    }
}

static sync_cell_t* sync_array_reserve_cell(
    sync_array_t   *arr,  /*!< in: wait array */
    void           *object, /*!< in: pointer to the object to wait for */
    uint32          type,  /*!< in: lock request type */
    const char     *file,  /*!< in: file where requested */
    uint32          line)  /*!< in: line where requested */
{
    sync_cell_t *cell;

    cell = arr->alloc_sync_cell();
    cell->request_type = type;
    cell->wait_object.lock = (rw_lock_t*)(object);
    cell->waiting = false;
    cell->file = file;
    cell->line = line;
    cell->thread_id = os_thread_get_curr_id();

    /* Make sure the event is reset and also store the value of signal_count at which the event was reset. */
    os_event_t event = sync_cell_get_event(cell);
    cell->signal_count = os_event_reset(event);

    LOGGER_DEBUG(LOGGER, LOG_MODULE_RWLOCK,
        "sync_array_reserve_cell: arr %p cell %p rwlock %p thread %lu\n",
        arr, cell, object, cell->thread_id);

    return cell;
}

sync_array_t* sync_array_get_and_reserve_cell(
    void         *object, /*!< in: pointer to the object to wait for */
    uint32        type,	/*!< in: lock request type */
    const char   *file,	/*!< in: file where requested */
    uint32        line,	/*!< in: line where requested */
    sync_cell_t **cell)	/*!< out: the cell reserved, never NULL */
{
    sync_array_t *sync_arr = NULL;
    const uint32 loop_count = 16;
    uint32 rnd= (os_thread_get_internal_id() & UINT_MAX32);

    *cell = NULL;
    for (uint32 i = 0; i < loop_count && *cell == NULL; ++i) {
        sync_arr = rw_lock_sync_mgr.get_sync_array(rnd + i);
        *cell = sync_array_reserve_cell(sync_arr, object, type, file, line);
    }

    return sync_arr;
}

static void sync_array_free_cell(sync_array_t *arr, sync_cell_t *&cell)
{
    mutex_enter(&arr->sync_cell_mutex, NULL);
    ut_a(cell->wait_object.lock != NULL);
    cell->wait_object.lock =  NULL;
    cell->waiting = FALSE;
    cell->signal_count = 0;
    mutex_exit(&arr->sync_cell_mutex);

    arr->free_sync_cell(cell);
}

static void sync_array_wait_event(sync_array_t *arr, sync_cell_t*& cell)
{
    mutex_enter(&arr->sync_cell_mutex, NULL);
    ut_a(cell->wait_object.lock);
    ut_a(!cell->waiting);
    ut_ad(os_thread_get_curr_id() == cell->thread_id);
    cell->waiting = TRUE;
    mutex_exit(&arr->sync_cell_mutex);

    os_event_wait(sync_cell_get_event(cell), cell->signal_count);

    sync_array_free_cell(arr, cell);
}

/* Checks that the rw-lock has been initialized and that there are no simultaneous shared and exclusive locks. */
inline bool32 rw_lock_validate(const rw_lock_t *lock)
{
    uint32 waiters;
    int32 lock_word;

    ut_ad(lock);

    waiters = lock->waiters;
    lock_word = lock->lock_word;

    ut_ad(lock->magic_n == RW_LOCK_MAGIC_N);
    ut_ad(waiters == 0 || waiters == 1);
    ut_ad(lock_word > -(2 * X_LOCK_DECR));
    ut_ad(lock_word <= X_LOCK_DECR);

    return TRUE;
}

inline void rw_lock_create_func(
    rw_lock_t    *lock, /*!< in: pointer to memory */
    //uint32        level,
    const char   *cfile_name,  /*!< in: file name where created */
    uint32        cline)             /*!< in: file line where created */
{
    lock->lock_word = X_LOCK_DECR;
    lock->waiters = 0;

    /* We set this value to signify that lock->writer_thread
    contains garbage at initialization and cannot be used for recursive x-locking. */
    lock->recursive = FALSE;
    memset((void *)&lock->writer_thread_id, 0, sizeof lock->writer_thread_id);

    lock->cfile_name = cfile_name;

    /* This should hold in practice. If it doesn't then we need to
    split the source file anyway. Or create the locks on lines less than 8192. cline is unsigned:13. */
    ut_ad(cline <= 8192);
    lock->cline = (unsigned int)cline;
    lock->last_s_file_name = "not yet reserved";
    lock->last_x_file_name = "not yet reserved";
    lock->last_s_line = 0;
    lock->last_x_line = 0;
    lock->event = os_event_create(0);
    lock->wait_ex_event = os_event_create(0);

    //spin_lock(&rw_lock_list_lock, NULL);
    //ut_ad(UT_LIST_GET_FIRST(rw_lock_list) == NULL || UT_LIST_GET_FIRST(rw_lock_list)->magic_n == RW_LOCK_MAGIC_N);
    //UT_LIST_ADD_FIRST(list_node, rw_lock_list, lock);
    //spin_unlock(&rw_lock_list_lock);

    UT_LIST_INIT(lock->debug_list);
    lock->magic_n = RW_LOCK_MAGIC_N;
}


inline void rw_lock_destroy_func(rw_lock_t *lock)
{
    os_rmb;
    ut_ad(rw_lock_validate(lock));
    ut_a(lock->lock_word == X_LOCK_DECR);

    //spin_lock(&rw_lock_list_lock, NULL);
    //os_event_destroy(lock->event);
    //os_event_destroy(lock->wait_ex_event);
    //UT_LIST_REMOVE(list_node, rw_lock_list, lock);
    //spin_unlock(&rw_lock_list_lock);
}

inline bool32 rw_lock_lock_word_decr(rw_lock_t *lock, uint32 amount)
{
    os_rmb;
    int32 local_lock_word = lock->lock_word;
    while (local_lock_word > 0) {
        if (atomic32_compare_and_swap(&lock->lock_word, local_lock_word, local_lock_word - amount)) {
            return TRUE;
        }
        local_lock_word = lock->lock_word;
    }
    return FALSE;
}

inline int32 rw_lock_lock_word_incr(rw_lock_t *lock, uint32 amount)
{
    return(atomic32_add(&lock->lock_word, amount));
}

inline bool32 rw_lock_s_lock_low(
    rw_lock_t  *lock, /*!< in: pointer to rw-lock */
    uint32      pass, /*!< in: pass value; != 0, if the lock will be passed to another thread to unlock */
    const char *file_name, /*!< in: file name where lock requested */
    uint32      line) /*!< in: line where requested */
{
    if (!rw_lock_lock_word_decr(lock, 1)) {
        /* Locking did not succeed */
        return FALSE;
    }

    ut_d(rw_lock_sync_mgr.add_rwlock_debug_info(lock, pass, RW_LOCK_SHARED, file_name, line));

    /* These debugging values are not set safely: they may be incorrect
    or even refer to a line that is invalid for the file name. */
    lock->last_s_file_name = file_name;
    lock->last_s_line = line;

    return(TRUE);	/* locking succeeded */
}

inline void rw_lock_set_waiter_flag(rw_lock_t *lock)
{
    (void)atomic32_compare_and_swap(&lock->waiters, 0, 1);
}

inline void rw_lock_reset_waiter_flag(rw_lock_t *lock)
{
    (void) atomic32_compare_and_swap(&lock->waiters, 1, 0);
}

inline void rw_lock_s_lock_spin(
    rw_lock_t   *lock, /*!< in: pointer to rw-lock */
    uint32       pass, /*!< in: pass value; != 0, if the lock will be passed to another thread to unlock */
    const char  *file_name, /*!< in: file name where lock requested */
    uint32       line) /*!< in: line where requested */
{
    uint32          i = 0; /* spin round count */
    sync_array_t   *sync_arr;
    uint32          spin_count = 0;
    uint32          count_os_wait = 0;

    /* We reuse the thread id to index into the counter, cache it here for efficiency. */
    uint64          thread_internal_id = os_thread_get_internal_id();

    ut_ad(rw_lock_validate(lock));
    rw_lock_stats.rw_s_spin_wait_count.add(thread_internal_id, 1);

lock_loop:

    /* Spin waiting for the writer field to become free */
    os_rmb;
    while (i < SYNC_SPIN_ROUNDS && lock->lock_word <= 0) {
        if (SYNC_SPIN_WAIT_DELAY) {
            os_thread_delay(ut_rnd_interval(0, SYNC_SPIN_WAIT_DELAY));
        }
        i++;
    }

    if (i == SYNC_SPIN_ROUNDS) {
        os_thread_yield();
    }

    ++spin_count;

    /* We try once again to obtain the lock */
    if (rw_lock_s_lock_low(lock, pass, file_name, line)) {
        if (count_os_wait > 0) {
            rw_lock_stats.rw_s_os_wait_count.add(thread_internal_id, count_os_wait);
        }
        rw_lock_stats.rw_s_spin_round_count.add(thread_internal_id, spin_count);
        return; /* Success */
    } else {
        if (i < SYNC_SPIN_ROUNDS) {
            goto lock_loop;
        }

        ++count_os_wait;

        sync_cell_t *cell;
        sync_arr = sync_array_get_and_reserve_cell(lock, RW_LOCK_SHARED, file_name, line, &cell);
        /* Set waiters before checking lock_word to ensure wake-up signal is sent.
           This may lead to some unnecessary signals. */
        rw_lock_set_waiter_flag(lock);

        if (rw_lock_s_lock_low(lock, pass, file_name, line)) {
            sync_array_free_cell(sync_arr, cell);
            if (count_os_wait > 0) {
                rw_lock_stats.rw_s_os_wait_count.add(thread_internal_id, count_os_wait);
            }
            rw_lock_stats.rw_s_spin_round_count.add(thread_internal_id, spin_count);
            return; /* Success */
        }

        sync_array_wait_event(sync_arr, cell);
        i = 0;
        goto lock_loop;
    }
}

inline void rw_lock_s_lock_func(
    rw_lock_t  *lock, /*!< in: pointer to rw-lock */
    uint32      pass, /*!< in: pass value; != 0, if the lock will be passed to another thread to unlock */
    const char *file_name,/*!< in: file name where lock requested */
    uint32      line) /*!< in: line where requested */
{
    ut_ad(!rw_lock_own(lock, RW_LOCK_SHARED));
    ut_ad(!rw_lock_own(lock, RW_LOCK_EXCLUSIVE));

    if (!rw_lock_s_lock_low(lock, pass, file_name, line)) {
        /* Did not succeed, try spin wait */
        rw_lock_s_lock_spin(lock, pass, file_name, line);
    }
}

inline void rw_lock_s_unlock_func(
    uint32      pass, /*!< in: pass value; != 0, if the lock may have been passed to another thread to unlock */
    rw_lock_t  *lock) /*!< in/out: rw-lock */
{
    ut_ad(lock->lock_word > -X_LOCK_DECR);
    ut_ad(lock->lock_word != 0);
    ut_ad(lock->lock_word < X_LOCK_DECR);

    ut_d(rw_lock_sync_mgr.remove_rwlock_debug_info(lock, pass, RW_LOCK_SHARED));

    /* Increment lock_word to indicate 1 less reader */
    if (rw_lock_lock_word_incr(lock, 1) == 0) {
        /* wait_ex waiter exists. It may not be asleep, but we signal
        anyway. We do not wake other waiters, because they can't
        exist without wait_ex waiter and wait_ex waiter goes first.*/
        os_event_set(lock->wait_ex_event);
    }

    ut_ad(rw_lock_validate(lock));
}

inline void rw_lock_set_writer_id_and_recursion_flag(
    rw_lock_t *lock,  /*!< in/out: lock to work on */
    bool32     recursive)  /*!< in: TRUE if recursion allowed */
{
    os_thread_id_t curr_thread = os_thread_get_curr_id();
    os_thread_id_t local_thread;
    bool32 success;

    /* Prevent Valgrind warnings about writer_thread being
    uninitialized.  It does not matter if writer_thread is
    uninitialized, because we are comparing writer_thread against
    itself, and the operation should always succeed. */
    //UNIV_MEM_VALID(&lock->writer_thread, sizeof lock->writer_thread);

    local_thread = lock->writer_thread_id;
    success = atomic32_compare_and_swap(&lock->writer_thread_id, local_thread, curr_thread);
    ut_a(success);
    lock->recursive = recursive;
}

inline void rw_lock_x_lock_wait(
    rw_lock_t   *lock, /*!< in: pointer to rw-lock */
    uint32       pass, /*!< in: pass value; != 0, if the lock will be passed to another thread to unlock */
    const char  *file_name, /*!< in: file name where lock requested */
    uint32       line) /*!< in: line where requested */
{
    uint32          i = 0;
    sync_array_t   *sync_arr;
    uint32          n_spins = 0;
    uint64          count_os_wait = 0;
    uint64          thread_internal_id = os_thread_get_internal_id();

    os_rmb;
    ut_ad(lock->lock_word <= 0);

    while (lock->lock_word < 0) {
        if (SYNC_SPIN_WAIT_DELAY) {
            os_thread_delay(ut_rnd_interval(0, SYNC_SPIN_WAIT_DELAY));
        }

        if(i < SYNC_SPIN_ROUNDS) {
            i++;
            os_rmb;
            continue;
        }

        /* If there is still a reader, then go to sleep.*/
        ++n_spins;

        sync_cell_t *cell;
        sync_arr = sync_array_get_and_reserve_cell(lock, RW_LOCK_WAIT_EXCLUSIVE, file_name, line, &cell);
        i = 0;

        /* Check lock_word to ensure wake-up isn't missed.*/
        if (lock->lock_word < 0) {
            count_os_wait++;

            /* Add debug info as it is needed to detect possible deadlock.
               We must add info for WAIT_EX thread for deadlock detection to work properly. */
            ut_d(rw_lock_sync_mgr.add_rwlock_debug_info(lock, pass, RW_LOCK_WAIT_EXCLUSIVE, file_name, line));

            sync_array_wait_event(sync_arr, cell);

            ut_d(rw_lock_sync_mgr.remove_rwlock_debug_info(lock, pass, RW_LOCK_WAIT_EXCLUSIVE));

            /* It is possible to wake when lock_word < 0.
               We must pass the while-loop check to proceed.*/
        } else {
            sync_array_free_cell(sync_arr, cell);
            break;
        }
    }

    rw_lock_stats.rw_x_spin_round_count.add(thread_internal_id, n_spins);
    if (count_os_wait > 0) {
        rw_lock_stats.rw_x_os_wait_count.add(thread_internal_id, count_os_wait);
    }
}

inline bool32 rw_lock_x_lock_low(
    rw_lock_t  *lock, /*!< in: pointer to rw-lock */
    uint32      pass, /*!< in: pass value; != 0, if the lock will be passed to another thread to unlock */
    const char *file_name, /*!< in: file name where lock requested */
    uint32      line) /*!< in: line where requested */
{
    if (rw_lock_lock_word_decr(lock, X_LOCK_DECR)) {
        /* lock->recursive also tells us if the writer_thread field is stale or active.
           As we are going to write our own thread id in that field it must be that the
           current writer_thread value is not active. */
        ut_a(!lock->recursive);

        /* Decrement occurred: we are writer or next-writer. */
        rw_lock_set_writer_id_and_recursion_flag(lock, pass ? FALSE : TRUE);
        rw_lock_x_lock_wait(lock, pass, file_name, line);
    } else {
        os_thread_id_t thread_id = os_thread_get_curr_id();
        if (!pass) {
            os_rmb;
        }

        /* Decrement failed: relock or failed lock */
        if (!pass && lock->recursive && os_thread_eq(lock->writer_thread_id, thread_id)) {
            /* Relock */
            if (lock->lock_word == 0) {
                lock->lock_word -= X_LOCK_DECR;
            } else {
                ut_ad(lock->lock_word <= -X_LOCK_DECR);
                --lock->lock_word;
            }
        } else {
            /* Another thread locked before us */
            return FALSE;
        }
    }

    ut_d(rw_lock_sync_mgr.add_rwlock_debug_info(lock, pass, RW_LOCK_EXCLUSIVE, file_name, line));
    lock->last_x_file_name = file_name;
    lock->last_x_line = (unsigned int) line;

    return TRUE;
}

inline void rw_lock_x_lock_func(
	rw_lock_t  *lock, /*!< in: pointer to rw-lock */
	uint32      pass, /*!< in: pass value; != 0, if the lock will be passed to another thread to unlock */
	const char *file_name,/*!< in: file name where lock requested */
	uint32      line) /*!< in: line where requested */
{
    uint32          i = 0; /*!< spin round count */
    sync_array_t   *sync_arr;
    uint32          spin_count = 0;
    uint64          count_os_wait = 0;
    bool            spinning = false;
    uint64          thread_internal_id = os_thread_get_internal_id();

    ut_ad(rw_lock_validate(lock));
    ut_ad(!rw_lock_own(lock, RW_LOCK_SHARED));

lock_loop:

    if (rw_lock_x_lock_low(lock, pass, file_name, line)) {
        if (count_os_wait > 0) {
            rw_lock_stats.rw_x_os_wait_count.add(thread_internal_id, count_os_wait);
        }
        rw_lock_stats.rw_x_spin_round_count.add(thread_internal_id, spin_count);
        /* Locking succeeded */
        return;
    } else {
        if (!spinning) {
          spinning = true;
          rw_lock_stats.rw_x_spin_wait_count.add(thread_internal_id, 1);
        }

        /* Spin waiting for the lock_word to become free */
        os_rmb;
        while (i < SYNC_SPIN_ROUNDS && lock->lock_word <= 0) {
            if (SYNC_SPIN_WAIT_DELAY) {
                os_thread_delay(ut_rnd_interval(0, SYNC_SPIN_WAIT_DELAY));
            }
            i++;
        }
        spin_count += i;

        if (i == SYNC_SPIN_ROUNDS) {
            os_thread_yield();
        } else {
            goto lock_loop;
        }
    }

    sync_cell_t *cell;
    sync_arr = sync_array_get_and_reserve_cell(lock, RW_LOCK_EXCLUSIVE, file_name, line, &cell);

    /* Waiters must be set before checking lock_word, to ensure signal
    is sent. This could lead to a few unnecessary wake-up signals. */
    rw_lock_set_waiter_flag(lock);

    if (rw_lock_x_lock_low(lock, pass, file_name, line)) {
        sync_array_free_cell(sync_arr, cell);

        if (count_os_wait > 0) {
            rw_lock_stats.rw_x_os_wait_count.add(thread_internal_id, count_os_wait);
        }
        rw_lock_stats.rw_x_spin_round_count.add(thread_internal_id, spin_count);

        return; /* Locking succeeded */
    }

    /* these stats may not be accurate */
    ++count_os_wait;
    sync_array_wait_event(sync_arr, cell);

    i = 0;
    goto lock_loop;
}

inline bool32 rw_lock_x_lock_func_nowait(rw_lock_t *lock, const char* file_name, uint32 line)
{
    os_thread_id_t curr_thread = os_thread_get_curr_id();
    bool32 success = atomic32_compare_and_swap(&lock->lock_word, X_LOCK_DECR, 0);
    if (success) {
        rw_lock_set_writer_id_and_recursion_flag(lock, TRUE);
    } else if (lock->recursive && os_thread_eq(lock->writer_thread_id, curr_thread)) {
        /* Relock: this lock_word modification is safe since no other
           threads can modify (lock, unlock, or reserve) lock_word while
           there is an exclusive writer and this is the writer thread. */
        if (lock->lock_word == 0) {
            lock->lock_word -= X_LOCK_DECR;
        } else {
            lock->lock_word--;
        }
        /* Watch for too many recursive locks */
        ut_ad(lock->lock_word < 0);
    } else {
        /* Failure */
        return FALSE;
    }

    ut_d(rw_lock_sync_mgr.add_rwlock_debug_info(lock, 0, RW_LOCK_EXCLUSIVE, file_name, line));
    lock->last_x_file_name = file_name;
    lock->last_x_line = line;
    ut_ad(rw_lock_validate(lock));
    return TRUE;
}

inline void rw_lock_x_unlock_func(uint32 pass, rw_lock_t *lock)
{
    ut_ad(lock->lock_word == 0 || lock->lock_word <= -X_LOCK_DECR);

    if (lock->lock_word == 0) {
        /* Last caller in a possible recursive chain. */
        lock->recursive = FALSE;
    }

    ut_d(rw_lock_sync_mgr.remove_rwlock_debug_info(lock, pass, RW_LOCK_EXCLUSIVE));

    if (lock->lock_word == 0) {
        /* There is 1 x-lock */
        /* atomic increment is needed, because it is last */
        if (rw_lock_lock_word_incr(lock, X_LOCK_DECR) <= 0) {
            ut_error;
        }

        /* This no longer has an X-lock. So it is now free for S-locks by other threads.
           We need to signal read/write waiters. We do not need to signal wait_ex waiters,
           since they cannot exist when there is a writer. */
        if (lock->waiters) {
            rw_lock_reset_waiter_flag(lock);
            os_event_set(lock->event);
        }
    } else if (lock->lock_word == -X_LOCK_DECR) {
        /* There are 2 x-locks */
        lock->lock_word += X_LOCK_DECR;
    } else {
        /* There are more than 2 x-locks. */
        ut_ad(lock->lock_word < -X_LOCK_DECR);
        lock->lock_word += 1;
    }

    ut_ad(rw_lock_validate(lock));
}

uint32 rw_lock_get_writer(const rw_lock_t *lock)
{
    int32 lock_word = lock->lock_word;
    ut_ad(lock_word <= X_LOCK_DECR);

    if (lock_word > 0) {
        /* return NOT_LOCKED in s-lock state, like the writer
           member of the old lock implementation. */
        return(RW_LOCK_NOT_LOCKED);
    } else if (lock_word == 0 || lock_word <= -X_LOCK_DECR) {
        return(RW_LOCK_EXCLUSIVE);
    } else {
        ut_ad(lock_word > -X_LOCK_DECR);
        return(RW_LOCK_WAIT_EXCLUSIVE);
    }
}

inline uint32 rw_lock_get_reader_count(const rw_lock_t *lock)
{
    int32 lock_word = lock->lock_word;
    if (lock_word > 0) {
        /* s-locked, no x-waiters */
        return(X_LOCK_DECR - lock_word);
    } else if (lock_word < 0 && lock_word > -X_LOCK_DECR) {
        /* s-locked, with x-waiters */
        return((uint32)(-lock_word));
    }
    return(0);
}

// Returns the value of writer_count for the lock.
// Does not reserve the lock mutex,
// so the caller must be sure it is not changed during the call.
// return value of writer_count
inline uint32 rw_lock_get_x_lock_count(const rw_lock_t *lock)
{
    int32 lock_copy = lock->lock_word;
    if ((lock_copy != 0) && (lock_copy > -X_LOCK_DECR)) {
        return(0);
    }
    return((lock_copy == 0) ? 1 : (2 - (lock_copy + X_LOCK_DECR)));
}

status_t sync_init(memory_pool_t* pool)
{
    ut_a(sync_initialized == FALSE);

    sync_initialized = TRUE;
    return rw_lock_sync_mgr.init(pool);
}

void sync_close(void)
{
    rw_lock_sync_mgr.destroy();
    sync_initialized = FALSE;
}

