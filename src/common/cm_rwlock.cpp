#include "cm_rwlock.h"
#include "cm_dbug.h"
#include "cm_memory.h"
#include "cm_random.h"
#include "cm_log.h"

#define SYNC_SPIN_ROUNDS         30
#define SYNC_SPIN_WAIT_DELAY     6

/** The global array of wait cells for implementation of the database's own mutexes and read-write locks */
static sync_array_t       **sync_wait_array;
static uint32               sync_array_size = 32;

typedef UT_LIST_BASE_NODE_T(rw_lock_t) rw_lock_list_t;

static rw_lock_stats_t      rw_lock_stats;

/** count of how many times an object has been signalled */
static uint64               sg_count;

/* The global list of rw-locks */
static rw_lock_list_t       rw_lock_list;
static spinlock_t           rw_lock_list_lock;
static bool32               sync_initialized = FALSE;

static os_mutex_t           rw_lock_debug_mutex;
static os_event_t           rw_lock_debug_event;
static bool32               rw_lock_debug_waiters;

static const char*          rw_lock_type_desc[] = {
    "RW_LOCK_NOT_LOCKED",
    "RW_LOCK_EXCLUSIVE",
    "RW_LOCK_SHARED",
    "RW_LOCK_WAIT_EXCLUSIVE"
};

static bool32 sync_array_detect_deadlock(sync_array_t *arr, sync_cell_t *start, sync_cell_t *cell, uint32 depth);

static void rw_lock_debug_mutex_enter();
static void rw_lock_debug_mutex_exit();



static sync_array_t* sync_array_create(uint32 n_cells)  /*!< in: number of cells in the array to create */
{
    sync_array_t  *arr;

    ut_a(n_cells > 0);

    /* Allocate memory for the data structures */
    arr = (sync_array_t*)malloc(sizeof(*arr));
    memset(arr, 0x0, sizeof(*arr));

    arr->array = (sync_cell_t*)malloc(sizeof(sync_cell_t) * n_cells);
    memset(arr->array, 0x0, sizeof(sync_cell_t) * n_cells);

    arr->n_cells = n_cells;
    arr->first_free_slot = UINT32_UNDEFINED;

    spin_lock_init(&arr->lock);

    return arr;
}

static void sync_array_free(sync_array_t *arr)
{
    ut_a(arr->n_reserved == 0);

    my_free(arr->array);
    my_free(arr);
}

static void sync_array_init(uint32 n_threads)
{
    uint32  n_slots;

    ut_a(sync_wait_array == NULL);
    ut_a(n_threads > 0);

    sync_wait_array = (sync_array_t**)(malloc(sizeof(*sync_wait_array) * sync_array_size));

    n_slots = 1 + (n_threads - 1) / sync_array_size;
    for (uint32 i = 0; i < sync_array_size; ++i) {
        sync_wait_array[i] = sync_array_create(n_slots);
    }
}

static void sync_array_close(void)
{
    for (uint32 i = 0; i < sync_array_size; ++i) {
        sync_array_free(sync_wait_array[i]);
    }

    my_free(sync_wait_array);
    sync_wait_array = NULL;
}

static sync_array_t* sync_array_get(void)
{
    uint32 i;
    static atomic32_t count;

    i = atomic32_inc(&count);

    return(sync_wait_array[i % sync_array_size]);
}

static sync_cell_t* sync_array_get_nth_cell(sync_array_t *arr, uint32 n)
{
    ut_a(arr);
    ut_a(n < arr->n_cells);

    return(arr->array + n);
}

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

    spin_lock(&arr->lock, NULL);

    if (arr->first_free_slot != UINT32_UNDEFINED) {
        /* Try and find a slot in the free list */
        ut_ad(arr->first_free_slot < arr->next_free_slot);
        cell = sync_array_get_nth_cell(arr, arr->first_free_slot);
        arr->first_free_slot = cell->line;
    } else if (arr->next_free_slot < arr->n_cells) {
        /* Try and find a slot after the currently allocated slots */
        cell = sync_array_get_nth_cell(arr, arr->next_free_slot);
        ++arr->next_free_slot;
    } else {
        spin_unlock(&arr->lock);

        // We should return NULL and if there is more than
        // one sync array, try another sync array instance.
        return(NULL);
    }

    ++arr->res_count;

    ut_ad(arr->n_reserved < arr->n_cells);
    ut_ad(arr->next_free_slot <= arr->n_cells);

    ++arr->n_reserved;

    /* Reserve the cell. */
    //ut_ad(cell->latch.mutex == NULL);
    cell->request_type = type;
    cell->wait_object.lock = (rw_lock_t*)(object);
    cell->waiting = false;
    cell->file = file;
    cell->line = line;

    spin_unlock(&arr->lock);

    cell->thread_id = os_thread_get_curr_id();
    cell->reservation_time = time(NULL);

    /* Make sure the event is reset and also store the value of
    signal_count at which the event was reset. */
    os_event_t event = sync_cell_get_event(cell);
    cell->signal_count = os_event_reset(event);

    LOGGER_DEBUG(LOGGER, "sync_array_reserve_cell: arr %p cell %p rwlock %p thread %lu\n",
        arr, cell, object, cell->thread_id);

    return(cell);
}

sync_array_t* sync_array_get_and_reserve_cell(
    void         *object, /*!< in: pointer to the object to wait for */
    uint32        type,	/*!< in: lock request type */
    const char   *file,	/*!< in: file where requested */
    uint32        line,	/*!< in: line where requested */
    sync_cell_t **cell)	/*!< out: the cell reserved, never NULL */
{
    sync_array_t *sync_arr = NULL;

    *cell = NULL;
    for (uint32 i = 0; i < sync_array_size && *cell == NULL; ++i) {
        /* Although the sync_array is get in a random way currently,
        we still try at most sync_array_size times, in case any
        of the sync_array we get is full */
        sync_arr = sync_array_get();
        *cell = sync_array_reserve_cell(sync_arr, object, type, file, line);
    }

    /* This won't be true every time, for the loop above may execute
    more than srv_sync_array_size times to reserve a cell.
    But an assertion here makes the code more solid. */
    ut_a(*cell != NULL);

    return(sync_arr);
}

static void sync_array_free_cell(sync_array_t *arr, sync_cell_t *&cell)
{
    spin_lock(&arr->lock, NULL);

    ut_a(cell->wait_object.lock != NULL);

    cell->waiting = FALSE;
    cell->wait_object.lock =  NULL;
    cell->signal_count = 0;

    /* Setup the list of free slots in the array */
    cell->line = arr->first_free_slot;

    arr->first_free_slot = (uint32)(cell - arr->array);

    ut_a(arr->n_reserved > 0);
    arr->n_reserved--;

    if (arr->next_free_slot > arr->n_cells / 2 && arr->n_reserved == 0) {
#ifdef UNIV_DEBUG
        for (ulint i = 0; i < arr->next_free_slot; ++i) {
            cell = sync_array_get_nth_cell(arr, i);

            ut_ad(!cell->waiting);
            ut_ad(cell->wait_object.lock == 0);
            ut_ad(cell->signal_count == 0);
        }
#endif /* UNIV_DEBUG */
        arr->next_free_slot = 0;
        arr->first_free_slot = UINT32_UNDEFINED;
    }

    spin_unlock(&arr->lock);

    cell = 0;
}

static void sync_array_wait_event(sync_array_t *arr, sync_cell_t*& cell)
{
    spin_lock(&arr->lock, NULL);

    ut_a(cell->wait_object.lock);
    ut_a(!cell->waiting);
    ut_ad(os_thread_get_curr_id() == cell->thread_id);

    cell->waiting = TRUE;

    /* We use simple enter to the mutex below, because if we cannot acquire it at once,
    mutex_enter would call recursively sync_array routines, leading to trouble.
    rw_lock_debug_mutex freezes the debug lists. */

    rw_lock_debug_mutex_enter();
    if (TRUE == sync_array_detect_deadlock(arr, cell, cell, 0)) {
        fputs("########################################\nDeadlock Detected!", stderr);
        ut_error;
    }
    rw_lock_debug_mutex_exit();

    spin_unlock(&arr->lock);

    os_event_wait(sync_cell_get_event(cell), cell->signal_count);

    sync_array_free_cell(arr, cell);

    cell = 0;
}

uint32 rw_lock_get_writer(const rw_lock_t *lock)
{
    int32 lock_word = lock->lock_word;

	ut_ad(lock_word <= X_LOCK_DECR);

	if (lock_word > X_LOCK_HALF_DECR) {
		/* return NOT_LOCKED in s-lock state, like the writer
		member of the old lock implementation. */
		return(RW_LOCK_NOT_LOCKED);
	} else if (lock_word == 0
		   || lock_word == -X_LOCK_HALF_DECR
		   || lock_word <= -X_LOCK_DECR) {
		/* x-lock with sx-lock is also treated as RW_LOCK_EX */
		return(RW_LOCK_EXCLUSIVE);
	} else {
		/* x-waiter with sx-lock is also treated as RW_LOCK_WAIT_EX
		e.g. -X_LOCK_HALF_DECR < lock_word < 0 : without sx
		     -X_LOCK_DECR < lock_word < -X_LOCK_HALF_DECR : with sx */
		return(RW_LOCK_WAIT_EXCLUSIVE);
	}

}

void rw_lock_debug_print(FILE *f, rw_lock_debug_t *info)
{
    uint32 rwt;

    rwt = info->lock_type;

    fprintf(f, "Locked: thread %lu file %s line %lu  ",
        (ulong) info->thread_id, info->file_name,
        (ulong) info->line);
    if (rwt == RW_LOCK_SHARED) {
        fputs("S-LOCK", f);
    } else if (rwt == RW_LOCK_EXCLUSIVE) {
        fputs("X-LOCK", f);
    } else if (rwt == RW_LOCK_WAIT_EXCLUSIVE) {
        fputs("WAIT X-LOCK", f);
    } else {
        ut_error;
    }
    if (info->pass != 0) {
        fprintf(f, " pass value %lu", (ulong) info->pass);
    }
    putc('\n', f);
}

uint32 rw_lock_get_reader_count(const rw_lock_t *lock)
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

static void sync_array_object_signalled()
{
    ++sg_count;
}

static void sync_array_cell_print(FILE *file, sync_cell_t *cell)
{
    rw_lock_t *rwlock;
    uint32     type;
    uint32     writer;

    type = cell->request_type;

    fprintf(file, "--Thread %lu has waited at %s line %lu for %.2f seconds the semaphore:\n",
        (ulong) cell->thread_id,  cell->file, (ulong) cell->line, difftime(time(NULL), cell->reservation_time));

    if (type == RW_LOCK_EXCLUSIVE || type == RW_LOCK_WAIT_EXCLUSIVE || type == RW_LOCK_SHARED) {

        fputs(type == RW_LOCK_EXCLUSIVE ? "X-lock on" : type == RW_LOCK_WAIT_EXCLUSIVE ? "X-lock (wait_ex) on" : "S-lock on", file);

        rwlock = cell->wait_object.lock;
        fprintf(file, " RW-latch at %p created in file %s line %lu\n",
            (void*) rwlock, rwlock->cfile_name, (ulong) rwlock->cline);

        writer = rw_lock_get_writer(rwlock);
        if (writer != RW_LOCK_NOT_LOCKED) {
            fprintf(file, "a writer (thread id %lu) has reserved it in mode %s",
                (ulong) rwlock->writer_thread,
                writer == RW_LOCK_EXCLUSIVE ? " exclusive\n" : " wait exclusive\n");
        }

        fprintf(file,
            "number of readers %d, waiters flag %d, lock_word: %lx\n"
            "Last time read locked in file %s line %lu\n"
            "Last time write locked in file %s line %lu\n",
            rw_lock_get_reader_count(rwlock),
            rwlock->waiters,
            (ulong)(rwlock->lock_word),
            rwlock->last_s_file_name, (ulong)(rwlock->last_s_line),
            rwlock->last_x_file_name, (ulong)(rwlock->last_x_line));
    } else {
        ut_error;
    }

    if (!cell->waiting) {
        fputs("wait has ended\n", file);
    }
}

static sync_cell_t* sync_array_find_thread(
    sync_array_t *arr, /*!< in: wait array */
    os_thread_id_t thread) /*!< in: thread id */
{
    sync_cell_t *cell;

    for (uint32 i = 0; i < arr->n_cells; i++) {
        cell = sync_array_get_nth_cell(arr, i);
        if (cell->wait_object.lock) {
            LOGGER_DEBUG(LOGGER, "sync_array_find_thread: i %lu  arr %p thread %llu cell %p rw lock %p\n",
                i, arr, cell->thread_id, cell, cell->wait_object.lock);
        }
        if (cell->wait_object.lock != NULL && os_thread_eq(cell->thread_id, thread)) {
            LOGGER_DEBUG(LOGGER, "sync_array_find_thread: arr %p thread %lu cell %p rw lock %p\n",
                arr, thread, cell, cell->wait_object.lock);
            return(cell); /* Found */
        }
    }

    LOGGER_DEBUG(LOGGER, "sync_array_find_thread: arr %p thread %lu not found cell\n", arr, thread);

    return NULL; /* Not found */
}

static bool32 sync_array_deadlock_step(
    sync_array_t    *arr,	/*!< in: wait array; NOTE! the caller must own the mutex to array */
    sync_cell_t     *start,	/*!< in: cell where recursive search started */
    os_thread_id_t   thread,	/*!< in: thread to look at */
    uint32           pass,	/*!< in: pass value */
    uint32           depth)	/*!< in: recursion depth */
{
    sync_cell_t *new_cell;

    if (pass != 0) {
        /* If pass != 0, then we do not know which threads are
        responsible of releasing the lock, and no deadlock can be detected. */
        return(FALSE);
    }

    new_cell = sync_array_find_thread(arr, thread);
    if (new_cell == start) {
        /* Deadlock */
        fputs("########################################\n"
              "DEADLOCK of threads detected!\n", stderr);
        return(TRUE);
    } else if (new_cell) {
        return(sync_array_detect_deadlock(arr, start, new_cell, depth + 1));
    }
    return(FALSE);
}

void sync_array_report_error(rw_lock_t *lock, rw_lock_debug_t *debug, sync_cell_t *cell)
{
    fprintf(stderr, "rw-lock %p ", (void*) lock);
    sync_array_cell_print(stderr, cell);
    rw_lock_debug_print(stderr, debug);
}

static bool32 sync_array_detect_deadlock(
    sync_array_t    *arr, /*!< in: wait array; NOTE! the caller must own the mutex to array */
    sync_cell_t     *start, /*!< in: cell where recursive search started */
    sync_cell_t     *cell, /*!< in: cell to search */
    uint32          depth) /*!< in: recursion depth */
{
    rw_lock_t       *lock;
    os_thread_id_t   thread;
    bool32           ret;
    rw_lock_debug_t *debug;

    ut_a(arr);
    ut_a(start);
    ut_a(cell);
    ut_ad(cell->wait_object.lock);
    ut_ad(os_thread_get_curr_id() == start->thread_id);
    ut_ad(depth < 100);

    depth++;

    if (!cell->waiting) {
        return(FALSE); /* No deadlock here */
    }

    LOGGER_DEBUG(LOGGER, "sync_array_detect_deadlock: arr %p cell %p rwlock %p thread %lu\n",
        arr, cell, cell->wait_object.lock, os_thread_get_curr_id());

    switch (cell->request_type) {
    case RW_LOCK_EXCLUSIVE:
    case RW_LOCK_WAIT_EXCLUSIVE:
        lock = cell->wait_object.lock;
        for (debug = UT_LIST_GET_FIRST(lock->debug_list);
             debug != NULL;
             debug = UT_LIST_GET_NEXT(list_node, debug)) {

            LOGGER_DEBUG(LOGGER, "sync_array_detect_deadlock: arr %p debug_list rwlock %p thread %lu\n",
                arr, debug->lock, debug->thread_id);

            thread = debug->thread_id;
            switch (debug->lock_type) {
            case RW_LOCK_EXCLUSIVE:
            case RW_LOCK_WAIT_EXCLUSIVE:
                if (os_thread_eq(thread, cell->thread_id)) {
                    break;
                }
                /* fall through */
            case RW_LOCK_SHARED:
                /* The (wait) x-lock request can block infinitely only if someone (can be also cell thread) is holding s-lock,
                or someone (cannot be cell thread) (wait) x-lock or sx-lock, and he is blocked by start thread */
                ret = sync_array_deadlock_step(arr, start, thread, debug->pass, depth);
                if (ret) {
                    sync_array_report_error(lock, debug, cell);
                    rw_lock_debug_print(stderr, debug);
                    return(TRUE);
                }
            }
        }

        return(false);

    case RW_LOCK_SHARED:
        lock = cell->wait_object.lock;
        for (debug = UT_LIST_GET_FIRST(lock->debug_list);
             debug != 0;
             debug = UT_LIST_GET_NEXT(list_node, debug)) {

            LOGGER_DEBUG(LOGGER, "sync_array_detect_deadlock: arr %p debug_list rwlock %p thread %lu\n",
                arr, debug->lock, debug->thread_id);

            thread = debug->thread_id;
            if (debug->lock_type == RW_LOCK_EXCLUSIVE || debug->lock_type == RW_LOCK_WAIT_EXCLUSIVE) {
                /* The s-lock request can block infinitely only if someone (can also be cell thread) is holding (wait) x-lock,
                and he is blocked by start thread */
                ret = sync_array_deadlock_step(arr, start, thread, debug->pass, depth);
                if (ret) {
                    sync_array_report_error(lock, debug, cell);
                    return(TRUE);
                }
            }
        }
        return(false);

    default:
        ut_error;
    }

    return(TRUE);	/* Execution never reaches this line: for compiler fooling only */
}

static void sync_array_print_info(FILE *file, sync_array_t *arr)
{
    uint32  count = 0;

    spin_lock(&arr->lock, NULL);

    fprintf(file, "OS WAIT ARRAY INFO: reservation count %lu\n", arr->res_count);

    for (uint32 i = 0; count < arr->n_reserved; ++i) {
        sync_cell_t *cell = sync_array_get_nth_cell(arr, i);
        if (cell->wait_object.lock != 0) {
            count++;
            sync_array_cell_print(file, cell);
        }
    }

    spin_unlock(&arr->lock);
}

void sync_array_print(FILE *file)
{
    for (uint32 i = 0; i < sync_array_size; ++i) {
        sync_array_print_info(file, sync_wait_array[i]);
    }

    fprintf(file, "OS WAIT ARRAY INFO: signal count %llu\n", sg_count);
}


static void sync_print_wait_info(FILE *file) {
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


static void rw_lock_debug_mutex_enter()
{
    for (;;) {
        if (os_mutex_tryenter(&rw_lock_debug_mutex)) {
            return;
        }

        os_event_reset(rw_lock_debug_event);
        rw_lock_debug_waiters = TRUE;

        if (os_mutex_tryenter(&rw_lock_debug_mutex)) {
            return;
        }

        os_event_wait(rw_lock_debug_event);
    }
}

static void rw_lock_debug_mutex_exit()
{
    os_mutex_exit(&rw_lock_debug_mutex);

    if (rw_lock_debug_waiters) {
        rw_lock_debug_waiters = FALSE;
        os_event_set(rw_lock_debug_event);
    }
}

void rw_lock_remove_debug_info(
    rw_lock_t   *lock,  /*!< in: rw-lock */
    uint32       pass,  /*!< in: pass value */
    uint32       lock_type)  /*!< in: lock type */
{
    rw_lock_debug_t*	info;

    ut_ad(lock);

    if ((pass == 0) && (lock_type != RW_LOCK_WAIT_EXCLUSIVE)) {
        //sync_check_unlock(lock);
    }

    rw_lock_debug_mutex_enter();

    for (info = UT_LIST_GET_FIRST(lock->debug_list);
         info != 0;
         info = UT_LIST_GET_NEXT(list_node, info)) {

        if (pass == info->pass
            && (pass != 0 || os_thread_eq(info->thread_id, os_thread_get_curr_id()))
            && info->lock_type == lock_type) {

            /* Found! */
            UT_LIST_REMOVE(list_node, lock->debug_list, info);

            rw_lock_debug_mutex_exit();

            my_free(info);
            return;
        }
    }

    ut_error;
}

void rw_lock_add_debug_info(
    rw_lock_t  *lock,   /*!< in: rw-lock */
    uint32      pass,   /*!< in: pass value */
    uint32      lock_type,  /*!< in: lock type */
    const char *file_name,  /*!< in: file where requested */
    uint32      line)   /*!< in: line where requested */
{
    ut_ad(file_name != NULL);

    rw_lock_debug_t *info = (rw_lock_debug_t *)malloc(sizeof(rw_lock_debug_t));

    rw_lock_debug_mutex_enter();

    info->lock = lock;
    info->pass = pass;
    info->line = line;
    info->lock_type = lock_type;
    info->file_name = file_name;
    info->thread_id = os_thread_get_curr_id();
    UT_LIST_ADD_FIRST(list_node, lock->debug_list, info);

    LOGGER_DEBUG(LOGGER, "rw_lock_add_debug_info: rwlock %p lock type %s\n",
        lock, rw_lock_type_desc[info->lock_type]);

    rw_lock_debug_mutex_exit();
}

static void rw_lock_list_print_info(FILE *file)
{
    uint32 count = 0;

    spin_lock(&rw_lock_list_lock, NULL);

    fputs("-------------\nRW-LATCH INFO\n-------------\n", file);

    for (const rw_lock_t* lock = UT_LIST_GET_FIRST(rw_lock_list);
         lock != NULL;
         lock = UT_LIST_GET_NEXT(list_node, lock)) {

        count++;

        if (lock->lock_word != X_LOCK_DECR) {
            fprintf(file, "RW-LOCK: %p ", (void*) lock);
            if (lock->waiters) {
                fputs(" Waiters for the lock exist\n", file);
            } else {
                putc('\n', file);
            }

            rw_lock_debug_t* info;
            rw_lock_debug_mutex_enter();
            for (info = UT_LIST_GET_FIRST(lock->debug_list);
                 info != NULL;
                 info = UT_LIST_GET_NEXT(list_node, info)) {
                rw_lock_debug_print(file, info);
            }
            rw_lock_debug_mutex_exit();
        }
    }

    fprintf(file, "Total number of rw-locks %lu\n", count);
    spin_unlock(&rw_lock_list_lock);
}

void sync_check_init(size_t max_threads)
{
  /* Init the rw-lock & mutex list and create the mutex to protect it. */
  UT_LIST_INIT(rw_lock_list);
  spin_lock_init(&rw_lock_list_lock);

  sync_array_init((uint32)max_threads);
}

/** Checks that the rw-lock has been initialized and that there are no simultaneous shared and exclusive locks. */
bool32 rw_lock_validate(const rw_lock_t *lock)
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

    return (true);
}

bool32 rw_lock_own(rw_lock_t *lock, uint32 lock_type)
{
    ut_ad(lock);
    ut_ad(rw_lock_validate(lock));

    rw_lock_debug_mutex_enter();

    for (const rw_lock_debug_t* info = UT_LIST_GET_FIRST(lock->debug_list);
         info != NULL;
         info = UT_LIST_GET_NEXT(list_node, info)) {

        if (os_thread_eq(info->thread_id, os_thread_get_curr_id())
            && info->pass == 0
            && info->lock_type == lock_type) {

            rw_lock_debug_mutex_exit();
            /* Found! */

            return(TRUE);
        }
    }
    rw_lock_debug_mutex_exit();

    return(FALSE);
}

void rw_lock_create_func(
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
    lock->sx_recursive = 0;
    memset((void *)&lock->writer_thread, 0, sizeof lock->writer_thread);

    lock->cfile_name = cfile_name;

    /* This should hold in practice. If it doesn't then we need to
    split the source file anyway. Or create the locks on lines less than 8192. cline is unsigned:13. */
    ut_ad(cline <= 8192);
    lock->cline = (unsigned int)cline;

    lock->count_os_wait = 0;
    lock->last_s_file_name = "not yet reserved";
    lock->last_x_file_name = "not yet reserved";
    lock->last_s_line = 0;
    lock->last_x_line = 0;
    lock->event = os_event_create(0);
    lock->wait_ex_event = os_event_create(0);

    lock->is_block_lock = 0;

    spin_lock(&rw_lock_list_lock, NULL);
    ut_ad(UT_LIST_GET_FIRST(rw_lock_list) == NULL || UT_LIST_GET_FIRST(rw_lock_list)->magic_n == RW_LOCK_MAGIC_N);
    UT_LIST_ADD_FIRST(list_node, rw_lock_list, lock);
    spin_unlock(&rw_lock_list_lock);

    UT_LIST_INIT(lock->debug_list);
    lock->magic_n = RW_LOCK_MAGIC_N;
    //lock->level = level;
}


void rw_lock_destroy_func(rw_lock_t *lock)
{
    os_rmb;
    ut_ad(rw_lock_validate(lock));
    ut_a(lock->lock_word == X_LOCK_DECR);

    spin_lock(&rw_lock_list_lock, NULL);
    os_event_destroy(lock->event);
    os_event_destroy(lock->wait_ex_event);
    UT_LIST_REMOVE(list_node, rw_lock_list, lock);
    spin_unlock(&rw_lock_list_lock);
}

bool32 rw_lock_lock_word_decr(rw_lock_t *lock, uint32 amount, int32 threshold)
{
    os_rmb;
    int32 local_lock_word = lock->lock_word;
    while (local_lock_word > threshold) {
        if (atomic32_compare_and_swap(&lock->lock_word, local_lock_word, local_lock_word - amount)) {
            return(TRUE);
        }
        local_lock_word = lock->lock_word;
    }
    return FALSE;
}

int32 rw_lock_lock_word_incr(rw_lock_t *lock, uint32 amount)
{
    return(atomic32_add(&lock->lock_word, amount));
}

bool32 rw_lock_s_lock_low(
    rw_lock_t  *lock, /*!< in: pointer to rw-lock */
    uint32      pass, /*!< in: pass value; != 0, if the lock will be passed to another thread to unlock */
    const char *file_name, /*!< in: file name where lock requested */
    uint32      line) /*!< in: line where requested */
{
    if (!rw_lock_lock_word_decr(lock, 1, 0)) {
        /* Locking did not succeed */
        return(FALSE);
    }

    ut_d(rw_lock_add_debug_info(lock, pass, RW_LOCK_SHARED, file_name, line));

    /* These debugging values are not set safely: they may be incorrect
    or even refer to a line that is invalid for the file name. */
    lock->last_s_file_name = file_name;
    lock->last_s_line = line;

    return(TRUE);	/* locking succeeded */
}

void rw_lock_set_waiter_flag(rw_lock_t *lock)
{
    (void)atomic32_compare_and_swap(&lock->waiters, 0, 1);
}

void rw_lock_reset_waiter_flag(rw_lock_t *lock)
{
    (void) atomic32_compare_and_swap(&lock->waiters, 1, 0);
}

void rw_lock_s_lock_spin(
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

    ut_ad(rw_lock_validate(lock));
    rw_lock_stats.rw_s_spin_wait_count.inc();

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
            lock->count_os_wait += count_os_wait;
            rw_lock_stats.rw_s_os_wait_count.add(count_os_wait);
        }
        rw_lock_stats.rw_s_spin_round_count.add(spin_count);
        return; /* Success */
    } else {
        if (i < SYNC_SPIN_ROUNDS) {
            goto lock_loop;
        }

        ++count_os_wait;

        sync_cell_t *cell;
        sync_arr = sync_array_get_and_reserve_cell(lock, RW_LOCK_SHARED, file_name, line, &cell);
        /* Set waiters before checking lock_word to ensure wake-up
        signal is sent. This may lead to some unnecessary signals. */
        rw_lock_set_waiter_flag(lock);

        if (rw_lock_s_lock_low(lock, pass, file_name, line)) {
            sync_array_free_cell(sync_arr, cell);
            if (count_os_wait > 0) {
                lock->count_os_wait += count_os_wait;
                rw_lock_stats.rw_s_os_wait_count.add(count_os_wait);
            }
            rw_lock_stats.rw_s_spin_round_count.add(spin_count);
            return; /* Success */
        }

        sync_array_wait_event(sync_arr, cell);
        i = 0;
        goto lock_loop;
    }
}

void rw_lock_s_lock_func(
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

void rw_lock_s_unlock_func(
    uint32      pass, /*!< in: pass value; != 0, if the lock may have been passed to another thread to unlock */
    rw_lock_t  *lock) /*!< in/out: rw-lock */
{
    ut_ad(lock->lock_word > -X_LOCK_DECR);
    ut_ad(lock->lock_word != 0);
    ut_ad(lock->lock_word < X_LOCK_DECR);

    ut_d(rw_lock_remove_debug_info(lock, pass, RW_LOCK_SHARED));

    /* Increment lock_word to indicate 1 less reader */
    int32 lock_word = rw_lock_lock_word_incr(lock, 1);
    if (lock_word == 0 || lock_word == -X_LOCK_HALF_DECR) {
        /* wait_ex waiter exists. It may not be asleep, but we signal
        anyway. We do not wake other waiters, because they can't
        exist without wait_ex waiter and wait_ex waiter goes first.*/
        os_event_set(lock->wait_ex_event);
        sync_array_object_signalled();
    }

    ut_ad(rw_lock_validate(lock));
}

void rw_lock_set_writer_id_and_recursion_flag(
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

    local_thread = lock->writer_thread;
    success = atomic32_compare_and_swap(&lock->writer_thread, local_thread, curr_thread);
    ut_a(success);
    lock->recursive = recursive;
}

void rw_lock_x_lock_wait(
    rw_lock_t   *lock, /*!< in: pointer to rw-lock */
    uint32       pass, /*!< in: pass value; != 0, if the lock will be passed to another thread to unlock */
    int32        threshold, /*!< in: threshold to wait for */
    const char  *file_name, /*!< in: file name where lock requested */
    uint32       line) /*!< in: line where requested */
{
    uint32          i = 0;
    sync_array_t   *sync_arr;
    uint32          n_spins = 0;
    uint64          count_os_wait = 0;

    os_rmb;
    ut_ad(lock->lock_word <= threshold);

    while (lock->lock_word < threshold) {
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
        if (lock->lock_word < threshold) {
            count_os_wait++;

            /* Add debug info as it is needed to detect possible deadlock.
            We must add info for WAIT_EX thread for deadlock detection to work properly. */
            ut_d(rw_lock_add_debug_info(lock, pass, RW_LOCK_WAIT_EXCLUSIVE, file_name, line));

            sync_array_wait_event(sync_arr, cell);

            ut_d(rw_lock_remove_debug_info(lock, pass, RW_LOCK_WAIT_EXCLUSIVE));

            /* It is possible to wake when lock_word < 0.
            We must pass the while-loop check to proceed.*/
        } else {
            sync_array_free_cell(sync_arr, cell);
            break;
        }
    }

    rw_lock_stats.rw_x_spin_round_count.add(n_spins);

    if (count_os_wait > 0) {
        lock->count_os_wait += (uint32)count_os_wait;
        rw_lock_stats.rw_x_os_wait_count.add(count_os_wait);
    }
}

bool32 rw_lock_x_lock_low(
    rw_lock_t  *lock, /*!< in: pointer to rw-lock */
    uint32      pass, /*!< in: pass value; != 0, if the lock will be passed to another thread to unlock */
    const char *file_name, /*!< in: file name where lock requested */
    uint32      line) /*!< in: line where requested */
{
    if (rw_lock_lock_word_decr(lock, X_LOCK_DECR, X_LOCK_HALF_DECR)) {

        /* lock->recursive also tells us if the writer_thread field is stale or active.
        As we are going to write our own thread id in that field it must be that the
        current writer_thread value is not active. */
        ut_a(!lock->recursive);

        /* Decrement occurred: we are writer or next-writer. */
        rw_lock_set_writer_id_and_recursion_flag(lock, pass ? FALSE : TRUE);
        rw_lock_x_lock_wait(lock, pass, 0, file_name, line);

    } else {
        os_thread_id_t thread_id = os_thread_get_curr_id();

        if (!pass) {
            os_rmb;
        }

        /* Decrement failed: relock or failed lock */
        if (!pass && lock->recursive && os_thread_eq(lock->writer_thread, thread_id)) {
            /* Other s-locks can be allowed. If it is request x recursively while holding sx lock,
               this x lock should be along with the latching-order. */

            /* The existing X or SX lock is from this thread */
            if (rw_lock_lock_word_decr(lock, X_LOCK_DECR, 0)) {
                /* There is at least one SX-lock from this thread, but no X-lock. */
                /* Wait for any the other S-locks to be released. */
                rw_lock_x_lock_wait(lock, pass, -X_LOCK_HALF_DECR, file_name, line);
            } else {
                /* At least one X lock by this thread already exists. Add another. */
                if (lock->lock_word == 0 || lock->lock_word == -X_LOCK_HALF_DECR) {
                    lock->lock_word -= X_LOCK_DECR;
                } else {
                    ut_ad(lock->lock_word <= -X_LOCK_DECR);
                    --lock->lock_word;
                }
            }
        } else {
            /* Another thread locked before us */
            return(FALSE);
        }
    }

    ut_d(rw_lock_add_debug_info(lock, pass, RW_LOCK_EXCLUSIVE, file_name, line));

    lock->last_x_file_name = file_name;
    lock->last_x_line = (unsigned int) line;

    return(TRUE);
}

void rw_lock_x_lock_func(
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

    ut_ad(rw_lock_validate(lock));
    ut_ad(!rw_lock_own(lock, RW_LOCK_SHARED));

lock_loop:

    if (rw_lock_x_lock_low(lock, pass, file_name, line)) {
        if (count_os_wait > 0) {
            lock->count_os_wait += (uint32)count_os_wait;
            rw_lock_stats.rw_x_os_wait_count.add(count_os_wait);
        }
        rw_lock_stats.rw_x_spin_round_count.add(spin_count);

        /* Locking succeeded */
        return;
    } else {
        if (!spinning) {
          spinning = true;
          rw_lock_stats.rw_x_spin_wait_count.inc();
        }

        /* Spin waiting for the lock_word to become free */
        os_rmb;
        while (i < SYNC_SPIN_ROUNDS && lock->lock_word <= X_LOCK_HALF_DECR) {
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
            lock->count_os_wait += (uint32)count_os_wait;
            rw_lock_stats.rw_x_os_wait_count.add(count_os_wait);
        }
        rw_lock_stats.rw_x_spin_round_count.add(spin_count);

        return; /* Locking succeeded */
    }

    /* these stats may not be accurate */
    ++count_os_wait;
    sync_array_wait_event(sync_arr, cell);

    i = 0;
    goto lock_loop;
}

bool32 rw_lock_x_lock_func_nowait(rw_lock_t *lock, const char* file_name, uint32 line)
{
    bool32 success;

    success = atomic32_compare_and_swap(&lock->lock_word, X_LOCK_DECR, 0);

    if (success) {
        rw_lock_set_writer_id_and_recursion_flag(lock, TRUE);
    } else if (lock->recursive && os_thread_eq(lock->writer_thread, os_thread_get_curr_id())) {
        /* Relock: this lock_word modification is safe since no other
        threads can modify (lock, unlock, or reserve) lock_word while
        there is an exclusive writer and this is the writer thread. */
        if (lock->lock_word == 0 || lock->lock_word == -X_LOCK_HALF_DECR) {
            /* There are 1 x-locks */
            lock->lock_word -= X_LOCK_DECR;
        } else if (lock->lock_word <= -X_LOCK_DECR) {
            /* There are 2 or more x-locks */
            lock->lock_word--;
        } else {
            /* Failure */
            return(FALSE);
        }

        /* Watch for too many recursive locks */
        ut_ad(lock->lock_word < 0);

    } else {
        /* Failure */
        return(FALSE);
    }

   ut_d(rw_lock_add_debug_info(lock, 0, RW_LOCK_EXCLUSIVE, file_name, line));

    lock->last_x_file_name = file_name;
    lock->last_x_line = line;

    ut_ad(rw_lock_validate(lock));

    return(TRUE);
}

void rw_lock_x_unlock_func(uint32 pass, rw_lock_t *lock)
{
    ut_ad(lock->lock_word == 0 || lock->lock_word == -X_LOCK_HALF_DECR
          || lock->lock_word <= -X_LOCK_DECR);

    if (lock->lock_word == 0) {
        /* Last caller in a possible recursive chain. */
        lock->recursive = FALSE;
    }

    ut_d(rw_lock_remove_debug_info(lock, pass, RW_LOCK_EXCLUSIVE));

    if (lock->lock_word == 0 || lock->lock_word == -X_LOCK_HALF_DECR) {
        /* There is 1 x-lock */
        /* atomic increment is needed, because it is last */
        if (rw_lock_lock_word_incr(lock, X_LOCK_DECR) <= 0) {
            ut_error;
        }

        /* This no longer has an X-lock but it may still have
        an SX-lock. So it is now free for S-locks by other threads.
        We need to signal read/write waiters.
        We do not need to signal wait_ex waiters, since they cannot
        exist when there is a writer. */
        if (lock->waiters) {
            rw_lock_reset_waiter_flag(lock);
            os_event_set(lock->event);
            sync_array_object_signalled();
        }
    } else if (lock->lock_word == -X_LOCK_DECR || lock->lock_word == -(X_LOCK_DECR + X_LOCK_HALF_DECR)) {
        /* There are 2 x-locks */
        lock->lock_word += X_LOCK_DECR;
    } else {
        /* There are more than 2 x-locks. */
        ut_ad(lock->lock_word < -X_LOCK_DECR);
        lock->lock_word += 1;
    }

    ut_ad(rw_lock_validate(lock));
}

void sync_init(void)
{
    ut_a(sync_initialized == FALSE);

    sync_initialized = TRUE;

    /* Init the mutex list and create the mutex to protect it. */
    spin_lock_init(&rw_lock_list_lock);
    UT_LIST_INIT(rw_lock_list);

    sync_array_size = 32;
    sync_array_init(32);

    os_mutex_create(&rw_lock_debug_mutex);
    rw_lock_debug_event = os_event_create("rw_lock_debug_event");
    rw_lock_debug_waiters = FALSE;
}

void sync_close(void)
{
    os_mutex_destroy(&rw_lock_debug_mutex);
    os_event_destroy(rw_lock_debug_event);
    rw_lock_debug_waiters = FALSE;

    sync_array_close();

    sync_initialized = FALSE;
}

void sync_print(FILE *file)
{
    rw_lock_list_print_info(file);
    sync_array_print(file);
    sync_print_wait_info(file);
}

