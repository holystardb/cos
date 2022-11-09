#include "sync_rwlock.h"
#include "cm_dbug.h"
#include "cm_memory.h"
#include "cm_random.h"

/** The global array of wait cells for implementation of the database's own mutexes and read-write locks */
static sync_array_t     **sync_wait_array;
uint32                    sync_array_size = 32;

/** Global list of database mutexes (not OS mutexes) created. */
typedef UT_LIST_BASE_NODE_T(mutex_t)  mutex_list_base_node_t;

/** Global list of database mutexes (not OS mutexes) created */
mutex_list_base_node_t                mutex_list;
spinlock_t                            mutex_list_lock;
bool32                                sync_initialized = FALSE;

static sync_array_t* sync_array_create(uint32 n_cells)  /*!< in: number of cells in the array to create */
{
    sync_array_t  *arr;

    ut_a(n_cells > 0);

    /* Allocate memory for the data structures */
    arr = (sync_array_t*)my_malloc(sizeof(*arr));
    memset(arr, 0x0, sizeof(*arr));

    arr->array = (sync_cell_t*)my_malloc(sizeof(sync_cell_t) * n_cells);
    memset(arr->array, 0x0, sizeof(sync_cell_t) * n_cells);

    arr->n_cells = n_cells;
    spin_lock_init(arr->lock);

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
    ut_a(n_threads > sync_array_size);

    sync_wait_array = (sync_array_t**)(my_malloc(sizeof(*sync_wait_array) * sync_array_size));

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

    //if (type == SYNC_MUTEX) {
    //    return(((ib_mutex_t*) cell->wait_object)->event);
    //} else if (type == RW_LOCK_WAIT_EXCLUSIVE) {
    //    return(((rw_lock_t*) cell->wait_object)->wait_ex_event);
    //} else { /* RW_LOCK_SHARED and RW_LOCK_EXCLUSIVE wait on the same event */
    //    return(((rw_lock_t*) cell->wait_object)->event);
    //}
    return NULL;
}

static void sync_array_reserve_cell(
    sync_array_t    *arr,  /*!< in: wait array */
    void            *object, /*!< in: pointer to the object to wait for */
    uint32          type,  /*!< in: lock request type */
    const char     *file,  /*!< in: file where requested */
    uint32          line,  /*!< in: line where requested */
    uint32         *index) /*!< out: index of the reserved cell */
{
    sync_cell_t *cell;
    os_event_t   event;
    uint32       i;

    ut_a(object);
    ut_a(index);

    spin_lock(&arr->lock, NULL);

    arr->res_count++;

    /* Reserve a new cell. */
    for (i = 0; i < arr->n_cells; i++) {
        cell = sync_array_get_nth_cell(arr, i);
        if (cell->wait_object == NULL) {
            cell->waiting = FALSE;
            cell->wait_object = object;
            cell->file = file;
            cell->line = line;
            cell->request_type = type;
            //if (type == SYNC_MUTEX) {
            //    cell->old_wait_mutex = static_cast<mutex_t*>(object);
            //} else {
            //    cell->old_wait_rw_lock = static_cast<rw_lock_t*>(object);
            //}

            arr->n_reserved++;
            *index = i;

            spin_unlock(&arr->lock);

            /* Make sure the event is reset and also store the value of signal_count at which the event was reset. */
            event = sync_cell_get_event(cell);
            //cell->signal_count = os_event_reset(event);
            cell->reservation_time = time(NULL);
            cell->thread = os_thread_get_curr_id();
            return;
        }
    }

    ut_error; /* No free cell found */
}


sync_array_t* sync_array_get_and_reserve_cell(
    void*		object, /*!< in: pointer to the object to wait for */
    uint32		type,	/*!< in: lock request type */
    const char*	file,	/*!< in: file where requested */
    uint32		line,	/*!< in: line where requested */
    sync_cell_t**	cell)	/*!< out: the cell reserved, never NULL */
{
    sync_array_t *sync_arr = NULL;

    *cell = NULL;
    for (ulint i = 0; i < sync_array_size && *cell == NULL; ++i) {
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
    sync_cell_t *cell;

    spin_lock(&arr->lock, NULL);

    cell = sync_array_get_nth_cell(arr, index);

    ut_a(cell->wait_object.lock != NULL);

    cell->waiting = FALSE;
    cell->wait_object.lock =  NULL;
    cell->signal_count = 0;

    /* Setup the list of free slots in the array */
    cell->line = arr->first_free_slot;

    arr->first_free_slot = cell - arr->array;

    ut_a(arr->n_reserved > 0);
    arr->n_reserved--;

    if (arr->next_free_slot > arr->n_cells / 2 && arr->n_reserved == 0) {
#ifdef UNIV_DEBUG
        for (ulint i = 0; i < arr->next_free_slot; ++i) {
            cell = sync_array_get_nth_cell(arr, i);

            ut_ad(!cell->waiting);
            ut_ad(cell->latch.mutex == 0);
            ut_ad(cell->signal_count == 0);
        }
#endif /* UNIV_DEBUG */
        arr->next_free_slot = 0;
        arr->first_free_slot = ULINT_UNDEFINED;
    }

    spin_unlock(&arr->lock);

    cell = 0;
}

static void sync_array_wait_event(sync_array_t *arr, uint32 index)
{
    sync_cell_t *cell;
    os_event_t  event;

    ut_a(arr);

    spin_lock(&arr->lock, NULL);

    cell = sync_array_get_nth_cell(arr, index);

    ut_a(cell->wait_object);
    ut_a(!cell->waiting);
    ut_ad(os_thread_get_curr_id() == cell->thread);

    event = sync_cell_get_event(cell);
    cell->waiting = TRUE;

    /* We use simple enter to the mutex below, because if
    we cannot acquire it at once, mutex_enter would call
    recursively sync_array routines, leading to trouble.
    rw_lock_debug_mutex freezes the debug lists. */
    //rw_lock_debug_mutex_enter();
    if (TRUE == sync_array_detect_deadlock(arr, cell, cell, 0)) {
        fputs("########################################\n", stderr);
        ut_error;
    }
    //rw_lock_debug_mutex_exit();

    spin_unlock(&arr->lock);

    os_event_wait(event/*, cell->signal_count*/);

    sync_array_free_cell(arr, index);
}

/******************************************************************//**
Returns the write-status of the lock - this function made more sense
with the old rw_lock implementation.
@return	RW_LOCK_NOT_LOCKED, RW_LOCK_EXCLUSIVE, RW_LOCK_WAIT_EXCLUSIVE */
uint32 rw_lock_get_writer(const rw_lock_t *lock)
{
    int32 lock_word = lock->lock_word;

    if (lock_word > 0) {
        /* return NOT_LOCKED in s-lock state, like the writer member of the old lock implementation. */
        return(RW_LOCK_NOT_LOCKED);
    } else if ((lock_word == 0) || (lock_word <= -X_LOCK_DECR)) {
        return(RW_LOCK_EXCLUSIVE);
    } else {
        ut_ad(lock_word > -X_LOCK_DECR);
        return(RW_LOCK_WAIT_EXCLUSIVE);
    }
}

/*********************************************************************//**
Prints info of a debug struct. */
void rw_lock_debug_print(FILE *f, rw_lock_debug_t *info)
{
    uint32 rwt;

    rwt = info->lock_type;

    fprintf(f, "Locked: thread %lu file %s line %lu  ",
        (ulong) os_thread_pf(info->thread_id), info->file_name,
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

/******************************************************************//**
Reports info of a wait array cell. */
static void sync_array_cell_print(
	FILE*		file,	/*!< in: file where to print */
	sync_cell_t*	cell)	/*!< in: sync cell */
{
	mutex_t   *mutex;
	rw_lock_t *rwlock;
	uint32     type;
	uint32     writer;

	type = cell->request_type;

	fprintf(file,
		"--Thread %lu has waited at %s line %lu"
		" for %.2f seconds the semaphore:\n",
		(ulong) os_thread_pf(cell->thread),
		cell->file, (ulong) cell->line,
		difftime(time(NULL), cell->reservation_time));

	if (type == SYNC_MUTEX) {
		/* We use old_wait_mutex in case the cell has already been freed meanwhile */
		mutex = cell->old_wait_mutex;

		fprintf(file,
			"Mutex at %p created file %s line %lu, lock var %lu\n"
#ifdef UNIV_SYNC_DEBUG
			"Last time reserved in file %s line %lu, "
#endif /* UNIV_SYNC_DEBUG */
			"waiters flag %lu\n",
			(void*) mutex, innobase_basename(mutex->cfile_name),
			(ulong) mutex->cline,
			(ulong) mutex->lock_word,
#ifdef UNIV_SYNC_DEBUG
			mutex->file_name, (ulong) mutex->line,
#endif /* UNIV_SYNC_DEBUG */
			(ulong) mutex->waiters);

	} else if (type == RW_LOCK_EXCLUSIVE
		   || type == RW_LOCK_WAIT_EXCLUSIVE
		   || type == RW_LOCK_SHARED) {

		fputs(type == RW_LOCK_EXCLUSIVE ? "X-lock on"
		      : type == RW_LOCK_WAIT_EXCLUSIVE ? "X-lock (wait_ex) on"
		      : "S-lock on", file);

		rwlock = cell->old_wait_rw_lock;

		fprintf(file,
			" RW-latch at %p created in file %s line %lu\n",
			(void*) rwlock, innobase_basename(rwlock->cfile_name),
			(ulong) rwlock->cline);
		writer = rw_lock_get_writer(rwlock);
		if (writer != RW_LOCK_NOT_LOCKED) {
			fprintf(file,
				"a writer (thread id %lu) has"
				" reserved it in mode %s",
				(ulong) os_thread_pf(rwlock->writer_thread),
				writer == RW_LOCK_EXCLUSIVE
				? " exclusive\n"
				: " wait exclusive\n");
		}

		fprintf(file,
			"number of readers %lu, waiters flag %lu, "
                        "lock_word: %lx\n"
			"Last time read locked in file %s line %lu\n"
			"Last time write locked in file %s line %lu\n",
			(ulong) rw_lock_get_reader_count(rwlock),
			(ulong) rwlock->waiters,
			rwlock->lock_word,
			rwlock->last_s_file_name,
			(ulong) rwlock->last_s_line,
			rwlock->last_x_file_name,
			(ulong) rwlock->last_x_line);
	} else {
		ut_error;
	}

	if (!cell->waiting) {
		fputs("wait has ended\n", file);
	}
}


/******************************************************************//**
Looks for a cell with the given thread id.
@return	pointer to cell or NULL if not found */
static sync_cell_t* sync_array_find_thread(
    sync_array_t *arr, /*!< in: wait array */
    os_thread_id_t thread) /*!< in: thread id */
{
    uint32 i;
    sync_cell_t *cell;

    for (i = 0; i < arr->n_cells; i++) {
        cell = sync_array_get_nth_cell(arr, i);
        if (cell->wait_object != NULL && os_thread_eq(cell->thread, thread)) {
            return(cell);	/* Found */
        }
    }

    return NULL; /* Not found */
}

/******************************************************************//**
Recursion step for deadlock detection.
@return TRUE if deadlock detected */
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

/******************************************************************//**
This function is called only in the debug version. Detects a deadlock
of one or more threads because of waits of semaphores.
@return TRUE if deadlock detected */
static bool32 sync_array_detect_deadlock(
    sync_array_t    *arr,	/*!< in: wait array; NOTE! the caller must own the mutex to array */
    sync_cell_t     *start,	/*!< in: cell where recursive search started */
    sync_cell_t     *cell,	/*!< in: cell to search */
    uint32          depth)	/*!< in: recursion depth */
{
    mutex_t*	mutex;
    rw_lock_t*	lock;
    os_thread_id_t	thread;
    bool32		ret;
    rw_lock_debug_t*debug;

    ut_a(arr);
    ut_a(start);
    ut_a(cell);
    ut_ad(cell->wait_object);
    ut_ad(os_thread_get_curr_id() == start->thread);
    ut_ad(depth < 100);

    depth++;

    if (!cell->waiting) {
        return(FALSE); /* No deadlock here */
    }

    if (cell->request_type == SYNC_MUTEX) {
        mutex = (mutex_t*)(cell->wait_object);
        if (mutex->level != 0) {
            thread = mutex->thread_id;
            /* Note that mutex->thread_id above may be
            also OS_THREAD_ID_UNDEFINED, because the
            thread which held the mutex maybe has not
            yet updated the value, or it has already
            released the mutex: in this case no deadlock
            can occur, as the wait array cannot contain
            a thread with ID_UNDEFINED value. */
            ret = sync_array_deadlock_step(arr, start, thread, 0, depth);
            if (ret) {
                fprintf(stderr,
                    "Mutex %p owned by thread %lu file %s line %lu\n",
                    mutex, (ulong) mutex->thread_id,
                    mutex->file_name, (ulong) mutex->line);
                    sync_array_cell_print(stderr, cell);

                return(TRUE);
            }
        }

        return(FALSE); /* No deadlock */

    } else if (cell->request_type == RW_LOCK_EXCLUSIVE || cell->request_type == RW_LOCK_WAIT_EXCLUSIVE) {

        lock = (rw_lock_t*)(cell->wait_object);

        for (debug = UT_LIST_GET_FIRST(lock->debug_list);
            debug != 0;
            debug = UT_LIST_GET_NEXT(list_node, debug)) {

            thread = debug->thread_id;

            if (((debug->lock_type == RW_LOCK_EXCLUSIVE) && !os_thread_eq(thread, cell->thread))
                || ((debug->lock_type == RW_LOCK_WAIT_EXCLUSIVE) && !os_thread_eq(thread, cell->thread))
                || (debug->lock_type == RW_LOCK_SHARED)) {

                /* The (wait) x-lock request can block infinitely only if someone (can be also cell thread) is holding s-lock,
                or someone(cannot be cell thread) (wait) x-lock, and he is blocked by start thread */

                ret = sync_array_deadlock_step(arr, start, thread, debug->pass, depth);
                if (ret) {
print:
                    fprintf(stderr, "rw-lock %p ", (void*) lock);
                    sync_array_cell_print(stderr, cell);
                    rw_lock_debug_print(stderr, debug);
                    return(TRUE);
                }
            }
        }

        return(FALSE);

    } else if (cell->request_type == RW_LOCK_SHARED) {

        lock = (rw_lock_t*)(cell->wait_object);

        for (debug = UT_LIST_GET_FIRST(lock->debug_list);
             debug != 0;
             debug = UT_LIST_GET_NEXT(list_node, debug)) {

            thread = debug->thread_id;
            if ((debug->lock_type == RW_LOCK_EXCLUSIVE) || (debug->lock_type == RW_LOCK_WAIT_EXCLUSIVE)) {
                /* The s-lock request can block infinitely
                only if someone (can also be cell thread) is
                holding (wait) x-lock, and he is blocked by start thread */

                ret = sync_array_deadlock_step(arr, start, thread, debug->pass, depth);
                if (ret) {
                    goto print;
                }
            }
        }

        return(FALSE);

    } else {
        ut_error;
    }

    return(TRUE);	/* Execution never reaches this line: for compiler fooling only */
}


void sync_init(void)
{
    ut_a(sync_initialized == FALSE);

    sync_initialized = TRUE;

    /* Init the mutex list and create the mutex to protect it. */
    spin_lock_init(mutex_list_lock);
    UT_LIST_INIT(mutex_list);

    sync_array_size = 32;
    sync_array_init(32);
}

void sync_close(void)
{
    mutex_t *mutex;

    sync_array_close();

    for (mutex = UT_LIST_GET_FIRST(mutex_list); mutex != NULL; /* No op */) {
        mutex_destroy(mutex);
        mutex = UT_LIST_GET_FIRST(mutex_list);
    }

    sync_initialized = FALSE;
}

static void mutex_set_waiters(    mutex_t *mutex,    uint32 n)
{
    volatile uint32 *ptr;  /* declared volatile to ensure that the value is stored to memory */

    ut_ad(mutex);

    ptr = &(mutex->waiters);
    *ptr = n;  /* Here we assume that the write of a single word in memory is atomic */
}

static uint32 mutex_get_waiters(mutex_t *mutex)
{
    const volatile uint32 *ptr;	/*!< declared volatile to ensure that the value is read from memory */
    ut_ad(mutex);

    ptr = &(mutex->waiters);

    return(*ptr);
}

static void mutex_signal_object(mutex_t *mutex)
{
    mutex_set_waiters(mutex, 0);
    os_event_set(mutex->event);
    //sync_array_object_signalled();
}

static bool32 mutex_validate(const mutex_t *mutex)
{
    ut_a(mutex);
    ut_a(mutex->magic_n == MUTEX_MAGIC_N);

    return(TRUE);
}

void mutex_create_func(mutex_t *mutex, char *cfile_name, uint32 cline)
{
    atomic32_test_and_set(&mutex->lock_word, 0);

    mutex->event = os_event_create(NULL);
    mutex_set_waiters(mutex, 0);

    mutex->cfile_name = cfile_name;
    mutex->cline = cline;
    mutex->count_os_wait = 0;
    mutex->magic_n = MUTEX_MAGIC_N;

    /* Check that lock_word is aligned; this is important on Intel */
    ut_ad(((uint32)(mutex->lock_word) % 4 == 0));

    spin_lock(&mutex_list_lock, NULL);
    ut_ad(UT_LIST_GET_LEN(mutex_list) == 0 || UT_LIST_GET_FIRST(mutex_list)->magic_n == MUTEX_MAGIC_N);
    UT_LIST_ADD_FIRST(list, mutex_list, mutex);
    spin_unlock(&mutex_list_lock);
}

void mutex_destroy_func(mutex_t *mutex)
{
    ut_ad(mutex_validate(mutex));
    ut_a(mutex->lock_word == 0);
    ut_a(mutex_get_waiters(mutex) == 0);

    spin_lock(&mutex_list_lock, NULL);

    ut_ad(!UT_LIST_GET_PREV(list, mutex) || UT_LIST_GET_PREV(list, mutex)->magic_n == MUTEX_MAGIC_N);
    ut_ad(!UT_LIST_GET_NEXT(list, mutex) || UT_LIST_GET_NEXT(list, mutex)->magic_n == MUTEX_MAGIC_N);

    UT_LIST_REMOVE(list, mutex_list, mutex);

    spin_unlock(&mutex_list_lock);

    os_event_destroy(mutex->event);

    mutex->magic_n = 0;
}

bool32 mutex_own(const mutex_t *mutex)
{
    ut_ad(mutex_validate(mutex));
    return (mutex->lock_word == 1 && os_thread_eq(mutex->thread_id, os_thread_get_curr_id()));
}

bool32 mutex_enter_nowait_func(mutex_t *mutex, const char *file_name, uint32 line)
{
    ut_ad(mutex_validate(mutex));

    if (!atomic32_test_and_set(&mutex->lock_word, 1)) {
        ut_d(mutex->thread_id = os_thread_get_curr_id());
        return TRUE; /* Succeeded! */
    }

    return FALSE;
}

#define SYNC_SPIN_ROUNDS         30
#define SYNC_SPIN_WAIT_DELAY     6

static void mutex_spin_wait(mutex_t *mutex, const char *file_name, uint32 line)
{
    uint32        i; /* spin round count */
    uint32        index; /* index of the reserved wait cell */
    sync_array_t *sync_arr;

    ut_ad(mutex);

mutex_loop:

    i = 0;

spin_loop:

    while (mutex->lock_word != 0 && i < SYNC_SPIN_ROUNDS) {
        if (SYNC_SPIN_WAIT_DELAY) {
            os_thread_delay(ut_rnd_interval(0, SYNC_SPIN_WAIT_DELAY));
        }
        i++;
    }
    if (i == SYNC_SPIN_ROUNDS) {
        os_thread_yield();
    }

    if (atomic32_test_and_set(&mutex->lock_word, 1) == 0) {
        ut_d(mutex->thread_id = os_thread_get_curr_id());
        return;  /* Succeeded! */
    }

    i++;
    if (i < SYNC_SPIN_ROUNDS) {
        goto spin_loop;
    }

    sync_arr = sync_array_get();
    sync_array_reserve_cell(sync_arr, mutex, SYNC_MUTEX, file_name, line, &index);

    mutex_set_waiters(mutex, 1);

    /* Try to reserve still a few times */
    for (i = 0; i < 4; i++) {
        if (atomic32_test_and_set(&mutex->lock_word, 1) == 0) {
            sync_array_free_cell(sync_arr, index);
            ut_d(mutex->thread_id = os_thread_get_curr_id());

            return;    /* Succeeded! Free the reserved wait cell */

            /* Note that in this case we leave the waiters field set to 1. 
            We cannot reset it to zero, as we do not know if there are other waiters. */
        }
    }

    mutex->count_os_wait++;
    sync_array_wait_event(sync_arr, index);

    goto mutex_loop;
}

void mutex_enter_func(mutex_t *mutex, const char *file_name, uint32 line)
{
    ut_ad(mutex_validate(mutex));
    ut_ad(!mutex_own(mutex));

    if (!atomic32_test_and_set(&mutex->lock_word, 1)) {
        ut_d(mutex->thread_id = os_thread_get_curr_id());
        return;  /* Succeeded! */
    }

    mutex_spin_wait(mutex, file_name, line);
}

void mutex_exit_func(mutex_t *mutex)
{
    ut_ad(mutex_own(mutex));
    ut_d(mutex->thread_id = (os_thread_id_t) UINT32_UNDEFINED);

    atomic32_test_and_set(&mutex->lock_word, 0);

    if (mutex_get_waiters(mutex) != 0) {
        mutex_signal_object(mutex);
    }
}


/**************************************************************************************
*
**************************************************************************************/

typedef UT_LIST_BASE_NODE_T(rw_lock_t) rw_lock_list_t;

/* The global list of rw-locks */
rw_lock_list_t rw_lock_list;
spinlock_t rw_lock_list_lock;



/******************************************************************//**
Inserts the debug information for an rw-lock. */
void rw_lock_add_debug_info(
    rw_lock_t*	lock,		/*!< in: rw-lock */
    uint32		pass,		/*!< in: pass value */
    uint32		lock_type,	/*!< in: lock type */
    const char*	file_name,	/*!< in: file where requested */
    uint32		line)		/*!< in: line where requested */
{
    rw_lock_debug_t*	info;

    ut_ad(lock);
    ut_ad(file_name);

    info = ((rw_lock_debug_t*) my_malloc(sizeof(rw_lock_debug_t)));

    //rw_lock_debug_mutex_enter();

    info->file_name = file_name;
    info->line = line;
    info->lock_type = lock_type;
    info->thread_id = os_thread_get_curr_id();
    info->pass = pass;

    UT_LIST_ADD_FIRST(list, lock->debug_list, info);

    //rw_lock_debug_mutex_exit();

    if ((pass == 0) && (lock_type != RW_LOCK_WAIT_EXCLUSIVE)) {
        //sync_thread_add_level(lock, lock->level, lock_type == RW_LOCK_EXCLUSIVE && lock->lock_word < 0);
    }
}

void sync_check_init(size_t max_threads)
{
  /* Init the rw-lock & mutex list and create the mutex to protect it. */
  UT_LIST_INIT(rw_lock_list);
  spin_lock_init(rw_lock_list_lock);

  sync_array_init((uint32)max_threads);
}

void rw_lock_create_func(
    rw_lock_t *lock, /*!< in: pointer to memory */
    const char *cfile_name,  /*!< in: file name where created */
    uint32 cline)             /*!< in: file line where created */
{
    ut_ad(lock->magic_n == RW_LOCK_MAGIC_N);

    /* If this is the very first time a synchronization object is
    created, then the following call initializes the sync system. */

    mutex_create(&lock->mutex);

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
    //lock->level = level;
}

/** Checks that the rw-lock has been initialized and that there are no simultaneous shared and exclusive locks. */
bool rw_lock_validate(const rw_lock_t *lock)
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

/******************************************************************//**
Checks if the thread has locked the rw-lock in the specified mode, with the pass value == 0.
@return TRUE if locked */
bool32 rw_lock_own(rw_lock_t *lock, uint32 lock_type)
{
return TRUE;

/*
    rw_lock_debug_t*	info;

    ut_ad(lock);
    ut_ad(rw_lock_validate(lock));

    rw_lock_debug_mutex_enter();
    info = UT_LIST_GET_FIRST(lock->debug_list);
    while (info != NULL) {
        if (os_thread_eq(info->thread_id, os_thread_get_curr_id()) && (info->pass == 0) && (info->lock_type == lock_type)) {
            rw_lock_debug_mutex_exit();
            return TRUE;
        }
        info = UT_LIST_GET_NEXT(list, info);
    }
    rw_lock_debug_mutex_exit();

    return FALSE;
*/
}

void rw_lock_destroy_func(rw_lock_t *lock)
{
    ut_ad(rw_lock_validate(lock));
    ut_a(lock->lock_word == X_LOCK_DECR);

    spin_lock(&rw_lock_list_lock, NULL);
    mutex_destroy(&lock->mutex);
    os_event_destroy(lock->event);
    os_event_destroy(lock->wait_ex_event);
    UT_LIST_REMOVE(list_node, rw_lock_list, lock);
    spin_unlock(&rw_lock_list_lock);
}





/******************************************************************//**
Two different implementations for decrementing the lock_word of a rw_lock:
one for systems supporting atomic operations, one for others. This does
does not support recusive x-locks: they should be handled by the caller and
need not be atomic since they are performed by the current lock holder.
Returns true if the decrement was made, false if not.
@return	TRUE if decr occurs */
bool32 rw_lock_lock_word_decr(rw_lock_t *lock, uint32 amount)
{
    int32 local_lock_word = lock->lock_word;
    while (local_lock_word > 0) {
        if (atomic32_compare_and_swap(&lock->lock_word, local_lock_word, local_lock_word - amount)) {
            return(TRUE);
        }
        local_lock_word = lock->lock_word;
    }
    return(FALSE);
}

/******************************************************************//**
Increments lock_word the specified amount and returns new value.
@return lock->lock_word after increment */
int32 rw_lock_lock_word_incr(rw_lock_t *lock, uint32 amount)
{
    return(atomic32_add(&lock->lock_word, amount));
}

bool32 rw_lock_s_lock_low(
    rw_lock_t*	lock,	/*!< in: pointer to rw-lock */
    uint32		pass, /*!< in: pass value; != 0, if the lock will be passed to another thread to unlock */
    const char*	file_name, /*!< in: file name where lock requested */
    uint32		line)	/*!< in: line where requested */
{
    if (!rw_lock_lock_word_decr(lock, 1, 0)) {
        /* Locking did not succeed */
        return(FALSE);
    }

    ut_d(rw_lock_add_debug_info(lock, pass, RW_LOCK_SHARED, file_name, line);

    /* These debugging values are not set safely: they may be incorrect
    or even refer to a line that is invalid for the file name. */
    lock->last_s_file_name = file_name;
    lock->last_s_line = line;

    return(TRUE);	/* locking succeeded */
}

/********************************************************************//**
Sets lock->waiters to 1. It is not an error if lock->waiters is already
1. On platforms where ATOMIC builtins are used this function enforces a memory barrier. */
void rw_lock_set_waiter_flag(rw_lock_t *lock)
{
    (void)atomic32_compare_and_swap(&lock->waiters, 0, 1);
}


/********************************************************************//**
Resets lock->waiters to 0. It is not an error if lock->waiters is already
0. On platforms where ATOMIC builtins are used this function enforces a memory barrier. */
void rw_lock_reset_waiter_flag(rw_lock_t *lock)
{
    (void) atomic32_compare_and_swap(&lock->waiters, 1, 0);
}

/******************************************************************//**
Lock an rw-lock in shared mode for the current thread. If the rw-lock is
locked in exclusive mode, or there is an exclusive lock request waiting,
the function spins a preset time (controlled by SYNC_SPIN_ROUNDS), waiting
for the lock, before suspending the thread. */
void rw_lock_s_lock_spin(
    rw_lock_t*	lock,	/*!< in: pointer to rw-lock */
    uint32		pass,	/*!< in: pass value; != 0, if the lock will be passed to another thread to unlock */
    const char*	file_name, /*!< in: file name where lock requested */
    uint32		line)	/*!< in: line where requested */
{
    uint32          i = 0; /* spin round count */
    sync_array_t   *sync_arr;
    uint32          spin_count = 0;
    uint64          count_os_wait = 0;

    /* We reuse the thread id to index into the counter, cache it here for efficiency. */

    ut_ad(rw_lock_validate(lock));

    //rw_lock_stats.rw_s_spin_wait_count.add(counter_index, 1);
lock_loop:

    /* Spin waiting for the writer field to become free */
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
            //rw_lock_stats.rw_s_os_wait_count
        }
        //rw_lock_stats.rw_s_spin_round_count.add(counter_index, i);
        return; /* Success */
    } else {
        if (i < SYNC_SPIN_ROUNDS) {
            goto lock_loop;
        }

        ++count_os_wait;

        sync_cell_t *cell;
        sync_arr = sync_array_get_reserve_cell(lock, RW_LOCK_SHARED, file_name, line, &cell);
        /* Set waiters before checking lock_word to ensure wake-up
        signal is sent. This may lead to some unnecessary signals. */
        rw_lock_set_waiter_flag(lock);

        if (rw_lock_s_lock_low(lock, pass, file_name, line)) {
            sync_array_free_cell(sync_arr, cell);
            if (count_os_wait > 0) {
                lock->count_os_wait += count_os_wait;
                //rw_lock_stats.rw_s_os_wait_count
            }
            //rw_lock_stats.rw_s_spin_round_count.add(counter_index, i);
            return; /* Success */
        }

        sync_array_wait_event(sync_arr, cell);
        i = 0;
        goto lock_loop;
    }
}


/******************************************************************//**
NOTE! Use the corresponding macro, not directly this function! Lock an
rw-lock in shared mode for the current thread. If the rw-lock is locked
in exclusive mode, or there is an exclusive lock request waiting, the
function spins a preset time (controlled by SYNC_SPIN_ROUNDS), waiting for
the lock, before suspending the thread. */
void rw_lock_s_lock_func(
    rw_lock_t*	lock,	/*!< in: pointer to rw-lock */
    uint32		pass,	/*!< in: pass value; != 0, if the lock will be passed to another thread to unlock */
    const char*	file_name,/*!< in: file name where lock requested */
    uint32		line)	/*!< in: line where requested */
{
    /* NOTE: As we do not know the thread ids for threads which have
    s-locked a latch, and s-lockers will be served only after waiting
    x-lock requests have been fulfilled, then if this thread already
    owns an s-lock here, it may end up in a deadlock with another thread
    which requests an x-lock here. Therefore, we will forbid recursive
    s-locking of a latch: the following assert will warn the programmer
    of the possibility of this kind of a deadlock. If we want to implement
    safe recursive s-locking, we should keep in a list the thread ids of
    the threads which have s-locked a latch. This would use some CPU
    time. */

    if (rw_lock_s_lock_low(lock, pass, file_name, line)) {
        return; /* Success */
    } else {
        /* Did not succeed, try spin wait */
        rw_lock_s_lock_spin(lock, pass, file_name, line);
        return;
    }
}

/******************************************************************//**
Releases a shared mode lock. */
void rw_lock_s_unlock_func(
#ifdef UNIV_SYNC_DEBUG
    uint32		pass,	/*!< in: pass value; != 0, if the lock may have been passed to another thread to unlock */
#endif
    rw_lock_t*	lock)	/*!< in/out: rw-lock */
{
    ut_ad(lock->lock_word > -X_LOCK_DECR);
    ut_ad(lock->lock_word != 0);
    ut_ad(lock->lock_word < X_LOCK_DECR);

#ifdef UNIV_SYNC_DEBUG
    rw_lock_remove_debug_info(lock, pass, RW_LOCK_SHARED);
#endif

    /* Increment lock_word to indicate 1 less reader */
    if (rw_lock_lock_word_incr(lock, 1) == 0) {
        /* wait_ex waiter exists. It may not be asleep, but we signal anyway.
        We do not wake other waiters, because they can't
        exist without wait_ex waiter and wait_ex waiter goes first.*/
        os_event_set(lock->wait_ex_event);
        //sync_array_object_signalled();
    }

    ut_ad(rw_lock_validate(lock));
}

/******************************************************************//**
This function sets the lock->writer_thread and lock->recursive fields.
For platforms where we are using atomic builtins instead of lock->mutex
it sets the lock->writer_thread field using atomics to ensure memory
ordering. Note that it is assumed that the caller of this function
effectively owns the lock i.e.: nobody else is allowed to modify
lock->writer_thread at this point in time.
The protocol is that lock->writer_thread MUST be updated BEFORE the
lock->recursive flag is set. */
void rw_lock_set_writer_id_and_recursion_flag(
    rw_lock_t*	lock,		/*!< in/out: lock to work on */
    bool32		recursive)	/*!< in: TRUE if recursion allowed */
{
    os_thread_id_t	curr_thread	= os_thread_get_curr_id();
    os_thread_id_t	local_thread;
    bool32		success;

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


/******************************************************************//**
Function for the next writer to call. Waits for readers to exit.
The caller must have already decremented lock_word by X_LOCK_DECR. */
void rw_lock_x_lock_wait(
    rw_lock_t*	lock,	/*!< in: pointer to rw-lock */
#ifdef UNIV_SYNC_DEBUG
    uint32		pass,	/*!< in: pass value; != 0, if the lock will be passed to another thread to unlock */
#endif
    const char*	file_name,/*!< in: file name where lock requested */
    uint32		line)	/*!< in: line where requested */
{
    uint32		index;
    uint32		i = 0;
    sync_array_t*	sync_arr;
    size_t		counter_index;

    /* We reuse the thread id to index into the counter, cache it here for efficiency. */
    counter_index = (size_t) os_thread_get_curr_id();

    ut_ad(lock->lock_word <= 0);

    while (lock->lock_word < 0) {
        if (SYNC_SPIN_WAIT_DELAY) {
            os_thread_delay(ut_rnd_interval(0, SYNC_SPIN_WAIT_DELAY));
        }

        if(i < SYNC_SPIN_ROUNDS) {
            i++;
            continue;
        }

        /* If there is still a reader, then go to sleep.*/
        //rw_lock_stats.rw_x_spin_round_count.add(counter_index, i);
        sync_arr = sync_array_get();
        sync_array_reserve_cell(sync_arr, lock, RW_LOCK_WAIT_EXCLUSIVE, file_name, line, &index);
        i = 0;
        /* Check lock_word to ensure wake-up isn't missed.*/
        if (lock->lock_word < 0) {
            /* these stats may not be accurate */
            lock->count_os_wait++;
            //rw_lock_stats.rw_x_os_wait_count.add(counter_index, 1);

            /* Add debug info as it is needed to detect possible
            deadlock. We must add info for WAIT_EX thread for
            deadlock detection to work properly. */
#ifdef UNIV_SYNC_DEBUG
            rw_lock_add_debug_info(lock, pass, RW_LOCK_WAIT_EXCLUSIVE, file_name, line);
#endif

            sync_array_wait_event(sync_arr, index);

            /* It is possible to wake when lock_word < 0.
            We must pass the while-loop check to proceed.*/
        } else {
            sync_array_free_cell(sync_arr, index);
        }
    }
    //rw_lock_stats.rw_x_spin_round_count.add(counter_index, i);
}

/******************************************************************//**
Low-level function for acquiring an exclusive lock.
@return	FALSE if did not succeed, TRUE if success. */
bool32 rw_lock_x_lock_low(
    rw_lock_t*	lock,	/*!< in: pointer to rw-lock */
    uint32		pass,	/*!< in: pass value; != 0, if the lock will be passed to another thread to unlock */
    const char*	file_name,/*!< in: file name where lock requested */
    uint32		line)	/*!< in: line where requested */
{
    if (rw_lock_lock_word_decr(lock, X_LOCK_DECR)) {

        /* lock->recursive also tells us if the writer_thread
        field is stale or active. As we are going to write
        our own thread id in that field it must be that the
        current writer_thread value is not active. */
        ut_a(!lock->recursive);

        /* Decrement occurred: we are writer or next-writer. */
        rw_lock_set_writer_id_and_recursion_flag(lock, pass ? FALSE : TRUE);

        rw_lock_x_lock_wait(lock,
#ifdef UNIV_SYNC_DEBUG
            pass,
#endif
            file_name, line);

    } else {
        os_thread_id_t thread_id = os_thread_get_curr_id();

        /* Decrement failed: relock or failed lock */
        if (!pass && lock->recursive && os_thread_eq(lock->writer_thread, thread_id)) {
            /* Relock */
            if (lock->lock_word == 0) {
                lock->lock_word -= X_LOCK_DECR;
            } else {
                --lock->lock_word;
            }
        } else {
            /* Another thread locked before us */
            return(FALSE);
        }
    }
#ifdef UNIV_SYNC_DEBUG
    rw_lock_add_debug_info(lock, pass, RW_LOCK_EXCLUSIVE, file_name, line);
#endif
    lock->last_x_file_name = file_name;
    lock->last_x_line = (unsigned int) line;

    return(TRUE);
}

/******************************************************************//**
NOTE! Use the corresponding macro, not directly this function! Lock an
rw-lock in exclusive mode for the current thread. If the rw-lock is locked
in shared or exclusive mode, or there is an exclusive lock request waiting,
the function spins a preset time (controlled by SYNC_SPIN_ROUNDS), waiting
for the lock before suspending the thread. If the same thread has an x-lock
on the rw-lock, locking succeed, with the following exception: if pass != 0,
only a single x-lock may be taken on the lock. NOTE: If the same thread has
an s-lock, locking does not succeed! */
void rw_lock_x_lock_func(
	rw_lock_t*	lock,	/*!< in: pointer to rw-lock */
	uint32		pass,	/*!< in: pass value; != 0, if the lock will be passed to another thread to unlock */
	const char*	file_name,/*!< in: file name where lock requested */
	uint32		line)	/*!< in: line where requested */
{
    uint32		i;	/*!< spin round count */
    uint32		index;	/*!< index of the reserved wait cell */
    sync_array_t*	sync_arr;
    bool32		spinning = FALSE;
    size_t		counter_index;

    /* We reuse the thread id to index into the counter, cache it here for efficiency. */
    counter_index = (size_t) os_thread_get_curr_id();

    ut_ad(rw_lock_validate(lock));
    ut_ad(!rw_lock_own(lock, RW_LOCK_SHARED));

    i = 0;

lock_loop:

    if (rw_lock_x_lock_low(lock, pass, file_name, line)) {
        //rw_lock_stats.rw_x_spin_round_count.add(counter_index, i);
        return;	/* Locking succeeded */
    } else {
        if (!spinning) {
            spinning = TRUE;
            //rw_lock_stats.rw_x_spin_wait_count.add(counter_index, 1);
        }

        /* Spin waiting for the lock_word to become free */
        while (i < SYNC_SPIN_ROUNDS && lock->lock_word <= 0) {
            if (SYNC_SPIN_WAIT_DELAY) {
                os_thread_delay(ut_rnd_interval(0, SYNC_SPIN_WAIT_DELAY));
            }
            i++;
        }
        if (i == SYNC_SPIN_ROUNDS) {
            os_thread_yield();
        } else {
            goto lock_loop;
        }
    }

    //rw_lock_stats.rw_x_spin_round_count.add(counter_index, i);

    sync_arr = sync_array_get();
    sync_array_reserve_cell(sync_arr, lock, RW_LOCK_EXCLUSIVE, file_name, line, &index);

    /* Waiters must be set before checking lock_word, to ensure signal
    is sent. This could lead to a few unnecessary wake-up signals. */
    rw_lock_set_waiter_flag(lock);

    if (rw_lock_x_lock_low(lock, pass, file_name, line)) {
        sync_array_free_cell(sync_arr, index);
        return; /* Locking succeeded */
    }

    /* these stats may not be accurate */
    lock->count_os_wait++;
    //rw_lock_stats.rw_x_os_wait_count.add(counter_index, 1);
    sync_array_wait_event(sync_arr, index);

    i = 0;
    goto lock_loop;
}


/******************************************************************//**
NOTE! Use the corresponding macro, not directly this function! Lock an
rw-lock in exclusive mode for the current thread if the lock can be obtained immediately.
@return TRUE if success */
bool32 rw_lock_x_lock_func_nowait(rw_lock_t *lock, const char* file_name, uint32 line)
{
    os_thread_id_t curr_thread = os_thread_get_curr_id();
    bool32 success = atomic32_compare_and_swap(&lock->lock_word, X_LOCK_DECR, 0);

    if (success) {
        rw_lock_set_writer_id_and_recursion_flag(lock, TRUE);
    } else if (lock->recursive && os_thread_eq(lock->writer_thread, curr_thread)) {
        /* Relock: this lock_word modification is safe since no other
        threads can modify (lock, unlock, or reserve) lock_word while
        there is an exclusive writer and this is the writer thread. */
        if (lock->lock_word == 0) {
            lock->lock_word = -X_LOCK_DECR;
        } else {
            lock->lock_word--;
        }

        /* Watch for too many recursive locks */
        ut_ad(lock->lock_word < 0);

    } else {
        /* Failure */
        return(FALSE);
    }

#ifdef UNIV_SYNC_DEBUG
    rw_lock_add_debug_info(lock, 0, RW_LOCK_EXCLUSIVE, file_name, line);
#endif

    lock->last_x_file_name = file_name;
    lock->last_x_line = line;

    ut_ad(rw_lock_validate(lock));

    return(TRUE);
}


/******************************************************************//**
Releases an exclusive mode lock. */
void rw_lock_x_unlock_func(rw_lock_t *lock)
{
    ut_ad(lock->lock_word == 0 || lock->lock_word <= -X_LOCK_DECR);

    /* lock->recursive flag also indicates if lock->writer_thread is
    valid or stale. If we are the last of the recursive callers
    then we must unset lock->recursive flag to indicate that the
    lock->writer_thread is now stale.
    Note that since we still hold the x-lock we can safely read the
    lock_word. */
    if (lock->lock_word == 0) {
        /* Last caller in a possible recursive chain. */
        lock->recursive = FALSE;
    }

    uint32 x_lock_incr;
    if (lock->lock_word == 0) {
        x_lock_incr = X_LOCK_DECR;
    } else if (lock->lock_word == -X_LOCK_DECR) {
        x_lock_incr = X_LOCK_DECR;
    } else {
        ut_ad(lock->lock_word < -X_LOCK_DECR);
        x_lock_incr = 1;
    }

    if (rw_lock_lock_word_incr(lock, x_lock_incr) == X_LOCK_DECR) {
        /* Lock is now free. May have to signal read/write waiters.
        We do not need to signal wait_ex waiters, since they cannot
        exist when there is a writer. */
        if (lock->waiters) {
            rw_lock_reset_waiter_flag(lock);
            os_event_set(lock->event);
            //sync_array_object_signalled();
        }
    }

    ut_ad(rw_lock_validate(lock));
}

