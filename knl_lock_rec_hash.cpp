#include "knl_lock_rec_hash.h"
#include "cm_random.h"
#include "knl_hash_table.h"

/* The lock system */
lock_sys_t* lock_sys = NULL;


//Creates the lock system at database start
void lock_sys_create(uint32 n_cells) /*!< in: number of slots in lock hash table */
{
    uint32 lock_sys_sz;

    lock_sys_sz = sizeof(*lock_sys) + OS_THREAD_MAX_N * sizeof(srv_slot_t);

    lock_sys = static_cast<lock_sys_t*>(ut_zalloc_nokey(lock_sys_sz));

    void *ptr = &lock_sys[1];

    lock_sys->waiting_threads = static_cast<srv_slot_t*>(ptr);
    lock_sys->last_slot = lock_sys->waiting_threads;

    mutex_create(LATCH_ID_LOCK_SYS, &lock_sys->mutex);
    mutex_create(LATCH_ID_LOCK_SYS_WAIT, &lock_sys->wait_mutex);

    lock_sys->timeout_event = os_event_create(0);
    lock_sys->rec_hash = hash_create(n_cells);
    lock_sys->prdt_hash = hash_create(n_cells);
    lock_sys->prdt_page_hash = hash_create(n_cells);

    if (!srv_read_only_mode) {
        lock_latest_err_file = os_file_create_tmpfile(NULL);
        ut_a(lock_latest_err_file);
    }
}


/** Calculates the fold value of a page file address: used in inserting or
 searching for a lock in the hash table.
 @return folded value */
uint32 lock_rec_fold(space_id_t space,  /*!< in: space */
                    page_no_t page_no) /*!< in: page number */
{
  return (ut_fold_ulint_pair(space, page_no));
}

/** Calculates the hash value of a page file address: used in inserting or
 searching for a lock in the hash table.
 @return hashed value */
uint32 lock_rec_hash(space_id_t space,  /*!< in: space */
                    page_no_t page_no) /*!< in: page number */
{
  return (HASH_CALC_HASH(lock_sys->rec_hash, lock_rec_fold(space, page_no)));
}

/** Gets the heap_no of the smallest user record on a page.
 @return heap_no of smallest user record, or PAGE_HEAP_NO_SUPREMUM */
uint32 lock_get_min_heap_no(const buf_block_t *block) /*!< in: buffer block */
{
  const page_t *page = block->frame;

  //if (page_is_comp(page)) {
  //  return (rec_get_heap_no_new(
  //      page + rec_get_next_offs(page + PAGE_NEW_INFIMUM, TRUE)));
  //} else {
  //  return (rec_get_heap_no_old(
  //      page + rec_get_next_offs(page + PAGE_OLD_INFIMUM, FALSE)));
  //}
  return 0;
}

/** Get the lock hash table */
HASH_TABLE *lock_hash_get(uint32 mode) /*!< in: lock mode */
{
  //if (mode & LOCK_PREDICATE) {
  //  return (lock_sys->prdt_hash);
  //} else if (mode & LOCK_PRDT_PAGE) {
  //  return (lock_sys->prdt_page_hash);
  //} else {
    return (lock_sys->rec_hash);
//  }
}


