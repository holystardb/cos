#ifndef _KNL_LOCK_REC_HASH_H
#define _KNL_LOCK_REC_HASH_H

#include "cm_type.h"
#include "cm_hashtable.h"
#include "knl_server.h"
#include "knl_buf.h"

/* CPU cache line size; a constant 64 for now.  */
#define CACHE_LINE_SIZE 64

/* A memcached constant; also defined in default_engine.h */
#define POWER_LARGEST  200


/** The lock system struct */
struct lock_sys_t{
	char pad1[CACHE_LINE_SIZE]; /*!< padding to prevent other memory update hotspots from residing on the same memory cache line */
	mutex_t mutex; /*!< Mutex protecting the locks */
	HASH_TABLE*	rec_hash;		/*!< hash table of the record locks */
    HASH_TABLE*	prdt_hash;		/*!< hash table of the predicate lock */
    HASH_TABLE*	prdt_page_hash;		/*!< hash table of the page lock */

	char		pad2[CACHE_LINE_SIZE];	/*!< Padding */
	os_mutex_t wait_mutex;		/*!< Mutex protecting the next two fields */
	srv_slot_t*	waiting_threads;	/*!< Array  of user threads
						suspended while waiting for
						locks within InnoDB, protected
						by the lock_sys->wait_mutex */
	srv_slot_t*	last_slot; /*!< highest slot ever used in the waiting_threads array, protected by lock_sys->wait_mutex */
	bool32		rollback_complete; /*!< TRUE if rollback of all recovered transactions is complete. Protected by lock_sys->mutex */

	uint32		n_lock_max_wait_time; /*!< Max wait time */

	os_event_t	timeout_event; /*!< Set to the event that is created in the lock wait monitor thread. A value of 0 means the thread is not active */

	bool32		timeout_thread_active; /*!< True if the timeout thread is running */
};



/** Calculates the fold value of a page file address: used in inserting or
 searching for a lock in the hash table.
 @return folded value */
uint32 lock_rec_fold(space_id_t space, page_no_t page_no);

/** Calculates the hash value of a page file address: used in inserting or
 searching for a lock in the hash table.
 @return hashed value */
uint32 lock_rec_hash(space_id_t space, page_no_t page_no);

/** Gets the heap_no of the smallest user record on a page.
 @return heap_no of smallest user record, or PAGE_HEAP_NO_SUPREMUM */
uint32 lock_get_min_heap_no(const buf_block_t *block);

/** Get the lock hash table */
HASH_TABLE *lock_hash_get(uint32 mode);






#endif  /* _KNL_LOCK_REC_HASH_H */
