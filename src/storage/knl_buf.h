#ifndef _KNL_BUF_H
#define _KNL_BUF_H

#include "cm_type.h"
#include "cm_error.h"
#include "cm_list.h"
#include "cm_datetime.h"
#include "cm_dbug.h"
#include "cm_rwlock.h"
#include "cm_rbt.h"
#include "cm_mutex.h"
#include "knl_mtr.h"
//#include "knl_fsp.h"
#include "knl_file_system.h"
#include "knl_hash_table.h"
#include "knl_page_size.h"
#include "knl_page_id.h"
#include "knl_server.h"

#ifdef __cplusplus
extern "C" {
#endif



// The maximum number of buffer pools that can be defined
constexpr uint32 MAX_BUFFER_POOLS = 64;
// The maximum number of page_hash locks
constexpr uint32 MAX_PAGE_HASH_LOCKS = 4096;
// Maximum number of concurrent buffer pool watches
//#define BUF_POOL_WATCH_SIZE (srv_n_purge_threads + 1)

#define BUF_PAGE_ACCESS_WINDOW    3000000     // us, increase touch_number if access interval > BUF_ACCESS_WINDOW
#define BUF_HOT_PAGE_TCH          3           // consider buffer is hot if its touch_number >= BUF_TCH_AGE
#define BUF_PAGE_AGE_DEC_FACTOR   2






/** Get appropriate page_hash_lock. */
#define buf_page_hash_lock_get(buf_pool, page_id) \
  hash_get_lock((buf_pool)->page_hash, (page_id).fold())

/** If not appropriate page_hash_lock, relock until appropriate. */
#define buf_page_hash_lock_s_confirm(hash_lock, buf_pool, page_id) \
  hash_lock_s_confirm(hash_lock, (buf_pool)->page_hash, (page_id).fold())

#define buf_page_hash_lock_x_confirm(hash_lock, buf_pool, page_id) \
  hash_lock_x_confirm(hash_lock, (buf_pool)->page_hash, (page_id).fold())

#define buf_block_get_frame(block) ((block)->frame)



enum class Page_fetch {
    // Get always
    NORMAL,

    // get if in pool
    IF_IN_POOL,

    // get if in pool, do not make the block young in the LRU list
    PEEK_IF_IN_POOL,

    RESIDENT
};

typedef enum en_buf_page_state {
  BUF_BLOCK_POOL_WATCH, /*!< a sentinel for the buffer pool
                        watch, element of buf_pool->watch[] */
  BUF_BLOCK_ZIP_PAGE,   /*!< contains a clean
                        compressed page */
  BUF_BLOCK_ZIP_DIRTY,  /*!< contains a compressed
                        page that is in the
                        buf_pool->flush_list */

  BUF_BLOCK_NOT_USED,      /*!< is in the free list;
                           must be after the BUF_BLOCK_ZIP_
                           constants for compressed-only pages
                           @see buf_block_state_valid() */
  BUF_BLOCK_READY_FOR_USE, /*!< when buf_LRU_get_free_block
                           returns a block, it is in this state */
  BUF_BLOCK_FILE_PAGE,     /*!< contains a buffered file page */
  BUF_BLOCK_MEMORY,        /*!< contains some main memory object */
  BUF_BLOCK_REMOVE_HASH    /*!< hash index should be removed
                           before putting to the free list */
} buf_page_state_t;

/** Flags for io_fix types */
typedef enum en_buf_io_fix {
  BUF_IO_NONE = 0, // no pending I/O
  BUF_IO_READ,     // read pending
  BUF_IO_WRITE,    // write pending
  BUF_IO_PIN       // disallow relocation of block and its removal of from the flush_list
} buf_io_fix_t;



class buf_page_t {
public:
    /** Page id. */
    page_id_t id;

    /** Page size. */
    page_size_t size;

    // Count of how manyfold this block is currently bufferfixed
    atomic32_t buf_fix_count;

    /** type of pending I/O operation. */
    buf_io_fix_t io_fix;

    /** Block state. */
    buf_page_state_t state;

    // if this block is currently being flushed to disk,
    // this tells the flush_type.
    uint32 flush_type : 2;
    // index number of the buffer pool that this block belongs to
    uint32 buf_pool_index : 6;
    uint32 in_page_hash : 1;
    uint32 in_flush_list : 1; // protected by buf_pool->flush_list_mutex
    uint32 in_free_list : 1;
    uint32 in_LRU_list : 1;
    // this is set to TRUE when fsp frees a page in buffer pool;
    // protected by buf_block_t::mutex
    uint32 file_page_was_freed : 1;
    uint32 reserved : 19;

    // node used in chaining to buf_pool->page_hash or buf_pool->zip_hash
    buf_page_t* hash;

    // based on state, this is a list node, protected either by buf_pool->mutex or by buf_pool->flush_list_mutex,
    // in one of the following lists in buf_pool:
    //        - BUF_BLOCK_NOT_USED:   free_pages
    //        - BUF_BLOCK_FILE_PAGE:  flush_list
    //        - BUF_BLOCK_ZIP_DIRTY:  flush_list
    //        - BUF_BLOCK_ZIP_PAGE:   zip_clean
    //UT_LIST_NODE_T(buf_page_t) list_node;

    // Writes to this field must be covered by both block->mutex and buf_pool->flush_list_mutex.
    // Hence reads can happen while holding any one of the two mutexes.
    uint64 recovery_lsn;
    // protected by block->mutex
    uint64 newest_modification;

    UT_LIST_NODE_T(buf_page_t) LRU_list_node;

    uint16 touch_number;
    // time of first access, or 0 if the block was never accessed in the buffer pool.
    // Protected by block mutex
    date_t access_time;


};

class buf_block_t {
public:
    buf_page_t page; /*!< page information; this must be the first field,
                          so that buf_pool->page_hash can point to buf_page_t or buf_block_t */
    byte* frame;     /*!< pointer to buffer frame which
                          is of size UNIV_PAGE_SIZE, and aligned to an address divisible by UNIV_PAGE_SIZE */

    rw_lock_t        rw_lock; /*!< read-write lock of the buffer frame */

    UT_LIST_NODE_T(buf_block_t) list_node_LRU;
    UT_LIST_NODE_T(buf_block_t) list_node_flush;
    UT_LIST_NODE_T(buf_block_t) list_node;

    uint64 modify_clock; /*!< this clock is incremented every
                            time a pointer to a record on the
                            page may become obsolete; this is
                            used in the optimistic cursor
                            positioning: if the modify clock has
                            not changed, we know that the pointer
                            is still valid; this field may be
                            changed if the thread (1) owns the LRU
                            list mutex and the page is not
                            bufferfixed, or (2) the thread has an
                            x-latch on the block, or (3) the block
                            must belong to an intrinsic table */

    uint32 n_hash_helps;      /*!< counter which controls building
                           of a new hash index for the page */
    volatile uint32 n_bytes;  /*!< recommended prefix length for hash
                           search: number of bytes in
                           an incomplete last field */
    volatile uint32 n_fields; /*!< recommended prefix length for hash
                           search: number of full fields */
    volatile bool32 left_side; /*!< true or false, depending on
                           whether the leftmost record of several
                           records with the same prefix should be
                           indexed in the hash index */
                           /* @} */

    unsigned curr_n_fields : 10; /*!< prefix length for hash indexing: number of full fields */
    unsigned curr_n_bytes : 15;  /*!< number of bytes in hash indexing */
    unsigned curr_left_side : 1; /*!< TRUE or FALSE in hash indexing */

    mutex_t mutex;

    page_no_t get_page_no() const { return (page.id.page_no()); }
    space_id_t get_space_id() const { return (page.id.space_id()); }
    page_no_t get_next_page_no() const { return (mach_read_from_4(frame + FIL_PAGE_NEXT)); }
    page_no_t get_prev_page_no() const { return (mach_read_from_4(frame + FIL_PAGE_PREV)); }
    page_type_t get_page_type() const { return (mach_read_from_2(frame + FIL_PAGE_TYPE) & FIL_PAGE_TYPE_MASK); }
    bool32 is_resident() const { return (mach_read_from_2(frame + FIL_PAGE_TYPE) & FIL_PAGE_TYPE_RESIDENT_FLAG) == FIL_PAGE_TYPE_RESIDENT_FLAG; }
    uint32 get_fix_count() const { return page.buf_fix_count; }
};

// The buffer pool statistics structure
typedef struct st_buf_pool_stat {
    uint32 n_page_gets;            /*!< number of page gets performed;
                                also successful searches through the adaptive hash index are
                                counted as page gets; this field is NOT protected by the buffer pool mutex */
    uint32 n_pages_read;           /*!< number of read operations. Accessed atomically. */
    uint32 n_pages_written;        /*!< number of write operations. Accessed atomically. */
    atomic32_t n_pages_created;        /*!< number of pages created in the pool with no read. Accessed atomically. */
    uint32 n_ra_pages_read_rnd;    /*!< number of pages read in as part of random read ahead. Not protected. */
    uint32 n_ra_pages_read;        /*!< number of pages read in as part of read ahead. Not protected. */
    uint32 n_ra_pages_evicted;     /*!< number of read ahead pages that are evicted without
                             being accessed. Protected by LRU_list_mutex. */
    uint32 n_pages_made_young;     /*!< number of pages made young, in
                            calls to buf_LRU_make_block_young(). Protected by LRU_list_mutex. */
    uint32 n_pages_not_made_young; /*!< number of pages not made young because the first access
                        was not long enough ago, in buf_page_peek_if_too_old(). Not protected. */
    uint32 LRU_bytes;              /*!< LRU size in bytes. Protected by LRU_list_mutex. */
    uint32 flush_list_bytes;       /*!< flush_list size in bytes. Protected by flush_list_mutex */
    mutex_stats_t flush_list_mutex_stat;

} buf_pool_stat_t;

/** Flags for flush types */
typedef enum en_buf_flush {
    BUF_FLUSH_LRU = 0,     /*!< flush via the LRU list */
    BUF_FLUSH_LIST,        /*!< flush via the flush list of dirty blocks */
    BUF_FLUSH_SINGLE_PAGE, /*!< flush via the LRU list but only a single page */
    BUF_FLUSH_N_TYPES      /*!< index of last element + 1  */
} buf_flush_t;

typedef struct st_buf_pool {
    mutex_t    mutex;      /*!< Buffer pool mutex of this instance */

    mutex_t LRU_list_mutex;  /*!< LRU list mutex */
    mutex_t free_list_mutex; /*!< free and withdraw list mutex */
    mutex_t flush_state_mutex; /*!< Flush state protection mutex */


    uint32         size;  // size of frames[] and blocks[]
    unsigned char *mem;   /*!< pointer to the memory area which was allocated for the frames */
    buf_block_t   *blocks;  /*!< array of buffer control blocks */
    uint64         mem_size;

    uint32         instance_no;            /*!< Array index of this buffer pool instance */
    uint64         curr_pool_size;         /*!< Current pool size in bytes */
    uint32         LRU_old_ratio;          /*!< Reserve this much of the buffer pool for "old" blocks */


  page_no_t read_ahead_area;   /*!< size in pages of the area which
                               the read-ahead algorithms read if invoked */
  HASH_TABLE *page_hash;     /*!< hash table of buf_page_t or buf_block_t file pages,
                               buf_page_in_file() == TRUE, indexed by (space_id, offset).
                               page_hash is protected by an array of mutexes. */
  HASH_TABLE *page_hash_old; /*!< old pointer to page_hash to be freed after resizing buffer pool */
  HASH_TABLE *zip_hash;      /*!< hash table of buf_block_t blocks
                               whose frames are allocated to the zip buddy system, indexed by block->frame */
  uint32 n_pend_reads;          /*!< number of pending read operations. Accessed atomically */
  uint32 n_pend_unzip;          /*!< number of pending decompressions. Accessed atomically. */

  uint64 last_printout_time;
  /*!< when buf_print_io was last time called. Accesses not protected. */
  //buf_buddy_stat_t buddy_stat[BUF_BUDDY_SIZES_MAX + 1];
  /*!< Statistics of buddy system,
  indexed by block size. Protected by zip_free mutex, except for the used field, which is also accessed atomically */
  buf_pool_stat_t stat;     /*!< current statistics */
  buf_pool_stat_t old_stat; /*!< old statistics */


  mutex_t flush_list_mutex; /*!< mutex protecting the
                                flush list access. This mutex
                                protects flush_list, flush_rbt
                                and bpage::list pointers when
                                the bpage is on flush_list. It
                                also protects writes to
                                bpage::oldest_modification and
                                flush_list_hp */
  UT_LIST_BASE_NODE_T(buf_block_t) flush_list; /*!< base node of the modified block list */

  bool32 init_flush[BUF_FLUSH_N_TYPES];
  /*!< this is TRUE when a flush of the given type is being initialized.
  Protected by flush_state_mutex. */
  uint32 n_flush[BUF_FLUSH_N_TYPES];
  /*!< this is the number of pending writes in the given flush type.
  Protected by flush_state_mutex. */
  os_event_t no_flush[BUF_FLUSH_N_TYPES];
  /*!< this is in the set state when there is no flush batch
  of the given type running. Protected by flush_state_mutex. */
  ib_rbt_t *flush_rbt;    /*!< a red-black tree is used exclusively during recovery to
                          speed up insertions in the flush_list. This tree contains
                          blocks in order of oldest_modification LSN and is kept in sync with the flush_list.
                          Each member of the tree MUST also be on the flush_list.
                          This tree is relevant only in recovery and is set to NULL once the recovery is over.
                          Protected by flush_list_mutex */
  uint32 freed_page_clock; /*!< a sequence number used
                         to count the number of buffer
                         blocks removed from the end of
                         the LRU list; NOTE that this
                         counter may wrap around at 4
                         billion! A thread is allowed
                         to read this for heuristic
                         purposes without holding any
                         mutex or latch. For non-heuristic
                         purposes protected by LRU_list_mutex */
  bool32 try_LRU_scan;     /*!< Set to FALSE when an LRU
                          scan for free block fails. This
                          flag is used to avoid repeated
                          scans of LRU list when we know
                          that there is no free block
                          available in the scan depth for
                          eviction. Set to TRUE whenever
                          we flush a batch from the
                          buffer pool. Accessed protected by
                          memory barriers. */

  uint64 track_page_lsn; /* Pagge Tracking start LSN. */

  uint64 max_lsn_io; /* Maximum LSN for which write io has already started. */

  UT_LIST_BASE_NODE_T(buf_block_t) free_pages;

  UT_LIST_BASE_NODE_T(buf_page_t) withdraw;
  /*!< base node of the withdraw
  block list. It is only used during
  shrinking buffer pool size, not to
  reuse the blocks will be removed.
  Protected by free_list_mutex */

  uint32 withdraw_target; /*!< target length of withdraw block list, when withdrawing */

    UT_LIST_BASE_NODE_T(buf_page_t) LRU;  /*!< base node of the LRU list */

    // pointer to the about LRU_old_ratio/BUF_LRU_OLD_RATIO_DIV oldest blocks in the LRU list;
    // NULL if LRU length less than BUF_LRU_OLD_MIN_LEN;
    // NOTE: when LRU_old != NULL, its length should always equal LRU_old_len
    buf_page_t* LRU_old;
    // length of the LRU list from the block to which LRU_old points onward, including that block;
    // 0 if LRU_old == NULL;
    // NOTE: LRU_old_len must be adjusted whenever LRU_old shrinks or grows!
    uint32 LRU_old_len;

} buf_pool_t;


extern status_t buf_pool_init(uint64 total_size, uint32 n_instances, uint32 page_hash_lock_count);
extern uint32 buf_pool_get_instances();
extern inline buf_pool_t* buf_pool_get(uint32 id);
extern inline buf_pool_t* buf_pool_from_page_id(const page_id_t &page_id);
extern inline buf_pool_t* buf_pool_from_bpage(const buf_page_t *bpage);
extern inline buf_pool_t* buf_pool_from_block(const buf_block_t *block);
extern lsn_t buf_pool_get_recovery_lsn(void);




// in: buffer pool instance, or NULL for round-robin selection of the buffer pool
extern inline buf_block_t* buf_block_alloc(buf_pool_t* buf_pool);
// Frees a buffer block which does not contain a file page
extern inline void buf_block_free(buf_pool_t* buf_pool, buf_block_t* block);
extern buf_block_t* buf_block_align(const byte* ptr);

extern inline void buf_block_lock_and_fix(buf_block_t* block, rw_lock_type_t lock_type, mtr_t* mtr);
extern inline void buf_block_unlock(buf_block_t* block, rw_lock_type_t lock_type, mtr_t* mtr);

extern inline void buf_block_set_state(buf_block_t *block, buf_page_state_t state);
extern inline buf_page_state_t buf_block_get_state(const buf_block_t *block);

// Initializes a page to the buffer buf_pool
// not read from file even if it cannot be found in the buffer buf_pool
extern inline buf_block_t* buf_page_create(const page_id_t& page_id, const page_size_t& page_size,
    rw_lock_type_t rw_latch, Page_fetch mode, mtr_t* mtr);

// This is the general function used to get access to a database page
extern inline buf_block_t* buf_page_get_gen(const page_id_t& page_id, const page_size_t& page_size,
    rw_lock_type_t rw_latch, buf_block_t* guess, Page_fetch mode, mtr_t* mtr);

#define buf_page_get(ID, SIZE, RW_LOCK, MTR) buf_page_get_gen(ID, SIZE, RW_LOCK, NULL, Page_fetch::NORMAL, MTR)


// Completes an asynchronous read or write request of a file page to or from the buffer pool
extern inline bool32 buf_page_io_complete(buf_page_t* bpage, buf_io_fix_t io_type, bool32 evict);
extern inline bool32 buf_page_can_relocate(buf_page_t* bpage);
extern inline buf_page_t* buf_page_hash_get_low(buf_pool_t* buf_pool, const page_id_t& page_id);

extern inline bool32 buf_page_in_file(const buf_page_t* bpage);

extern inline buf_io_fix_t buf_page_get_io_fix(const buf_page_t* bpage);
extern inline void buf_page_set_io_fix(buf_page_t* bpage, buf_io_fix_t io_fix);
extern inline buf_page_state_t buf_page_get_state(const buf_page_t* bpage);
extern inline void buf_page_set_state(buf_page_t* bpage, buf_page_state_t state);
extern inline uint32 buf_page_fix(buf_page_t* bpage);
extern inline void buf_page_unfix(buf_page_t* bpage);
extern inline date_t buf_page_is_accessed(const buf_page_t* bpage);
extern inline void buf_page_set_accessed(buf_page_t* bpage, date_t access_time);
extern inline mutex_t* buf_page_get_mutex(const buf_page_t* bpage);
extern inline lsn_t buf_page_get_newest_modification(const buf_page_t* bpage);
extern buf_page_t* buf_page_hash_get_locked(
    buf_pool_t* buf_pool,
    const page_id_t& page_id,
    rw_lock_t** lock, /*!< in/out: lock of the page hash acquired if bpage is found. NULL otherwise.
                      If NULL is passed then the hash_lock is released by this function */
    uint32 lock_mode); /*!< in: RW_LOCK_EXCLUSIVE or RW_LOCK_SHARED. Ignored if lock == NULL */

#ifdef __cplusplus
}
#endif


#endif  /* _KNL_BUF_H */
