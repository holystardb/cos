#ifndef _KNL_BUF_H
#define _KNL_BUF_H

#include "cm_type.h"
#include "cm_error.h"
#include "cm_list.h"
#include "cm_dbug.h"
#include "cm_rwlock.h"
#include "cm_rbt.h"
#include "cm_mutex.h"
#include "knl_mtr.h"
#include "knl_fsp.h"
#include "knl_hash_table.h"
#include "knl_page_size.h"
#include "knl_server.h"

#ifdef __cplusplus
extern "C" {
#endif



/** The maximum number of buffer pools that can be defined */
constexpr uint32 MAX_BUFFER_POOLS = 64;

/** Maximum number of concurrent buffer pool watches */
#define BUF_POOL_WATCH_SIZE (srv_n_purge_threads + 1)

/*!< The maximum number of page_hash locks */
#define MAX_PAGE_HASH_LOCKS 1024




/** Get appropriate page_hash_lock. */
#define buf_page_hash_lock_get(buf_pool, page_id) \
  hash_get_lock((buf_pool)->page_hash, (page_id).fold())

/** If not appropriate page_hash_lock, relock until appropriate. */
#define buf_page_hash_lock_s_confirm(hash_lock, buf_pool, page_id) \
  hash_lock_s_confirm(hash_lock, (buf_pool)->page_hash, (page_id).fold())

#define buf_page_hash_lock_x_confirm(hash_lock, buf_pool, page_id) \
  hash_lock_x_confirm(hash_lock, (buf_pool)->page_hash, (page_id).fold())

#define buf_block_get_frame(block) (block)->frame



enum class Page_fetch {
    /** Get always */
    NORMAL,

    /** Same as NORMAL, but hint that the fetch is part of a large scan.
    Try not to flood the buffer pool with pages that may not be accessed again any time soon. */
    SCAN,

    /** get if in pool */
    IF_IN_POOL,

    /** get if in pool, do not make the block young in the LRU list */
    PEEK_IF_IN_POOL,

    /** get and bufferfix, but set no latch; we have separated this case, because
    it is error-prone programming not to set a latch, and it  should be used with care */
    NO_LATCH,

    /** Get the page only if it's in the buffer pool, if not then set a watch on the page. */
    IF_IN_POOL_OR_WATCH,

    /** Like Page_fetch::NORMAL, but do not mind if the file page has been freed. */
    POSSIBLY_FREED
};

/** Page identifier. */
class page_id_t {
 public:
  page_id_t() : m_space(), m_page_no(), m_fold() {}

  page_id_t(space_id_t space, page_no_t page_no)
      : m_space(space), m_page_no(page_no), m_fold(UINT32_UNDEFINED) {}

  page_id_t(const page_id_t &) = default;

  /** Retrieve the tablespace id.
  @return tablespace id */
  inline space_id_t space() const { return (m_space); }

  /** Retrieve the page number.
  @return page number */
  inline page_no_t page_no() const { return (m_page_no); }

  /** Retrieve the fold value.
  @return fold value */
  inline uint32 fold() const {
    /* Initialize m_fold if it has not been initialized yet. */
    if (m_fold == UINT32_UNDEFINED) {
      m_fold = (m_space << 20) + m_space + m_page_no;
      ut_ad(m_fold != UINT32_UNDEFINED);
    }

    return (m_fold);
  }

  /** Copy the values from a given page_id_t object.
  @param[in]	src	page id object whose values to fetch */
  inline void copy_from(const page_id_t &src) {
    m_space = src.space();
    m_page_no = src.page_no();
    m_fold = src.fold();
  }

  /** Reset the values from a (space, page_no).
  @param[in]	space	tablespace id
  @param[in]	page_no	page number */
  inline void reset(space_id_t space, page_no_t page_no) {
    m_space = space;
    m_page_no = page_no;
    m_fold = UINT32_UNDEFINED;
  }

  /** Reset the page number only.
  @param[in]	page_no	page number */
  inline void set_page_no(page_no_t page_no) {
    m_page_no = page_no;
    m_fold = UINT32_UNDEFINED;
  }

  /** Check if a given page_id_t object is equal to the current one.
  @param[in]	a	page_id_t object to compare
  @return true if equal */
  inline bool equals_to(const page_id_t &a) const {
    return (a.space() == m_space && a.page_no() == m_page_no);
  }

 private:
  /** Tablespace id. */
  space_id_t m_space;

  /** Page number. */
  page_no_t m_page_no;

  /** A fold value derived from m_space and m_page_no,
  used in hashing. */
  mutable uint32 m_fold;

  /* Disable implicit copying. */
  void operator=(const page_id_t &) = delete;

  /** Declare the overloaded global operator<< as a friend of this
  class. Refer to the global declaration for further details.  Print
  the given page_id_t object.
  @param[in,out]	out	the output stream
  @param[in]	page_id	the page_id_t object to be printed
  @return the output stream */
  //friend std::ostream &operator<<(std::ostream &out, const page_id_t &page_id);
};

enum buf_page_state {
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
  BUF_BLOCK_MEMORY,        /*!< contains some main memory
                           object */
  BUF_BLOCK_REMOVE_HASH    /*!< hash index should be removed
                           before putting to the free list */
};

/** Flags for io_fix types */
enum buf_io_fix {
  BUF_IO_NONE = 0, /**< no pending I/O */
  BUF_IO_READ,     /**< read pending */
  BUF_IO_WRITE,    /**< write pending */
  BUF_IO_PIN       /**< disallow relocation of
                   block and its removal of from
                   the flush_list */
};



class buf_page_t {
public:
    /** Page id. */
    page_id_t id;

    /** Page size. */
    page_size_t size;

    /** Count of how manyfold this block is currently bufferfixed. */
    atomic32_t buf_fix_count;

    /** type of pending I/O operation. */
    buf_io_fix io_fix;

    /** Block state. */
    buf_page_state state;

    bool32 in_page_hash;
    bool32 in_flush_list;
    bool32 in_free_list;
    bool32 in_LRU_list;
    bool32 in_unzip_LRU_list;
    bool32 in_withdraw_list;

    spinlock_t lock;


    /** if this block is currently being flushed to disk, this tells
    the flush_type.  @see buf_flush_t */
    unsigned flush_type : 2;

    /** index number of the buffer pool that this block belongs to */
    unsigned buf_pool_index : 6;

    buf_page_t *hash; /*!< node used in chaining to buf_pool->page_hash or buf_pool->zip_hash */

    spinlock_t spin_lock;      /*!< protecting this block:
                         state (also protected by the buffer pool mutex), io_fix, buf_fix_count, and accessed; */
    rw_lock_t rw_lock; /*!< read-write lock of the buffer frame */


    UT_LIST_NODE_T(buf_page_t) list_node;

    uint64 newest_modification;
    uint64 oldest_modification;

    UT_LIST_NODE_T(buf_page_t) LRU;
    unsigned old:1; /*!< TRUE if the block is in the old blocks in buf_pool->LRU_old */
    unsigned freed_page_clock:31;/*!< the value of
					buf_pool->freed_page_clock
					when this block was the last
					time put to the head of the
					LRU list; a thread is allowed
					to read this for heuristic
					purposes without holding any
					mutex or latch */

    unsigned access_time; /*!< time of first access, or 0 if the block was never accessed in the buffer pool.
                               Protected by block mutex */

    bool32 file_page_was_freed;  /*!< this is set to TRUE when fsp frees a page in buffer pool;
                                      protected by buf_pool->zip_mutex or buf_block_t::mutex. */

};

struct buf_block_t {
  buf_page_t page; /*!< page information; this must be the first field,
                     so that buf_pool->page_hash can point to buf_page_t or buf_block_t */
  byte *frame;     /*!< pointer to buffer frame which
                   is of size UNIV_PAGE_SIZE, and aligned to an address divisible by UNIV_PAGE_SIZE */

  rw_lock_t        rw_lock; /*!< read-write lock of the buffer frame */

  UT_LIST_NODE_T(buf_block_t) unzip_LRU;  /* node of the decompressed LRU list */

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

  unsigned curr_n_fields : 10; /*!< prefix length for hash indexing:
                              number of full fields */
  unsigned curr_n_bytes : 15;  /*!< number of bytes in hash
                               indexing */
  unsigned curr_left_side : 1; /*!< TRUE or FALSE in hash indexing */

  mutex_t mutex;

  /** Get the page number of the current buffer block.
  @return page number of the current buffer block. */
  page_no_t get_page_no() const { return (page.id.page_no()); }

  /** Get the next page number of the current buffer block.
  @return next page number of the current buffer block. */
  //page_no_t get_next_page_no() const {
  //  return (mach_read_from_4(frame + FIL_PAGE_NEXT));
  //}

  /** Get the page type of the current buffer block.
  @return page type of the current buffer block. */
  //page_type_t get_page_type() const {
  //  return (mach_read_from_2(frame + FIL_PAGE_TYPE));
  //}

  /** Get the page type of the current buffer block as string.
  @return page type of the current buffer block as string. */
  //const char *get_page_type_str() const;
};



/** A chunk of buffers. The buffer pool is allocated in chunks. */
typedef struct st_buf_chunk {
  uint64 size;           /*!< size of frames[] and blocks[] */
  unsigned char *mem;   /*!< pointer to the memory area which was allocated for the frames */

  buf_block_t *blocks;  /*!< array of buffer control blocks */
  uint64 mem_size;


} buf_chunk_t;

/** @brief The buffer pool statistics structure. */
struct buf_pool_stat_t {
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
};

/** Flags for flush types */
enum buf_flush_t {
    BUF_FLUSH_LRU = 0,     /*!< flush via the LRU list */
    BUF_FLUSH_LIST,        /*!< flush via the flush list of dirty blocks */
    BUF_FLUSH_SINGLE_PAGE, /*!< flush via the LRU list but only a single page */
    BUF_FLUSH_N_TYPES      /*!< index of last element + 1  */
};

struct buf_pool_t {
  spinlock_t    lock;      /*!< Buffer pool mutex of this instance */

  mutex_t chunks_mutex;    /*!< protects (de)allocation of chunks:
                                - changes to chunks, n_chunks are performed
                                  while holding this latch,
                                - reading buf_pool_should_madvise requires
                                  holding this latch for any buf_pool_t
                                - writing to buf_pool_should_madvise requires
                                  holding these latches for all buf_pool_t-s
                                */
  mutex_t LRU_list_mutex;  /*!< LRU list mutex */
  mutex_t free_list_mutex; /*!< free and withdraw list mutex */
  mutex_t flush_state_mutex; /*!< Flush state protection mutex */

  uint32 instance_no;            /*!< Array index of this buffer pool instance */
  uint32 curr_pool_size;         /*!< Current pool size in bytes */
  uint32 LRU_old_ratio;          /*!< Reserve this much of the buffer pool for "old" blocks */

  volatile uint32 n_chunks;     /*!< number of buffer pool chunks */
  volatile uint32 n_chunks_new; /*!< new number of buffer pool chunks */
  buf_chunk_t *chunks;         /*!< buffer pool chunks */
  buf_chunk_t *chunks_old;     /*!< old buffer pool chunks to be freed after resizing buffer pool */
  uint32 curr_size;             /*!< current pool size in pages */
  uint32 old_size;              /*!< previous pool size in pages */
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
  UT_LIST_BASE_NODE_T(buf_page_t) flush_list; /*!< base node of the modified block list */
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

  UT_LIST_BASE_NODE_T(buf_page_t) free; /*!< base node of the free block list */

  UT_LIST_BASE_NODE_T(buf_page_t) withdraw;
  /*!< base node of the withdraw
  block list. It is only used during
  shrinking buffer pool size, not to
  reuse the blocks will be removed.
  Protected by free_list_mutex */

  uint32 withdraw_target; /*!< target length of withdraw block list, when withdrawing */

  UT_LIST_BASE_NODE_T(buf_page_t) LRU;  /*!< base node of the LRU list */

  buf_page_t *LRU_old; /*!< pointer to the about
                       LRU_old_ratio/BUF_LRU_OLD_RATIO_DIV
                       oldest blocks in the LRU list;
                       NULL if LRU length less than
                       BUF_LRU_OLD_MIN_LEN;
                       NOTE: when LRU_old != NULL, its length
                       should always equal LRU_old_len */
  uint32 LRU_old_len;   /*!< length of the LRU list from
                       the block to which LRU_old points
                       onward, including that block;
                       see buf0lru.cc for the restrictions
                       on this value; 0 if LRU_old == NULL;
                       NOTE: LRU_old_len must be adjusted
                       whenever LRU_old shrinks or grows! */

  buf_page_t *watch;
  /*!< Sentinel records for buffer
  pool watches. Scanning the array is
  protected by taking all page_hash
  latches in X. Updating or reading an
  individual watch page is protected by
  a corresponding individual page_hash
  latch. */

  /** A wrapper for buf_pool_t::allocator.alocate_large which also advices the
  OS that this chunk should not be dumped to a core file if that was requested.
  Emits a warning to the log and disables @@global.core_file if advising was
  requested but could not be performed, but still return true as the allocation
  itself succeeded.
  @param[in]	  mem_size  number of bytes to allocate
  @param[in,out]  chunk     mem and mem_pfx fields of this chunk will be updated
                            to contain information about allocated memory region
  @return true iff allocated successfully */
  bool allocate_chunk(ulonglong mem_size, buf_chunk_t *chunk);

  /** A wrapper for buf_pool_t::allocator.deallocate_large which also advices
  the OS that this chunk can be dumped to a core file.
  Emits a warning to the log and disables @@global.core_file if advising was
  requested but could not be performed.
  @param[in]  chunk   mem and mem_pfx fields of this chunk will be used to
                      locate the memory region to free */
  void deallocate_chunk(buf_chunk_t *chunk);

};


dberr_t buf_pool_init(uint64 total_size, uint32 n_instances);
buf_pool_t* buf_pool_get(const page_id_t &page_id);
buf_pool_t* buf_pool_from_bpage(const buf_page_t *bpage);
buf_pool_t* buf_pool_from_block(const buf_block_t *block);




// in: buffer pool instance, or NULL for round-robin selection of the buffer pool
buf_block_t* buf_block_alloc(buf_pool_t *buf_pool);

// Frees a buffer block which does not contain a file page
void buf_block_free(buf_block_t *    block);

buf_block_t* buf_block_align(const byte* ptr);


/********************************************************************//**
Initializes a page to the buffer buf_pool.
The page is usually not read from a file even if it cannot be found in the buffer buf_pool.
This is one of the functions which perform to a block a state transition NOT_USED => FILE_PAGE (the other is buf_page_get_gen).
@return pointer to the block, page bufferfixed */
buf_block_t* buf_page_create(
    const page_id_t &page_id,
    const page_size_t &page_size,
    rw_lock_type_t rw_latch,
    mtr_t *mtr);


/** This is the general function used to get access to a database page.
    @param[in] page_id      page id
    @param[in] page_size    page size
    @param[in] rw_latch     RW_S_LATCH, RW_X_LATCH, RW_NO_LATCH
    @param[in] guess        guessed block or NULL
    @param[in] mode         Fetch mode.
    @param[in] file         file name
    @param[in] line         line where called
    @param[in] mtr          mini-transaction
    @param[in] dirty_with_no_latch mark page as dirty even if page is being pinned without any latch
    @return pointer to the block or NULL */
buf_block_t *buf_page_get_gen(const page_id_t &page_id, const page_size_t &page_size,
    uint32 rw_latch, buf_block_t *guess, Page_fetch mode,
    const char *file, uint32 line, mtr_t *mtr, bool32 dirty_with_no_latch = FALSE);


/** NOTE! The following macros should be used instead of buf_page_get_gen,
 to improve debugging. Only values RW_S_LATCH and RW_X_LATCH are allowed in LA! */
#define buf_page_get(ID, SIZE, LA, MTR)                                        \
    buf_page_get_gen(ID, SIZE, LA, NULL, Page_fetch::NORMAL, __FILE__, __LINE__, MTR)

/** Use these macros to bufferfix a page with no latching. Remember not to
 read the contents of the page unless you know it is safe. Do not modify
 the contents of the page! We have separated this case, because it is
 error-prone programming not to set a latch, and it should be used with care. */
#define buf_page_get_with_no_latch(ID, SIZE, MTR)                     \
    buf_page_get_gen(ID, SIZE, RW_NO_LATCH, NULL, Page_fetch::NO_LATCH, __FILE__, __LINE__, MTR)


/********************************************************************//**
Completes an asynchronous read or write request of a file page to or from the buffer pool.
@return true if successful */
bool32 buf_page_io_complete(buf_page_t* bpage);  /*!< in: pointer to the block in question */
bool32 buf_page_can_relocate(const buf_page_t *bpage);
buf_page_t *buf_page_alloc_descriptor(void);
void buf_page_free_descriptor(buf_page_t *bpage);
buf_page_t *buf_page_hash_get_low(buf_pool_t *buf_pool, const page_id_t &page_id);

bool32 buf_page_in_file(const buf_page_t *bpage);
bool32 buf_page_is_old(const buf_page_t* bpage);
void buf_page_set_old(buf_page_t* bpage, bool32 old);

enum buf_io_fix buf_page_get_io_fix(const buf_page_t *bpage);
enum buf_page_state buf_page_get_state(const buf_page_t *bpage);


/********************************************************************//**
Inits a page to the buffer buf_pool. */
static void buf_page_init(
    buf_pool_t  *buf_pool,/*!< in/out: buffer pool */
    uint32       space,  /*!< in: space id */
    uint32       offset, /*!< in: offset of the page within space in units of a page */
    uint32       fold,   /*!< in: buf_page_address_fold(space,offset) */
    uint32       zip_size,/*!< in: compressed page size, or 0 */
    buf_block_t *block);  /*!< in/out: block to init */

/*******************************************************************//**
Given a tablespace id and page number tries to get that page.
If the page is not in the buffer pool it is not loaded and NULL is returned.
Suitable for using when holding the lock_sys_t::mutex.
@return pointer to a page or NULL */
const buf_block_t* buf_page_try_get_func(
    uint32      space_id,/*!< in: tablespace id */
    uint32      page_no,/*!< in: page number */
    const char* file,   /*!< in: file name */
    uint32      line,   /*!< in: line where called */
    mtr_t*      mtr);    /*!< in: mini-transaction */

/********************************************************************//**
Moves a page to the start of the buffer pool LRU list.
This high-level function can be used to prevent an important page from slipping out of the buffer pool. */
void buf_page_make_young(buf_page_t* bpage);  /*!< in: buffer block of a file page */

/********************************************************************//**
Moves a page to the start of the buffer pool LRU list if it is too old.
This high-level function can be used to prevent an important page from slipping out of the buffer pool. */
void buf_page_make_young_if_needed(buf_page_t* bpage);  /*!< in/out: buffer block of a file page */

/********************************************************************//**
Returns TRUE if the page can be found in the buffer pool hash table.
NOTE that it is possible that the page is not yet read from disk, though.
@return TRUE if found in the page hash table */
bool32 buf_page_peek(
    uint32  space,   /*!< in: space id */
    uint32  offset); /*!< in: page number */

/********************************************************************//**
Decrements the bufferfix count of a buffer control block and releases a latch, if specified. */
void buf_page_release(
    buf_block_t *block,      /*!< in: buffer block */
    uint32       rw_latch);   /*!< in: RW_S_LATCH, RW_X_LATCH, RW_NO_LATCH */


void buf_ptr_get_fsp_addr(const void *ptr, uint32 *space,  fil_addr_t *addr);

void buf_block_set_state(buf_block_t *block, enum buf_page_state state);
enum buf_page_state buf_block_get_state(const buf_block_t *block);
spinlock_t *buf_page_get_mutex(const buf_page_t *bpage);


/* There are four different ways we can try to get a bpage or block
from the page hash:
    1) Caller already holds the appropriate page hash lock: in the case call buf_page_hash_get_low() function.
    2) Caller wants to hold page hash lock in x-mode
    3) Caller wants to hold page hash lock in s-mode
    4) Caller doesn't want to hold page hash lock */
#define buf_page_hash_get_s_locked(b, s, o, l) buf_page_hash_get_locked(b, s, o, l, RW_LOCK_SHARED)
#define buf_page_hash_get_x_locked(b, s, o, l) buf_page_hash_get_locked(b, s, o, l, RW_LOCK_EX)
#define buf_page_hash_get(b, s, o) buf_page_hash_get_locked(b, s, o, NULL, 0)

#define buf_block_hash_get_s_locked(b, s, o, l) buf_block_hash_get_locked(b, s, o, l, RW_LOCK_SHARED)
#define buf_block_hash_get_x_locked(b, s, o, l) buf_block_hash_get_locked(b, s, o, l, RW_LOCK_EX)
#define buf_block_hash_get(b, s, o) buf_block_hash_get_locked(b, s, o, NULL, 0)

    /** Test if page_hash lock is held in s-mode. */
#define buf_page_hash_lock_held_s(buf_pool, bpage) \
      rw_lock_own(buf_page_hash_lock_get((buf_pool), (bpage)->id), RW_LOCK_SHARED)
    
    /** Test if page_hash lock is held in x-mode. */
#define buf_page_hash_lock_held_x(buf_pool, bpage) \
      rw_lock_own(buf_page_hash_lock_get((buf_pool), (bpage)->id), RW_LOCK_EXCLUSIVE)

    /** Test if page_hash lock is held in x or s-mode. */
#define buf_page_hash_lock_held_s_or_x(buf_pool, bpage) \
      (buf_page_hash_lock_held_s((buf_pool), (bpage)) ||    \
       buf_page_hash_lock_held_x((buf_pool), (bpage)))

#define buf_block_hash_lock_held_s(buf_pool, block) \
      buf_page_hash_lock_held_s((buf_pool), &(block)->page)

#define buf_block_hash_lock_held_x(buf_pool, block) \
      buf_page_hash_lock_held_x((buf_pool), &(block)->page)

#define buf_block_hash_lock_held_s_or_x(buf_pool, block) \
      buf_page_hash_lock_held_s_or_x((buf_pool), &(block)->page)



#ifdef __cplusplus
}
#endif


#endif  /* _KNL_BUF_H */
