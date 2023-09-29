#include "knl_buf.h"
#include "cm_thread.h"
#include "cm_log.h"
#include "cm_memory.h"
#include "cm_datetime.h"
#include "knl_hash_table.h"
#include "knl_lock_rec_hash.h"
#include "knl_mtr.h"

/** The buffer pools of the database */
buf_pool_t *buf_pool_ptr;

static void buf_block_init_low(buf_block_t *block);



/** Frees the buffer pool global data structures. */
static void buf_pool_free() {
    //UT_DELETE(buf_stat_per_index);
    //UT_DELETE(buf_chunk_map_reg);
    //buf_chunk_map_reg = nullptr;

    my_free(buf_pool_ptr);
    buf_pool_ptr = nullptr;
}

/** Returns the control block of a file page, NULL if not found.
@param[in]	buf_pool	buffer pool instance
@param[in]	page_id		page id
@return block, NULL if not found */
buf_page_t *buf_page_hash_get_low(buf_pool_t *buf_pool, const page_id_t &page_id)
{
    buf_page_t *bpage;

#ifdef UNIV_DEBUG
    rw_lock_t *hash_lock;

    hash_lock = hash_get_lock(buf_pool->page_hash, page_id.fold());
    ut_ad(rw_lock_own(hash_lock, RW_LOCK_X) || rw_lock_own(hash_lock, RW_LOCK_S));
#endif /* UNIV_DEBUG */

    /* Look for the page in the hash table */

    HASH_SEARCH(hash, buf_pool->page_hash, page_id.fold(), buf_page_t *, bpage,
                ut_ad(bpage->in_page_hash && buf_page_in_file(bpage)),
                page_id.equals_to(bpage->id));
    if (bpage) {
        ut_a(buf_page_in_file(bpage));
        ut_ad(bpage->in_page_hash);
        ut_ad(buf_pool_from_bpage(bpage) == buf_pool);
    }

    return (bpage);
}


bool32 buf_page_in_file(const buf_page_t *bpage) /*!< in: pointer to control block */
{
    switch (buf_page_get_state(bpage)) {
    case BUF_BLOCK_POOL_WATCH:
        ut_error;
        break;
    case BUF_BLOCK_ZIP_PAGE:
    case BUF_BLOCK_ZIP_DIRTY:
    case BUF_BLOCK_FILE_PAGE:
        return (TRUE);
    case BUF_BLOCK_NOT_USED:
    case BUF_BLOCK_READY_FOR_USE:
    case BUF_BLOCK_MEMORY:
    case BUF_BLOCK_REMOVE_HASH:
        break;
    }

    return (FALSE);
}

enum buf_io_fix buf_page_get_io_fix_unlocked(const buf_page_t *bpage)
{
    ut_ad(bpage != NULL);

    enum buf_io_fix io_fix = bpage->io_fix;

#ifdef UNIV_DEBUG
    switch (io_fix) {
    case BUF_IO_NONE:
    case BUF_IO_READ:
    case BUF_IO_WRITE:
    case BUF_IO_PIN:
      return (io_fix);
    }
    ut_error;
#else  /* UNIV_DEBUG */
    return (io_fix);
#endif /* UNIV_DEBUG */
}

/** Allocates a buf_page_t descriptor. This function must succeed.
In case of failure we assert in this function.
 @return: the allocated descriptor. */
buf_page_t *buf_page_alloc_descriptor(void)
{
    buf_page_t *bpage;

    bpage = (buf_page_t *)my_malloc(sizeof *bpage);
    memset(bpage, 0x00, sizeof *bpage);
    ut_ad(bpage);
    //UNIV_MEM_ALLOC(bpage, sizeof *bpage);

    return (bpage);
}

/** Free a buf_page_t descriptor. */
void buf_page_free_descriptor(buf_page_t *bpage) /*!< in: bpage descriptor to free. */
{
    ut_free(bpage);
}

/** Determine if a buffer block can be relocated in memory.
  The block can be dirty, but it must not be I/O-fixed or bufferfixed. */
bool32 buf_page_can_relocate(const buf_page_t *bpage) /*!< control block being relocated */
{
    ut_ad(spin_lock_own(buf_page_get_mutex(bpage)));
    ut_ad(buf_page_in_file(bpage));
    ut_ad(bpage->in_LRU_list);

    return (buf_page_get_io_fix(bpage) == BUF_IO_NONE && bpage->buf_fix_count == 0);
}

enum buf_io_fix buf_page_get_io_fix(const buf_page_t *bpage) /*!< in: pointer to the control block */
{
    ut_ad(spin_lock_own(buf_page_get_mutex(bpage)));
    return buf_page_get_io_fix_unlocked(bpage);
}

/** Gets the mutex of a block.
 @return pointer to mutex protecting bpage */
mutex_t *buf_page_get_mutex(const buf_page_t *bpage) /*!< in: pointer to control block */
{
    buf_pool_t *buf_pool = buf_pool_from_bpage(bpage);

    switch (buf_page_get_state(bpage)) {
    case BUF_BLOCK_POOL_WATCH:
      ut_error;
    case BUF_BLOCK_ZIP_PAGE:
    case BUF_BLOCK_ZIP_DIRTY:
      //return (&buf_pool->zip_mutex);
    default:
      return (&((buf_block_t *)bpage)->mutex);
    }
}

buf_pool_t *buf_pool_from_bpage(const buf_page_t *bpage) /*!< in: buffer pool page */
{
    uint32 i = bpage->buf_pool_index;

    ut_ad(i < srv_buf_pool_instances);

    return (&buf_pool_ptr[i]);
}

/** Determine if a block is a sentinel for a buffer pool watch.
@param[in]	buf_pool	buffer pool instance
@param[in]	bpage		block
@return true if a sentinel for a buffer pool watch, false if not */
bool32 buf_pool_watch_is_sentinel(const buf_pool_t *buf_pool, const buf_page_t *bpage)
{
    /* We must own the appropriate hash lock. */
    ut_ad(buf_page_hash_lock_held_s_or_x(buf_pool, bpage));
    ut_ad(buf_page_in_file(bpage));

    if (bpage < &buf_pool->watch[0] || bpage >= &buf_pool->watch[BUF_POOL_WATCH_SIZE]) {
        //ut_ad(buf_page_get_state(bpage) != BUF_BLOCK_ZIP_PAGE || bpage->zip.data != NULL);
        return (FALSE);
    }

    ut_ad(buf_page_get_state(bpage) == BUF_BLOCK_ZIP_PAGE);
    //ut_ad(!bpage->in_zip_hash);
    ut_ad(bpage->in_page_hash);
    //ut_ad(bpage->zip.data == NULL);
    return (TRUE);
}



/** Increments the bufferfix count.
@param[in,out]	bpage	block to bufferfix
@return the count */
uint32 buf_block_fix(buf_page_t *bpage)
{
    uint32 count;
    count = atomic32_add(&bpage->buf_fix_count, 1);
    ut_ad(count > 0);
    return (count);
}

/** Increments the bufferfix count.
@param[in,out]	block	block to bufferfix
@return the count */
uint32 buf_block_fix(buf_block_t *block)
{
    return (buf_block_fix(&block->page));
}





void buf_page_set_accessed(buf_page_t *bpage) /*!< in/out: control block */
{
  ut_ad(mutex_own(buf_page_get_mutex(bpage)));

  ut_a(buf_page_in_file(bpage));

  if (bpage->access_time == 0) {
    /* Make this the time of the first access. */
    bpage->access_time = static_cast<uint>(get_time_ms());
  }
}

buf_block_t *buf_page_get_gen(const page_id_t &page_id, const page_size_t &page_size,
    uint32 rw_latch, buf_block_t *guess, Page_fetch mode,
    const char *file, uint32 line, mtr_t *mtr, bool dirty_with_no_latch)
{
  //if (mode == Page_fetch::NORMAL && !fsp_is_system_temporary(page_id.space())) {
  //  Buf_fetch_normal fetch(page_id, page_size);

  //  fetch.m_rw_latch = rw_latch;
  //  fetch.m_guess = guess;
  //  fetch.m_mode = mode;
  //  fetch.m_file = file;
  //  fetch.m_line = line;
  //  fetch.m_mtr = mtr;
  //  fetch.m_dirty_with_no_latch = dirty_with_no_latch;

  //  return (fetch.single_page());

  //} else {
  //  Buf_fetch_other fetch(page_id, page_size);

  //  fetch.m_rw_latch = rw_latch;
  //  fetch.m_guess = guess;
  //  fetch.m_mode = mode;
  //  fetch.m_file = file;
  //  fetch.m_line = line;
  //  fetch.m_mtr = mtr;
  //  fetch.m_dirty_with_no_latch = dirty_with_no_latch;

  //  return (fetch.single_page());
    return NULL;
  //}
}

void buf_page_init_low(buf_page_t *bpage) /*!< in: block to init */
{
    bpage->flush_type = BUF_FLUSH_LRU;
    bpage->io_fix = BUF_IO_NONE;
    bpage->buf_fix_count = 0;
    //bpage->freed_page_clock = 0;
    bpage->access_time = 0;
    bpage->newest_modification = 0;
    bpage->oldest_modification = 0;
    HASH_INVALIDATE(bpage, hash);

    ut_d(bpage->file_page_was_freed = FALSE);
}

/** Inits a page to the buffer buf_pool. The block pointer must be private to
the calling thread at the start of this function.
@param[in,out]	buf_pool	buffer pool
@param[in]	page_id		page id
@param[in]	page_size	page size
@param[in,out]	block		block to init */
static void buf_page_init(buf_pool_t *buf_pool, const page_id_t &page_id,
                          const page_size_t &page_size, buf_block_t *block)
{
  buf_page_t *hash_page;

  ut_ad(buf_pool == buf_pool_get(page_id));

  ut_ad(mutex_own(buf_page_get_mutex(&block->page)));
  ut_a(buf_block_get_state(block) != BUF_BLOCK_FILE_PAGE);

  ut_ad(rw_lock_own(buf_page_hash_lock_get(buf_pool, page_id), RW_LOCK_X));

  /* Set the state of the block */
  buf_block_set_state(block, BUF_BLOCK_FILE_PAGE);
  block->page.id.copy_from(page_id);

  buf_block_init_low(block);

  block->lock_hash_val = lock_rec_hash(page_id.space(), page_id.page_no());

  buf_page_init_low(&block->page);

  /* Insert into the hash table of file pages */

  hash_page = buf_page_hash_get_low(buf_pool, page_id);

  if (hash_page == NULL) {
    /* Block not found in hash table */
  } else if (buf_pool_watch_is_sentinel(buf_pool, hash_page)) {
    /* Preserve the reference count. */
    uint32_t buf_fix_count = hash_page->buf_fix_count;

    ut_a(buf_fix_count > 0);

    atomic32_add(&block->page.buf_fix_count, buf_fix_count);

    //buf_pool_watch_remove(buf_pool, hash_page);
  } else {
      LOG_PRINT_ERROR("Page %s already found in the hash table: %s block", page_id, hash_page);

    //ut_d(buf_print());
    //ut_d(buf_LRU_print());
    //ut_d(buf_validate());
   // ut_d(buf_LRU_validate());
    ut_ad(0);
  }

  //ut_ad(!block->page.in_zip_hash);
  //ut_ad(!block->page.in_page_hash);
  ut_d(block->page.in_page_hash = TRUE);

  block->page.id.copy_from(page_id);
  block->page.size.copy_from(page_size);

  HASH_INSERT(buf_page_t, hash, buf_pool->page_hash, page_id.fold(), &block->page);

  //if (page_size.is_compressed()) {
  //  page_zip_set_size(&block->page.zip, page_size.physical());
  //}
}

buf_block_t *buf_page_create(const page_id_t &page_id,
    const page_size_t &page_size, rw_lock_type_t rw_latch, mtr_t *mtr)
{
  buf_frame_t *frame;
  buf_block_t *block;
  buf_block_t *free_block = NULL;
  buf_pool_t *buf_pool = buf_pool_get(page_id);
  rw_lock_t *hash_lock;

  DBUG_ENTER("buf_page_create");

  ut_ad(mtr->is_active());
  ut_ad(page_id.space() != 0 || !page_size.is_compressed());

  //free_block = buf_LRU_get_free_block(buf_pool);

  mutex_enter(&buf_pool->LRU_list_mutex, NULL);

  hash_lock = buf_page_hash_lock_get(buf_pool, page_id);
  rw_lock_x_lock(hash_lock);

  block = (buf_block_t *)buf_page_hash_get_low(buf_pool, page_id);

  if (block && buf_page_in_file(&block->page) && !buf_pool_watch_is_sentinel(buf_pool, &block->page)) {

    ut_d(block->page.file_page_was_freed = FALSE);

    /* Page can be found in buf_pool */
    mutex_exit(&buf_pool->LRU_list_mutex);
    rw_lock_x_unlock(hash_lock);

    buf_block_free(free_block);

    DBUG_RETURN(buf_page_get(page_id, page_size, rw_latch, mtr));
  }

  /* If we get here, the page was not in buf_pool: init it there */

  DBUG_PRINT("create page %lu : %lu", page_id.space(), page_id.page_no());

  block = free_block;
  mutex_enter(&block->mutex, NULL);
  buf_page_init(buf_pool, page_id, page_size, block);
  buf_block_fix(block);
  buf_page_set_accessed(&block->page);
  mutex_exit(&block->mutex);

  /* Latch the page before releasing hash lock so that concurrent request for
  this page doesn't see half initialized page. ALTER tablespace for encryption
  and clone page copy can request page for any page id within tablespace
  size limit. */
  mtr_memo_type_t mtr_latch_type;

  if (rw_latch == RW_X_LATCH) {
    rw_lock_x_lock(&block->rw_lock);
    mtr_latch_type = MTR_MEMO_PAGE_X_FIX;
  } else {
    //rw_lock_sx_lock(&block->rw_lock);
    mtr_latch_type = MTR_MEMO_PAGE_SX_FIX;
  }
  mtr->memo_push(block, mtr_latch_type);

  rw_lock_x_unlock(hash_lock);

  /* The block must be put to the LRU list */
  //buf_LRU_add_block(&block->page, FALSE);

  atomic32_inc(&buf_pool->stat.n_pages_created);

  if (page_size.is_compressed()) {
  }

  mutex_exit(&buf_pool->LRU_list_mutex);

  /* Delete possible entries for the page from the insert buffer:
  such can exist if the page belonged to an index which was dropped */
  //ibuf_merge_or_delete_for_page(NULL, page_id, &page_size, TRUE);

  frame = block->frame;

  memset(frame + FIL_PAGE_PREV, 0xff, 4);
  memset(frame + FIL_PAGE_NEXT, 0xff, 4);
  //mach_write_to_2(frame + FIL_PAGE_TYPE, FIL_PAGE_TYPE_ALLOCATED);

  /* These 8 bytes are also repurposed for PageIO compression and must
  be reset when the frame is assigned to a new page id. See fil0fil.h.

  FIL_PAGE_FILE_FLUSH_LSN is used on the following pages:
  (1) The first page of the InnoDB system tablespace (page 0:0)
  (2) FIL_RTREE_SPLIT_SEQ_NUM on R-tree pages .

  Therefore we don't transparently compress such pages. */

  memset(frame + FIL_PAGE_FILE_FLUSH_LSN, 0, 8);

  DBUG_RETURN(block);
}

/******************************************************************//**
Returns a free block from the buf_pool. The block is taken off the free list.
If free list is empty, blocks are moved from the end of the LRU list to the free list. */
buf_block_t* buf_LRU_get_free_block(buf_pool_t *buf_pool)
{
    buf_block_t *block = NULL;
    bool         freed = false;
    uint32       n_iterations = 0;
    uint32       flush_failures = 0;

    //MONITOR_INC(MONITOR_LRU_GET_FREE_SEARCH);

loop:

    /* If there is a block in the free list, take it */
    //block = buf_LRU_get_free_only(buf_pool);
    if (block != NULL) {
        //ut_ad(buf_pool_from_block(block) == buf_pool);
        //block->skip_flush_check = false;
        //block->page.flush_observer = NULL;
        return(block);
    }

    //MONITOR_INC(MONITOR_LRU_GET_FREE_LOOPS );

    freed = false;
    os_rmb;
    if (buf_pool->try_LRU_scan || n_iterations > 0) {
        /* If no block was in the free list, search from the end of the LRU list and try to free a block there.
           If we are doing for the first time we'll scan only tail of the LRU list otherwise we scan the whole LRU list. */

        //freed = buf_LRU_scan_and_free_block(buf_pool, n_iterations > 0);
        if (!freed && n_iterations == 0) {
            /* Tell other threads that there is no point in scanning the LRU list.
               This flag is set to TRUE again when we flush a batch from this buffer pool. */
            buf_pool->try_LRU_scan = FALSE;
            os_rmb;
        }
    }

    if (freed) {
        goto loop;
    }
    /*
    if (n_iterations > 20 && srv_buf_pool_old_size == srv_buf_pool_size) {
        LOG_PRINT_WARNING("Difficult to find free blocks in the buffer pool(%lu search iterations)! "
            "%lu failed attempts to flush a page! Consider increasing the buffer pool size."
            "It is also possible that in your fsync is very slow."
            "Look at the number of fsyncs in diagnostic info below."
            " Pending flushes (fsync) log: %lu "
            " buffer pool: %lu "
            " OS file reads: %lu "
            " OS file writes: %lu "
            " OS fsyncs: %lu",
            n_iterations, flush_failures,
            fil_n_pending_log_flushes, fil_n_pending_tablespace_flushes,
            os_n_file_reads, os_n_file_writes, os_n_fsyncs);

        os_event_set(lock_sys->timeout_event);
    }
    */
    /* If we have scanned the whole LRU and still are unable to find a free block
       then we should sleep here to let the page_cleaner do an LRU batch for us. */

    //if (!srv_read_only_mode) {
    //    os_event_set(buf_flush_event);
    //}

    if (n_iterations > 1) {
        //MONITOR_INC( MONITOR_LRU_GET_FREE_WAITS );
        os_thread_sleep(10000);
    }

    /* No free block was found: try to flush the LRU list.
       This call will flush one page from the LRU and put it on the free list.
       That means that the free block is up for grabs for all user threads. */

    //if (!buf_flush_single_page_from_LRU(buf_pool)) {
        //MONITOR_INC(MONITOR_LRU_SINGLE_FLUSH_FAILURE_COUNT);
    //    ++flush_failures;
    //}

    srv_stats.buf_pool_wait_free.add(n_iterations, 1);

    n_iterations++;

    goto loop;
}

bool32 buf_page_is_old(const buf_page_t* bpage)
{
    ut_ad(buf_page_in_file(bpage));

    return bpage->old;
}

void buf_page_set_old(buf_page_t* bpage, bool32 old)
{
    ut_a(buf_page_in_file(bpage));
    ut_ad(bpage->in_LRU_list);

    bpage->old = old;
}


enum buf_page_state buf_page_get_state(const buf_page_t *bpage)
{
    enum buf_page_state state = bpage->state;

#ifdef UNIV_DEBUG
    switch (state) {
    case BUF_BLOCK_POOL_WATCH:
    case BUF_BLOCK_ZIP_PAGE:
    case BUF_BLOCK_ZIP_DIRTY:
    case BUF_BLOCK_NOT_USED:
    case BUF_BLOCK_READY_FOR_USE:
    case BUF_BLOCK_FILE_PAGE:
    case BUF_BLOCK_MEMORY:
    case BUF_BLOCK_REMOVE_HASH:
        break;
    default:
        ut_error;
    }
#endif /* UNIV_DEBUG */

    return(state);
}

void buf_page_set_state(buf_page_t *bpage, enum buf_page_state state)
{
#ifdef UNIV_DEBUG
    enum buf_page_state	old_state	= buf_page_get_state(bpage);

    switch (old_state) {
    case BUF_BLOCK_POOL_WATCH:
        ut_error;
        break;
    case BUF_BLOCK_ZIP_PAGE:
        ut_a(state == BUF_BLOCK_ZIP_DIRTY);
        break;
    case BUF_BLOCK_ZIP_DIRTY:
        ut_a(state == BUF_BLOCK_ZIP_PAGE);
        break;
    case BUF_BLOCK_NOT_USED:
        ut_a(state == BUF_BLOCK_READY_FOR_USE);
        break;
    case BUF_BLOCK_READY_FOR_USE:
        ut_a(state == BUF_BLOCK_MEMORY
             || state == BUF_BLOCK_FILE_PAGE
             || state == BUF_BLOCK_NOT_USED);
        break;
    case BUF_BLOCK_MEMORY:
        ut_a(state == BUF_BLOCK_NOT_USED);
        break;
    case BUF_BLOCK_FILE_PAGE:
        ut_a(state == BUF_BLOCK_NOT_USED
             || state == BUF_BLOCK_REMOVE_HASH);
        break;
    case BUF_BLOCK_REMOVE_HASH:
        ut_a(state == BUF_BLOCK_MEMORY);
        break;
    }
#endif /* UNIV_DEBUG */

    bpage->state = state;
    ut_ad(buf_page_get_state(bpage) == state);
}

void buf_block_set_state(buf_block_t *block, enum buf_page_state state)
{
    buf_page_set_state(&block->page, state);
}

/**********************************************************************//**
Gets the space id, page offset, and byte offset within page of a
pointer pointing to a buffer frame containing a file page. */
void buf_ptr_get_fsp_addr(const void *ptr, /*!< in: pointer to a buffer frame */
    uint32 *space,  /*!< out: space id */
    fil_addr_t *addr) /*!< out: page offset and byte offset */
{
    const page_t *page = (const page_t*) ut_align_down((void *)ptr, UNIV_PAGE_SIZE);

    *space = mach_read_from_4(page + FIL_PAGE_SPACE_ID);
    addr->page = mach_read_from_4(page + FIL_PAGE_OFFSET);
    addr->boffset = ut_align_offset(ptr, UNIV_PAGE_SIZE);
}

buf_block_t *buf_block_alloc(buf_pool_t *buf_pool)
{
    buf_block_t *block;
    uint32 index;
    static uint32 buf_pool_index;

    if (buf_pool == NULL) {
        /* We are allocating memory from any buffer pool,
           ensure we spread the grace on all buffer pool instances. */
        index = buf_pool_index++ % srv_buf_pool_instances;
        buf_pool = &buf_pool_ptr[index];
    }

    block = buf_LRU_get_free_block(buf_pool);

    buf_block_set_state(block, BUF_BLOCK_MEMORY);

    return (block);
}

void buf_block_free(buf_block_t *block) /*!< in, own: block to be freed */
{
    ut_a(buf_block_get_state(block) != BUF_BLOCK_FILE_PAGE);

    //buf_LRU_block_free_non_file_page(block);
}

enum buf_page_state buf_block_get_state(const buf_block_t *block)
{
    return (buf_page_get_state(&block->page));
}

static void buf_block_init_low(buf_block_t *block)
{
   //block->index = NULL;
   //block->made_dirty_with_no_latch = false;

   block->n_hash_helps = 0;
   block->n_fields = 1;
   block->n_bytes = 0;
   block->left_side = TRUE;
}

/** Initializes a buffer control block when the buf_pool is created. */
static void buf_block_init(
    buf_pool_t *buf_pool, /*!< in: buffer pool instance */
    buf_block_t *block,   /*!< in: pointer to control block */
    byte *frame)          /*!< in: pointer to buffer frame */
{
    block->frame = frame;

    block->page.buf_pool_index = buf_pool - buf_pool_ptr;
    block->page.state = BUF_BLOCK_NOT_USED;
    block->page.buf_fix_count = 0;
    block->page.io_fix = BUF_IO_NONE;
    //block->page.flush_observer = NULL;

    block->modify_clock = 0;

    //ut_d(block->page.file_page_was_freed = FALSE);

    //block->index = NULL;
    //block->made_dirty_with_no_latch = false;

    ut_d(block->page.in_page_hash = FALSE);
    ut_d(block->page.in_flush_list = FALSE);
    ut_d(block->page.in_free_list = FALSE);
    ut_d(block->page.in_LRU_list = FALSE);
    //ut_d(block->in_unzip_LRU_list = FALSE);
    //ut_d(block->in_withdraw_list = FALSE);

    //spin_lock_init(block->lock);

    rw_lock_create(&block->rw_lock);

    //ut_d(rw_lock_create(buf_block_debug_latch_key, &block->debug_latch, SYNC_NO_ORDER_CHECK));

    block->rw_lock.is_block_lock = 1;

    ut_ad(rw_lock_validate(&(block->rw_lock)));
}

static buf_chunk_t *buf_chunk_init(
    buf_pool_t *buf_pool, /*!< in: buffer pool instance */
    buf_chunk_t *chunk,   /*!< out: chunk of buffers */
    uint64 mem_size)   /*!< in: requested size in bytes */
{
    buf_block_t *block;
    byte *frame;
    uint64 i;

    /* Round down to a multiple of page size, although it already should be. */
    mem_size = ut_2pow_round(mem_size, UNIV_PAGE_SIZE);
    /* Reserve space for the block descriptors. */
    chunk->mem_size = mem_size + ut_2pow_round((mem_size / UNIV_PAGE_SIZE) * (sizeof *block) + (UNIV_PAGE_SIZE - 1), UNIV_PAGE_SIZE);
    chunk->mem = (uchar *)os_mem_alloc_large(&chunk->mem_size);
    if (chunk->mem == NULL) {
        return (NULL);
    }

    /* Dump core without large memory buffers */
    if (buf_pool_should_madvise) {
        madvise_dont_dump((char *)chunk->mem, chunk->mem_size);
    }

    /* Allocate the block descriptors from the start of the memory block. */
    chunk->blocks = (buf_block_t *)chunk->mem;

    /* Align a pointer to the first frame.  Note that when os_large_page_size is smaller than UNIV_PAGE_SIZE,
    we may allocate one fewer block than requested.  When it is bigger, we may allocate more blocks than requested. */
    frame = (byte *)ut_align(chunk->mem, UNIV_PAGE_SIZE);
    chunk->size = chunk->mem_size / UNIV_PAGE_SIZE - (frame != chunk->mem);

    /* Subtract the space needed for block descriptors. */
    {
        uint64 size = chunk->size;
        while (frame < (byte *)(chunk->blocks + size)) {
            frame += UNIV_PAGE_SIZE;
            size--;
        }
        chunk->size = size;
    }

    /* Init block structs and assign frames for them. Then we assign the frames to the first blocks (we already mapped the memory above). */
    block = chunk->blocks;
    for (i = chunk->size; i--;) {
        buf_block_init(buf_pool, block, frame);
        //UNIV_MEM_INVALID(block->frame, UNIV_PAGE_SIZE);

        /* Add the block to the free list */
        UT_LIST_ADD_LAST(list_node, buf_pool->free, &block->page);

        //ut_d(block->page.in_free_list = TRUE);
        //ut_ad(buf_pool_from_block(block) == buf_pool);

        block++;
        frame += UNIV_PAGE_SIZE;
    }

    return chunk;
}

static void buf_pool_create(buf_pool_t *buf_pool, uint64 buf_pool_size, uint32 instance_no, dberr_t *err)
{
    uint32 i;
    uint32 chunk_size;
    buf_chunk_t *chunk;


    ut_ad(buf_pool_size % srv_buf_pool_chunk_unit == 0);

    /* 1. Initialize general fields
    ------------------------------- */
    spin_lock_init(&buf_pool->chunks_mutex);
    spin_lock_init(&buf_pool->LRU_list_mutex);
    spin_lock_init(&buf_pool->free_list_mutex);
    spin_lock_init(&buf_pool->flush_state_mutex);

    if (buf_pool_size > 0) {
        spin_lock(&buf_pool->chunks_mutex, NULL);
        buf_pool->n_chunks = buf_pool_size / srv_buf_pool_chunk_unit;
        chunk_size = srv_buf_pool_chunk_unit;

        buf_pool->chunks = (buf_chunk_t *)my_malloc(buf_pool->n_chunks * sizeof(*chunk));
        buf_pool->chunks_old = NULL;

        UT_LIST_INIT(buf_pool->LRU);
        UT_LIST_INIT(buf_pool->free);
        UT_LIST_INIT(buf_pool->flush_list);
        //UT_LIST_INIT(buf_pool->withdraw, &buf_page_t::list);
        buf_pool->withdraw_target = 0;

        buf_pool->curr_size = 0;
        chunk = buf_pool->chunks;

        do {
            if (!buf_chunk_init(buf_pool, chunk, chunk_size)) {
                while (--chunk >= buf_pool->chunks) {
                    buf_block_t *block = chunk->blocks;

                    for (i = chunk->size; i--; block++) {
                        //os_mutex_free(&block->mutex);
                        //rw_lock_free(&block->lock);
                        //ut_d(rw_lock_free(&block->debug_latch));
                    }
                    //buf_pool->deallocate_chunk(chunk);
                }
                ut_free(buf_pool->chunks);
                buf_pool->chunks = nullptr;

                *err = DB_ERROR;
                spin_unlock(&buf_pool->chunks_mutex);
                return;
            }

            buf_pool->curr_size += chunk->size;
        } while (++chunk < buf_pool->chunks + buf_pool->n_chunks);

        spin_unlock(&buf_pool->chunks_mutex);

        buf_pool->instance_no = instance_no;
        //buf_pool->read_ahead_area = ut_min(BUF_READ_AHEAD_PAGES, ut_2_power_up(buf_pool->curr_size / BUF_READ_AHEAD_PORTION));
        buf_pool->curr_pool_size = buf_pool->curr_size * UNIV_PAGE_SIZE;

        buf_pool->old_size = buf_pool->curr_size;
        buf_pool->n_chunks_new = buf_pool->n_chunks;

        /* Number of locks protecting page_hash must be a power of two */
        //srv_n_page_hash_locks = ut_2_power_up(srv_n_page_hash_locks);
        //ut_a(srv_n_page_hash_locks != 0);
        //ut_a(srv_n_page_hash_locks <= MAX_PAGE_HASH_LOCKS);

        buf_pool->page_hash = HASH_TABLE_CREATE(2 * buf_pool->curr_size);
        //buf_pool->page_hash->heap = ;
        //buf_pool->page_hash->heaps = ;
        buf_pool->page_hash_old = NULL;

        buf_pool->last_printout_time = current_monotonic_time();
    }

    /* 2. Initialize flushing fields
    -------------------------------- */

    spin_lock_init(&buf_pool->flush_list_mutex);
    //for (i = BUF_FLUSH_LRU; i < BUF_FLUSH_N_TYPES; i++) {
    //  buf_pool->no_flush[i] = os_event_create(0);
    //}

    buf_pool->watch = (buf_page_t *)my_malloc(sizeof(*buf_pool->watch) * BUF_POOL_WATCH_SIZE);
    for (i = 0; i < BUF_POOL_WATCH_SIZE; i++) {
        buf_pool->watch[i].buf_pool_index = buf_pool->instance_no;
    }

    /* All fields are initialized by ut_zalloc_nokey(). */

    buf_pool->try_LRU_scan = TRUE;

    /* Dirty Page Tracking is disabled by default. */
    //buf_pool->track_page_lsn = LSN_MAX;

    buf_pool->max_lsn_io = 0;

    *err = DB_SUCCESS;
}

static void buf_pool_free_instance(buf_pool_t *buf_pool)
{
    buf_chunk_t *chunk;
    buf_chunk_t *chunks;
    buf_page_t *bpage;
    buf_page_t *prev_bpage = 0;

    mutex_destroy(&buf_pool->LRU_list_mutex);
    mutex_destroy(&buf_pool->free_list_mutex);
    mutex_destroy(&buf_pool->flush_state_mutex);
    mutex_destroy(&buf_pool->flush_list_mutex);

    for (bpage = UT_LIST_GET_LAST(buf_pool->LRU); bpage != NULL; bpage = prev_bpage) {
        prev_bpage = UT_LIST_GET_PREV(LRU, bpage);
        //buf_page_state state = buf_page_get_state(bpage);

        //ut_ad(buf_page_in_file(bpage));
        //ut_ad(bpage->in_LRU_list);

        //if (state != BUF_BLOCK_FILE_PAGE) {
        //  /* We must not have any dirty block except
        //  when doing a fast shutdown. */
        //  ut_ad(state == BUF_BLOCK_ZIP_PAGE || srv_fast_shutdown == 2);
        //  buf_page_free_descriptor(bpage);
        //}
    }

    ut_free(buf_pool->watch);
    buf_pool->watch = NULL;
    mutex_enter(&buf_pool->chunks_mutex, NULL);
    chunks = buf_pool->chunks;
    chunk = chunks + buf_pool->n_chunks;

    while (--chunk >= chunks) {
        buf_block_t *block = chunk->blocks;

        for (uint64 i = chunk->size; i--; block++) {
            //os_mutex_free(&block->mutex);
            //rw_lock_free(&block->lock);
            //ut_d(rw_lock_free(&block->debug_latch));
        }

        if (buf_pool_should_madvise) {
            madvise_dump((char *)chunk->mem, chunk->mem_size);
        }
        os_mem_free_large(chunk->mem, chunk->mem_size);
    }

    //for (ulint i = BUF_FLUSH_LRU; i < BUF_FLUSH_N_TYPES; ++i) {
    //  os_event_destroy(buf_pool->no_flush[i]);
    //}

    my_free(buf_pool->chunks);
    mutex_exit(&buf_pool->chunks_mutex);
    mutex_destroy(&buf_pool->chunks_mutex);
    //ha_clear(buf_pool->page_hash);
    HASH_TABLE_FREE(buf_pool->page_hash);
    HASH_TABLE_FREE(buf_pool->zip_hash);
}

dberr_t buf_pool_init(uint64 total_size, uint32 n_instances)
{
    uint32 i;
    const uint64 size = total_size / n_instances;

    ut_ad(n_instances > 0);
    ut_ad(n_instances <= MAX_BUFFER_POOLS);
    ut_ad(n_instances == srv_buf_pool_instances);

    srv_buf_pool_chunk_unit = total_size / srv_buf_pool_instances;
    if (total_size % srv_buf_pool_instances != 0) {
        ++srv_buf_pool_chunk_unit;
    }

    buf_pool_ptr = (buf_pool_t *)my_malloc(n_instances * sizeof *buf_pool_ptr);

    /* Magic nuber 8 is from empirical testing on a 4 socket x 10 Cores x 2 HT host.
       128G / 16 instances takes about 4 secs, compared to 10 secs without this optimisation.. */
    uint32 n_cores = 8;
    dberr_t errs[8], err = DB_SUCCESS;
    os_thread_t threads[8];

    for (uint32 i = 0; i < n_instances; /* no op */) {
        uint32 n = i + n_cores;

        if (n > n_instances) {
            n = n_instances;
        }

        for (uint32 id = i; id < n; ++id) {
            threads[id - i] = thread_start(buf_pool_create, &buf_pool_ptr[id], size, id, &errs[id - i]);
        }
        for (uint32 id = i; id < n; ++id) {
            if (!os_thread_join(threads[id - i]) || errs[id] != DB_SUCCESS) {
                err = DB_ERROR;
            }
        }

        if (err != DB_SUCCESS) {
            for (uint32 id = 0; id < n; ++id) {
                if (buf_pool_ptr[id].chunks != nullptr) {
                    buf_pool_free_instance(&buf_pool_ptr[id]);
                }
            }
            buf_pool_free();
            return (err);
        }

        /* Do the next block of instances */
        i = n;
    }

    //buf_LRU_old_ratio_update(100 * 3 / 8, FALSE);

    return DB_SUCCESS;
}

buf_pool_t *buf_pool_get(const page_id_t &page_id)
{
    /* 2log of BUF_READ_AHEAD_AREA (64) */
    page_no_t ignored_page_no = page_id.page_no() >> 6;

    page_id_t id(page_id.space(), ignored_page_no);

    uint32 i = id.fold() % srv_buf_pool_instances;

    return (&buf_pool_ptr[i]);
}

