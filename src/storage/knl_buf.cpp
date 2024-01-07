#include "knl_buf.h"
#include "cm_thread.h"
#include "cm_log.h"
#include "cm_memory.h"
#include "cm_datetime.h"
#include "knl_hash_table.h"
#include "knl_mtr.h"
#include "knl_buf_lru.h"
#include "knl_page.h"

/** The buffer pools of the database */
buf_pool_t *buf_pool_ptr;

static void buf_block_init_low(buf_block_t *block);





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
    //ut_ad(rw_lock_own(hash_lock, RW_LOCK_X) || rw_lock_own(hash_lock, RW_LOCK_S));
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

    bpage = (buf_page_t *)my_malloc(NULL, sizeof *bpage);
    memset(bpage, 0x00, sizeof *bpage);
    ut_ad(bpage);
    //UNIV_MEM_ALLOC(bpage, sizeof *bpage);

    return (bpage);
}

/** Free a buf_page_t descriptor. */
void buf_page_free_descriptor(buf_page_t *bpage) /*!< in: bpage descriptor to free. */
{
    free(bpage);
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
mutex_t* buf_page_get_mutex(const buf_page_t *bpage) /*!< in: pointer to control block */
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

buf_pool_t* buf_pool_from_bpage(const buf_page_t *bpage) /*!< in: buffer pool page */
{
    uint32 i = bpage->buf_pool_index;

    ut_ad(i < srv_buf_pool_instances);

    return (&buf_pool_ptr[i]);
}

buf_pool_t* buf_pool_from_block(const buf_block_t *block) /*!< in: block */
{
    return(buf_pool_from_bpage(&block->page));
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

    //if (bpage < &buf_pool->watch[0] || bpage >= &buf_pool->watch[BUF_POOL_WATCH_SIZE]) {
        //ut_ad(buf_page_get_state(bpage) != BUF_BLOCK_ZIP_PAGE || bpage->zip.data != NULL);
    //    return (FALSE);
    //}

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

// Calculates a folded value of a file page address to use in the page hash table
uint32 buf_page_address_fold(const page_id_t *page_id)
{
    return (page_id->space() << 20) + page_id->space() + page_id->page_no();
}


/********************************************************************//**
Function which inits a page for read to the buffer buf_pool. If the page is
(1) already in buf_pool, or
(2) if we specify to read only ibuf pages and the page is not an ibuf page, or
(3) if the space is deleted or being deleted,
then this function does nothing.
Sets the io_fix flag to BUF_IO_READ and sets a non-recursive exclusive lock
on the buffer frame. The io-handler must take care that the flag is cleared
and the lock released later.
@return	pointer to the block or NULL */
buf_page_t* buf_page_init_for_read(
    dberr_t    *err,    /*!< out: DB_SUCCESS or DB_TABLESPACE_DELETED */
    uint32      mode,   /*!< in: BUF_READ_IBUF_PAGES_ONLY, ... */
    page_id_t  *page_id)/*!< in: page number */
{
	buf_block_t  *block;
	buf_page_t   *bpage	= NULL;
	buf_page_t   *watch_page;
	rw_lock_t    *hash_lock;
	mtr_t         mtr;
	uint32        fold;
	bool32        lru = FALSE;
	void         *data;
	buf_pool_t   *buf_pool = buf_pool_get(page_id);

	ut_ad(buf_pool);

	*err = DB_SUCCESS;

	//ut_ad(mode == BUF_READ_ANY_PAGE);

	block = buf_LRU_get_free_block(buf_pool);
	ut_ad(block);
	ut_ad(buf_pool_from_block(block) == buf_pool);

	fold = buf_page_address_fold(page_id);
	hash_lock = buf_page_hash_lock_get(buf_pool, fold);

	mutex_enter(&buf_pool->mutex, NULL);

	rw_lock_x_lock(hash_lock);

	watch_page = buf_page_hash_get_low(buf_pool, page_id, fold);
	if (watch_page && !buf_pool_watch_is_sentinel(buf_pool, watch_page)) {
		/* The page is already in the buffer pool. */
		watch_page = NULL;

		rw_lock_x_unlock(hash_lock);
		if (block) {
			mutex_enter(&block->mutex);
			buf_LRU_block_free_non_file_page(block);
			mutex_exit(&block->mutex);
		}

		bpage = NULL;
		goto func_exit;
	}

	bpage = &block->page;

	mutex_enter(&block->mutex);

	ut_ad(buf_pool_from_bpage(bpage) == buf_pool);

	buf_page_init(buf_pool, page_id, fold, zip_size, block);
	rw_lock_x_unlock(hash_lock);

	/* The block must be put to the LRU list, to the old blocks */
	buf_LRU_add_block(bpage, TRUE/* to old blocks */);

	/* We set a pass-type x-lock on the frame because then
	the same thread which called for the read operation
	(and is running now at this point of code) can wait
	for the read to complete by waiting for the x-lock on
	the frame; if the x-lock were recursive, the same
	thread would illegally get the x-lock before the page
	read is completed.  The x-lock is cleared by the
	io-handler thread. */

	rw_lock_x_lock_gen(&block->lock, BUF_IO_READ);
	buf_page_set_io_fix(bpage, BUF_IO_READ);

	mutex_exit(&block->mutex);

	buf_pool->n_pend_reads++;
func_exit:
	buf_pool_mutex_exit(buf_pool);

	if (mode == BUF_READ_IBUF_PAGES_ONLY) {

		ibuf_mtr_commit(&mtr);
	}


#ifdef UNIV_SYNC_DEBUG
	ut_ad(!rw_lock_own(hash_lock, RW_LOCK_EX));
	ut_ad(!rw_lock_own(hash_lock, RW_LOCK_SHARED));
#endif /* UNIV_SYNC_DEBUG */

	ut_ad(!bpage || buf_page_in_file(bpage));
	return(bpage);
}


/********************************************************************//**
Low-level function which reads a page asynchronously from a file to the
buffer buf_pool if it is not already there, in which case does nothing.
Sets the io_fix flag and sets an exclusive lock on the buffer frame. The
flag is cleared and the x-lock released by an i/o-handler thread.
@return 1 if a read request was queued, 0 if the page already resided
in buf_pool, or if the page is in the doublewrite buffer blocks in
which case it is never read into the pool, or if the tablespace does
not exist or is being dropped
@return 1 if read request is issued. 0 if it is not */
static uint32 buf_read_page_low(
	dberr_t*	err,	/*!< out: DB_SUCCESS or DB_TABLESPACE_DELETED if we are
			trying to read from a non-existent tablespace, or a
			tablespace which is just now being dropped */
	bool	sync,	/*!< in: true if synchronous aio is desired */
	ulint	mode,	/*!< in: BUF_READ_IBUF_PAGES_ONLY, ...,
			ORed to OS_AIO_SIMULATED_WAKE_LATER (see below
			at read-ahead functions) */
	ulint	space,	/*!< in: space id */
	ulint	zip_size,/*!< in: compressed page size, or 0 */
	ibool	unzip,	/*!< in: TRUE=request uncompressed page */
	ib_int64_t tablespace_version, /*!< in: if the space memory object has
			this timestamp different from what we are giving here,
			treat the tablespace as dropped; this is a timestamp we
			use to stop dangling page reads from a tablespace
			which we have DISCARDed + IMPORTed back */
	ulint	offset)	/*!< in: page number */
{
	buf_page_t*	bpage;
	ulint		wake_later;
	ibool		ignore_nonexistent_pages;

	*err = DB_SUCCESS;

	wake_later = mode & OS_AIO_SIMULATED_WAKE_LATER;
	mode = mode & ~OS_AIO_SIMULATED_WAKE_LATER;

	ignore_nonexistent_pages = mode & BUF_READ_IGNORE_NONEXISTENT_PAGES;
	mode &= ~BUF_READ_IGNORE_NONEXISTENT_PAGES;

	if (space == TRX_SYS_SPACE && buf_dblwr_page_inside(offset)) {
		ut_print_timestamp(stderr);
		fprintf(stderr,
			"  InnoDB: Warning: trying to read"
			" doublewrite buffer page %lu\n",
			(ulong) offset);

		return(0);
	}

	if (ibuf_bitmap_page(zip_size, offset)
	    || trx_sys_hdr_page(space, offset)) {

		/* Trx sys header is so low in the latching order that we play
		safe and do not leave the i/o-completion to an asynchronous
		i/o-thread. Ibuf bitmap pages must always be read with
		syncronous i/o, to make sure they do not get involved in
		thread deadlocks. */

		sync = true;
	}

	/* The following call will also check if the tablespace does not exist
	or is being dropped; if we succeed in initing the page in the buffer
	pool for read, then DISCARD cannot proceed until the read has
	completed */
	bpage = buf_page_init_for_read(err, mode, space, zip_size, unzip,
				       tablespace_version, offset);
	if (bpage == NULL) {

		return(0);
	}

	ut_ad(buf_page_in_file(bpage));
	ut_a(buf_page_get_state(bpage) == BUF_BLOCK_FILE_PAGE);

	*err = fil_io(OS_FILE_READ | wake_later
		      | ignore_nonexistent_pages,
		      sync, space, 0, offset, 0, UNIV_PAGE_SIZE,
		      ((buf_block_t*) bpage)->frame, bpage);
	if (*err != DB_SUCCESS) {
		if (ignore_nonexistent_pages || *err == DB_TABLESPACE_DELETED) {
			buf_read_page_handle_error(bpage);
			return(0);
		}
		/* else */
		ut_error;
	}

	if (sync) {
		/* The i/o is already completed when we arrive from fil_read */
		if (!buf_page_io_complete(bpage)) {
			return(0);
		}
	}

	return(1);
}


/********************************************************************//**
High-level function which reads a page asynchronously from a file to the
buffer buf_pool if it is not already there. Sets the io_fix flag and sets
an exclusive lock on the buffer frame. The flag is cleared and the x-lock
released by the i/o-handler thread.
@return TRUE if page has been read in, FALSE in case of failure */
bool32 buf_read_page(page_id_t *page_id)
{
    uint32      count;
    dberr_t     err;


	/* We do the i/o in the synchronous aio mode to save thread
	switches: hence TRUE */

	count = buf_read_page_low(&err, true, BUF_READ_ANY_PAGE, space,
				  zip_size, FALSE,
				  tablespace_version, offset);
	srv_stats.buf_pool_reads.add(count);
	if (err == DB_TABLESPACE_DELETED) {
		ut_print_timestamp(stderr);
		fprintf(stderr,
			"  InnoDB: Error: trying to access"
			" tablespace %lu page no. %lu,\n"
			"InnoDB: but the tablespace does not exist"
			" or is just being dropped.\n",
			(ulong) space, (ulong) offset);
	}

	/* Increment number of I/O operations used for LRU policy. */
	buf_LRU_stat_inc_io();

	return(count > 0);
}


buf_block_t *buf_page_get_gen(page_id_t *page_id, const page_size_t &page_size,
    uint32 rw_latch, buf_block_t *guess, Page_fetch mode,
    const char *file, uint32 line, mtr_t *mtr, bool32 dirty_with_no_latch)
{
    buf_block_t  *block;
    uint32        fold;
    uint64        access_time;
    uint32        fix_type;
    bool32        must_read;
    rw_lock_t    *hash_lock;
    uint32        retries = 0;
    buf_pool_t   *buf_pool;

    ut_ad((rw_latch == RW_S_LATCH) || (rw_latch == RW_X_LATCH) || (rw_latch == RW_NO_LATCH));

    buf_pool = buf_pool_get(page_id);
    buf_pool->stat.n_page_gets++;

    fold = buf_page_address_fold(page_id);
    hash_lock = buf_page_hash_lock_get(buf_pool, fold);

    rw_lock_s_lock(hash_lock);
    block = (buf_block_t*) buf_page_hash_get_low(buf_pool, page_id, fold);
    if (block == NULL) {
        if (buf_read_page(space, zip_size, offset)) {

        }
    }

  /* We can release hash_lock after we acquire block_mutex to
  make sure that no state change takes place. */
  block_mutex = buf_page_get_mutex(&block->page);
  mutex_enter(block_mutex);
  
  /* Now safe to release page_hash mutex */
  rw_lock_s_unlock(hash_lock);


    must_read = buf_block_get_io_fix(block) == BUF_IO_READ;
    if (must_read && (mode == BUF_GET_IF_IN_POOL || mode == BUF_PEEK_IF_IN_POOL)) {
        /* The page is being read to buffer pool,
        but we cannot wait around for the read to complete. */

        mutex_exit(block_mutex);

        return(NULL);
    }

    mutex_enter(&block->mutex);
    buf_block_buf_fix_inc(block, file, line);
    mutex_exit(&block->mutex);


	if (rw_latch == RW_S_LATCH) {
		rw_lock_s_lock_inline(&(block->lock), 0, file, line);
	} else if (rw_latch == RW_X_LATCH) {
		ut_ad(rw_latch == RW_X_LATCH);
		rw_lock_x_lock_inline(&(block->lock), 0, file, line);
    } else else if (rw_latch == RW_NO_LATCH) {
		if (must_read) {
			/* Let us wait until the read operation completes */
            mutex_enter(&block->mutex);
            io_fix = buf_block_get_io_fix(block);
            mutex_exit(&block->mutex);

        }
    }

    mtr_memo_push(mtr, block, fix_type);

    return(block);
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

// Inits a page to the buffer buf_pool.
// The block pointer must be private to the calling thread at the start of this function.
static void buf_page_init(buf_pool_t *buf_pool, const page_id_t &page_id,
                          const page_size_t &page_size, buf_block_t *block)
{
    buf_page_t *hash_page;

    ut_ad(buf_pool == buf_pool_get(page_id));
    ut_ad(mutex_own(buf_page_get_mutex(&block->page)));
    ut_a(buf_block_get_state(block) != BUF_BLOCK_FILE_PAGE);
    ut_ad(rw_lock_own(buf_page_hash_lock_get(buf_pool, page_id), RW_LOCK_EXCLUSIVE));

    /* Set the state of the block */
    buf_block_set_state(block, BUF_BLOCK_FILE_PAGE);
    block->page.id.copy_from(page_id);

    buf_block_init_low(block);
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
        LOGGER_ERROR(LOGGER, "Page %s already found in the hash table: %s block", page_id, hash_page);
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

  free_block = buf_LRU_get_free_block(buf_pool);

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

    DBUG_RETURN(buf_page_get_with_no_latch(page_id, page_size, mtr));
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
  mtr_memo_push(mtr, block, mtr_latch_type);

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


// Decrements the bufferfix count of a buffer control block and releases a latch, if specified. */
void buf_page_release(buf_block_t *block, /*!< in: buffer block */
    uint32 rw_latch) /*!< in: RW_S_LATCH, RW_X_LATCH, RW_NO_LATCH */
{
    ut_ad(block);

    ut_a(buf_block_get_state(block) == BUF_BLOCK_FILE_PAGE);
    ut_a(block->page.buf_fix_count > 0);

    mutex_enter(&block->mutex);

#ifdef UNIV_SYNC_DEBUG
    rw_lock_s_unlock(&(block->debug_latch));
#endif
    block->page.buf_fix_count--;

    mutex_exit(&block->mutex);

    if (rw_latch == RW_S_LATCH) {
        rw_lock_s_unlock(&(block->rw_lock));
    } else if (rw_latch == RW_X_LATCH) {
        rw_lock_x_unlock(&(block->rw_lock));
    }
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

// Gets the block to whose frame the pointer is pointing to if found in this buffer pool instance.
buf_block_t* buf_block_align_instance(buf_pool_t* buf_pool, /*!< in: buffer in which the block resides */
    const byte* ptr) /*!< in: pointer to a frame */
{
    uint32 offs;
    if (ptr < buf_pool->blocks->frame) {
        return NULL;
    }

    offs = ptr - buf_pool->blocks->frame;
    offs >>= UNIV_PAGE_SIZE_SHIFT_DEF;
    if (offs < buf_pool->size) {
        buf_block_t*	block = &buf_pool->blocks[offs];

        /* The function buf_chunk_init() invokes
        buf_block_init() so that block[n].frame == block->frame + n * UNIV_PAGE_SIZE.  Check it. */
        ut_ad((char *)block->frame == (char *)page_align((void *)ptr));

        return(block);
    }
}


// Gets the block to whose frame the pointer is pointing to.
buf_block_t* buf_block_align(const byte* ptr) /*!< in: pointer to a frame */
{
    uint32 i;

    for (i = 0; i < srv_buf_pool_instances; i++) {
        buf_block_t* block = buf_block_align_instance(&buf_pool_ptr[i], ptr);
        if (block) {
            return(block);
        }
    }

    /* The block should always be found. */
    ut_error;
    return(NULL);
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

    mutex_create(&block->mutex);

    rw_lock_create(&block->rw_lock);

    //ut_d(rw_lock_create(buf_block_debug_latch_key, &block->debug_latch, SYNC_NO_ORDER_CHECK));

    block->rw_lock.is_block_lock = 1;

    ut_ad(rw_lock_validate(&(block->rw_lock)));
}

static bool32 buf_pool_fill_block(buf_pool_t *buf_pool, uint64 mem_size)
{
    buf_block_t *block;
    byte *frame;
    uint64 i;

    /* Round down to a multiple of page size, although it already should be. */
    mem_size = ut_2pow_round(mem_size, UNIV_PAGE_SIZE);
    /* Reserve space for the block descriptors. */
    buf_pool->mem_size = mem_size +
        ut_2pow_round((mem_size / UNIV_PAGE_SIZE) * sizeof(buf_block_t) + (UNIV_PAGE_SIZE - 1), UNIV_PAGE_SIZE);
    buf_pool->mem = (uchar *)os_mem_alloc_large(&buf_pool->mem_size);
    if (buf_pool->mem == NULL) {
        LOGGER_FATAL(LOGGER, "can not alloc memory, size = %lu", buf_pool->mem_size);
        return FALSE;
    }

    /* Dump core without large memory buffers */
    if (buf_pool_should_madvise) {
        madvise_dont_dump((char *)buf_pool->mem, buf_pool->mem_size);
    }

    /* Allocate the block descriptors from the start of the memory block. */
    buf_pool->blocks = (buf_block_t *)buf_pool->mem;

    /* Align a pointer to the first frame.
     * Note that when os_large_page_size is smaller than UNIV_PAGE_SIZE,
     * we may allocate one fewer block than requested.
     * When it is bigger, we may allocate more blocks than requested.
     */
    frame = (byte *)ut_align(buf_pool->mem, UNIV_PAGE_SIZE);
    buf_pool->size = buf_pool->mem_size / UNIV_PAGE_SIZE - (frame != buf_pool->mem);

    /* Subtract the space needed for block descriptors. */
    {
        uint64 size = buf_pool->size;
        while (frame < (byte *)(buf_pool->blocks + size)) {
            frame += UNIV_PAGE_SIZE;
            size--;
        }
        buf_pool->size = size;
    }

    /* Init block structs and assign frames for them.
     * Then we assign the frames to the first blocks (we already mapped the memory above).
     */
    block = buf_pool->blocks;
    for (i = buf_pool->size; i--;) {
        buf_block_init(buf_pool, block, frame);
        //UNIV_MEM_INVALID(block->frame, UNIV_PAGE_SIZE);

        /* Add the block to the free list */
        UT_LIST_ADD_LAST(list_node, buf_pool->free, &block->page);

        ut_d(block->page.in_free_list = TRUE);
        ut_ad(buf_pool_from_block(block) == buf_pool);

        block++;
        frame += UNIV_PAGE_SIZE;
    }

    return TRUE;
}

static void buf_pool_create_instance(buf_pool_t *buf_pool,
    uint64 buf_pool_size, uint32 instance_no, dberr_t *err)
{
    uint32 i;

    /* 1. Initialize general fields */
    mutex_create(&buf_pool->LRU_list_mutex);
    mutex_create(&buf_pool->free_list_mutex);
    mutex_create(&buf_pool->flush_state_mutex);

    UT_LIST_INIT(buf_pool->LRU);
    UT_LIST_INIT(buf_pool->free);
    UT_LIST_INIT(buf_pool->flush_list);
    //UT_LIST_INIT(buf_pool->withdraw, &buf_page_t::list);
    buf_pool->withdraw_target = 0;

    if (!buf_pool_fill_block(buf_pool, buf_pool_size)) {
        *err = DB_ERROR;
        return;
    }

    buf_pool->instance_no = instance_no;

    /* Number of locks protecting page_hash must be a power of two */
    srv_n_page_hash_locks = ut_2_power_up(srv_n_page_hash_locks);
    ut_a(srv_n_page_hash_locks != 0);
    ut_a(srv_n_page_hash_locks <= MAX_PAGE_HASH_LOCKS);

    buf_pool->page_hash = HASH_TABLE_CREATE(2 * buf_pool->size);

    /* We create a hash table protected by rw_locks for buf_pool->page_hash. */
    buf_pool->page_hash->sync_obj.rw_locks = (rw_lock_t*)(malloc(srv_n_page_hash_locks * sizeof(rw_lock_t)));
    for (uint32 i = 0; i < srv_n_page_hash_locks; i++) {
        rw_lock_create(buf_pool->page_hash->sync_obj.rw_locks + i);
    }
    buf_pool->page_hash->n_sync_obj = srv_n_page_hash_locks;
    buf_pool->page_hash->type = HASH_TABLE_SYNC_RW_LOCK;

    //buf_pool->page_hash->heap = ;
    //buf_pool->page_hash->heaps = ;
    buf_pool->page_hash_old = NULL;

    buf_pool->last_printout_time = current_monotonic_time();

    /* 2. Initialize flushing fields */
    mutex_create(&buf_pool->flush_list_mutex);
    //for (i = BUF_FLUSH_LRU; i < BUF_FLUSH_N_TYPES; i++) {
    //  buf_pool->no_flush[i] = os_event_create(0);
    //}

    //buf_pool->watch = (buf_page_t *)my_malloc(sizeof(*buf_pool->watch) * BUF_POOL_WATCH_SIZE);
    //for (i = 0; i < BUF_POOL_WATCH_SIZE; i++) {
    //    buf_pool->watch[i].buf_pool_index = buf_pool->instance_no;
    //}

    /* All fields are initialized by ut_zalloc_nokey(). */
    buf_pool->try_LRU_scan = TRUE;
    /* Dirty Page Tracking is disabled by default. */
    //buf_pool->track_page_lsn = LSN_MAX;
    buf_pool->max_lsn_io = 0;

    memset(&buf_pool->stat, 0x00, sizeof(buf_pool->stat));


    *err = DB_SUCCESS;
}

static void buf_pool_free_instance(buf_pool_t *buf_pool)
{
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

    free(buf_pool->watch);
    buf_pool->watch = NULL;

    buf_block_t *block = buf_pool->blocks;

    for (uint64 i = buf_pool->size; i--; block++) {
        //os_mutex_free(&block->mutex);
        //rw_lock_free(&block->lock);
        //ut_d(rw_lock_free(&block->debug_latch));
    }

    if (buf_pool_should_madvise) {
        madvise_dump((char *)buf_pool->mem, buf_pool->mem_size);
    }
    os_mem_free_large(buf_pool->mem, buf_pool->mem_size);


    //for (ulint i = BUF_FLUSH_LRU; i < BUF_FLUSH_N_TYPES; ++i) {
    //  os_event_destroy(buf_pool->no_flush[i]);
    //}

    //ha_clear(buf_pool->page_hash);
    HASH_TABLE_FREE(buf_pool->page_hash);
}

// Frees the buffer pool global data structures
static void buf_pool_free()
{
    my_free(buf_pool_ptr);
    buf_pool_ptr = nullptr;
}

dberr_t buf_pool_init(uint64 total_size, uint32 n_instances)
{
    uint32 i;
    const uint64 size = total_size / n_instances;

    ut_ad(n_instances > 0);
    ut_ad(n_instances <= MAX_BUFFER_POOLS);
    ut_ad(n_instances == srv_buf_pool_instances);

    buf_pool_ptr = (buf_pool_t *)ut_malloc(n_instances * sizeof(buf_pool_t));

    /* Magic number 8 is from empirical testing on a 4 socket x 10 Cores x 2 HT host.
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
            threads[id - i] = thread_start(buf_pool_create_instance, &buf_pool_ptr[id], size, id, &errs[id - i]);
        }
        for (uint32 id = i; id < n; ++id) {
            if (!os_thread_join(threads[id - i]) || errs[id] != DB_SUCCESS) {
                err = DB_ERROR;
            }
        }

        if (err != DB_SUCCESS) {
            for (uint32 id = 0; id < n; ++id) {
                buf_pool_free_instance(&buf_pool_ptr[id]);
            }
            buf_pool_free();
            return err;
        }

        /* Do the next block of instances */
        i = n;
    }

    //buf_LRU_old_ratio_update(100 * 3 / 8, FALSE);

    return DB_SUCCESS;
}

buf_pool_t *buf_pool_get(page_id_t *page_id)
{
    /* 2log of BUF_READ_AHEAD_AREA (64) */
    page_no_t ignored_page_no = page_id->page_no() >> 6;
    page_id_t id(page_id->space(), ignored_page_no);

    uint32 i = id.fold() % srv_buf_pool_instances;

    return (&buf_pool_ptr[i]);
}


/********************************************************************//**
Gets the smallest oldest_modification lsn for any page in the pool. Returns
zero if all modified pages have been flushed to disk.
@return oldest modification in pool, zero if none */
lsn_t buf_pool_get_oldest_modification(void)
{
    uint32      i;
    buf_page_t* bpage;
    lsn_t       lsn = 0;
    lsn_t       oldest_lsn = 0;

    /* When we traverse all the flush lists we don't want another
    thread to add a dirty page to any flush list. */
    //log_flush_order_mutex_enter();

    for (i = 0; i < srv_buf_pool_instances; i++) {
        buf_pool_t* buf_pool;

        //buf_pool = buf_pool_from_array(i);

        //buf_flush_list_mutex_enter(buf_pool);

        //bpage = UT_LIST_GET_LAST(buf_pool->flush_list);
        //if (bpage != NULL) {
        //    ut_ad(bpage->in_flush_list);
        //    lsn = bpage->oldest_modification;
        //}

        //buf_flush_list_mutex_exit(buf_pool);

        if (!oldest_lsn || oldest_lsn > lsn) {
            oldest_lsn = lsn;
        }
    }

    //log_flush_order_mutex_exit();

    /* The returned answer may be out of date: the flush_list can
    change after the mutex has been released. */

    return(oldest_lsn);
}

