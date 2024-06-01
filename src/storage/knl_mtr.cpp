#include "knl_mtr.h"
#include "cm_dbug.h"
#include "cm_queue.h"
#include "cm_util.h"
#include "knl_buf.h"
#include "knl_page.h"
#include "knl_server.h"
#include "knl_log.h"
#include "knl_fsp.h"
#include "knl_dblwrite.h"


#define MTR_MEM_POOL_COUNT    32
memory_pool_t    *mtr_mem_pools[MTR_MEM_POOL_COUNT];

static atomic32_t  n_mtr_mem_pools_index = 0;


void* mtr_queue_next_node(void *current);
void** mtr_queue_next_node_address(void *current);

log_t*    log_sys = NULL;
dyn_queue mtr_queue(mtr_queue_next_node, mtr_queue_next_node_address);

void* mtr_queue_next_node(void *current)
{
    return ((mtr_t *)current)->queue_next_mtr;
}

void** mtr_queue_next_node_address(void *current)
{
    return (void **)(&((mtr_t *)current)->queue_next_mtr);
}


bool32 mtr_init(memory_area_t *area)
{
    uint32 page_size = DYN_ARRAY_DATA_SIZE * 16; // 8KB
    uint32 local_page_count = 1024 * 1024 / page_size;  // 1MB
    uint32 max_page_count = -1;  // unlimited

    for (uint32 i = 0; i < MTR_MEM_POOL_COUNT; i++) {
        mtr_mem_pools[i] = mpool_create(area, local_page_count, max_page_count, page_size);
        if (mtr_mem_pools[i] == NULL) {
            return FALSE;
        }
    }

    return TRUE;
}

dyn_array_t* dyn_array_create(dyn_array_t *arr)	/* in: pointer to a memory buffer of size sizeof(dyn_array_t) */
{
    uint32 idx = atomic32_inc(&n_mtr_mem_pools_index);
    arr->pool = mtr_mem_pools[idx / MTR_MEM_POOL_COUNT];
    UT_LIST_INIT(arr->pages);
    arr->current_page_used = 0;

    UT_LIST_INIT(arr->used_blocks);
    UT_LIST_INIT(arr->free_blocks);

    // init for first block
    arr->first_block.data = arr->first_block_data;
    arr->first_block.used = 0;
    UT_LIST_ADD_FIRST(list_node, arr->used_blocks, &arr->first_block);

    ut_d(arr->buf_end = 0);
    ut_d(arr->magic_n = DYN_BLOCK_MAGIC_N);

    return arr;
}

void dyn_array_free(dyn_array_t *arr)
{
    memory_page_t * page;

    page = UT_LIST_GET_FIRST(arr->pages);
    while (page) {
        UT_LIST_REMOVE(list_node, arr->pages, page);
        mpool_free_page(arr->pool, page);
        page = UT_LIST_GET_FIRST(arr->pages);
    }
    arr->current_page_used = 0;
}

static char* dyn_array_alloc_block_data(dyn_array_t *arr)
{
    char *data;
    memory_page_t *page;

    page = UT_LIST_GET_LAST(arr->pages);
    if (page == NULL || arr->current_page_used + DYN_ARRAY_DATA_SIZE > arr->pool->page_size) {
        page = mpool_alloc_page(arr->pool);
        if (page == NULL) {
            return NULL;
        }
        arr->current_page_used = 0;
        UT_LIST_ADD_LAST(list_node, arr->pages, page);
    }

    data = MEM_PAGE_DATA_PTR(page) + arr->current_page_used;
    arr->current_page_used += DYN_ARRAY_DATA_SIZE;

    return data;
}

//Gets pointer to the start of data in a dyn array block.
static byte* dyn_block_get_data(dyn_block_t *block)
{
    ut_ad(block);
    return(const_cast<byte*>(block->data));
}

//Gets the number of used bytes in a dyn array block.
uint32 dyn_block_get_used(const dyn_block_t *block)
{
    ut_ad(block);
    return((block->used) & ~DYN_BLOCK_FULL_FLAG);
}

dyn_block_t* dyn_array_add_block(dyn_array_t *arr)
{
    dyn_block_t *block;

    ut_ad(arr);
    ut_ad(arr->magic_n == DYN_BLOCK_MAGIC_N);

    // old block
    block = dyn_array_get_last_block(arr);
    if (block) {
        block->used = block->used | DYN_BLOCK_FULL_FLAG;
    }

    // alloc a new block
    block = UT_LIST_GET_FIRST(arr->free_blocks);
    if (block) {
        UT_LIST_REMOVE(list_node, arr->free_blocks, block);
    } else {
        char *data = dyn_array_alloc_block_data(arr);
        if (data == NULL) {
            return NULL;
        }
        uint32 used = 0;
        const uint32 block_size = ut_align8(sizeof(dyn_block_t));
        while (used + block_size <= DYN_ARRAY_DATA_SIZE) {
            block = (dyn_block_t *)((char *)data + used);
            UT_LIST_ADD_LAST(list_node, arr->free_blocks, block);
            used += block_size;
        }
        block = UT_LIST_GET_FIRST(arr->free_blocks);
        UT_LIST_REMOVE(list_node, arr->free_blocks, block);
    }

    // set data for new block
    block->data = (byte *)dyn_array_alloc_block_data(arr);
    if (block->data == NULL) {
        UT_LIST_ADD_LAST(list_node, arr->free_blocks, block);
        return NULL;
    }
    block->used = 0;
    UT_LIST_ADD_LAST(list_node, arr->used_blocks, block);

    return block;
}

byte* dyn_array_open(dyn_array_t *arr, uint32 size)
{
    dyn_block_t *block;

    ut_ad(arr);
    ut_ad(arr->magic_n == DYN_BLOCK_MAGIC_N);
    ut_ad(size <= DYN_ARRAY_DATA_SIZE);
    ut_ad(size);

    block = dyn_array_get_last_block(arr);
    if (block == NULL || block->used + size > DYN_ARRAY_DATA_SIZE) {
        block = dyn_array_add_block(arr);
        if (block == NULL) {
            return NULL;
        }
    }

    ut_ad(block->used <= DYN_ARRAY_DATA_SIZE);
    ut_ad(arr->buf_end == 0);
    ut_d(arr->buf_end = block->used + size);

    return block->data + block->used;
}

void dyn_array_close(dyn_array_t *arr, byte *ptr)
{
    dyn_block_t *block;

    ut_ad(arr);
    ut_ad(arr->magic_n == DYN_BLOCK_MAGIC_N);

    block = dyn_array_get_last_block(arr);

    ut_ad(arr->buf_end + block->data >= ptr);

    block->used = ptr - block->data;

    ut_ad(block->used <= DYN_ARRAY_DATA_SIZE);
    ut_d(arr->buf_end = 0);
}

void* dyn_array_push(dyn_array_t *arr, uint32 size)
{
    dyn_block_t *block;
    uint32 used;

    ut_ad(arr);
    ut_ad(arr->magic_n == DYN_BLOCK_MAGIC_N);
    ut_ad(size <= DYN_ARRAY_DATA_SIZE);
    ut_ad(size);

    block = dyn_array_get_last_block(arr);
    if (block == NULL || block->used + size > DYN_ARRAY_DATA_SIZE) {
        block = dyn_array_add_block(arr);
        if (block == NULL) {
            return NULL;
        }
    }

    used = block->used;
    block->used += size;
    ut_ad(block->used <= DYN_ARRAY_DATA_SIZE);

    return block->data + used;
}

void dyn_push_string(dyn_array_t *arr, byte *str, uint32 len)
{
    byte *ptr;
    uint32 n_copied;

    while (len > 0) {
        if (len > DYN_ARRAY_DATA_SIZE) {
            n_copied = DYN_ARRAY_DATA_SIZE;
        } else {
            n_copied = len;
        }

        ptr = (byte*) dyn_array_push(arr, n_copied);
        memcpy(ptr, str, n_copied);
        str += n_copied;
        len -= n_copied;
    }
}

void* dyn_array_get_element(dyn_array_t *arr, uint32 pos)
{
    dyn_block_t *block;
    uint32 used;

    ut_ad(arr);
    ut_ad(arr->magic_n == DYN_BLOCK_MAGIC_N);

    /* Get the first array block */
    block = dyn_array_get_first_block(arr);
    ut_ad(block);
    used = block->used;

    while (pos >= used) {
        pos -= used;
        block = dyn_array_get_next_block(arr, block);
        ut_ad(block);
        used = block->used;
    }

    ut_ad(block);
    ut_ad(dyn_block_get_used(block) >= pos);

    return block->data + pos;
}

uint32 dyn_array_get_data_size(dyn_array_t *arr)
{
    dyn_block_t *block;
    uint32 sum = 0;

    ut_ad(arr);
    ut_ad(arr->magic_n == DYN_BLOCK_MAGIC_N);

    /* Get the first array block */
    block = dyn_array_get_first_block(arr);
    while (block != NULL) {
        sum += dyn_block_get_used(block);
        block = dyn_array_get_next_block(arr, block);
    }

    return sum;
}

// Checks if memo contains the given item.
bool32 mtr_memo_contains(mtr_t *mtr,
    const void *object, /*!< in: object to search */
    uint32 type) /*!< in: type of object */
{
    mtr_memo_slot_t *slot;
    dyn_array_t *memo;
    uint32 offset;

    ut_ad(mtr);
    ut_ad(mtr->magic_n == MTR_MAGIC_N);
    ut_ad(mtr->state == MTR_ACTIVE || mtr->state == MTR_COMMITTING);

    memo = &(mtr->memo);
    offset = dyn_array_get_data_size(memo);

    while (offset > 0) {
        offset -= sizeof(mtr_memo_slot_t);
        slot = (mtr_memo_slot_t*) dyn_array_get_element(memo, offset);
        if ((object == slot->object) && (type == slot->type)) {
            return(TRUE);
        }
    }

    return(FALSE);
}

bool32 mtr_memo_contains_page(mtr_t* mtr, /*!< in: mtr */
    const byte* ptr, /*!< in: pointer to buffer frame */
    uint32 type) /*!< in: type of object */
{
    return(mtr_memo_contains(mtr, buf_block_align(ptr), type));
}


// Releases the item in the slot given. */
static void mtr_memo_slot_release(mtr_t *mtr, mtr_memo_slot_t *slot)
{
    void *object = slot->object;
    slot->object = NULL;

    /* slot release is a local operation for the current mtr.
       We must not be holding the flush_order mutex while doing this. */
    //ut_ad(!mutex_own(&log_sys->log_flush_order_mutex));

    switch (slot->type) {
        case MTR_MEMO_PAGE_S_FIX:
        case MTR_MEMO_PAGE_X_FIX:
        case MTR_MEMO_BUF_FIX:
            buf_page_release((buf_block_t*)object, slot->type);
            break;
        case MTR_MEMO_S_LOCK:
            rw_lock_s_unlock((rw_lock_t*)object);
            break;
        case MTR_MEMO_X_LOCK:
            rw_lock_x_unlock((rw_lock_t*)object);
            break;
#ifdef UNIV_DEBUG
        default:
            ut_ad(slot->type == MTR_MEMO_MODIFY);
            ut_ad(mtr_memo_contains(mtr, object, MTR_MEMO_PAGE_X_FIX));
#endif /* UNIV_DEBUG */
    }
}

/***************************************************//**
Checks if a mini-transaction is dirtying a clean page.
@return TRUE if the mtr is dirtying a clean page. */
static bool32 mtr_block_dirtied(const buf_block_t *block) /*!< in: block being x-fixed */
{
    ut_ad(buf_block_get_state(block) == BUF_BLOCK_FILE_PAGE);
    ut_ad(block->page.buf_fix_count > 0);

    /* It is OK to read oldest_modification because no
    other thread can be performing a write of it and it
    is only during write that the value is reset to 0. */
    return(block->page.oldest_modification == 0);
}


// Pushes an object to an mtr memo stack
void mtr_memo_push(mtr_t *mtr,	/*!< in: mtr */
    void *object, /*!< in: object */
    uint32 type)  /*!< in: object type: MTR_MEMO_S_LOCK, ... */
{
    dyn_array_t *memo;
    mtr_memo_slot_t *slot;

    ut_ad(object);
    ut_ad(type >= MTR_MEMO_PAGE_S_FIX);
    ut_ad(type <= MTR_MEMO_X_LOCK);
    ut_ad(mtr);
    ut_ad(mtr->magic_n == MTR_MAGIC_N);
    ut_ad(mtr->state == MTR_ACTIVE);

    memo = &(mtr->memo);
    slot = (mtr_memo_slot_t*) dyn_array_push(memo, sizeof(mtr_memo_slot_t));
    slot->object = object;
    slot->type = type;
}


/* Releases the mlocks and other objects stored in an mtr memo.
   They are released in the order opposite to which they were pushed to the memo. */
static void mtr_memo_pop_all(mtr_t *mtr)
{
    ut_ad(mtr->magic_n == MTR_MAGIC_N);
    ut_ad(mtr->state == MTR_COMMITTING); /* Currently only used in commit */

    for (dyn_block_t* block = dyn_array_get_last_block(&mtr->memo);
         block;
         block = dyn_array_get_prev_block(&mtr->memo, block)) {
        const mtr_memo_slot_t *start = (mtr_memo_slot_t*)dyn_block_get_data(block);
        mtr_memo_slot_t *slot = (mtr_memo_slot_t*)(dyn_block_get_data(block) + dyn_block_get_used(block));

        ut_ad(!(dyn_block_get_used(block) % sizeof(mtr_memo_slot_t)));

        while (slot-- != start) {
            if (slot->object != NULL) {
                mtr_memo_slot_release(mtr, slot);
            }
        }
    }
}

uint32 mtr_get_log_mode(mtr_t *mtr)
{
    ut_ad(mtr);
    ut_ad(mtr->log_mode >= MTR_LOG_ALL);
    ut_ad(mtr->log_mode <= MTR_LOG_SHORT_INSERTS);

    return(mtr->log_mode);
}


// Releases the item in the slot given
static void mtr_memo_slot_note_modification(mtr_t *mtr, mtr_memo_slot_t *slot)
{
    ut_ad(mtr->modifications);
    //ut_ad(!srv_read_only_mode);
    ut_ad(mtr->magic_n == MTR_MAGIC_N);

    if (slot->object != NULL && slot->type == MTR_MEMO_PAGE_X_FIX) {
        buf_block_t *block = (buf_block_t*) slot->object;
        //buf_flush_note_modification(block, mtr);
    }
}

/**********************************************************//**
Add the modified pages to the buffer flush list.
They are released in the order opposite to which they were pushed to the memo.
NOTE! It is essential that the x-rw-lock on a modified buffer page is not released
before buf_page_note_modification is called for that page! 
Otherwise, some thread might race to modify it, and the flush list sort order on lsn would be destroyed. */
static void mtr_memo_note_modifications(mtr_t *mtr)
{
    dyn_array_t *memo;
    uint32 offset;

    //ut_ad(!srv_read_only_mode);
    ut_ad(mtr->magic_n == MTR_MAGIC_N);
    ut_ad(mtr->state == MTR_COMMITTING); /* Currently only used in commit */

    memo = &mtr->memo;
    offset = dyn_array_get_data_size(memo);

    while (offset > 0) {
        mtr_memo_slot_t* slot;
        offset -= sizeof(mtr_memo_slot_t);
        slot = static_cast<mtr_memo_slot_t*>(dyn_array_get_element(memo, offset));
        mtr_memo_slot_note_modification(mtr, slot);
    }
}

// Append the dirty pages to the flush list
static void mtr_add_dirtied_pages_to_flush_list(mtr_t *mtr)
{
    ut_ad(!srv_read_only_mode);

    if (mtr->modifications) {
        mtr_memo_note_modifications(mtr);
    }
}

//Writes the contents of a mini-transaction log, if any, to the database log. */
static void mtr_log_reserve_and_write(mtr_t *mtr)
{
    dyn_array_t *mlog;
    dyn_block_t *first_block;
    uint32 data_size;
    byte *first_data;
    uint64 start_lsn;

    //ut_ad(!srv_read_only_mode);

    mlog = &(mtr->log);
    first_block = dyn_array_get_first_block(mlog);
    first_data = dyn_block_get_data(first_block);

    if (mtr->n_log_recs > 1) {
        mlog_catenate_uint32(mtr, MLOG_MULTI_REC_END, MLOG_1BYTE);
    } else {
        *first_data = (byte)((uint32)*first_data | MLOG_SINGLE_REC_FLAG);
    }

    // If the queue was empty: we're the leader for this batch
    //bool32 leader= mtr_queue.append(mtr);

    /*
      If the queue was not empty, we're a follower and wait for the
      leader to process the queue. If we were holding a mutex, we have
      to release it before going to sleep.
    */
    //if (!leader) {
        //os_event_wait(os_event_t event, uint64 reset_sig_count);
    //}

    data_size = dyn_array_get_data_size(mlog);
    mtr->start_buf_lsn = log_buffer_reserve(data_size);
    mtr->end_lsn = mtr->start_buf_lsn.val.lsn + mtr->start_buf_lsn.data_len;
    if (mtr->log_mode == MTR_LOG_ALL) {
        start_lsn = mtr->start_buf_lsn.val.lsn;
        for (dyn_block_t* block = first_block;
             block != 0;
             block = dyn_array_get_next_block(mlog, block)) {
            start_lsn = log_buffer_write(start_lsn, dyn_block_get_data(block), dyn_block_get_used(block));
        }
        log_write_complete(&mtr->start_buf_lsn);
    } else {
        ut_ad(mtr->log_mode == MTR_LOG_NONE || mtr->log_mode == MTR_LOG_NO_REDO);
        /* Do nothing */
    }

    mtr_add_dirtied_pages_to_flush_list(mtr);
}



/*
 * Writes the initial part of a log record (3..11 bytes).
 * If the implementation of this function is changed,
 * all size parameters to mlog_open() should be adjusted accordingly!
 * return: new value of log_ptr
 */
byte* mlog_write_initial_log_record_fast(
    const byte* ptr,    /*!< in: pointer to (inside) a buffer frame holding the file page where modification is made */
    mlog_id_t   type,   /*!< in: log item type: MLOG_1BYTE, ... */
    byte*       log_ptr,/*!< in: pointer to mtr log which has been opened */
    mtr_t*      mtr)    /*!< in: mtr */
{
#ifdef UNIV_DEBUG
    buf_block_t* block;
#endif
    const byte*  page;
    uint32       space;
    uint32       offset;

    ut_ad(mtr_memo_contains_page(mtr, ptr, MTR_MEMO_PAGE_X_FIX));
    ut_ad(type <= MLOG_BIGGEST_TYPE);
    ut_ad(ptr && log_ptr);

    page = (const byte*) ut_align_down((void *)ptr, UNIV_PAGE_SIZE);
    space = mach_read_from_4(page + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
    offset = mach_read_from_4(page + FIL_PAGE_OFFSET);

    /* check whether the page is in the doublewrite buffer;
    the doublewrite buffer is located in pages FSP_EXTENT_SIZE, ..., 3 * FSP_EXTENT_SIZE - 1 in the system tablespace */
    if (space == FIL_SYSTEM_SPACE_ID && offset >= FSP_EXTENT_SIZE && offset < 3 * FSP_EXTENT_SIZE) {
        if (buf_dblwr_being_created) {
            /* Do nothing: we only come to this branch in an InnoDB database creation.
               We do not redo log anything for the doublewrite buffer pages. */
            return(log_ptr);
        } else {
            fprintf(stderr,
                "Error: trying to redo log a record of type "
                "%d on page %lu of space %lu in the doublewrite buffer, continuing anyway.\n"
                "Please post a bug report to bugs.mysql.com.\n",
                type, offset, space);
            ut_ad(0);
        }
    }

    mach_write_to_1(log_ptr, type);
    log_ptr++;
    log_ptr += mach_write_compressed(log_ptr, space);
    log_ptr += mach_write_compressed(log_ptr, offset);

    mtr->n_log_recs++;

#ifdef UNIV_LOG_DEBUG
    fprintf(stderr, "Adding to mtr log record type %lu space %lu page no %lu\n", (ulong) type, space, offset);
#endif

#ifdef UNIV_DEBUG
    /* We now assume that all x-latched pages have been modified! */
    block = (buf_block_t*) buf_block_align(ptr);
    if (!mtr_memo_contains(mtr, block, MTR_MEMO_MODIFY)) {
        mtr_memo_push(mtr, block, MTR_MEMO_MODIFY);
    }
#endif
    return log_ptr;
}

/*
 * Writes the initial part of a log record consisting of one-byte item
 * type and four-byte space and page numbers.
 * Also pushes info to the mtr memo that a buffer page has been modified. */
void mlog_write_initial_log_record(
    const byte *ptr, /*!< in: pointer to (inside) a buffer frame holding the file page where modification is made */
    mlog_id_t type, /*!< in: log item type: MLOG_1BYTE, ... */
    mtr_t *mtr) /*!< in: mini-transaction handle */
{
    byte*	log_ptr;

    ut_ad(type <= MLOG_BIGGEST_TYPE);
    ut_ad(type > MLOG_8BYTES);

    log_ptr = mlog_open(mtr, 11);

    /* If no logging is requested, we may return now */
    if (log_ptr == NULL) {
        return;
    }

    log_ptr = mlog_write_initial_log_record_fast(ptr, type, log_ptr, mtr);

    mlog_close(mtr, log_ptr);
}


/** Read 1 to 4 bytes from a file page buffered in the buffer pool.
@param[in] ptr pointer where to read
@param[in] type MLOG_1BYTE, MLOG_2BYTES, or MLOG_4BYTES
@return value read */
uint32 mlog_read_uint32(const byte* ptr, mlog_id_t type)
{
    switch (type) {
    case MLOG_1BYTE:
        return(mach_read_from_1(ptr));
    case MLOG_2BYTES:
        return(mach_read_from_2(ptr));
    case MLOG_4BYTES:
        return(mach_read_from_4(ptr));
    default:
        break;
    }

    ut_error;
    return(0);
}

/********************************************************//**
Writes 1, 2 or 4 bytes to a file page. Writes the corresponding log
record to the mini-transaction log if mtr is not NULL. */
void mlog_write_uint32(
	byte*		ptr,	/*!< in: pointer where to write */
	uint32		val,	/*!< in: value to write */
	mlog_id_t	type,	/*!< in: MLOG_1BYTE, MLOG_2BYTES, MLOG_4BYTES */
	mtr_t*		mtr)	/*!< in: mini-transaction handle */
{
	switch (type) {
	case MLOG_1BYTE:
		mach_write_to_1(ptr, val);
		break;
	case MLOG_2BYTES:
		mach_write_to_2(ptr, val);
		break;
	case MLOG_4BYTES:
		mach_write_to_4(ptr, val);
		break;
	default:
		ut_error;
	}

	if (mtr != 0) {
		byte*	log_ptr = mlog_open(mtr, 11 + 2 + 5);

		/* If no logging is requested, we may return now */
		if (log_ptr != 0) {
			log_ptr = mlog_write_initial_log_record_fast(ptr, type, log_ptr, mtr);

			mach_write_to_2(log_ptr, page_offset(ptr));
			log_ptr += 2;

			log_ptr += mach_write_compressed(log_ptr, val);

			mlog_close(mtr, log_ptr);
		}
	}
}

/********************************************************//**
Writes 8 bytes to a file page. Writes the corresponding log
record to the mini-transaction log, only if mtr is not NULL */
void mlog_write_uint64(
	byte*		ptr,	/*!< in: pointer where to write */
	uint64      val,	/*!< in: value to write */
	mtr_t*		mtr)	/*!< in: mini-transaction handle */
{
	mach_write_to_8(ptr, val);

	if (mtr != 0) {
		byte*	log_ptr = mlog_open(mtr, 11 + 2 + 9);

		/* If no logging is requested, we may return now */
		if (log_ptr != 0) {
			log_ptr = mlog_write_initial_log_record_fast(ptr, MLOG_8BYTES, log_ptr, mtr);

			mach_write_to_2(log_ptr, page_offset(ptr));
			log_ptr += 2;

			log_ptr += mach_ull_write_compressed(log_ptr, val);

			mlog_close(mtr, log_ptr);
		}
	}
}

/********************************************************//**
Writes a string to a file page buffered in the buffer pool. Writes the
corresponding log record to the mini-transaction log. */
void mlog_write_string(
	byte*		ptr,	/*!< in: pointer where to write */
	const byte*	str,	/*!< in: string to write */
	uint32		len,	/*!< in: string length */
	mtr_t*		mtr)	/*!< in: mini-transaction handle */
{
	ut_ad(ptr && mtr);
	ut_a(len < UNIV_PAGE_SIZE);

	memcpy(ptr, str, len);

	mlog_log_string(ptr, len, mtr);
}

// Catenates n bytes to the mtr log.
void mlog_catenate_string(
    mtr_t      *mtr,    /*!< in: mtr */
    byte       *str,    /*!< in: string to write */
    uint32      len)    /*!< in: string length */
{
    dyn_array_t*    mlog;

    if (mtr_get_log_mode(mtr) == MTR_LOG_NONE) {
        return;
    }

    mlog = &(mtr->log);

    dyn_push_string(mlog, str, len);
}

/********************************************************//**
Catenates 1 - 4 bytes to the mtr log. The value is not compressed. */
void mlog_catenate_uint32(mtr_t *mtr,
    uint32 val, /*!< in: value to write */
    uint32 type) /*!< in: MLOG_1BYTE, MLOG_2BYTES, MLOG_4BYTES */
{
    dyn_array_t *mlog;
    byte *ptr;

    if (mtr_get_log_mode(mtr) == MTR_LOG_NONE) {
        return;
    }

    mlog = &(mtr->log);
    ptr = (byte*) dyn_array_push(mlog, type);

    if (type == MLOG_4BYTES) {
        mach_write_to_4(ptr, val);
    } else if (type == MLOG_2BYTES) {
        mach_write_to_2(ptr, val);
    } else {
        ut_ad(type == MLOG_1BYTE);
        mach_write_to_1(ptr, val);
    }
}


/********************************************************//**
Logs a write of a string to a file page buffered in the buffer pool.
Writes the corresponding log record to the mini-transaction log. */
void mlog_log_string(
    byte *ptr, /*!< in: pointer written to */
    uint32 len, /*!< in: string length */
    mtr_t *mtr) /*!< in: mini-transaction handle */
{
    byte *log_ptr;

    ut_ad(ptr && mtr);
    ut_ad(len <= UNIV_PAGE_SIZE);

    log_ptr = mlog_open(mtr, 30);

    /* If no logging is requested, we may return now */
    if (log_ptr == NULL) {
        return;
    }

    log_ptr = mlog_write_initial_log_record_fast(ptr, MLOG_WRITE_STRING, log_ptr, mtr);
    mach_write_to_2(log_ptr, page_offset(ptr));
    log_ptr += 2;

    mach_write_to_2(log_ptr, len);
    log_ptr += 2;

    mlog_close(mtr, log_ptr);

    mlog_catenate_string(mtr, ptr, len);
}

/********************************************************//**
Reads 1 - 4 bytes from a file page buffered in the buffer pool.
@return	value read */
uint32 mtr_read_uint32(
	const byte*	ptr,	/*!< in: pointer from where to read */
	mlog_id_t	type,	/*!< in: MLOG_1BYTE, MLOG_2BYTES, MLOG_4BYTES */
	mtr_t*		mtr)    /*!< in: mini-transaction handle */
{
	ut_ad(mtr->state == MTR_ACTIVE);
	ut_ad(mtr_memo_contains_page(mtr, ptr, MTR_MEMO_PAGE_S_FIX)
	      || mtr_memo_contains_page(mtr, ptr, MTR_MEMO_PAGE_X_FIX));

	return mlog_read_uint32(ptr, type);
}

mtr_t* mtr_start(mtr_t *mtr)
{
    dyn_array_create(&(mtr->memo));
    dyn_array_create(&(mtr->log));

    mtr->log_mode = MTR_LOG_ALL;
    mtr->modifications = FALSE;
    mtr->n_log_recs = 0;
    mtr->inside_ibuf = FALSE;
    mtr->made_dirty = FALSE;
    //mtr->n_freed_pages = 0;

    ut_d(mtr->state = MTR_ACTIVE);
    ut_d(mtr->magic_n = MTR_MAGIC_N);

    return(mtr);
}

void mtr_commit(mtr_t *mtr)
{
    ut_ad(mtr);
    ut_ad(mtr->magic_n == MTR_MAGIC_N);
    ut_ad(mtr->state == MTR_ACTIVE);
    ut_ad(!mtr->inside_ibuf);
    ut_d(mtr->state = MTR_COMMITTING);

#ifndef UNIV_HOTBACKUP
    /* This is a dirty read, for debugging. */
    //ut_ad(!recv_no_log_write);

    if (mtr->modifications && mtr->n_log_recs) {
        //ut_ad(!srv_read_only_mode);
        mtr_log_reserve_and_write(mtr);
    }

    mtr_memo_pop_all(mtr);
#endif /* !UNIV_HOTBACKUP */

    dyn_array_free(&(mtr->memo));
    dyn_array_free(&(mtr->log));

    ut_d(mtr->state = MTR_COMMITTED);
}

byte* mlog_open(mtr_t *mtr, uint32 size)
{
    dyn_array_t *mlog;

    mtr->modifications = TRUE;

    if (mtr_get_log_mode(mtr) == MTR_LOG_NONE) {
        return(NULL);
    }

    mlog = &(mtr->log);

    return(dyn_array_open(mlog, size));
}

void mlog_close(mtr_t *mtr, byte *ptr)
{
    dyn_array_t *mlog;

    ut_ad(mtr_get_log_mode(mtr) != MTR_LOG_NONE);

    mlog = &(mtr->log);

    dyn_array_close(mlog, ptr);
}

void mtr_s_lock_func(rw_lock_t* lock, /*!< in: rw-lock */
    const char* file, /*!< in: file name */
    uint32 line, /*!< in: line number */
    mtr_t* mtr) /*!< in: mtr */
{
    ut_ad(mtr);
    ut_ad(lock);

    rw_lock_s_lock(lock);
    mtr_memo_push(mtr, lock, MTR_MEMO_S_LOCK);
}

void mtr_x_lock_func(rw_lock_t* lock, /*!< in: rw-lock */
    const char* file, /*!< in: file name */
    uint32 line, /*!< in: line number */
    mtr_t* mtr) /*!< in: mtr */
{
    ut_ad(mtr);
    ut_ad(lock);

    rw_lock_x_lock(lock);
    mtr_memo_push(mtr, lock, MTR_MEMO_X_LOCK);
}


