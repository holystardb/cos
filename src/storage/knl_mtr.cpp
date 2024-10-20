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
#include "knl_buf_flush.h"

static memory_pool_t    *mtr_memory_pool = NULL;


void* mtr_queue_next_node(void *current);
void** mtr_queue_next_node_address(void *current);

log_t*    log_sys = NULL;
dyn_queue mtr_queue(mtr_queue_next_node, mtr_queue_next_node_address);

inline void* mtr_queue_next_node(void *current)
{
    return ((mtr_t *)current)->queue_next_mtr;
}

inline void** mtr_queue_next_node_address(void *current)
{
    return (void **)(&((mtr_t *)current)->queue_next_mtr);
}


inline void mtr_init(memory_pool_t* pool)
{
    mtr_memory_pool = pool;
}

static inline dyn_array_t* dyn_array_create(dyn_array_t* arr, memory_pool_t* mem_pool)
{
    arr->pool = mem_pool;

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

static inline void dyn_array_free(dyn_array_t *arr)
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

static inline char* dyn_array_alloc_block_data(dyn_array_t *arr)
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
static inline byte* dyn_block_get_data(dyn_block_t *block)
{
    return block->data;
}

//Gets the number of used bytes in a dyn array block.
inline uint32 dyn_block_get_used(const dyn_block_t* block)
{
    return ((block->used) & ~DYN_BLOCK_FULL_FLAG);
}

inline void dyn_block_set_used(dyn_block_t* block, uint32 used)
{
    ut_a(used <= DYN_ARRAY_DATA_SIZE);
    if (block->used & DYN_BLOCK_FULL_FLAG) {
        block->used = used | DYN_BLOCK_FULL_FLAG;
    } else {
        block->used = used;
    }
}

inline dyn_block_t* dyn_array_add_block(dyn_array_t* arr)
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
    if (block == NULL) {  // create blocks
        // get buf for create blocks
        char *data = dyn_array_alloc_block_data(arr);
        if (data == NULL) {
            return NULL;
        }

        // create blocks and insert into free_blocks
        uint32 used = 0;
        const uint32 block_size = ut_align8(sizeof(dyn_block_t));
        while (used + block_size <= DYN_ARRAY_DATA_SIZE) {
            block = (dyn_block_t *)((char *)data + used);
            block->data = NULL;
            UT_LIST_ADD_LAST(list_node, arr->free_blocks, block);
            used += block_size;
        }

        // get new block from free_blocks
        block = UT_LIST_GET_FIRST(arr->free_blocks);
    }

    // set data for new block
    if (block->data == NULL) {
        block->data = (byte *)dyn_array_alloc_block_data(arr);
        if (block->data == NULL) {
            return NULL;
        }
    }

    // init
    block->used = 0;
    UT_LIST_REMOVE(list_node, arr->free_blocks, block);
    UT_LIST_ADD_LAST(list_node, arr->used_blocks, block);

    return block;
}

inline void dyn_array_release_block(dyn_array_t* arr, dyn_block_t* block)
{
    UT_LIST_REMOVE(list_node, arr->used_blocks, block);
    UT_LIST_ADD_LAST(list_node, arr->free_blocks, block);

    dyn_block_t* last = dyn_array_get_last_block(arr);
    if (last && last->used & DYN_BLOCK_FULL_FLAG) {
        last->used = dyn_block_get_used(last);
    }
}

inline byte* dyn_array_open(dyn_array_t* arr, uint32 size)
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

inline void dyn_array_close(dyn_array_t* arr, byte* ptr)
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

inline void* dyn_array_push(dyn_array_t* arr, uint32 size)
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

inline void dyn_push_string(dyn_array_t* arr, byte* str, uint32 len)
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

inline void* dyn_array_get_element(dyn_array_t* arr, uint32 pos)
{
    dyn_block_t *block;
    uint32 used;

    ut_ad(arr);
    ut_ad(arr->magic_n == DYN_BLOCK_MAGIC_N);

    /* Get the first array block */
    block = dyn_array_get_first_block(arr);
    ut_ad(block);
    used = dyn_block_get_used(block);

    while (pos >= used) {
        pos -= used;
        block = dyn_array_get_next_block(arr, block);
        ut_ad(block);
        used = dyn_block_get_used(block);
    }

    ut_ad(block);
    ut_ad(dyn_block_get_used(block) >= pos);

    return block->data + pos;
}

inline uint32 dyn_array_get_data_size(dyn_array_t* arr)
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
inline bool32 mtr_memo_contains(mtr_t* mtr, const void* object, uint32 type)
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

inline bool32 mtr_memo_contains_page(mtr_t* mtr, const byte* ptr, /*!< in: pointer to buffer frame */
    uint32 type) /*!< in: type of object */
{
    return(mtr_memo_contains(mtr, buf_block_align(ptr), type));
}

// Releases the item in the slot given. */
static inline void mtr_memo_slot_release(mtr_t* mtr, mtr_memo_slot_t* slot)
{
    void *object = slot->object;
    slot->object = NULL;

    switch (slot->type) {
        case MTR_MEMO_PAGE_S_FIX:
            rw_lock_s_unlock(&((buf_block_t*)object)->rw_lock);
            break;
        case MTR_MEMO_PAGE_X_FIX:
            rw_lock_x_unlock(&((buf_block_t*)object)->rw_lock);
            break;
        case MTR_MEMO_BUF_FIX:
            buf_page_unfix(&((buf_block_t*)object)->page);
            break;
        case MTR_MEMO_S_LOCK:
            rw_lock_s_unlock((rw_lock_t*)object);
            break;
        case MTR_MEMO_X_LOCK:
            rw_lock_x_unlock((rw_lock_t*)object);
            break;
#ifdef UNIV_DEBUG1
        default:
            ut_ad(slot->type == MTR_MEMO_MODIFY);
            ut_ad(mtr_memo_contains(mtr, object, MTR_MEMO_PAGE_X_FIX));
#endif // UNIV_DEBUG
    }
}

// Checks if a mini-transaction is dirtying a clean page.
// return TRUE if the mtr is dirtying a clean page.
static inline bool32 mtr_block_dirtied(const buf_block_t* block)
{
    ut_ad(buf_block_get_state(block) == BUF_BLOCK_FILE_PAGE);
    ut_ad((block->is_resident() && block->page.buf_fix_count == 0) ||
          (!block->is_resident() && block->page.buf_fix_count > 0));

    /* It is OK to read recovery_lsn because no
       other thread can be performing a write of it and it
       is only during write that the value is reset to 0. */
    return (block->page.recovery_lsn == 0);
}

// Sets and returns a savepoint in mtr
// return savepoint
inline uint32 mtr_set_savepoint(mtr_t* mtr)
{
    dyn_array_t* memo;

    ut_ad(mtr->magic_n == MTR_MAGIC_N);
    ut_ad(mtr->state == MTR_ACTIVE);

    memo = &(mtr->memo);

    return dyn_array_get_data_size(memo);
}

// Releases in an mtr memo after a savepoint
inline void mtr_release_at_savepoint(mtr_t* mtr, uint32 savepoint)
{
    uint32 sum = dyn_array_get_data_size(&mtr->memo);
    uint32 pos = savepoint;

    ut_ad(mtr->magic_n == MTR_MAGIC_N);
    ut_ad(mtr->state == MTR_ACTIVE);
    ut_ad(sum > savepoint);

    for (dyn_block_t* block = dyn_array_get_last_block(&mtr->memo);
         block && pos < sum;
         block = dyn_array_get_prev_block(&mtr->memo, block)) {

        uint32 used = dyn_block_get_used(block);
        ut_ad(!(used % sizeof(mtr_memo_slot_t)));

        const mtr_memo_slot_t *start;
        mtr_memo_slot_t *slot = (mtr_memo_slot_t*)(dyn_block_get_data(block) + used);

        if (pos <= sum - used) {
            start = (mtr_memo_slot_t*)dyn_block_get_data(block);
            while (slot-- != start) {
                if (slot->object != NULL) {
                    mtr_memo_slot_release(mtr, slot);
                }
            }
            dyn_array_release_block(&mtr->memo, block);
            sum -= used;
        } else {
            start = (mtr_memo_slot_t*)(dyn_block_get_data(block) + pos);
            while (slot-- != start) {
                if (slot->object != NULL) {
                    mtr_memo_slot_release(mtr, slot);
                }
            }
            dyn_block_set_used(block, used - (sum - pos));
            sum = pos; // for exit for_loop
        }
    }
}

// Pushes an object to an mtr memo stack
inline mtr_memo_slot_t* mtr_memo_push(mtr_t *mtr,	/*!< in: mtr */
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

    /* If this mtr has x-fixed a clean page or a resident page,
       then we set the made_dirty flag. This tells us if we need to
       grab log_flush_order_mutex at mtr_commit so that we
       can insert the dirtied page to the flush list. */
    if (!mtr->made_dirty && (type == MTR_MEMO_PAGE_X_FIX || ((const buf_block_t*)object)->is_resident())) {
        mtr->made_dirty = mtr_block_dirtied((const buf_block_t*)object);
    }

    memo = &(mtr->memo);
    slot = (mtr_memo_slot_t*) dyn_array_push(memo, sizeof(mtr_memo_slot_t));
    slot->object = object;
    slot->type = type;

    return slot;
}

// Releases an object in the memo stack
inline bool32 mtr_memo_release(mtr_t* mtr, void* object, uint32 type)
{
    ut_ad(mtr->magic_n == MTR_MAGIC_N);
    ut_ad(mtr->state == MTR_ACTIVE);

    for (dyn_block_t* block = dyn_array_get_last_block(&mtr->memo);
         block;
         block = dyn_array_get_prev_block(&mtr->memo, block)) {

        uint32 used = dyn_block_get_used(block);
        ut_ad(!(used % sizeof(mtr_memo_slot_t)));

        const mtr_memo_slot_t* start = (mtr_memo_slot_t*)dyn_block_get_data(block);
        mtr_memo_slot_t* slot  = (mtr_memo_slot_t*)(dyn_block_get_data(block) + used);

        uint32 count = 0;
        while (slot-- != start) {
            if (object == slot->object && type == slot->type) {
                mtr_memo_slot_release(mtr, slot);

                if (count > 0) {
                    memmove((char *)slot, (char *)(slot + 1), count * sizeof(mtr_memo_slot_t));
                }
                dyn_block_set_used(block, used - sizeof(mtr_memo_slot_t));

                if (used - sizeof(mtr_memo_slot_t) == 0) {
                    dyn_array_release_block(&mtr->memo, block);
                }
                return TRUE;
            }
            count++;
        }
    }

    return FALSE;
}


// Releases the mlocks and other objects stored in an mtr memo.
// They are released in the order opposite to which they were pushed to the memo.
static inline void mtr_memo_pop_all(mtr_t *mtr)
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

inline uint32 mtr_get_log_mode(mtr_t *mtr)
{
    ut_ad(mtr);
    ut_ad(mtr->log_mode >= MTR_LOG_ALL);
    ut_ad(mtr->log_mode <= MTR_LOG_SHORT_INSERTS);

    return(mtr->log_mode);
}

// Append the dirty pages to the flush list
static inline void mtr_add_dirtied_pages_to_flush_list(mtr_t* mtr)
{
    ut_ad(!srv_read_only_mode);

    if (!mtr->modifications) {
        return;
    }

    ut_ad(mtr->magic_n == MTR_MAGIC_N);
    ut_ad(mtr->state == MTR_COMMITTING); /* Currently only used in commit */

    dyn_array_t* memo = &mtr->memo;
    uint32 offset = dyn_array_get_data_size(memo);

    //if (mtr->made_dirty) {
    //    log_flush_order_mutex_enter();
    //}

    while (offset > 0) {
        mtr_memo_slot_t* slot;
        buf_block_t* block;

        offset -= sizeof(mtr_memo_slot_t);
        slot = (mtr_memo_slot_t*)dyn_array_get_element(memo, offset);

        block = (buf_block_t*)slot->object;
        if (block != NULL && (slot->type == MTR_MEMO_PAGE_X_FIX || block->is_resident())) {
            buf_flush_note_modification(block, mtr);
        }
    }

    //if (mtr->made_dirty) {
    //    log_flush_order_mutex_exit();
    //}
}

//Writes the contents of a mini-transaction log, if any, to the database log.
static inline void mtr_log_reserve_and_write(mtr_t *mtr)
{
    ut_ad(!srv_read_only_mode);

    dyn_array_t* mlog = &(mtr->log);

    // set rec flag
    dyn_block_t* first_block = dyn_array_get_first_block(mlog);
    if (mtr->n_log_recs > 1) {
        mlog_catenate_uint32(mtr, MLOG_MULTI_REC_END, MLOG_1BYTE);
    } else {
        byte* mlog_id = dyn_block_get_data(first_block) + 4 /* reserved 4bytes for length */;
        *mlog_id = *mlog_id | MLOG_SINGLE_REC_FLAG;
    }

    // alloc from log buffer
    uint32 data_size = dyn_array_get_data_size(mlog);
    ut_ad(data_size > 2);
    ut_a(data_size <= UINT_MAX16);
    log_buffer_reserve(&mtr->start_buf_lsn, data_size);
    mtr->end_lsn = mtr->start_buf_lsn.val.lsn + mtr->start_buf_lsn.data_len;

    // length for log
    mach_write_to_2(dyn_block_get_data(first_block), data_size - 2);

    // add dirtied pages to flush list
    mtr_add_dirtied_pages_to_flush_list(mtr);

    // copy data to log buffer
    if (mtr->log_mode == MTR_LOG_ALL) {
        uint64 start_lsn = mtr->start_buf_lsn.val.lsn;
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
}

/*
 * Writes the initial part of a log record (3..11 bytes).
 * If the implementation of this function is changed,
 * all size parameters to mlog_open() should be adjusted accordingly!
 * return: new value of log_ptr
 */
inline byte* mlog_write_initial_log_record_fast(
    const byte* ptr,    /*!< in: pointer to (inside) a buffer frame holding the file page where modification is made */
    mlog_id_t   type,   /*!< in: log item type: MLOG_1BYTE, ... */
    byte*       log_ptr,/*!< in: pointer to mtr log which has been opened */
    mtr_t*      mtr)    /*!< in: mtr */
{
    const byte*  page;
    uint32       space;
    uint32       offset;

    ut_ad(type <= MLOG_BIGGEST_TYPE);
    ut_ad(ptr && log_ptr);
#ifdef UNIV_DEBUG
    buf_block_t* block = (buf_block_t*) buf_block_align(ptr);
    if (block->is_resident()) {
        // refcount == 0 if page is redident
    } else {
        ut_ad(mtr_memo_contains_page(mtr, ptr, MTR_MEMO_BUF_FIX));
        ut_ad(mtr_memo_contains_page(mtr, ptr, MTR_MEMO_PAGE_X_FIX));
    }
#endif

    page = (const byte*) ut_align_down((void *)ptr, UNIV_PAGE_SIZE);
    space = mach_read_from_4(page + FIL_PAGE_SPACE);
    offset = mach_read_from_4(page + FIL_PAGE_OFFSET);

    mach_write_to_1(log_ptr, type);
    log_ptr++;
    log_ptr += mach_write_compressed(log_ptr, space);
    log_ptr += mach_write_compressed(log_ptr, offset);

    mtr->n_log_recs++;

#ifdef UNIV_DEBUG1
    /* We now assume that all x-latched pages have been modified! */
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
inline void mlog_write_initial_log_record(
    const byte *ptr, /*!< in: pointer to (inside) a buffer frame holding the file page where modification is made */
    mlog_id_t type, /*!< in: log item type: MLOG_1BYTE, ... */
    mtr_t *mtr) /*!< in: mini-transaction handle */
{
    byte* log_ptr;

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
inline uint32 mlog_read_uint32(const byte* ptr, mlog_id_t type)
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
inline void mlog_write_uint32(
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

	if (mtr != NULL) {
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
inline void mlog_write_uint64(
	byte*		ptr,	/*!< in: pointer where to write */
	uint64      val,	/*!< in: value to write */
	mtr_t*		mtr)	/*!< in: mini-transaction handle */
{
	mach_write_to_8(ptr, val);

	if (mtr != NULL) {
		byte*	log_ptr = mlog_open(mtr, 11 + 2 + 9);

		/* If no logging is requested, we may return now */
		if (log_ptr != NULL) {
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
inline void mlog_write_string(
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
inline void mlog_catenate_string(
    mtr_t      *mtr,    /*!< in: mtr */
    byte       *str,    /*!< in: string to write */
    uint32      len)    /*!< in: string length */
{
    dyn_array_t*    mlog;

    if (str == NULL || len == 0 || mtr_get_log_mode(mtr) == MTR_LOG_NONE) {
        return;
    }

    mlog = &(mtr->log);

    dyn_push_string(mlog, str, len);
}

/********************************************************//**
Catenates 1 - 4 bytes to the mtr log. The value is not compressed. */
inline void mlog_catenate_uint32(mtr_t *mtr,
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


// Catenates a compressed ulint to mlog.
inline void mlog_catenate_uint32_compressed(mtr_t* mtr, uint32 val)
{
    byte* log_ptr;

    log_ptr = mlog_open(mtr, 10);

    /* If no logging is requested, we may return now */
    if (log_ptr == NULL) {
        return;
    }

    log_ptr += mach_write_compressed(log_ptr, val);

    mlog_close(mtr, log_ptr);
}

// Catenates a 64-bit integer to mlog.
inline void mlog_catenate_uint64(mtr_t* mtr, uint64 val)
{
    dyn_array_t *mlog;
    byte *ptr;

    if (mtr_get_log_mode(mtr) == MTR_LOG_NONE) {
        return;
    }

    mlog = &(mtr->log);
    ptr = (byte*) dyn_array_push(mlog, MLOG_8BYTES);
    mach_write_to_8(ptr, val);
}

// Catenates a compressed 64-bit integer to mlog.
inline void mlog_catenate_uint64_compressed(mtr_t* mtr, uint64 val)
{
    byte* log_ptr;

    log_ptr = mlog_open(mtr, 15);

    /* If no logging is requested, we may return now */
    if (log_ptr == NULL) {
        return;
    }

    log_ptr += mach_ull_write_compressed(log_ptr, val);

    mlog_close(mtr, log_ptr);
}




/********************************************************//**
Logs a write of a string to a file page buffered in the buffer pool.
Writes the corresponding log record to the mini-transaction log. */
inline void mlog_log_string(
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

// Writes a log record
inline void mlog_write_log(uint32 type, uint32 space_id, uint32 page_no,
    byte  *str, /*!< in: string to write */
    uint32 len, /*!< in: string length */
    mtr_t* mtr)
{
    byte* log_ptr;

    log_ptr = mlog_open(mtr, 11);
    if (log_ptr == NULL) {
        return;
    }

    mach_write_to_1(log_ptr, type);
    log_ptr++;

    /* We write dummy space id and page number */
    log_ptr += mach_write_compressed(log_ptr, space_id);
    log_ptr += mach_write_compressed(log_ptr, page_no);

    mtr->n_log_recs++;

    mlog_close(mtr, log_ptr);

    mlog_catenate_string(mtr, str, len);
}

// Reads 1 - 4 bytes from a file page buffered in the buffer pool.
inline uint32 mtr_read_uint32(
    const byte* ptr,    /*!< in: pointer from where to read */
    mlog_id_t   type,   /*!< in: MLOG_1BYTE, MLOG_2BYTES, MLOG_4BYTES */
    mtr_t*      mtr)    /*!< in: mini-transaction handle */
{
    ut_ad(mtr->state == MTR_ACTIVE);
    ut_ad(mtr_memo_contains_page(mtr, ptr, MTR_MEMO_PAGE_S_FIX)
          || mtr_memo_contains_page(mtr, ptr, MTR_MEMO_PAGE_X_FIX));

    return mlog_read_uint32(ptr, type);
}

inline mtr_t* mtr_start(mtr_t *mtr)
{
    dyn_array_create(&(mtr->memo), mtr_memory_pool);
    dyn_array_create(&(mtr->log), mtr_memory_pool);
    mtr->log.first_block.used = 2; // reserved 2 bytes for length

    mtr->log_mode = MTR_LOG_ALL;
    mtr->modifications = FALSE;
    mtr->n_log_recs = 0;
    mtr->inside_ibuf = FALSE;
    mtr->made_dirty = FALSE;

    ut_d(mtr->state = MTR_ACTIVE);
    ut_d(mtr->magic_n = MTR_MAGIC_N);

    return(mtr);
}

inline void mtr_commit(mtr_t *mtr)
{
    ut_ad(mtr->magic_n == MTR_MAGIC_N);
    ut_ad(mtr->state == MTR_ACTIVE);
    ut_ad(!mtr->inside_ibuf);
    ut_d(mtr->state = MTR_COMMITTING);

    if (mtr->modifications && mtr->n_log_recs) {
        mtr_log_reserve_and_write(mtr);
    }

    mtr_memo_pop_all(mtr);

    dyn_array_free(&(mtr->memo));
    dyn_array_free(&(mtr->log));

    ut_d(mtr->state = MTR_COMMITTED);
}

inline byte* mlog_open(mtr_t *mtr, uint32 size)
{
    dyn_array_t *mlog;

    mtr->modifications = TRUE;

    if (mtr_get_log_mode(mtr) == MTR_LOG_NONE) {
        return(NULL);
    }

    mlog = &(mtr->log);

    return(dyn_array_open(mlog, size));
}

inline void mlog_close(mtr_t *mtr, byte *ptr)
{
    dyn_array_t *mlog;

    ut_ad(mtr_get_log_mode(mtr) != MTR_LOG_NONE);

    mlog = &(mtr->log);

    dyn_array_close(mlog, ptr);
}

inline void mtr_s_lock_func(rw_lock_t* lock, /*!< in: rw-lock */
    const char* file, /*!< in: file name */
    uint32 line, /*!< in: line number */
    mtr_t* mtr) /*!< in: mtr */
{
    ut_ad(mtr);
    ut_ad(lock);

    rw_lock_s_lock(lock);
    mtr_memo_push(mtr, lock, MTR_MEMO_S_LOCK);
}

inline void mtr_x_lock_func(rw_lock_t* lock, /*!< in: rw-lock */
    const char* file, /*!< in: file name */
    uint32 line, /*!< in: line number */
    mtr_t* mtr) /*!< in: mtr */
{
    ut_ad(mtr);
    ut_ad(lock);

    rw_lock_x_lock(lock);
    mtr_memo_push(mtr, lock, MTR_MEMO_X_LOCK);
}


