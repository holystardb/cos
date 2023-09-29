#include "knl_mtr.h"
#include "cm_dbug.h"
#include "cm_util.h"
#include "knl_buf.h"
#include "knl_page.h"
#include "knl_server.h"

#define MTR_POOL_COUNT          16
memory_pool_t           mtr_pools[MTR_POOL_COUNT];

static volatile uint32  n_mtr_pools_index = 0;

bool32 mtr_init(memory_area_t *area)
{
    uint32 local_page_count = 128;  // 1MB
    uint32 max_page_count = -1;  // unlimited
    uint32 page_size = DYN_ARRAY_BLOCK_SIZE * 16;

    for (uint32 i = 0; i < MTR_POOL_COUNT; i++) {
        //mtr_pools[i] = mpool_create(area, local_page_count, max_page_count, page_size);
        //if (mtr_pools[i] == NULL) {
        //    return FALSE;
        //}
    }

    return TRUE;
}

dyn_array_t* dyn_array_create(dyn_array_t *arr)	/* in: pointer to a memory buffer of size sizeof(dyn_array_t) */
{
    uint32 idx = n_mtr_pools_index++;
    arr->pool = &mtr_pools[idx & 0x0F];
    UT_LIST_INIT(arr->pages);
    UT_LIST_INIT(arr->blocks);
    arr->current_page_used = 0;

    return arr;
}

void dyn_array_free(dyn_array_t *arr)
{
    memory_page_t * page;

    page = UT_LIST_GET_FIRST(arr->pages);
    while (page) {
        UT_LIST_REMOVE(list_node, arr->pages, page);
        //mpool_free_page(pool, page);
        page = UT_LIST_GET_FIRST(arr->pages);
    }

    arr->current_page_used = 0;
}

dyn_block_t* dyn_array_add_block(dyn_array_t *arr)
{
    dyn_block_t *block;
    memory_page_t *page;

    page = UT_LIST_GET_LAST(arr->pages);
    if (page == NULL || arr->current_page_used + DYN_ARRAY_BLOCK_SIZE > arr->pool->page_size) {
        page = mpool_alloc_page(arr->pool);
        if (page == NULL) {
            return NULL;
        }
        arr->current_page_used = 0;
        UT_LIST_ADD_LAST(list_node, arr->pages, page);
    }

    block = (dyn_block_t *)((char *)page + ut_align8(sizeof(memory_page_t)) + arr->current_page_used);
    arr->current_page_used += DYN_ARRAY_BLOCK_SIZE;
    block->used = 0;
    UT_LIST_ADD_LAST(list_node, arr->blocks, block);

    return block;
}


byte* dyn_array_open(dyn_array_t *arr, uint32 size)
{
    dyn_block_t *block;

    ut_ad(size <= DYN_ARRAY_DATA_SIZE);
    ut_ad(size);

    block = UT_LIST_GET_LAST(arr->blocks);
    if (block == NULL || block->used + size > DYN_ARRAY_DATA_SIZE) {
        block = dyn_array_add_block(arr);
        if (block == NULL) {
            return NULL;
        }
    }

    ut_ad(block->used <= DYN_ARRAY_DATA_SIZE);

    return block->data + block->used;
}

void dyn_array_close(dyn_array_t *arr, byte *ptr)
{
    dyn_block_t *block;

    block = UT_LIST_GET_LAST(arr->blocks);
    block->used = ptr - block->data;

    ut_ad(block->used <= DYN_ARRAY_DATA_SIZE);
}

void* dyn_array_push(dyn_array_t *arr, uint32 size)
{
    dyn_block_t *block;
    uint32 used;

    ut_ad(size <= DYN_ARRAY_DATA_SIZE);
    ut_ad(size);

    block = UT_LIST_GET_LAST(arr->blocks);
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

    /* Get the first array block */
    block = UT_LIST_GET_FIRST(arr->blocks);
    ut_ad(block);
    used = block->used;

    while (pos >= used) {
        pos -= used;
        block = UT_LIST_GET_NEXT(list_node, block);
        ut_ad(block);
        used = block->used;
    }

    ut_ad(block->used >= pos);

    return block->data + pos;
}

uint32 dyn_array_get_data_size(dyn_array_t *arr)
{
    dyn_block_t *block;
    uint32 sum = 0;

    /* Get the first array block */
    block = UT_LIST_GET_FIRST(arr->blocks);
    while (block != NULL) {
        sum += block->used;
        block = UT_LIST_GET_NEXT(list_node, block);
    }

    return sum;
}


mtr_t* mtr_start(mtr_t *mtr)
{
    dyn_array_create(&(mtr->memo));
    dyn_array_create(&(mtr->log));

    mtr->log_mode = MTR_LOG_ALL;
    mtr->modifications = FALSE;
    mtr->n_log_recs = 0;

    return(mtr);
}

void mtr_commit(mtr_t *mtr)
{
}

byte* mlog_open(mtr_t *mtr, uint32 size)
{
    return NULL;
}

void mlog_close(mtr_t *mtr, byte *ptr)
{
}

byte* mlog_write_initial_log_record_fast(
    const byte* ptr,    /*!< in: pointer to (inside) a buffer frame holding the file page where modification is made */
    mlog_id_t   type,   /*!< in: log item type: MLOG_1BYTE, ... */
    byte*       log_ptr,/*!< in: pointer to mtr log which has been opened */
    mtr_t*      mtr)    /*!< in: mtr */
{
    return NULL;
}

/********************************************************//**
Writes the initial part of a log record consisting of one-byte item
type and four-byte space and page numbers. Also pushes info
to the mtr memo that a buffer page has been modified. */
void mlog_write_initial_log_record(
	const byte*	ptr,	/*!< in: pointer to (inside) a buffer
				frame holding the file page where
				modification is made */
	mlog_id_t	type,	/*!< in: log item type: MLOG_1BYTE, ... */
	mtr_t*		mtr)	/*!< in: mini-transaction handle */
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

/********************************************************//**
Catenates n bytes to the mtr log. */
void mlog_catenate_string(
    mtr_t*      mtr,    /*!< in: mtr */
    const byte* str,    /*!< in: string to write */
    uint32      len)    /*!< in: string length */
{
//    if (mtr_get_log_mode(mtr) == MTR_LOG_NONE) {
//        return;
//    }
//    mtr->get_log()->push(str, ib_uint32_t(len));
}


/********************************************************//**
Logs a write of a string to a file page buffered in the buffer pool.
Writes the corresponding log record to the mini-transaction log. */
void mlog_log_string(
	byte*	ptr,	/*!< in: pointer written to */
	uint32	len,	/*!< in: string length */
	mtr_t*	mtr)	/*!< in: mini-transaction handle */
{
	byte*	log_ptr;

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


/***************************************************//**
Checks if a mini-transaction is dirtying a clean page.
@return TRUE if the mtr is dirtying a clean page. */
bool32 mtr_block_dirtied(const buf_block_t *block) /*!< in: block being x-fixed */
{
    ut_ad(buf_block_get_state(block) == BUF_BLOCK_FILE_PAGE);
    ut_ad(block->page.buf_fix_count > 0);

    /* It is OK to read oldest_modification because no
    other thread can be performing a write of it and it
    is only during write that the value is reset to 0. */
    return(block->page.oldest_modification == 0);
}

void mtr_t::memo_push(void* object, uint32 type) /*!< in: object type: MTR_MEMO_S_LOCK, ... */
{
	mtr_memo_slot_t* slot;

	ut_ad(object);
	ut_ad(type >= MTR_MEMO_PAGE_S_FIX);
	ut_ad(type <= MTR_MEMO_X_LOCK);
	ut_ad(magic_n == MTR_MAGIC_N);
	ut_ad(state == MTR_ACTIVE);

	/* If this mtr has x-fixed a clean page then we set
	the made_dirty flag. This tells us if we need to
	grab log_flush_order_mutex at mtr_commit so that we
	can insert the dirtied page to the flush list. */
	if (type == MTR_MEMO_PAGE_X_FIX && !made_dirty) {
		made_dirty = mtr_block_dirtied((const buf_block_t*) object);
	}

	slot = (mtr_memo_slot_t*) dyn_array_push(&memo, sizeof *slot);
	slot->object = object;
	slot->type = type;
}


void mtr_t::s_lock(rw_lock_t* lock, const char* file, uint32 line)
{
    rw_lock_s_lock(lock);
    memo_push(lock, MTR_MEMO_S_LOCK);
}

void mtr_t::x_lock(rw_lock_t* lock, const char* file, uint32 line)
{
    rw_lock_x_lock(lock);
    memo_push(lock, MTR_MEMO_X_LOCK);
}

/********************************************************//**
Reads 1 - 4 bytes from a file page buffered in the buffer pool.
@return	value read */
uint32 mtr_read_uint(
	const byte*	ptr,	/*!< in: pointer from where to read */
	mlog_id_t	type,	/*!< in: MLOG_1BYTE, MLOG_2BYTES, MLOG_4BYTES */
	mtr_t*		mtr)    /*!< in: mini-transaction handle */
{
	ut_ad(mtr->state == MTR_ACTIVE);
	ut_ad(mtr_memo_contains_page(mtr, ptr, MTR_MEMO_PAGE_S_FIX)
	      || mtr_memo_contains_page(mtr, ptr, MTR_MEMO_PAGE_X_FIX));

	return mlog_read_uint32(ptr, type);
}


