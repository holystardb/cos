#include "knl_mtr.h"
#include "cm_dbug.h"

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

			log_ptr += mach_u64_write_compressed(log_ptr, val);

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
Logs a write of a string to a file page buffered in the buffer pool.
Writes the corresponding log record to the mini-transaction log. */
void mlog_log_string(
	byte*	ptr,	/*!< in: pointer written to */
	ulint	len,	/*!< in: string length */
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

