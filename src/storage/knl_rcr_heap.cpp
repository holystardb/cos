#include "knl_heap.h"
#include "cm_log.h"
#include "knl_heap_fsm.h"
#include "knl_trx.h"
#include "knl_trx_undo.h"
#include "knl_trx_rseg.h"

#define HEAP_TOAST_CHUNK_DATA_MAX_SIZE    4000
#define HEAP_TOAST_DATA_MAX_SIZE          0x40000000  // 1GB


const uint16 HEAP_ROW_MAX_SIZE =
    (UNIV_PAGE_SIZE - FIL_PAGE_DATA - FIL_PAGE_DATA_END - sizeof(row_dir_t) - sizeof(itl_t) - sizeof(row_id_t));
#define HEAP_MAX_CHAIN_COUNT 18


typedef struct st_heap_entry {
    uint32 space;
    uint32 map_root_page;
} heap_entry_t;


// Create the root node for a new index tree.
uint32 heap_create_entry(uint32 space_id)
{
    mtr_t mtr;
    status_t err;
    uint32 page_no = fsm_create(space_id);
    printf("\n\n\n\n\n");

    mtr_start(&mtr);

    const page_size_t page_size(space_id);
    buf_block_t* block[8] = {NULL};
    for (uint32 i = 0; i < 8; i++) {
        err = fsp_alloc_free_page(space_id, page_size, Page_fetch::NORMAL, &block[i], &mtr);
        if (err != CM_SUCCESS) {
            //LOG_ERROR(LOGGER,
            //          "failed to create heap entry of table, space id %u table name %s",
            //          space_id, table_name);
            goto err_exit;
        }
    }

    for (uint32 i = 0; i < 8; i++) {
        fsm_add_free_page(space_id, page_no, block[i]->get_page_no(), &mtr);
    }

    mtr_commit(&mtr);

    return page_no;

err_exit:

    for (uint32 i = 0; i < 8; i++) {
        const page_id_t page_id(space_id, block[i]->get_page_no());
        fsp_free_page(page_id, page_size, &mtr);
    }

    mtr.modifications = FALSE;
    mtr_commit(&mtr);

    return page_no;
}

static inline itl_t* heap_get_itl(page_t* page, uint8 itl_id)
{
    if (itl_id == HEAP_INVALID_ITL_ID) {
        return NULL;
    }

    heap_page_header_t* hdr = page + HEAP_HEADER_OFFSET;
    uint32 itl_count = mach_read_from_2(hdr + HEAP_HEADER_ITLS);
    if (itl_count < itl_id) {
        // panic
        return NULL;
    }

    uint16 upper = mach_read_from_2(hdr + HEAP_HEADER_UPPER);
    return (itl_t *)(page + upper + itl_id * sizeof(itl_t));
}

inline row_header_t* heap_get_row_by_dir(page_t* page, row_dir_t* dir)
{
    return (row_header_t *)(page + dir->offset);
}

static inline row_dir_t *heap_get_dir(page_t* page, uint32 slot)
{
    uint32 offset;

    offset = UNIV_PAGE_SIZE - FIL_PAGE_DATA_END;
    offset -= mach_read_from_2(page + HEAP_HEADER_OFFSET + HEAP_HEADER_ITLS) * sizeof(itl_t);
    offset -= (slot + 1) * sizeof(row_dir_t);

    return (row_dir_t *)(page + offset);
}

inline void row_dir_init(row_dir_t* dir)
{
    dir->scn = 0;
    dir->undo_rollptr = (FIL_NULL << 16);
    dir->offset = (1 << 15);
    //dir->undo_page_no = FIL_NULL;
    //dir->undo_page_offset = 0;
    //dir->is_free = 1;
    //dir->is_ow_scn = 0;
    //dir->is_lob_part = 0;

}

static inline row_dir_t* heap_alloc_dir(buf_block_t* block, uint32* dir_slot, mtr_t* mtr)
{
    row_dir_t* dir;
    mlog_id_t mlog_type;
    page_t* page = buf_block_get_frame(block);
    heap_page_header_t* hdr = page + HEAP_HEADER_OFFSET;

    *dir_slot = mach_read_from_2(hdr + HEAP_HEADER_FIRST_FREE_DIR);
    if (*dir_slot == HEAP_NO_FREE_DIR) {
#ifdef UNIV_DEBUG
        uint16 upper = mach_read_from_2(hdr + HEAP_HEADER_UPPER);
        uint16 lower = mach_read_from_2(hdr + HEAP_HEADER_LOWER);
        ut_ad(upper >= lower);
        ut_ad(upper - lower >= sizeof(row_dir_t));
#endif
        uint16 offset = (uint16)mach_read_from_2(hdr + HEAP_HEADER_UPPER) - sizeof(row_dir_t);
        mach_write_to_2(hdr + HEAP_HEADER_UPPER, offset, MLOG_2BYTES);

        *dir_slot = mach_read_from_2(hdr + HEAP_HEADER_DIRS);
        mach_write_to_2(hdr + HEAP_HEADER_DIRS, *dir_slot + 1, MLOG_2BYTES);
        dir = heap_get_dir(page, *dir_slot);
        row_dir_init(dir);

        mlog_type = MLOG_HEAP_NEW_DIR;
    } else {
        dir = heap_get_dir(page, *dir_slot);
        row_dir_init(dir);
        mach_write_to_2(hdr + HEAP_HEADER_FIRST_FREE_DIR, dir->free_next_dir, MLOG_2BYTES);
        mlog_type = MLOG_HEAP_ALLOC_DIR;
    }

    // redo
    if (mtr) {
        const uint32 buf_size = 2;
        byte buf[buf_size];
        mach_write_to_2(buf, *dir_slot);
        mlog_write_log(mlog_type, block->get_space_id(), block->get_page_no(), buf, buf_size, mtr);
    }

    return dir;
}

static inline void heap_free_dir(buf_block_t* block, row_dir_t* dir, uint32 dir_slot, mtr_t* mtr)
{
    heap_page_header_t* hdr = buf_block_get_frame(block) + HEAP_HEADER_OFFSET;
    dir->free_next_dir = mach_read_from_2(hdr + HEAP_HEADER_FIRST_FREE_DIR);
    dir->is_free = 1;
    mach_write_to_2(hdr + HEAP_HEADER_FIRST_FREE_DIR, dir_slot, MLOG_2BYTES);

    // redo
    if (mtr) {
        const uint32 buf_size = 2;
        byte buf[buf_size];
        mach_write_to_2(buf, dir_slot);
        mlog_write_log(MLOG_HEAP_FREE_DIR, block->get_space_id(), block->get_page_no(), buf, buf_size, mtr);
    }
}

inline uint8 heap_row_get_itl_id(row_header_t* row)
{
    return mach_read_from_1((uchar*)row + HEAP_TUPLE_HEADER_ITL);
}

inline void heap_row_set_itl_id(row_header_t* row, uint8 itl_id)
{
    mach_write_to_1((uchar*)row + HEAP_TUPLE_HEADER_ITL, itl_id);
}

static void heap_reorganize_page(que_sess_t* sess, buf_block_t* block, mtr_t* mtr)
{
    row_dir_t* dir;
    row_header_t* row;
    page_t* page = buf_block_get_frame(block);
    heap_page_header_t* header = page + HEAP_HEADER_OFFSET;
    uint32 dir_count = mach_read_from_2(header + HEAP_HEADER_DIRS);

    for (uint32 i = 0; i < dir_count; i++) {
        dir = heap_get_dir(page, i);
        if (dir->is_free) {
            continue;
        }

        row = HEAP_GET_ROW(page, dir);
        if (row->is_deleted) {
            itl_t* itl = heap_get_itl(page, heap_row_get_itl_id(row));
            // itl == NULL: commited, itl is reused
            if (itl == NULL) {
                heap_row_set_itl_id(row, HEAP_INVALID_ITL_ID);
                dir->is_free = 1;
                dir->free_next_dir = mach_read_from_2(header + HEAP_HEADER_FIRST_FREE_DIR);
                mach_write_to_2(header + HEAP_HEADER_FIRST_FREE_DIR, i);
                continue;
            }
            // itl->is_active == TRUE: commited, but itl is not reused
            if (!itl->is_active && itl->scn < sess->min_query_scn) {
                ut_ad(itl->trx_slot_id.id != TRANSACTION_INVALID_ID);
                heap_row_set_itl_id(row, HEAP_INVALID_ITL_ID);
                dir->scn = itl->scn;
                dir->is_ow_scn = itl->is_ow_scn;
                dir->is_free = 1;
                dir->free_next_dir = mach_read_from_2(header + HEAP_HEADER_FIRST_FREE_DIR);
                mach_write_to_2(header + HEAP_HEADER_FIRST_FREE_DIR, i);
                continue;
            }
        }

        // temporarily save row slot to row->slot, so we can use row itself to find it's dir,
        // we can temporarily use the high bits(0x8000) as reorganizing flag
        dir->offset = row->slot;
        ut_ad(i <= (~ROW_REORGANIZING_FLAG));
        row->slot = (i | ROW_REORGANIZING_FLAG);
    }

    // traverse row one by one from first row after heap page head
    uint16 lower = mach_read_from_2(header + HEAP_HEADER_LOWER);
    // get first row
    row = (row_header_t *)((char *)page + HEAP_HEADER_SIZE);
    row_header_t* free_addr = row;
    while ((char *)row < (char *)page + lower) {
        if ((row->slot & ROW_REORGANIZING_FLAG) == 0) {
            // row has been deleted, compact it's space directly
            row = (row_header_t *)((char *)row + row->size);
            continue;
        }

        // don't clear the reorganizing flag here, just get the actual row slot
        uint16 slot = (row->slot & ~ROW_REORGANIZING_FLAG);
        dir = heap_get_dir(page, row->slot);

        // move current row to the new position
        uint16 origin_row_size = row->size;
        uint16 copy_size = row->size;
        if (row->is_migrate) {
            itl_t* itl = heap_get_itl(page, heap_row_get_itl_id(row));
            // if transactoin is in progress, it's free space cannot be compacted
            if (!itl->is_active && row->size != HEAP_MIN_ROW_SIZE) {
                copy_size = HEAP_MIN_ROW_SIZE;
                // if transaction is committed and row is migrated, we compact it's space
                row->size = HEAP_MIN_ROW_SIZE;
            }
        }
        if (row != free_addr && copy_size != 0) {
            memmove(free_addr, row, copy_size);
        }

        /* restore the row and its dir */
        free_addr->size = copy_size;
        free_addr->slot = (dir->offset);
        dir->offset = (uint16)((char *)free_addr - (char *)page);

        // now, handle the next row
        free_addr = (row_header_t *)((char *)free_addr + free_addr->size);
        row = (row_header_t *)((char *)row + origin_row_size);
    }

    // reset the latest page free begin position, free_addr - page is less than UNIV_PAGE_SIZE_DEF
    ut_ad((char *)free_addr - (char *)page) < UNIV_PAGE_SIZE_DEF);
    mach_write_to_2(header + HEAP_HEADER_LOWER, (uint16)((char *)free_addr - (char *)page));

    // write redo log
    if (mtr) {
        mlog_write_log(MLOG_PAGE_REORGANIZE, block->get_space_id(), block->get_page_no(), NULL, 0, mtr);
    }
}

static itl_t* heap_create_itl(buf_block_t* block, uint8* itl_id, mtr_t* mtr)
{
    page_t* page = buf_block_get_frame(block);
    heap_page_header_t* header = HEAP_PAGE_GET_HEADER(page);

    uint16 free_size = mach_read_from_2(header + HEAP_HEADER_FREE_SIZE);
    uint16 itl_count = mach_read_from_2(header + HEAP_HEADER_ITLS);
    if (itl_count >= HEAP_PAGE_MAX_ITLS || free_size < sizeof(itl_t)) {
        return NULL;
    }

    uint16 lower = mach_read_from_2(header + HEAP_HEADER_LOWER);
    uint16 upper = mach_read_from_2(header + HEAP_HEADER_UPPER);
    uint16 dir_count = mach_read_from_2(header + HEAP_HEADER_DIRS);
    if (lower + sizeof(itl_t) > upper) {
        heap_reorganize_page(block, mtr);
    }

    if (dir_count > 0) {
        byte* dest = page + upper - sizeof(itl_t);
        memmove(dest, page + upper, dir_count * sizeof(row_dir_t));
    }

    itl_t* itl = (itl_t *)(page + upper + dir_count * sizeof(row_dir_t));
    memset(itl, 0x00, sizeof(itl_t));
    itl->trx_slot_id.id = TRANSACTION_INVALID_ID;
    *itl_id = itl_count;

    mach_write_to_2(header + HEAP_HEADER_ITLS, itl_count + 1);
    mach_write_to_2(header + HEAP_HEADER_UPPER, upper - sizeof(itl_t));
    mach_write_to_2(header + HEAP_HEADER_FREE_SIZE, free_size - sizeof(itl_t));

    // write redo log
    mlog_write_log(MLOG_HEAP_NEW_ITL, block->get_space_id(), block->get_page_no(), NULL, 0, mtr);
    mlog_catenate_uint32(mtr, *itl_id, MLOG_1BYTE);

    return itl;
}

static void heap_reuse_itl(buf_block_t* block, itl_t* itl, uint8 itl_id, mtr_t* mtr)
{
    page_t* page = buf_block_get_frame(block);
    heap_page_header_t* header = HEAP_PAGE_GET_HEADER(page);
    uint32 dir_count = mach_read_from_2(header + HEAP_HEADER_DIRS);

    for (uint32 i = 0; i < dir_count; i++) {
        row_dir_t* dir = heap_get_dir(page, i);
        if (dir->is_free) {
            continue;
        }

        row_header_t* row = heap_get_row_by_dir(page, dir);
        if (heap_row_get_itl_id(row) != itl_id) {
            continue;
        }

        heap_row_set_itl_id(row, HEAP_INVALID_ITL_ID);
        dir->scn = itl->scn;
        dir->is_ow_scn = itl->is_ow_scn;
        //if (row->is_changed == FALSE) {
        //    row->is_changed = TRUE;
        //}
    }
    //
    itl->trx_slot_id.id = TRANSACTION_INVALID_ID;

    // redo
    const uint32 buf_size = 10;
    byte buf[buf_size];
    mach_write_to_1(buf, itl_id);
    mach_write_to_8(buf + 1, itl->scn);
    mach_write_to_1(buf + 9, itl->is_ow_scn);
    mlog_write_log(MLOG_HEAP_REUSE_ITL, block->get_space_id(), block->get_page_no(), buf, buf_size, mtr);
}

static itl_t* heap_alloc_itl(buf_block_t* block, trx_t* trx, mtr_t* mtr, uint8* itl_id)
{
    itl_t *ret_itl = NULL, *tmp_itl;
    bool32 is_reused_itl = FALSE;
    page_t* page = buf_block_get_frame(block);
    heap_page_header_t* header = page + HEAP_HEADER_OFFSET;
    uint32 itl_count = mach_read_from_2(header + HEAP_HEADER_ITLS);

    ut_a(itl_id);

    for (uint32 i = 0; i < itl_count; i++) {
        tmp_itl = heap_get_itl(page, i);
        if (tmp_itl->trx_slot_id.id == trx->trx_slot_id.id) {
            // the current transaction already has an ITL.
            *itl_id = i;
            return tmp_itl;
        }

        if (!tmp_itl->is_active && tmp_itl->trx_slot_id.id == TRANSACTION_INVALID_ID) {
            if (ret_itl == NULL) {
                ret_itl = tmp_itl;
                *itl_id = i;
            }
            // We find an available ITL, but we still need to continue searching to
            // check if the current transaction already has an ITL.
            continue;
        } else if (ret_itl) {
            // We have obtained a usable ITL,
            // so there is no need to check the transaction status of tmp_itl.
            continue;
        }

        //
        trx_status_t trx_status;
        trx_get_status_by_itl(tmp_itl->trx_slot_id, &trx_status);
        if (trx_status.status != XACT_END) {
            continue;
        }
        tmp_itl->is_active = FALSE;
        tmp_itl->scn = trx_status.scn;
        tmp_itl->is_ow_scn = trx_status.is_ow_scn;
        heap_reuse_itl(page, tmp_itl, i, mtr);

        ut_ad(ret_itl == NULL);
        ret_itl = tmp_itl;
        *itl_id = i;
    }

    if (ret_itl == NULL) {
        // No available or reusable ITLs were found, a new one needs to be allocated.
        ret_itl = heap_create_itl(block, itl_id, mtr);
    } else {
        memset(ret_itl, 0x00, sizeof(itl_t));
        ret_itl->trx_slot_id.id = TRANSACTION_INVALID_ID;
    }

    return ret_itl;
}

inline void heap_set_itl_trx_end(buf_block_t* block,
    trx_slot_id_t slot_id, uint8 itl_id, uint64 scn, mtr_t* mtr)
{
    ut_ad(rw_lock_own(&block->rw_lock, RW_X_LATCH));

    page_t* page = buf_block_get_frame(block);

    itl_t* itl = heap_get_itl(page, itl_id);
    ut_a(itl);
    ut_a(itl->trx_slot_id.id == slot_id.id);
    ut_a(itl->is_active);
    itl->scn = scn;
    itl->is_active = FALSE;
    itl->is_ow_scn = FALSE;

    uint16 free_size = mach_read_from_2(page + HEAP_HEADER_OFFSET + HEAP_HEADER_FREE_SIZE);
    if (itl->fsc > 0) {
        // for delete row
        ut_ad(free_size >= itl->fsc);
        mach_write_to_2(page + HEAP_HEADER_OFFSET + HEAP_HEADER_FREE_SIZE, free_size - itl->fsc);
        heap_try_change_map(sess, heap, page_id);
    }

    // redo
    const uint32 buf_size = 11;
    byte buf[buf_size];
    mach_write_to_1(buf, itl_id);
    mach_write_to_8(buf + 1, scn);
    mach_write_to_2(buf + 9, itl->fsc);
    mlog_write_log(MLOG_HEAP_CLEAN_ITL, block->get_space_id(), block->get_page_no(), buf, buf_size, mtr);
}

bool32 heap_lock_row(que_sess_t* session, buf_block_t* block, row_header_t* row, mtr_t* mtr)
{
    itl_t* itl;
    page_t* page = buf_block_get_frame(block);
    heap_page_header_t* header = HEAP_PAGE_GET_HEADER(page);

    if (heap_row_get_itl_id(row) != HEAP_INVALID_ITL_ID) {
        itl = heap_get_itl(page, heap_row_get_itl_id(row));
        if (itl->trx_slot_id.id == session->trx->trx_slot_id.id) {
            // row is locked
            return TRUE;
        }

        trx_status_t trx_status;
        trx_get_status_by_itl(itl->trx_slot_id, &trx_status);
        if (trx_status.status != XACT_END) {
            // wait and retry
        }
    }

    uint8 itl_id;
    itl = heap_alloc_itl(block, session->trx, mtr, &itl_id);
    if (itl == NULL) {
        // wait and retry
    }

    //row_dir_t* dir = heap_get_dir(page, row->slot);
    //heap_row_set_itl_id(row, itl_id);

    return TRUE;
}

uint16 heap_get_insert_size(uint16 row_size, bool32 is_alloc_itl)
{
    uint16 size;

    size = sizeof(row_dir_t) + row_size + is_alloc_itl ? sizeof(itl_t) : 0;

    return size;
}


inline void heap_page_init(buf_block_t* block, dict_table_t* table, mtr_t* mtr)
{
    page_t* page = buf_block_get_frame(block);
    mlog_write_uint32(page + FIL_PAGE_TYPE, FIL_PAGE_TYPE_HEAP, MLOG_2BYTES, mtr);

    uint16 lower = FIL_PAGE_DATA + HEAP_HEADER_SIZE;
    uint16 upper = UNIV_PAGE_SIZE_DEF - FIL_PAGE_DATA_END - sizeof(itl_t) * table->init_trans;
    uchar str[HEAP_HEADER_SIZE];
    mach_write_to_8(str + HEAP_HEADER_LSN, 0);
    mach_write_to_8(str + HEAP_HEADER_SCN, 0);
    mach_write_to_2(str + HEAP_HEADER_LOWER, lower);
    mach_write_to_2(str + HEAP_HEADER_UPPER, upper);
    mach_write_to_2(str + HEAP_HEADER_FREE_SIZE, upper - lower);
    mach_write_to_2(str + HEAP_HEADER_FIRST_FREE_DIR, 0);
    mach_write_to_2(str + HEAP_HEADER_DIRS, 0);
    mach_write_to_2(str + HEAP_HEADER_ROWS, 0);
    mach_write_to_1(str + HEAP_HEADER_ITLS, table->init_trans);
    mlog_write_string(HEAP_HEADER_OFFSET + page, str, HEAP_HEADER_SIZE, mtr);
}

static inline uint32 heap_get_page_free_space(buf_block_t* block)
{
    page_t* page = buf_block_get_frame(block);
    return mach_read_from_1(page + HEAP_HEADER_OFFSET + HEAP_HEADER_FREE_SIZE);
}

static buf_block_t* heap_get_page_for_tuple(dict_table_t* table, uint32 page_no, uint16 row_size, mtr_t* mtr)
{
    const page_id_t page_id(table->space_id, page_no);
    const page_size_t page_size(table->space_id);
    buf_block_t* block = buf_page_get(page_id, page_size, RW_X_LATCH, mtr);
    ut_a(block->get_page_no() == page_no);
    //buf_block_dbg_add_level(block, SYNC_DICT_HEADER);

    page_t* page = buf_block_get_frame(block);
    heap_page_header_t* page_header = page + HEAP_HEADER_OFFSET;
    if (mach_read_from_1(page_header + HEAP_HEADER_FREE_SIZE) < row_size) {
        rw_lock_x_unlock(&(block->rw_lock));
        return NULL;
    }

    return block;
}

static buf_block_t* heap_find_free_page(dict_table_t* table,
    uint16 row_size, fsm_search_path_t& search_path, mtr_t* mtr)
{
    search_path.category = fsm_get_needed_to_category(table, row_size);

retry:

    page_no_t page_no = fsm_search_free_page(table, search_path.category, search_path);
    if (page_no == INVALID_PAGE_NO) {
        return NULL;
    }

    buf_block_t* block = heap_get_page_for_tuple(table, page_no, row_size, mtr);
    if (block == NULL) {
        goto retry;
    }

    return block;
}




// place tuple at specified page
// caller must hold BUFFER_LOCK_EXCLUSIVE on the buffer.
//void RelationPutHeapTuple(Relation relation,
//					 Buffer buffer,
//					 HeapTuple tuple,
//					 bool token)
//{
//
//}


inline status_t heap_check_row_record_size(row_header_t* row, uint32 len)
{
    if (row->size + len > ROW_RECORD_MAX_SIZE) {
        CM_SET_ERROR(ERR_ROW_RECORD_TOO_BIG, row->size + len);
        return CM_ERROR;
    }

    return CM_SUCCESS;
}

#define ROW_NULL_BITS_IN_BYTES(b)   (((b) + 7) / 8)


static status_t heap_insert_row_ext(que_sess_t *sess,
    dict_table_t* table, const dfield_t* field, row_id_t* row_id)
{
    return CM_SUCCESS;
}


// Builds a ROW_FORMAT=COMPACT record out of a data tuple
static inline status_t heap_convert_dtuple_to_rec(que_sess_t* sess,
    dict_table_t* table, dtuple_t* tuple, row_header_t* row)
{
    byte*  nulls_ptr;
    byte*  data;
    uint32 null_bytes = ROW_NULL_BITS_IN_BYTES(table->column_count);

    row->col_count = table->column_count;
    row->size = (uint16)OFFSET_OF(row_header_t, null_bits) + null_bytes;
    row->flag = 0;
    data = (byte*)row + row->size;
    nulls_ptr = (byte*)row + OFFSET_OF(row_header_t, null_bits);
    memset(nulls_ptr, 0x00, null_bytes);

    for (uint32 i = 0; i < tuple->n_fields; i++) {
        const dfield_t* field = tuple->fields + i;
        if (dfield_is_null(field)) {
            nulls_ptr += (i / 8);
            *nulls_ptr |= (1 << (7 - i / 8));
            continue;
        }
        uint32 field_len = dfield_get_len(field);
        uint32 compressed_size = 2;//mach_get_compressed_size(field_len);

        if (dfield_is_ext(field)) {
            CM_RETURN_IF_ERROR(heap_check_row_record_size(row, sizeof(row_id_t)));

            row_id_t row_id;
            CM_RETURN_IF_ERROR(heap_insert_row_ext(sess, table, field, &row_id));

            row->is_ext = TRUE;
            mach_write_to_8(data, row_id.id);
            data += sizeof(row_id_t);
            row->size += sizeof(row_id_t);
            continue;
        }

        // check size of row
        CM_RETURN_IF_ERROR(heap_check_row_record_size(row, field_len + compressed_size));
        // length
        //data += mach_write_compressed(data, field_len);
        mach_write_to_2(data, field_len);
        data += compressed_size;
        // data
        memcpy(data, dfield_get_data(field), field_len);
        data += field_len;
        //
        row->size += compressed_size + field_len;
    }

    return CM_SUCCESS;
}


static inline row_header_t* heap_prepare_insert(que_sess_t* sess, dict_table_t* table, dtuple_t* tuple)
{
    status_t ret;
    row_header_t* row;

    row = (row_header_t*)mcontext_stack_push(sess->mcontext_stack, ROW_RECORD_MAX_SIZE);
    ret = heap_convert_dtuple_to_rec(sess, table, tuple, row);

    return ret == CM_SUCCESS ? row : NULL;
}

void heap_insert_write_redo(buf_block_t* block, row_header_t *row, row_dir_t* dir, uint32 dir_slot, mtr_t* mtr)
{
    const uint32 buf_size = 2 + 2 + sizeof(row_dir_t);
    byte buf[buf_size];

    mach_write_to_2(buf, dir_slot);
    memcpy(buf + 2, (byte *)dir, sizeof(row_dir_t));
    mach_write_to_2(buf + 2 + sizeof(row_dir_t), row->size);

    mlog_write_log(MLOG_HEAP_INSERT, block->get_space_id(), block->get_page_no(), buf, buf_size, mtr);
    mlog_catenate_string(mtr, (byte *)row, row->size);
}


static void heap_insert_row_into_page(que_sess_t *sess, buf_block_t* block, row_header_t *row,
    command_id_t cid, undo_data_t* undo_data, mtr_t* mtr)
{
    page_t* page = buf_block_get_frame(block);
    heap_page_header_t* hdr = page + HEAP_HEADER_OFFSET;
    uint16 upper = mach_read_from_2(hdr + HEAP_HEADER_UPPER);
    uint16 lower = mach_read_from_2(hdr + HEAP_HEADER_LOWER);

    // 1. check compact
    if (lower + row->size + sizeof(row_dir_t) > upper) {
        heap_reorganize_page(block, mtr);
    }

    // 2. alloc directory
    uint32 dir_slot;
    row_dir_t* dir = heap_alloc_dir(block, &dir_slot, mtr);
    dir->is_free = 0;
    dir->scn = cid;
    dir->is_ow_scn = 0;
    dir->offset = lower;  // new position, dont overwrite old data
    // Sets roll ptr field of row
    dir->undo_space_index = undo_data->undo_space_index;
    dir->undo_page_no = undo_data->undo_page_no;
    dir->undo_page_offset = undo_data->undo_page_offset;
    //row->is_changed = 1;

    // 3. 
    undo_data->rec_mgr.m_type = UNDO_HEAP_INSERT;
    undo_data->rec_mgr.m_cid = cid;
    undo_data->rec_mgr.m_insert.row_id.space_id = block->get_space_id();
    undo_data->rec_mgr.m_insert.row_id.page_no = block->get_page_no();
    undo_data->rec_mgr.m_insert.row_id.slot = dir_slot;

    // 4. insert row to page
    uint32 rows = mach_read_from_2(hdr + HEAP_HEADER_ROWS);
    uint32 free_size = mach_read_from_2(hdr + HEAP_HEADER_FREE_SIZE);
    mach_write_to_2(hdr + HEAP_HEADER_LOWER, lower + row->size);
    mach_write_to_2(hdr + HEAP_HEADER_FREE_SIZE, free_size - row->size);
    mach_write_to_2(hdr + HEAP_HEADER_ROWS, rows + 1);
    memcpy(page + lower, (const byte *)row, row->size);

    // 5. redo
    heap_insert_write_redo(block, row, dir, dir_slot, mtr);
}

static status_t heap_insert_row(que_sess_t *sess, dict_table_t* table, row_header_t *row)
{
    status_t ret = CM_SUCCESS;
    mtr_t mtr;
    uint32 cost_size;
    fsm_search_path_t search_path;
    buf_block_t* block;
    uint64 query_min_scn = 0;
    heap_insert_assist_t assist;

    mtr_start(&mtr);

    assist.need_redo = DICT_NEED_REDO(table);
    if (sess->attr->attr_rep.enable_logical_rep) {
        assist.table_id = table->id;
    }

    cost_size = row->size + sizeof(itl_t) + sizeof(row_dir_t);
    block = heap_find_free_page(table, cost_size, search_path, &mtr);
    if (block == NULL) {
        ret = CM_ERROR;
        goto err_exit;
    }

    // set itl
    uint8 itl_id = HEAP_INVALID_ITL_ID;
    itl_t* itl = heap_alloc_itl(block, sess->trx, &mtr, &itl_id);
    if (itl == NULL) {
        CM_SET_ERROR(ERR_ALLOC_ITL, table->name, block->get_space_id(), block->get_page_no());
        ret = ERR_ALLOC_ITL;
        goto err_exit;
    }
    itl->trx_slot_id.id == sess->trx->trx_slot_id.id;
    itl->is_active = 1;
    heap_row_set_itl_id(row, itl_id);

    //
    undo_data_t undo_data;
    undo_data.undo_op = UNDO_INSERT_OP;
    undo_data.query_min_scn = query_min_scn;
    undo_data->rec_mgr.m_data_size = TRX_UNDO_REC_EXTRA_SIZE + sizeof(row_id_t);
    if (trx_undo_prepare(sess, &undo_data, &mtr) != CM_SUCCESS) {
        ret = CM_ERROR;
        goto err_exit;
    }

    //
    heap_insert_row_into_page(block, row, sess->cid, &undo_data, &mtr);

    trx_undo_write_log_rec(sess, &undo_data, &mtr);

    // change catagory of page
    uint16 avail = heap_get_page_free_space(block);
    uint8 category = fsm_space_avail_to_category(table, avail);
    if (category != search_path.category) {
        fsm_recursive_set_catagory(table, search_path, category, &mtr);
    }

    // add page to fast_clean_page_list
    sess->fast_clean_mgr.append_clean_block(block->get_space_id(), block->get_page_no(), block, itl_id);

err_exit:

    mtr_commit(&mtr);

    return ret;
}

status_t heap_insert(que_sess_t* sess, insert_node_t* insert_node)
{
    status_t ret = CM_SUCCESS;
    mtr_t mtr;
    row_header_t* row;

    CM_SAVE_STACK(&sess->stack);

    // Fill in tuple header fields and toast the tuple if necessary
    row = heap_prepare_insert(sess, (dict_table_t*)insert_node->table, insert_node->heap_row);
    if (row == NULL) {
        CM_RESTORE_STACK(&sess->stack);
        return CM_ERROR;
    }

    //
    trx_start_if_not_started(sess);


    mtr_start(&mtr);

    // 
    //if (lock_table_shared(session, cursor->dc_entity, LOCK_INF_WAIT) != CT_SUCCESS) {
    //    err = CM_ERROR;
    //    goto err_exit;
    //}

    ret = heap_insert_row(sess, insert_node->table, row);
    if (ret != CM_SUCCESS) {
        goto err_exit;
    }

err_exit:

    mtr_commit(&mtr);

    CM_RESTORE_STACK(&sess->stack);

    return ret;
}

status_t heap_multi_insert()
{
    return CM_SUCCESS;
}

void heap_delete_write_redo(buf_block_t* block, row_header_t *row, row_dir_t* dir, uint32 dir_slot, mtr_t* mtr)
{
    const uint32 buf_size = 11;
    byte buf[buf_size];

    mach_write_to_2(buf, dir_slot);
    mach_write_to_4(buf + 2, dir->scn); // cid
    mach_write_to_4(buf + 6, dir->undo_ptr);
    mach_write_to_1(buf + 10, row->itl_id);

    mlog_write_log(MLOG_HEAP_DELETE, block->get_space_id(), block->get_page_no(), buf, buf_size, mtr);
}

static status_t heap_delete_row(que_sess_t *sess, dict_table_t* table, row_id_t row_id)
{
    status_t ret = CM_SUCCESS;
    mtr_t mtr;
    uint32 cost_size;
    fsm_search_path_t search_path;
    buf_block_t* block;
    uint64 query_min_scn = 0;
    heap_insert_assist_t assist;

    mtr_start(&mtr);

    assist.need_redo = DICT_NEED_REDO(table);
    if (sess->attr->attr_rep.enable_logical_rep) {
        assist.table_id = table->id;
    }

    // get block by row_id
    const page_id_t page_id(row_id.space_id, row_id.page_no);
    const page_size_t page_size(row_id.space_id);
    buf_block_t* block = buf_page_get(page_id, page_size, RW_X_LATCH, mtr);
    ut_ad(block->get_page_no() == row_id.page_no);
    ut_ad(block->get_page_type() == FIL_PAGE_TYPE_HEAP);
    //buf_block_dbg_add_level(block, SYNC_DICT_HEADER);
    page_t* page = buf_block_get_frame(block);

    row_dir_t* dir = heap_get_dir(page, (uint32)row_id.slot);
    ut_ad(!dir->is_free);
    ut_ad(dir->scn < sess->cid);

    row_header_t* row = HEAP_GET_ROW(page, dir);
    ut_ad(!row->is_deleted);

    itl_t* itl;
    uint32 old_itl_id = HEAP_INVALID_ITL_ID;
    if (row->itl_id == HEAP_INVALID_ITL_ID) {
        // transaction is committed or aborted for row
        itl = heap_alloc_itl(block, sess->trx, mtr, &row->itl_id);
        ut_ad(itl);
    } else {
        // row->itl is current transaction
        old_itl_id = row->itl_id;
        itl = heap_get_itl(page, row->itl_id);
        ut_ad(itl);
        ut_ad(itl->is_active); // row is locked by heap_fetch
    }
    itl->fsc += row->size;

    //
    undo_data_t undo_data;
    undo_data.undo_op = UNDO_MODIFY_OP;
    undo_data.query_min_scn = query_min_scn;
    undo_data.rec_mgr.m_type = undo_type_t::UNDO_HEAP_DELETE;
    undo_data.rec_mgr.m_cid = sess->cid;
    undo_data.rec_mgr.m_delete.row_id = row_id;
    undo_data.rec_mgr.m_delete.old_dir = dir;
    undo_data.rec_mgr.m_delete.old_itl_id = old_itl_id;

    dir->scn = sess->cid;
    dir->is_ow_scn = 0;
    row->is_deleted = 1;
    row->is_changed = 1;

    heap_page_header_t* hdr = page + HEAP_HEADER_OFFSET;
    uint32 rows = mach_read_from_2(hdr + HEAP_HEADER_ROWS);
    ut_ad(rows > 0);
    mach_write_to_2(hdr + HEAP_HEADER_ROWS, rows - 1);

    undo_data->rec_mgr.m_data_size = TRX_UNDO_REC_EXTRA_SIZE + sizeof(row_id_t) + sizeof(row_dir_t) + 1;
    if (trx_undo_prepare(sess, &undo_data, &mtr) != CM_SUCCESS) {
        ret = CM_ERROR;
        goto err_exit;
    }
    trx_undo_write_log_rec(sess, &undo_data, &mtr);

    // Sets roll ptr field of row
    dir->undo_space_index = undo_data->undo_space_index;
    dir->undo_page_no = undo_data->undo_page_no;
    dir->undo_page_offset = undo_data->undo_page_offset;
    heap_delete_write_redo(block, row, dir, row_id.slot, &mtr);

    // add page to fast_clean_page_list
    sess->fast_clean_mgr.append_clean_block(block->get_space_id(), block->get_page_no(), block, row->itl_id);

err_exit:

    mtr_commit(&mtr);

    return ret;

}

status_t heap_delete(que_sess_t* sess, delete_node_t* delete_node)
{
    status_t ret = CM_SUCCESS;
    mtr_t mtr;
    row_header_t* row;

    CM_SAVE_STACK(&sess->stack);

    // Fill in tuple header fields and toast the tuple if necessary
    row = heap_prepare_insert(sess, (dict_table_t*)delete_node->table, delete_node->heap_row);
    if (row == NULL) {
        return CM_ERROR;
    }

    //
    trx_start_if_not_started(sess);


    mtr_start(&mtr);

    // 
    //if (lock_table_shared(session, cursor->dc_entity, LOCK_INF_WAIT) != CT_SUCCESS) {
    //    err = CM_ERROR;
    //    goto err_exit;
    //}

    //if (table->is_contain_lob) {
    //    if (CT_SUCCESS != lob_delete(sess, cursor)) {
    //        return CT_ERROR;
    //    }
    //}


    if (heap_delete_row(sess, insert_node->table, row) != CM_SUCCESS) {
        ret = CM_ERROR;
        goto err_exit;
    }

err_exit:

    mtr_commit(&mtr);

    CM_RESTORE_STACK(&sess->stack);

    return ret;
}

typedef enum en_heap_update_mode {
    UPDATE_INPLACE = 1,  // column size not changed
    UPDATE_INPAGE  = 2,  // current page space is enough
    UPDATE_MIGRATE = 3   // need migrate row to another page
} heap_update_mode_t;


status_t heap_update(que_sess_t* sess, update_node_t* update_node)
{
    status_t ret = CM_SUCCESS;
    mtr_t mtr;
    row_header_t* row;

    CM_SAVE_STACK(&sess->stack);

    // Fill in tuple header fields and toast the tuple if necessary
    row = heap_prepare_insert(sess, (dict_table_t*)insert_node->table, insert_node->heap_row);
    if (row == NULL) {
        return CM_ERROR;
    }

    //
    trx_start_if_not_started(sess);


    mtr_start(&mtr);

    // 
    //if (lock_table_shared(session, cursor->dc_entity, LOCK_INF_WAIT) != CT_SUCCESS) {
    //    err = CM_ERROR;
    //    goto err_exit;
    //}

    //if (table->is_contain_lob) {
    //    if (CT_SUCCESS != lob_delete(sess, cursor)) {
    //        return CT_ERROR;
    //    }
    //}


    if (heap_delete_row(sess, insert_node->table, row) != CM_SUCCESS) {
        ret = CM_ERROR;
        goto err_exit;
    }

err_exit:

    mtr_commit(&mtr);

    CM_RESTORE_STACK(&sess->stack);

    return ret;
}

/*
typedef struct st_knl_scan_range {
    union {
        // index scan range
        struct {
            char l_buf[CT_KEY_BUF_SIZE];
            char r_buf[CT_KEY_BUF_SIZE];
            char org_buf[CT_KEY_BUF_SIZE];
            knl_scan_key_t l_key;
            knl_scan_key_t r_key;
            knl_scan_key_t org_key;
            bool32 is_equal;
        };

        // table scan range
        struct {
            page_id_t l_page;
            page_id_t r_page;
        };
    };
} knl_scan_range_t;
*/


inline status_t heap_row_id_move_to_next_page(page_t* copy_page, scan_cursor_t* cursor)
{
    //if (IS_SAME_PAGID(cursor->scan_range.r_page, AS_PAGID(page->head.id))) {
    //    SET_ROWID_PAGE(&cursor->rowid, INVALID_PAGID);
    //} else {
    //    SET_ROWID_PAGE(&cursor->rowid, AS_PAGID(page->next));
    //}

    cursor->row_id.page_no = ((buf_block_t*)copy_page)->get_next_page_no();
    cursor->row_id.slot = HEAP_PAGE_INVALID_SLOT;

    return CM_SUCCESS;
}

inline status_t heap_get_page_id_by_row_id(const row_id_t& row_id, const page_id_t& page_id)
{
    page_id.reset(row_id.file, row_id.page_no);

    return CM_SUCCESS;
}

static status_t heap_undo_row_prev_version(que_sess_t* sess, scan_cursor_t* cursor, undo_rec_mgr_t* undo_mgr)
{
    switch (undo_mgr->m_type) {
    case UNDO_HEAP_DELETE:
        cursor->row->is_deleted = 0;
        heap_row_set_itl_id(cursor->row, undo_mgr.m_delete.old_itl_id);
        memcpy(cursor->row_dir, undo_mgr.m_delete.old_dir, sizeof(row_dir_t));
        break;
    case UNDO_HEAP_UPDATE:

        break;
    default:
        ut_error;
        break;
    }

    return CM_SUCCESS;
}

static status_t heap_row_build_rcr_version(que_sess_t* sess, scan_cursor_t* cursor,
    row_header_t* row, bool32 *is_found)
{
    trx_undo_rec_hdr_t* undo_rec;
    uint32 undo_rec_size;
    undo_rec_mgr_t undo_mgr;
    mtr_t mtr;

    for (;;) {
        mtr_start(&mtr);

        // read undo record by rollptr from undo log page
        undo_rec = trx_undo_get_undo_rec_by_rollptr(undo_rec_size, cursor->row_dir.undo_rollptr, cursor->row_id, &mtr);
        if (undo_rec == NULL) {
            mtr_commit(&mtr);
            return CM_ERROR;
        }
        // parse undo record
        undo_mgr.deserialize(undo_rec, undo_rec_size);
        // check visibility for cursor->row
        if (undo_mgr.m_type == UNDO_HEAP_INSERT) { 
            /* It was a freshly inserted version */
            *is_found = FALSE;
            mtr_commit(&mtr);
            return CM_SUCCESS;
        }
        // undo prev row to cursor->row
        if (heap_undo_row_prev_version(sess, undo_mgr, mtr) != CM_SUCCESS) {
            mtr_commit(&mtr);
            return CM_ERROR;
        }
        if (cursor->row_dir.scn <= cursor->query_scn) {
            /* The view already sees this version */
            *is_found = TRUE;
            mtr_commit(&mtr);
            return CM_SUCCESS;
        }

        mtr_commit(&mtr);
    }

    return CM_SUCCESS;
}

static status_t heap_get_row(que_sess_t* sess, scan_cursor_t* cursor, page_t* page, bool32 *is_found)
{
    trx_status_t trx_status;
    itl_t *itl = NULL;

    row_dir_t* dir = heap_get_dir(page, (uint32)cursor->row_id.slot);
    if (dir->is_free) {
        *is_found = FALSE;
        return CM_SUCCESS;
    }

    row_header_t* row = HEAP_GET_ROW(page, dir);
    if (row->is_migrate) {
        *is_found = FALSE;
        return CM_SUCCESS;
    }

    if (row->itl_id == HEAP_INVALID_ITL_ID) {
        trx_status.status = XACT_END;
        trx_status.is_ow_scn = (uint8)dir->is_ow_scn;
        trx_status.scn = dir->scn;
    } else {
        itl = heap_get_itl(page, row->itl_id);
        trx_get_status_by_itl(itl->trx_slot_id, &trx_status);
        if (itl->is_active && trx_status.status == XACT_END) {
            cursor->is_cleanout = TRUE;
        }
    }

    //
    cursor->undo_space_index = dir->undo_space_index;
    cursor->undo_page_no = dir->undo_page_no;
    cursor->undo_page_offset = dir->undo_page_offset;

    if (trx_status.status == XACT_END) {
        if (trx_status.scn <= cursor->query_scn) {
            *is_found = !row->is_deleted;
            if (*is_found) {
                cursor->row_dir = dir;
                memcpy(cursor->row, row, row->size);
            }
            return CM_SUCCESS;
        }
        if (trx_status.is_ow_scn) {
            //CT_LOG_RUN_ERR("snapshot too old, detail: dir owscn %llu, query scn %llu", cursor->scn, query_scn);
            return ERR_SNAPSHOT_TOO_OLD;
        }
    } else {
        // same transaction
        if (itl->trx_slot_id == sess->trx->trx_slot_id) {
            if (dir->scn < sess->cid) {
                *is_found = !(row->is_deleted);
                if (*is_found) {
                    cursor->row_dir = dir;
                    memcpy(cursor->row, row, row->size);
                }
                return CM_SUCCESS;
            }
        }

        // xa transaction
        if (sess->kernel->is_xa_consistency &&
            (trx_status.status == XACT_XA_PREPARE || trx_status.status == XACT_XA_ROLLBACK) &&
            trx_status.scn < cursor->query_scn) {
            //CT_LOG_DEBUG_INF("need read wait.prepare_scn[%llu] <= query_scn[%llu]", txn_info.scn, query_scn);
            sess->wait_xid = itl->trx_slot_id;
            sess->wait_row_id = cursor->row_id;
            return CM_SUCCESS;
        }
    }

    // Fetch a previous version of the row if the current one is not visible in the snapshot
    cursor->row_dir = dir;
    memcpy(cursor->row, row, row->size);
    return heap_row_build_rcr_version(sess, cursor, is_found);
}

static status_t heap_scan_full_page(que_sess_t* sess, scan_cursor_t* cursor, bool32 *is_found)
{
    status_t err;
    page_t* copy_page = (page_t *)cursor->cache_page_buf;
    heap_page_header_t* page_hdr = copy_page + HEAP_HEADER_OFFSET;
    *is_found = FALSE;

    for (;;) {
        // get next row
        if (cursor->row_id.slot == HEAP_PAGE_INVALID_SLOT) {
            cursor->row_id.slot = 0;
        } else {
            cursor->row_id.slot++;
        }
        // check if it is necessary to switch to next page
        if (cursor->row_id.slot == mach_read_from_2(page_hdr + HEAP_HEADER_DIRS)) {
            return heap_row_id_move_to_next_page(copy_page, cursor);
        }

        //if ((uint16)cursor->row_id.slot > mach_read_from_2(page_hdr + HEAP_HEADER_DIRS)) {
            //CT_THROW_ERROR(ERR_OBJECT_ALREADY_DROPPED, "table");
        //    return CM_ERROR;
        //}

        if (heap_get_row(sess, cursor, copy_page, is_found) != CM_SUCCESS) {
            return CM_ERROR;
        }
        if (*is_found) {
            return CM_SUCCESS;
        }

        if (UNLIKELY(sess->wait_xid.id != TRANSACTION_INVALID_ID)) {
            // backspace_cursor, we will recheck this row next time
            ut_ad(cursor->row_id.slot != HEAP_PAGE_INVALID_SLOT);
            if (cursor->row_id.slot <= 0) {
                cursor->row_id.slot = HEAP_PAGE_INVALID_SLOT;
            } else {
                cursor->row_id.slot--;
            }
            return CM_SUCCESS;
        }
    }

    return CM_ERROR;
}

static status_t heap_read_page_to_cache(que_sess_t* sess, scan_cursor_t* cursor, scn_t* query_scn)
{
    status_t err;
    
    mtr_t init_mtr, *mtr = &init_mtr;

    mtr_start(mtr);

    const page_id_t page_id;
    err = heap_get_page_id_by_row_id(page_id, cursor->row_id);
    CM_RETURN_IF_ERROR(err);

    const page_size_t page_size(page_id.get_space_id());
    buf_block_t* block = buf_page_get(page_id, page_size, RW_S_LATCH, mtr);
    if (block == NULL) {
        return CM_ERROR;
    }

    memcpy(cursor->cache_page_buf, buf_block_get_frame(block), page_size.physical());

    mtr_commit(mtr);

    return CM_SUCCESS;
}

bool32 heap_page_cached_invalid(que_sess_t *session, scan_cursor_t *cursor)
{
    date_t timeout;
/*
    if (cursor->row_id.slot != INVALID_PAGE_SLOT) {
        if (cursor->isolevel != (uint8)ISOLATION_CURR_COMMITTED) {
            return FALSE;
        }

        timeout = (date_t)
            ((uint64)session->kernel->undo_ctx.retention * MICROSECS_PER_SECOND / RETENTION_TIME_PERCENT);

        return (bool32)((KNL_NOW(session) - cursor->cc_cache_time) >= timeout);
    }
*/
    return TRUE;
}

struct TABLE_SHARE {
  TABLE_SHARE() = default;

    ulong reclength{0};               /* Recordlength */
    ulong stored_rec_length{0};       /* Stored record length
                                      (no generated-only generated fields) */

    uint16 row_buff_length{0};   /* Size of table->record[] buffer */

    byte* default_row_values{NULL};      /* row with default values */

};

void a()
{
      if (!(record = (uchar *)share->mem_root.Alloc(rec_buff_length)))
    goto err; /* purecov: inspected */
  share->default_values = record;
}

static status_t heap_fetch_by_page(que_sess_t* sess, scan_cursor_t* cursor, bool32 *is_found)
{
    status_t err;

retry_fetch:

    if (cursor->action == CURSOR_ACTION_SELECT) {
        if (heap_page_cached_invalid(sess, cursor)) {
            err = heap_read_page_to_cache(sess, cursor);
            CM_RETURN_IF_ERROR(err);
        }

        if (heap_scan_full_page(sess, cursor, is_found) != CM_SUCCESS) {
            return CM_ERROR;
        }

        if (UNLIKELY(sess->wait_xid.id != TRANSACTION_INVALID_ID)) {
            //CT_LOG_DEBUG_INF("fetch row begin read wait.");
            err = sess->wait_transaction_end();
            CM_RETURN_IF_ERROR(err);
            goto retry_fetch;
        }
    } else {

    }

    return CM_SUCCESS;
}


// Delayed page cleanout for heap
void heap_cleanout_page(que_sess_t* sess, scan_cursor_t* cursor, dict_table_t* table, row_id_t row_id)
{
    heap_t *heap = NULL;
    heap_page_t *page = NULL;
    bool32 changed = CT_FALSE;
    bool32 lock_inuse = CT_FALSE;
    uint8 owner_list;
    mtr_t mtr;

    //if (DB_IS_READONLY(sess)) {
    //    return;
    //}

    // may be called during rollback, already in atmatic operation
    //if (session->atomic_op) {
    //    return;
    //}

    mtr_start(&mtr);

    // for delay cleaning page, test the table is locked or not, and try to locking
    // if table is locked by ddl/dcl(include truncate table) or dc invalidated, return FALSE immediate
    //if (!lock_table_without_xact(session, cursor->dc_entity, &lock_inuse)) {
    //    cm_reset_error();
    //    return;
    //}

    // get block by row_id
    // get block by row_id
    const page_id_t page_id(row_id.space_id, row_id.page_no);
    const page_size_t page_size(row_id.space_id);
    buf_block_t* block = buf_page_get(page_id, page_size, RW_X_LATCH, &mtr);
    ut_ad(block->get_page_no() == row_id.page_no);
    ut_ad(block->get_page_type() == FIL_PAGE_TYPE_HEAP);
    //buf_block_dbg_add_level(block, SYNC_DICT_HEADER);

    page_t* page = buf_block_get_frame(block);

    //heap_cleanout_itls(sess, cursor, page, &changed, &mtr);

    // change catagory of page
    uint16 avail = heap_get_page_free_space(block);
    uint8 category = fsm_space_avail_to_category(table, avail);
    //fsm_recursive_set_catagory(table, search_path, category, &mtr);
    //heap_try_change_map(session, heap, page_id);

    //unlock_table_without_xact(session, cursor->dc_entity, lock_inuse);

    mtr_commit(&mtr);
}


// retrieve tuple with given tid.
// tuple->t_self is the TID to fetch.
// We pin the buffer holding the tuple, fill in the remaining fields of tuple.
status_t heap_fetch(que_sess_t* sess, scan_cursor_t* cursor)
{
    status_t status;
    row_id_t row_id;
    dict_table_t* table;
    //Snapshot snapshot,     HeapTuple tuple, Buffer *userbuf

    //if (IS_DUAL_TABLE(table)) {
    //    return dual_fetch();
    //}

    for (;;) {
        if (HEAP_PAGE_INVALID_ROWID(cursor->row_id)) {
            cursor->is_found = FALSE;
            cursor->is_eof = TRUE;
            return CM_SUCCESS;
        }

        row_id = cursor->row_id;
        if (heap_fetch_by_page(sess, cursor, &cursor->is_found) != CM_SUCCESS) {
            status = CM_ERROR;
            break;
        }

#define IS_SAME_PAGID_BY_ROWID(id1, id2) ((id1).page_no == (id2).page_no && (id1).space_id == (id2).space_id)

        if (!IS_SAME_PAGID_BY_ROWID(row_id, cursor->row_id)) {
            if (sess->is_canceled) {
                status = CM_ERROR;
                break;
            }

            if (sess->is_killed) {
                status = CM_ERROR;
                break;
            }
            // page cleanout during full page scan
            if (cursor->is_cleanout) {
                heap_cleanout_page(sess, cursor, table, row_id);
                cursor->is_cleanout = FALSE;
            }
        }


        if (!cursor->is_found) {
            continue;
        }

        //if (knl_match_cond(session, cursor, &cursor->is_found) != CM_SUCCESS) {
        //    status = CM_ERROR;
        //    break;
        //}

        if (!cursor->is_found) {
            continue;
        }

        if (cursor->action <= CURSOR_ACTION_SELECT) {
            status = CM_SUCCESS;
            break;
        }

        // for update / delete
        if (heap_lock_row(sess, cursor, &cursor->is_found) != CM_SUCCESS) {
            status = CM_ERROR;
            break;
        }

        if (!cursor->is_found) {
            continue;
        }
        status = CM_SUCCESS;
        break;
    }

    return CM_SUCCESS;
}


status_t knl_match_cond(que_sess_t* sess, scan_cursor_t* cursor, bool32* matched)
{
    knl_match_cond_t match_push_cond = NULL;
    knl_session_t *se = (knl_session_t *)session;

    if (IS_INDEX_ONLY_SCAN(cursor)) {
        idx_decode_row(se, cursor, cursor->offsets, cursor->lens, &cursor->data_size);
        cursor->decode_cln_total = ((index_t *)cursor->index)->desc.column_count;
    } else {
        cm_decode_row_ex((char *)cursor->row, cursor->offsets, cursor->lens, cursor->decode_count, &cursor->data_size,
                         &cursor->decode_cln_total);
    }

    match_cond = se->match_cond;

    if (cursor->stmt == NULL || match_cond == NULL) {
        *matched = CT_TRUE;
        return CT_SUCCESS;
    }


    ExecReadyInterpretedExpr


    return match_cond(cursor->stmt, matched);
}


status_t heap_fetch_by_rowid(que_sess_t* session, scan_cursor_t* cursor)
{
    return CM_SUCCESS;
}

//status_t heap_getnext(TableScanDesc sscan, ScanDirection direction)
//{
//    return CM_SUCCESS;
//}

/*
TableScanDesc
heap_beginscan(Relation relation, Snapshot snapshot,
			   int nkeys, ScanKey key,
			   ParallelTableScanDesc parallel_scan,
			   uint32 flags)
{
}

void heap_endscan(TableScanDesc sscan)
{
}
*/

void heap_undo_insert(que_sess_t* sess, trx_t* trx, trx_undo_rec_hdr_t* undo_rec, uint32 undo_rec_size)
{
    mtr_t mtr;
    undo_rec_mgr_t undo_mgr;

    mtr_start(&mtr);

    // parse undo record
    undo_mgr.deserialize(undo_rec, undo_rec_size);
    ut_a(undo_mgr.m_type == UNDO_HEAP_INSERT);

    // get heap page
    const page_id_t page_id(undo_mgr.m_insert.row_id->space_id, undo_mgr.m_insert.row_id->page_no);
    const page_size_t page_size(page_id.get_space_id());
    buf_block_t* block = buf_page_get(page_id, page_size, RW_X_LATCH, mtr);
    ut_ad(block->get_page_no() == page_id.get_page_no());
    ut_ad(block->get_page_type() == FIL_PAGE_TYPE_HEAP);
    //buf_block_dbg_add_level(block, SYNC_DICT_HEADER);
    page_t* page = buf_block_get_frame(block);

    // get heap row
    row_dir_t* dir = heap_get_dir(page, (uint32)undo_mgr.m_insert.row_id->slot);
    ut_ad(!dir->is_free);
    row_header_t* row = HEAP_GET_ROW(page, dir);
    ut_a_log(heap_row_get_itl_id(row) != HEAP_INVALID_ITL_ID,
        "row's itl id is invalid, panic info: page %u-%u type %u",
        page_id.get_space_id(), page_id.get_page_no(), FIL_PAGE_TYPE_HEAP);
    itl_t* itl = heap_get_itl(page, FIL_PAGE_TYPE_HEAP(row));
    ut_a_log(itl->trx_slot_id.id == sess->trx->trx_slot_id.id,
        "the xid of itl and trx are not equal, panic info: page %u-%u type %u itl xid %llu trx xid %llu",
        page_id.get_space_id(), page_id.get_page_no(), FIL_PAGE_TYPE_HEAP, itl->trx_slot_id.id, sess->trx.trx_slot_id.id);

    // undo row
    heap_page_header_t* page_hdr = page + HEAP_HEADER_OFFSET;
    uint32 rows = mach_read_from_2(page_hdr + HEAP_HEADER_ROWS);
    uint32 free_size = mach_read_from_2(page_hdr + HEAP_HEADER_FREE_SIZE);
    mach_write_to_2(page_hdr + HEAP_HEADER_FREE_SIZE, free_size + row->size);
    mach_write_to_2(page_hdr + HEAP_HEADER_ROWS, rows - 1);

    row->is_deleted = 1;
    heap_row_set_itl_id(row, HEAP_INVALID_ITL_ID);
    heap_free_dir(block, dir, undo_mgr.m_insert.row_id->slot, mtr);

    // write redo log
    const uint32 buf_size = 4;
    byte buf[buf_size];
    mach_write_to_2(buf, undo_mgr.m_insert.row_id->slot);
    mach_write_to_2(buf+2, row->size);
    mlog_write_log(MLOG_HEAP_UNDO_INSERT, block->get_space_id(), block->get_page_no(), buf, buf_size, mtr);

    mtr_commit(&mtr);
}

void heap_undo_delete(que_sess_t* sess, trx_t* trx, trx_undo_rec_hdr_t* undo_rec, uint32 undo_rec_size)
{
    mtr_t mtr;
    undo_rec_mgr_t undo_mgr;

    mtr_start(&mtr);

    // parse undo record
    undo_mgr.deserialize(undo_rec, undo_rec_size);
    ut_a(undo_mgr.m_type == UNDO_HEAP_DELETE);

    // get heap page
    const page_id_t page_id(undo_mgr.m_delete.row_id->space_id, undo_mgr.m_delete.row_id->page_no);
    const page_size_t page_size(page_id.get_space_id());
    buf_block_t* block = buf_page_get(page_id, page_size, RW_X_LATCH, mtr);
    ut_ad(block->get_page_no() == page_id.get_page_no());
    ut_ad(block->get_page_type() == FIL_PAGE_TYPE_HEAP);
    //buf_block_dbg_add_level(block, SYNC_DICT_HEADER);
    page_t* page = buf_block_get_frame(block);

    // get heap row
    row_dir_t* dir = heap_get_dir(page, (uint32)undo_mgr.m_delete.row_id->slot);
    ut_ad(!dir->is_free);
    row_header_t* row = HEAP_GET_ROW(page, dir);
    ut_a_log(heap_row_get_itl_id(row) != HEAP_INVALID_ITL_ID,
        "row's itl id is invalid, panic info: page %u-%u type %u",
        page_id.get_space_id(), page_id.get_page_no(), FIL_PAGE_TYPE_HEAP);
    itl_t* itl = heap_get_itl(page, FIL_PAGE_TYPE_HEAP(row));
    ut_a_log(itl->trx_slot_id.id == sess->trx->trx_slot_id.id,
        "the xid of itl and trx are not equal, panic info: page %u-%u type %u itl xid %llu trx xid %llu",
        page_id.get_space_id(), page_id.get_page_no(), FIL_PAGE_TYPE_HEAP, itl->trx_slot_id.id, sess->trx.trx_slot_id.id);

    // undo row
    heap_page_header_t* page_hdr = page + HEAP_HEADER_OFFSET;
    uint32 rows = mach_read_from_2(page_hdr + HEAP_HEADER_ROWS);
    mach_write_to_2(page_hdr + HEAP_HEADER_ROWS, rows - 1);
    itl->fsc -= row->size;
    row->is_deleted = 0;
    heap_row_set_itl_id(row, undo_mgr.m_delete.old_itl_id);
    memcpy(dir, undo_mgr.m_delete.old_dir, sizeof(row_dir_t));

    // write redo log
    const uint32 buf_size = 3 + sizeof(row_dir_t);
    byte buf[buf_size];
    mach_write_to_2(buf, undo_mgr.m_delete.row_id->slot);
    mach_write_to_1(buf+2, undo_mgr.m_delete.old_itl_id);
    memcpy(buf+3, undo_mgr.m_delete.old_dir, sizeof(row_dir_t));
    mlog_write_log(MLOG_HEAP_UNDO_DELETE, block->get_space_id(), block->get_page_no(), buf, buf_size, mtr);

    mtr_commit(&mtr);
}

byte* heap_insert_replay(uint32 type, uint64 lsn, byte* log_rec_ptr, byte* log_end_ptr, void* block)
{
    ut_ad(type == MLOG_HEAP_INSERT);
    ut_a(log_end_ptr - log_rec_ptr > 2 + 2 + sizeof(row_dir_t));

    page_t* page = buf_block_get_frame((buf_block_t *)block);
    uint32 rec_size = mach_read_from_2(log_rec_ptr);

    uint64 page_lsn = mach_read_from_8(page + HEAP_HEADER_OFFSET + HEAP_HEADER_LSN);
    if (page_lsn >= lsn) {
        return log_rec_ptr + 2 + 2 + sizeof(row_dir_t) + rec_size;
    }

    uint32 offset = mach_read_from_2(log_rec_ptr + 2);
    row_dir_t* dir = (row_dir_t*)(log_rec_ptr + 4);


    LOGGER_TRACE(LOGGER, LOG_MODULE_RECOVERY,
        "trx_rseg_replay_begin_slot: type %lu block (%p space_id %lu page_no %lu) rseg_id %lu slot %lu xnum %llu",
        type, block, block ? ((buf_block_t *)block)->get_space_id() : INVALID_SPACE_ID,
        block ? ((buf_block_t *)block)->get_page_no() : INVALID_PAGE_NO,
        trx_slot_id.rseg_id, trx_slot_id.slot, trx_slot_id.xnum);

    return log_rec_ptr;

/*
    mlog_write_log(MLOG_HEAP_INSERT, block->get_space_id(), block->get_page_no(), NULL, 0, mtr);
    mlog_catenate_uint32(mtr, lower + rec->size, MLOG_2BYTES);
    mlog_catenate_uint32(mtr, free_size - rec->size, MLOG_2BYTES);
    mlog_catenate_uint32(mtr, rows + 1, MLOG_2BYTES);
    mlog_catenate_string(mtr, (byte *)dir, sizeof(row_dir_t));
    mlog_catenate_string(mtr, (byte *)rec, rec->size);
*/
    return NULL;
}

byte* heap_undo_insert_replay(uint32 type, uint64 lsn, byte* log_rec_ptr, byte* log_end_ptr, void* block)
{
    ut_ad(type == MLOG_HEAP_UNDO_INSERT);
    ut_a(log_end_ptr - log_rec_ptr > 4);

    page_t* page = buf_block_get_frame((buf_block_t *)block);
    uint64 page_lsn = mach_read_from_8(page + HEAP_HEADER_OFFSET + HEAP_HEADER_LSN);
    if (page_lsn >= lsn) {
        return log_rec_ptr + 4;
    }

    uint32 dir_slot = mach_read_from_2(log_rec_ptr);
    uint32 row_size = mach_read_from_2(log_rec_ptr + 2);

    row_dir_t* dir = heap_get_dir(page, (uint32)dir_slot);
    ut_ad(!dir->is_free);
    row_header_t* row = HEAP_GET_ROW(page, dir);
    ut_ad(row->size == row_size);

    // undo row
    heap_page_header_t* hdr = page + HEAP_HEADER_OFFSET;
    uint32 rows = mach_read_from_2(hdr + HEAP_HEADER_ROWS);
    uint32 free_size = mach_read_from_2(hdr + HEAP_HEADER_FREE_SIZE);
    mach_write_to_2(hdr + HEAP_HEADER_FREE_SIZE, free_size + row->size);
    mach_write_to_2(hdr + HEAP_HEADER_ROWS, rows - 1);

    row->is_deleted = 1;
    heap_row_set_itl_id(row, HEAP_INVALID_ITL_ID);
    heap_free_dir(block, dir, dir_slot, NULL);

    LOGGER_TRACE(LOGGER, LOG_MODULE_RECOVERY,
        "heap_undo_insert_replay: type %lu block (%p space_id %lu page_no %lu) dir %lu",
        type, block, block ? ((buf_block_t *)block)->get_space_id() : INVALID_SPACE_ID,
        block ? ((buf_block_t *)block)->get_page_no() : INVALID_PAGE_NO, dir_slot);

    return log_rec_ptr+4;
}

