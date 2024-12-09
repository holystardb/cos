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
    uint32 page_no = fsm_create(space_id);

    mtr_start(&mtr);

    const page_size_t page_size(space_id);
    buf_block_t* block[8] = {NULL};
    for (uint32 i = 0; i < 8; i++) {
        block[i] = fsp_alloc_free_page(space_id, page_size, Page_fetch::NORMAL, &mtr);
        if (block[i] == NULL) {
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
    return (itl_t *)(page + UNIV_PAGE_SIZE - FIL_PAGE_DATA_END - (itl_id + 1) * sizeof(itl_t));
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

static inline row_dir_t *heap_alloc_free_dir(page_t* page, uint32* dir_slot, mtr_t* mtr)
{
    row_dir_t* dir;
    heap_page_header_t* hdr = page + HEAP_HEADER_OFFSET;

    *dir_slot = mach_read_from_2(hdr + HEAP_HEADER_FIRST_FREE_DIR);
    if (*dir_slot == HEAP_NO_FREE_DIR) {
        *dir_slot = mach_read_from_2(hdr + HEAP_HEADER_DIRS);
        mlog_write_uint32(hdr + HEAP_HEADER_DIRS, *dir_slot + 1, MLOG_2BYTES, mtr);

        dir = heap_get_dir(page, *dir_slot);
        dir->scn = 0;
        dir->undo_page_no = FIL_NULL;
        dir->undo_page_offset = 0;
        dir->is_free = 1;
        dir->is_ow_scn = 0;

        uint16 offset = (uint16)mach_read_from_2(hdr + HEAP_HEADER_UPPER) - sizeof(row_dir_t);
        mlog_write_uint32(hdr + HEAP_HEADER_UPPER, offset, MLOG_2BYTES, mtr);
    } else {
        dir = heap_get_dir(page, *dir_slot);

        mlog_write_uint32(hdr + HEAP_HEADER_FIRST_FREE_DIR, dir->free_next_dir, MLOG_2BYTES, mtr);
    }

    return dir;
}

inline uint8 heap_row_get_itl_id(row_header_t* row)
{
    return row->itl_id;
}

inline void heap_row_set_itl_id(row_header_t* row, uint8 itl_id)
{
    row->itl_id = itl_id;
}

/*
 * Compact deleted or free space after update migration, the algorithm is as follow:
 * 1.transfer row slot from row dir into row itself by swap row sprs_count and dir offset,
 *   so we can use row itself to locate its row dir.
 * 2.traverse row one by one, if row has been delete, compact it's space.
 * 3.restore the row sprs_count and dir offset after row compact.
 * we use sprs_count instead of column_count as a temp store, because column_count
 * is 10 bits, can only represent 1024 rows. When page is 32K, it's would out of bound.
 */
static void heap_reorganize_page(buf_block_t* block)
{
    row_dir_t*  dir = NULL;
    row_header_t* row = NULL;
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
            LOGGER_PANIC_CHECK(LOGGER, LOG_MODULE_HEAP, heap_row_get_itl_id(row) != HEAP_INVALID_ITL_ID,
                "row itl id is invalid, space id %u page no %u page type %u",
                block->get_space_id(), block->get_page_no(), block->get_page_type());

            itl_t* itl = heap_get_itl(page, heap_row_get_itl_id(row));
            if (!itl->is_active) {
                heap_row_set_itl_id(row, HEAP_INVALID_ITL_ID);
                dir->scn = itl->scn;
                dir->is_ow_scn = itl->is_ow_scn;
                dir->is_free = 1;
                dir->free_next_dir = mach_read_from_2(header + HEAP_HEADER_FIRST_FREE_DIR);
                mach_write_to_2(header + HEAP_HEADER_FIRST_FREE_DIR, i);
                continue;
            }
        }

        /*
         * the max page size is 32K(0x8000) which means the max row size in
         * page is less than 32K, and the row->size is 2bytes(max size 64K),
         * so we can temporarily use the high bits as compacting mask
         */
        //LOGGER_PANIC_CHECK(LOGGER, (row->size & ROW_COMPACTING_MASK) == 0,
        //    "current row is compacting, space id %u page no %u page type %u",
        //    block->get_space_id(), block->get_page_no(), block->get_page_type());

        //row->size |= ROW_COMPACTING_MASK;

        // temporarily save row slot to row->sprs_count, so we can use row itself to find it's dir
        //dir->offset = row->sprs_count;
        //row->sprs_count = i;
    }

    // traverse row one by one from first row after heap page head
    uint16 lower = mach_read_from_2(header + HEAP_HEADER_LOWER);
    row = (row_header_t *)((char *)page + HEAP_HEADER_SIZE);
    row_header_t* free_addr = row;

    while ((char *)row < (char *)page + lower) {
        //if ((row->size & ROW_COMPACTING_MASK) == 0) {
        //    // row has been deleted, compact it's space directly
        //    row = (row_header_t *)((char *)row + row->size);
        //    continue;
        //}

        // don't clear the compacting mask here, just get the actual row size
        uint16 row_size = 0;// (row->size & ~ROW_COMPACTING_MASK);

        //LOGGER_PANIC_CHECK(LOGGER, row->sprs_count < dir_count,
        //    "count of column is more than page's dirs, space id %u page no %u page type %u sprs_count %u dirs %u",
        //    block->get_space_id(), block->get_page_no(), block->get_page_type() row->sprs_count, dir_count);

        //dir = heap_get_dir(page, row->sprs_count);

        // move current row to the new compacted position
        uint16 copy_size = (row->is_ext) ? (uint16)HEAP_MIN_ROW_SIZE : row_size;
        if (row != free_addr && copy_size != 0) {
            memmove(free_addr, row, copy_size);
        }

        /* restore the row and its dir */
        free_addr->size = copy_size;
        //free_addr->sprs_count = (dir->offset);
        dir->offset = (uint16)((char *)free_addr - (char *)page);

        // now, handle the next row
        free_addr = (row_header_t *)((char *)free_addr + copy_size);
        row = (row_header_t *)((char *)row + row_size);
    }

    // reset the latest page free begin position,
    // free_addr - page is less than DEFAULT_PAGE_SIZE(8192)
    mach_write_to_2(header + HEAP_HEADER_UPPER, (uint16)((char *)free_addr - (char *)page));
}

static itl_t* heap_create_itl(buf_block_t* block, uint8* itl_id, mtr_t* mtr)
{
    itl_t* itl;
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
        heap_reorganize_page(block);
    }

    if (dir_count > 0) {
        byte* dest = page + upper - sizeof(itl_t);
        memmove(dest, page + upper, dir_count * sizeof(row_dir_t));
    }

    itl = (itl_t *)(page + upper + dir_count * sizeof(row_dir_t));
    *itl_id = itl_count + 1;

    mach_write_to_2(header + HEAP_HEADER_ITLS, itl_count + 1);
    mach_write_to_2(header + HEAP_HEADER_UPPER, upper - sizeof(itl_t));
    mach_write_to_2(header + HEAP_HEADER_FREE_SIZE, free_size - sizeof(itl_t));

    return itl;
}


static void heap_reuse_itl(page_t* page, itl_t* itl, uint8 itl_id, mtr_t* mtr)
{
    heap_page_header_t* header = HEAP_PAGE_GET_HEADER(page);
    row_dir_t* dir;
    row_header_t* row;

    uint32 dir_count = mach_read_from_2(header + HEAP_HEADER_DIRS);

    for (uint32 i = 0; i < dir_count; i++) {
        dir = heap_get_dir(page, i);
        if (dir->is_free) {
            continue;
        }

        row = heap_get_row_by_dir(page, dir);
        if (heap_row_get_itl_id(row) != itl_id) {
            continue;
        }

        heap_row_set_itl_id(row, HEAP_INVALID_ITL_ID);
        if (row->is_change == FALSE) {
            row->is_change = TRUE;
            continue;
        }

        dir->scn = itl->scn;
        dir->is_ow_scn = itl->is_ow_scn;
        if (row->is_deleted) {
            dir->is_free = TRUE;
            dir->free_next_dir = mach_read_from_2(header + HEAP_HEADER_FIRST_FREE_DIR);
            mach_write_to_2(header + HEAP_HEADER_FIRST_FREE_DIR, i);
        }
    }
}

static itl_t* heap_alloc_itl(buf_block_t* block, trx_t* trx, mtr_t* mtr, uint8* itl_id)
{
    itl_t* ret_itl = NULL;
    page_t* page = buf_block_get_frame(block);
    heap_page_header_t* header = page + HEAP_HEADER_OFFSET;
    uint32 itl_count = mach_read_from_2(header + HEAP_HEADER_ITLS);

    for (uint32 i = 0; i < itl_count; i++) {
        itl_t* itl = heap_get_itl(page, i);
        if (itl->trx_slot_id.id == trx->trx_slot_id.id) {
            if (itl_id) {
                *itl_id = i;
            }
            return itl;
        }

        if (!itl->is_active) {
            if (ret_itl == NULL) {
                ret_itl = itl;
                *itl_id = i;
            }
            // Continue searching,
            // Check if transaction already has an ITL
            continue;
        }

        trx_status_t trx_status;
        trx_get_status_by_itl(itl->trx_slot_id, &trx_status);
        if (trx_status.status != XACT_END) {
            continue;
        }

        if (ret_itl == NULL) {
            ret_itl = itl;
            *itl_id = i;
        }
    }

    if (ret_itl == NULL) {
        ret_itl = heap_create_itl(block, itl_id, mtr);
        mlog_write_log(MLOG_HEAP_NEW_ITL, block->get_space_id(), block->get_page_no(), NULL, 0, mtr);
        mlog_catenate_uint32(mtr, *itl_id, MLOG_1BYTE);
    } else {
        heap_reuse_itl(page, ret_itl, *itl_id, mtr);
        mlog_write_log(MLOG_HEAP_REUSE_ITL, block->get_space_id(), block->get_page_no(), NULL, 0, mtr);
        mlog_catenate_uint32(mtr, *itl_id, MLOG_1BYTE);
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

    mlog_write_log(MLOG_HEAP_CLEAN_ITL, block->get_space_id(), block->get_page_no(), NULL, 0, mtr);
    mlog_catenate_uint32(mtr, itl_id, MLOG_1BYTE);
    mlog_catenate_uint64(mtr, scn);
}

itl_t* heap_alloc_itl_new(que_sess_t* session, page_t* page, mtr_t* mtr, uint8* itl_id)
{
    itl_t* itl;
    itl_t* ret_itl = NULL;
    bool32 is_free_itl = FALSE;
    heap_page_header_t* header = HEAP_PAGE_GET_HEADER(page);
/*
    if (header->used_itls > 0) {
        uint8 itl_id = header->first_used_itl;
        while (itl_id < 0xFF) {
            itl = heap_get_itl(page, itl_id);
            if (itl->trx_slot.id == session->slot.id) {
                return itl;
            }
            itl_id = itl->next;
        }
    }

    if (header->free_itls > 0) {
        itl = heap_get_itl(page, header->first_free_itl);
        //header->first_free_itl = itl->next;
        //header->free_itls--;
        ret_itl = itl;
        is_free_itl = TRUE;
    }

    if (ret_itl == NULL && header->used_itls > 0) {
        uint8 itl_id = header->first_used_itl;
        while (itl_id < 0xFF) {
            itl = heap_get_itl(page, itl_id);

            trx_status_t trx_status;
            trx_get_status_by_itl(itl, &trx_status);
            if (trx_status.status == XACT_END) {
                // modify row csn
                ret_itl = itl;
                break;
            }

            itl_id = itl->next;
        }
    }

    if (ret_itl == NULL) {
        ret_itl = heap_create_itl(page, mtr);
    } else {
        ret_itl = heap_reuse_itl(page, ret_itl, is_free_itl, mtr);
    }
*/
    return ret_itl;
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


static void heap_insert_row_into_page(buf_block_t* block, row_header_t *rec,
    command_id_t cid, undo_data_t* undo_data, mtr_t* mtr)
{
    page_t* page = buf_block_get_frame(block);
    heap_page_header_t* hdr = page + HEAP_HEADER_OFFSET;
    uint16 upper = mach_read_from_2(hdr + HEAP_HEADER_UPPER);
    uint16 lower = mach_read_from_2(hdr + HEAP_HEADER_LOWER);

    // 1. check compact
    if (lower + rec->size + sizeof(row_dir_t) > upper) {
        heap_reorganize_page(block);
        mlog_write_log(MLOG_PAGE_REORGANIZE, block->get_space_id(), block->get_page_no(), NULL, 0, mtr);
    }

    // 2. alloc directory
    uint32 dir_slot;
    row_dir_t* dir = heap_alloc_free_dir(page, &dir_slot, mtr);
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
    undo_data->rec.type = UNDO_HEAP_INSERT;
    undo_data->rec.snapshot.scn = dir->scn;
    undo_data->rec.snapshot.undo_page_no = dir->undo_page_no;
    undo_data->rec.snapshot.offsets = dir->offsets;
    undo_data->rec.space_id = block->get_space_id();
    undo_data->rec.page_no = block->get_page_no();
    undo_data->rec.dir_slot = dir_slot;

    // 4. insert row to page
    uint32 rows = mach_read_from_2(hdr + HEAP_HEADER_ROWS);
    uint32 free_size = mach_read_from_2(hdr + HEAP_HEADER_FREE_SIZE);
    mach_write_to_2(hdr + HEAP_HEADER_LOWER, lower + rec->size);
    mach_write_to_2(hdr + HEAP_HEADER_FREE_SIZE, free_size - rec->size);
    mach_write_to_2(hdr + HEAP_HEADER_ROWS, rows + 1);
    memcpy(page + lower, (const byte *)rec, rec->size);

    // 5. redo
    mlog_write_log(MLOG_HEAP_INSERT, block->get_space_id(), block->get_page_no(), NULL, 0, mtr);
    mlog_catenate_uint32(mtr, lower + rec->size, MLOG_2BYTES);
    mlog_catenate_uint32(mtr, free_size - rec->size, MLOG_2BYTES);
    mlog_catenate_uint32(mtr, rows + 1, MLOG_2BYTES);
    mlog_catenate_string(mtr, (byte *)dir, sizeof(row_dir_t));
    mlog_catenate_string(mtr, (byte *)rec, rec->size);
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
    uint8 itl_id;
    itl_t* itl = heap_alloc_itl(block, sess->trx, &mtr, &itl_id);
    ut_ad(itl);
    heap_row_set_itl_id(row, itl_id);

    //
    undo_data_t undo_data;
    undo_data.undo_op = UNDO_INSERT_OP;
    undo_data.query_min_scn = query_min_scn;
    undo_data.rec.data_size = 0;
    //if (cursor->nologging_type != SESSION_LEVEL) {
        if (trx_undo_prepare(sess, &undo_data, &mtr) != CM_SUCCESS) {
            ret = CM_ERROR;
            goto err_exit;
        }
    //}

    heap_insert_row_into_page(block, row, sess->cid, &undo_data, &mtr);

    trx_undo_write_log_rec(sess, &undo_data, &mtr);

    // change catagory of page
    uint16 avail = heap_get_page_free_space(block);
    uint8 category = fsm_space_avail_to_category(table, avail);
    if (category != search_path.category) {
        fsm_recursive_set_catagory(table, search_path, category, &mtr);
    }

    // add page to fast_clean_page_list
    sess_append_fast_clean_page_list(sess, block, itl_id);

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

    if (heap_insert_row(sess, insert_node->table, row) != CM_SUCCESS) {
        ret = CM_ERROR;
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

status_t heap_delete()
{
    return CM_SUCCESS;
}

status_t heap_update()
{
    return CM_SUCCESS;
}

// retrieve tuple with given tid.
// tuple->t_self is the TID to fetch.
// We pin the buffer holding the tuple, fill in the remaining fields of tuple.
//status_t heap_fetch(Snapshot snapshot,     HeapTuple tuple, Buffer *userbuf)
//{
//    return CM_SUCCESS;
//}
//
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
