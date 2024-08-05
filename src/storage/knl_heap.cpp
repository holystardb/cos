#include "knl_heap.h"

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
uint32 heap_create_entry(uint32        space_id, char* table_name)
{
    mtr_t mtr;
    uint32 page_no = fsm_create(space_id);

    mtr_start(&mtr);

    const page_size_t page_size(0);
    buf_block_t* block[8] = {NULL};
    for (uint32 i = 0; i < 8; i++) {
        block[i] = fsp_alloc_free_page(space_id, page_size, mtr);
        if (block[i] == NULL) {
            LOG_ERROR(LOGGER,
                      "failed to create heap entry of table, space id %u table name %s",
                      space_id, table_name);
            goto err_exit;
        }
    }

    uint32 n = fseg_get_n_frag_pages(inode, mtr);
    for (uint32 i = 0; i < 8; i++) {
        fseg_set_nth_frag_page_no(inode, n + i, block[i]->get_page_no(), mtr);
        fsm_add_free_page(space_id, block2->get_page_no(), block[i]->get_page_no(), mtr);
    }

    mtr_commit(&mtr);

    return page_no;

err_exit:

    mtr.modifications = FALSE;
    mtr_commit(&mtr);

    for (uint32 i = 0; i < 8; i++) {
        const page_id_t page_id(space_id, block[i]->get_page_no());
        fsp_free_page(page_id, page_size, mtr);
    }

    return page_no;
}

static inline itl_t* heap_get_itl(page_t* page, uint8 id)
{
    return (itl_t *)(page + UNIV_PAGE_SIZE - FIL_PAGE_DATA_END - (id + 1) * sizeof(trx_itl_t));
}

static itl_t* heap_alloc_itl(buf_block_t* block, trx_t* trx, mtr_t* mtr, uint8* itl_id)
{
    itl_t* ret_itl = NULL;
    page_t* page = buf_block_get_frame(block);
    heap_page_header_t* header = page + HEAP_HEADER_OFFSET;
    uint32   itl_count = mach_read_from_2(header + HEAP_HEADER_ITLS);

    for (uint32 i = 0; i < itl_count i++) {
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
            }
            // Continue searching,
            // Check if transaction already has an ITL
            continue;
        }

        trx_status_t trx_status;
        trx_get_status_by_itl(itl, &trx_status);
        if (trx_status.status != XACT_END) {
            continue;
        }

        if (ret_itl == NULL) {
            ret_itl = itl;
        }
    }

    if (ret_itl == NULL) {
        ret_itl = heap_create_itl(header, mtr);
    } else {
        ret_itl = heap_reuse_itl(header, mtr);
    }

    return ret_itl;
}

inline row_header_t* heap_get_row_by_dir(page_t* page, row_dir_t* dir)
{
    return (row_header_t *)(page + dir->offset);
}

inline uint8 heap_row_get_itl_id(row_header_t* row)
{
    return row->itl_id;
}

inline void heap_row_set_itl_id(row_header_t* row, uint8 itl_id)
{
    row->itl_id = itl_id;
}


void heap_reuse_itl1(session_t* session, page_t* page, trx_itl_t* itl, uint8 itl_id)
{
    heap_page_header_t* header = HEAP_PAGE_GET_HEADER(page);
    row_dir_t* dir;
    row_header_t* row;

    for (uint32 i = 0; i < header->dirs; i++) {
        dir = heap_get_dir(page, i);
        if (dir->is_free) {
            continue;
        }

        row = heap_get_row_by_dir(page, dir);
        if (heap_row_get_itl_id(row) != itl_id) {
            continue;
        }

        heap_row_set_itl_id(row, HEAP_INVALID_ITL_ID);
        if (row->is_change) {
            row->is_change = 1;
            continue;
        }

        dir->scn = itl->scn;
        dir->is_overwrite_scn = itl->is_overwrite_scn;
        if (row->is_deleted) {
            dir->is_free = 1;
            dir->next_free_dir = header->first_free_dir;
            header->first_free_dir = i;
        }
    }
}

void heap_organize_page(session_t* session, page_t* page, mtr_t* mtr)
{
}

trx_itl_t* heap_create_itl(session_t* session, page_t* page, mtr_t* mtr, uint8* itl_id)
{
    trx_itl_t* itl;
    heap_page_header_t* header = HEAP_PAGE_GET_HEADER(page);

    if (header->used_itls + header->free_itls >= HEAP_PAGE_MAX_ITLS ||
        header->free_size < sizeof(trx_itl_t)) {
        return NULL;
    }

    if (header->lower + sizeof(trx_itl_t) > header->upper) {
        heap_organize_page(session, header, mtr);
    }

    page_t* page;
    char* dest = (char *)page + header->upper - sizeof(trx_itl_t);
    if (header->dirs > 0) {
        memmove(dest, (char *)page + header->upper, header->dirs * sizeof(row_dir_t));

    }

    itl = (trx_itl_t *)((char *)page + header->upper + header->dirs * sizeof(row_dir_t));
    //header

    header->used_itls++;
    header->upper -= sizeof(trx_itl_t);
    header->free_size -= sizeof(trx_itl_t);
}

itl_t* heap_alloc_itl(session_t* session, page_t* page, mtr_t* mtr, uint8* itl_id)
{
    itl_t* itl;
    itl_t* ret_itl = NULL;
    bool32 is_free_itl = FALSE;
    heap_page_header_t* header = HEAP_PAGE_GET_HEADER(page);

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
        ret_itl = heap_create_itl(header, mtr);
    } else {
        heap_reuse_itl(header, ret_itl, is_free_itl, mtr);
    }

    return ret_itl;
}

bool32 heap_lock_row(session_t* session, page_t* page, row_header_t* row, mtr_t* mtr)
{
    itl_t* itl;
    heap_page_header_t* header = HEAP_PAGE_GET_HEADER(page);

    if (heap_row_get_itl_id(row) != HEAP_INVALID_ITL_ID) {
        itl = heap_get_itl(page, heap_row_get_itl_id(row));
        if (itl->slot.id == session->slot.id) {
            // row is locked
            return TRUE;
        }

        trx_status_t trx_status;
        trx_get_status_by_itl(itl, &trx_status);
        if (trx_status.status != XACT_END) {
            // wait and retry
        }
    }

    uint8 itl_id;
    itl = heap_alloc_itl(session, page, mtr, &itl_id);
    if (itl == NULL) {
        // wait and retry
    }

    row_dir_t* dir = heap_get_dir(page, row->);
    heap_row_set_itl_id(row, itl_id)
}

uint16 heap_get_insert_size(uint16 row_size, bool32 is_alloc_itl)
{
    uint16 size;

    size = sizeof(row_dir_t) + row_size + is_alloc_itl ? sizeof(trx_itl_t) : 0;

    return size;
}


static void heap_page_init(buf_block_t* block, dict_table_t* table, mtr_t* mtr)
{
    page_t* page = buf_block_get_frame(block);
    mlog_write_uint32(page + FIL_PAGE_TYPE, FIL_PAGE_HEAP_DATA, MLOG_2BYTES, mtr);

    uint16 lower = FIL_PAGE_DATA + HEAP_HEADER_SIZE;
    uint16 upper = UNIV_PAGE_SIZE_DEF - FIL_PAGE_DATA_END - sizeof(trx_itl_t) * table->init_trans;
    char str[HEAP_HEADER_SIZE];
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
    const page_size_t page_size(0);
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
void RelationPutHeapTuple(Relation relation,
					 Buffer buffer,
					 HeapTuple tuple,
					 bool token)
{

}


// Builds a ROW_FORMAT=COMPACT record out of a data tuple
static inline void heap_convert_dtuple_to_rec(dict_table_t* table, dtuple_t* tuple, rec_header_t* rec)
{
    byte*  nulls_ptr;
    byte*  data;
    uint32 null_bytes = REC_NULL_BITS_IN_BYTES(table->n_cols);

    rec->column_count = table->n_cols;
    rec->flags = 0;
    rec->size = OFFSET_OF(rec_header_t, null_bits) + null_bytes;
    nulls_ptr = &rec->null_bits;
    memset(nulls_ptr, 0x00, null_bytes);
    data = (byte*)rec + rec->size;

    for (uint32 i = 0; i < tuple->n_fields; i++) {
        const dfield_t* field = tuple->fields[i];

        if (dfield_is_null(field)) {
            nulls_ptr += (i / 8);
            *nulls_ptr |= (1 << (7 - i / 8));
            continue;
        }

        //if (dfield_is_ext(field)) {
        //}

        memcpy(data, dfield_get_data(field), dfield_get_len(field));
        data += dfield_get_len(field);
        rec->size += dfield_get_len(field);
    }
}


static inline rec_header_t* heap_prepare_insert(que_sess_t* sess, dict_table_t* table, dtuple_t* tuple)
{
    rec_header_t* rec;
    memory_stack_context_t* context;

    rec = (rec_header_t*)mcontext_stack_push(context, ROW_MAX_SIZE);

    heap_convert_dtuple_to_rec(table, tuple, rec);

    return rec;
}

#define DICT_NEED_REDO(table) (!((table)->type == DICT_TYPE_TABLE_NOLOGGING || (table)->type == DICT_TYPE_TEMP_TABLE_SESSION))

static inline row_dir_t *heap_get_dir(page_t* page, uint32 slot)
{
    uint32 offset;

    offset = UNIV_PAGE_SIZE - FIL_PAGE_DATA_END;
    offset -= mach_read_from_2(page + HEAP_HEADER_OFFSET + HEAP_HEADER_ITLS) * sizeof(itl_t);
    offset -= (*slot + 1) * sizeof(row_dir_t);

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
        dir->undo_page_no = TRX_UNDO_INVALID_PAGENO;
        dir->undo_page_offset = 0;
        dir->is_free = 1;
        dir->is_overwrite_scn = 0;

        uint16 offset = mach_read_from_2(hdr + HEAP_HEADER_UPPER) - sizeof(row_dir_t);
        mlog_write_uint32(hdr + HEAP_HEADER_UPPER, offset, MLOG_2BYTES, mtr);
    } else {
        dir = heap_get_dir(page, *dir_slot);

        mlog_write_uint32(hdr + HEAP_HEADER_FIRST_FREE_DIR, dir->next_free_dir, MLOG_2BYTES, mtr);
    }

    return dir;
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
    row_head_t* row = NULL;
    page_t *page = buf_block_get_frame(block);
    heap_page_header_t* header = page + HEAP_HEADER_OFFSET;
    uint32 dir_count = mach_read_from_2(header + HEAP_HEADER_DIRS);

    for (uint32 i = 0; i < dir_count i++) {
        dir = heap_get_dir(page, i);
        if (dir->is_free) {
            continue;
        }

        row = HEAP_GET_ROW(page, dir);
        if (row->is_deleted) {
            LOGGER_PANIC_CHECK(LOGGER, ROW_ITL_ID(row) != HEAP_INVALID_ITL_ID,
                "row itl id is invalid, space id %u page no %u page type %u",
                block->get_space_id(), block->get_page_no(), block->get_page_type());

            itl_t* itl = heap_get_itl(page, ROW_ITL_ID(row));
            if (!itl->is_active) {
                ROW_SET_ITL_ID(row, HEAP_INVALID_ITL_ID);
                dir->scn = itl->scn;
                dir->is_overwrite_scn = itl->is_overwrite_scn;
                dir->is_free = 1;
                dir->next_free_dir = mach_read_from_2(header + HEAP_HEADER_FIRST_FREE_DIR);
                mach_write_to_2(header + HEAP_HEADER_FIRST_FREE_DIR, i);
                continue;
            }
        }

        /*
         * the max page size is 32K(0x8000) which means the max row size in
         * page is less than 32K, and the row->size is 2bytes(max size 64K),
         * so we can temporarily use the high bits as compacting mask
         */
        LOGGER_PANIC_CHECK(LOGGER, (row->size & ROW_COMPACTING_MASK) == 0,
            "current row is compacting, space id %u page no %u page type %u",
            block->get_space_id(), block->get_page_no(), block->get_page_type());

        row->size |= ROW_COMPACTING_MASK;

        // temporarily save row slot to row->sprs_count, so we can use row itself to find it's dir
        dir->offset = row->sprs_count;
        row->sprs_count = i;
    }

    // traverse row one by one from first row after heap page head
    uint16 lower = mach_read_from_2(header + HEAP_HEADER_LOWER)
    row = (row_head_t *)((char *)page + HEAP_HEADER_SIZE);
    row_head_t* free_addr = row;

    while ((char *)row < (char *)page + lower) {
        if ((row->size & ROW_COMPACTING_MASK) == 0) {
            // row has been deleted, compact it's space directly
            row = (row_head_t *)((char *)row + row->size);
            continue;
        }

        // don't clear the compacting mask here, just get the actual row size
        uint16 row_size = (row->size & ~ROW_COMPACTING_MASK);

        LOGGER_PANIC_CHECK(LOGGER, row->sprs_count < dir_count,
            "count of column is more than page's dirs, space id %u page no %u page type %u sprs_count %u dirs %u",
            block->get_space_id(), block->get_page_no(), block->get_page_type() row->sprs_count, dir_count);

        dir = heap_get_dir(page, row->sprs_count);

        // move current row to the new compacted position
        uint16 copy_size = (row->is_link) ? (uint16)HEAP_MIN_ROW_SIZE : row_size;
        if (row != free_addr && copy_size != 0) {
            memmove(free_addr, row, copy_size);
        }

        /* restore the row and its dir */
        free_addr->size = copy_size;
        free_addr->sprs_count = (dir->offset);
        dir->offset = (uint16)((char *)free_addr - (char *)page);

        // now, handle the next row
        free_addr = (row_head_t *)((char *)free_addr + copy_size);
        row = (row_head_t *)((char *)row + row_size);
    }

    // reset the latest page free begin position,
    // free_addr - page is less than DEFAULT_PAGE_SIZE(8192)
    mach_write_to_2(header + HEAP_HEADER_UPPER, (uint16)((char *)free_addr - (char *)page));
}

static void heap_insert_row_into_page(    buf_block_t* block, rec_header_t *rec,
    command_id_t cid, undo_data_t* undo, trx_undo_page_t* undo_page, mtr_t* mtr)
{
    page_t* page = buf_block_get_frame(block);
    heap_page_header_t* hdr = page + HEAP_HEADER_OFFSET;
    uint16 upper = mach_read_from_2(hdr + HEAP_HEADER_UPPER);
    uint16 lower = mach_read_from_2(hdr + HEAP_HEADER_LOWER);

    // check compact
    if (lower + rec->size + sizeof(row_dir_t) > upper) {
        heap_reorganize_page(block);
        mlog_write_log(MLOG_PAGE_REORGANIZE, block->get_space_id(), block->get_page_no(), NULL, 0, mtr);
    }

    uint16 dir_slot;
    row_dir_t* dir = heap_alloc_free_dir(page, &dir_slot, mtr);
    dir->is_free = 0;
    dir->scn = cid;
    dir->is_overwrite_scn = 0;
    dir->offset = lower;  // new position, dont overwrite old data
    // Sets roll ptr field of row
    dir->undo_page_no = undo_page->page_no;
    dir->undo_page_offset = undo_page->offset;
    //row->is_changed = 1;

    undo->type = UNDO_HEAP_INSERT;
    undo->dir = dir; // old dir value
    undo->row_id.space_id = block->get_space_id();
    undo->row_id.page_no = block->get_page_no();
    undo->row_id.dir_slot = dir_slot;
    undo->data_size = 0;

    // insert row
    uint32 rows = mach_read_from_2(hdr + HEAP_HEADER_ROWS);
    uint32 free_size = mach_read_from_2(hdr + HEAP_HEADER_FREE_SIZE);
    mach_write_to_2(hdr + HEAP_HEADER_LOWER, lower + rec->size);
    mach_write_to_2(hdr + HEAP_HEADER_FREE_SIZE, free_size - rec->size);
    mach_write_to_2(hdr + HEAP_HEADER_ROWS, rows + 1);
    memcpy(page + lower, (const byte *)rec, rec->size);

    mlog_write_log(HEAP_INSERT, block->get_space_id(), block->get_page_no(), NULL, 0, mtr);
    mlog_catenate_uint32(mtr, lower + rec->size, MLOG_2BYTES);
    mlog_catenate_uint32(mtr, free_size - rec->size, MLOG_2BYTES);
    mlog_catenate_uint32(mtr, rows + 1, MLOG_2BYTES);
    mlog_catenate_string(mtr, (byte *)dir, sizeof(row_dir_t));
    mlog_catenate_string(mtr, (byte *)rec, rec->size);
}


static status_t heap_insert_row(que_sess_t *sess, dict_table_t* table, rec_header_t *rec)
{
    status_t  ret = CM_SUCCESS;
    mtr_t mtr;
    uint32 cost_size;
    fsm_search_path_t search_path;
    buf_block_t* block;
    uint64 query_min_scn;
    heap_insert_assist_t assist;

    mtr_start(&mtr);

    assist.need_redo = DICT_NEED_REDO(table);
    if (sess->attr->attr_rep.enable_logical_rep) {
        assist.table_id = table->id;
    }

    cost_size = rec->size + sizeof(itl_t) + sizeof(row_dir_t);
    block = heap_find_free_page(table, cost_size, search_path, &mtr);
    if (block == NULL) {
        ret = CM_ERROR;
        goto err_exit;
    }

    // set itl
    uint8 itl_id;
    itl_t* itl = heap_alloc_itl(block, trx, mtr, itl_id);
    ROW_SET_ITL_ID(rec, itl_id);

    //if (cursor->nologging_type != SESSION_LEVEL) {
        trx_undo_page_t* undo_page;
        undo_page = trx_undo_prepare(sess, TRX_UNDO_INSERT, sizeof(row_dir_t), query_min_scn, mtr);
        if (undo_page == NULL) {
            ret = CM_ERROR;
            goto err_exit;
        }
    //}

    undo_data_t undo_data;
    row_dir_t* dir;
    heap_insert_row_into_page(block, rec, sess->cid, &undo_data, undo_page, mtr);

    trx_undo_write(sess, undo, mtr);

    // change catagory of page
    uint16 avail = heap_get_page_free_space(block);
    uint8 category = fsm_space_avail_to_category(table, avail);
    if (category != search_path.category) {
        fsm_recursive_set_catagory(table, search_path, category, &mtr);
    }

err_exit:

    mtr_commit(&mtr);

    return ret;
}

static status_t heap_chain_insert_row(que_sess_t *sess, dict_table_t* table, rec_header_t *rec)
{
    return CM_SUCCESS;
}

status_t heap_insert(que_sess_t* sess, insert_node_t* insert_node)
{
    dict_table_t* table = insert_node->table;
    heap_tuple_t* tuple;
    uint32 options;

    status_t ret = CM_SUCCESS;
    mtr_t mtr;
    trx_t* trx;

    uint64 xid;
    rec_header_t* row;

    // Fill in tuple header fields and toast the tuple if necessary
    row = heap_prepare_insert(sess, insert_node->table, insert_node->heap_row);

    mtr_start(&mtr);

    trx_start_if_not_started(sess, mtr);

    // 
    //if (lock_table_shared(session, cursor->dc_entity, LOCK_INF_WAIT) != CT_SUCCESS) {
    //    err = CM_ERROR;
    //    goto err_exit;
    //}

    if (row->size <= HEAP_ROW_MAX_SIZE) {
        if (heap_insert_row(sess, table, row) != CM_SUCCESS) {
            ret = CM_ERROR;
            goto err_exit;
        }
    } else {
        if (heap_chain_insert_row(sess, table, row) != CM_SUCCESS) {
            ret = CM_ERROR;
            goto err_exit;
        }
    }

err_exit:

    mtr_commit(&mtr);

    return ret;
}

status_t heap_multi_insert()
{
    return CM_SUCCESS;
}

status_t heap_delete(ItemPointer tid, CommandId cid)
{
    return CM_SUCCESS;
}

status_t heap_update(ItemPointer otid, HeapTuple newtup, CommandId cid)
{
    return CM_SUCCESS;
}

// retrieve tuple with given tid.
// tuple->t_self is the TID to fetch.
// We pin the buffer holding the tuple, fill in the remaining fields of tuple.
status_t heap_fetch(Snapshot snapshot,     HeapTuple tuple, Buffer *userbuf)
{
    return CM_SUCCESS;
}

status_t heap_getnext(TableScanDesc sscan, ScanDirection direction)
{
    return CM_SUCCESS;
}

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