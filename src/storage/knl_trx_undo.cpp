#include "knl_trx_undo.h"
#include "knl_trx.h"
#include "knl_mtr.h"
#include "knl_trx_rseg.h"

const uint16 TRX_UNDO_PAGE_REUSE_LIMIT = (UNIV_PAGE_SIZE / 4);

const uint16 undo_rec_max_size = UNIV_PAGE_SIZE - FIL_PAGE_DATA - FIL_PAGE_DATA_END
    - TRX_UNDO_PAGE_HDR_SIZE - TRX_UNDO_SEG_HDR_SIZE - TRX_UNDO_LOG_HDR_SIZE;

static inline void trx_undo_page_redo(
    page_t* undo_page,  // in: undo log page
    uint32  old_free,   // in: start offset of the inserted entry
    uint32  new_free,   // in: end offset of the entry
    mtr_t*  mtr);



// Builds a roll pointer
static inline roll_ptr_t trx_undo_build_roll_ptr(
    bool32 is_insert, // in: TRUE if insert undo log
    uint32 rseg_id, // in: rollback segment id
    uint32 page_no, // in: page number
    uint32 offset)  // in: offset of the undo entry within page
{
    roll_ptr_t roll_ptr;

    ut_ad(is_insert == 0 || is_insert == 1);
    ut_ad(rseg_id < TRX_RSEG_MAX_COUNT);
    ut_ad(offset < UNIV_PAGE_SIZE);

    roll_ptr = (roll_ptr_t) is_insert << 55
        | (roll_ptr_t) rseg_id << 48
        | (roll_ptr_t) page_no << 16
        | offset;
    return(roll_ptr);
}

// Decodes a roll pointer
static inline void trx_undo_decode_roll_ptr(
    roll_ptr_t  roll_ptr,  // in: roll pointer
    bool32*     is_insert, // out: TRUE if insert undo log
    uint32*     rseg_id,   // out: rollback segment id
    uint32*     page_no,   // out: page number
    uint32*     offset)    // out: offset of the undo entry within page
{
    ut_ad(roll_ptr < (1ULL << 56));
    *offset = (uint32) roll_ptr & 0xFFFF;
    roll_ptr >>= 16;
    *page_no = (uint32) roll_ptr & 0xFFFFFFFF;
    roll_ptr >>= 32;
    *rseg_id = (uint32) roll_ptr & 0x7F;
    roll_ptr >>= 7;
    *is_insert = (bool32) roll_ptr;
}

static inline bool32 trx_undo_roll_ptr_is_insert(roll_ptr_t roll_ptr)
{
    ut_ad(roll_ptr < (1ULL << 56));
    return((bool32)(roll_ptr >> 55));
}





// Reuses a cached undo log
// if TRX_UNDO_UPDATE and size == 0, need a new page
static inline trx_undo_page_t* trx_undo_reuse_page(
    trx_rseg_t* rseg, uint32 type, uint16 size, uint64 min_scn)
{
    trx_undo_page_t*  undo_page;

    mutex_enter(&(rseg->undo_cache_mutex));

    if (type == TRX_UNDO_INSERT) {
        SLIST_GET_AND_REMOVE_FIRST(list_node, rseg->insert_undo_cache, undo_page);
    } else {
        // 1. get from undo cache
        trx_undo_page_t* prev = NULL;
        undo_page = SLIST_GET_FIRST(rseg->update_undo_cache);
        while (undo_page && (undo_page->free_size < size || undo_page->scn < min_scn)) {
            prev = undo_page;
            undo_page = SLIST_GET_NEXT(list_node, undo_page);
        }
        if (undo_page) {
            SLIST_REMOVE(list_node, rseg->update_undo_cache, prev, undo_page);
        } else {
            // 2. get from insert cache
            SLIST_GET_AND_REMOVE_FIRST(list_node, rseg->insert_undo_cache, undo_page);
        }
    }

    if (undo_page == NULL) {
        undo_page = SLIST_GET_FIRST(rseg->history_undo_list);
        if (undo_page && undo_page->scn < min_scn) {
            SLIST_GET_AND_REMOVE_FIRST(list_node, rseg->history_undo_list, undo_page);
        } else {
            undo_page = NULL;
        }
    }

    mutex_exit(&(rseg->undo_cache_mutex));

    return undo_page;
}

// Initializes the fields in an undo log segment page
static inline void trx_undo_page_init(trx_undo_page_t* undo_page, uint32 type)
{
    const page_id_t page_id(FIL_UNDO_SPACE_ID, undo_page->page_no);
    const page_size_t page_size(0);

    mtr_t mtr;

    mtr_start(&mtr);

    undo_page->block = buf_page_get_gen(page_id, page_size, RW_NO_LATCH, Page_fetch::RESIDENT, &mtr);
    ut_a(undo_page->block);
    page_t* page = buf_block_get_frame(undo_page->block);

    mach_write_to_2(page + FIL_PAGE_TYPE, FIL_PAGE_UNDO_LOG);

    trx_undo_page_hdr_t* page_hdr = page + TRX_UNDO_PAGE_HDR;
    mach_write_to_2(page_hdr + TRX_UNDO_PAGE_TYPE, type);
    mach_write_to_2(page_hdr + TRX_UNDO_PAGE_START, TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_HDR_SIZE);
    mach_write_to_2(page_hdr + TRX_UNDO_PAGE_FREE, TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_HDR_SIZE);

    // redo log
    mlog_write_initial_log_record(page, MLOG_UNDO_INIT, &mtr);
    mlog_catenate_uint32_compressed(&mtr, type);

    mtr_commit(&mtr);
}


// Creates a new undo log header in file.
// NOTE that this function has its own log record type MLOG_UNDO_HDR_CREATE.
// You must NOT change the operation of this function!
// return undo record byte offset on page
static inline void trx_undo_append_update_undo_page_list(
    trx_undo_page_t* undo_page, trx_t* trx, uint64 min_scn, mtr_t* mtr)
{
    page_t* page = buf_block_get_frame(undo_page->block);


    // 0. 
    if (undo_page->type != TRX_UNDO_UPDATE || undo_page->scn < min_scn) {
        
    }

    // 1. undo log page header

    trx_undo_page_hdr_t* page_hdr = page + TRX_UNDO_PAGE_HDR;

    uint32 free = mach_read_from_2(page_hdr + TRX_UNDO_PAGE_FREE);
    uint32 new_free = free + TRX_UNDO_LOG_HDR_SIZE;
    ut_a(free + TRX_UNDO_LOG_XA_HDR_SIZE < UNIV_PAGE_SIZE - 100);

    mach_write_to_2(page_hdr + TRX_UNDO_PAGE_START, new_free);
    mach_write_to_2(page_hdr + TRX_UNDO_PAGE_FREE, new_free);


    // 2. undo segment heder
    uint32 prev_log = 0;
    if (SLIST_GET_LEN(trx->update_undo) == 0) {
        trx_undo_seg_hdr_t* seg_hdr = page + TRX_UNDO_SEG_HDR;
        mach_write_to_2(seg_hdr + TRX_UNDO_STATE, TRX_UNDO_PAGE_STATE_ACTIVE);

        flst_init(seg_hdr + TRX_UNDO_PAGE_LIST, mtr);

        prev_log = mach_read_from_2(seg_hdr + TRX_UNDO_LAST_LOG);
        if (prev_log != 0) {
            trx_undo_log_hdr_t* prev_log_hdr = page + prev_log;
            mach_write_to_2(prev_log_hdr + TRX_UNDO_NEXT_LOG, free);
        }

        mach_write_to_2(seg_hdr + TRX_UNDO_LAST_LOG, free);

    }

    // 3. undo log header

    trx_undo_log_hdr_t* log_hdr = page + free;

    mach_write_to_8(log_hdr + TRX_UNDO_TRX_ID, trx->trx_slot_id.id);
    mach_write_to_2(log_hdr + TRX_UNDO_LOG_START, new_free);
    mach_write_to_1(log_hdr + TRX_UNDO_XAID_EXISTS, FALSE);
    mach_write_to_1(log_hdr + TRX_UNDO_DICT_TRANS, FALSE);
    mach_write_to_2(log_hdr + TRX_UNDO_NEXT_LOG, 0);
    mach_write_to_2(log_hdr + TRX_UNDO_PREV_LOG, prev_log);

    // 4. Write the log record about the header creation
    mlog_write_initial_log_record(page, MLOG_UNDO_HDR_CREATE, mtr);
    mlog_catenate_uint64_compressed(mtr, trx->trx_slot_id.id);

    // 5. add flst

    trx_undo_page_t* seg_undo_page = SLIST_GET_FIRST(trx->update_undo);
    if (seg_undo_page) {
        page = buf_block_get_frame(seg_undo_page->block);
        flst_add_last(page + TRX_UNDO_SEG_HDR + TRX_UNDO_PAGE_LIST,
            page_hdr + TRX_UNDO_PAGE_NODE, mtr);
    }
    SLIST_ADD_LAST(list_node, trx->update_undo, undo_page);

    // 6.
    undo_page->offset = new_free;
    undo_page->type = TRX_UNDO_UPDATE;
}

// Initializes a cached insert undo log header page for new use
// return undo record byte offset on page
static inline void trx_undo_append_insert_undo_page_list(
    trx_undo_page_t* undo_page, trx_t* trx, mtr_t* mtr)
{
    page_t* page = buf_block_get_frame(undo_page->block);


    // 1. undo log page header

    trx_undo_page_hdr_t* page_hdr = page + TRX_UNDO_PAGE_HDR;
    uint32 new_free = TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE + TRX_UNDO_LOG_HDR_SIZE;
    mach_write_to_2(page_hdr + TRX_UNDO_PAGE_TYPE, TRX_UNDO_INSERT);
    mach_write_to_2(page_hdr + TRX_UNDO_PAGE_START, new_free);
    mach_write_to_2(page_hdr + TRX_UNDO_PAGE_FREE, new_free);

    // 2. undo segment header

    if (SLIST_GET_LEN(trx->insert_undo) == 0) {
        trx_undo_seg_hdr_t* seg_hdr = page + TRX_UNDO_SEG_HDR;
        mach_write_to_2(seg_hdr + TRX_UNDO_STATE, TRX_UNDO_PAGE_STATE_ACTIVE);
        flst_init(seg_hdr + TRX_UNDO_PAGE_LIST, mtr);
    }

    // 3. undo log header

    trx_undo_log_hdr_t* log_hdr = page + TRX_UNDO_LOG_HDR;
    mach_write_to_8(log_hdr + TRX_UNDO_TRX_ID, trx->trx_slot_id.id);
    mach_write_to_2(log_hdr + TRX_UNDO_LOG_START, new_free);
    mach_write_to_1(log_hdr + TRX_UNDO_XAID_EXISTS, FALSE);
    mach_write_to_1(log_hdr + TRX_UNDO_DICT_TRANS, FALSE);

    // 4. Write the log record MLOG_UNDO_HDR_REUSE or MLOG_UNDO_HDR_CREATE
    if (SLIST_GET_LEN(trx->insert_undo) == 0) {
        // an undo log header initialization.
        mlog_write_initial_log_record(page, MLOG_UNDO_HDR_CREATE, mtr);
    } else {
        // an undo log header reuse
        mlog_write_initial_log_record(page, MLOG_UNDO_HDR_REUSE, mtr);
    }
    mlog_catenate_uint64(mtr, trx->trx_slot_id.id);

    // 5. add flst

    trx_undo_page_t* seg_undo_page = SLIST_GET_FIRST(trx->insert_undo);
    if (seg_undo_page) {
        page = buf_block_get_frame(seg_undo_page->block);
        flst_add_last(page + TRX_UNDO_SEG_HDR + TRX_UNDO_PAGE_LIST,
            page_hdr + TRX_UNDO_PAGE_NODE, mtr);
    }
    SLIST_ADD_LAST(list_node, trx->insert_undo, undo_page);

    // 6.
    undo_page->offset = TRX_UNDO_LOG_HDR + TRX_UNDO_LOG_HDR_SIZE;
    undo_page->type = TRX_UNDO_INSERT;
}


static trx_undo_page_t* trx_get_undo_page_from_other_rseg(
    trx_t* trx, uint32 type, uint16 size, uint64 min_scn)
{
    uint32 cur_rseg_id = trx->trx_slot_id.rseg_id;
    trx_undo_page_t* undo_page;
    trx_rseg_t* rseg;
    uint32 page_count = 0, rseg_id = cur_rseg_id;
    uint32 insert_cache_page_count = 0, insert_rseg_id = cur_rseg_id;
    uint32 update_cache_page_count = 0, update_rseg_id = cur_rseg_id;
    uint32 history_page_count = 0, history_rseg_id = cur_rseg_id;
    uint32 history_first_page_scn = 0;
    uint32 scn_rseg_id = cur_rseg_id;

    for (uint32 i = 0; i < trx_sys->rseg_count && i < TRX_RSEG_MAX_COUNT; i++) {
        if (i == cur_rseg_id) {
            continue;
        }

        rseg = TRX_GET_RSEG(i);

        mutex_enter(&rseg->undo_cache_mutex);

        if (insert_cache_page_count < SLIST_GET_LEN(rseg->insert_undo_cache)) {
            insert_cache_page_count = SLIST_GET_LEN(rseg->insert_undo_cache);
            insert_rseg_id= i;
        }
        if (update_cache_page_count < SLIST_GET_LEN(rseg->update_undo_cache)) {
            update_cache_page_count = SLIST_GET_LEN(rseg->update_undo_cache);
            update_rseg_id= i;
        }
        if (history_page_count < SLIST_GET_LEN(rseg->history_undo_list)) {
            history_page_count = SLIST_GET_LEN(rseg->history_undo_list);
            history_rseg_id= i;
        }
        if (page_count < SLIST_GET_LEN(rseg->history_undo_list) +
                SLIST_GET_LEN(rseg->insert_undo_cache) +
                SLIST_GET_LEN(rseg->update_undo_cache)) {
            page_count = SLIST_GET_LEN(rseg->history_undo_list) +
                SLIST_GET_LEN(rseg->insert_undo_cache) +
                SLIST_GET_LEN(rseg->update_undo_cache);
            history_rseg_id = i;
        }

        undo_page = SLIST_GET_FIRST(rseg->history_undo_list);
        if (undo_page->scn < min_scn) {
            scn_rseg_id= i;
        }

        mutex_exit(&rseg->undo_cache_mutex);
    }

    if (history_rseg_id != cur_rseg_id) {
        undo_page = trx_undo_reuse_page(TRX_GET_RSEG(history_rseg_id), type, size, min_scn);
        if (undo_page) {
            return undo_page;
        }
    }

    if (type == TRX_UNDO_INSERT && insert_rseg_id != cur_rseg_id) {
        undo_page = trx_undo_reuse_page(TRX_GET_RSEG(insert_rseg_id), type, size, min_scn);
        if (undo_page) {
            return undo_page;
        }
    }

    if (type == TRX_UNDO_UPDATE && update_rseg_id != cur_rseg_id) {
        undo_page = trx_undo_reuse_page(TRX_GET_RSEG(update_rseg_id), type, size, min_scn);
        if (undo_page) {
            return undo_page;
        }
    }

    if (scn_rseg_id != cur_rseg_id) {
        undo_page = trx_undo_reuse_page(TRX_GET_RSEG(scn_rseg_id), type, size, min_scn);
        if (undo_page) {
            return undo_page;
        }
    }

    return NULL;
}


// Assigns an undo log page for a transaction.
static trx_undo_page_t* trx_get_undo_page(trx_t* trx, uint32 type, uint16 size, uint64 min_scn)
{
    trx_rseg_t* rseg;
    trx_undo_page_t* undo_page;

    rseg = TRX_GET_RSEG(trx->trx_slot_id.rseg_id);

    // 1 get undo page from cache of current rseg
    undo_page = trx_undo_reuse_page(rseg, type, size, min_scn);
    if (undo_page) {
        return undo_page;
    }

    // 2 get undo page from max cache of other rseg
    undo_page = trx_get_undo_page_from_other_rseg(trx, type, size, min_scn);
    if (undo_page) {
        return undo_page;
    }

    // 3 get undo page from insert undo cache of other rseg
    for (uint32 i = 0; i < trx_sys->rseg_count && i < TRX_RSEG_MAX_COUNT; i++) {
        rseg = TRX_GET_RSEG((i + trx->trx_slot_id.rseg_id) % trx_sys->rseg_count);

        mutex_enter(&rseg->undo_cache_mutex);
        SLIST_GET_AND_REMOVE_FIRST(list_node, rseg->insert_undo_cache, undo_page);
        mutex_exit(&rseg->undo_cache_mutex);
        if (undo_page) {
            return undo_page;
        }
    }

    // 4 get undo page from history of other rseg
    for (uint32 i = 0; i < trx_sys->rseg_count && i < TRX_RSEG_MAX_COUNT; i++) {
        rseg = TRX_GET_RSEG((i + trx->trx_slot_id.rseg_id) % trx_sys->rseg_count);

        mutex_enter(&rseg->undo_cache_mutex);
        SLIST_GET_AND_REMOVE_FIRST(list_node, rseg->history_undo_list, undo_page);
        mutex_exit(&rseg->undo_cache_mutex);
        if (undo_page) {
            return undo_page;
        }
    }

    return NULL;
}

static inline void trx_slot_set_undo_page(trx_undo_page_t* undo_page,
    trx_slot_id_t slot_id, uint32 type, mtr_t* mtr)
{
    trx_slot_t* trx_slot = (trx_slot_t *)TRX_GET_RSEG_TRX_SLOT(slot_id);

#ifdef UNIV_DEBUG
    page_t* page = buf_block_get_frame(undo_page->block);
    ut_ad((byte *)trx_slot == (page + sizeof(trx_slot_t)));
    ut_ad((byte *)trx_slot == (page + (uint32)slot_id.slot * sizeof(trx_slot_t)));
#endif

    if (type == TRX_UNDO_INSERT) {
        mlog_write_uint32((byte *)trx_slot + TRX_RSEG_SLOT_INSERT_LOG_PAGE, undo_page->page_no, MLOG_4BYTES, mtr);
    } else {
        mlog_write_uint32((byte *)trx_slot + TRX_RSEG_SLOT_UPDATE_LOG_PAGE, undo_page->page_no, MLOG_4BYTES, mtr);
    }
}

static inline trx_undo_page_t* trx_undo_add_undo_page(
    trx_t* trx, uint32 type, uint16 size, uint64 min_scn, mtr_t* mtr)
{
    trx_undo_page_t* undo_page;

    ut_ad(type == TRX_UNDO_INSERT || type == TRX_UNDO_UPDATE);

    undo_page = trx_get_undo_page(trx, type, size, min_scn);
    if (undo_page == NULL) {
        return NULL;
    }

    if (undo_page->block == NULL) {
        trx_undo_page_init(undo_page, type);
    }

    if (type == TRX_UNDO_INSERT) {
        trx_undo_append_insert_undo_page_list(undo_page, trx, mtr);
    } else if (type == TRX_UNDO_UPDATE) {
        trx_undo_append_update_undo_page_list(undo_page, trx, size == 0 ? SCN_MAX : min_scn, mtr);
    }

    return undo_page;
}

static inline trx_undo_page_t* trx_undo_assign_undo_page(
    trx_t* trx, uint32 type, uint16 size, uint64 min_scn, mtr_t* mtr)
{
    trx_undo_page_t* undo_page;

    undo_page = trx_undo_add_undo_page(trx, type, size, min_scn, mtr);
    if (LIKELY(undo_page != NULL)) {
        trx_slot_set_undo_page(undo_page, trx->trx_slot_id, type, mtr);
    }

    return undo_page;
}

// Calculates the free space left for extending an undo log record.
static inline uint32 trx_undo_left(
    const page_t* page, /* in: undo log page */
    const byte* ptr) /* in: pointer to page */
{
    /* The '- 10' is a safety margin, in case we have some small calculation error below */
    return(UNIV_PAGE_SIZE - (ptr - page) - 10 - FIL_PAGE_DATA_END);
}


// Set the next and previous pointers in the undo page for the undo record that was written to ptr.
// Update the first free value by the number of bytes written for this undo record.
// return offset of the inserted entry on the page if succeeded, 0 if fail.
static uint32 trx_undo_page_set_next_prev_and_add(
    page_t* undo_page, // in/out: undo log page
    byte*   ptr, // in: ptr up to where data has been written on this undo page
    mtr_t*  mtr)
{
    uint32 first_free; /* offset within undo_page */
    uint32 end_of_rec; /* offset within undo_page */
    // pointer within undo_page that points to the next free offset value within undo_page
    byte*  ptr_to_first_free;

    ut_ad(ptr > undo_page);
    ut_ad(ptr < undo_page + UNIV_PAGE_SIZE);

    if (UNLIKELY(trx_undo_left(undo_page, ptr) < 2)) {
        return(0);
    }

    ptr_to_first_free = undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE;
    first_free = mach_read_from_2(ptr_to_first_free);

    /* Write offset of the previous undo log record */
    mach_write_to_2(ptr, first_free);
    ptr += 2;

    end_of_rec = ptr - undo_page;

    /* Write offset of the next undo log record */
    mach_write_to_2(undo_page + first_free, end_of_rec);

    /* Update the offset to first free undo record */
    mach_write_to_2(ptr_to_first_free, end_of_rec);

    /* Write this log entry to the UNDO redo log */
    trx_undo_page_redo(undo_page, first_free, end_of_rec, mtr);

    return(first_free);
}


static uint32 trx_undo_page_report_insert(dict_table_t* table, page_t* undo_page, trx_t* trx, mtr_t* mtr)
{
    uint32 first_free;
    byte* ptr;

    ut_ad(mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE) == TRX_UNDO_INSERT);

    first_free = mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE);
    ptr = undo_page + first_free;

    ut_ad(first_free <= UNIV_PAGE_SIZE);

    if (trx_undo_left(undo_page, ptr) < 2 + 1 + 11 + 11) {
        /* Not enough space for writing the general parameters */
        return(0);
    }

    /* Reserve 2 bytes for the pointer to the next undo log record */
    ptr += 2;

    /* Store first some general parameters to the undo log */
    //*ptr++ = TRX_UNDO_INSERT_REC;
    //ptr += mach_ull_write_much_compressed(ptr, trx->undo_no);
    ptr += mach_ull_write_much_compressed(ptr, table->id);

    /* Store then the fields required to uniquely determine the record
    to be inserted in the clustered index */
    /*
	for (i = 0; i < dict_index_get_n_unique(index); i++) {

		const dfield_t*	field = dtuple_get_nth_field(clust_entry, i);
		uint32 flen	= dfield_get_len(field);

		if (trx_undo_left(undo_page, ptr) < 5) {
			return(0);
		}

		ptr += mach_write_compressed(ptr, flen);
		if (flen != UNIV_SQL_NULL) {
			if (trx_undo_left(undo_page, ptr) < flen) {
				return(0);
			}

			ut_memcpy(ptr, dfield_get_data(field), flen);
			ptr += flen;
		}
	}
    */
	return trx_undo_page_set_next_prev_and_add(undo_page, ptr, mtr);
}

#if 0

// Reports in the undo log of an update or delete marking of a clustered index record.
// return byte offset of the inserted undo log entry on the page if succeed, 0 if fail.
static uint32 trx_undo_page_report_modify(
	page_t*		undo_page,	/*!< in: undo log page */
	trx_t*		trx,		/*!< in: transaction */
	dict_index_t*	index,		/*!< in: clustered index where update or
					delete marking is done */
	const rec_header_t*	rec,		/*!< in: clustered index record which
					has NOT yet been modified */
	const ulint*	offsets,	/*!< in: rec_get_offsets(rec, index) */
	const upd_t*	update,		/*!< in: update vector which tells the
					columns to be updated; in the case of
					a delete, this should be set to NULL */
	ulint		cmpl_info,	/*!< in: compiler info on secondary
					index updates */
	mtr_t*		mtr)		/*!< in: mtr */
{
	dict_table_t*	table;
	ulint		first_free;
	byte*		ptr;
	const byte*	field;
	ulint		flen;
	ulint		col_no;
	ulint		type_cmpl;
	byte*		type_cmpl_ptr;
	ulint		i;
	trx_id_t	trx_id;
	ibool		ignore_prefix = FALSE;
	byte		ext_buf[REC_VERSION_56_MAX_INDEX_COL_LEN
				+ BTR_EXTERN_FIELD_REF_SIZE];

	ut_a(dict_index_is_clust(index));
	ut_ad(rec_offs_validate(rec, index, offsets));
	ut_ad(mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR
			       + TRX_UNDO_PAGE_TYPE) == TRX_UNDO_UPDATE);
	table = index->table;

	first_free = mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR
				      + TRX_UNDO_PAGE_FREE);
	ptr = undo_page + first_free;

	ut_ad(first_free <= UNIV_PAGE_SIZE);

	if (trx_undo_left(undo_page, ptr) < 50) {

		/* NOTE: the value 50 must be big enough so that the general
		fields written below fit on the undo log page */

		return(0);
	}

	/* Reserve 2 bytes for the pointer to the next undo log record */
	ptr += 2;

	/* Store first some general parameters to the undo log */

	if (!update) {
		ut_ad(!rec_get_deleted_flag(rec, dict_table_is_comp(table)));
		type_cmpl = TRX_UNDO_DEL_MARK_REC;
	} else if (rec_get_deleted_flag(rec, dict_table_is_comp(table))) {
		type_cmpl = TRX_UNDO_UPD_DEL_REC;
		/* We are about to update a delete marked record.
		We don't typically need the prefix in this case unless
		the delete marking is done by the same transaction
		(which we check below). */
		ignore_prefix = TRUE;
	} else {
		type_cmpl = TRX_UNDO_UPD_EXIST_REC;
	}

	type_cmpl |= cmpl_info * TRX_UNDO_CMPL_INFO_MULT;
	type_cmpl_ptr = ptr;

	*ptr++ = (byte) type_cmpl;
	ptr += mach_ull_write_much_compressed(ptr, trx->undo_no);

	ptr += mach_ull_write_much_compressed(ptr, table->id);

	/*----------------------------------------*/
	/* Store the state of the info bits */

	*ptr++ = (byte) rec_get_info_bits(rec, dict_table_is_comp(table));

	/* Store the values of the system columns */
	field = rec_get_nth_field(rec, offsets,
				  dict_index_get_sys_col_pos(
					  index, DATA_TRX_ID), &flen);
	ut_ad(flen == DATA_TRX_ID_LEN);

	trx_id = trx_read_trx_id(field);

	/* If it is an update of a delete marked record, then we are
	allowed to ignore blob prefixes if the delete marking was done
	by some other trx as it must have committed by now for us to
	allow an over-write. */
	if (ignore_prefix) {
		ignore_prefix = (trx_id != trx->id);
	}
	ptr += mach_ull_write_compressed(ptr, trx_id);

	field = rec_get_nth_field(rec, offsets,
				  dict_index_get_sys_col_pos(
					  index, DATA_ROLL_PTR), &flen);
	ut_ad(flen == DATA_ROLL_PTR_LEN);

	ptr += mach_ull_write_compressed(ptr, trx_read_roll_ptr(field));

	/*----------------------------------------*/
	/* Store then the fields required to uniquely determine the
	record which will be modified in the clustered index */

	for (i = 0; i < dict_index_get_n_unique(index); i++) {

		field = rec_get_nth_field(rec, offsets, i, &flen);

		/* The ordering columns must not be stored externally. */
		ut_ad(!rec_offs_nth_extern(offsets, i));
		ut_ad(dict_index_get_nth_col(index, i)->ord_part);

		if (trx_undo_left(undo_page, ptr) < 5) {
			return(0);
		}

		ptr += mach_write_compressed(ptr, flen);

		if (flen != UNIV_SQL_NULL) {
			if (trx_undo_left(undo_page, ptr) < flen) {
				return(0);
			}

			ut_memcpy(ptr, field, flen);
			ptr += flen;
		}
	}

	/*----------------------------------------*/
	/* Save to the undo log the old values of the columns to be updated. */

	if (update) {
		if (trx_undo_left(undo_page, ptr) < 5) {
			return(0);
		}

		ptr += mach_write_compressed(ptr, upd_get_n_fields(update));

		for (i = 0; i < upd_get_n_fields(update); i++) {

			ulint	pos = upd_get_nth_field(update, i)->field_no;

			/* Write field number to undo log */
			if (trx_undo_left(undo_page, ptr) < 5) {
				return(0);
			}

			ptr += mach_write_compressed(ptr, pos);

			/* Save the old value of field */
			field = rec_get_nth_field(rec, offsets, pos, &flen);

			if (trx_undo_left(undo_page, ptr) < 15) {
				return(0);
			}

			if (rec_offs_nth_extern(offsets, pos)) {
				const dict_col_t* col
					= dict_index_get_nth_col(index, pos);
				ulint prefix_len
					= dict_max_field_len_store_undo(table, col);

				ut_ad(prefix_len + BTR_EXTERN_FIELD_REF_SIZE
				      <= sizeof ext_buf);

				ptr = trx_undo_page_report_modify_ext(
					ptr,
					col->ord_part
					&& !ignore_prefix
					&& flen < REC_ANTELOPE_MAX_INDEX_COL_LEN
					? ext_buf : NULL, prefix_len,
					dict_table_zip_size(table),
					&field, &flen);

				/* Notify purge that it eventually has to
				free the old externally stored field */

				trx->update_undo->del_marks = TRUE;

				*type_cmpl_ptr |= TRX_UNDO_UPD_EXTERN;
			} else {
				ptr += mach_write_compressed(ptr, flen);
			}

			if (flen != UNIV_SQL_NULL) {
				if (trx_undo_left(undo_page, ptr) < flen) {
					return(0);
				}

				ut_memcpy(ptr, field, flen);
				ptr += flen;
			}
		}
	}

	/*----------------------------------------*/
	/* In the case of a delete marking, and also in the case of an update
	where any ordering field of any index changes, store the values of all
	columns which occur as ordering fields in any index. This info is used
	in the purge of old versions where we use it to build and search the
	delete marked index records, to look if we can remove them from the
	index tree. Note that starting from 4.0.14 also externally stored
	fields can be ordering in some index. Starting from 5.2, we no longer
	store REC_MAX_INDEX_COL_LEN first bytes to the undo log record,
	but we can construct the column prefix fields in the index by
	fetching the first page of the BLOB that is pointed to by the
	clustered index. This works also in crash recovery, because all pages
	(including BLOBs) are recovered before anything is rolled back. */

	if (!update || !(cmpl_info & UPD_NODE_NO_ORD_CHANGE)) {
		byte*	old_ptr = ptr;

		trx->update_undo->del_marks = TRUE;

		if (trx_undo_left(undo_page, ptr) < 5) {

			return(0);
		}

		/* Reserve 2 bytes to write the number of bytes the stored
		fields take in this undo record */

		ptr += 2;

		for (col_no = 0; col_no < dict_table_get_n_cols(table);
		     col_no++) {

			const dict_col_t*	col
				= dict_table_get_nth_col(table, col_no);

			if (col->ord_part) {
				ulint	pos;

				/* Write field number to undo log */
				if (trx_undo_left(undo_page, ptr) < 5 + 15) {

					return(0);
				}

				pos = dict_index_get_nth_col_pos(index,
								 col_no);
				ptr += mach_write_compressed(ptr, pos);

				/* Save the old value of field */
				field = rec_get_nth_field(rec, offsets, pos,
							  &flen);

				if (rec_offs_nth_extern(offsets, pos)) {
					const dict_col_t*	col =
						dict_index_get_nth_col(
							index, pos);
					ulint			prefix_len =
						dict_max_field_len_store_undo(
							table, col);

					ut_a(prefix_len < sizeof ext_buf);

					ptr = trx_undo_page_report_modify_ext(
						ptr,
						flen < REC_ANTELOPE_MAX_INDEX_COL_LEN
						&& !ignore_prefix
						? ext_buf : NULL, prefix_len,
						dict_table_zip_size(table),
						&field, &flen);
				} else {
					ptr += mach_write_compressed(
						ptr, flen);
				}

				if (flen != UNIV_SQL_NULL) {
					if (trx_undo_left(undo_page, ptr)
					    < flen) {

						return(0);
					}

					ut_memcpy(ptr, field, flen);
					ptr += flen;
				}
			}
		}

		mach_write_to_2(old_ptr, ptr - old_ptr);
	}

	/*----------------------------------------*/
	/* Write pointers to the previous and the next undo log records */
	if (trx_undo_left(undo_page, ptr) < 2) {

		return(0);
	}

	mach_write_to_2(ptr, first_free);
	ptr += 2;
	mach_write_to_2(undo_page + first_free, ptr - undo_page);

	mach_write_to_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE,
			ptr - undo_page);

	/* Write to the REDO log about this change in the UNDO log */

	trx_undof_page_add_undo_rec_log(undo_page, first_free,
					ptr - undo_page, mtr);
	return(first_free);
}

#endif

inline trx_undo_page_t* trx_undo_prepare(que_sess_t *sess, uint32 type,
    uint32 size, uint64 query_min_scn, mtr_t* mtr)
{
    trx_undo_page_t* undo_page;
    trx_t* trx = (trx_t *)sess->trx;

    ut_ad(trx);
    ut_ad(type == TRX_UNDO_INSERT || type == TRX_UNDO_UPDATE);

    if (undo_rec_max_size < size + TRX_UNDO_LOG_HDR_SIZE) {
        CM_SET_ERROR(ERR_UNDO_RECORD_TOO_BIG, size + TRX_UNDO_LOG_HDR_SIZE);
        return NULL;
    }

    if (type == TRX_UNDO_INSERT) {
        if (SLIST_GET_LEN(trx->insert_undo) == 0) {
            trx_undo_assign_undo_page(trx, type, size + TRX_UNDO_LOG_HDR_SIZE, query_min_scn, mtr);
        }
        undo_page = SLIST_GET_LAST(trx->insert_undo);
    } else { //  TRX_UNDO_UPDATE
        if (SLIST_GET_LEN(trx->update_undo) == 0) {
            trx_undo_assign_undo_page(trx, type, size + TRX_UNDO_LOG_HDR_SIZE, query_min_scn, mtr);
        }
        undo_page = SLIST_GET_LAST(trx->update_undo);
    }

    if (undo_page && undo_page->free_size < size) {
        // We have to extend the undo log by one page,
        // add a page to an undo log
        undo_page = trx_undo_add_undo_page(trx, type, size, query_min_scn, mtr);
    }

     if (undo_page == NULL) {
         CM_SET_ERROR(ERR_NO_FREE_UNDO_PAGE);
    }

    return undo_page;
}

static inline void trx_undo_page_redo(
    page_t* undo_page,  // in: undo log page
    uint32  old_free,   // in: start offset of the inserted entry
    uint32  new_free,   // in: end offset of the entry
    mtr_t*  mtr)
{
    byte*       log_ptr;
    const byte* log_end;
    uint32      len;

    log_ptr = mlog_open(mtr, 13);
    if (log_ptr == NULL) {
        return;
    }

    log_end = &log_ptr[13];
    log_ptr = mlog_write_initial_log_record_fast(undo_page, MLOG_UNDO_INSERT, log_ptr, mtr);
    len = new_free - old_free - 4;

    mach_write_to_2(log_ptr, len);
    log_ptr += 2;

    mlog_close(mtr, log_ptr);

    mlog_catenate_string(mtr, undo_page + old_free + 2, len);
}

static inline status_t trx_undo_insert_undo_rec_into_page(trx_undo_page_t* undo_page,
    undo_data_t* undo_data, trx_t* trx, mtr_t* mtr)
{
    page_t* page = buf_block_get_frame(undo_page->block);

    ut_ad(mach_read_from_2(page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE) == TRX_UNDO_INSERT);

    uint32 first_free = mach_read_from_2(page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE);
    ut_ad(first_free <= UNIV_PAGE_SIZE);

    byte* ptr = page + first_free;

    // check enough space for writing
    if (trx_undo_left(page, ptr) < UNDO_DATA_HEADER_SIZE + undo_data->data_size) {
        CM_SET_ERROR(ERR_NO_FREE_UNDO_PAGE);
        return CM_ERROR;
    }

    // 1. Reserve 2 bytes for the pointer to the next undo log record
    ptr += 2;

    // 2. Store first some general parameters to the undo log
    *ptr++ = 0;//TRX_UNDO_INSERT_REC;
    ptr += mach_ull_write_much_compressed(ptr, trx->trx_slot_id.id);
    //ptr += mach_ull_write_much_compressed(ptr, table->id);

    // 3. Store data
    memcpy(ptr, (byte *)undo_data, UNDO_DATA_HEADER_SIZE);
    ptr += UNDO_DATA_HEADER_SIZE;
    if (undo_data->data_size > 0) {
        memcpy(ptr, undo_data->data, undo_data->data_size);
        ptr += undo_data->data_size;
    }

    // 4. Write offset of previous and next undo log record
    // Write offset of the previous undo log record
    mach_write_to_2(ptr, first_free);
    ptr += 2;
    // Write offset of the next undo log record
    mach_write_to_2(page + first_free, ptr - page);

    // 5. Update the offset to first free undo record
    mach_write_to_2(page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE, ptr - page);

    // 6. Write to the REDO log about this change in the UNDO log
    trx_undo_page_redo(page, first_free, ptr - page, mtr);

    return CM_SUCCESS;
}

inline status_t trx_undo_write(    que_sess_t *sess,   undo_data_t* undo_data, mtr_t* mtr)
{
    trx_undo_page_t* undo_page;
    trx_t* trx = (trx_t *)sess->trx;

    ut_ad(trx);

    if (undo_data->type == TRX_UNDO_INSERT_OP) {
        undo_page = SLIST_GET_LAST(trx->insert_undo);
    } else {
        ut_ad(undo_data->type == TRX_UNDO_MODIFY_OP);
        undo_page = SLIST_GET_LAST(trx->update_undo);
    }
    ut_ad(undo_page->free_size >= undo_data->data_size + UNDO_DATA_HEADER_SIZE);

    return trx_undo_insert_undo_rec_into_page(undo_page, undo_data, trx, mtr);
}

static inline void trx_undo_set_page_state(trx_undo_page_t* undo_page, uint32 state, mtr_t* mtr)
{
    page_t* page = buf_block_get_frame(undo_page->block);

    trx_undo_seg_hdr_t* seg_hdr = page + TRX_UNDO_SEG_HDR;
    mlog_write_uint32(seg_hdr + TRX_UNDO_STATE, state, MLOG_2BYTES, mtr);
}

inline void trx_undo_cleanup(trx_t* trx, scn_t scn, mtr_t* mtr)
{
    trx_undo_page_t* undo_page;
    bool32 is_reused = FALSE;

    // 1. set page state

    if (SLIST_GET_LEN(trx->update_undo) > 0) {
        undo_page = SLIST_GET_FIRST(trx->update_undo);
        if (SLIST_GET_LEN(trx->update_undo) == 1 &&
            undo_page->free_size >= TRX_UNDO_PAGE_REUSE_LIMIT) {
            is_reused = TRUE;
            undo_page->scn = scn;
            trx_undo_set_page_state(undo_page, TRX_UNDO_PAGE_STATE_CACHED, mtr);
        } else {
            while (undo_page) {
                undo_page->scn = scn;
                trx_undo_set_page_state(undo_page, TRX_UNDO_PAGE_STATE_HISTORY_LIST, mtr);
                undo_page = SLIST_GET_NEXT(list_node, undo_page);
            }
        }
    }

    if (SLIST_GET_LEN(trx->insert_undo) > 0) {
        undo_page = SLIST_GET_FIRST(trx->insert_undo);
        while (undo_page) {
            trx_undo_set_page_state(undo_page, TRX_UNDO_PAGE_FREE, mtr);
            undo_page = SLIST_GET_NEXT(list_node, undo_page);
        }
    }

    // 2. memory list

    trx_rseg_t* rseg = TRX_GET_RSEG(trx->trx_slot_id.rseg_id);
    mutex_enter(&(rseg->undo_cache_mutex));

    if (SLIST_GET_LEN(trx->insert_undo) > 0) {
        SLIST_APPEND_SLIST(list_node, rseg->insert_undo_cache, trx->insert_undo);
    }

    if (SLIST_GET_LEN(trx->update_undo) > 0) {
        if (is_reused) {
            SLIST_APPEND_SLIST(list_node, rseg->update_undo_cache, trx->update_undo);
        } else {
            SLIST_APPEND_SLIST(list_node, rseg->history_undo_list, trx->update_undo);
        }
    }

    mutex_exit(&(rseg->undo_cache_mutex));

    // 3.
    SLIST_INIT(trx->insert_undo);
    SLIST_INIT(trx->update_undo);
}


