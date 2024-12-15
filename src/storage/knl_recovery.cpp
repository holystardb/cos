#include "knl_recovery.h"

#include "cm_log.h"
#include "knl_buf_flush.h"
#include "knl_file_system.h"
#include "knl_fsp.h"
#include "knl_trx_rseg.h"


recovery_sys_t       g_recovery_sys;

mlog_dispatch_t      g_mlog_dispatch[MLOG_BIGGEST_TYPE] = { 0 };


static mlog_dispatch_t g_mlog_replay_table[MLOG_BIGGEST_TYPE] = {
    {MLOG_1BYTE,  mlog_replay_nbytes, mlog_replay_check},
    {MLOG_2BYTES, mlog_replay_nbytes, mlog_replay_check},
    {MLOG_4BYTES, mlog_replay_nbytes, mlog_replay_check},
    {MLOG_8BYTES, mlog_replay_nbytes, mlog_replay_check},

    {MLOG_INIT_FILE_PAGE2, fsp_replay_init_file_page, mlog_replay_check},
    {MLOG_TRX_RSEG_PAGE_INIT, trx_rseg_replay_trx_slot_page_init, mlog_replay_check},
    {MLOG_TRX_RSEG_SLOT_BEGIN, trx_rseg_replay_begin_slot, mlog_replay_check},
    {MLOG_TRX_RSEG_SLOT_END, trx_rseg_replay_end_slot, mlog_replay_check},

    /* end */
    {MLOG_BIGGEST_TYPE, NULL, NULL}
};



// return < 0 if b2 < b1, 0 if b2 == b1, > 0 if b2 > b1 */
static int32 recovery_block_cmp(const void* p1, const void* p2)
{
    const buf_block_t* b1 = *(const buf_block_t**)p1;
    const buf_block_t* b2 = *(const buf_block_t**)p2;

    ut_ad(b1 != NULL);
    ut_ad(b2 != NULL);

    int32 ret = (int32)(b2->page.id.space_id() - b1->page.id.space_id());
    return ret ? ret : (int32)(b2->page.id.page_no() - b1->page.id.page_no());
}

recovery_sys_t* recovery_init(memory_pool_t* mem_pool)
{
    recovery_sys_t* recv_sys = &g_recovery_sys;

    recv_sys->recovered_buf = log_sys->buf;
    recv_sys->recovered_buf_size = log_sys->buf_size;
    recv_sys->recovered_buf_data_len = 0;
    recv_sys->recovered_buf_offset = 0;
    recv_sys->last_hdr_no = 0;
    recv_sys->last_checkpoint_no = 0;
    recv_sys->is_first_rec = TRUE;
    recv_sys->log_rec_len = 0;
    recv_sys->log_rec_offset = 0;
    recv_sys->is_read_log_done = FALSE;

    recv_sys->block_rbt = rbt_create(sizeof(buf_block_t *), recovery_block_cmp, mem_pool);
    if (recv_sys->block_rbt == NULL) {
        return NULL;
    }

    memset(g_mlog_dispatch, 0x00, sizeof(mlog_dispatch_t) * MLOG_BIGGEST_TYPE);
    for (uint32 i = 0; i < MLOG_BIGGEST_TYPE; i++) {
        mlog_dispatch_t* dispatch = &g_mlog_replay_table[i];
        g_mlog_dispatch[dispatch->type] = *dispatch;
    }

    srv_recovery_on = TRUE;

    return recv_sys;
}

void recovery_destroy(recovery_sys_t* recv_sys)
{
    rbt_free(recv_sys->block_rbt);
    recv_sys->block_rbt = NULL;

    srv_recovery_on = FALSE;
}


inline bool32 mlog_replay_check(uint32 type, byte* log_rec_ptr, byte* log_end_ptr)
{
    return TRUE;
}


bool32 log_recovery()
{
    //log_sys->current_group = 0;
    return TRUE;
}


#define LOG_BLOCK_REMAIN_DATA_LEN(recv_sys)  \
    (OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE - recv_sys->log_block_read_offset)

#define LOG_BLOCK_GET_DATA(recv_sys)         \
    (recv_sys->log_block + recv_sys->log_block_read_offset)


// Looks for the maximum consistent checkpoint from the log groups.
static status_t recv_find_max_checkpoint(uint32* max_field) // out: LOG_CHECKPOINT_1 or LOG_CHECKPOINT_2
{
    uint64 checkpoint_no, max_checkpoint_no = 0, group_write_offset, lsn;
    uint32 group_id, field, fold;
    byte*  buf = log_sys->checkpoint_buf;

    *max_field = 0;

    for (field = LOG_CHECKPOINT_1; field <= LOG_CHECKPOINT_2; field += LOG_CHECKPOINT_2 - LOG_CHECKPOINT_1) {

        log_checkpoint_read(field);

        // Check the consistency of the checkpoint info
        fold = ut_fold_binary(buf, LOG_CHECKPOINT_CHECKSUM_1);
        if ((fold & 0xFFFFFFFF) != mach_read_from_4(buf + LOG_CHECKPOINT_CHECKSUM_1)) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_RECOVERY,
                "Checkpoint is invalid at %lu, calc checksum %lu, checksum1 = %lu",
                field, fold & 0xFFFFFFFF, mach_read_from_4(buf + LOG_CHECKPOINT_CHECKSUM_1));
            goto not_consistent;
        }

        fold = ut_fold_binary(buf + LOG_CHECKPOINT_LSN, LOG_CHECKPOINT_CHECKSUM_2 - LOG_CHECKPOINT_LSN);
        if ((fold & 0xFFFFFFFF) != mach_read_from_4(buf + LOG_CHECKPOINT_CHECKSUM_2)) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_RECOVERY,
                "Checkpoint is invalid at %lu, calc checksum %lu, checksum2 %lu",
                field, fold & 0xFFFFFFFF, mach_read_from_4(buf + LOG_CHECKPOINT_CHECKSUM_2));
            goto not_consistent;
        }

        lsn = mach_read_from_8(buf + LOG_CHECKPOINT_LSN);
        group_id = mach_read_from_4(buf + LOG_CHECKPOINT_OFFSET_LOW32);
        group_write_offset = mach_read_from_8(buf + LOG_CHECKPOINT_OFFSET_HIGH32);
        checkpoint_no = mach_read_from_8(buf + LOG_CHECKPOINT_NO);

        LOGGER_INFO(LOGGER, LOG_MODULE_RECOVERY,
            "Checkpoint point (lsn %llu, checkpoint no %llu) found in group (id %lu, offset %llu)",
            lsn, checkpoint_no, group_id, group_write_offset);

        if (checkpoint_no >= max_checkpoint_no) {
            max_checkpoint_no = checkpoint_no;
            *max_field = field;
        }

not_consistent:
        ;
    }

    return CM_SUCCESS;
}

static inline uint32 recovery_get_log_rec_prefix_len(byte* log_rec)
{
    uint32 len = 1;
    uint32 space_id = mach_read_compressed(log_rec + len);
    len += mach_get_compressed_size(space_id);
    uint32 page_no = mach_read_compressed(log_rec + len);
    len += mach_get_compressed_size(page_no);

    return len;
}

static status_t recovery_read_log_blocks(recovery_sys_t* recv_sys)
{
    status_t err;
    uint64 offset;
    uint32 read_len = UNIV_PAGE_SIZE * 128;  // 2MB
    log_group_t* group = &log_sys->groups[recv_sys->cur_group_id];

    // 1 no remaining data in the current group, switch to group file
    if (group->file_size == recv_sys->cur_group_offset) {
        recv_sys->cur_group_id = (recv_sys->cur_group_id + 1) % log_sys->group_count;
        recv_sys->cur_group_offset = LOG_BUF_WRITE_MARGIN;
    }

    // 2 calc read length
    group = &log_sys->groups[recv_sys->cur_group_id];
    offset = recv_sys->cur_group_offset;
    ut_ad(recv_sys->recovered_buf_size > recv_sys->recovered_buf_data_len);
    if (recv_sys->recovered_buf_data_len + read_len > recv_sys->recovered_buf_size) {
        // adjust read_len by recv_sys->recovered_buf
        read_len = recv_sys->recovered_buf_size - recv_sys->recovered_buf_data_len;
    }
    ut_ad(group->file_size >= recv_sys->cur_group_offset);
    if (group->file_size < recv_sys->cur_group_offset + read_len) {
        // adjust read_len by group->file_size
        read_len = (uint32)(group->file_size - recv_sys->cur_group_offset);
    }

    // 3 read file
    LOGGER_DEBUG(LOGGER, LOG_MODULE_RECOVERY,
        "log_group_file_read: group %s offset %llu read len %lu", group->name, offset, read_len);
    ut_ad(offset % OS_FILE_LOG_BLOCK_SIZE == 0);
    ut_ad(read_len % OS_FILE_LOG_BLOCK_SIZE == 0);
    ut_ad(recv_sys->recovered_buf_data_len % OS_FILE_LOG_BLOCK_SIZE == 0);
    err = log_group_file_read(group, recv_sys->recovered_buf + recv_sys->recovered_buf_data_len, read_len, offset);
    if (err != CM_SUCCESS) {
        return CM_ERROR;
    }
    
    // 4 check for valid log block
    for (uint32 i = 0; i < recv_sys->recovered_buf_data_len + read_len; i += OS_FILE_LOG_BLOCK_SIZE) {
        recv_sys->last_log_block = recv_sys->recovered_buf + i;
        uint64 hdr_no = log_block_get_hdr_no(recv_sys->last_log_block);
        uint64 checkpoint_no = log_block_get_checkpoint_no(recv_sys->last_log_block);
        if (recv_sys->last_hdr_no > hdr_no) {
            recv_sys->last_log_block_writed_offset =
                recv_sys->cur_group_offset +  //
                (i - recv_sys->recovered_buf_data_len) +  //
                log_block_get_data_len(recv_sys->last_log_block);
            recv_sys->recovered_buf_data_len = i + log_block_get_data_len(recv_sys->last_log_block);
            recv_sys->limit_lsn = recv_sys->recovered_buf_lsn + recv_sys->recovered_buf_data_len;
            recv_sys->is_read_log_done = TRUE;
            return CM_SUCCESS;
        }
        // recv_sys->last_hdr_no may be equal to hdr_no,
        // because the last REC is incomplete, the log blocks is reserved.
        recv_sys->last_hdr_no = hdr_no;
        recv_sys->last_checkpoint_no = checkpoint_no;
    }

    recv_sys->recovered_buf_data_len += read_len;
    recv_sys->cur_group_offset += read_len;

    // switch to next group file
    if (recv_sys->cur_group_offset == group->file_size) {
        recv_sys->cur_group_id = (recv_sys->cur_group_id + 1) % log_sys->group_count;
        recv_sys->cur_group_offset = LOG_BUF_WRITE_MARGIN;
    }

    return CM_SUCCESS;
}

static inline uint32 recovery_parse_next_log_rec_size(recovery_sys_t* recv_sys)
{
    byte* log_rec;
    uint32 remain_len;

    ut_ad(recv_sys->log_block);

    remain_len = LOG_BLOCK_REMAIN_DATA_LEN(recv_sys);
    if (remain_len < MTR_LOG_LEN_SIZE) {
        //
        ut_ad(recv_sys->recovered_buf_data_len >=
            recv_sys->log_block - recv_sys->recovered_buf + OS_FILE_LOG_BLOCK_SIZE + OS_FILE_LOG_BLOCK_SIZE);

        byte buf[OS_FILE_LOG_BLOCK_SIZE];
        memcpy(buf, LOG_BLOCK_GET_DATA(recv_sys), remain_len);
        memcpy(buf + remain_len,
            recv_sys->log_block + OS_FILE_LOG_BLOCK_SIZE + LOG_BLOCK_HDR_SIZE,
            MTR_LOG_LEN_SIZE - remain_len);
        log_rec = buf;
    } else {
        log_rec = LOG_BLOCK_GET_DATA(recv_sys);
    }

    return mach_read_from_2(log_rec);
}


static status_t recovery_read_next_log_rec(recovery_sys_t* recv_sys)
{
    status_t err;

    // init log_rec
    recv_sys->log_rec_len = 0;
    recv_sys->log_rec_offset = 0;
    recv_sys->log_rec = NULL;

retry_copy_remain:

    // check
    if (!recv_sys->is_read_log_done &&
        recv_sys->recovered_buf_data_len <= recv_sys->recovered_buf_offset + OS_FILE_LOG_BLOCK_SIZE) {

        // move data
        if (recv_sys->recovered_buf_data_len > 0) {
            memmove(recv_sys->recovered_buf,
                recv_sys->recovered_buf + recv_sys->recovered_buf_offset,
                recv_sys->recovered_buf_data_len - recv_sys->recovered_buf_offset);
            recv_sys->recovered_buf_data_len -= recv_sys->recovered_buf_offset;
            recv_sys->recovered_buf_offset = 0;
            recv_sys->recovered_buf_lsn += recv_sys->recovered_buf_offset;
        }

        //
        err = recovery_read_log_blocks(recv_sys);
        if (err != CM_SUCCESS) {
            return CM_ERROR;
        }
    }


    // finish
    if (recv_sys->is_read_log_done && recv_sys->recovered_buf_offset == recv_sys->recovered_buf_data_len) {
        return CM_SUCCESS;
    }

    //
    if (recv_sys->is_first_rec) {
        recv_sys->is_first_rec = FALSE;

        recv_sys->log_block = recv_sys->recovered_buf;
        recv_sys->recovered_buf_offset += OS_FILE_LOG_BLOCK_SIZE;

        recv_sys->log_block_first_rec_offset = log_block_get_first_rec_group(recv_sys->log_block);
        recv_sys->log_block_read_offset = recv_sys->log_block_first_rec_offset;
        recv_sys->log_block_lsn =
            recv_sys->checkpoint_lsn -
            (recv_sys->checkpoint_group_offset - recv_sys->cur_group_offset);
        recv_sys->recovered_buf_lsn = recv_sys->log_block_lsn;
    }

    //
    if (recv_sys->log_block == NULL) {
        ut_ad(recv_sys->log_block_read_offset == 0);

        recv_sys->log_block = recv_sys->recovered_buf + recv_sys->recovered_buf_offset;
        recv_sys->recovered_buf_offset += OS_FILE_LOG_BLOCK_SIZE;

        recv_sys->log_block_lsn += OS_FILE_LOG_BLOCK_SIZE;
        recv_sys->log_block_first_rec_offset = log_block_get_first_rec_group(recv_sys->log_block);
        recv_sys->log_block_read_offset = LOG_BLOCK_HDR_SIZE;
    }

    // a new log_rec
    if (recv_sys->log_rec_len == 0) {
        recv_sys->log_rec_len = recovery_parse_next_log_rec_size(recv_sys) + MTR_LOG_LEN_SIZE;
        ut_a(recv_sys->log_rec_len > 0 && recv_sys->log_rec_len <= 0xFFFF);
        recv_sys->log_rec_offset = 0;
        recv_sys->log_rec_start_lsn = recv_sys->log_block_lsn + recv_sys->log_block_read_offset;
        recv_sys->log_rec_end_lsn = recv_sys->log_rec_start_lsn;
    }

    // copy data
    uint32 copy_data_len;
    if (recv_sys->log_rec_len < recv_sys->log_rec_offset + LOG_BLOCK_REMAIN_DATA_LEN(recv_sys)) {
        copy_data_len = recv_sys->log_rec_len - recv_sys->log_rec_offset;

        memcpy(recv_sys->log_rec_buf + recv_sys->log_rec_offset,
            LOG_BLOCK_GET_DATA(recv_sys), copy_data_len);

        if (recv_sys->log_rec_offset == 0) {
            recv_sys->log_rec_end_lsn += copy_data_len;
        } else {
            recv_sys->log_rec_end_lsn += LOG_BLOCK_HDR_SIZE + copy_data_len;
        }
        recv_sys->log_rec_offset += copy_data_len;

        recv_sys->log_block_read_offset += copy_data_len;
    } else {
        copy_data_len = LOG_BLOCK_REMAIN_DATA_LEN(recv_sys);

        memcpy(recv_sys->log_rec_buf + recv_sys->log_rec_offset,
            LOG_BLOCK_GET_DATA(recv_sys), copy_data_len);

        if (recv_sys->log_rec_offset + copy_data_len == recv_sys->log_rec_len) {
            recv_sys->log_rec_end_lsn += copy_data_len;
        } else if (recv_sys->log_rec_offset == 0) {
            recv_sys->log_rec_end_lsn += (OS_FILE_LOG_BLOCK_SIZE - recv_sys->log_block_read_offset);
        } else {
            recv_sys->log_rec_end_lsn += OS_FILE_LOG_BLOCK_SIZE;
        }
        recv_sys->log_rec_offset += copy_data_len;

        recv_sys->log_block = NULL;
        recv_sys->log_block_read_offset  = 0;
    }

    // If it is not a complete record
    if (recv_sys->log_rec_len > recv_sys->log_rec_offset) {
        goto retry_copy_remain;
    }

    //
    recv_sys->log_rec = recv_sys->log_rec_buf;

    return CM_SUCCESS;
}

static bool32 recovery_is_need_get_block(uint32 type, bool32* is_create_page)
{
    bool32 result = FALSE;
    *is_create_page = FALSE;

    switch (type) {
    case MLOG_1BYTE:
    case MLOG_2BYTES:
    case MLOG_4BYTES:
    case MLOG_8BYTES:
        result = TRUE;
        break;

    case MLOG_INIT_FILE_PAGE2:
    case MLOG_TRX_RSEG_PAGE_INIT:
        result = TRUE;
        *is_create_page = TRUE;
        break;

    default:
        break;
    }

    return result;
}

static status_t recovery_replay_log_rec(recovery_sys_t* recv_sys)
{
    status_t ret = CM_SUCCESS;
    mtr_t mtr;
    uint32 type;
    date_t begin_time = cm_now();

    ut_ad(recv_sys->log_rec_len > MTR_LOG_LEN_SIZE);
    ut_ad(recv_sys->log_rec != NULL);
    ut_ad(recv_sys->log_rec_end_lsn - recv_sys->log_rec_start_lsn >= recv_sys->log_rec_len);

    byte* end_ptr = recv_sys->log_rec + recv_sys->log_rec_len;
    byte* log_rec_ptr = recv_sys->log_rec + MTR_LOG_LEN_SIZE;
    uint32 single_rec = (uint32)*log_rec_ptr & MLOG_SINGLE_REC_FLAG;
    if (single_rec == 0) {  // multi rec
        if (MLOG_MULTI_REC_END != mach_read_from_1(end_ptr - 1)) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_RECOVERY,
                "recovery_replay_log_rec: invalid log at lsn (%llu - %llu) in group %s",
                recv_sys->log_rec_start_lsn, recv_sys->log_rec_end_lsn,
                log_sys->groups[recv_sys->cur_group_id].name);
            return CM_ERROR;
        }
    }

    LOGGER_TRACE(LOGGER, LOG_MODULE_RECOVERY,
        "starting an replay batch of log records at lsn (%llu - %llu) in group %s",
        recv_sys->log_rec_start_lsn, recv_sys->log_rec_end_lsn,
        log_sys->groups[recv_sys->cur_group_id].name);

    mtr_start(&mtr);

    while (log_rec_ptr < end_ptr) {
        //
        type = (byte)((uint32)*log_rec_ptr & ~MLOG_SINGLE_REC_FLAG);
        ut_ad(type > 0 && type <= MLOG_BIGGEST_TYPE);
        log_rec_ptr++;

        if (type == MLOG_MULTI_REC_END) {
            // Found the end mark for the records
            LOGGER_TRACE(LOGGER, LOG_MODULE_RECOVERY,
                "recovery_replay_log_rec: multi_rec_end for lsn (%llu - %llu) in group %s",
                recv_sys->log_rec_start_lsn, recv_sys->log_rec_end_lsn,
                log_sys->groups[recv_sys->cur_group_id].name);
            ut_ad(single_rec == FALSE);
            break;
        }


        //
        buf_block_t* block = NULL;
        bool32 is_create_page = FALSE;
        space_id_t space_id = INVALID_SPACE_ID;
        page_no_t page_no = INVALID_PAGE_NO;
        if (recovery_is_need_get_block(type, &is_create_page)) {
            space_id = mach_read_compressed(log_rec_ptr);
            log_rec_ptr += mach_get_compressed_size(space_id);
            page_no = mach_read_compressed(log_rec_ptr);
            log_rec_ptr += mach_get_compressed_size(page_no);
            //
            Page_fetch mode;
            if (is_create_page) {
                uint32 page_type = mach_read_from_2(log_rec_ptr);
                mode = (page_type & FIL_PAGE_TYPE_RESIDENT_FLAG) ? Page_fetch::RESIDENT : Page_fetch::NORMAL;
            }
            //
            const page_id_t page_id(space_id, page_no);
            const page_size_t page_size(space_id);
            if (is_create_page) {
                block = buf_page_create(page_id, page_size, RW_X_LATCH, mode, &mtr);
            } else {
                block = buf_page_get(page_id, page_size, RW_X_LATCH, &mtr);
            }
            if (block == NULL) {
                LOGGER_ERROR(LOGGER, LOG_MODULE_RECOVERY,
                    "recovery_replay_log_rec: cannot find block(space id %u, page no %u)",
                    space_id, page_no);
                ret = CM_ERROR;
                goto err_exit;
            }
            //buf_block_dbg_add_level(block, SYNC_NO_ORDER_CHECK);

            //
            const ib_rbt_node_t* rbt_node = rbt_lookup(recv_sys->block_rbt, &block);
            if (rbt_node == NULL) {
                rbt_insert(recv_sys->block_rbt, &block, &block);
            }
        }

        // replay
        if (g_mlog_dispatch[type].log_rec_replay == NULL) {
            LOGGER_ERROR(LOGGER, LOG_MODULE_RECOVERY,
                "recovery_replay_log_rec: invalid log type %lu block (%p space_id %lu page_no %lu)",
                type, block, space_id, page_no);
            ret = CM_ERROR;
            goto err_exit;
        }
        log_rec_ptr = g_mlog_dispatch[type].log_rec_replay(type, log_rec_ptr, end_ptr, block);
        if (log_rec_ptr == NULL) {
            ret = CM_ERROR;
            goto err_exit;
        }
    }
    //
    ut_a(log_rec_ptr == end_ptr);

    // add block to flush_list
    log_flush_order_mutex_enter();
    for (const ib_rbt_node_t* rbt_node = rbt_first(recv_sys->block_rbt);
         rbt_node != NULL;
         rbt_node = rbt_next(recv_sys->block_rbt, rbt_node))
    {
        buf_block_t* block = *(buf_block_t **)rbt_value(buf_block_t**, rbt_node);
        buf_flush_recv_note_modification(block, recv_sys->log_rec_start_lsn, recv_sys->log_rec_end_lsn);
    }
    log_flush_order_mutex_exit();

err_exit:

    mtr.modifications = FALSE;
    mtr_commit(&mtr);

    rbt_clear(recv_sys->block_rbt);

    date_t end_time = cm_now();
    LOGGER_DEBUG(LOGGER, LOG_MODULE_RECOVERY,
        "replay batch completed: len %u lsn (%llu - %llu) in group %s, total time %llu micro-seconds",
        recv_sys->log_rec_len, recv_sys->log_rec_start_lsn, recv_sys->log_rec_end_lsn,
        log_sys->groups[recv_sys->cur_group_id].name, end_time - begin_time);

    return ret;
}

static status_t recovery_get_last_checkpoint_info(recovery_sys_t* recv_sys)
{
    status_t err;
    uint32 max_ckpt_field = 0;

    // 1 Look for the latest checkpoint from any of the log groups
    err = recv_find_max_checkpoint(&max_ckpt_field);
    if (err != CM_SUCCESS) {
        return CM_ERROR;
    }
    if (max_ckpt_field != LOG_CHECKPOINT_1 && max_ckpt_field != LOG_CHECKPOINT_2) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_RECOVERY, "No valid checkpoint was found");
        return CM_ERROR;
    }

    // 2 read and set checkpoint info
    log_checkpoint_read(max_ckpt_field);

    recv_sys->checkpoint_lsn = mach_read_from_8(log_sys->checkpoint_buf + LOG_CHECKPOINT_LSN);
    recv_sys->checkpoint_no = mach_read_from_8(log_sys->checkpoint_buf + LOG_CHECKPOINT_NO);
    recv_sys->archived_lsn = mach_read_from_8(log_sys->checkpoint_buf + LOG_CHECKPOINT_ARCHIVED_LSN);
    recv_sys->checkpoint_group_id = mach_read_from_4(log_sys->checkpoint_buf + LOG_CHECKPOINT_OFFSET_LOW32);
    recv_sys->checkpoint_group_offset = mach_read_from_8(log_sys->checkpoint_buf + LOG_CHECKPOINT_OFFSET_HIGH32);

    ut_a(recv_sys->checkpoint_group_id < log_sys->group_count);
    ut_a(recv_sys->checkpoint_group_offset >= LOG_BUF_WRITE_MARGIN);

    recv_sys->cur_group_id = recv_sys->checkpoint_group_id;

    return CM_SUCCESS;
}

static void recovery_reset_log_sys(recovery_sys_t* recv_sys)
{
    uint64 file_size = 0;
    log_group_t* checkpoint_group = &log_sys->groups[recv_sys->checkpoint_group_id];
    checkpoint_group->base_lsn = recv_sys->base_lsn;
    checkpoint_group->status = LogGroupStatus::ACTIVE;
    file_size = log_group_get_capacity(checkpoint_group);

    for (uint32 i = 1; i < log_sys->group_count; i++) {
        log_group_t* group = &log_sys->groups[(recv_sys->checkpoint_group_id + i) % log_sys->group_count];

        group->base_lsn = checkpoint_group->base_lsn + file_size;

        //
        if (recv_sys->checkpoint_group_id > recv_sys->cur_group_id) {
            if (group->id < recv_sys->checkpoint_group_id &&
                group->id > recv_sys->cur_group_id) {
                checkpoint_group->status = LogGroupStatus::INACTIVE;
            } else {
                checkpoint_group->status = LogGroupStatus::ACTIVE;
            }
        } else {
            if (group->id > recv_sys->checkpoint_group_id &&
                group->id <= recv_sys->cur_group_id) {
                checkpoint_group->status = LogGroupStatus::ACTIVE;
            } else {
                checkpoint_group->status = LogGroupStatus::INACTIVE;
            }
        }

        //
        file_size += (group->file_size - LOG_BUF_WRITE_MARGIN);
    }

    // checkpoint
    log_sys->last_checkpoint_lsn = recv_sys->checkpoint_lsn;
    log_sys->next_checkpoint_no = recv_sys->checkpoint_no + 1;

    // base lsn for log_buffer
    log_sys->buf_base_lsn = checkpoint_group->base_lsn;
    // Move the data of last BLOCK to the forefront of log_buffer
    if (recv_sys->recovered_buf_offset > 0) {
        memmove(recv_sys->recovered_buf,
            recv_sys->recovered_buf + recv_sys->recovered_buf_offset,
            recv_sys->recovered_buf_data_len - recv_sys->recovered_buf_offset);
    }
    // buf_lsn
    log_sys->buf_lsn.val.lsn = recv_sys->limit_lsn;
    log_sys->buf_lsn.val.slot_index = 0;
    log_sys->slot_write_pos = 0;
    // position for writer thread and flusher thread
    log_sys->writer_writed_lsn = recv_sys->limit_lsn;
    log_sys->current_write_group_id = recv_sys->cur_group_id;
    log_sys->flusher_flushed_lsn = recv_sys->limit_lsn;
    log_sys->current_flush_group_id = recv_sys->cur_group_id;
    for (uint32 i = 0; i < log_sys->group_count; i++) {
        if (i == log_sys->current_write_group_id) {
            log_sys->groups[log_sys->group_count].status = LogGroupStatus::CURRENT;
        } else {
            log_sys->groups[log_sys->group_count].status = LogGroupStatus::INACTIVE;
        }
    }
}


// Recovers from a checkpoint.
// When this function returns, the database is able to start processing of new user transactions,
// but the function recv_recovery_from_checkpoint_finish should be called later to complete
// the recovery and free the resources used in it.
status_t recovery_from_checkpoint_start(recovery_sys_t* recv_sys)
{
    status_t err;

    // 1
    err = recovery_get_last_checkpoint_info(recv_sys);
    if (err != CM_SUCCESS) {
        return CM_ERROR;
    }

    // 2 read log and replay

    // init position for read
    recv_sys->cur_group_id = recv_sys->checkpoint_group_id;
    recv_sys->cur_group_offset = ut_uint64_align_down(recv_sys->checkpoint_group_offset, OS_FILE_LOG_BLOCK_SIZE);
    recv_sys->base_lsn = recv_sys->checkpoint_lsn -
        (recv_sys->checkpoint_group_offset - LOG_BUF_WRITE_MARGIN); // lsn for block0

    // read and replay log_rec
    byte* log_rec = NULL;
    err = recovery_read_next_log_rec(recv_sys);
    if (err != CM_SUCCESS) {
        return CM_ERROR;
    }
    while (recv_sys->log_rec) {
        // replay
        err = recovery_replay_log_rec(recv_sys);
        if (err != CM_SUCCESS) {
            return CM_ERROR;
        }

        // get next log_rec
        err = recovery_read_next_log_rec(recv_sys);
        if (err != CM_SUCCESS) {
            return CM_ERROR;
        }
    }

    // 3 
    recovery_reset_log_sys(recv_sys);

    return CM_SUCCESS;
}

// Completes recovery from a checkpoint.
status_t recovery_from_checkpoint_finish(recovery_sys_t* recv_sys)
{
    // Apply the hashed log records to the respective file pages

    //if (srv_force_recovery < SRV_FORCE_NO_LOG_REDO) {
    //    recv_apply_hashed_log_recs(TRUE);
    //}

    /* By acquring the mutex we ensure that the recv_writer thread
       won't trigger any more LRU batchtes. Now wait for currently
       in progress batches to finish. */
    //buf_flush_wait_LRU_batch_end();

    //trx_rollback_or_clean_recovered(FALSE);

    recovery_destroy(recv_sys);

    return CM_SUCCESS;
}

