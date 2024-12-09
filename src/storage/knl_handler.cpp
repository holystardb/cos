#include "cm_log.h"
#include "knl_handler.h"
#include "knl_server.h"
#include "knl_buf.h"
#include "knl_redo.h"
#include "knl_dict.h"
#include "knl_fsp.h"
#include "knl_trx.h"
#include "knl_recovery.h"
#include "knl_trx_rseg.h"
#include "knl_checkpoint.h"
#include "knl_start.h"


status_t knl_server_init(char* base_dir, attribute_t* attr)
{
    return server_open_or_create_database(base_dir, attr);
}

status_t knl_server_end()
{
    return server_shutdown_database();
}

void knl_handler::savepoint(que_sess_t* sess, trx_savepoint_t* savepoint)
{
    if (sess->trx == NULL) {
        trx_start_if_not_started(sess);
    }

    trx_savepoint(sess, sess->trx, savepoint);
}

void knl_handler::rollback(que_sess_t* sess, trx_savepoint_t* savepoint)
{
    if (sess->trx) {
        trx_rollback(sess, sess->trx, savepoint);
    }
}

void knl_handler::commit(que_sess_t* sess)
{
    if (sess->trx) {
        trx_commit(sess, sess->trx);
    }
}

status_t knl_handler::insert_row(que_sess_t* sess, scan_cursor_t* cursor)
{
    status_t err;

    trx_savepoint_t save_point;
    savepoint(sess, &save_point);

    // heap
    err = heap_insert(sess, cursor->insert_node);
    if (err != CM_SUCCESS) {
        goto err_exit;
    }

    // index
    dict_index_t* index = UT_LIST_GET_FIRST(cursor->insert_node->table->indexes);
    while (index) {
        //
        

        index = UT_LIST_GET_NEXT(list_node, index);
    }


    return CM_SUCCESS;

err_exit:

    rollback(sess, &save_point);

    return CM_ERROR;
}

status_t knl_handler::update_row(que_sess_t* sess, scan_cursor_t* cursor)
{
    status_t err;

    trx_savepoint_t save_point;
    savepoint(sess, &save_point);

    // heap
    err = heap_insert(sess, cursor->insert_node);
    if (err != CM_SUCCESS) {
        goto err_exit;
    }

    // index
    dict_index_t* index = UT_LIST_GET_FIRST(cursor->insert_node->table->indexes);
    while (index) {
        //
        

        index = UT_LIST_GET_NEXT(list_node, index);
    }


    return CM_SUCCESS;

err_exit:

    rollback(sess, &save_point);

    return CM_ERROR;
}

status_t knl_handler::delete_row(que_sess_t* sess, scan_cursor_t* cursor)
{
    return CM_SUCCESS;
}

status_t knl_handler::fetch_by_rowid(scan_cursor_t* scan, byte* buf, bool32* is_found)
{
    return CM_SUCCESS;
}

int knl_handler::index_init(uint32 index)
{
    return 0;
}

int knl_handler::index_end()
{
    return 0;
}

// buf: in/out, buffer for the row
int32 knl_handler::index_fetch(scan_cursor_t* scan, byte* buf, row_fetch_match_mode match_mode)
{
    return 0;
}

int32 knl_handler::index_first(scan_cursor_t* scan, byte* buf)
{
    return index_fetch(scan, buf, FETCH_ROW_KEY_EXACT);
}

int32 knl_handler::index_last(scan_cursor_t* scan, byte* buf)
{
    return index_fetch(scan, buf, FETCH_ROW_KEY_EXACT);
}

// Reads the next or previous row from a cursor, which must have previously been positioned using index_read.
// return 0, HA_ERR_END_OF_FILE, or error number
// buf: in/out, buffer for the row
int32 knl_handler::general_fetch(    scan_cursor_t* scan, byte* buf,
    row_fetch_direction direction, row_fetch_match_mode match_mode)
{
    return 0;
}

int32 knl_handler::index_next(    scan_cursor_t* scan, byte* buf)
{
    return general_fetch(scan, buf, FETCH_ROW_NEXT, FETCH_ROW_KEY_EXACT);
}

int32 knl_handler::index_prev(    scan_cursor_t* scan, byte* buf)
{
    return general_fetch(scan, buf, FETCH_ROW_PREV, FETCH_ROW_KEY_EXACT);
}


