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


// heap access method interface
scan_cursor_t* heap_begin_scan(memory_context_t mem_ctx,
    dict_table_t* table, snapshot_t snapshot, int32 key_count, scan_key_t keys)
{
    scan_cursor_t* scan;
    memory_stack_context_t* mem_ctx_stack;
    scan = New(mem_ctx)scan_cursor_t(mem_ctx_stack);
    scan->table = table;
    //scan->snapshot = snapshot;
    scan->key_count = key_count;
    //scan->flags = flags;

    if (scan->key_count > 0) {
        scan->keys = (scan_key_t*)mem_alloc(mem_ctx, sizeof(scan_key_t) * scan->key_count);
        memcpy(scan->keys, keys, sizeof(scan_key_t) * scan->key_count);
    } else {
        scan->keys = NULL;
    }
    return scan;
}

void heap_end_scan(scan_cursor_t* scan)
{
    if (scan->keys) {
        mem_free(scan->keys);
    }
    delete scan;
}

bool32 heap_get_next_slot(scan_cursor_t* scan, row_fetch_direction_t direction, heap_tuple_slot_t* slot)
{
    //heapgettup(scan, direction, sscan->rs_nkeys, sscan->rs_key);

    return TRUE;
}

heap_tuple_t* heap_get_next(scan_cursor_t* scan, row_fetch_direction_t direction)
{
    heap_tuple_t* tuple;
    return tuple;
}


void heap_slot_get_attrs(tuple_slot_t* slot)
{
    heap_tuple_slot_t* hslot = (heap_tuple_slot_t *)slot;
    slot_deform_heap_tuple(slot, hslot->tuple);
}




tuple_slot_t * create_tuple_table_slot(memory_context_t mem_ctx, dict_table_t* table)
{
    uint32 alloc_size = sizeof(tuple_slot_t);
    if (table) {
        alloc_size += table->column_count * sizeof(Datum) + table->column_count * sizeof(uint16);
    }

    tuple_slot_t* slot = (tuple_slot_t*)mem_alloc_zero(mem_ctx, alloc_size);
    if (unlikely(slot == NULL)) {
        return NULL;
    }

    slot->type = T_TupleTableSlot;
    slot->table_id = table->id;
    if (table) {
        slot->column_count = table->column_count;
        slot->values = (Datum *)((char *)slot + sizeof(tuple_slot_t));
        slot->lens = (uint16 *)((char *)slot + sizeof(tuple_slot_t) + table->column_count * sizeof(Datum));
    }

    return slot;
}

void destroy_tuple_table_slot(tuple_slot_t* slot)
{
    //ExecClearTuple(slot);
    mem_free(slot);
}


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

static void
heapam_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
					int options, BulkInsertState bistate)
{
	bool		shouldFree = true;
	HeapTuple	tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);

	/* Update the tuple with table oid */
	slot->tts_tableOid = RelationGetRelid(relation);
	tuple->t_tableOid = slot->tts_tableOid;

	/* Perform the insertion, and copy the resulting ItemPointer */
	heap_insert(relation, tuple, cid, options, bistate);
	ItemPointerCopy(&tuple->t_self, &slot->tts_tid);

	if (shouldFree)
		pfree(tuple);
}


status_t knl_handler::insert_row(que_sess_t* sess, scan_cursor_t* cursor)
{
    status_t err;

    trx_savepoint_t save_point;
    savepoint(sess, &save_point);

    static Datum values[MAXATTR];   /* current row's attribute values */
    static bool Nulls[MAXATTR];
    /* Prepare to insert or update pg_default_acl entry */
    MemSet(values, 0, sizeof(values));
    MemSet(nulls, false, sizeof(nulls));
    values[Anum_pg_default_acl_oid - 1] = ObjectIdGetDatum(defAclOid);
    values[Anum_pg_default_acl_defaclrole - 1] = ObjectIdGetDatum(iacls->roleid);
    values[Anum_pg_default_acl_defaclnamespace - 1] = ObjectIdGetDatum(iacls->nspid);
    values[Anum_pg_default_acl_defaclobjtype - 1] = CharGetDatum(objtype);
    values[Anum_pg_default_acl_defaclacl - 1] = PointerGetDatum(new_acl);

	tuple = heap_form_tuple(tupDesc, values, Nulls);


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


