#include "knl_record.h"

// n: in, index of the field
inline uint8 rec_get_nth_column_bits(rec_t* rec, uint16 n)
{
    byte* nulls_ptr;
    ut_ad(rec_has_null(rec));
    nulls_ptr = rec + HEAP_TUPLE_HEADER_SIZE;
    nulls_ptr += (n / 8);

    return ((*nulls_ptr) & (1 << (7 - n / 8)));
}

inline void rec_set_nth_column_bits(rec_t* rec, uint16 n, uint8 bits)
{
    byte* nulls_ptr;

    nulls_ptr = rec + OFFSET_OF(row_header_t, null_bits);
    nulls_ptr += (n / 8);
    *nulls_ptr |= (bits << (7 - n / 8));
}

inline status_t rec_get_columns_offset(rec_t* rec,
    uint16* size, uint16* col_count, uint16* offsets, uint16* lens)
{
    ut_ad(rec);
    ut_ad(size);
    ut_ad(col_count);
    ut_ad(offsets);
    ut_ad(lens);

    *size = rec_get_size(rec);
    *col_count = rec_get_column_count(rec);

    if (*size > ROW_RECORD_MAX_SIZE || *col_count > ROW_MAX_COLUMN_COUNT) {
        return CM_ERROR;
    }

    bool32 has_nulls = rec_has_null(rec);
    uint16 null_bytes = has_nulls ? rec_get_null_bits_in_bytes(*col_count) : 0;
    uint16 pos = HEAP_TUPLE_HEADER_SIZE + null_bytes;

    for (uint16 i = 0; i < *col_count; i++) {
        if (has_nulls && rec_get_nth_column_bits(rec, i) == 0) {
            lens[i] = REC_NULL_VALUE_LEN;
            continue;
        }
        // length
        lens[i] = mach_read_compressed(rec + pos);
        pos += mach_get_compressed_size(lens[i]);
        // value
        offsets[i] = pos;
        pos += lens[i];
    }

    return CM_SUCCESS;
}


// get an offset to the nth data column in a record.
// return offset from the origin of rec
inline uint16 rec_get_nth_column_offset(
    rec_t* rec,
    uint16 n, // in: index of the column
    uint16* len) // out: length of the column; REC_NULL_VALUE_LEN if null
{
    ut_ad(rec);

    uint16 size = rec_get_size(rec);
    uint16 col_count = rec_get_column_count(rec);

    if (size > ROW_RECORD_MAX_SIZE || col_count > ROW_MAX_COLUMN_COUNT || col_count < (n+1)) {
        *len = 0;
        return 0;
    }

    uint16 offset = 0;
    uint16 null_bytes = rec_get_null_bits_in_bytes(col_count);
    uint16 pos = OFFSET_OF(row_header_t, null_bits) + null_bytes;

    for (uint16 i = 0; i < n; i++) {
        if (rec_get_nth_column_bits(rec, i) == 0) {
            *len = REC_NULL_VALUE_LEN;
            continue;
        }
        // length
        *len = mach_read_compressed(rec + pos);
        pos += mach_get_compressed_size(*len);
        // value
        offset = pos;
        pos += *len;
    }

    return offset;
}


// get the number of fields in a record
#define rec_get_column_count(rec)                   (((row_header_t *)(rec))->column_count)
#define rec_get_size(rec)                           (((row_header_t *)(rec))->size)
#define rec_has_null(rec)                           (((row_header_t *)(rec))->is_has_nulls)


// n: in, index of the field
inline bool32 tuple_check_nth_col_is_null(heap_tuple_t* tuple, uint16 nth)
{
    bool32 is_null = FALSE;
    if (tuple->is_has_nulls) {
        byte* nulls_ptr = (byte *)tuple + HEAP_TUPLE_HEADER_SIZE;
        nulls_ptr += (nth / 8);
        is_null = ((*nulls_ptr) & (1 << (7 - nth / 8))) ? FALSE : TRUE;
    }
    return is_null;
}

inline void tuple_set_nth_col_null(heap_tuple_t* tuple, uint16 nth)
{
    byte* nulls_ptr = (byte *)tuple + HEAP_TUPLE_HEADER_SIZE;
    nulls_ptr += (nth / 8);
    *nulls_ptr |= (1 << (7 - nth/ 8));
    tuple->is_has_nulls = TRUE;
}

uint16 heap_tuple_compute_data_size(tuple_slot_t* slot)
{
    uint16 data_length = 0;

    for (uint32 i = 0; i < slot->table->column_count; i++)
    {
        dict_col_t* col = slot->table->columns[i];
        if (slot->lens[i] != HEAP_TUPLE_NULL_VALUE_LEN) {
            data_length += slot->lens[i];
        }
        if (col->default_const_value == NULL) {
            continue;
        }
        if (g_data_type_desc[col->mtype]->fixed_length > 0) {
            data_length += g_data_type_desc[col->mtype]->fixed_length;
        } else if (g_data_type_desc[col->mtype]->is_blob) {
            data_length += sizeof(row_dir_t);
        } else {
            data_length += col->default_const_value_len;
        }
    }
    return data_length;
}


// Builds a ROW_FORMAT=COMPACT record out of a data tuple
static inline status_t heap_form_tuple(que_sess_t* sess, tuple_slot_t* slot, heap_tuple_t* tuple)
{
    byte*  nulls_ptr;
    byte*  data;
    dict_table_t* table;
    uint32 null_bytes = HEAP_TUPLE_NULL_BITMAP_LENGTH(table->column_count);
    uint16 data_length = heap_tuple_compute_data_size(slot);
    uint16 size = HEAP_TUPLE_HEADER_SIZE + null_bytes + data_length;

    tuple = (heap_tuple_t*)mcontext_stack_push(sess->mcontext_stack, size);
    if (tuple == NULL) {
        return CM_ERROR;
    }
    memset(tuple, 0x00, size);
    tuple->size = size;
    tuple->column_count = table->column_count;
    tuple->flag = 0;

    data = (byte*)tuple + HEAP_TUPLE_HEADER_SIZE + null_bytes;

    for (uint32 i = 0; i < slot->table->column_count; i++) {
        dict_col_t* col = slot->table->columns[i];

        if (slot->lens[i] == HEAP_TUPLE_NULL_VALUE_LEN) {
            if (col->default_const_value) {
                memcpy(data, col->default_const_value, col->default_const_value_len);
                data += col->default_const_value_len;
            } else {
                tuple_set_nth_col_null(tuple, i);
            }
            continue;
        }

        if (g_data_type_desc[col->mtype]->fixed_length > 0) {
            memcpy(data, slot->values[i], g_data_type_desc[col->mtype]->fixed_length);
            data += g_data_type_desc[col->mtype]->fixed_length;
        } else if (col->is_ext) {
            tuple->is_ext = TRUE;
            memcpy(data, slot->values[i], slot->lens[i]);
            data += slot->lens[i];
        } else {
            mach_write_to_2(data, slot->lens[i]);
            memcpy(data + 2, slot->values[i], slot->lens[i]);
            data += 2 + slot->lens[i];
        }
    }

    return CM_SUCCESS;
}


status_t heap_deform_tuple(tuple_slot_t* slot, heap_tuple_t* tuple)
{
    uint16 column_count = ut_min(slot->column_count, tuple->column_count);

    if (tuple->size > ROW_RECORD_MAX_SIZE || tuple->column_count > ROW_MAX_COLUMN_COUNT) {
        return CM_ERROR;
    }

    /* We can only fetch as many attributes as the tuple has. */

    uint16 null_bytes = tuple->is_has_nulls ? HEAP_TUPLE_NULL_BITMAP_LENGTH(tuple->column_count) : 0;
    byte* data_ptr = (byte *)tuple + HEAP_TUPLE_HEADER_SIZE + null_bytes;
    for (uint16 i = 0; i < tuple->column_count; i++) {
        if (tuple_check_nth_col_is_null(tuple, i)) {
            slot->lens[i] = REC_NULL_VALUE_LEN;
            continue;
        }

        dict_col_t* col = slot->table->columns[i];
        if (g_data_type_desc[col->mtype]->fixed_length > 0) {
            slot->values[i] = PointerGetDatum(data_ptr);
            slot->lens[i] = g_data_type_desc[col->mtype]->fixed_length;
        } else if (col->is_ext) {
            slot->values[i] = PointerGetDatum(data_ptr);
            slot->lens[i] = sizeof(row_id_t);
        } else {
            slot->lens[i] = mach_read_from_2(data_ptr);
            slot->values[i] = PointerGetDatum(data_ptr + 2);
        }
        data_ptr += slot->lens[i];
    }

    /* other columns */
    for (uint16 i = tuple->column_count; i < slot->column_count; i++) {
        dict_col_t* col = slot->table->columns[i];
        if (col->nullable) {
            slot->lens[i] = REC_NULL_VALUE_LEN;
            continue;
        }

        ut_ad(col->default_const_value);
        slot->lens[i] = col->default_const_value_len;
        slot->values[i] = PointerGetDatum(col->default_const_value);
    }

    return CM_SUCCESS;

}

