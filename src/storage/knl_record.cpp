#include "knl_record.h"

// n: in, index of the field
inline uint8 rec_get_nth_column_bits(rec_t* rec, uint16 n)
{
    byte* nulls_ptr;

    nulls_ptr = rec + OFFSET_OF(row_header_t, null_bits);
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

    uint16 null_bytes = rec_get_null_bits_in_bytes(*col_count);
    uint16 pos = OFFSET_OF(row_header_t, null_bits) + null_bytes;

    for (uint16 i = 0; i < *col_count; i++) {
        if (rec_get_nth_column_bits(rec, i) == 0) {
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

