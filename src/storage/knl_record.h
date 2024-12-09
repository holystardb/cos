#ifndef _KNL_RECORD_H
#define _KNL_RECORD_H

#include "cm_type.h"
#include "knl_data_type.h"

typedef struct st_dtuple dtuple_t;
typedef struct st_dfield dfield_t;


// 8 bytes,Structure for an SQL data field
struct st_dfield {
    void*       data;     // pointer to data
    uint32      ext : 1;  // TRUE=externally stored, FALSE=local
    uint32      len : 31; // data length
    dtype_t     type;     // type of data
};

struct st_dfield1 {
    uint32          field_no : 16;
    uint32          orig_len : 16; // original length
    //dfield_value_t  val;
};

// Structure for an SQL data tuple of fields (logical record)
struct st_dtuple {
    uint32      info_bits;
    uint32      n_fields;  // number of fields in dtuple
    dfield_t*   fields;
    UT_LIST_NODE_T(dtuple_t) list_node;
};


extern inline dtuple_t* dtuple_create_from_mem(
    void*  buf,      // in, out: buffer to use
    uint32 buf_size, // in: buffer size
    uint32 n_fields) // in: number of fields
{
    ut_ad(buf != NULL);
    ut_a(buf_size >= sizeof(dtuple_t) + (n_fields) * sizeof(dfield_t));

    dtuple_t* tuple = (dtuple_t*)buf;
    tuple->info_bits = 0;
    tuple->n_fields = n_fields;
    tuple->fields = (dfield_t*)&tuple[1];

    return tuple;
}

extern inline dtuple_t* dtuple_create(
    memory_stack_context_t* mem_stack_ctx,
    uint32 n_fields)
{
    void*  buf;
    uint32 buf_size;
    dtuple_t* tuple;

    buf_size = sizeof(dtuple_t) + (n_fields) * sizeof(dfield_t);
    buf = mcontext_stack_push(mem_stack_ctx, buf_size);
    tuple = dtuple_create_from_mem(buf, buf_size, n_fields);

    return tuple;
}

#define dtuple_get_nth_field(tuple, n) ((dfield_t*)(tuple)->fields + (n))

extern inline bool32 dfield_is_null(const dfield_t* field)
{
    return field->type.is_null;
}

extern inline void dfield_set_null(dfield_t* field)
{
    field->type.is_null = TRUE;
}

extern inline void dfield_set_data(dfield_t* field, const void* data, uint32 len)
{
    field->data = (void*)data;
    field->ext = 0;
    field->len = len;
    field->type.is_null = (len == 0);
}

extern inline void* dfield_get_data(const dfield_t* field)
{
    return((void*) field->data);
}

extern inline uint32 dfield_get_len(const dfield_t* field)
{
    return field->len;
}

extern inline void dfield_set_len(dfield_t* field, uint32 len)
{
    field->ext = 0;
    field->len = len;
}

extern inline uint32 dfield_is_ext(const dfield_t* field)
{
    return(field->ext);
}

extern inline void dfield_set_ext(dfield_t* field)
{
    field->ext = 1;
}

extern inline dtype_t* dfield_get_type(const dfield_t* field)
{
    return((dtype_t*) &(field->type));
}

extern inline void dfield_set_type(dfield_t* field, const dtype_t* type)
{
    field->type = *type;
}

/*-----------------------------------------------------------------------------------*/

// define the physical record simply as an array of bytes
typedef byte    rec_t;

#define FLEXIBLE_ARRAY_MEMBER       0 /* empty */


#pragma pack(4)

// row_header_t: 
//   size: 2B
//   column count: 10bit
//   bitmap: 0  null, 1 4byte,  2 8byte,  3 variant
typedef struct st_row_header
{
    union {
        struct {
            uint16 size;  // row size
            uint16 col_count : 10;  // column count
            uint16 flag : 6;
        };
        struct {
            uint16 aligned1;  // aligned row size
            uint16 aligned2 : 10; // aligned column count
            uint16 is_deleted : 1;
            uint16 is_ext : 1;  // externally stored
            uint16 is_migr : 1;   // migration flag
            uint16 is_change : 1; // changed flag after be locked
            uint16 reserved : 2;
        };
    };
    uint8  itl_id;  // row itl id
    uint8  null_bits;
} row_header_t;

#pragma pack()





// get the number of fields in a record
#define rec_get_column_count(rec)                   (((row_header_t *)(rec))->col_count)
#define rec_get_size(rec)                           (((row_header_t *)(rec))->size)
#define rec_get_null_bits_in_bytes(col_count)       (((col_count) + 7) / 8)

#define REC_NULL_VALUE_LEN                          (uint16)0xFFFF

extern inline uint8 rec_get_nth_column_bits(rec_t* rec, uint16 n);
extern inline void rec_set_nth_column_bits(rec_t* rec, uint16 n, uint8 bits);

#define rec_get_nth_column(rec, n, len)  ((rec) + rec_get_nth_column_offset(rec, n, len))

extern inline uint16 rec_get_nth_column_offset(
    rec_t* rec,
    uint16 n, // in: index of the field
    uint16* len); // out: length of the field; REC_NULL_VALUE_LEN if null

extern inline status_t rec_get_columns_offset(rec_t* rec,
    uint16* size, uint16* col_count, uint16* offsets, uint16* lens);



#endif  /* _KNL_DATA_H */
