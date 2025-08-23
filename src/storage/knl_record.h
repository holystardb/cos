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


typedef struct st_tuple_slot
{
    NodeTag       type;
    dict_table_t* table;
    row_id_t      row_id;
    uint16        column_count;
    Datum*        values; // current per-attribute values
    uint16*       lens; // current per-attribute lenght, 0xFFFF if is null

    //MemoryContext tts_mcxt;   /* slot itself is in this context */
} tuple_slot_t;



#pragma pack(4)

/** using in heap reorganize page */
#define ROW_REORGANIZING_FLAG         0x8000


// row_header_t: 
//   size: 2B
//   column count: 10bit
//   bitmap: 0  null, 1 4byte,  2 8byte,  3 variant
typedef struct st_row_header
{
    union {
        struct {
            uint16 size;  // row size, must be the first member variable in row_head_t
            uint16 col_count : 10;  // column count
            uint16 flag : 6;
        };
        struct {
            uint16 aligned1;  // aligned row size
            uint16 aligned2 : 10; // aligned column count
            uint16 is_deleted : 1;
            uint16 is_ext : 1; // externally stored
            uint16 is_migrate : 1; // migration flag, row data is on another page
            uint16 is_changed : 1; // changed flag after be locked
            uint16 is_has_nulls : 1;
            uint16 reserved : 1;
        };
        struct {
            uint16 aligned3;  // aligned row size
            uint16 slot;      // temporarily save dir slot of row for heap_reorganize_page
        };
    };
    uint8  itl_id;  // row itl id

    /* MORE DATA FOLLOWS AT END OF STRUCT */
    // null bits
    // columns data
} row_header_t;

#define HEAP_TUPLE_HEADER_ITL                         (OFFSET_OF(heap_tuple_t, itl_id))
#define HEAP_TUPLE_HEADER_SIZE                        (OFFSET_OF(heap_tuple_t, itl_id) + 1)
#define HEAP_TUPLE_NULL_BITMAP_LENGTH(column_count)   (((column_count) + 7) / 8)
#define HEAP_TUPLE_NULL_VALUE_LEN                     (uint16)0xFFFF


// row_header_t: 
//   size: 2B
//   column count: 10bit
//   bitmap: 0  null, 1 4byte,  2 8byte,  3 variant
typedef struct st_heap_tuple
{
    union {
        struct {
            uint16 size;  // row size, must be the first member variable in row_head_t
            uint16 column_count : 10;
            uint16 flag : 6;
        };
        struct {
            uint16 aligned1;  // aligned row size
            uint16 aligned2 : 10; // aligned column count
            uint16 is_deleted : 1;
            uint16 is_ext : 1; // externally stored
            uint16 is_migrate : 1; // migration flag, row data is on another page
            uint16 is_changed : 1; // changed flag after be locked
            uint16 is_has_nulls : 1;
            uint16 reserved : 1;
        };
        struct {
            uint16 aligned3;  // aligned row size
            uint16 slot;      // temporarily save dir slot of row for heap_reorganize_page
        };
    };
    uint8  itl_id;  // row itl id

    /* MORE DATA FOLLOWS AT END OF STRUCT */
    // null bits
    // columns data
} heap_tuple_t;

typedef struct st_heap_tuple_slot
{
    tuple_slot_t  base;
    heap_tuple_t* tuple;   /* physical tuple */
    uint32        off;     /* saved state for slot_deform_heap_tuple */
    heap_tuple_t  tupdata; /* optional workspace for storing tuple */
} heap_tuple_slot_t;


typedef struct st_index_tuple
{
    row_header_t   header;
    row_id_t       heap_row_id;  /* reference TID to heap tuple */

	/* ---------------
	 * t_info is laid out in the following fashion:
	 *
	 * 15th (high) bit: has nulls
	 * 14th bit: has var-width attributes
	 * 13th bit: AM-defined meaning
	 * 12-0 bit: size of tuple
	 * ---------------
	 */

	unsigned short t_info;		/* various info about tuple */
    /* MORE DATA FOLLOWS AT END OF STRUCT */
} index_tuple_t;




#pragma pack()


/* routines for a TupleTableSlot implementation */
struct TupleTableSlotOps
{
	/* Minimum size of the slot */
	size_t		base_slot_size;

	/* Initialization. */
	void		(*init) (TupleTableSlot *slot);

	/* Destruction. */
	void		(*release) (TupleTableSlot *slot);

	/*
	 * Clear the contents of the slot. Only the contents are expected to be
	 * cleared and not the tuple descriptor. Typically an implementation of
	 * this callback should free the memory allocated for the tuple contained
	 * in the slot.
	 */
	void		(*clear) (TupleTableSlot *slot);

	/*
	 * Fill up first natts entries of tts_values and tts_isnull arrays with
	 * values from the tuple contained in the slot. The function may be called
	 * with natts more than the number of attributes available in the tuple,
	 * in which case it should set tts_nvalid to the number of returned
	 * columns.
	 */
	void		(*getsomeattrs) (TupleTableSlot *slot, int natts);

	/*
	 * Returns value of the given system attribute as a datum and sets isnull
	 * to false, if it's not NULL. Throws an error if the slot type does not
	 * support system attributes.
	 */
	Datum		(*getsysattr) (TupleTableSlot *slot, int attnum, bool *isnull);

	/*
	 * Make the contents of the slot solely depend on the slot, and not on
	 * underlying resources (like another memory context, buffers, etc).
	 */
	void		(*materialize) (TupleTableSlot *slot);

	/*
	 * Copy the contents of the source slot into the destination slot's own
	 * context. Invoked using callback of the destination slot.
	 */
	void		(*copyslot) (TupleTableSlot *dstslot, TupleTableSlot *srcslot);

	/*
	 * Return a heap tuple "owned" by the slot. It is slot's responsibility to
	 * free the memory consumed by the heap tuple. If the slot can not "own" a
	 * heap tuple, it should not implement this callback and should set it as
	 * NULL.
	 */
	HeapTuple	(*get_heap_tuple) (TupleTableSlot *slot);

	/*
	 * Return a minimal tuple "owned" by the slot. It is slot's responsibility
	 * to free the memory consumed by the minimal tuple. If the slot can not
	 * "own" a minimal tuple, it should not implement this callback and should
	 * set it as NULL.
	 */
	MinimalTuple (*get_minimal_tuple) (TupleTableSlot *slot);

	/*
	 * Return a copy of heap tuple representing the contents of the slot. The
	 * copy needs to be palloc'd in the current memory context. The slot
	 * itself is expected to remain unaffected. It is *not* expected to have
	 * meaningful "system columns" in the copy. The copy is not be "owned" by
	 * the slot i.e. the caller has to take responsibility to free memory
	 * consumed by the slot.
	 */
	HeapTuple	(*copy_heap_tuple) (TupleTableSlot *slot);

	/*
	 * Return a copy of minimal tuple representing the contents of the slot.
	 * The copy needs to be palloc'd in the current memory context. The slot
	 * itself is expected to remain unaffected. It is *not* expected to have
	 * meaningful "system columns" in the copy. The copy is not be "owned" by
	 * the slot i.e. the caller has to take responsibility to free memory
	 * consumed by the slot.
	 */
	MinimalTuple (*copy_minimal_tuple) (TupleTableSlot *slot);
};





// get the number of fields in a record
#define rec_get_column_count(rec)                   (((row_header_t *)(rec))->column_count)
#define rec_get_size(rec)                           (((row_header_t *)(rec))->size)
#define rec_get_null_bits_in_bytes(col_count)       (((col_count) + 7) / 8)
#define rec_has_null(rec)                           (((row_header_t *)(rec))->is_has_nulls)
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
