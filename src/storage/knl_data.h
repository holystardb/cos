#ifndef _KNL_DATA_H
#define _KNL_DATA_H

#include "cm_type.h"
#include "knl_data_type.h"

typedef struct st_dtuple dtuple_t;
typedef struct st_dfield dfield_t;


// Structure for an SQL data field
typedef struct st_dfield_value {
    void*       data;     // pointer to data
    uint32      ext : 1;  // TRUE=externally stored, FALSE=local
    uint32      len : 31; // data length
    dtype_t     type;     // type of data
} dfield_value_t;

struct st_dfield {
    uint32          field_no : 16;
    uint32          orig_len : 16; // original length
    dfield_value_t  val;
};

// Structure for an SQL data tuple of fields (logical record)
struct st_dtuple {
    uint32      info_bits;
    uint32      n_fields;  // number of fields in dtuple
    dfield_t**  fields;
    UT_LIST_NODE_T(dtuple_t) list_node;
};

extern inline bool32 dfield_is_null(const dfield_t* field)
{
    return field->val.type.is_null;
}

extern inline void dfield_set_null(dfield_t* field)
{
    field->val.type.is_null = TRUE;
}

extern inline void dfield_set_data(dfield_t* field, const void* data, uint32 len)
{
    field->val.data = (void*)data;
    field->val.ext = 0;
    field->val.len = len;
    field->val.type.is_null = (len == 0);
}

extern inline void* dfield_get_data(const dfield_t* field)
{
    return((void*) field->val.data);
}

extern inline uint32 dfield_get_len(const dfield_t* field)
{
    return field->val.len;
}

extern inline void dfield_set_len(dfield_t* field, uint32 len)
{
    field->val.ext = 0;
    field->val.len = len;
}

extern inline uint32 dfield_is_ext(const dfield_t* field)
{
    return(field->val.ext);
}

extern inline void dfield_set_ext(dfield_t* field)
{
    field->val.ext = 1;
}

extern inline dtype_t* dfield_get_type(const dfield_t* field)
{
    return((dtype_t*) &(field->val.type));
}

extern inline void dfield_set_type(dfield_t* field, const dtype_t* type)
{
    field->val.type = *type;
}


#endif  /* _KNL_DATA_H */
