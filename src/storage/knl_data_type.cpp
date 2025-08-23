#include "knl_data_type.h"

data_type_desc_t* g_data_type_desc[DATA_TYPE_MAX] = { NULL };

data_type_desc_t data_type_desc_def[] = {
    { DATA_STRING,     "string",        0, 0 },
    { DATA_VARCHAR,    "varchar",       0, 0 },
    { DATA_CHAR,       "char",          1, 0 },
    { DATA_BINARY,     "binary",        0, 1 },
    { DATA_VARBINARY,  "varbinary",     0, 1 },
    { DATA_RAW,        "raw",           0, 1 },
    { DATA_CLOB,       "clob",          0, 1 },
    { DATA_BLOB,       "blob",          0, 1 },

    { DATA_NUMBER,     "number",        8, 0 },
    { DATA_BIGINT,     "bigint",        8, 0 },
    { DATA_INT,        "int",           4, 0 },
    { DATA_SMALLINT,   "smallint",      2, 0 },

};



void data_type_desc_init()
{
    uint32 count = array_elements(data_type_desc_def);
    for (uint32 i = 0; i < count; i++) {
        data_type_desc_t* desc = &data_type_desc_def[i];
        g_data_type_desc[desc->type] = desc;
    }
}

