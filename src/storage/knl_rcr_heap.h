#ifndef _KNL_RCR_HEAP_H
#define _KNL_RCR_HEAP_H

#include "cm_type.h"
#include "knl_record.h"
#include "knl_trx_types.h"
#include "knl_dict.h"
#include "knl_session.h"

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus



/*
    head
    heap_page_header
    rows
    free space
    dir
    itl
    tail
*/


// --------------------------------------------------------------------------

typedef byte    heap_map_header_t;

#define HEAP_MAP_ROOT_PAGE_HEADER   FIL_PAGE_DATA

#define HEAP_MAP_LEVEL              0
#define HEAP_MAP_UP_PAGE            2
#define HEAP_MAP_UP_OFFSET          6
#define HEAP_MAP_FSEG               8 // all page for table, only for level = 0
#define HEAP_MAP_LIST0_COUNT        1
#define HEAP_MAP_LIST1_COUNT        1
#define HEAP_MAP_LIST2_COUNT        1
#define HEAP_MAP_LIST3_COUNT        1
#define HEAP_MAP_LIST4_COUNT        1
#define HEAP_MAP_LIST5_COUNT        1


#define HEAP_MAP_FREE               0

#define HEAP_MAP_LIST0_PAGE         (8 + FLST_BASE_NODE_SIZE)
#define HEAP_MAP_LIST1_PAGE         (8 + 2 * FLST_BASE_NODE_SIZE)
#define HEAP_MAP_LIST2_PAGE         (8 + 3 * FLST_BASE_NODE_SIZE)
#define HEAP_MAP_LIST3_PAGE         (8 + 4 * FLST_BASE_NODE_SIZE)
#define HEAP_MAP_LIST4_PAGE         (8 + 5 * FLST_BASE_NODE_SIZE)
#define HEAP_MAP_LIST5_PAGE         (8 + 6 * FLST_BASE_NODE_SIZE)
#define HEAP_MAP_LIST6_PAGE         (8 + 7 * FLST_BASE_NODE_SIZE)
#define HEAP_MAP_LIST7_PAGE         (8 + 8 * FLST_BASE_NODE_SIZE)

#define HEAP_MAP_HEADER_SIZE        (8 + 9 * FLST_BASE_NODE_SIZE)


// --------------------------------------------------------------------------

typedef byte    heap_map_page_node_header_t;

#define HEAP_MAP_PAGE_NODE_HEADER      FIL_PAGE_DATA
#define HEAP_MAP_ROOT_PAGE_NODE_HEADER (FIL_PAGE_DATA + HEAP_MAP_HEADER_SIZE)

#define HEAP_MAP_LIST_NODE_FLST             0
#define HEAP_MAP_LIST_NODE_USED_COUNT  FLST_NODE_SIZE
#define HEAP_MAP_LIST_NODE_FIRST_USED  (4 + FLST_NODE_SIZE)
#define HEAP_MAP_LIST_NODE_FIRST_FREE  (6 + FLST_NODE_SIZE)

#define HEAP_MAP_PAGE_NODE_HEADER_SIZE (8 + FLST_NODE_SIZE)

#define INVALID_MAP_LIST_NODE_OFFSET   0xFFFF


// --------------------------------------------------------------------------
// page item of map page

typedef byte    heap_map_list_node_t;

#define HEAP_MAP_LIST_NODE_PAGE_NO    0
#define HEAP_MAP_LIST_NODE_PREV       4
#define HEAP_MAP_LIST_NODE_NEXT       6

#define HEAP_MAP_LIST_NODE_SIZE       8


/* A struct for storing heap_map_list_node_t, when it is used in C program data structures. */
typedef struct st_map_list_node {
    uint32    page_no;    // page number of heap data
    uint16    offset;
    uint16    prev;       // node offset
    uint16    next;       // node offset
} map_list_node_t;

typedef struct st_map_list_node_id {
    map_list_node_t  node;
    uint32           owner_page_no;   // page number of node
} map_list_node_id_t;


typedef struct st_map_index {
    uint64 file : 10;  // map page
    // 10bit: 1024
    // 12bit: 4096
    // 14bit: 16384, 16k
    uint64 page : 30;  
    // 30bit: 1G * 8K = 8T
    // 28bit: 256M * 8K = 2T
    uint64 slot : 16;    // map slot
    uint64 list_id : 8;  // map list id

    uint32   space_id : 20; // 1M
    uint32   slot : 12; // 4096
    uint32   page_no;
} map_index_t;


// --------------------------------------------------------------------------

typedef byte    heap_page_header_t;

#define HEAP_HEADER_OFFSET          FIL_PAGE_DATA

#define HEAP_HEADER_LSN             0
#define HEAP_HEADER_SCN             8
#define HEAP_HEADER_LOWER           16  /* offset to start of free space */
#define HEAP_HEADER_UPPER           18  /* offset to end of free space */
#define HEAP_HEADER_FREE_SIZE       20
#define HEAP_HEADER_FIRST_FREE_DIR  22
#define HEAP_HEADER_DIRS            24  // row directory count
#define HEAP_HEADER_ROWS            26  // row count
#define HEAP_HEADER_ITLS            28  // itl count
#define HEAP_HEADER_MAP_PAGE        30  // heap fsm map page
#define HEAP_HEADER_MAP_SLOT        34  // slot in heap fsm map page
#define HEAP_HEADER_MAP_LIST_ID     36  // map list id

#define HEAP_HEADER_SIZE            37  //


#define HEAP_PAGE_MAX_ITLS       0xFF
#define HEAP_INVALID_ITL_ID      0xFF
#define HEAP_NO_FREE_DIR         0x7FFF

// ---------------------------------------------------------------

#define HEAP_PAGE_GET_HEADER(page)   (heap_page_header_t *)(HEAP_HEADER_OFFSET + page)
#define HEAP_HEADER_GET_PAGE(header) (page_t *)((char *)header - HEAP_HEADER_OFFSET)

#define HEAP_GET_ROW(page, dir)      (row_header_t *)((char *)(page) + (dir)->offset)
#define HEAP_ROW_DATA_OFFSET(row) \
    (cm_row_init_size((row)->is_csf, ROW_COLUMN_COUNT(row)) + ((row)->is_migr ? sizeof(row_id_t) : 0))

#define HEAP_ROW_ID_LENGTH           10
#define HEAP_MIN_ROW_SIZE            (sizeof(row_header_t) + HEAP_ROW_ID_LENGTH)


// A line pointer on a buffer page
typedef struct st_item_id
{
    uint32  lp_off:15,  /* offset to tuple (from start of page) */
            lp_flags:2, /* state of line pointer, see below */
            lp_len:15;  /* byte length of tuple */
} item_id_t;

typedef struct st_heap_insert_assist {
    uint64 table_id; // for logical decode
    uint64 scn;  // next scn, for logical decode
    uint32 cid;
    uint32 undo_page_no;
    uint16 undo_page_offset;
    uint8  dir;
    uint8  need_redo;
    uint8  need_undo;
    //char   data[FLEXIBLE_ARRAY_MEMBER];
} heap_insert_assist_t;


struct insert_node_t {
    uint32         type;   // INS_VALUES, INS_SEARCHED, or INS_DIRECT
    dict_table_t*  table;  // table where to insert

    dtuple_t*      heap_row;    // row to insert
    UT_LIST_BASE_NODE_T(dtuple_t) index_rows; // one for each index
};

struct delete_node_t {
    uint32         type;   // INS_VALUES, INS_SEARCHED, or INS_DIRECT
    dict_table_t*  table;  // table where to insert

    dtuple_t*      heap_row;    // row to insert
    UT_LIST_BASE_NODE_T(dtuple_t) index_rows; // one for each index
};

struct udate_node_t {
    uint32         type;   // INS_VALUES, INS_SEARCHED, or INS_DIRECT
    dict_table_t*  table;  // table where to insert

    uint32         info_bits;	/*!< new value of info bits to record; default is 0 */
    uint32         n_fields;  // number of update fields
    //upd_field_t*    fields;    // array of update fields


    dtuple_t*      heap_row;    // row to insert
    UT_LIST_BASE_NODE_T(dtuple_t) index_rows; // one for each index
};


// -------------------------------------------------------------------

#pragma pack(4)

typedef struct st_heap_header {
    uint64         lsn;
    uint64         scn;
    uint16         lower;
    uint16         upper;
    uint16         free_size;
    uint16         first_free_dir;
    uint16         dir_count;
    uint16         row_count;
    uint8          itl_count;
} heap_header_t;

// itl_t: 20 Bytes
typedef struct st_itl {
    uint64         scn;
    trx_slot_id_t  trx_slot_id;
    uint16         fsc; // free space credit (bytes) for delete row
    uint16         is_active : 1;  // committed or not
    uint16         is_ow_scn : 1;   // txn scn overwrite or not
    uint16         is_copied : 1;  // itl is copied or not
    uint16         is_free : 1;
    uint16         unused : 12;
} itl_t;


#define ROW_ID_FILE_BITS          16  // 65536
#define ROW_ID_SLOT_BITS          12  // 4096
#define ROW_ID_PAGE_NO_BITS       32  // 4GB * 16KB = 64TB
#define ROW_ID_UNUSED_BITS        4


#define HEAP_PAGE_INVALID_SLOT           (uint32)((1 << ROW_ID_SLOT_BITS) - 1)
#define HEAP_PAGE_INVALID_FILE_ID        (uint32)((1 << ROW_ID_FILE_BITS) - 1)
#define HEAP_PAGE_INVALID_ROWID(row_id)  (row_id.id == UINT_MAX64)


// row_id_t: 8 Bytes
typedef union st_row_id1
{
    uint64 id;
    struct {
        uint64 page_no  : ROW_ID_PAGE_NO_BITS;
        uint64 file     : ROW_ID_FILE_BITS;  // file index
        uint64 slot     : ROW_ID_SLOT_BITS;  // dir index
        uint64 reserved : ROW_ID_UNUSED_BITS;

    };
} row_id1_t;


// logical_row_id_t: 8 Bytes
typedef struct st_row_id
{
    page_no_t  page_no;
    space_id_t space_id;
    uint32     slot; // dir slot
} row_id_t;

typedef union st_row_undo_rollptr {
    uint32 undo_roll_ptr;
    struct {
        uint32  undo_space_index : 4;
        uint32  undo_page_no : 12;
        uint32  undo_page_offset : 16;
    };
} row_undo_rollptr_t;


// row_dir_t: 16 Bytes
typedef struct st_row_dir {
    uint64  scn; // commit scn or command id
    union {
        uint32 undo_rollptr;
        struct {
            uint32  undo_space_index : 4;
            uint32  undo_page_no : 12; // 800 * 4k * 16k = 50GB
            uint32  undo_page_offset : 16;
        };
    };
    union {
        uint32 offsets;
        struct {
            uint16 offset; // offset of row
            uint16 is_free : 1;  // dir is free
            uint16 is_ow_scn : 1;
            uint16 is_lob_part : 1;  // for lob
            uint16 aligned : 13;
        };
        struct {
            uint16 offset; // offset of row
            uint16 is_free : 1;  // dir is free
            uint16 free_next_dir : 15; // if dir is free
        };
    };
} row_dir_t;

/*
typedef struct st_heap_page_header
{
    uint64      lsn;
    uint64      scn;
    uint16      lower;      // offset to start of free space
    uint16      upper;      // offset to end of free space
    uint16      free_size;  // free size of page
    uint16      first_free_dir;
    uint16      dirs;
    uint16      rows;
    uint8       used_itls;
    uint8       first_used_itl;
    uint8       free_itls;
    uint8       first_free_itl;
} heap_page_header_t;
*/


#pragma pack()

// --------------------------------------------------------------------------

// --------------------------------------------------------------------------

//extern bool32 heap_extend_table_segment(dict_table_t* table);

extern uint32 heap_create_entry(uint32 space_id);

extern inline void heap_page_init(buf_block_t* block, dict_table_t* table, mtr_t* mtr);

extern status_t heap_insert(que_sess_t* sess, insert_node_t* insert_node);

extern inline void heap_set_itl_trx_end(buf_block_t* block,
    trx_slot_id_t slot_id, uint8 itl_id, uint64 scn, mtr_t* mtr);


#ifdef __cplusplus
}
#endif // __cplusplus


#endif  /* _KNL_RCR_HEAP_H */
