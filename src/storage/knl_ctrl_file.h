#ifndef _KNL_CTRL_FILE_H
#define _KNL_CTRL_FILE_H

#include "cm_type.h"
#include "cm_error.h"
#include "cm_counter.h"
#include "m_ctype.h"
#include "cm_file.h"
#include "cm_memory.h"
#include "cm_vm_pool.h"
#include "knl_defs.h"



class db_data_file_t {
public:
    db_data_file_t();

public:
    uint32    node_id;
    uint32    space_id;
    char      file_name[DB_DATA_FILE_NAME_MAX_LEN];
    uint64    size;
    uint64    max_size;
    bool32    autoextend;
    uint32    status;

    UT_LIST_NODE_T(db_data_file_t) list_node;

    byte* serialize(byte* buf_ptr, byte* end_ptr);
    byte* deserialize(byte* buf_ptr, byte* end_ptr);

    status_t delete_data_file();
    status_t create_data_file(bool32 is_extend_file);

};

class db_space_t {
public:
    db_space_t();

public:
    mutex_t         mutex;
    char            space_name[DB_OBJECT_NAME_MAX_LEN];
    uint32          space_id;
    uint32          page_hwm;  // page count
    uint64          size;
    uint64          max_size;
    union {
        uint32      flags;
        struct {
            uint32  is_autoextend : 1;
            uint32  is_single_table : 1;
            uint32  reserved : 30;
        };
    };

    UT_LIST_BASE_NODE_T(db_data_file_t) data_file_list;

    byte* serialize(byte* buf_ptr, byte* end_ptr);
    byte* deserialize(byte* buf_ptr, byte* end_ptr);

    status_t create_data_files(bool32 is_extend_file);
    status_t add_data_file(db_data_file_t* data_file);

};

class db_ctrl_t {
public:
    db_ctrl_t();

public:
    mutex_t         mutex;
    bool32          io_in_progress;

    uint64          version;
    uint64          ver_num;
    uint64          init_time;  // 
    uint64          start_time;
    uint64          scn;
    char            database_name[DB_OBJECT_NAME_MAX_LEN];
    CHARSET_INFO*   charset_info;
    uint32          redo_data_file_count;
    uint32          undo_data_file_count;
    uint32          temp_data_file_count;
    uint32          system_space_count;
    uint32          user_space_count;
    uint32          user_space_data_file_count;

    db_space_t      system_spaces[DB_SYSTEM_SPACE_MAX_COUNT];
    db_data_file_t  system_data_files[DB_SYSTEM_DATA_FILE_MAX_COUNT];
    db_space_t      user_spaces[DB_USER_SPACE_MAX_COUNT];
    db_data_file_t  user_data_files[DB_USER_DATA_FILE_MAX_COUNT];

    status_t set_database_info(char* db_name, CHARSET_INFO* charset);

    status_t add_dbwr_file(char* file_name, uint64 size);
    status_t add_systrans_file(char* file_name, uint32 rseg_count);
    status_t add_redo_file(char* file_name, uint64 size);
    status_t add_undo_file(char* file_name, uint64 size, uint64 max_size);
    status_t add_system_file(char* file_name, uint64 size, uint64 max_size, bool32 autoextend);
    status_t add_temp_file(char* file_name, uint64 size, uint64 max_size);
    status_t add_user_space_file(char* space_name, char* file_name, uint64 size, uint64 max_size, bool32 autoextend);
    status_t add_user_space(char* space_name);

    status_t serialize_core(byte* buf, uint32 buf_size);
    status_t serialize_space(byte* buf_ptr, byte* end_ptr, uint32 space_count, db_space_t* spaces);
    status_t serialize_data_file(byte* buf_ptr, byte* end_ptr, uint32 file_count, db_data_file_t* files);
    status_t serialize(byte* buf, uint32 buf_size);

    status_t deserialize_core(byte* buf, uint32 buf_size);
    status_t deserialize_space(byte* buf_ptr, byte* end_ptr, uint32 space_count, db_space_t* spaces);
    status_t deserialize_data_file(byte* buf_ptr, byte* end_ptr, uint32 file_count, db_data_file_t* files);
    status_t deserialize(byte* buf, uint32 buf_size);

    status_t save_to_ctrl_file(char* name, byte* buf, uint32 buf_size);
    status_t read_from_ctrl_file(char* name, byte* buf, uint32 buf_size);

    db_space_t* get_system_space_by_space_id(uint32 space_id);
    db_space_t* get_user_space_by_space_id(uint32 space_id);
    db_space_t* get_user_space_by_pos(uint32 index) {
        return (index < DB_USER_SPACE_MAX_COUNT) ? &user_spaces[index] : NULL;
    }

    db_data_file_t* get_data_file_by_node_id(uint32 node_id);
    status_t add_system_space(char* space_name, uint32 space_id);
    status_t set_data_file(db_data_file_t* file, uint32 space_id, uint32 node_id,
        char* name, uint64 size, uint64 max_size, bool32 autoextend);

};

typedef struct st_db_charset_info {
    const char* name;
    CHARSET_INFO *charset_info;
} db_charset_info_t;




//------------------------------------------------------------------
extern status_t srv_create_ctrl_files();
extern status_t srv_create_data_files_at_createdatabase();

extern status_t db_ctrl_create_database(char *database_name, char *charset_name);
extern status_t db_ctrl_add_system_file(char* file_name, uint64 size, uint64 max_size, bool32 autoextend);
extern status_t db_ctrl_add_systrans_file(char* file_name, uint32 rseg_count);
extern status_t db_ctrl_add_redo_file(char* file_name, uint64 size);
extern status_t db_ctrl_add_dbwr_file(char* file_name, uint64 size);
extern status_t db_ctrl_add_undo_file(char* file_name, uint64 size, uint64 max_size);
extern status_t db_ctrl_add_temp_file(char* file_name, uint64 size, uint64 max_size);
extern status_t db_ctrl_add_user_space(char* space_name);
extern status_t db_ctrl_add_user_space_file(char* space_name, char* file_name, uint64 size, uint64 max_size, bool32 autoextend);

extern status_t read_ctrl_file(char *name, db_ctrl_t *ctrl);
extern CHARSET_INFO* db_ctrl_get_charset_info(char* charset_name);


#endif  /* _KNL_CTRL_FILE_H */
