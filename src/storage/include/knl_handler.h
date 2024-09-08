#ifndef _KNL_HANDLER_H
#define _KNL_HANDLER_H

#include "cm_type.h"
#include "cm_memory.h"
#include "cm_attribute.h"

enum ha_rkey_function {
  HA_READ_KEY_EXACT,			/* Find first record else error */
  HA_READ_KEY_OR_NEXT,			/* Record or next record */
  HA_READ_KEY_OR_PREV,			/* Record or previous */
  HA_READ_AFTER_KEY,			/* Find next rec. after key-record */
  HA_READ_BEFORE_KEY,			/* Find next rec. before key-record */
  HA_READ_PREFIX,			/* Key which as same prefix */
  HA_READ_PREFIX_LAST			/* Last key with the same prefix */			
};

typedef struct st_ha_create_information
{
  ulong table_options;
  //enum db_type db_type;
  //enum row_type row_type;
  ulong avg_row_length;
  ulonglong max_rows,min_rows;
  ulonglong auto_increment_value;
  char *comment,*password;
  char *create_statement;
  uint options;					/* OR of HA_CREATE_ options */
  uint raid_type,raid_chunks;
  ulong raid_chunksize;
  bool32 if_not_exists;
  ulong used_fields;
  //SQL_LIST merge_list;
} HA_CREATE_INFO;


class ha_innobase//: public handler
{
public:
   ha_innobase()
   {
   }

   ~ha_innobase() {}


    int open(const char *name, int mode, uint test_if_locked);
    void initialize(void);
    int close(void);
    double scan_time();

    int write_row(byte * buf);
    int update_row(const byte * old_data, byte * new_data);
    int delete_row(const byte * buf);

    int index_init(uint index);
    int index_end();
    int index_read(byte * buf, const byte * key, uint key_len, enum ha_rkey_function find_flag);
    int index_read_idx(byte * buf, uint index, const byte * key, uint key_len, enum ha_rkey_function find_flag);
    int index_next(byte * buf);
    int index_next_same(byte * buf, const byte *key, uint keylen);
    int index_prev(byte * buf);
    int index_first(byte * buf);
    int index_last(byte * buf);

    //int create(const char *name, register TABLE *form, HA_CREATE_INFO *create_info);
    int delete_table(const char *name);
    int rename_table(const char* from, const char* to);
    //int check(THD* thd, HA_CHECK_OPT* check_opt);
    char* update_table_comment(const char* comment);

};


extern bool32 knl_server_init(void);
extern bool32 knl_server_start(memory_area_t* marea);

extern status_t server_open_or_create_database(char* base_dir, attribute_t* attr);

#endif  /* _KNL_HANDLER_H */
