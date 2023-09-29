#ifndef _KNL_DICT_H
#define _KNL_DICT_H

#include "cm_type.h"
#include "cm_error.h"
#include "cm_mutex.h"
#include "knl_hash_table.h"

/** Max number of rollback segments */
#define TRX_SYS_N_RSEGS 128

/** Minimum and Maximum number of undo tablespaces */
#define FSP_MIN_UNDO_TABLESPACES 2
#define FSP_MAX_UNDO_TABLESPACES (TRX_SYS_N_RSEGS - 1)

/** Data structure for a database table.  Most fields will be
initialized to 0, NULL or FALSE in dict_mem_table_create(). */
struct dict_table_t {

};


/* Dictionary system struct */
struct dict_sys_t {
  mutex_t mutex;          /*!< mutex protecting the data dictionary; */
  uint64  row_id;             /*!< the next row id to assign;
                               NOTE that at a checkpoint this
                               must be written to the dict system
                               header and flushed to a file; in
                               recovery this must be derived from
                               the log records */
  HASH_TABLE *table_hash;    /*!< hash table of the tables, based on name */
  HASH_TABLE *table_id_hash; /*!< hash table of the tables, based on id */
  uint32 size;                   /*!< varying space in bytes occupied by the data dictionary table and index objects */
  /** Handler to sys_* tables, they're only for upgrade */
  dict_table_t *sys_tables;  /*!< SYS_TABLES table */
  dict_table_t *sys_columns; /*!< SYS_COLUMNS table */
  dict_table_t *sys_indexes; /*!< SYS_INDEXES table */
  dict_table_t *sys_fields;  /*!< SYS_FIELDS table */
  dict_table_t *sys_virtual; /*!< SYS_VIRTUAL table */

  /** Permanent handle to mysql.innodb_table_stats */
  dict_table_t *table_stats;
  /** Permanent handle to mysql.innodb_index_stats */
  dict_table_t *index_stats;
  /** Permanent handle to mysql.innodb_ddl_log */
  dict_table_t *ddl_log;
  /** Permanent handle to mysql.innodb_dynamic_metadata */
  dict_table_t *dynamic_metadata;

  UT_LIST_BASE_NODE_T(dict_table_t)
  table_LRU; /*!< List of tables that can be evicted
             from the cache */
  UT_LIST_BASE_NODE_T(dict_table_t)
  table_non_LRU; /*!< List of tables that can't be
                 evicted from the cache */

  /** Iterate each table.
  @tparam Functor visitor
  @param[in,out]  functor to be invoked on each table */
  template <typename Functor>
  void for_each_table(Functor &functor) {
    mutex_enter(&mutex);

    HASH_TABLE *hash = table_id_hash;

    for (uint32 i = 0; i < hash->n_cells; i++) {
      for (dict_table_t *table =
               static_cast<dict_table_t *>(HASH_GET_FIRST(hash, i));
           table;
           table = static_cast<dict_table_t *>(HASH_GET_NEXT(id_hash, table))) {
        functor(table);
      }
    }

    mutex_exit(&mutex);
  }

  /** Check if a tablespace id is a reserved one
  @param[in]	space	tablespace id to check
  @return true if a reserved tablespace id, otherwise false */
  //static bool is_reserved(space_id_t space) {
  //  return (space >= dict_sys_t::s_reserved_space_id || fsp_is_session_temporary(space));
  //}

  /** Set of ids of DD tables */
  //static std::set<dd::Object_id> s_dd_table_ids;

  /** Check if a table is hardcoded. it only includes the dd tables
  @param[in]	id	table ID
  @retval true	if the table is a persistent hard-coded table
                  (dict_table_t::is_temporary() will not hold)
  @retval false	if the table is not hard-coded
                  (it can be persistent or temporary) */
  //static bool is_dd_table_id(uint64 id) {
  //  return (s_dd_table_ids.find(id) != s_dd_table_ids.end());
  //}

  /** The first ID of the redo log pseudo-tablespace */
#define REDO_SPACE_FIRST_ID       0xFFFFFFF0UL
  //static constexpr space_id_t s_log_space_first_id = 0xFFFFFFF0UL;

  /** Use maximum UINT value to indicate invalid space ID. */
#define INVALID_SPACE_ID          0xFFFFFFFF
  //static constexpr space_id_t s_invalid_space_id = 0xFFFFFFFF;

  /** The data dictionary tablespace ID. */
#define DICT_SPACE_ID             0xFFFFFFFE
  //static constexpr space_id_t s_space_id = 0xFFFFFFFE;

  /** The innodb_temporary tablespace ID. */
#define TEMP_SPACE_ID             0xFFFFFFFD
  //static constexpr space_id_t s_temp_space_id = 0xFFFFFFFD;

  /** The number of space IDs dedicated to each undo tablespace */
#define UNDO_SPACE_ID_RANGE       512
  //static constexpr space_id_t undo_space_id_range = 512;

  /** The lowest undo tablespace ID. */
#define UNDO_SPACE_MIN_ID         (REDO_SPACE_FIRST_ID - (FSP_MAX_UNDO_TABLESPACES * UNDO_SPACE_ID_RANGE))
  //static constexpr space_id_t s_min_undo_space_id = s_log_space_first_id - (FSP_MAX_UNDO_TABLESPACES * undo_space_id_range);

  /** The highest undo  tablespace ID. */
#define UNDO_SPACE_MAX_ID         (REDO_SPACE_FIRST_ID - 1)
  //static constexpr space_id_t s_max_undo_space_id = s_log_space_first_id - 1;

  /** The first reserved tablespace ID */
#define RESERVED_SPACE_ID         UNDO_SPACE_MIN_ID
  //static constexpr space_id_t s_reserved_space_id = s_min_undo_space_id;

  /** Leave 1K space_ids and start space_ids for temporary
  general tablespaces (total 400K space_ids)*/
#define TEMP_SPACE_MAX_ID         (RESERVED_SPACE_ID - 1000)
  //static constexpr space_id_t s_max_temp_space_id = s_reserved_space_id - 1000;

  /** Lowest temporary general space id */
#define TEMP_SPACE_MIN_ID         (RESERVED_SPACE_ID - 1000 - 400000)
  //static constexpr space_id_t s_min_temp_space_id = s_reserved_space_id - 1000 - 400000;

  /** The dd::Tablespace::id of the dictionary tablespace. */
  //static constexpr dd::Object_id s_dd_space_id = 1;

  /** The dd::Tablespace::id of innodb_system. */
  //static constexpr dd::Object_id s_dd_sys_space_id = 2;

  /** The dd::Tablespace::id of innodb_temporary. */
  //static constexpr dd::Object_id s_dd_temp_space_id = 3;

  /** The name of the data dictionary tablespace. */
  static const char *s_dd_space_name;

  /** The file name of the data dictionary tablespace. */
  static const char *s_dd_space_file_name;

  /** The name of the hard-coded system tablespace. */
  static const char *s_sys_space_name;

  /** The name of the predefined temporary tablespace. */
  static const char *s_temp_space_name;

  /** The file name of the predefined temporary tablespace. */
  static const char *s_temp_space_file_name;

  /** The hard-coded tablespace name innodb_file_per_table. */
  static const char *s_file_per_table_name;

  /** These two undo tablespaces cannot be dropped. */
  static const char *s_default_undo_space_name_1;
  static const char *s_default_undo_space_name_2;

  /** The table ID of mysql.innodb_dynamic_metadata */
  //static constexpr table_id_t s_dynamic_meta_table_id = 2;

  /** The clustered index ID of mysql.innodb_dynamic_metadata */
  //static constexpr space_index_t s_dynamic_meta_index_id = 2;
};




#endif  /* _KNL_DICT_H */
