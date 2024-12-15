#ifndef _KNL_DEFS_H
#define _KNL_DEFS_H

#include "cm_type.h"


typedef uint32              space_id_t;
typedef uint32              page_no_t;
typedef uint64              ib_uint64_t;
typedef uint64              ib_id_t;
typedef byte                page_t;
typedef uint16              page_type_t;
typedef byte                buf_frame_t;

typedef uint64              lsn_t;
typedef uint64              scn_t;

#define SCN_MAX             UINT_MAX64
#define INVALID_SCN         UINT_MAX64

typedef uint64              table_id_t;
typedef uint64              index_id_t;
typedef uint64              object_id_t;

typedef uint32              command_id_t;

#define FIRST_COMMAND_ID    0
#define INVALID_COMMAND_ID  (~(command_id_t)0)

#define DB_CTRL_FILE_MAX_COUNT          3
#define DB_CTRL_FILE_VERSION            1
#define DB_CTRL_FILE_VERSION_NUM        1

#define DB_REDO_FILE_MAX_COUNT          16
#define DB_UNDO_FILE_MAX_COUNT          16
#define DB_TEMP_FILE_MAX_COUNT          64

#define DB_SPACE_DATA_FILE_MAX_COUNT    65536

#define DB_SYSTEM_SPACE_ID              0
#define DB_SYSTRANS_SPACE_ID            1
#define DB_SYSAUX_SPACE_ID              2
#define DB_DBWR_SPACE_ID                3
#define DB_REDO_SPACE_ID                4
#define DB_TEMP_SPACE_ID                5
#define DB_DICT_SPACE_ID                6
#define DB_UNDO_START_SPACE_ID          8
#define DB_UNDO_END_SPACE_ID            23

#define DB_SYSTEM_SPACE_MAX_COUNT       64
#define DB_UNDO_SPACE_MAX_COUNT         16

#define DB_USER_SPACE_FIRST_ID          DB_SYSTEM_SPACE_MAX_COUNT
#define DB_USER_SPACE_MAX_COUNT         256

#define DB_SPACE_INVALID_ID             0xFFFFFFFF

#define DB_SYSTEM_FILNODE_ID            0
#define DB_SYSTRANS_FILNODE_ID          1
#define DB_SYSAUX_FILNODE_ID            2
#define DB_DBWR_FILNODE_ID              3
#define DB_UNDO_START_FILNODE_ID        4
#define DB_UNDO_END_FILNODE_ID          19


#define DB_SYSTEM_DATA_FILE_MAX_COUNT   128
#define DB_USER_DATA_FILE_FIRST_NODE_ID 128

#define DB_DATA_FILNODE_INVALID_ID      0xFFFFFFFF


#define DB_DATA_FILE_NAME_MAX_LEN       256
#define DB_OBJECT_NAME_MAX_LEN          64

#define USER_SPACE_MAX_COUNT            10240
#define FILE_NODE_COUNT_PER_USER_SPACE  1024

#endif  /* _KNL_DEFS_H */
