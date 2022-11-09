#ifndef _CM_ERROR_H
#define _CM_ERROR_H

#include "cm_type.h"

enum dberr_t {
    DB_ERROR_UNSET = 0,
    DB_SUCCESS_LOCKED_REC = 9,    /*!< like DB_SUCCESS, but a new explicit record lock was created */
    DB_SUCCESS = 10,

    /* The following are error codes */
    DB_ERROR = 11,
    DB_INTERRUPTED,
    DB_OUT_OF_MEMORY,
    DB_OUT_OF_FILE_SPACE,
    DB_QUE_THR_SUSPENDED,
    DB_CORRUPTION,            /*!< data structure corruption noticed */

    DB_NOT_FOUND,            /*!< Generic error code for "Not found" type of errors */

    DB_IO_ERROR = 100, /*!< Generic IO error */
    DB_MUST_GET_MORE_FILE_SPACE,    /*!< the database has to be stopped and restarted with more file space */

    DB_LOCK_WAIT,
    DB_LOCK_WAIT_TIMEOUT,        /*!< lock wait lasted too long */
    DB_DEADLOCK,

    DB_UNSUPPORTED,

    DB_ROLLBACK,

    /* meta data error codes */
    DB_DICT_CHANGED = 200,        /*!< Some part of table dictionary has changed. Such as index dropped or foreign key dropped */
    DB_DUPLICATE_KEY,
    DB_FOREIGN_DUPLICATE_KEY,    /*!< foreign key constraints activated by the operation would lead to a duplicate key in some table */
    DB_MISSING_HISTORY,        /*!< required history data has been deleted due to lack of space in rollback segment */
    DB_CLUSTER_NOT_FOUND,
    DB_TABLE_NOT_FOUND,
    DB_TABLE_IS_BEING_USED,
    DB_CANNOT_ADD_CONSTRAINT,    /*!< adding a foreign key constraint to a table failed */
    DB_CANNOT_DROP_CONSTRAINT,    /*!< dropping a foreign key constraint from a table failed */

    DB_NO_REFERENCED_ROW,  /*!< referenced key value not found for a foreign key in an insert or update of a row */
    DB_ROW_IS_REFERENCED,  /*!< cannot delete or update a row because it contains a key value which is referenced */

    DB_TABLESPACE_EXISTS,        /*!< we cannot create a new single-table
                    tablespace because a file of the same name already exists */
    DB_TABLESPACE_DELETED,        /*!< tablespace was deleted or is being dropped right now */
    DB_TABLESPACE_NOT_FOUND,    /*<! Attempt to delete a tablespace instance that was not found in the tablespace hash table */



    DB_TOO_BIG_RECORD,        /*!< a record in an index would not fit on a compressed page,
                     or it would become bigger than 1/2 free space in an uncompressed page frame */
    DB_RECORD_NOT_FOUND = 1500,
    DB_END_OF_INDEX,


};

const char* ut_strerr(dberr_t num);
char *my_strerror(char *buf, size_t len, int nr);

#define EE_ERROR_FIRST          1 /*Copy first error nr.*/

#define EE_ERROR_LAST           33 /* Copy last error nr */
/* Add error numbers before EE_ERROR_LAST and change it accordingly. */

extern const char *glob_error_messages[];  /* my_error_messages is here */


extern const char** get_global_errmsgs();



#endif   // _CM_ERROR_H
