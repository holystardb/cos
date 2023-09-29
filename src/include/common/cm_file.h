#ifndef _CM_FILE_H
#define _CM_FILE_H

#include "cm_type.h"


#ifdef __cplusplus
extern "C" {
#endif

/* Options for file_create */
#define OS_FILE_OPEN                        1
#define OS_FILE_CREATE                      2
#define OS_FILE_OVERWRITE                   3

/* Options for file_create */
#define OS_FILE_AIO                         1
#define OS_FILE_SYNC                        2

/* Error codes from os_file_get_last_error */
#define OS_FILE_NOT_FOUND                   1
#define OS_FILE_DISK_FULL                   2
#define OS_FILE_ALREADY_EXISTS              3
#define OS_FILE_AIO_RESOURCES_RESERVED      4  /* wait for OS aio resources to become available again */
#define OS_FILE_ERROR_NOT_SPECIFIED         5
#define OS_FILE_ACCESS_DENIED               6
#define OS_FILE_PATH_NOT_FOUND              7

#define OS_FILE_READ                        10
#define OS_FILE_WRITE                       11

#ifdef __WIN__
#define os_file_t                           HANDLE
#define OS_FILE_INVALID_HANDLE              INVALID_HANDLE_VALUE
#else
typedef int                                 os_file_t;
#define OS_FILE_INVALID_HANDLE              -1
#endif


bool32 os_open_file(    char *name, uint32 create_mode, uint32 purpose, os_file_t *file);
bool32 os_close_file(os_file_t file);
bool32 os_del_file(char *name);
bool32 os_pread_file(os_file_t file, uint64 offset, void *buf, uint32 size, uint32 *read_size);
bool32 os_pwrite_file(os_file_t file, uint64 offset, void *buf, uint32 size);
bool32 os_fsync_file(os_file_t file);
bool32 os_fdatasync_file(os_file_t file);
bool32 os_chmod_file(os_file_t file, uint32 perm);
bool32 os_truncate_file(os_file_t file, uint64 offset);
uint32 os_file_get_last_error();
bool32 os_file_handle_error(os_file_t file, char *name);
bool32 os_file_get_size(os_file_t file, uint64 *size);
bool32 os_file_extend(char *file_name, os_file_t file, uint64 extend_size);


bool32 get_app_path(char* str);
int32 get_file_size(char  *file_name, long long *file_byte_size);


#ifdef __cplusplus
}
#endif

#endif  /* _CM_FILE_H */

