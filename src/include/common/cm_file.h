#ifndef _CM_FILE_H
#define _CM_FILE_H

#include "cm_type.h"
#include "cm_list.h"
#include "cm_mutex.h"

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
#define OS_FILE_IO_COMPLETION               0
#define OS_FILE_NOT_FOUND                   1
#define OS_FILE_DISK_FULL                   2
#define OS_FILE_ALREADY_EXISTS              3
#define OS_FILE_AIO_RESOURCES_RESERVED      4  /* wait for OS aio resources to become available again */
#define OS_FILE_ERROR_NOT_SPECIFIED         5
#define OS_FILE_ACCESS_DENIED               6
#define OS_FILE_PATH_NOT_FOUND              7
#define OS_FILE_IO_TIMEOUT                  8
#define OS_FILE_IO_ABANDONED                9



/* io type */
#define OS_FILE_READ                        1
#define OS_FILE_WRITE                       2

#ifdef __WIN__
#define os_file_t                           HANDLE
#define OS_FILE_INVALID_HANDLE              INVALID_HANDLE_VALUE
#else
typedef int                                 os_file_t;
#define OS_FILE_INVALID_HANDLE              -1
#endif

void os_file_init();
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



//-----------------------------------------------------------------------------------
#ifndef __WIN__
#include "libaio.h"
#endif

typedef struct st_os_aio_array os_aio_array_t;

/** The asynchronous i/o array slot structure */
typedef struct st_os_aio_slot {
    UT_LIST_NODE_T(struct st_os_aio_slot) list_node;
    bool32      is_used;    /* TRUE if this slot is used */
    time_t      used_time;  /*!< time when used */
    uint32      len;        /* length of the block to read or write */
    byte*       buf;        /* buffer used in i/o */
    uint32      type;       /* OS_FILE_READ or OS_FILE_WRITE */
    uint64      offset;     /* file offset in bytes */
    os_file_t   file;       /* file where to read or write */
    const char* name;       /* file name or path */
    void*       message1;
    void*       message2;

#ifdef __WIN__
    HANDLE      handle;     /* handle object we need in the OVERLAPPED struct */
    OVERLAPPED  control;    /* Windows control block for the aio request */
#else
    struct iocb control;    /* Linux control block for aio */
    int         n_bytes;    /* bytes written/read. */
#endif /* __WIN__ */
    int         ret;        /* AIO return code */
}os_aio_slot_t;

typedef struct st_os_aio_context {
    os_aio_array_t    *aio_array;
#ifdef __WIN__
#else
    io_context_t      *io_ctx;
#endif /* __WIN__ */

    mutex_t     mutex;  /* the mutex protecting the aio context */
    UT_LIST_BASE_NODE_T(os_aio_slot_t) slots;

    UT_LIST_NODE_T(struct st_os_aio_context) list_node; // for array
}os_aio_context_t;

/** The asynchronous i/o array structure */
typedef struct st_os_aio_array {
    mutex_t           mutex;  /* the mutex protecting the aio array */
    os_event_t        aio_slot_event;
    os_event_t        aio_context_event;
    uint32            slot_count;
    uint32            io_context_count;
    os_aio_slot_t     *aio_slots;  /* Pointer to the slots in the array */
    os_aio_context_t  *aio_contexts;
    UT_LIST_BASE_NODE_T(os_aio_slot_t) free_slots;
    UT_LIST_BASE_NODE_T(os_aio_context_t) free_contexts;
    UT_LIST_BASE_NODE_T(os_aio_context_t) used_contexts;

#ifdef __WIN__
    //HANDLE*           handles; /* Pointer to an array of OS native event handles where we copied the handles from slots, 
    //                        in the same order. This can be used in WaitForMultipleObjects; used only in Windows */
#else
    io_context_t     *aio_ctxs; /* completion queue for IO */
    //struct io_event  *aio_events;
#endif /* __WIN__ */
} os_aio_array_t;


os_aio_array_t* os_aio_array_create(uint32 max_io_operation_count, uint32 io_context_count);
void os_aio_array_free(os_aio_array_t* aio_array);

os_aio_context_t* os_aio_array_alloc_context(os_aio_array_t* aio_array);
void os_aio_array_free_context(os_aio_context_t* aio_context);

bool32 os_file_aio_submit(
    os_aio_context_t* aio_context,
    uint32      type,      /* in: OS_FILE_READ or OS_FILE_WRITE */
    const char* name,      /* in: name of the file or path as a null-terminated string */
    os_file_t   file,      /* in: handle to a file */
    void*       buf,       /* in: buffer where to read or from which to write */
    uint32      count,     /* in: number of bytes to read or write */
    uint64      offset,    /* in: file offset where to read or write */
    void*       message1,  /* in: message to be passed along with the aio operation */
    void*       message2); /* in: message to be passed along with the aio operation */

//Waits for an aio operation to complete.
int os_file_aio_wait(os_aio_context_t* aio_context,       uint32 microseconds);

void* os_aio_open_dl();
void os_aio_close_dl(void *lib_handle);

#ifdef __cplusplus
}
#endif

#endif  /* _CM_FILE_H */

