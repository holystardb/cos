#ifndef _CM_FILE_H
#define _CM_FILE_H

#include "cm_type.h"
#include "cm_list.h"
#include "cm_mutex.h"

#ifndef __WIN__
#include <libaio.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

// Options for file_create
#define OS_FILE_OPEN                        1
#define OS_FILE_CREATE                      2
#define OS_FILE_OVERWRITE                   3
#define OS_FILE_TRUNCATE_EXISTING           4
#define OS_FILE_OPEN_ALWAYS                 5

// Options for file_create
#define OS_FILE_AIO                         1
#define OS_FILE_SYNC                        2


#define OS_FILE_IO_INPROCESS                (-1)
#define OS_FILE_IO_COMPLETION               0

// Error codes from os_file_get_last_error
#define OS_FILE_NOT_FOUND                   1
#define OS_FILE_DISK_FULL                   2
#define OS_FILE_ALREADY_EXISTS              3
#define OS_FILE_AIO_RESOURCES_RESERVED      4  // wait for OS aio resources to become available again
#define OS_FILE_ERROR_NOT_SPECIFIED         5
#define OS_FILE_ACCESS_DENIED               6
#define OS_FILE_PATH_NOT_FOUND              7
#define OS_FILE_IO_TIMEOUT                  8
#define OS_FILE_IO_ABANDONED                9
#define OS_FILE_IO_WAIT_FAILED              10


/* io type */
#define OS_FILE_READ                        1
#define OS_FILE_WRITE                       2

/* File types for directory entry data type */

enum os_file_type_t {
    OS_FILE_TYPE_UNKNOWN = 0,
    OS_FILE_TYPE_FILE, /* regular file */
    OS_FILE_TYPE_DIR,  /* directory */
    OS_FILE_TYPE_LINK  /* symbolic link */
};


extern void os_file_init();
extern bool32 os_open_file(    char *name, uint32 create_mode, uint32 purpose, os_file_t *file);
extern bool32 os_close_file(os_file_t file);
extern bool32 os_del_file(char *name);
extern bool32 os_pread_file(os_file_t file, uint64 offset, void *buf, uint32 size, uint32 *read_size);
extern bool32 os_pwrite_file(os_file_t file, uint64 offset, void *buf, uint32 size);
extern bool32 os_fsync_file(os_file_t file);
extern bool32 os_fdatasync_file(os_file_t file);
extern bool32 os_chmod_file(os_file_t file, uint32 perm);
extern bool32 os_truncate_file(os_file_t file, uint64 offset);
extern int32 os_file_get_last_error();
extern void os_file_get_last_error_desc(char *desc, uint32 size);
extern void os_file_get_error_desc_by_err(int32 err, char *desc, uint32 size);
extern bool32 os_file_handle_error(const char *name, const char* operation, bool32 should_exit);
extern bool32 os_file_get_size(os_file_t file, uint64 *size);
extern bool32 os_file_extend(char *file_name, os_file_t file, uint64 extend_size);
extern bool32 os_file_status(const char* path, bool32 *exists, os_file_type_t *type);
extern bool32 os_file_rename(const char* oldpath, const char* newpath);
extern bool32 os_file_set_eof(os_file_t file);
extern bool32 os_file_create_directory(const char *pathname, bool32 fail_if_exists);
extern bool32 get_app_path(char* str);
extern int32 get_file_size(char  *file_name, long long *file_byte_size);



//-----------------------------------------------------------------------------------



// windows MAXIMUM_WAIT_OBJECTS does not allow more than 64
#define OS_AIO_N_PENDING_IOS_PER_THREAD     64

#define AIO_SLOT_IS_IO_INPROCESS(slot)      (slot->ret == OS_FILE_IO_INPROCESS)
#define AIO_SLOT_IS_IO_COMPLETION(slot)     (slot->ret == OS_FILE_IO_COMPLETION)

typedef struct st_os_aio_array os_aio_array_t;
typedef struct st_os_aio_context os_aio_context_t;
typedef struct st_os_aio_slot os_aio_slot_t;

typedef status_t (*aio_slot_func)(int32 code, os_aio_slot_t* slot);


/** The asynchronous i/o array slot structure */
struct st_os_aio_slot {
    UT_LIST_NODE_T(os_aio_slot_t) list_node;
    os_aio_context_t* context;
    uint32          len;        /* length of the block to read or write */
    os_file_t       file;       /* file where to read or write */
    bool32          is_used;    /* TRUE if this slot is used */
    byte*           buf;        /* buffer used in i/o */
    time_t          used_time;  /*!< time when used */
    uint32          type;       /* OS_FILE_READ or OS_FILE_WRITE */
    uint64          offset;     /* file offset in bytes */
    const char*     name;       /* file name or path */
    void*           message1;
    void*           message2;
    aio_slot_func   callback_func;

#ifdef __WIN__
    HANDLE          handle;     /* handle object we need in the OVERLAPPED struct */
    OVERLAPPED      control;    /* Windows control block for the aio request */
#else
    struct iocb     control;    /* Linux control block for aio */
    uint32          n_bytes;    /* bytes written/read. */
#endif /* __WIN__ */
    int32           ret;        /* AIO return code */
};

struct st_os_aio_context {
    os_aio_array_t    *array;
#ifdef __WIN__
    HANDLE*            handles;
#else
    io_context_t       io_context;
    struct io_event   *io_events; /* collect completed IOs */
#endif /* __WIN__ */

    bool32             is_used;
    uint32             slot_count;
    os_aio_slot_t     *slots;
    os_event_t         slot_event;

    mutex_t            mutex;  /* the mutex protecting the aio context */
    UT_LIST_BASE_NODE_T(os_aio_slot_t) free_slots;

    UT_LIST_NODE_T(os_aio_context_t) list_node; // for array
};

/** The asynchronous i/o array structure */
typedef struct st_os_aio_array {
    mutex_t            mutex;  /* the mutex protecting the aio array */
    os_event_t         context_event;
    uint32             context_count;
    os_aio_context_t  *contexts;

    UT_LIST_BASE_NODE_T(os_aio_context_t) free_contexts;
} os_aio_array_t;


extern os_aio_array_t* os_aio_array_create(uint32 io_pending_count_per_context, uint32 io_context_count);
extern void os_aio_array_free(os_aio_array_t* array);

extern os_aio_context_t* os_aio_array_alloc_context(os_aio_array_t* array);
extern void os_aio_array_free_context(os_aio_context_t* context);
extern os_aio_context_t* os_aio_array_get_nth_context(os_aio_array_t* array, uint32 index);

extern void os_aio_context_free_slot(os_aio_slot_t* slot);

extern os_aio_slot_t* os_file_aio_submit(
    os_aio_context_t* context,
    uint32            type,      /* in: OS_FILE_READ or OS_FILE_WRITE */
    const char*       name,      /* in: name of the file or path as a null-terminated string */
    os_file_t         file,      /* in: handle to a file */
    void*             buf,       /* in: buffer where to read or from which to write */
    uint32            count,     /* in: number of bytes to read or write */
    uint64            offset,    /* in: file offset where to read or write */
    aio_slot_func     slot_func = NULL,
    void*             message1 = NULL,  /* in: message to be passed along with the aio operation */
    void*             message2 = NULL); /* in: message to be passed along with the aio operation */

extern int32 os_file_aio_slot_wait(os_aio_slot_t* slot, uint32 timeout_us);

//Waits for an aio operation to complete.
extern int32 os_file_aio_context_wait(os_aio_context_t* context,
    os_aio_slot_t** slot, uint64 timeout_us = OS_WAIT_INFINITE_TIME);

#ifndef __WIN__
extern void* os_aio_open_dl();
extern void os_aio_close_dl(void *lib_handle);
#endif


//-----------------------------------------------------------------------------------



#ifdef __cplusplus
}
#endif

#endif  /* _CM_FILE_H */

