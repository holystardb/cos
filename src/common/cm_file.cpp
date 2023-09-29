#include "cm_file.h"
#include "cm_mutex.h"
#include "cm_dbug.h"
#include "cm_memory.h"

#ifdef __WIN__
#include "io.h"
#endif // __WIN__


/* We use these mutexes to protect lseek + file i/o operation, if the OS does not provide an atomic pread or pwrite, or similar */
#define OS_FILE_N_SEEK_MUTEXES          16
os_mutex_t os_file_seek_mutexes[OS_FILE_N_SEEK_MUTEXES];

void os_file_init()
{
#ifdef __WIN__
    for (uint32 i = 0; i < OS_FILE_N_SEEK_MUTEXES; i++) {
        os_mutex_create(&os_file_seek_mutexes[i]);
    }
#endif
}

bool32 os_open_file(char *name, uint32 create_mode, uint32 purpose, os_file_t *file)
{
    bool32 success = TRUE;

try_again:

#ifdef __WIN__
    DWORD create_flag;
    DWORD attributes = 0;

    if (create_mode == OS_FILE_OPEN) {
        create_flag = OPEN_EXISTING;
    } else if (create_mode == OS_FILE_CREATE) {
        create_flag = CREATE_NEW;
    } else if (create_mode == OS_FILE_OVERWRITE) {
        create_flag = CREATE_ALWAYS;
    } else {
        create_flag = 0;
        ut_error;
    }

    if (purpose == OS_FILE_AIO) {
        /* use asynchronous (overlapped) io and no buffering of writes in the OS */
        attributes |= FILE_FLAG_OVERLAPPED;
    }

    *file = CreateFile(name,
        GENERIC_READ | GENERIC_WRITE, /* read and write access */
        FILE_SHARE_READ,/* file can be read by other processes */
        NULL,    /* default security attributes */
        create_flag,
        attributes,
        NULL);    /* no template file */

#else

    int create_flag;
    bool32 retry;

    if (create_mode == OS_FILE_OPEN) {
        create_flag = O_RDWR;
    } else if (create_mode == OS_FILE_CREATE) {
        create_flag = O_RDWR | O_CREAT | O_EXCL;
    } else if (create_mode == OS_FILE_OVERWRITE) {
        create_flag = O_RDWR | O_CREAT | O_TRUNC;
    } else {
        create_flag = 0;
        ut_error;
    }

    if (purpose == OS_FILE_NORMAL) {
        create_flag = create_flag | O_SYNC;
    }

    if (create_mode == OS_FILE_CREATE) {
        *file = open(name, create_flag, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
    } else {
        *file = open(name, create_flag);
    }
#endif

    if (*file == OS_FILE_INVALID_HANDLE) {
        if (os_file_handle_error(*file, name)) {
            goto try_again;
        }
        success = FALSE;
    }

    return success;
}

bool32 os_close_file(os_file_t file)
{
    if (file == OS_FILE_INVALID_HANDLE)
        return FALSE;

#ifdef __WIN__
    bool32 ret = CloseHandle(file);
    if (ret) {
        return TRUE;
    }
#else
    int ret = close(file);
    if (ret != -1) {
        return TRUE;
    }
#endif

    os_file_handle_error(file, NULL);

    return FALSE;
}

bool32 os_del_file(char *name)
{
#ifdef __WIN__

    bool32 ret = DeleteFile(name);
    if (ret) {
        return TRUE;
    }

#else

    int ret = unlink(name);
    if (ret != -1) {
        return TRUE;
    }

#endif

    return FALSE;
}

bool32 os_pread_file(os_file_t file, uint64 offset, void *buf, uint32 size, uint32 *read_size)
{
    bool32 retry;

try_again:

#ifdef __WIN__

    DWORD offset_low; /* least significant 32 bits of file offset where to read */
    LONG offset_high; /* most significant 32 bits of offset */
    DWORD ret;
    DWORD len;

    /* Protect the seek / read operation with a mutex */
    uint32 i = ((uint64)file) % OS_FILE_N_SEEK_MUTEXES;

    offset_low = (offset & 0xFFFFFFFF);
    offset_high = (offset >> 32);

    os_mutex_enter(&os_file_seek_mutexes[i]);

    ret = SetFilePointer(file, offset_low, &offset_high, FILE_BEGIN);
    if (ret == 0xFFFFFFFF && GetLastError() != NO_ERROR) {
        os_mutex_exit(&os_file_seek_mutexes[i]);
        goto error_handling;
    } 
    ret = ReadFile(file, buf, size, &len, NULL);

    os_mutex_exit(&os_file_seek_mutexes[i]);

    *read_size = len;
    if (ret && *read_size == size) {
        return TRUE;
    }

#else

    ssize_t ret = pread64(file, (char *)buf, size, offset);
    if ((uint32)ret == size) {
        return TRUE;
    }

#endif

#ifdef __WIN__
error_handling:
#endif

    retry = os_file_handle_error(file, NULL); 
    if (retry) {
        goto try_again;
    }

    return FALSE;
}

bool32 os_pwrite_file(os_file_t file, uint64 offset, void *buf, uint32 size)
{
    bool32 retry;

try_again:

#ifdef __WIN__

    DWORD offset_low; /* least significant 32 bits of file offset where to read */
    LONG offset_high; /* most significant 32 bits of offset */
    DWORD ret;
    DWORD len;

    /* Protect the seek / read operation with a mutex */
    uint32 i = ((uint64)file) % OS_FILE_N_SEEK_MUTEXES;

    offset_low = (offset & 0xFFFFFFFF);
    offset_high = (offset >> 32);

    os_mutex_enter(&os_file_seek_mutexes[i]);

    ret = SetFilePointer(file, offset_low, &offset_high, FILE_BEGIN);
    if (ret == 0xFFFFFFFF && GetLastError() != NO_ERROR) {
        os_mutex_exit(&os_file_seek_mutexes[i]);
        goto error_handling;
    } 
    ret = WriteFile(file, buf, size, &len, NULL);

    os_mutex_exit(&os_file_seek_mutexes[i]);

    if (ret && len == size) {
        return TRUE;
    }

#else

    ssize_t ret = pwrite64(file, (char *)buf, size, offset);
    if ((uint32)ret == size) {
        return TRUE;
    }

#endif
    
#ifdef __WIN__
error_handling:
#endif

    retry = os_file_handle_error(file, NULL); 
    if (retry) {
        goto try_again;
    }

    return FALSE;
}

bool32 os_fsync_file(os_file_t file)
{
#ifdef __WIN__

    bool32 ret = FlushFileBuffers(file);
    if (ret) {
        return TRUE;
    }

#else

    int ret = fsync(file);
    if (ret == 0) {
        return TRUE;
    }

    /* Since Linux returns EINVAL if the 'file' is actually a raw device, we choose to ignore that error */
    if (errno == EINVAL) {
        return(TRUE);
    }

#endif

    os_file_handle_error(file, NULL);

    return FALSE;
}

bool32 os_fdatasync_file(os_file_t file)
{
#ifdef __WIN__

    bool32 ret = FlushFileBuffers(file);
    if (ret) {
        return TRUE;
    }

#else

    int ret = fdatasync(file);
    if (ret == 0) {
        return TRUE;
    }

    /* Since Linux returns EINVAL if the 'file' is actually a raw device, we choose to ignore that error */
    if (errno == EINVAL) {
        return(TRUE);
    }

#endif

    os_file_handle_error(file, NULL);

    return FALSE;
}

bool32 os_chmod_file(os_file_t file, uint32 perm)
{
#ifndef __WIN__
    int32 err_no = fchmod(file, perm);
    if (err_no != 0) {
        //errno
        return FALSE;
    }
#endif

    return TRUE;
}

bool32 os_truncate_file(os_file_t file, uint64 offset)
{
#ifdef __WIN__
    //if (_chsize_s(file, offset) != 0) {
    if (0) {
#else
    if (ftruncate(file, offset) != 0) {
#endif
        return FALSE;
    }
    return TRUE;
}

//If the number is not known to this program, the OS error number + 100 is returned.
uint32 os_file_get_last_error()
{
    uint32 err;

#ifdef __WIN__
    err = (uint32) GetLastError();
    if (err == ERROR_FILE_NOT_FOUND) {
        return OS_FILE_NOT_FOUND;
    } else if (err == ERROR_DISK_FULL) {
        return OS_FILE_DISK_FULL;
    } else if (err == ERROR_FILE_EXISTS) {
        return OS_FILE_ALREADY_EXISTS;
    } else if (err == ERROR_PATH_NOT_FOUND) {
        return OS_FILE_PATH_NOT_FOUND;
    } else if (err == ERROR_ACCESS_DENIED) {
        return OS_FILE_ACCESS_DENIED;
    } else {
        return 100 + err;
    }
#else
    err = (uint32) errno;
    if (err == ENOSPC ) {
        return(OS_FILE_DISK_FULL);
#ifdef POSIX_ASYNC_IO
    } else if (err == EAGAIN) {
        return(OS_FILE_AIO_RESOURCES_RESERVED);
#endif
    } else if (err == ENOENT) {
        return(OS_FILE_NOT_FOUND);
    } else if (err == EEXIST) {
        return(OS_FILE_ALREADY_EXISTS);
    } else if (err == EACCES) {
        return OS_FILE_ACCESS_DENIED;
    } else {
        return 100 + err;
    }
#endif
}

/* out: TRUE if we should retry the operation */
bool32 os_file_handle_error(os_file_t file, char *name)
{
    uint32 err;

    UT_NOT_USED(file);

    err = os_file_get_last_error();
    if (err == OS_FILE_DISK_FULL) {
        fprintf(stderr, "\n");
        if (name) {
            fprintf(stderr, "Encountered a problem with file %s.\n", name);
        }
        fprintf(stderr,
           "Cannot continue operation.\n"
           "Disk is full. Try to clean the disk to free space.\n"
           "Delete a possible created file and restart.\n");
        exit(1);
    } else if (err == OS_FILE_AIO_RESOURCES_RESERVED) {
        return TRUE;
    } else if (err == OS_FILE_ALREADY_EXISTS) {
        return FALSE;
    } else {
        if (name) {
            fprintf(stderr, "File name %s\n", name);
        }
        fprintf(stderr, "Cannot continue operation.\n");
        exit(1);
    }

    return FALSE;
}

bool32 os_file_extend(char *file_name, os_file_t file, uint64 extend_size)
{
    byte        buf[512];
    uint64      offset, len = 0;
    uint32      n_bytes = 512;

    /* Write buffer full of zeros */
    memset(buf, 0x00, n_bytes);

    if (!os_file_get_size(file, &offset)) {
        return FALSE;
    }

    while (len < extend_size) {
        if (!os_pwrite_file(file, offset + len, buf, n_bytes)) {
            goto error_handling;
        }
        len += n_bytes;
    }

    if (os_fsync_file(file)) {
        return(TRUE);
    }

error_handling:

    os_file_handle_error(file, file_name); 

    return FALSE;
}

bool32 os_file_get_size(os_file_t file, uint64 *size)
{
#ifdef __WIN__
    DWORD high;
    DWORD low;
    low = GetFileSize(file, &high);
    if ((low == 0xFFFFFFFF) && (GetLastError() != NO_ERROR)) {
        return FALSE;
    }
    *size = ((uint64)high << 32) + low;
#else
    off_t offs = lseek(file, 0, SEEK_END);
    *size = (uint64)offs;
#endif

    return TRUE;
}



bool32 get_app_path(char* str)
{
#ifdef _WIN32
    GetCurrentDirectory(255, str);
#else
    sprintf(str, "%s/", getenv("HOME"));
#endif	
    
    return TRUE;
}

int32 get_file_size(char *file_name, long long *file_byte_size)
{
    FILE *fp;
    errno_t err;

    err = fopen_s(&fp, file_name, "r");
    if (err != 0) {
        return -1;
    }
    if (0 != _fseeki64(fp, 0, SEEK_END)) {
        fclose(fp);
        return -1;
    }
    *file_byte_size = _ftelli64(fp);
    fclose(fp);

    return 0;
}



/***********************************************************************************************
*                                         aio                                                  *
***********************************************************************************************/


/** The asynchronous i/o array slot structure */
typedef struct st_os_aio_slot {
    bool32      is_read;    /* TRUE if a read operation */
    uint32      pos;        /* index of the slot in the aio array */
    bool32      reserved;   /* TRUE if this slot is reserved */
    time_t      reservation_time;/*!< time when reserved */
    uint32      len;        /* length of the block to read or write */
    byte*       buf;        /* buffer used in i/o */
    uint32      type;       /* OS_FILE_READ or OS_FILE_WRITE */
    uint64      offset;     /* file offset in bytes */
    os_file_t   file;       /* file where to read or write */
    const char* name;       /* file name or path */
    bool32      io_already_done;/* used only in simulated aio:
                                   TRUE if the physical i/o already made 
                                   and only the slot message needs to be passed to the caller of os_aio_simulated_handle */
    void*       message1;
    void*       message2;

#ifdef __WIN__
    HANDLE      handle;     /* handle object we need in the OVERLAPPED struct */
    OVERLAPPED  control;    /* Windows control block for the aio request */
#else
    struct iocb control;    /* Linux control block for aio */
    int         n_bytes;    /* bytes written/read. */
    int         ret;        /* AIO return code */
#endif /* __WIN__ */
}os_aio_slot_t;

/** The asynchronous i/o array structure */
typedef struct st_os_aio_array {
    os_mutex_t  mutex;  /* the mutex protecting the aio array */
    os_event_t  not_full; /* The event which is set to the signaled state when there is space in the aio outside the ibuf segment */
    os_event_t  is_empty; /* The event which is set to the signaled state when there are no pending i/os in this array */
    uint32      n_slots;  /* Total number of slots in the aio array. This must be divisible by n_threads. */
    uint32      n_segments;  /* Number of segments in the aio array of pending aio requests.
                                A thread can wait separately for any one of the segments. */
    uint32      cur_seg; /* We reserve IO requests in round robin fashion to different segments.
                            This points to the segment that is to be used to service next IO request. */
    uint32      n_reserved;  /* Number of reserved slots in the aio array outside the ibuf segment */
    os_aio_slot_t*  slots;  /* Pointer to the slots in the array */

#ifdef __WIN__
    HANDLE*		handles; /* Pointer to an array of OS native event handles where we copied the handles from slots, 
                            in the same order. This can be used in WaitForMultipleObjects; used only in Windows */
#else
    io_context_t*       aio_ctx; /* completion queue for IO. There is one such queue per segment.
                                    Each thread will work on one ctx exclusively. */
    struct io_event*    aio_events; /* The array to collect completed IOs.
                                       There is one such event for each possible pending IO.
                                       The size of the array is equal to n_slots. */
#endif /* __WIN__ */
}os_aio_array_t;

os_aio_slot_t* os_aio_array_get_nth_slot(os_aio_array_t* array, uint32 index)
{
    ut_a(index < array->n_slots);

    return(&array->slots[index]);
}

/*
    n: maximum number of pending aio operations allowed; n must be divisible by n_segments
    n_segments: number of segments in the aio array
*/
os_aio_array_t* os_aio_array_create(uint32 n, uint32 n_segments)
{
    os_aio_array_t*  array;
#ifdef __WIN__
    OVERLAPPED*      over;
#else
    struct io_event* io_event = NULL;
#endif /* __WIN__ */

    ut_a(n > 0);
    ut_a(n_segments > 0);

    array = (os_aio_array_t*)ut_malloc(sizeof(*array));
    memset(array, 0x0, sizeof(*array));
    os_mutex_create(&array->mutex);
    array->not_full = os_event_create(NULL);
    array->is_empty = os_event_create(NULL);
    os_event_set(array->is_empty);
    array->n_slots = n;
    array->n_segments = n_segments;
    array->slots = (os_aio_slot_t*)ut_malloc(n * sizeof(*array->slots));
    memset(array->slots, 0x0, sizeof(n * sizeof(*array->slots)));

#ifdef __WIN__
    array->handles = (HANDLE*)ut_malloc(n * sizeof(HANDLE));
#else
    array->aio_ctx = NULL;
    array->aio_events = NULL;

    /* Initialize the io_context array. One io_context per segment in the array. */
    array->aio_ctx = (io_context**)ut_malloc(n_segments * sizeof(*array->aio_ctx));
    for (uint32 i = 0; i < n_segments; ++i) {
        if (!os_aio_linux_create_io_ctx(n/n_segments, &array->aio_ctx[i])) {
            /* If something bad happened during aio setup we should call it a day and return right away.
               We don't care about any leaks because a failure to initialize the io subsystem means that the
               server (or atleast the innodb storage engine) is not going to startup. */
            return(NULL);
        }
    }

    /* Initialize the event array. One event per slot. */
    io_event = (struct io_event*)ut_malloc(n * sizeof(*io_event));
    memset(io_event, 0x0, sizeof(*io_event) * n);
    array->aio_events = io_event;
#endif /* __WIN__ */

    for (uint32 i = 0; i < n; i++) {
        os_aio_slot_t* slot;
        slot = os_aio_array_get_nth_slot(array, i);
        slot->pos = i;
        slot->reserved = FALSE;
#ifdef __WIN__
        slot->handle = CreateEvent(NULL,TRUE, FALSE, NULL);
        over = &slot->control;
        over->hEvent = slot->handle;
        array->handles[i] = over->hEvent;
#else
        memset(&slot->control, 0x0, sizeof(slot->control));
        slot->n_bytes = 0;
        slot->ret = 0;
#endif /* __WIN__ */
    }

    return(array);
}

void os_aio_array_free(os_aio_array_t*& array)
{
#ifdef __WIN__
    for (uint32 i = 0; i < array->n_slots; i++) {
        os_aio_slot_t* slot = os_aio_array_get_nth_slot(array, i);
        CloseHandle(slot->handle);
    }
    ut_free(array->handles);
#else
    ut_free(array->aio_events);
    ut_free(array->aio_ctx);
#endif /* __WIN__ */

    os_mutex_destroy(&array->mutex);
    os_event_destroy(array->not_full);
    os_event_destroy(array->is_empty);

    ut_free(array->slots);
    ut_free(array);

    array = 0;
}

os_aio_slot_t* os_aio_array_reserve_slot(
    uint32          type,   /* in: OS_FILE_READ or OS_FILE_WRITE */
    os_aio_array_t* array,  /* in: aio array */
    void*           message1,/* in: message to be passed along with the aio operation */
    void*           message2,/* in: message to be passed along with the aio operation */
    os_file_t       file,   /* in: file handle */
    const char*     name,   /* in: name of the file or path as a null-terminated string */
    void*           buf,    /* in: buffer where to read or from which to write */
    uint64          offset, /* in: file offset */
    uint32          len)    /* in: length of the block to read or write */
{
    os_aio_slot_t*  slot = NULL;
#ifdef __WIN__
    OVERLAPPED*     control;
#else
    struct iocb*    iocb;
    off_t           aio_offset;
#endif /* __WIN__ */


#ifdef __WIN__
    ut_a((len & 0xFFFFFFFFUL) == len);
#endif /* __WIN__ */


loop:

    os_mutex_enter(&array->mutex);

    if (array->n_reserved == array->n_slots) {
        os_mutex_exit(&array->mutex);
        os_event_wait(&array->not_full);
        goto loop;
    }

    for (uint32 i = 0; i < array->n_slots; i++) {
        slot = os_aio_array_get_nth_slot(array, i);
        if (slot->reserved == FALSE) {
            goto found;
        }
    }

    /* We MUST always be able to get hold of a reserved slot. */
    ut_error;

found:
    ut_a(slot->reserved == FALSE);
    array->n_reserved++;

    if (array->n_reserved == 1) {
        os_event_reset(array->is_empty);
    }

    if (array->n_reserved == array->n_slots) {
        os_event_reset(array->not_full);
    }

    slot->reserved = TRUE;
    slot->reservation_time = time(NULL);
    slot->message1 = message1;
    slot->message2 = message2;
    slot->file     = file;
    slot->name     = name;
    slot->len      = len;
    slot->type     = type;
    slot->buf      = (byte*)buf;
    slot->offset   = offset;
    slot->io_already_done = FALSE;

#ifdef __WIN__
    control = &slot->control;
    control->Offset = (DWORD) offset & 0xFFFFFFFF;
    control->OffsetHigh = (DWORD) (offset >> 32);
    ResetEvent(slot->handle);
#else
    /* Check if we are dealing with 64 bit arch. If not then make sure that offset fits in 32 bits. */
    aio_offset = (off_t) offset;

    ut_a(sizeof(aio_offset) >= sizeof(offset)  || ((os_offset_t) aio_offset) == offset);

    iocb = &slot->control;
    if (type == OS_FILE_READ) {
        io_prep_pread(iocb, file, buf, len, aio_offset);
    } else {
        ut_a(type == OS_FILE_WRITE);
        io_prep_pwrite(iocb, file, buf, len, aio_offset);
    }

    iocb->data = (void*) slot;
    slot->n_bytes = 0;
    slot->ret = 0;
#endif /* __WIN__ */

    os_mutex_exit(&array->mutex);

    return slot;
}

void os_aio_array_free_slot(os_aio_array_t* array, os_aio_slot_t* slot)
{
    os_mutex_enter(&array->mutex);

    ut_ad(slot->reserved);

    slot->reserved = FALSE;
    array->n_reserved--;

    if (array->n_reserved == array->n_slots - 1) {
        os_event_set(array->not_full);
    }

    if (array->n_reserved == 0) {
        os_event_set(array->is_empty);
    }

#ifdef __WIN__
    ResetEvent(slot->handle);
#else
    memset(&slot->control, 0x0, sizeof(slot->control));
    slot->n_bytes = 0;
    slot->ret = 0;
#endif
    os_mutex_exit(&array->mutex);
}

#ifdef __WIN__
bool32 os_aio_windows_handle(
    os_aio_array_t* array,
    uint32  pos,        /* this parameter is used only in sync aio: wait for the aio slot at this position */
    void**   message1,   /* in: message to be passed along with the aio operation */
    void**   message2,   /* in: message to be passed along with the aio operation */
    uint32* type)       /* out: OS_FILE_WRITE or ..._READ */
{
    os_aio_slot_t*  slot;
    uint32          i;
    bool32          ret_val;
    bool32          ret;
    DWORD           len;
    bool32          retry = FALSE;

    if (pos != UINT32_UNDEFINED) {
        WaitForSingleObject(os_aio_array_get_nth_slot(array, pos)->handle, INFINITE);
        i = pos;
    } else {
        i = WaitForMultipleObjects((DWORD)array->n_slots, array->handles, FALSE, INFINITE);
    }

    os_mutex_enter(&array->mutex);

    ut_a(i >= WAIT_OBJECT_0 && i <= WAIT_OBJECT_0 + array->n_slots);

    slot = os_aio_array_get_nth_slot(array, i);
    ut_a(slot->reserved);

    ret = GetOverlappedResult(slot->file, &(slot->control), &len, TRUE);
    *message1 = slot->message1;
    *message2 = slot->message2;
    *type = slot->type;
    if (ret && len == slot->len) {
        ret_val = TRUE;
    } else if (os_file_handle_error(slot->file, (char *)slot->name)) {
        retry = TRUE;
    } else {
        ret_val = FALSE;
    }

    os_mutex_exit(&array->mutex);

    if (retry) { /* retry failed read/write operation synchronously. No need to hold array->mutex. */
        ut_a((slot->len & 0xFFFFFFFFUL) == slot->len);
        switch (slot->type) {
        case OS_FILE_WRITE:
            ret = WriteFile(slot->file, slot->buf, (DWORD) slot->len, &len, &(slot->control));
            break;
        case OS_FILE_READ:
            ret = ReadFile(slot->file, slot->buf, (DWORD) slot->len, &len, &(slot->control));
            break;
        default:
            ut_error;
        }

        if (!ret && GetLastError() == ERROR_IO_PENDING) {
            /* aio was queued successfully!
               We want a synchronous i/o operation on a file where we also use async i/o:
               in Windows we must use the same wait mechanism as for async i/o */
            ret = GetOverlappedResult(slot->file, &(slot->control), &len, TRUE);
        }

        ret_val = ret && len == slot->len;
    }

    os_aio_array_free_slot(array, slot);

    return ret_val;
}

#else

void os_aio_linux_collect(os_aio_array_t* array, uint32 segment, uint32 seg_size)
{
    int                 i;
    int                 ret;
    uint32              start_pos;
    uint32              end_pos;
    struct timespec     timeout;
    struct io_event*    events;
    struct io_context*  io_ctx;

    /* sanity checks. */
    ut_ad(array != NULL);
    ut_ad(seg_size > 0);
    ut_ad(segment < array->n_segments);

    /* Which part of event array we are going to work on. */
    events = &array->aio_events[segment * seg_size];

    /* Which io_context we are going to use. */
    io_ctx = array->aio_ctx[segment];

    /* Starting point of the segment we will be working on. */
    start_pos = segment * seg_size;

    /* End point. */
    end_pos = start_pos + seg_size;

retry:

    /* Initialize the events. The timeout value is arbitrary.
    We probably need to experiment with it a little. */
    memset(events, 0, sizeof(*events) * seg_size);
    timeout.tv_sec = 0;
    timeout.tv_nsec = OS_AIO_REAP_TIMEOUT;

    ret = io_getevents(io_ctx, 1, seg_size, events, &timeout);
    if (ret > 0) {
        for (i = 0; i < ret; i++) {
            os_aio_slot_t*  slot;
            struct iocb*    control;

            control = (struct iocb*) events[i].obj;
            ut_a(control != NULL);

            slot = (os_aio_slot_t*) control->data;

            /* Some sanity checks. */
            ut_a(slot != NULL);
            ut_a(slot->reserved);

#if defined(UNIV_AIO_DEBUG)
            fprintf(stderr, "io_getevents[%c]: slot[%p] ctx[%p] seg[%lu]\n",
                (slot->type == OS_FILE_WRITE) ? 'w' : 'r', slot, io_ctx, segment);
#endif

            /* We are not scribbling previous segment. */
            ut_a(slot->pos >= start_pos);
            /* We have not overstepped to next segment. */
            ut_a(slot->pos < end_pos);

            /* Mark this request as completed. The error handling will be done in the calling function. */
            os_mutex_enter(array->mutex);
            slot->n_bytes = events[i].res;
            slot->ret = events[i].res2;
            slot->io_already_done = TRUE;
            os_mutex_exit(array->mutex);
        }
        return;
    }

    //if (unlikely(srv_shutdown_state == SRV_SHUTDOWN_EXIT_THREADS)) {
    //    return;
    //}

    /* This error handling is for any error in collecting the
    IO requests. The errors, if any, for any particular IO
    request are simply passed on to the calling routine. */

    switch (ret) {
    case -EAGAIN:
        /* Not enough resources! Try again. */
    case -EINTR:
        /* Interrupted! I have tested the behaviour in case of an
        interrupt. If we have some completed IOs available then
        the return code will be the number of IOs. We get EINTR only
        if there are no completed IOs and we have been interrupted. */
    case 0:
        /* No pending request! Go back and check again. */
        goto retry;
    }

    /* All other errors should cause a trap for now. */
    ut_print_timestamp(stderr);
    fprintf(stderr, "os_aio: unexpected ret_code[%d] from io_getevents()!\n", ret);
    ut_error;
}

bool32 os_aio_linux_handle(
    uint32  global_seg, /* in: segment number in the aio array to wait for;
                segment 0 is the ibuf i/o thread, segment 1 is log i/o thread,
                then follow the non-ibuf read threads, and the last are the non-ibuf write threads. */
    void**  message1,    /* out: the messages passed with the */
    void**  message2,	/* aio request; note that in case the aio operation failed,
                           these output parameters are valid and can be used to restart the operation. */
    uint32* type)  /* out: OS_FILE_WRITE or ..._READ */
{
    uint32          segment;
    os_aio_array_t* array;
    os_aio_slot_t*  slot;
    uint32          n;
    uint32          i;
    bool32          ret = FALSE;

    /* Should never be doing Sync IO here. */
    ut_a(global_seg != UINT32_UNDEFINED);

    /* Find the array and the local segment. */
    segment = os_aio_get_array_and_local_segment(&array, global_seg);
    n = array->n_slots / array->n_segments;

    /* Loop until we have found a completed request. */
    for (;;) {
        bool32 any_reserved = FALSE;
        os_mutex_enter(array->mutex);
        for (i = 0; i < n; ++i) {
            slot = os_aio_array_get_nth_slot(array, i + segment * n);
            if (!slot->reserved) {
                continue;
            } else if (slot->io_already_done) {
                /* Something for us to work on. */
                goto found;
            } else {
                any_reserved = TRUE;
            }
        }
        os_mutex_exit(array->mutex);

        /* There is no completed request.
        If there is no pending request at all, and the system is being shut down, exit. */
        if (unlikely(!any_reserved
                     && srv_shutdown_state == SRV_SHUTDOWN_EXIT_THREADS)) {
            *message1 = NULL;
            *message2 = NULL;
            return(TRUE);
        }

        /* Wait for some request. Note that we return from wait iff we have found a request. */
        //srv_set_io_thread_op_info(global_seg, "waiting for completed aio requests");
        os_aio_linux_collect(array, segment, n);
    }

found:

    /* Note that it may be that there are more then one completed IO requests.
    We process them one at a time. We may have a case
    here to improve the performance slightly by dealing with all requests in one sweep. */
    //srv_set_io_thread_op_info(global_seg, "processing completed aio requests");

    /* Ensure that we are scribbling only our segment. */
    ut_a(i < n);
    ut_ad(slot != NULL);
    ut_ad(slot->reserved);
    ut_ad(slot->io_already_done);

    *message1 = slot->message1;
    *message2 = slot->message2;
    *type = slot->type;

    if (slot->ret == 0 && slot->n_bytes == (long) slot->len) {
        ret = TRUE;
    } else {
        errno = -slot->ret;

        /* os_file_handle_error does tell us if we should retry this IO.
        As it stands now, we don't do this retry when reaping requests from a different context than the dispatcher.
        This non-retry logic is the same for windows and linux native AIO.
        We should probably look into this to transparently re-submit the IO. */
        os_file_handle_error(slot->name, "Linux aio");
        ret = FALSE;
    }

    os_mutex_exit(array->mutex);

    os_aio_array_free_slot(array, slot);

    return(ret);
}

bool32 os_aio_linux_dispatch(os_aio_array_t* array, os_aio_slot_t* slot)
{
    int             ret;
    uint32          io_ctx_index;
    struct iocb*    iocb;

    ut_ad(slot != NULL);
    ut_ad(array);

    ut_a(slot->reserved);

    /* Find out what we are going to work with.
    The iocb struct is directly in the slot. The io_context is one per segment. */

    iocb = &slot->control;
    io_ctx_index = (slot->pos * array->n_segments) / array->n_slots;

    ret = io_submit(array->aio_ctx[io_ctx_index], 1, &iocb);

#if defined(UNIV_AIO_DEBUG)
    fprintf(stderr,
        "io_submit[%c] ret[%d]: slot[%p] ctx[%p] seg[%lu]\n",
        (slot->type == OS_FILE_WRITE) ? 'w' : 'r', ret, slot,
        array->aio_ctx[io_ctx_index], (ulong) io_ctx_index);
#endif

    /* io_submit returns number of successfully queued requests or -errno. */
    if (unlikely(ret != 1)) {
        errno = -ret;
        return FALSE;
    }

    return TRUE;
}

#endif

bool32 os_file_aio_init()
{

    return TRUE;
}

void os_file_aio_free()
{

}

bool32 os_file_aio_func(
    os_aio_array_t* array,
    uint32      type,   /* in: OS_FILE_READ or OS_FILE_WRITE */
    uint32      mode,   /* in: OS_FILE_SYNC or OS_FILE_AIO */
    const char* name,   /* in: name of the file or path as a null-terminated string */
    os_file_t   file,   /* in: handle to a file */
    void*       buf,    /* in: buffer where to read or from which to write */
    uint64      offset, /* in: file offset where to read or write */
    uint32      n,      /* in: number of bytes to read or write */
    void*       message1,/* in: message to be passed along with the aio operation */
    void*       message2)/* in: message to be passed along with the aio operation */
{
    os_aio_slot_t*  slot;
    bool32          ret = TRUE;
    DWORD           len = (DWORD) n;
#ifdef __WIN__
    bool32          retval;
    void*           dummy_mess1;
    void*           dummy_mess2;
    uint32          dummy_type;
#endif

try_again:

    slot = os_aio_array_reserve_slot(type, array, message1, message2, file, name, buf, offset, n);
    if (type == OS_FILE_READ) {
        //os_n_file_reads++;
        //os_bytes_read_since_printout += n;
#ifdef __WIN__
        ret = ReadFile(file, buf, (DWORD) n, &len, &(slot->control));
#else
        if (!os_aio_linux_dispatch(array, slot)) {
            goto err_exit;
        }
#endif /* __WIN__ */
    } else if (type == OS_FILE_WRITE) {
        //os_n_file_writes++;
#ifdef __WIN__
        ret = WriteFile(file, buf, (DWORD) n, &len, &(slot->control));
#else
        if (!os_aio_linux_dispatch(array, slot)) {
            goto err_exit;
        }
#endif /* WIN_ASYNC_IO */
    } else {
        ut_error;
    }

#ifdef __WIN__
    if ((ret && len == n) || (!ret && GetLastError() == ERROR_IO_PENDING)) {
        /* aio was queued successfully! */
        if (mode == OS_FILE_SYNC) {
            /* We want a synchronous i/o operation on a file where we also use async i/o:
               in Windows we must use the same wait mechanism as for async i/o */
            retval = os_aio_windows_handle(array, slot->pos, &dummy_mess1, &dummy_mess2, &dummy_type);
            return retval;
        }
        return TRUE;
    }
    goto err_exit;
#endif /* __WIN__ */

    /* aio was queued successfully! */
    return TRUE;

err_exit:

    os_aio_array_free_slot(array, slot);

    if (os_file_handle_error(file, (char *)name)) {
        goto try_again;
    }

    return FALSE;
}

