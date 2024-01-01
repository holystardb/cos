#include "cm_file.h"
#include "cm_mutex.h"
#include "cm_dbug.h"
#include "cm_memory.h"

#ifdef __WIN__
#include "io.h"
#endif // __WIN__

/*================================================ file io  ==================================================*/


/* We use these mutexes to protect lseek + file i/o operation, if the OS does not provide an atomic pread or pwrite, or similar */
#define OS_FILE_N_SEEK_MUTEXES          13
os_mutex_t os_file_seek_mutexes[OS_FILE_N_SEEK_MUTEXES];
bool32     is_os_file_mutexs_inited = FALSE;

void os_file_init()
{
#ifdef __WIN__
    if (is_os_file_mutexs_inited) {
        return;
    }
    is_os_file_mutexs_inited = TRUE;

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
        FILE_SHARE_READ | FILE_SHARE_WRITE,/* file can be read by other processes */
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

#endif // __WIN__

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

void os_file_get_last_error_desc(char *desc, uint32 size)
{
    uint32   err;

#ifdef __WIN__
    err = (uint32) GetLastError();
    if (err == ERROR_FILE_NOT_FOUND) {
        sprintf_s(desc, size, "OS_FILE_NOT_FOUND");
    } else if (err == ERROR_DISK_FULL) {
        sprintf_s(desc, size, "OS_FILE_DISK_FULL");
    } else if (err == ERROR_FILE_EXISTS) {
        sprintf_s(desc, size, "OS_FILE_ALREADY_EXISTS");
    } else if (err == ERROR_PATH_NOT_FOUND) {
        sprintf_s(desc, size, "OS_FILE_PATH_NOT_FOUND");
    } else if (err == ERROR_ACCESS_DENIED) {
        sprintf_s(desc, size, "OS_FILE_ACCESS_DENIED");
    } else {
        sprintf_s(desc, size, "unknow error %d", err);
    }
#else
    err = (uint32) errno;
    if (err == ENOSPC ) {
        sprintf_s(desc, size, "OS_FILE_DISK_FULL");
    } else if (err == EAGAIN) {
        sprintf_s(desc, size, "OS_FILE_AIO_RESOURCES_RESERVED");
    } else if (err == ENOENT) {
        sprintf_s(desc, size, "OS_FILE_NOT_FOUND");
    } else if (err == EEXIST) {
        sprintf_s(desc, size, "OS_FILE_ALREADY_EXISTS");
    } else if (err == EACCES) {
        sprintf_s(desc, size, "OS_FILE_ACCESS_DENIED");
    } else {
        sprintf_s(desc, size, "unknow error %d", err);
    }
#endif
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
    } else if (err == EAGAIN) {
        return(OS_FILE_AIO_RESOURCES_RESERVED);
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

bool32 os_file_rename(const char* oldpath, const char* newpath)
{
#ifdef DBUG_OUTPUT
    os_file_type_t type;
    bool32 exists;

    /* New path must not exist. */
    ut_ad(os_file_status(newpath, &exists, &type));
    ut_ad(!exists);

    /* Old path must exist. */
    ut_ad(os_file_status(oldpath, &exists, &type));
    ut_ad(exists);
#endif /* DBUG_OUTPUT */

#ifdef __WIN__
    bool32 ret;
    ret = MoveFile((LPCTSTR) oldpath, (LPCTSTR) newpath);
    if (ret) {
        return(TRUE);
    }

    return(FALSE);
#else
    int ret;
    ret = rename(oldpath, newpath);
    if (ret != 0) {
        return(FALSE);
    }

    return(TRUE);
#endif /* __WIN__ */
}

bool32 os_file_status(const char* path, bool32 *exists, os_file_type_t *type)
{
#ifdef __WIN__
    int ret;
    struct _stat64 statinfo;

    ret = _stat64(path, &statinfo);
    if (ret && (errno == ENOENT || errno == ENOTDIR)) {
        /* file does not exist */
        *exists = FALSE;
        return(TRUE);
    } else if (ret) {
        /* file exists, but stat call failed */
        return(FALSE);
    }

    if (_S_IFDIR & statinfo.st_mode) {
        *type = OS_FILE_TYPE_DIR;
    } else if (_S_IFREG & statinfo.st_mode) {
        *type = OS_FILE_TYPE_FILE;
    } else {
        *type = OS_FILE_TYPE_UNKNOWN;
    }

    *exists = TRUE;

    return(TRUE);
#else
    int ret;
    struct stat statinfo;

    ret = stat(path, &statinfo);
    if (ret && (errno == ENOENT || errno == ENOTDIR)) {
        /* file does not exist */
        *exists = FALSE;
        return(TRUE);
    } else if (ret) {
        /* file exists, but stat call failed */
        return(FALSE);
    }

    if (S_ISDIR(statinfo.st_mode)) {
        *type = OS_FILE_TYPE_DIR;
    } else if (S_ISLNK(statinfo.st_mode)) {
        *type = OS_FILE_TYPE_LINK;
    } else if (S_ISREG(statinfo.st_mode)) {
        *type = OS_FILE_TYPE_FILE;
    } else {
        *type = OS_FILE_TYPE_UNKNOWN;
    }

    *exists = TRUE;

    return(TRUE);
#endif
}

bool32 os_file_set_eof(os_file_t file)
{
#ifdef __WIN__
    //HANDLE h = (HANDLE) _get_osfhandle(fileno(file));
    return(SetEndOfFile(file));
#else /* __WIN__ */
    return(!ftruncate(fileno(file), ftell(file)));
#endif /* __WIN__ */
}

bool32 os_file_create_directory(const char *pathname, bool32 fail_if_exists)
{
#ifdef __WIN__
    BOOL rcode;
    rcode = CreateDirectory((LPCTSTR) pathname, NULL);
    if (!(rcode != 0 || (GetLastError() == ERROR_ALREADY_EXISTS && !fail_if_exists))) {
        return(FALSE);
    }

    return(TRUE);
#else
    int rcode;
    rcode = mkdir(pathname, 0770);
    if (!(rcode == 0 || (errno == EEXIST && !fail_if_exists))) {
        /* failure */
        return(FALSE);
    }

    return (TRUE);
#endif /* __WIN__ */
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



/*================================================ aio ==================================================*/



#define AIO_GET_EVENTS_MAX_COUNT     256

#ifndef __WIN__

typedef int (*os_aio_setup)(int max_events, io_context_t *ctx);
typedef int (*os_aio_destroy)(io_context_t ctx);
typedef int (*os_aio_submit)(io_context_t ctx, long nr, struct iocb *ios[]);
typedef int (*os_aio_cancel)(io_context_t ctx, struct iocb *iocb, io_event_t *event);
typedef int (*os_aio_getevents)(io_context_t ctx, long min_nr, long nr,
    io_event_t *events, struct timespec *timeout);

static os_aio_setup aio_setup_func = NULL;
static os_aio_destroy aio_destroy_func = NULL;
static os_aio_submit aio_submit_func = NULL;
static os_aio_cancel aio_cancel_func = NULL;
static os_aio_getevents aio_getevents_func = NULL;


void* os_aio_open_dl()
{
    void *lib_handle = dlopen("libaio.so.1", RTLD_LAZY);
    if (lib_handle != NULL) {
        aio_setup_func = (os_aio_setup)dlsym(lib_handle, "io_setup");
        aio_destroy_func = (os_aio_destroy)dlsym(lib_handle, "io_destroy");
        aio_submit_func = (os_aio_submit)dlsym(lib_handle, "io_submit");
        aio_cancel_func = (os_aio_cancel)dlsym(lib_handle, "io_cancel");
        aio_getevents_func = (os_aio_getevents)dlsym(lib_handle, "io_getevents");
    }

    return lib_handle;
}

void os_aio_close_dl(void *lib_handle)
{
    dlclose(lib_handle);
}

#endif

static os_aio_slot_t* os_aio_array_get_nth_slot(os_aio_array_t* array, uint32 index)
{
    ut_a(index < array->slot_count);

    return(&array->aio_slots[index]);
}

static os_aio_slot_t* os_aio_array_alloc_slot(
    os_aio_array_t* aio_array,  /* in: aio array */
    uint32          type,   /* in: OS_FILE_READ or OS_FILE_WRITE */
    void*           message1,/* in: message to be passed along with the aio operation */
    void*           message2,/* in: message to be passed along with the aio operation */
    os_file_t       file,   /* in: file handle */
    const char*     name,   /* in: name of the file or path as a null-terminated string */
    void*           buf,    /* in: buffer where to read or from which to write */
    uint64          offset, /* in: file offset */
    uint32          len)    /* in: length of the block to read or write */
{
    uint64          signal_count = 0;
    os_aio_slot_t*  slot = NULL;
#ifdef __WIN__
    OVERLAPPED*     control;
#else
    cm_iocb_t*      iocb;
    off_t           aio_offset;
#endif /* __WIN__ */

#ifdef __WIN__
    ut_a((len & 0xFFFFFFFFUL) == len);
#endif /* __WIN__ */

loop:

    mutex_enter(&aio_array->mutex);
    if (UT_LIST_GET_LEN(aio_array->free_slots) == 0) {
        mutex_exit(&aio_array->mutex);
        os_event_wait_time(aio_array->aio_slot_event, 1000, signal_count);
        signal_count = os_event_reset(aio_array->aio_slot_event);
        goto loop;
    }
    slot = UT_LIST_GET_FIRST(aio_array->free_slots);
    UT_LIST_REMOVE(list_node, aio_array->free_slots, slot);
    mutex_exit(&aio_array->mutex);

    slot->is_used   = TRUE;
    slot->used_time = time(NULL);
    slot->message1  = message1;
    slot->message2  = message2;
    slot->file      = file;
    slot->name      = name;
    slot->len       = len;
    slot->type      = type;
    slot->buf       = (byte*)buf;
    slot->offset    = offset;

#ifdef __WIN__
    control = &slot->control;
    control->Offset = (DWORD) offset & 0xFFFFFFFF;
    control->OffsetHigh = (DWORD) (offset >> 32);
    ResetEvent(slot->handle);
#else
    /* Check if we are dealing with 64 bit arch. If not then make sure that offset fits in 32 bits. */
    aio_offset = (off_t) offset;

    ut_a(sizeof(aio_offset) >= sizeof(offset) || ((os_offset_t) aio_offset) == offset);

    iocb = &slot->control;
    if (type == OS_FILE_READ) {
        io_prep_pread(iocb, file, buf, len, aio_offset);
    } else {
        ut_a(type == OS_FILE_WRITE);
        io_prep_pwrite(iocb, file, buf, len, aio_offset);
    }

    iocb->data = (void*) slot;
    slot->n_bytes = 0;
    slot->ret = OS_FILE_IO_TIMEOUT;
#endif /* __WIN__ */

    return slot;
}

static void os_aio_array_free_slot(os_aio_array_t* aio_array, os_aio_slot_t* slot)
{
    uint32 len;

    ut_ad(slot->is_used);

    slot->is_used = FALSE;

#ifdef __WIN__
    ResetEvent(slot->handle);
#else
    memset(&slot->control, 0x0, sizeof(slot->control));
    slot->n_bytes = 0;
    slot->ret = 0;
#endif

    mutex_enter(&aio_array->mutex);
    len = UT_LIST_GET_LEN(aio_array->free_slots);
    UT_LIST_ADD_LAST(list_node, aio_array->free_slots, slot);
    mutex_exit(&aio_array->mutex);

    if (len == 0) {
        os_event_set(aio_array->aio_slot_event);
    }
}

#ifdef __WIN__
static int os_aio_windows_handle(os_aio_context_t* aio_context, uint32 microseconds)
{
    os_aio_slot_t*  slot;
    uint32          index;
    int             ret_val;
    bool32          ret;
    DWORD           len;
    DWORD           dwMilliseconds = INFINITE;
    HANDLE          events[AIO_GET_EVENTS_MAX_COUNT];
    DWORD           event_count;
    os_aio_slot_t*  slot_array[AIO_GET_EVENTS_MAX_COUNT];

    mutex_enter(&aio_context->mutex, NULL);
    event_count = UT_LIST_GET_LEN(aio_context->slots);
    slot = UT_LIST_GET_FIRST(aio_context->slots);
    for (uint32 i = 0; i < event_count; i++) {
        events[i] = slot->handle;
        slot_array[i] = slot;
        slot = UT_LIST_GET_NEXT(list_node, slot);
    }
    mutex_exit(&aio_context->mutex);

    if (microseconds != OS_WAIT_INFINITE_TIME) {
        dwMilliseconds = microseconds / 1000;
    }

    index = WaitForMultipleObjects(event_count, events, TRUE, dwMilliseconds);
    if (index == WAIT_OBJECT_0) {
        ret_val = OS_FILE_IO_COMPLETION;
        for (uint32 i = 0; i < event_count; i++) {
            slot = slot_array[i];
            ut_a(slot->is_used);

            ret = GetOverlappedResult(slot->file, &(slot->control), &len, TRUE);
            if (ret && len == slot->len) {
                slot->ret = OS_FILE_IO_COMPLETION;
            } else {
                slot->ret = os_file_get_last_error();
            }
        }
    } else if (index == WAIT_TIMEOUT) {
        ret_val = OS_FILE_IO_TIMEOUT;
    } else if (index == WAIT_FAILED) {
        ret_val = os_file_get_last_error();
    } else if (index == WAIT_ABANDONED_0) {
        ret_val = OS_FILE_IO_ABANDONED;
    }

    return ret_val;
}

#else

static bool32 os_aio_linux_handle(os_aio_context_t* aio_context, uint32 microseconds)
{
    os_aio_slot_t*    slot;
    int               ret;
    struct timespec   timeout;
    struct io_event   events[AIO_GET_EVENTS_MAX_COUNT];
    DWORD             event_count;

    mutex_enter(&aio_context->mutex, NULL);
    event_count = UT_LIST_GET_LEN(aio_context->slots);
    mutex_exit(&aio_context->mutex);

retry:

    if (microseconds != OS_WAIT_INFINITE_TIME) {
        timeout.tv_sec = microseconds / 1000000;
        timeout.tv_nsec = (microseconds % 1000000) * 1000;
    }

    memset(events, 0, sizeof(struct io_event) * event_count);
    ret = aio_getevents_func(aio_context->io_ctx, 1, event_count, events,
                          microseconds == OS_WAIT_INFINITE_TIME ? NULL : &timeout);
    if (ret <= 0) {
        //if (unlikely(srv_shutdown_state == SRV_SHUTDOWN_EXIT_THREADS)) {
        //    return;
        //}

        /* This error handling is for any error in collecting the IO requests.
           The errors, if any, for any particular IO request are simply passed on to the calling routine. */
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
        //ut_print_timestamp(stderr);
        //fprintf(stderr, "os_aio: unexpected ret_code[%d] from io_getevents()!\n", ret);
        ut_error;
    }

    //
    for (uint32 i = 0; i < ret; i++) {
        struct iocb* control = (struct iocb*) events[i].obj;
        ut_a(control != NULL);

        slot = (os_aio_slot_t*) control->data;
        ut_a(slot != NULL);
        ut_a(slot->is_used);

        /* Mark this request as completed.
           The error handling will be done in the calling function. */
        if (events[i].res2 == 0 && events[i].res == (long) slot->len) {
            slot->ret = OS_FILE_IO_COMPLETION;
        } else {
            errno = -events[i].res2;
            slot->ret = os_file_get_last_error();
        }
    }

    return OS_FILE_IO_COMPLETION;
}

static bool32 os_aio_linux_submit(os_aio_context_t* aio_context, os_aio_slot_t* slot)
{
    int           ret;
    cm_iocb_t*    iocb;

    ut_ad(slot != NULL);
    ut_a(slot->is_used == TRUE);

    iocb = &slot->control;
    ret = aio_submit_func(aio_context, 1, &iocb);

    fprintf(stderr,
        "io_submit[%c] ret[%d]: slot[%p] ctx[%p]\n",
        (slot->type == OS_FILE_WRITE) ? 'w' : 'r', ret, slot, aio_context, (ulong));

    /* io_submit returns number of successfully queued requests or -errno. */
    if (unlikely(ret != 1)) {
        errno = -ret;
        return FALSE;
    }

    return TRUE;
}

#endif

bool32 os_file_aio_submit(
    os_aio_context_t* aio_context,
    uint32      type,     /* in: OS_FILE_READ or OS_FILE_WRITE */
    const char* name,     /* in: name of the file or path as a null-terminated string */
    os_file_t   file,     /* in: handle to a file */
    void*       buf,      /* in: buffer where to read or from which to write */
    uint32      count,    /* in: number of bytes to read or write */
    uint64      offset,   /* in: file offset where to read or write */
    void*       message1, /* in: message to be passed along with the aio operation */
    void*       message2) /* in: message to be passed along with the aio operation */
{
    os_aio_slot_t*  slot;

    if (type != OS_FILE_READ && type != OS_FILE_WRITE) {
        return FALSE;
    }

    slot = os_aio_array_alloc_slot(aio_context->aio_array,
        type, message1, message2, file, name, buf, offset, count);
    if (slot == NULL) {
        return FALSE;
    }

    mutex_enter(&aio_context->mutex, NULL);
    if (UT_LIST_GET_LEN(aio_context->slots) >= AIO_GET_EVENTS_MAX_COUNT) {
        mutex_exit(&aio_context->mutex);
        os_aio_array_free_slot(aio_context->aio_array, slot);
        return FALSE;
    }
    UT_LIST_ADD_LAST(list_node, aio_context->slots, slot);
    mutex_exit(&aio_context->mutex);

#ifdef __WIN__

    DWORD           offset_low; /* least significant 32 bits of file offset where to read */
    LONG            offset_high; /* most significant 32 bits of offset */
    DWORD           ret2;
    uint32          mutex_index;
    DWORD           len = (DWORD)count;
    bool32          ret = TRUE;

    /* Protect the seek / read operation with a mutex */
    mutex_index = ((uint64)file) % OS_FILE_N_SEEK_MUTEXES;
    offset_low = (offset & 0xFFFFFFFF);
    offset_high = (offset >> 32);

    os_mutex_enter(&os_file_seek_mutexes[mutex_index]);
    ret2 = SetFilePointer(file, offset_low, &offset_high, FILE_BEGIN);
    if (ret2 == 0xFFFFFFFF && GetLastError() != NO_ERROR) {
        os_mutex_exit(&os_file_seek_mutexes[mutex_index]);
        goto err_exit;
    }
    if (type == OS_FILE_READ) {
        ret = ReadFile(file, buf, (DWORD)count, &len, &(slot->control));
    } else {
        ret = WriteFile(file, buf, (DWORD)count, &len, &(slot->control));
    }
    os_mutex_exit(&os_file_seek_mutexes[mutex_index]);

    if ((ret && len == count) || (!ret && GetLastError() == ERROR_IO_PENDING)) {
        /* aio was queued successfully! */
        return TRUE;
    }

    goto err_exit;

#else

    if (!os_aio_linux_submit(aio_context, slot)) {
        goto err_exit;
    }

#endif // __WIN__

    /* aio was queued successfully! */
    return TRUE;

err_exit:

    return FALSE;
}

//Waits for an aio operation to complete.
int os_file_aio_wait(os_aio_context_t* aio_context, uint32 microseconds)
{
    int ret;

#ifdef __WIN__
    ret = os_aio_windows_handle(aio_context, microseconds);
#else
    ret = os_aio_linux_handle(aio_context, microseconds);
#endif /* WIN_ASYNC_IO */

    return ret;
}

os_aio_context_t* os_aio_array_alloc_context(os_aio_array_t* aio_array)
{
    uint64            signal_count = 0;
    os_aio_context_t *ctx;

retry:

    mutex_enter(&aio_array->mutex, NULL);
    ctx = UT_LIST_GET_FIRST(aio_array->free_contexts);
    if (ctx) {
        UT_LIST_REMOVE(list_node, aio_array->free_contexts, ctx);
        UT_LIST_ADD_LAST(list_node, aio_array->used_contexts, ctx);
    }
    mutex_exit(&aio_array->mutex);

    if (ctx == NULL) {
        os_event_wait_time(aio_array->aio_context_event, 1000, signal_count);
        signal_count = os_event_reset(aio_array->aio_context_event);
        goto retry;
    }

    return ctx;
}

void os_aio_array_free_context(os_aio_context_t* aio_context)
{
    uint32 aio_context_len;
    os_aio_slot_t* slot;
    os_aio_array_t* aio_array = aio_context->aio_array;

    mutex_enter(&aio_context->mutex, NULL);
    while ((slot = UT_LIST_GET_FIRST(aio_context->slots))) {
        UT_LIST_REMOVE(list_node, aio_context->slots, slot);
        os_aio_array_free_slot(aio_array, slot);
    }
    mutex_exit(&aio_context->mutex);

    mutex_enter(&aio_array->mutex, NULL);
    aio_context_len = UT_LIST_GET_LEN(aio_array->free_contexts);
    UT_LIST_REMOVE(list_node, aio_array->used_contexts, aio_context);
    UT_LIST_ADD_LAST(list_node, aio_array->free_contexts, aio_context);
    mutex_exit(&aio_array->mutex);

    if (aio_context_len == 0) {
        os_event_set(aio_array->aio_context_event);
    }
}

os_aio_array_t* os_aio_array_create(uint32 max_io_operation_count, uint32 io_context_count)
{
    os_aio_array_t*   array;
#ifdef __WIN__
    OVERLAPPED*        over;
#else
    int max_events = 10;
#endif /* __WIN__ */

    ut_a(max_io_operation_count > 0);
    ut_a(io_context_count > 0);

    array = (os_aio_array_t *)malloc(sizeof(os_aio_array_t));
    memset(array, 0x0, sizeof(os_aio_array_t));
    UT_LIST_INIT(array->free_slots);
    UT_LIST_INIT(array->free_contexts);
    UT_LIST_INIT(array->used_contexts);
    mutex_create(&array->mutex);
    array->aio_slot_event = os_event_create(NULL);
    array->aio_context_event = os_event_create(NULL);
    array->slot_count = max_io_operation_count;
    array->io_context_count = io_context_count;

    //
    array->aio_contexts = (os_aio_context_t *)malloc(io_context_count * sizeof(os_aio_context_t));
    memset(array->aio_contexts, 0x0, sizeof(io_context_count * sizeof(os_aio_context_t)));
    for (uint32 i = 0; i < io_context_count; i++) {
        os_aio_context_t *ctx = array->aio_contexts + i;
        ctx->aio_array = array;
        mutex_create(&ctx->mutex);
        UT_LIST_INIT(ctx->slots);
        UT_LIST_ADD_LAST(list_node, array->free_contexts, ctx);
    }

#ifdef __WIN__

#else
    /* Initialize the io_context array. One io_context per segment in the array. */
    array->aio_ctxs = (io_context_t **)malloc(io_context_count * sizeof(io_context_t));
    for (uint32 i = 0; i < io_context_count; ++i) {
        if (aio_setup_func(max_events, &array->aio_ctxs[i]) != 0) {
            goto err_exit;
        }
    }

    for (uint32 i = 0; i < io_context_count; i++) {
        os_aio_context_t *ctx = array->aio_contexts + i;
        ctx->io_ctx = array->aio_ctxs[i];
    }
#endif /* __WIN__ */

    //
    array->aio_slots = (os_aio_slot_t *)malloc(max_io_operation_count * sizeof(os_aio_slot_t));
    memset(array->aio_slots, 0x0, sizeof(max_io_operation_count * sizeof(os_aio_slot_t)));
    for (uint32 i = 0; i < max_io_operation_count; i++) {
        os_aio_slot_t* slot;
        slot = os_aio_array_get_nth_slot(array, i);
        slot->is_used = TRUE;
        UT_LIST_ADD_LAST(list_node, array->free_slots, slot);
#ifdef __WIN__
        slot->handle = CreateEvent(NULL,TRUE, FALSE, NULL);
        over = &slot->control;
        over->hEvent = slot->handle;
#else
        memset(&slot->control, 0x0, sizeof(slot->control));
        slot->n_bytes = 0;
        slot->ret = 0;
#endif /* __WIN__ */
    }

    return array;

#ifndef __WIN__

err_exit:

#endif
    return NULL;
}

void os_aio_array_free(os_aio_array_t* aio_array)
{
#ifdef __WIN__
    for (uint32 i = 0; i < aio_array->slot_count; i++) {
        os_aio_slot_t* slot = os_aio_array_get_nth_slot(aio_array, i);
        CloseHandle(slot->handle);
    }
    //free(aio_array->handles);
#else
    //free(aio_array->aio_events);
    free(aio_array->aio_ctxs);
#endif /* __WIN__ */

    mutex_destroy(&aio_array->mutex);

    os_event_destroy(aio_array->aio_slot_event);
    os_event_destroy(aio_array->aio_context_event);

    free(aio_array->aio_slots);
    free(aio_array->aio_contexts);
    free(aio_array);
}


