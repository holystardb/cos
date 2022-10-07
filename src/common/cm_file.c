#include "cm_file.h"
#include "cm_mutex.h"
#include "cm_dbug.h"
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
        os_mutex_init(&os_file_seek_mutexes[i]);
    }
#endif
}

bool32 os_open_file(    char *name, uint32 create_mode, uint32 purpose, os_file_t *file)
{
    bool32 success = TRUE;

try_again:

#ifdef __WIN__
    DWORD create_flag;
    DWORD attributes;

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
        attributes = 0;
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
    DWORD offset_high; /* most significant 32 bits of offset */
    DWORD ret;

    /* Protect the seek / read operation with a mutex */
    uint32 i = ((uint64)file) % OS_FILE_N_SEEK_MUTEXES;

    offset_low = (offset & 0xFFFFFFFF);
    offset_high = (offset >> 32);

    os_mutex_lock(&os_file_seek_mutexes[i]);

    ret = SetFilePointer(file, offset_low, &offset_high, FILE_BEGIN);
    if (ret == 0xFFFFFFFF && GetLastError() != NO_ERROR) {
        os_mutex_unlock(&os_file_seek_mutexes[i]);
        goto error_handling;
    } 
    ret = ReadFile(file, buf, size, read_size, NULL);

    os_mutex_unlock(&os_file_seek_mutexes[i]);

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
    DWORD offset_high; /* most significant 32 bits of offset */
    DWORD ret;
    DWORD len;

    /* Protect the seek / read operation with a mutex */
    uint32 i = ((uint64)file) % OS_FILE_N_SEEK_MUTEXES;

    offset_low = (offset & 0xFFFFFFFF);
    offset_high = (offset >> 32);

    os_mutex_lock(&os_file_seek_mutexes[i]);

    ret = SetFilePointer(file, offset_low, &offset_high, FILE_BEGIN);
    if (ret == 0xFFFFFFFF && GetLastError() != NO_ERROR) {
        os_mutex_unlock(&os_file_seek_mutexes[i]);
        goto error_handling;
    } 
    ret = WriteFile(file, buf, size, &len, NULL);

    os_mutex_unlock(&os_file_seek_mutexes[i]);

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
bool32 os_file_handle_error(os_file_t file,         char *name)
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


