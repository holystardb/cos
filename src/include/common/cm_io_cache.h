#ifndef _CM_IO_CACHE_H_
#define _CM_IO_CACHE_H_

#include "cm_type.h"

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

enum cache_type
{
    TYPE_NOT_SET = 0,
    READ_CACHE,
    WRITE_CACHE,
    SEQ_READ_APPEND /* sequential read or append */,
    READ_FIFO, 
    READ_NET,
    WRITE_NET
};


struct io_cache_t
{
    /* Offset in file corresponding to the first byte of uchar* buffer. */
    uint64 pos_in_file;
    /*
      The offset of end of file for READ_CACHE and WRITE_CACHE.
      For SEQ_READ_APPEND it the maximum of the actual end of file and
      the position represented by read_end.
    */
    uint64 end_of_file;
    /* Points to current read position in the buffer */
    uchar *read_pos;
    /* the non-inclusive boundary in the buffer for the currently valid read */
    uchar  *read_end;
    uchar  *buffer;               /* The read buffer */
    /* Used in ASYNC_IO */
    uchar  *request_pos;
    
    /* Only used in WRITE caches and in SEQ_READ_APPEND to buffer writes */
    uchar  *write_buffer;
    /*
      Only used in SEQ_READ_APPEND, and points to the current read position
      in the write buffer. Note that reads in SEQ_READ_APPEND caches can
      happen from both read buffer (uchar* buffer) and write buffer
      (uchar* write_buffer).
    */
    uchar *append_read_pos;
    /* Points to current write position in the write buffer */
    uchar *write_pos;
    /* The non-inclusive boundary of the valid write area */
    uchar *write_end;
};



#ifdef __cplusplus
}
#endif // __cplusplus

#endif // _CM_IO_CACHE_H_
