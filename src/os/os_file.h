#ifndef _OS_FILE_H
#define _OS_FILE_H

#include "os_type.h"


#ifdef __cplusplus
extern "C" {
#endif

bool32 get_app_path(char* str);
int32 file_size(char  *file_name, long long *file_byte_size);


#ifdef __cplusplus
}
#endif

#endif  /* _OS_FILE_H */

