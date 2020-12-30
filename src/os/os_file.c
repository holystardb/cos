#include "os_file.h"

bool32 get_app_path(char* str)
{
#ifdef _WIN32
    GetCurrentDirectory(255, str);
#else
    sprintf(str, "%s/", getenv("HOME"));
#endif	
    
    return TRUE;
}

int32 file_size(char  *file_name, long long *file_byte_size)
{
    FILE *fp;
    fp = fopen(file_name, "r");
    if (!fp) {
        return -1;
    }
    _fseeki64(fp, 0, SEEK_END);
    *file_byte_size = _ftelli64(fp);
    fclose(fp);

    return 0;
}
