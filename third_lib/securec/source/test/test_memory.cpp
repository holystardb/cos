#include "securec.h"
//#include <stdlib.h>
//#include <string.h>
//#define __STDC_WANT_LIB_EXT1__ 1
//#include <stdio.h>

//#include <memory.h>

#define M_STR_SIZE    1024
#define M_BUF_SIZE    1024


int main(int argc, char *argv[])
{
    char buf1[M_BUF_SIZE], str[M_STR_SIZE];
    memset_s(buf1, M_BUF_SIZE, 0x00, M_BUF_SIZE);
    memcpy_s(buf1, M_BUF_SIZE, "memory: this is a test", strlen("memory: this is a test") + 1);
    snprintf_s(str, M_STR_SIZE, M_STR_SIZE - 1, "string: %s - %d", "this is a test", 1);

    printf("buf: %s\n", buf1);
    printf("str: %s\n", str);

    return 0;
}


