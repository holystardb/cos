#include "cm_type.h"
#include "cm_memory.h"
#include "m_ctype.h"
#include "cm_thread.h"
#include "cm_log.h"

CHARSET_INFO *system_charset_info;

int main_(int argc, char *argv[])
{
    //system_charset_info = &my_charset_utf8_general_ci;
    ////system_charset_info = &my_charset_latin1_bin;
    ////system_charset_info = &my_charset_utf8_bin;

    //const char *p1 = "PRIMARY";
    //uint32 len1 = my_mbcharlen(system_charset_info, (uchar)*p1);
    //printf("p1 = %d\n", len1);

    //const char *p2 = "中国人民";
    //uint32 len2 = my_mbcharlen(system_charset_info, (uchar)*p2);
    //uint32 ll = my_mbcharlen(system_charset_info, (uchar)*p2);
    //printf("p2 = %d, mb len = %d, len = %lld\n", len2, ll, strlen(p2));

    //if (my_strcasecmp(system_charset_info, "PRIMARY", "PRIMARY") == 0) {
    //    printf("result = 0\n");
    //}
        
    printf("\n\n*********************** done ***********************\n");

    return 0;
}
