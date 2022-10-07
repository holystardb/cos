#include "cm_type.h"
#include "cm_memory.h"
#include "m_ctype.h"

CHARSET_INFO *system_charset_info;

int main(int argc, char *argv[])
{

    system_charset_info = &my_charset_utf8_general_ci;
    system_charset_info = &my_charset_latin1_bin;
    system_charset_info = &my_charset_utf8_bin;

    printf("\n\n*********************** done ***********************\n");

    return 0;
}
