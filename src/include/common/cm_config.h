#ifndef _CM_CONFIG_H
#define _CM_CONFIG_H

#include "cm_type.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    uint32 num_lines;
    char** lines;
} config_lines;

extern config_lines* read_lines_from_config_file(char *config_file);
extern bool32 parse_key_value_from_config_line(char *line, char **section, char **key, char **value);

//**************************************************************************************************************//

extern int get_private_profile_int(const char *section, const char *entry, int def, char *file_name);
extern unsigned long long get_private_profile_longlong(const char *section, const char *entry, unsigned long long def, char *file_name);
extern int get_private_profile_string(const char *section, const char *entry, const char *def, char *buffer, int buffer_len, char *file_name);
extern int get_private_profile_hex(const char *section, const char *entry, int def, char *file_name);

extern int write_private_profile_string(const char *section, const char *entry, char *buffer, char *file_name);
extern int write_private_profile_int(const char *section, const char *entry, int data, char *file_name);
extern unsigned long long write_private_profile_longlong(const char *section, const char *entry, unsigned long long data, char *file_name);

#ifdef __cplusplus
}
#endif

#endif  /* _CM_CONFIG_H */

