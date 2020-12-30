#include "os_type.h"
#include "os_config.h"
#include "os_getopt.h"
#include "os_file.h"
#include "os_rijndael.h"
#include <direct.h>
#include <io.h>

typedef enum {
    NONE_FLAG     = 0,
    ENCRYPT_FLAG  = 1,
    DECRYPT_FLAG  = 2,
} encrypt_type_t;

typedef struct {
    char suffix[256];
} suffix_t;

typedef struct {
    char dir[256];
} exclude_dir_t;

char  g_config_file[255];
/* 128 bit key */
uint8 g_encrypt_init_key[] = {
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 
    0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f};


static int get_options(int argc, char **argv)
{
    struct option long_options[] = {
         { "config",   required_argument,   NULL,    'c'},
         { "help",     no_argument,         NULL,    'h'},
         {      0,     0,                   0,       0},
    };

    int ch;
    while ((ch = getopt_long(argc, argv, "c:h", long_options, NULL)) != -1)
    {
        switch (ch)
        {
        case 'c':
            strcpy(g_config_file, optarg);
            break;
        case 'h':
            printf("files_encrypt -c encrypt.ini\n");
            break;
        default:
            printf("unknow option:%c\n", ch);
        }
    }
    return 0;
} /* get_options */

void aes_encrypt_file(char *src_file, char *output_file)
{
    char     *buf;
    FILE     *r_fp = NULL, *w_fp = NULL;
    size_t    len;
    long long file_byte_size;

    r_fp = fopen(src_file, "rb");
    w_fp = fopen(output_file, "wb+");
    if (r_fp == NULL) {
        printf("error: cannot open file %s \n", src_file);
        goto err_exit;
    }
    if (w_fp == NULL) {
        printf("error: cannot open file %s \n", output_file);
        goto err_exit;
    }

    if (file_size(src_file, &file_byte_size) != 0) {
        printf("error: cannot get file size  %s \n", src_file);
        goto err_exit;
    }
    fwrite(&file_byte_size, 1, sizeof(long long), w_fp);
    
    buf = (char *)malloc(1024 * 1024);
    len = fread(buf, 1, 1024 * 1024, r_fp);
    while (len > 0) {
        if (len % 16 != 0) {
            uint32 i, pad_count;
            pad_count = 16 - (len % 16);
            for (i = 0; i < pad_count; i++) {
                buf[len + i] = 0;
            }
            len = len + pad_count;
        }
        aes_encrypt_ecb(AES_CYPHER_128, (uint8*)buf, len, g_encrypt_init_key);
        fwrite(buf, 1, len, w_fp);
        fflush(w_fp);
        //
        len = fread(buf, 1, 1024 * 1024, r_fp);
    }

err_exit:

    if (r_fp != NULL) {
        fclose(r_fp);
    }
    if (w_fp != NULL) {
        fclose(w_fp);
    }
}

void aes_decrypt_file(char *src_file, char *output_file)
{
    char     *buf;
    FILE     *r_fp = NULL, *w_fp = NULL;
    size_t    len;
    long long file_byte_size;

    r_fp = fopen(src_file, "rb");
    w_fp = fopen(output_file, "wb+");
    if (r_fp == NULL) {
        printf("error: cannot open file %s \n", src_file);
        goto err_exit;
    }
    if (w_fp == NULL) {
        printf("error: cannot open file %s \n", output_file);
        goto err_exit;
    }

    fread(&file_byte_size, 1, sizeof(long long), r_fp);

    buf = (char *)malloc(1024 * 1024);
    len = fread(buf, 1, 1024 * 1024, r_fp);
    while (len > 0) {
        if (len % 16 != 0) {
            printf("error: invalid encrypt file %s \n", src_file);
            break;
        }
        aes_decrypt_ecb(AES_CYPHER_128, (uint8*)buf, len, g_encrypt_init_key);
        if (file_byte_size >= len) {
            fwrite(buf, 1, len, w_fp);
        } else {
            fwrite(buf, 1, (size_t)file_byte_size, w_fp);
        }
        fflush(w_fp);
        file_byte_size = file_byte_size > len ? file_byte_size - len : 0;
        //
        len = fread(buf, 1, 1024 * 1024, r_fp);
    }

err_exit:

    if (r_fp != NULL) {
        fclose(r_fp);
    }
    if (w_fp != NULL) {
        fclose(w_fp);
    }
}

int find_data_from_dir(char *dir, struct _finddata_t fdata[], int32 size, int32 *count)
{
    _int64 fr;
    
    if (_chdir(dir) == -1) {
        printf("error: cannot get dir %s\n", dir);
        return -1;
    }
    fr = _findfirst("*", fdata);
    if (fr == 0) {
        printf("error: cannot findfirst from %s\n", dir);
        return -1;
    }
    *count = 1;
    while (size > *count) {
        if (_findnext(fr, fdata + *count)) {
            _findclose(fr);
            break;
        }
        (*count)++;
    }
    return *count;
}

void print_dirname_by_level(int32 level, char *dir_name, char *result_info)
{
    printf("    ");
    for (int32 i = 1; i < level; i++) {
        printf("|---");
    }
    if (result_info[0] == '\0') {
        printf("+ %s\n", dir_name);
    } else {
        printf("+ %s (%s)\n", dir_name, result_info);
    }
}

void print_filename_by_level(int32 level, char *file_name, char *result_info)
{
    printf("    ");
    for (int32 i = 1; i < level - 1; i++) {
        printf("    ");
    }
    if (level > 1) {
        printf("|---");
    }
    printf(" %s (%s)\n", file_name, result_info);
}

void recursion_encrypt_decryption(int32 level, char *plain_dir, suffix_t *suffix, uint32 suffix_count,
    char *encrypt_dir, exclude_dir_t *exclude_dir, uint32 exclude_dir_count, encrypt_type_t type)
{
    struct _finddata_t  fdata[100];
    int32               count = 0, ret, len, suffix_len, name_len;
    char                plain_file[256], encrypt_file[256];

    ret = find_data_from_dir(type == ENCRYPT_FLAG ? plain_dir : encrypt_dir, fdata, 100, &count);
    if (ret == -1) {
        return;
    }

    for (int32 i = 0; i < count; i++)
    {
        if (fdata[i].attrib == _A_SUBDIR && (strcmp(fdata[i].name, ".") == 0 || strcmp(fdata[i].name, "..") == 0)) {
            continue;
        }
        if (fdata[i].attrib == _A_SUBDIR) {
            bool32 is_found = FALSE;
            for (uint32 j = 0; j < exclude_dir_count; j++) {
                if (strcmp(fdata[i].name, exclude_dir[j].dir) == 0) {
                    is_found = TRUE;
                    break;
                }
            }
            if (is_found) {
                print_dirname_by_level(level + 1, fdata[i].name, "skip");
                continue;
            }
        }
        if (fdata[i].attrib == _A_SUBDIR) {
            print_dirname_by_level(level + 1, fdata[i].name, "");
            len = snprintf(plain_file, 256, "%s\\%s", plain_dir, fdata[i].name);
            plain_file[len] = '\0';
            len = snprintf(encrypt_file, 256, "%s\\%s", encrypt_dir, fdata[i].name);
            encrypt_file[len] = '\0';
            recursion_encrypt_decryption(level+1, plain_file, suffix, suffix_count, encrypt_file, exclude_dir, exclude_dir_count, type);
        }
    }

    for (int32 i = 0; i < count; i++)
    {
        bool32 is_need_enc = FALSE;
        if (fdata[i].attrib == _A_SUBDIR) {
            continue;
        }

        name_len = strlen(fdata[i].name);
        for (uint32 j = 0; j < suffix_count; j++) {
            suffix_len = strlen(suffix[j].suffix);
            if (suffix_len == 0 || name_len <= suffix_len) {
                continue;
            }
            if (strncmp(fdata[i].name + name_len - suffix_len, suffix[j].suffix, suffix_len) == 0) {
                is_need_enc = TRUE;
                break;
            }
        }
        if (is_need_enc == FALSE) {
            print_filename_by_level(level + 1, fdata[i].name, "skip");
            continue;
        }

        if (type == ENCRYPT_FLAG) {
            len = snprintf(plain_file, 256, "%s\\%s", plain_dir, fdata[i].name);
            plain_file[len] = '\0';
            len = snprintf(encrypt_file, 256, "%s\\%s.enc", encrypt_dir, fdata[i].name);
            encrypt_file[len] = '\0';
            aes_encrypt_file(plain_file, encrypt_file);
        } else if (type == DECRYPT_FLAG) {
            len = snprintf(plain_file, 256, "%s\\%s", plain_dir, fdata[i].name);
            plain_file[len - 4] = '\0';
            len = snprintf(encrypt_file, 256, "%s\\%s", encrypt_dir, fdata[i].name);
            encrypt_file[len] = '\0';
            aes_decrypt_file(encrypt_file, plain_file);
        }
        print_filename_by_level(level + 1, fdata[i].name, "done");
    }
}

int main(int argc, char *argv[])
{
    char          plain_dir[256], encrypt_dir[256], plain_file_suffix[256], exclude_dir_name[256], encrypt_key[16];
    suffix_t      suffix[100] = { 0 };
    exclude_dir_t exclude_dir[100] = { 0 };
    uint32        type, suffix_count, exclude_dir_count, len;
    
    get_options(argc, argv);
    if (g_config_file[0] == '\0') {
        printf("invalid config\n");
        return -1;
    }

    type = get_private_profile_int("general", "type", 0, g_config_file);
    get_private_profile_string("general", "encrypt_key", "", encrypt_key, 16, g_config_file);
    get_private_profile_string("general", "plain_dir", "", plain_dir, 256, g_config_file);
    get_private_profile_string("general", "include_suffix", "", plain_file_suffix, 256, g_config_file);
    get_private_profile_string("general", "exclude_dir_name", "", exclude_dir_name, 256, g_config_file);
    get_private_profile_string("general", "encrypt_dir", "", encrypt_dir, 256, g_config_file);

    for (uint32 i = 0; i < 16; i++) {
        if (encrypt_key[i] != '\0') {
            g_encrypt_init_key[i] = encrypt_key[i];
        }
    }
    len = strlen(plain_file_suffix);
    if (len > 0) {
        uint32 j = 0;
        suffix_count = 1;
        for (uint32 i = 0; i < len && suffix_count < 100; i++) {
            if (plain_file_suffix[i] == ',') {
                suffix[suffix_count - 1].suffix[j] = '\0';
                j = 0;
                suffix_count++;
                continue;
            }
            suffix[suffix_count - 1].suffix[j] = plain_file_suffix[i];
            j++;
        }
    }
    len = strlen(exclude_dir_name);
    if (len > 0) {
        uint32 j = 0;
        exclude_dir_count = 1;
        for (uint32 i = 0; i < len && exclude_dir_count < 100; i++) {
            if (exclude_dir_name[i] == ',') {
                exclude_dir[exclude_dir_count - 1].dir[j] = '\0';
                j = 0;
                exclude_dir_count++;
                continue;
            }
            exclude_dir[exclude_dir_count - 1].dir[j] = exclude_dir_name[i];
            j++;
        }
    }

    if (type == ENCRYPT_FLAG) {
        printf("\n\n*************************** encryption ***************************\n");
        printf("    From: %s\n    To: %s\n\n\n", plain_dir, encrypt_dir);
    } else if (type == DECRYPT_FLAG) {
        printf("\n\n*************************** decryption ***************************\n");
        printf("    From: %s\n    To: %s\n\n\n", encrypt_dir, plain_dir);
        strcpy(suffix[0].suffix, ".enc");
        suffix[0].suffix[4] = '\0';
        suffix_count = 1;
    }
    recursion_encrypt_decryption(0, plain_dir, suffix, suffix_count, encrypt_dir, exclude_dir, exclude_dir_count, type);
    printf("\n\n*************************** done ***************************\n");

    return 0;
}

