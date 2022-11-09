#define _CRT_SECURE_NO_WARNINGS

#include "cm_type.h"
#include "cm_config.h"
#include "cm_getopt.h"
#include "cm_file.h"
#include "cm_list.h"
#include "cm_mutex.h"
#include "cm_thread.h"
#include "cm_encrypt.h"
#include <direct.h>
#include <io.h>


typedef enum {
    NONE_FLAG     = 0,
    ENCRYPT_FLAG  = 1,
    DECRYPT_FLAG  = 2,
} encrypt_type_t;

typedef struct {
    char name[255];
} def_name_t;

typedef struct st_file_name
{
    char           *name;
    char            plain_file[1024];
    char            encrypt_file[1024];
    encrypt_type_t  type;
    int32           level;
    long long       begin_pos;
    long long       end_pos;
    struct st_file_name *first_block;
    int             block_num;
    UT_LIST_NODE_T(struct st_file_name) list_node;
} file_name_t;

#define M_THREAD_COUNT      32

typedef struct
{
    int                     count;
    spinlock_t              lock;
    os_event_t              os_event;
    os_thread_id_t          thread_id[M_THREAD_COUNT];
    os_thread_t             handle[M_THREAD_COUNT];
    int                     interval;
    UT_LIST_BASE_NODE_T(file_name_t) list;
} file_mgr_t;

spinlock_t g_print_lock;
file_mgr_t g_file_mgr;
static char  g_config_file[255];
/* 128 bit key */
uint8 g_encrypt_init_key[] = {
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 
    0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f};

// 40MB
uint32 g_encrypt_file_size = 1024 * 1024 * 40;

bool32 g_exited = FALSE;

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
            strcpy_s(g_config_file, 255, optarg);
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
    char     *buf = NULL, *out = NULL;
    FILE     *r_fp = NULL, *w_fp = NULL;
    size_t    len, byte_count = 0;
    int       index = 0, fread_count = 0;
    long long file_byte_size;
    aes_cypher_t aes;
    errno_t   err;

    if (get_file_size(src_file, &file_byte_size) != 0) {
        printf("aes_encrypt_file: cannot get file size  %s \n", src_file);
        return;
    }

    aes_set_key(&aes, g_encrypt_init_key, 16, AES_CYPHER_128, NULL);

    err = fopen_s(&r_fp, src_file, "rb");
    if (err != 0) {
        printf("aes_encrypt_file: cannot open file %s, %s \n", src_file, strerror(errno));
        goto err_exit;
    }
    err = fopen_s(&w_fp, output_file, "wb+");
    if (err != 0) {
        printf("aes_encrypt_file: cannot open file %s, %s \n", output_file, strerror(errno));
        goto err_exit;
    }

    fwrite(&file_byte_size, 1, sizeof(long long), w_fp);

    buf = (char *)malloc(1024 * 1024);
    out = (char *)malloc(1024 * 1024);
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
        aes_encrypt(&aes, (uint8*)buf, (int)len, (uint8 *)out, 1024*1024);
        fwrite(out, 1, len, w_fp);
        fflush(w_fp);

        fread_count++;
        if (fread_count > file_byte_size / (1024 * 1024) + 1) {
            printf("aes_encrypt_file: error, read data has exceeded file size %s \n", src_file);
            break;
        }

        byte_count += len;
        if (byte_count % g_encrypt_file_size == 0) {
            fclose(w_fp);

            //
            char sub_output_file[1024];
            int32 cnt;
            index++;
            cnt = snprintf(sub_output_file, 1024, "%s.%d", output_file, index);
            sub_output_file[cnt] = '\0';
            err = fopen_s(&w_fp, sub_output_file, "wb+");
            if (err != 0) {
                printf("aes_encrypt_file: cannot open file %s, %s \n", sub_output_file, strerror(errno));
                goto err_exit;
            }
        }
        //
        len = fread(buf, 1, 1024 * 1024, r_fp);
    }

err_exit:

    if (buf) free(buf);
    if (out) free(out);

    if (r_fp != NULL) {
        fclose(r_fp);
    }
    if (w_fp != NULL) {
        fclose(w_fp);
    }
}

void aes_encrypt_file_block(char *src_file, long long begin_pos, long long end_pos, char *output_file)
{
    char     *buf = NULL, *out = NULL;
    FILE     *r_fp = NULL, *w_fp = NULL;
    size_t    len;
    int       index = 0, fread_count = 0;
    long long file_byte_size, count;
    aes_cypher_t aes;
    errno_t   err;

    //printf("aes_encrypt_file_block begin: begin_pos %lld end_pos %lld, %s \n", begin_pos, end_pos, src_file);

    aes_set_key(&aes, g_encrypt_init_key, 16, AES_CYPHER_128, NULL);

    if (get_file_size(src_file, &file_byte_size) != 0) {
        printf("aes_encrypt_file_block: cannot get file size  %s \n", src_file);
        return;
    }
    if (begin_pos > end_pos || file_byte_size < begin_pos || file_byte_size < end_pos) {
        printf("aes_encrypt_file_block: invalid file %s, begin_pos %lld end_pos %lld file size  %lld \n",
            src_file, begin_pos, end_pos, file_byte_size);
        return;
    }

    err = fopen_s(&r_fp, src_file, "rb");
    if (err) {
        printf("aes_encrypt_file_block: cannot open file %s, %s \n", src_file, strerror(errno));
        goto err_exit;
    }
    if (0 != _fseeki64(r_fp, begin_pos, SEEK_SET)) {
        printf("aes_encrypt_file_block: cannot fseek for file %s, %s \n", src_file, strerror(errno));
        goto err_exit;
    }
    err = fopen_s(&w_fp, output_file, "wb+");
    if (err) {
        printf("aes_encrypt_file_block: cannot open file %s, %s \n", output_file, strerror(errno));
        goto err_exit;
    }

    // When it is the first block, the file size needs to be written
    if (begin_pos == 0) {
        fwrite(&file_byte_size, 1, sizeof(long long), w_fp);
    }
    count = begin_pos;
    buf = (char *)malloc(1024 * 1024);
    out = (char *)malloc(1024 * 1024);
    while (1) {
        len = fread(buf, 1, 1024 * 1024, r_fp);
        if (len <= 0) {
            break;
        }

        if (len % 16 != 0) {
            uint32 i, pad_count;
            pad_count = 16 - (len % 16);
            for (i = 0; i < pad_count; i++) {
                buf[len + i] = 0;
            }
            len = len + pad_count;
        }
        aes_encrypt(&aes, (uint8*)buf, (uint32)len, (uint8 *)out, 1024*1024);
        fwrite(out, 1, len, w_fp);
        fflush(w_fp);

        fread_count++;
        if (fread_count > (end_pos - begin_pos) / (1024 * 1024) + 1) {
            printf("aes_encrypt_file: error, read data has exceeded file size %s \n", src_file);
            break;
        }

        if (count + (long long)len >= end_pos) {
            break;
        }
        count += len;
    }

    //printf("aes_encrypt_file_block end: begin_pos %lld end_pos %lld, %s \n", begin_pos, end_pos, src_file);

err_exit:

    if (buf) free(buf);
    if (out) free(out);

    if (r_fp != NULL) {
        fclose(r_fp);
    }
    if (w_fp != NULL) {
        fclose(w_fp);
    }
}

void aes_decrypt_file(char *src_file, char *output_file)
{
    char     *buf = NULL, *out = NULL;
    FILE     *r_fp = NULL, *w_fp = NULL;
    size_t    len;
    int index = 0, fread_count = 0;
    long long file_size, file_byte_size;
    aes_cypher_t aes;
    errno_t   err;

    //printf("aes_decrypt_file begin: %s \n", src_file);

    aes_set_key(&aes, g_encrypt_init_key, 16, AES_CYPHER_128, NULL);

    if (get_file_size(src_file, &file_size) != 0) {
        printf("aes_decrypt_file: cannot get file size  %s \n", src_file);
        return;
    }

    err = fopen_s(&r_fp, src_file, "rb");
    if (err) {
        printf("aes_decrypt_file: cannot open file %s, %s \n", src_file, strerror(errno));
        goto err_exit;
    }
    err = fopen_s(&w_fp, output_file, "wb+");
    if (err) {
        printf("aes_decrypt_file: cannot open file %s, %s \n", output_file, strerror(errno));
        goto err_exit;
    }

    fread(&file_byte_size, 1, sizeof(long long), r_fp);

    buf = (char *)malloc(1024 * 1024);
    out = (char *)malloc(1024 * 1024);
    len = fread(buf, 1, 1024 * 1024, r_fp);
    while (len > 0) {
        if (len % 16 != 0) {
            printf("aes_decrypt_file: invalid encrypt file %s \n", src_file);
            break;
        }
        aes_decrypt(&aes, (uint8*)buf, (int)len, (uint8 *)out, 1024 * 1024);
        if (file_byte_size >= (long long)len) {
            fwrite(out, 1, len, w_fp);
        } else {
            fwrite(out, 1, (size_t)file_byte_size, w_fp);
        }
        fflush(w_fp);

        fread_count++;
        if (fread_count > file_size / (1024 * 1024) + 1) {
            printf("aes_decrypt_file: error, read data has exceeded file size %s \n", src_file);
            break;
        }

        file_byte_size = file_byte_size > (long long)len ? file_byte_size - len : 0;
        len = fread(buf, 1, 1024 * 1024, r_fp);

        //
        if (len == 0 && file_byte_size > 0) {
            fclose(r_fp);
            fread_count = 0;

            char sub_src_file[1024];
            int32 cnt;
            index++;
            cnt = snprintf(sub_src_file, 1024, "%s.%d", src_file, index);
            sub_src_file[cnt] = '\0';

            if (get_file_size(sub_src_file, &file_size) != 0) {
                printf("aes_decrypt_file: cannot get file size  %s \n", sub_src_file);
                return;
            }

            err = fopen_s(&r_fp, sub_src_file, "rb");
            if (err) {
                printf("aes_decrypt_file: cannot open file %s, %s \n", sub_src_file, strerror(errno));
                goto err_exit;
            }
            len = fread(buf, 1, 1024 * 1024, r_fp);
        }
    }

err_exit:

    if (buf) free(buf);
    if (out) free(out);
    if (r_fp != NULL) {
        fclose(r_fp);
    }
    if (w_fp != NULL) {
        fclose(w_fp);
    }
}

int find_data_from_dir(char *dir, struct _finddata_t fdata[], int32 size, int32 *count)
{
    intptr_t fr;
    
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
    spin_lock(&g_print_lock, NULL);
    printf("    ");
    for (int32 i = 1; i < level; i++) {
        printf("|---");
    }
    if (result_info[0] == '\0') {
        printf("+ %s\n", dir_name);
    } else {
        printf("+ %s (%s)\n", dir_name, result_info);
    }
    spin_unlock(&g_print_lock);
}

void print_filename_by_level(int32 level, char *file_name, char *result_info)
{
    spin_lock(&g_print_lock, NULL);
    printf("    ");
    for (int32 i = 1; i < level - 1; i++) {
        printf("    ");
    }
    if (level > 1) {
        printf("|---");
    }
    printf(" %s (%s)\n", file_name, result_info);
    spin_unlock(&g_print_lock);
}

void* file_worker_thread_entry(void *arg)
{
    file_name_t *block;

    while(!g_exited)
    {
        spin_lock(&g_file_mgr.lock, NULL);
        block = UT_LIST_GET_FIRST(g_file_mgr.list);
        if (block != NULL) {
            UT_LIST_REMOVE(list_node, g_file_mgr.list, block);
            spin_unlock(&g_file_mgr.lock);
        } else {
            spin_unlock(&g_file_mgr.lock);
            os_event_wait_time(g_file_mgr.os_event, 100000);
            os_event_reset(g_file_mgr.os_event);
            continue;
        }

        time_t begin_time = time(NULL);
        if (block->type == ENCRYPT_FLAG) {
            aes_encrypt_file_block(block->plain_file, block->begin_pos, block->end_pos, block->encrypt_file);
        } else if (block->type == DECRYPT_FLAG) {
            aes_decrypt_file(block->encrypt_file, block->plain_file);
        }

        if (block->begin_pos == 0) { // first block
            while (1) {
                spin_lock(&g_file_mgr.lock, NULL);
                if (block->block_num <= 1) {
                    spin_unlock(&g_file_mgr.lock);
                    break;
                }
                spin_unlock(&g_file_mgr.lock);
                os_thread_sleep(100000);
            }

            char time_str[100] = {0};
            sprintf_s(time_str, 100, "done, %lld seconds", time(NULL) - begin_time);
            print_filename_by_level(block->level, block->name, time_str);

            spin_lock(&g_file_mgr.lock, NULL);
            g_file_mgr.count--;
            spin_unlock(&g_file_mgr.lock);
        } else if (block->first_block) {

            //char time_str[100] = { 0 };
            //sprintf(time_str, "%lld seconds", time(NULL) - begin_time);
            //printf("%s %s\n", block->plain_file, time_str);

            spin_lock(&g_file_mgr.lock, NULL);
            block->first_block->block_num--;
            spin_unlock(&g_file_mgr.lock);
        }

        free(block);
    }
    
    return NULL;
}

void recursion_encrypt_decryption_concurrent(int32 level, char *plain_dir, char *encrypt_dir,
    def_name_t *include_file, uint32 include_file_count, def_name_t *include_suffix, uint32 suffix_count,
    def_name_t *exclude_dir, uint32 dir_count, encrypt_type_t type)
{
    struct _finddata_t  fdata[255];
    int32               count = 0, ret, len, name_len;
    char                plain_file[1024], encrypt_file[1024];
    time_t              cur_time = time(NULL);

    ret = find_data_from_dir(type == ENCRYPT_FLAG ? plain_dir : encrypt_dir, fdata, 1024, &count);
    if (ret == -1) {
        return;
    }

    for (int32 i = 0; i < count; i++)
    {
        if ((fdata[i].attrib & _A_SUBDIR) && (strcmp(fdata[i].name, ".") == 0 || strcmp(fdata[i].name, "..") == 0)) {
            continue;
        }
        if (fdata[i].attrib & _A_SUBDIR) {
            bool32 is_found = FALSE;
            for (uint32 j = 0; j < dir_count; j++) {
                if (strcmp(fdata[i].name, exclude_dir[j].name) == 0) {
                    is_found = TRUE;
                    break;
                }
            }
            if (is_found) {
                print_dirname_by_level(level + 1, fdata[i].name, "skip");
                continue;
            }
        }
        if (fdata[i].attrib & _A_SUBDIR) {
            print_dirname_by_level(level + 1, fdata[i].name, "");
            len = snprintf(plain_file, 1024, "%s\\%s", plain_dir, fdata[i].name);
            plain_file[len] = '\0';
            len = snprintf(encrypt_file, 1024, "%s\\%s", encrypt_dir, fdata[i].name);
            encrypt_file[len] = '\0';
            recursion_encrypt_decryption_concurrent(level+1, plain_file, encrypt_file, include_file, include_file_count,
                include_suffix, suffix_count, exclude_dir, dir_count, type);
        }
    }

    spin_lock(&g_file_mgr.lock, NULL);
    if (g_file_mgr.count != 0) {
        printf("recursion_encrypt_decryption: g_file_mgr.count(%d) is not zero.", g_file_mgr.count);
        spin_unlock(&g_file_mgr.lock);
        return;
    }
    spin_unlock(&g_file_mgr.lock);

    for (int32 i = 0; i < count; i++)
    {
        bool32 is_need_enc;
        if (fdata[i].attrib & _A_SUBDIR) {
            continue;
        }
        name_len = (int32)strlen(fdata[i].name);
        //
        if (suffix_count > 0) {
            is_need_enc = FALSE;
            for (uint32 j = 0; j < suffix_count; j++) {
                len = (int32)strlen(include_suffix[j].name);
                if (len == 0 || name_len <= len) {
                    continue;
                }
                if (strncmp(fdata[i].name + name_len - len, include_suffix[j].name, len) == 0) {
                    is_need_enc = TRUE;
                    break;
                }
            }
            if (is_need_enc == FALSE) {
                print_filename_by_level(level + 1, fdata[i].name, "skip");
                continue;
            }
        }
        //
        if (include_file_count > 0) {
            is_need_enc = FALSE;
            for (uint32 j = 0; j < include_file_count; j++) {
                len = (int32)strlen(include_file[j].name);
                if (name_len != len + (type == ENCRYPT_FLAG ? 0 : 4)) {
                    continue;
                }
                if (strncmp(fdata[i].name, include_file[j].name, len) == 0) {
                    is_need_enc = TRUE;
                    break;
                }
            }
            if (is_need_enc == FALSE) {
                print_filename_by_level(level + 1, fdata[i].name, "skip");
                continue;
            }
        }
        //
        if (cur_time - fdata[i].time_write > g_file_mgr.interval) {
            char buff[50];
            strftime(buff, 30, "%Y-%m-%d %H:%M:%S -- skip", localtime(&fdata[i].time_write));
            print_filename_by_level(level + 1, fdata[i].name, buff);
            continue;
        }

        spin_lock(&g_file_mgr.lock, NULL);
        g_file_mgr.count++;
        spin_unlock(&g_file_mgr.lock);

        file_name_t *first_block = (file_name_t *)malloc(sizeof(file_name_t));
        first_block->type = type;
        first_block->level = level + 1;
        first_block->name = fdata[i].name;
        first_block->first_block = NULL;
        first_block->begin_pos = 0;
        first_block->block_num = 1;
        
        if (type == ENCRYPT_FLAG) {
            len = snprintf(first_block->plain_file, 1024, "%s\\%s", plain_dir, fdata[i].name);
            first_block->plain_file[len] = '\0';
            len = snprintf(first_block->encrypt_file, 1024, "%s\\%s.enc", encrypt_dir, fdata[i].name);
            first_block->encrypt_file[len] = '\0';

            if (get_file_size(first_block->plain_file, &first_block->end_pos) != 0) {
                printf("recursion_encrypt_decryption: cannot get file size  %s \n", first_block->plain_file);
                continue;
            }

            if (first_block->end_pos > g_encrypt_file_size) {
                long long file_byte_size = first_block->end_pos;
                uint32 blk_count = (uint32)(file_byte_size / g_encrypt_file_size) + 1;
                first_block->end_pos = g_encrypt_file_size;
                first_block->block_num = blk_count;

                for (uint32 blk = 1; blk < blk_count; blk++) {
                    file_name_t *other_block = (file_name_t *)malloc(sizeof(file_name_t));
                    other_block->type = type;
                    other_block->level = level + 1;
                    other_block->name = fdata[i].name;
                    len = snprintf(other_block->plain_file, 1024, "%s\\%s", plain_dir, fdata[i].name);
                    other_block->plain_file[len] = '\0';
                    len = snprintf(other_block->encrypt_file, 1024, "%s\\%s.enc.%d", encrypt_dir, fdata[i].name, blk);
                    other_block->encrypt_file[len] = '\0';
                    other_block->begin_pos = blk * g_encrypt_file_size;
                    if (other_block->begin_pos + g_encrypt_file_size >= file_byte_size) {
                        other_block->end_pos = file_byte_size;
                    } else {
                        other_block->end_pos = other_block->begin_pos + g_encrypt_file_size;
                    }
                    other_block->first_block = first_block;

                    spin_lock(&g_file_mgr.lock, NULL);
                    //printf("add last: begin %lld end %lld, %s \n", other_block->begin_pos, other_block->end_pos, other_block->plain_file);
                    UT_LIST_ADD_LAST(list_node, g_file_mgr.list, other_block);
                    if (1 == UT_LIST_GET_LEN(g_file_mgr.list)) {
                        spin_unlock(&g_file_mgr.lock);
                        os_event_set_signal(g_file_mgr.os_event);
                    } else {
                        spin_unlock(&g_file_mgr.lock);
                    }
                }
            }

        } else if (type == DECRYPT_FLAG) {
            len = snprintf(first_block->plain_file, 1024, "%s\\%s", plain_dir, fdata[i].name);
            first_block->plain_file[len - 4] = '\0';
            len = snprintf(first_block->encrypt_file, 1024, "%s\\%s", encrypt_dir, fdata[i].name);
            first_block->encrypt_file[len] = '\0';
        }

        spin_lock(&g_file_mgr.lock, NULL);
        //printf("add last: begin %lld end %lld, %s \n", first_block->begin_pos, first_block->end_pos, first_block->plain_file);
        UT_LIST_ADD_LAST(list_node, g_file_mgr.list, first_block);
        if (1 == UT_LIST_GET_LEN(g_file_mgr.list)) {
            spin_unlock(&g_file_mgr.lock);
            os_event_set_signal(g_file_mgr.os_event);
        } else {
            spin_unlock(&g_file_mgr.lock);
        }
    }

    while(1)
    {
        os_thread_sleep(100000);
        
        int cnt;
        spin_lock(&g_file_mgr.lock, NULL);
        cnt = g_file_mgr.count;
        spin_unlock(&g_file_mgr.lock);
        
        if (cnt <= 0) {
            break;
        }
    }
}


void recursion_encrypt_decryption_concurrent1(int32 level, char *plain_dir, char *encrypt_dir,
    def_name_t *include_file, uint32 include_file_count, def_name_t *include_suffix, uint32 suffix_count,
    def_name_t *exclude_dir, uint32 dir_count, encrypt_type_t type)
{
    struct _finddata_t  fdata[255];
    int32               count = 0, ret, len, name_len;
    char                plain_file[1024], encrypt_file[1024];

    ret = find_data_from_dir(type == ENCRYPT_FLAG ? plain_dir : encrypt_dir, fdata, 1024, &count);
    if (ret == -1) {
        return;
    }

    for (int32 i = 0; i < count; i++)
    {
        if ((fdata[i].attrib & _A_SUBDIR) && (strcmp(fdata[i].name, ".") == 0 || strcmp(fdata[i].name, "..") == 0)) {
            continue;
        }
        if (fdata[i].attrib & _A_SUBDIR) {
            bool32 is_found = FALSE;
            for (uint32 j = 0; j < dir_count; j++) {
                if (strcmp(fdata[i].name, exclude_dir[j].name) == 0) {
                    is_found = TRUE;
                    break;
                }
            }
            if (is_found) {
                print_dirname_by_level(level + 1, fdata[i].name, "skip");
                continue;
            }
        }
        if (fdata[i].attrib & _A_SUBDIR) {
            print_dirname_by_level(level + 1, fdata[i].name, "");
            len = snprintf(plain_file, 1024, "%s\\%s", plain_dir, fdata[i].name);
            plain_file[len] = '\0';
            len = snprintf(encrypt_file, 1024, "%s\\%s", encrypt_dir, fdata[i].name);
            encrypt_file[len] = '\0';
            recursion_encrypt_decryption_concurrent(level+1, plain_file, encrypt_file, include_file, include_file_count,
                include_suffix, suffix_count, exclude_dir, dir_count, type);
        }
    }

    spin_lock(&g_file_mgr.lock, NULL);
    if (g_file_mgr.count != 0) {
        printf("error: g_file_mgr.count(%d) is not zero.", g_file_mgr.count);
        spin_unlock(&g_file_mgr.lock);
        return;
    }
    spin_unlock(&g_file_mgr.lock);

    for (int32 i = 0; i < count; i++)
    {
        bool32 is_need_enc;
        if (fdata[i].attrib & _A_SUBDIR) {
            continue;
        }
        name_len = (int32)strlen(fdata[i].name);
        //
        if (suffix_count > 0) {
            is_need_enc = FALSE;
            for (uint32 j = 0; j < suffix_count; j++) {
                len = (int32)strlen(include_suffix[j].name);
                if (len == 0 || name_len <= len) {
                    continue;
                }
                if (strncmp(fdata[i].name + name_len - len, include_suffix[j].name, len) == 0) {
                    is_need_enc = TRUE;
                    break;
                }
            }
            if (is_need_enc == FALSE) {
                print_filename_by_level(level + 1, fdata[i].name, "skip");
                continue;
            }
        }
        //
        if (include_file_count > 0) {
            is_need_enc = FALSE;
            for (uint32 j = 0; j < include_file_count; j++) {
                len = (int32)strlen(include_file[j].name);
                if (name_len != len + (type == ENCRYPT_FLAG ? 0 : 4)) {
                    continue;
                }
                if (strncmp(fdata[i].name, include_file[j].name, len) == 0) {
                    is_need_enc = TRUE;
                    break;
                }
            }
            if (is_need_enc == FALSE) {
                print_filename_by_level(level + 1, fdata[i].name, "skip");
                continue;
            }
        }

        spin_lock(&g_file_mgr.lock, NULL);
        g_file_mgr.count++;
        spin_unlock(&g_file_mgr.lock);

        file_name_t *data = (file_name_t *)malloc(sizeof(file_name_t));
        data->type = type;
        data->level = level + 1;
        data->name = fdata[i].name;
        
        if (type == ENCRYPT_FLAG) {
            len = snprintf(data->plain_file, 1024, "%s\\%s", plain_dir, fdata[i].name);
            data->plain_file[len] = '\0';
            len = snprintf(data->encrypt_file, 1024, "%s\\%s.enc", encrypt_dir, fdata[i].name);
            data->encrypt_file[len] = '\0';
        } else if (type == DECRYPT_FLAG) {
            len = snprintf(data->plain_file, 1024, "%s\\%s", plain_dir, fdata[i].name);
            data->plain_file[len - 4] = '\0';
            len = snprintf(data->encrypt_file, 1024, "%s\\%s", encrypt_dir, fdata[i].name);
            data->encrypt_file[len] = '\0';
        }

        spin_lock(&g_file_mgr.lock, NULL);
        UT_LIST_ADD_LAST(list_node, g_file_mgr.list, data);
        if (1 == UT_LIST_GET_LEN(g_file_mgr.list)) {
            spin_unlock(&g_file_mgr.lock);
            os_event_set_signal(g_file_mgr.os_event);
        } else {
            spin_unlock(&g_file_mgr.lock);
        }
    }

    while(1)
    {
        os_thread_sleep(100000);
        
        int cnt;
        spin_lock(&g_file_mgr.lock, NULL);
        cnt = g_file_mgr.count;
        spin_unlock(&g_file_mgr.lock);
        
        if (cnt <= 0) {
            break;
        }
    }
}

void recursion_encrypt_decryption(int32 level, char *plain_dir, char *encrypt_dir,
    def_name_t *include_file, uint32 include_file_count, def_name_t *include_suffix, uint32 suffix_count,
    def_name_t *exclude_dir, uint32 dir_count, encrypt_type_t type)
{
    struct _finddata_t  fdata[255];
    int32               count = 0, ret, len, name_len;
    char                plain_file[1024], encrypt_file[1024];

    ret = find_data_from_dir(type == ENCRYPT_FLAG ? plain_dir : encrypt_dir, fdata, 1024, &count);
    if (ret == -1) {
        return;
    }

    for (int32 i = 0; i < count; i++)
    {
        if ((fdata[i].attrib & _A_SUBDIR) && (strcmp(fdata[i].name, ".") == 0 || strcmp(fdata[i].name, "..") == 0)) {
            continue;
        }
        if (fdata[i].attrib & _A_SUBDIR) {
            bool32 is_found = FALSE;
            for (uint32 j = 0; j < dir_count; j++) {
                if (strcmp(fdata[i].name, exclude_dir[j].name) == 0) {
                    is_found = TRUE;
                    break;
                }
            }
            if (is_found) {
                print_dirname_by_level(level + 1, fdata[i].name, "skip");
                continue;
            }
        }
        if (fdata[i].attrib & _A_SUBDIR) {
            print_dirname_by_level(level + 1, fdata[i].name, "");
            len = snprintf(plain_file, 1024, "%s\\%s", plain_dir, fdata[i].name);
            plain_file[len] = '\0';
            len = snprintf(encrypt_file, 1024, "%s\\%s", encrypt_dir, fdata[i].name);
            encrypt_file[len] = '\0';
            recursion_encrypt_decryption(level+1, plain_file, encrypt_file, include_file, include_file_count,
                include_suffix, suffix_count, exclude_dir, dir_count, type);
        }
    }

    for (int32 i = 0; i < count; i++)
    {
        bool32 is_need_enc;
        if (fdata[i].attrib & _A_SUBDIR) {
            continue;
        }
        name_len = (int32)strlen(fdata[i].name);
        //
        if (suffix_count > 0) {
            is_need_enc = FALSE;
            for (uint32 j = 0; j < suffix_count; j++) {
                len = (int32)strlen(include_suffix[j].name);
                if (len == 0 || name_len <= len) {
                    continue;
                }
                if (strncmp(fdata[i].name + name_len - len, include_suffix[j].name, len) == 0) {
                    is_need_enc = TRUE;
                    break;
                }
            }
            if (is_need_enc == FALSE) {
                print_filename_by_level(level + 1, fdata[i].name, "skip");
                continue;
            }
        }
        //
        if (include_file_count > 0) {
            is_need_enc = FALSE;
            for (uint32 j = 0; j < include_file_count; j++) {
                len = (int32)strlen(include_file[j].name);
                if (name_len != len + (type == ENCRYPT_FLAG ? 0 : 4)) {
                    continue;
                }
                if (strncmp(fdata[i].name, include_file[j].name, len) == 0) {
                    is_need_enc = TRUE;
                    break;
                }
            }
            if (is_need_enc == FALSE) {
                print_filename_by_level(level + 1, fdata[i].name, "skip");
                continue;
            }
        }

        time_t begin_time = time(NULL);
        if (type == ENCRYPT_FLAG) {
            len = snprintf(plain_file, 1024, "%s\\%s", plain_dir, fdata[i].name);
            plain_file[len] = '\0';
            len = snprintf(encrypt_file, 1024, "%s\\%s.enc", encrypt_dir, fdata[i].name);
            encrypt_file[len] = '\0';
            aes_encrypt_file(plain_file, encrypt_file);
        } else if (type == DECRYPT_FLAG) {
            len = snprintf(plain_file, 1024, "%s\\%s", plain_dir, fdata[i].name);
            plain_file[len - 4] = '\0';
            len = snprintf(encrypt_file, 1024, "%s\\%s", encrypt_dir, fdata[i].name);
            encrypt_file[len] = '\0';
            aes_decrypt_file(encrypt_file, plain_file);
        }
        
        char time_str[100] = {0};
        sprintf_s(time_str, 100, "done, %lld seconds", time(NULL) - begin_time);
        print_filename_by_level(level + 1, fdata[i].name, time_str);
    }
}

void def_split(char *def_str, def_name_t *def, uint32 size, uint32 *count)
{
    uint32  len, j = 0;
    
    len = (int32)strlen(def_str);
    if (len == 0) {
        return;
    }
    
    *count = 1;
    for (uint32 i = 0; i < len && *count < size; i++) {
        if (def_str[i] == ',') {
            def[*count - 1].name[j] = '\0';
            j = 0;
            (*count)++;
            continue;
        }
        def[*count - 1].name[j] = def_str[i];
        j++;
    }
    def[*count - 1].name[j] = '\0';
}

int main21(int argc, char *argv[])
{
    char plain_dir[1024], encrypt_dir[1024], include_file_str[1024], include_suffix_str[1024], exclude_dir_str[1024], encrypt_key[255];
    def_name_t include_file[100] = { 0 }, include_suffix[100] = { 0 }, exclude_dir[100] = { 0 };
    uint32 include_file_count = 0, suffix_count = 0, dir_count = 0;
    encrypt_type_t type;
    int thread_count, interval;
    time_t begin_time = time(NULL);
    
    get_options(argc, argv);
    if (g_config_file[0] == '\0') {
        printf("invalid config\n");
        return -1;
    }

    thread_count = get_private_profile_int("general", "thread_count", 8, g_config_file);
    type = (encrypt_type_t)get_private_profile_int("general", "type", 0, g_config_file);
    get_private_profile_string("general", "encrypt_key", "", encrypt_key, 255, g_config_file);
    get_private_profile_string("general", "plain_dir", "", plain_dir, 1024, g_config_file);
    get_private_profile_string("general", "include_file", "", include_file_str, 1024, g_config_file);
    get_private_profile_string("general", "include_suffix", "", include_suffix_str, 1024, g_config_file);
    get_private_profile_string("general", "exclude_dir_name", "", exclude_dir_str, 1024, g_config_file);
    get_private_profile_string("general", "encrypt_dir", "", encrypt_dir, 1024, g_config_file);
    g_encrypt_file_size = get_private_profile_int("general", "encrypt_file_size", 1024 * 1024 * 40, g_config_file);
    interval = get_private_profile_int("general", "interval", 3600 * 24 * 3, g_config_file);
    
    for (uint32 i = 0; i < 16; i++) {
        if (encrypt_key[i] != '\0') {
            g_encrypt_init_key[i] = encrypt_key[i];
        } else {
            break;
        }
    }
    def_split(include_file_str, include_file, 100, &include_file_count);
    def_split(include_suffix_str, include_suffix, 100, &suffix_count);
    def_split(exclude_dir_str, exclude_dir, 100, &dir_count);

    if (type == ENCRYPT_FLAG) {
        printf("\n\n*************************** encryption ***************************\n");
        printf("    Thread count: %d\n", thread_count);
        printf("    From        : %s\n", plain_dir);
        printf("    To          : %s\n\n\n", encrypt_dir);
    } else if (type == DECRYPT_FLAG) {
        printf("\n\n*************************** decryption ***************************\n");
        printf("    Thread count: %d\n", thread_count);
        printf("    From        : %s\n", encrypt_dir);
        printf("    To          : %s\n\n\n", plain_dir);
        strcpy_s(include_suffix[0].name, 255, ".enc");
        include_suffix[0].name[4] = '\0';
        suffix_count = 1;
    }

    spin_lock_init(&g_print_lock);

    g_file_mgr.count = 0;
    spin_lock_init(&g_file_mgr.lock);
    g_file_mgr.interval = interval;
    g_file_mgr.os_event = os_event_create(NULL);
    UT_LIST_INIT(g_file_mgr.list);
    if (thread_count > 1) {
        for (int i = 0; i < thread_count && i < M_THREAD_COUNT; i++) {
            g_file_mgr.handle[i] = os_thread_create(file_worker_thread_entry, NULL, &g_file_mgr.thread_id[i]);
            if (!os_thread_is_valid(g_file_mgr.handle[i])) {
                printf("failed to create thread");
                return -1;
            }
        }
        recursion_encrypt_decryption_concurrent(0, plain_dir, encrypt_dir, include_file, include_file_count,
            include_suffix, suffix_count, exclude_dir, dir_count, type);
    }
    else {
        recursion_encrypt_decryption(0, plain_dir, encrypt_dir, include_file, include_file_count,
            include_suffix, suffix_count, exclude_dir, dir_count, type);
    }

    printf("\n\n*********************** done (%lld seconds) ***********************\n", time(NULL) - begin_time);

    g_exited = TRUE;

    return 0;
}
