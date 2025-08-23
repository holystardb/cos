#include "knl_server.h"
#include "cm_file.h"
#include "cm_util.h"
#include "cm_log.h"
#include "knl_fsp.h"
#include "knl_redo.h"
#include "knl_trx_types.h"

#include "m_ctype.h"

/*------------------------- global config ------------------------ */




bool32 buf_pool_should_madvise;


/* The number of purge threads to use.*/
uint32 srv_purge_threads;
/* Use srv_n_io_[read|write]_threads instead. */
uint32 srv_n_file_io_threads;
uint32 srv_read_io_threads = 8;
uint32 srv_write_io_threads = 8;
uint32 srv_sync_io_contexts = 16;

uint32 srv_read_io_timeout_seconds = 30;
uint32 srv_write_io_timeout_seconds = 30;

uint32 srv_buf_LRU_old_threshold_ms = 1000;


/** in read-only mode. We don't do any
recovery and open all tables in RO mode instead of RW mode. We don't
sync the max trx id to disk either. */
bool32 srv_read_only_mode = FALSE;


// TRUE when applying redo log records during crash recovery; FALSE otherwise.
// Note that this is FALSE while a background thread is rolling back incomplete transactions.
bool32 srv_recovery_on = FALSE;


bool32 srv_archive_recovery = FALSE;


//TRUE means that recovery is running and no operations on the log files are allowed yet.
bool32 recv_no_ibuf_operations;

/** TRUE if writing to the redo log (mtr_commit) is forbidden.
Protected by log_sys->mutex. */
bool32 recv_no_log_write = FALSE;

shutdown_state_enum_t srv_shutdown_state = SHUTDOWN_NONE;

os_aio_array_t* srv_os_aio_async_read_array = NULL;
os_aio_array_t* srv_os_aio_async_write_array = NULL;
os_aio_array_t* srv_os_aio_sync_array = NULL;

memory_area_t*  srv_memory_sga = NULL;
vm_pool_t*      srv_temp_mem_pool = NULL;
memory_pool_t*  srv_common_mpool = NULL;
memory_pool_t*  srv_plan_mem_pool = NULL;
memory_pool_t*  srv_mtr_memory_pool = NULL;
memory_pool_t*  srv_dictionary_mem_pool = NULL;

CHARSET_INFO    all_charsets[MY_ALL_CHARSETS_SIZE];


srv_stats_t     srv_stats;

db_ctrl_t*      srv_ctrl_file = NULL;
char            srv_data_home[1024] = {0};
const uint32    srv_data_home_len = 1023;

/*-------------------------------------------------- */



db_charset_info_t srv_db_charset_info[] =
{
//    {my_charset_big5_chinese_ci.name, &my_charset_big5_chinese_ci},
//    {my_charset_big5_bin.name, &my_charset_big5_bin},
    {my_charset_bin.name, &my_charset_bin},
//    {my_charset_cp932_japanese_ci.name, &my_charset_cp932_japanese_ci},
//    {my_charset_cp932_bin.name, &my_charset_cp932_bin},
//    {"", &my_charset_cp1250_czech_ci},
//    {"", &my_charset_eucjpms_japanese_ci},
//    {"", &my_charset_eucjpms_bin},
//    {"", &my_charset_euckr_korean_ci},
//    {"", &my_charset_euckr_bin},
    {my_charset_filename.name, &my_charset_filename},
    {my_charset_gb2312_chinese_ci.name, &my_charset_gb2312_chinese_ci},
    {my_charset_gb2312_bin.name, &my_charset_gb2312_bin},
    {my_charset_gbk_chinese_ci.name, &my_charset_gbk_chinese_ci},
    {my_charset_gbk_bin.name, &my_charset_gbk_bin},
    {my_charset_gb18030_chinese_ci.name, &my_charset_gb18030_chinese_ci},
    {my_charset_gb18030_bin.name, &my_charset_gb18030_bin},
//    {my_charset_latin1_german2_ci.name, &my_charset_latin1_german2_ci},
    {my_charset_latin1.name, &my_charset_latin1},
    {my_charset_latin1_bin.name, &my_charset_latin1_bin},
/*
    {"", &my_charset_latin2_czech_ci},
    {"", &my_charset_sjis_japanese_ci},
    {"", &my_charset_sjis_bin},
    {"", &my_charset_tis620_thai_ci},
    {"", &my_charset_tis620_bin},
    {"", &my_charset_ucs2_general_ci},
    {"", &my_charset_ucs2_bin},
    {"", &my_charset_ucs2_unicode_ci},
    {"", &my_charset_ucs2_general_mysql500_ci},
    {"", &my_charset_ujis_japanese_ci},
    {"", &my_charset_ujis_bin},
    {"", &my_charset_utf16_bin},
    {"", &my_charset_utf16_general_ci},
    {"", &my_charset_utf16_unicode_ci},
    {"", &my_charset_utf16le_bin},
    {"", &my_charset_utf16le_general_ci},
    {"", &my_charset_utf32_bin},
    {"", &my_charset_utf32_general_ci},
    {"", &my_charset_utf32_unicode_ci},
*/
    {my_charset_utf8_general_ci.name, &my_charset_utf8_general_ci},
    {my_charset_utf8_tolower_ci.name, &my_charset_utf8_tolower_ci},
    {my_charset_utf8_unicode_ci.name, &my_charset_utf8_unicode_ci},
    {my_charset_utf8_bin.name, &my_charset_utf8_bin},
    {my_charset_utf8_general_mysql500_ci.name, &my_charset_utf8_general_mysql500_ci},
    {my_charset_utf8mb4_bin.name, &my_charset_utf8mb4_bin},
    {my_charset_utf8mb4_general_ci.name, &my_charset_utf8mb4_general_ci},
    {my_charset_utf8mb4_unicode_ci.name, &my_charset_utf8mb4_unicode_ci},

    {NULL, NULL},
};







