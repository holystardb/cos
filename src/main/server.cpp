#include "cm_type.h"
#include "cm_memory.h"
#include "cm_log.h"
#include "knl_handler.h"
#include "cm_dbug.h"
#include "cm_config.h"
#include "cm_thread.h"

#include "guc.h"

/*
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
            printf("socks -c socks.ini\n");
            break;
        default:
            printf("unknow option:%c\n", ch);
        }
    }
    return 0;
}


static void reload_config()
{
    g_socks_mgr.log_level = get_private_profile_int("general", "log_level", 7, g_config_file);
    g_socks_mgr.log_type = get_private_profile_int("general", "log_type", 1, g_config_file);
    get_private_profile_string("general", "username", "", g_socks_mgr.user, 255, g_config_file);
    g_socks_mgr.user_len = (uint8)strlen(g_socks_mgr.user);
    get_private_profile_string("general", "password", "", g_socks_mgr.password, 255, g_config_file);
    g_socks_mgr.passwd_len = (uint8)strlen(g_socks_mgr.password);
    g_socks_mgr.connect_timeout_sec = get_private_profile_int("general", "connect_timeout", 3, g_config_file);
    g_socks_mgr.poll_timeout_sec = get_private_profile_int("general", "poll_timeout", 10, g_config_file);

    srv_create_auth_md5();
    log_init((log_level_t)g_socks_mgr.log_level, NULL, NULL);
}

static bool32 load_config(int argc, char **argv)
{
    char        path[256];

    get_options(argc, argv);
    if (g_config_file[0] == '\0') {
        printf("invalid config\n");
        return FALSE;
    }

    g_socks_mgr.type = (proxy_type_t)get_private_profile_int("general", "type", 0, g_config_file);
    g_socks_mgr.encrypt_type = (encrypt_type_t)get_private_profile_int("general", "encrypt_type", 0, g_config_file);
    g_socks_mgr.thread_count = get_private_profile_int("general", "thread_count", 10, g_config_file);
    get_private_profile_string("general", "bind-address", "0.0.0.0", g_socks_mgr.local_host, 255, g_config_file);
    g_socks_mgr.local_port = get_private_profile_int("general", "port", 1080, g_config_file);
    g_socks_mgr.data_buf_size = get_private_profile_int("general", "socket_buf_size", 20480, g_config_file);
    //
    g_socks_mgr.log_level = get_private_profile_int("general", "log_level", 7, g_config_file);
    g_socks_mgr.log_type = get_private_profile_int("general", "log_type", 1, g_config_file);
    get_private_profile_string("general", "username", "", g_socks_mgr.user, 255, g_config_file);
    g_socks_mgr.user_len = (uint8)strlen(g_socks_mgr.user);
    get_private_profile_string("general", "password", "", g_socks_mgr.password, 255, g_config_file);
    g_socks_mgr.passwd_len = (uint8)strlen(g_socks_mgr.password);
    g_socks_mgr.connect_timeout_sec = get_private_profile_int("general", "connect_timeout", 3, g_config_file);
    g_socks_mgr.poll_timeout_sec = get_private_profile_int("general", "poll_timeout", 10, g_config_file);

    if (g_socks_mgr.type == L_PROXY) {
        get_private_profile_string("remote", "host", "0.0.0.0", g_socks_mgr.remote_host, 255, g_config_file);
        g_socks_mgr.remote_port = get_private_profile_int("remote", "port", 1090, g_config_file);
    }

    srv_create_auth_md5();
    
    get_app_path(path);
    log_init((log_level_t)g_socks_mgr.log_level, path, "socks");

    return TRUE;
}
*/


void initialize_guc_options(char *config_file)
{
    char *section = NULL, *key = NULL, *value = NULL;
    config_lines* lines;

    lines = read_lines_from_config_file(config_file);

    build_guc_variables();
    for (int i = 0; i < lines->num_lines; i++) {
        if (!parse_key_value_from_config_line(lines->lines[i], &section, &key, &value)) {
            printf("Invalid config: %s\n", key);
            return;
        }
        if (key == NULL) {
            continue;
        }
        printf("config: key %s value %s\n", key, value);
        config_generic* conf = find_guc_variable(key);
        if (conf == NULL) {
            printf("Invalid config: not found %s\n", key);
            continue;
        }
        set_guc_option_value(conf, value);
    }
}


int main(int argc, const char *argv[])
{
    char *config_file = "D:\\MyWork\\cos\\etc\\server.ini";

    initialize_guc_options(config_file);

    knl_server_init();

    return 0;
}

