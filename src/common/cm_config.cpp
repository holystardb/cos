#include "cm_config.h"
#include "cm_util.h"
#include "securec.h"


#ifdef __cplusplus
extern "C" {
#endif

#define MAX_LINE_LENGTH    1024

static int read_line(FILE *fp, char *bp)
{
    char c = '\0';
    int i = 0;
    bool32 isAssgin = 0;
    /* Read one line from the source file */
    while (1) {
        c = getc(fp);
        if (c == '\n') {
            break;
        }

        if (c == '\r') {
            continue;
        }

        if (c == '=') {
            isAssgin = 1;
        }

        if (feof(fp)) {
            /* return FALSE on unexpected EOF */
            if (isAssgin == 1) {
                bp[i] = '\0';
                return(1);
            } else {
                return(0);
            }
        }
        bp[i++] = c;
    }
    bp[i] = '\0';
    return(1);
}

static bool32 char_is_space_or_tab(char ch)
{
    if (ch == 0x09 /*tab*/ || ch == 0x20 /*space*/) {
        return TRUE;
    }

    return FALSE;
}

/************************************************************************
* Function:     Get_private_profile_int()
* Arguments:    <char *> section - the name of the section to search for
*               <char *> entry - the name of the entry to find the value of
*               <int> def - the default value in the event of a failed read
*               <char *> file_name - the name of the .ini file to read from
* Returns:      the value located at entry
*************************************************************************/
int get_private_profile_int(const char *section, const char *entry, int def, char *file_name)
{
    errno_t err;
    FILE *fp;
    //Try to fix the issue that the MAX_PATH should be 256, not 80
    char buff[MAX_LINE_LENGTH];
    char *ep;
    char t_section[MAX_LINE_LENGTH];
    char value[12];
    size_t len = strlen(entry);
    int i;
    //To support negative number convert
    bool32 b_IsNegative = FALSE;

    err = fopen_s(&fp, file_name, "r");
    if (err != 0) {
        return(0);
    }
    sprintf_s(t_section, MAX_LINE_LENGTH, "[%s]", section); /* Format the section name */
    /*  Move through file 1 line at a time until a section is matched or EOF */
    do {
        if (!read_line(fp, buff)) {
            fclose(fp);
            return(def);
        }
    } while (strcmp(buff, t_section));
    /* Now that the section has been found, find the entry.
     * Stop searching upon leaving the section's area. */
    do {
        if (!read_line(fp, buff) || buff[0] == '[') { //130516 Willy modify '\0' to '[' for parser ini bug.
            fclose(fp);
            return(def);
        }
    } while (strncmp(buff, entry, len));
    
    ep = strrchr(buff, '=');   /* Parse out the equal sign */
    ep++;
    if (!strlen(ep)) { /* No setting? */
        return(def);
    }
    while (char_is_space_or_tab(ep[0]) && strlen(ep) > 0) {
        ep++;
    }
    /* Copy only numbers fail on characters */
    //To support negative number convert
    if (ep[0] == '-') {
        b_IsNegative = TRUE;
        for (i = 1; isdigit(ep[i]); i++) {
            value[i - 1] = ep[i];
        }
        value[--i] = '\0';
    } else {
        for (i = 0; isdigit(ep[i]); i++) {
            value[i] = ep[i];
        }
        value[i] = '\0';
    }
    fclose(fp);                /* Clean up and return the value */
    //To support negative number convert
    if (b_IsNegative) {
        return (0 - atoi(value));
    } else {
        return(atoi(value));
    }
}

unsigned long long get_private_profile_longlong(const char *section, const char *entry, unsigned long long def, char *file_name)
{
    errno_t err;
    FILE *fp;
    char buff[MAX_LINE_LENGTH];
    char *ep;
    char t_section[MAX_LINE_LENGTH];
    char value[16];
    size_t len = strlen(entry);
    int i;

    err = fopen_s(&fp, file_name, "r");
    if (err != 0) {
        return(0);
    }
    sprintf_s(t_section, MAX_LINE_LENGTH, "[%s]", section); /* Format the section name */
    /*  Move through file 1 line at a time until a section is matched or EOF */
    do {
        if (!read_line(fp, buff)) {
            fclose(fp);
            return(def);
        }
    } while (strcmp(buff, t_section));
    /* Now that the section has been found, find the entry.
     * Stop searching upon leaving the section's area. */
    do {
        if (!read_line(fp, buff) || buff[0] == '[') { //130516 Willy modify '\0' to '[' for parser ini bug.
            fclose(fp);
            return(def);
        }
    } while (strncmp(buff, entry, len));
    
    ep = strrchr(buff, '=');   /* Parse out the equal sign */
    ep++;
    if (!strlen(ep)) {         /* No setting? */
        return(def);
    }
    while (char_is_space_or_tab(ep[0]) && strlen(ep) > 0) {
        ep++;
    }
    /* Copy only numbers fail on characters */
    for (i = 0; isdigit(ep[i]); i++) {
        value[i] = ep[i];
    }
    value[i] = '\0';
    fclose(fp);                /* Clean up and return the value */
    return(_atoi64(value));
}

/**************************************************************************
* Function:     Get_private_profile_string()
* Arguments:    <char *> section - the name of the section to search for
*               <char *> entry - the name of the entry to find the value of
*               <char *> def - default string in the event of a failed read
*               <char *> buffer - a pointer to the buffer to copy into
*               <int> buffer_len - the max number of characters to copy
*               <char *> file_name - the name of the .ini file to read from
* Returns:      the number of characters copied into the supplied buffer
***************************************************************************/
int get_private_profile_string(const char *section, const char *entry, const char *def, char *buffer, int buffer_len, char *file_name)
{
    errno_t err;
    FILE *fp;
    char buff[MAX_LINE_LENGTH];
    char *ep;
    char t_section[MAX_LINE_LENGTH];
    size_t len = strlen(entry), i;

    err = fopen_s(&fp, file_name, "r");
    if (err != 0) {
        return(0);
    }
    sprintf_s(t_section, MAX_LINE_LENGTH, "[%s]", section);  /* Format the section name */
    /*  Move through file 1 line at a time until a section is matched or EOF */
    do {
        if (!read_line(fp, buff)) {
            strncpy_s(buffer, buffer_len, def, buffer_len);
            return (int)strlen(buffer);
        }
    } while (strcmp(buff, t_section));
    /* Now that the section has been found, find the entry.
     * Stop searching upon leaving the section's area. */
    do {
        if (!read_line(fp, buff) || buff[0] == '[') { //130516 Willy modify '\0' to '[' for parser ini bug.
            fclose(fp);
            strncpy_s(buffer, buffer_len, def, buffer_len);
            return (int)strlen(buffer);
        }
    } while (strncmp(buff, entry, len));

    ep = strrchr(buff, '=');   /* Parse out the equal sign */
    do {
        ep++;
    } while(!strncmp(ep, " ", 1));   //Remove the blank space

    /* Copy up to buffer_len chars to buffer */
    len = (int)strlen(ep);
    for (i = 0; i < len; i++) {
        if (ep[i] == '#') {
            ep[i] = '\0';
            len = i;
            break;
        }
    }
    if (len > 0) {
        for (i = len - 1; i >= 0; i--) {
            if (ep[i] != ' ') {
                break;
            }
            ep[i] = '\0';
        }
    }
    strcpy_s(buffer, buffer_len, ep);
    buffer[buffer_len - 1] = '\0';
    fclose(fp);               /* Clean up and return the amount copied */
    return (int)strlen(buffer);
}

int get_private_profile_hex(const char *section, const char *entry, int def, char *file_name)
{
    char valBuf[16], valBuf2[16];
    int data;

    memset(valBuf, 0, sizeof(valBuf));
    memset(valBuf2, 0, sizeof(valBuf2));

    sprintf_s(valBuf2, 16, "0x%x", def);
    get_private_profile_string(section, entry, valBuf2, &valBuf[0], sizeof(valBuf), file_name);
    data = 0;
    sscanf_s(valBuf, "0x%x", &data);
    return data;
}

/*************************************************************************
 * Function:    Write_private_profile_string()
 * Arguments:   <char *> section - the name of the section to search for
 *              <char *> entry - the name of the entry to find the value of
 *              <char *> buffer - pointer to the buffer that holds the string
 *              <char *> file_name - the name of the .ini file to read from
 * Returns:     TRUE if successful, otherwise FALSE
 *************************************************************************/
int write_private_profile_string(const char *section, const char *entry, char *buffer, char *file_name)
{
    errno_t err;
    FILE *rfp, *wfp;
    //Try to fix the issue that the MAX_PATH should be 256, not 80
    char buff[MAX_LINE_LENGTH];
    char t_section[MAX_LINE_LENGTH];
    int len = (int)strlen(entry);
#ifdef __WIN__
    char tmp_name[15];
    tmpnam_s(tmp_name, 15); /* Get a temporary file name to copy to */
#else
    char tmp_name[] = "temp-XXXXXX";
    mkstemp(tmp_name);
#endif
    sprintf_s(t_section, MAX_LINE_LENGTH, "[%s]", section); /* Format the section name */

    err = fopen_s(&rfp, file_name, "r");
    if (err == 0) { /* If the .ini file doesn't exist */
        err = fopen_s(&wfp, file_name, "w");
        if (err != 0) { /*  then make one */
            return(0);
        }
        fprintf(wfp, "%s\n", t_section);
        fprintf(wfp, "%s=%s\n", entry, buffer);
        fclose(wfp);
        return(1);
    }

    err = fopen_s(&wfp, tmp_name, "w");
    if (err != 0) {
        fclose(rfp);
        return(0);
    }

    /* Move through the file one line at a time until a section is
     * matched or until EOF. Copy to temp file as it is read. */
    do {
        if (!read_line(rfp, buff)) {
            /* Failed to find section, so add one to the end */
            fprintf(wfp, "\n%s\n", t_section);
            fprintf(wfp, "%s=%s\n", entry, buffer);
            /* Clean up and rename */
            fclose(rfp);
            fclose(wfp);
            unlink(file_name);
            rename(tmp_name, file_name);
            return(1);
        }
        fprintf(wfp, "%s\n", buff);
    } while (strcmp(buff, t_section));

    /* Now that the section has been found, find the entry. Stop searching
     * upon leaving the section's area. Copy the file as it is read
     * and create an entry if one is not found.  */
    while (1) {
        if (!read_line(rfp, buff)) {
            /* EOF without an entry so make one */
            fprintf(wfp, "%s=%s\n", entry, buffer);
            /* Clean up and rename */
            fclose(rfp);
            fclose(wfp);
            unlink(file_name);
            rename(tmp_name, file_name);
            return(1);

        }
        if (!strncmp(buff, entry, len) || buff[0] == '\0') {
            break;
        }
        fprintf(wfp, "%s\n", buff);
    }

    if (buff[0] == '\0') {
        fprintf(wfp, "%s=%s\n", entry, buffer);
        do {
            fprintf(wfp, "%s\n", buff);
        }
        while (read_line(rfp, buff));
    } else {
        fprintf(wfp, "%s=%s\n", entry, buffer);
        while (read_line(rfp, buff)) {
            fprintf(wfp, "%s\n", buff);
        }
    }
    /* Clean up and rename */
    fclose(wfp);
    fclose(rfp);
    unlink(file_name);
    rename(tmp_name, file_name);
    return(1);
}

int write_private_profile_int(const char *section, const char *entry, int data, char *file_name)
{
    char valBuf[16];
    memset(valBuf, 0, 16);
    sprintf_s(valBuf, 16, "%d", data);
    return write_private_profile_string(section, entry, valBuf, file_name);
}

unsigned long long write_private_profile_longlong(const char *section, const char *entry, unsigned long long data, char *file_name)
{
    char valBuf[16];
    memset(valBuf, 0, 16);
    sprintf_s(valBuf, 16, "%llu", data);
    return write_private_profile_string(section, entry, valBuf, file_name);
}


//**************************************************************************************************************//

config_lines* read_lines_from_config_file(char *config_file)
{
    errno_t err;
    FILE *fp;
    char *ep;;
    char buff[MAX_LINE_LENGTH];
    const int max_lines = 1024;
    config_lines* result;

    result = (config_lines *)malloc(sizeof(config_lines) + max_lines * sizeof(char *));
    result->num_lines = 0;
    result->lines = (char **)((char *)result + sizeof(config_lines));

    err = fopen_s(&fp, config_file, "r");
    if (err != 0) {
        exit(1);
    }

    for(;;) {
        if (!read_line(fp, buff)) {
            break;
        }

        ep = buff;
        while (char_is_space_or_tab(ep[0]) && strlen(ep) > 0) {
            ep++;
        }

        if (ep[0] != '\0') {
            size_t len = strlen(ep) + 1;
            result->lines[result->num_lines] = (char *)malloc(len);
            strncpy_s(result->lines[result->num_lines], len, ep, len);
            result->num_lines++;
        }
    }

    result->lines[result->num_lines] = NULL;

    fclose(fp);

    return result;
}

bool32 parse_key_value_from_config_line(char *line, char **section, char **key, char **value)
{
    char *end, *tmp_key, *tmp_value;

    *section = NULL;
    *key = NULL;
    *value = NULL;

    if (strlen(line) <= 0) {
        return false;
    }

    while (char_is_space_or_tab(line[0]) && strlen(line) > 0) {
        line++;
    }

    if (line[0] == '[') {  // section
        line++;
        end = strchr(line, ']');
        if (end == NULL) {
            return false;
        }
        end[0] = '\0';
        *section = line;
        return true;
    }

    tmp_key = line;
    end = tmp_value = strchr(line, '=');   /* Parse out the equal sign */

    if (end == NULL) {  // only key
        end = strchr(line, '#');
        if (end == NULL) {
            end = line + strlen(line);
        }
        end--;
        while (char_is_space_or_tab(end[0]) && end > tmp_key) {
            end[0] = '\0';
            end--;
        }
        *key = line;
        return true;
    }

    end--;
    while (char_is_space_or_tab(end[0]) && end > tmp_key) {
        end[0] = '\0';
        end--;
    }

    tmp_value[0] = '\0';
    tmp_value++;
    while (char_is_space_or_tab(tmp_value[0]) && strlen(tmp_value) > 0) {
        tmp_value++;
    }

    end = tmp_value;
    while ((end[0] != '#' && end[0] != ' ') && strlen(end) > 0) {
        end++;
    }
    if ((size_t)(end - tmp_value) < strlen(tmp_value)) {
        end[0] = '\0';
    }

    *key = tmp_key;
    *value = tmp_value;

    return true;
}

#ifdef __cplusplus
}
#endif
