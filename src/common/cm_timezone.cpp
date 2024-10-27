#include "cm_timezone.h"
#include "cm_text.h"
#include "cm_log.h"
#include "cm_dbug.h"
#include "securec.h"

#ifdef WIN32
#include <Winbase.h>
#endif

const char *g_default_tzoffset_fmt = "%c%02d:%02d";


/* db time zone, every module could get it */
static timezone_info_t g_db_timezone;

/* the buffer of the text_t should be prepared in advance and
   the length of buffer should be no less than TIMEZONE_OFFSET_STRLEN */
status_t cm_tzoffset2text(timezone_info_t tz, text_t* text)
{
    int32 hour, minute;
    uchar sign;

    if (!cm_validate_timezone(tz)) {
        //CM_THROW_ERROR(ERR_VALUE_ERROR, "an invalid timezone offset value");
        return CM_ERROR;
    }

    hour = TIMEZONE_GET_HOUR(tz);
    minute = TIMEZONE_GET_MINUTE(tz);
    sign = (hour < 0) ? '-' : '+';

    /* sprintf_s will copy the '\0' in the format string as well */
    snprintf_s(text->str, TIMEZONE_OFFSET_STRLEN, TIMEZONE_OFFSET_STRLEN - 1,
        g_default_tzoffset_fmt, sign, abs(hour), minute);

    text->len = TIMEZONE_OFFSET_STRLEN - 1; /* '\0' should not be counted as the text length */

    return CM_SUCCESS;
}

static num_errno_t cm_text2tzoffset_core(text_t* text, timezone_info_t* tz)
{
    char c;
    bool32 is_neg = FALSE;
    bool32 colon_found = FALSE;
    uint32 i = 0;
    int32 hour = 0;
    int32 min = 0;
    int32 *val_ptr = &hour;

    cm_trim_text(text);

    if (text->len < 3 || text->len > 64) {
        return NERR_ERROR;
    }
    /* handle the sign */
    c = text->str[i];
    if (CM_IS_SIGN_CHAR(c)) {
        is_neg = (c == '-');
        if ((++i) >= text->len) {
            return NERR_ERROR;
        }
    }

    for (; i < text->len; i++) {
        c = text->str[i];
        if (CM_IS_COLON(c)) {
            if (!colon_found) {
                colon_found = TRUE;
                /* after colon, switch to min */
                hour = *val_ptr;
                val_ptr = &min;
                continue;
            } else {
                /* duplicate colon */
                return NERR_ERROR;
            }
        }

        if (!CM_IS_DIGIT(c)) {
            return NERR_UNEXPECTED_CHAR;
        } else {
            *val_ptr = (*val_ptr) * CM_DEFAULT_DIGIT_RADIX + CM_C2D(c);
        }
    }

    if (!colon_found) {
        return NERR_ERROR;
    }

    hour = (is_neg) ? (0 - hour) : hour;
    if (!cm_validate_hour_min_fortz(hour, min)) {
        return NERR_OVERFLOW;
    }

    *((int16 *)tz) = (int16)((is_neg) ? (hour * TIMEZONE_MINUTES_PER_HOUR - min)
                                      : (hour * TIMEZONE_MINUTES_PER_HOUR + min));

    return NERR_SUCCESS;
}

/*
 * the function to convert a timezone offset string("[+|-]TZH:TZM") into a timezone_info_t
 * if the input string is not valid(wrong format, invalid TZH or TZM, etc),
 * this function would return CT_ERROR and report an error
 */
status_t cm_text2tzoffset(text_t* text, timezone_info_t* tz)
{
    num_errno_t retval;

    retval = cm_text2tzoffset_core(text, tz);
    switch (retval) {
        case NERR_SUCCESS:
            break;
        case NERR_ERROR:
            //CM_THROW_ERROR(ERR_VALUE_ERROR, "string does not match the timezone offset format");
            return CM_ERROR;
        case NERR_UNEXPECTED_CHAR:
            //CM_THROW_ERROR(ERR_VALUE_ERROR, "unrecognized character in the specified timezone offset");
            return CM_ERROR;
        case NERR_OVERFLOW:
            //CM_THROW_ERROR(ERR_VALUE_ERROR,
            //    "invalid time zone offset. time zone offset must be between -12:00 and 14:00");
            return CM_ERROR;
        default:
            //CM_THROW_ERROR(ERR_VALUE_ERROR, "unexpected return value");
            return CM_ERROR;
    }

    return CM_SUCCESS;
}

/*
 * get the local timezone offset in minutes on the executing system
 * this is also called ostimezone,  SYSTIMESTAMP and SYSDATE will also use this func
 *
 * if the return value is greater than zero, it means current timezone is to the EAST of GMT;
 * if the return value is less than zero, it means current timezone is to the WEST of GMT;
 */
int16 cm_get_local_tzoffset(void)
{
#ifdef WIN32
    LONG tzoffset = 0;
    TIME_ZONE_INFORMATION tmp;
    if (GetTimeZoneInformation(&tmp) == TIME_ZONE_ID_INVALID) {
        LOGGER_ERROR(LOGGER, LOG_MODULE_TIMEZONE,
            "error occurred during calling GetTimeZoneInformation(). errno: \"%lu\"", GetLastError());
        ut_error;
    } else {
        tzoffset = 0 - tmp.Bias; /* field "Bias" means west of GMT,
                                           just the opposite of the definition of our timezone offset */
    }
    return (int16)tzoffset;
#else
    struct tm result;
    time_t utc_time = time(NULL);
    int retval = 0;

    tzset();
    if (localtime_r(&utc_time, &result) != NULL) {
        retval = (int16)(result.tm_gmtoff / TIMEZONE_MINUTES_PER_HOUR); /* tm_gmtoff: seconds east of GMT */
    } else {
        /* when compiled with gcc on linux, errno is thread-safe. ref: http://www.unix.org/whitepapers/reentrant.html */
        LOGGER_ERROR(LOGGER, LOG_MODULE_TIMEZONE, "error occurred during calling localtime_r(). errno: \"%d\"", errno);
        ut_error;
    }

    return (int16)retval;
#endif
}

/*
 * this function is used to  get db time zone
 */
int16 cm_get_db_timezone(void)
{
    return g_db_timezone;
}

/*
 * this function is used to  set db time zone
 */
void cm_set_db_timezone(timezone_info_t tz)
{
    g_db_timezone = tz;
}
