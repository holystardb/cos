#include "cm_date.h"

#ifdef __cplusplus
extern "C" {
#endif

date_t current_utc_time()
{
    date_t         dt = UNIX_EPOCH;
    struct timeval tv;
    
    get_time_of_day(&tv);
    dt += ((int64)tv.tv_sec * MICROSECS_PER_SECOND + tv.tv_usec);
    return dt;
}

date_t current_time()
{
    date_t         dt = UNIX_EPOCH;
    int32          tz_min;
    struct timeval tv;
    
    get_time_of_day(&tv);
    tz_min = get_time_zone();
    dt += tz_min * 60 * 1000000LL;
    dt += ((int64)tv.tv_sec * MICROSECS_PER_SECOND + tv.tv_usec);
    return dt;
}

int32 get_time_zone()
{
#ifdef __WIN__
    TIME_ZONE_INFORMATION  tmp;
    GetTimeZoneInformation(&tmp);
    return tmp.Bias * (-1);
#else
    time_t t = time(NULL);
    struct tm now_time;
    localtime_r(&t, &now_time);
    return now_time.tm_gmtoff / SECONDS_PER_MIN;
#endif
}

date_t current_monotonic_time()
{
#ifdef __WIN__
    return current_time();
#else
    struct timespec signal_tv;
    date_t          dt;
    clock_gettime(CLOCK_MONOTONIC, &signal_tv);
    dt = ((int64)signal_tv.tv_sec * MICROSECS_PER_SECOND + signal_tv.tv_nsec / 1000);
    return dt;
#endif
}

void current_clock(date_clock_t *clock)
{
#ifdef __WIN__
    SYSTEMTIME     CMOSClock;
    
    /* get real clock from CMOS */
    GetLocalTime(&CMOSClock);

    clock->year      = (uint16)CMOSClock.wYear;
    clock->month     = (uint8)CMOSClock.wMonth;
    clock->day       = (uint8)CMOSClock.wDay;
    clock->hour      = (uint8)CMOSClock.wHour;
    clock->minute    = (uint8)CMOSClock.wMinute;
    clock->second    = (uint8)CMOSClock.wSecond;
    clock->milliseconds = (uint16)CMOSClock.wMilliseconds;
    clock->microseconds = 0;
    clock->week      = (uint8)CMOSClock.wDayOfWeek;
    if(CMOSClock.wDayOfWeek == 0) { // Sunday
        clock->week = 7;
    }
    
#else
    time_t dt;
    struct tm *tm1;
    struct tm tm1_r;
    struct timeval tp;
    
    /* get real clock from CMOS */
    gettimeofday(&tp,NULL);
    
    dt = tp.tv_sec;
    tm1 = localtime_r(&dt, &tm1_r);
    
    clock->year      = (uint16)(tm1->tm_year+1900);
    clock->month     = (uint8)tm1->tm_mon + 1;
    clock->day       = (uint8)tm1->tm_mday;
    clock->hour      = (uint8)tm1->tm_hour;
    clock->minute    = (uint8)tm1->tm_min;
    clock->second    = (uint8)tm1->tm_sec;
    clock->milliseconds = (uint16)(tp.tv_usec / 1000);
    clock->microseconds = (uint16)(tp.tv_usec % 1000);
    clock->week      = (uint8)tm1->tm_wday;
    if(tm1->tm_wday==0) {
        clock->week = 7;
    }
#endif
}

//convert second tick to clock.
void SECONDS_TO_CLOCK(uint64 *second_ticks, date_clock_t *ClockPtr)
{
    uint16   DaysPerYear = 365;  /* days per year */
    uint16   year;
    uint64   days, dayss;        /* days since 2020/01/01 */
    uint64   seconds;            /* seconds rest */
    uint8    month, day, week, hour, minute, second;
    uint8    DaysPerMonth[12]={31,28,31,30,31,30,31,31,30,31,30,31};

    days    = *second_ticks / SECONDS_PER_DAY;
    seconds = *second_ticks % SECONDS_PER_DAY;
    hour    = (uint8)(seconds / SECONDS_PER_HOUR);
    seconds = seconds % SECONDS_PER_HOUR;
    minute  = (uint8)(seconds / SECONDS_PER_MIN);
    second  = seconds % SECONDS_PER_MIN;
    dayss   = days;

    /* compute year */
    year = 2020;
    while (days >= DaysPerYear)	{
        days -= DaysPerYear;
        if ((++ year % 4) == 0)
            DaysPerYear = 366;
        else
            DaysPerYear = 365;
    }

    /* compute month */
    if (is_leap_year(year) == TRUE)
        DaysPerMonth[1] = 29;
    else
        DaysPerMonth[1] = 28;

    month = 1;
    while (days >= DaysPerMonth[month - 1]) {
        days -= DaysPerMonth[month - 1];
        month ++;
    }

    /* compute dat and week */
    day  = (uint8)(days + 1);
    week = (dayss + 6) % 7;
    if (week == 0) {
        week = 7;
    }

    ClockPtr->year   = year;
    ClockPtr->month  = month;
    ClockPtr->day    = day;
    ClockPtr->hour   = hour;
    ClockPtr->minute = minute;
    ClockPtr->second = second;
    ClockPtr->week   = week;
}

// convert clock to second tick.
static UINT8 DeltaDays[12] = {0, 3, 3, 6, 8, 11, 13, 16, 19, 21, 24, 26};
void CLOCK_TO_SECONDS(date_clock_t *ClockPtr, uint64 *second_ticks)
{
    uint64       DaysSince2020;
    uint64       SecondsSince2020;

    DaysSince2020  = ClockPtr->day - 1;
    DaysSince2020 += (ClockPtr->month - 1) * 28;
    DaysSince2020 += DeltaDays[ClockPtr->month - 1];
    if (ClockPtr->month > 2 && (ClockPtr->year % 4) == 0) {
        DaysSince2020++;
    }

    DaysSince2020 += (ClockPtr->year - 2020) * 366;
    DaysSince2020 += (ClockPtr->year - 2019) / 4;

    SecondsSince2020 = DaysSince2020 * SECONDS_PER_DAY + ClockPtr->hour * SECONDS_PER_HOUR
        + ClockPtr->minute * SECONDS_PER_MIN + ClockPtr->second;

    *second_ticks = SecondsSince2020;
}

bool32 is_leap_year(uint32 inyear)
{
    if ( (inyear % 4 == 0 && inyear % 100 != 0) || inyear % 400 == 0)
        return TRUE;

    return  FALSE;
}

void CLOCK_TO_MICRO_SECONDS(date_clock_t *ClockPtr, uint64 *micro_second_ticks)
{
     uint64 sec;
     
     CLOCK_TO_SECONDS(ClockPtr, &sec);
    *micro_second_ticks = sec * MICROSECS_PER_SECOND +
                          ClockPtr->milliseconds * MICROSECS_PER_MILLISECOND +
                          ClockPtr->microseconds;
}

void MICRO_SECONDS_TO_CLOCK(uint64 *micro_second_ticks, date_clock_t *ClockPtr)
{
    uint64 sec = *micro_second_ticks / MICROSECS_PER_SECOND;

    SECONDS_TO_CLOCK(&sec, ClockPtr);
    ClockPtr->milliseconds = (uint16)(*micro_second_ticks / MICROSECS_PER_MILLISECOND);
    ClockPtr->microseconds = (uint16)(*micro_second_ticks % MICROSECS_PER_MILLISECOND);
}


#ifdef __WIN__

int get_time_of_day(struct timeval *tv)

{
    FILETIME ft;
    unsigned __int64 tmpres = 0;
    static int tzflag = 0;
    
    if (tv != NULL)
    {
        GetSystemTimeAsFileTime(&ft);
        
        tmpres = ft.dwHighDateTime;
        tmpres <<= 32;
        tmpres |= ft.dwLowDateTime;

        tmpres /= 10; /*convert into microseconds*/
        /*convert file time to unix epoch*/
        tmpres -=  M_DELTA_EPOCH_IN_MICROSECS;
        tv->tv_sec = (long)(tmpres / 1000000UL);
        tv->tv_usec = (long)(tmpres % 1000000UL);
    }

    return 0;
}

#endif

#ifdef __cplusplus
}
#endif


