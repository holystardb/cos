#ifndef _CM_DATETIME_H
#define _CM_DATETIME_H

#include "cm_type.h"


#ifdef __cplusplus
extern "C" {
#endif

#define MICROSECS_PER_MILLISECOND   1000U
#define MICROSECS_PER_SECOND        1000000U
#define SECONDS_PER_MIN             60U
#define SECONDS_PER_HOUR            3600U
#define SECONDS_PER_DAY             86400U
#define UNIX_EPOCH                  (-946684800000000ll)


typedef int64           date_t;
//typedef uint64          time_t;

typedef struct
{
    uint8           second;         /* 0-59 */
    uint8           minute;         /* 0-59 */
    uint8           hour;           /* 0-23 */
    uint8           day;            /* 1-31 */
    uint8           month;          /* 1-12 */
    uint16          year;           /* 2020- */
    uint8           week;           /* 1-7 */
    uint16          milliseconds;   /* 0-999, 1000 milliseconds = 1 second */
    uint16          microseconds;   /* 0-999, 1000 microseconds = 1 millisecond*/
    int16           tz_offset;      /* time zone */
} date_clock_t;

void current_clock(date_clock_t *clock);
bool32 is_leap_year(uint32 inyear);
void CLOCK_TO_SECONDS(date_clock_t *ClockPtr, uint64 *second_ticks);
void SECONDS_TO_CLOCK(uint64 *second_ticks, date_clock_t *ClockPtr);

void CLOCK_TO_MICRO_SECONDS(date_clock_t *ClockPtr, uint64 *micro_second_ticks);
void MICRO_SECONDS_TO_CLOCK(uint64 *micro_second_ticks, date_clock_t *ClockPtr);


date_t current_utc_time();
date_t current_time();
int32 get_time_zone();

date_t current_monotonic_time();
uint64 get_time_us(uint64 *tloc);
uint32 get_time_ms(void);


#ifdef __WIN__
#if defined(_MSC_VER) || defined(_MSC_EXTENSIONS)
#define M_DELTA_EPOCH_IN_MICROSECS      11644473600000000Ui64
#else
#define M_DELTA_EPOCH_IN_MICROSECS      11644473600000000ULL
#endif
int get_time_of_day(struct timeval *tv);
#else
#define get_time_of_day(a)         gettimeofday(a, NULL)
#endif

#ifdef __cplusplus
}
#endif

#endif  /* _CM_DATETIME_H */

