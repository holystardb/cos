#ifndef _CM_TIMER_H
#define _CM_TIMER_H

#include "cm_type.h"
#include "cm_thread.h"
#include "cm_date.h"
#include "cm_atomic.h"

#ifdef __cplusplus
extern "C" {
#endif

#define CM_HOST_TIMEZONE (g_timer()->host_tz_offset)

typedef enum en_timer_status {
    TIMER_STATUS_RUNNING,
    TIMER_STATUS_PAUSING,
    TIMER_STATUS_PAUSED,
} timer_status_t;

typedef struct st_cm_timer {
    volatile date_detail_t detail;  // detail of date, yyyy-mm-dd hh24:mi:ss
    volatile date_t now;
    volatile date_t monotonic_now;  // not affected by user change
    volatile date_t today;          // the day with time 00:00:00
    volatile uint32 systime;        // seconds between timer started and now
    volatile int32 tz;              // time zone (min)
    volatile int64 host_tz_offset;  // host timezone offset (us)
    atomic64_t now_scn;
    atomic64_t sys_scn_valid;
    atomic64_t *system_scn;
    time_t db_init_time;
    timer_status_t status;

    os_thread_t    thread;
    os_thread_id_t thread_id;
    bool32         thread_exited;
} cm_timer_t;

extern status_t cm_start_timer(cm_timer_t *timer);
extern void cm_close_timer(cm_timer_t *timer);
extern inline cm_timer_t* g_timer(void);
extern inline date_t cm_get_sync_time(void);
extern inline void cm_set_sync_time(date_t time);
extern void cm_pause_timer(cm_timer_t *timer);
extern void cm_resume_timer(cm_timer_t *timer);

#ifdef __cplusplus
}
#endif
#endif
