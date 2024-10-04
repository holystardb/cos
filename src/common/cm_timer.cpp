#include "cm_timer.h"
#include "cm_log.h"

#define DAY_USECS          (uint64)86400000000
#define TIMER_INTERVAL_MS  2  // ms
#define MAX_INTERVAL_S     60 // sec

static cm_timer_t g_current_timer;
static date_t sync_time;

inline cm_timer_t *g_timer(void)
{
    return &g_current_timer;
}

inline date_t cm_get_sync_time(void)
{
    return sync_time;
}

inline void cm_set_sync_time(date_t time)
{
    sync_time = time;
}


#define CM_IS_INVALID_SCN(scn) ((scn) == 0 || (scn) == UINT_MAX64)

#define CM_TIME_TO_SCN(time_val, init_time) \
    ((uint64)((time_val)->tv_sec - (init_time)) << 32 | (uint64)(time_val)->tv_usec << 12)

#define CM_SCN_TO_TIME(scn, time_val, init_time)                                                    \
    do {                                                                                            \
        (time_val)->tv_sec = (long)((((uint64)(scn)) >> 32 & 0x00000000ffffffffULL) + (init_time)); \
        (time_val)->tv_usec = (long)(((uint64)(scn)) >> 12 & 0x00000000000fffffULL);                \
    } while (0)




static inline void timer_set_now_scn(cm_timer_t *timer, date_t interval_us)
{
    if (!atomic64_get(&timer->sys_scn_valid)) {
        return;
    }

    if (timer->system_scn == NULL) {
        return;
    }

    atomic64_t sys_scn = atomic64_get(timer->system_scn);
    if (CM_IS_INVALID_SCN(sys_scn)) {
        return;
    }

    atomic64_t *now_scn = &timer->now_scn;
    if ((*now_scn) < sys_scn) {
        atomic64_test_and_set(now_scn, sys_scn);
        return;
    }

    timeval_t old_time;
    timeval_t new_time;
    CM_SCN_TO_TIME(*now_scn, &old_time, timer->db_init_time);
    cm_date2timeval(timer->now, &new_time);

    // set scn by current timestamp
    int64 diff_sec = (int64)new_time.tv_sec - (int64)old_time.tv_sec;
    int64 diff_usec = (int64)new_time.tv_usec - (int64)old_time.tv_usec;
    if (diff_sec >= 0 && diff_usec >= 0) {
        diff_sec += diff_usec / MICROSECS_PER_SECOND;
    }
    if (diff_sec >= 0 && diff_sec < MAX_INTERVAL_S) {
        uint64 time_scn = CM_TIME_TO_SCN(&new_time, timer->db_init_time);
        atomic64_test_and_set(now_scn, time_scn);
        return;
    }
    date_t interval = interval_us;
    if (interval < 0 || interval / MICROSECS_PER_SECOND >= MAX_INTERVAL_S) {
        interval = TIMER_INTERVAL_MS * MICROSECS_PER_MILLISEC;
    }

    uint64 usec = (uint64)old_time.tv_usec + interval;
    old_time.tv_sec += usec / MICROSECS_PER_SECOND;
    old_time.tv_usec = usec % MICROSECS_PER_SECOND;
    uint64 interval_scn = CM_TIME_TO_SCN(&old_time, timer->db_init_time);
    atomic64_test_and_set(now_scn, interval_scn);
}

static void* timer_proc_thread(void *arg)
{
    cm_timer_t *timer = (cm_timer_t *)arg;
    date_t start_time = cm_now();
    int16 tz_min;

    sync_time = start_time;
    timer->status = TIMER_STATUS_RUNNING;

    while (!timer->thread_exited) {
        // In order to solve the thread deadlock problem caused by local_time_r function when fork child process.
        if (timer->status == TIMER_STATUS_PAUSING) {
            timer->status = TIMER_STATUS_PAUSED;
        }
        if (timer->status == TIMER_STATUS_PAUSED) {
            os_thread_sleep(MICROSECS_PER_MILLISEC);
            sync_time += MICROSECS_PER_MILLISEC;
            continue;
        }

        date_t old_time = timer->now;
        cm_now_detail((date_detail_t *)&timer->detail);
        timer->now = cm_encode_date((const date_detail_t *)&timer->detail);
        timer->monotonic_now = cm_monotonic_now();
        timer->today = (timer->now / DAY_USECS) * DAY_USECS;
        timer->systime = (uint32)((timer->now - start_time) / MICROSECS_PER_SECOND);

        // flush timezone
        tz_min = cm_get_local_tzoffset();
        timer->tz = tz_min;
        timer->host_tz_offset = tz_min * (int)SECONDS_PER_MIN * MICROSECS_PER_SECOND_LL;

        timer_set_now_scn(timer, timer->now - old_time);

        os_thread_sleep(TIMER_INTERVAL_MS);

        // update sync_time
        if (sync_time <= timer->now) {
            sync_time = timer->now;
        } else {
            sync_time += TIMER_INTERVAL_MS * MICROSECS_PER_MILLISEC;
        }
    }

    return NULL;
}

status_t cm_start_timer(cm_timer_t *timer)
{
    cm_now_detail((date_detail_t *)&timer->detail);
    timer->now = cm_encode_date((const date_detail_t *)&timer->detail);
    timer->monotonic_now = cm_monotonic_now();
    timer->today = (timer->now / DAY_USECS) * DAY_USECS;
    timer->systime = 0;
    int16 tz_min = cm_get_local_tzoffset();
    timer->tz = tz_min;
    timer->host_tz_offset = tz_min * (int)SECONDS_PER_MIN * MICROSECS_PER_SECOND_LL;
    timer->now_scn = 0;
    timer->sys_scn_valid = 0;
    timer->system_scn = NULL;
    timer->db_init_time = 0;
    timer->thread_exited = FALSE;
    timer->thread = os_thread_create(timer_proc_thread, timer, &timer->thread_id);

    return CM_SUCCESS;
}

void cm_close_timer(cm_timer_t *timer)
{
    timer->thread_exited = TRUE;
}

void cm_pause_timer(cm_timer_t *timer)
{
    timer->status = TIMER_STATUS_PAUSING;
    while (timer->status != TIMER_STATUS_PAUSED && !timer->thread_exited) {
        os_thread_sleep(3000000); // waitting 3s for changing status
    }
}

void cm_resume_timer(cm_timer_t *timer)
{
    timer->status = TIMER_STATUS_RUNNING;
}
