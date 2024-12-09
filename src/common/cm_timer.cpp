#include "cm_timer.h"
#include "cm_log.h"

#define DAY_USECS          (uint64)86400000000
#define TIMER_INTERVAL_MS  1  // ms
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

static void* timer_proc_thread(void *arg)
{
    cm_timer_t *timer = (cm_timer_t *)arg;
    date_t start_time = cm_now();
    int16 tz_min;

    LOGGER_INFO(LOGGER, LOG_MODULE_TIMER, "timer_thread starting ...");

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

        date_t old_time = timer->now_us;
        cm_now_detail((date_detail_t *)&timer->detail);
        timer->now_us = cm_encode_date((const date_detail_t *)&timer->detail);
        timer->monotonic_now_us = cm_monotonic_now();
        timer->today_us = (timer->now_us / DAY_USECS) * DAY_USECS;
        timer->systime_s = (uint32)((timer->now_us - start_time) / MICROSECS_PER_SECOND);

        // flush timezone
        tz_min = cm_get_local_tzoffset();
        timer->tz = tz_min;
        timer->host_tz_offset = tz_min * (int)SECONDS_PER_MIN * MICROSECS_PER_SECOND_LL;

        os_thread_sleep(TIMER_INTERVAL_MS);

        // update sync_time
        if (sync_time <= timer->now_us) {
            sync_time = timer->now_us;
        } else {
            sync_time += TIMER_INTERVAL_MS * MICROSECS_PER_MILLISEC;
        }
    }

    LOGGER_INFO(LOGGER, LOG_MODULE_TIMER, "timer_thread exited");

    return NULL;
}

status_t cm_start_timer(cm_timer_t *timer)
{
    LOGGER_DEBUG(LOGGER, LOG_MODULE_TIMER, "cm_start_timer: starting timer thread");

    cm_now_detail((date_detail_t *)&timer->detail);
    timer->now_us = cm_encode_date((const date_detail_t *)&timer->detail);
    timer->monotonic_now_us = cm_monotonic_now();
    timer->today_us = (timer->now_us / DAY_USECS) * DAY_USECS;
    timer->systime_s = 0;
    int16 tz_min = cm_get_local_tzoffset();
    timer->tz = tz_min;
    timer->host_tz_offset = tz_min * (int)SECONDS_PER_MIN * MICROSECS_PER_SECOND_LL;
    timer->thread_exited = FALSE;
    timer->thread = os_thread_create(timer_proc_thread, timer, &timer->thread_id);

    return CM_SUCCESS;
}

void cm_close_timer(cm_timer_t *timer)
{
    LOGGER_INFO(LOGGER, LOG_MODULE_TIMER, "timer_thread exiting ...");

    timer->thread_exited = TRUE;
}

void cm_pause_timer(cm_timer_t *timer)
{
    LOGGER_INFO(LOGGER, LOG_MODULE_TIMER, "timer_thread pausing ...");

    timer->status = TIMER_STATUS_PAUSING;
    while (timer->status != TIMER_STATUS_PAUSED && !timer->thread_exited) {
        os_thread_sleep(1000000); // waitting 1s for changing status
    }

    LOGGER_INFO(LOGGER, LOG_MODULE_TIMER, "timer_thread paused");
}

void cm_resume_timer(cm_timer_t *timer)
{
    LOGGER_INFO(LOGGER, LOG_MODULE_TIMER, "timer_thread resuming ...");

    timer->status = TIMER_STATUS_RUNNING;
}
