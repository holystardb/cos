#include "cm_mutex.h"


/***********************************************************************************************
*                                     os muetx                                                 *
***********************************************************************************************/

os_event_t os_event_create(char* name)
{
#ifdef _WIN32
    HANDLE	event;
    event = CreateEvent(NULL,	/* No security attributes */
                        TRUE,		/* Manual reset */
                        FALSE,		/* Initial state nonsignaled */
                        name);
    return(event);
#else
    os_event_t  event;
    event = (os_event_struct_t*)malloc(sizeof(struct os_event_struct));
    os_mutex_init(&(event->os_mutex));
    pthread_cond_init(&(event->cond_var), NULL);
    event->is_set = FALSE;
    return(event);
#endif
}

void os_event_set(os_event_t event)
{
#ifdef _WIN32	
    SetEvent(event);
#else
    os_mutex_lock(&(event->os_mutex));
    if (event->is_set) {
        /* Do nothing */
    } else {
        event->is_set = TRUE;
        event->signal_count += 1;
        pthread_cond_broadcast(&(event->cond_var));
    }
    os_mutex_unlock(&(event->os_mutex));
#endif
}

void os_event_set_signal(os_event_t event)
{
#ifdef _WIN32	
    SetEvent(event);
#else
    os_mutex_lock(&(event->os_mutex));
    if (event->is_set) {
        /* Do nothing */
    } else {
        event->is_set = TRUE;
        pthread_cond_signal(&(event->cond_var));
    }
    os_mutex_unlock(&(event->os_mutex));
#endif
}

uint64 os_event_reset(os_event_t event)
{
    uint64 ret = 0;

#ifdef _WIN32
    ResetEvent(event);
#else
    os_mutex_lock(&(event->os_mutex));
    if (!event->is_set) {
        /* Do nothing */
    } else {
        event->is_set = FALSE;
    }
    ret = event->signal_count;
    os_mutex_unlock(&(event->os_mutex));
#endif

    return ret;
}

void os_event_destroy(os_event_t event)
{
#ifdef _WIN32
    CloseHandle(event);
#else
    os_mutex_free(&(event->os_mutex));
    pthread_cond_destroy(&(event->cond_var));
    free(event);
#endif
}

void os_event_wait(os_event_t event, uint64 reset_sig_count)
{
#ifdef _WIN32
    DWORD err;
    /* Specify an infinite time limit for waiting */
    err = WaitForSingleObject(event, INFINITE);
#else
    os_mutex_lock(&(event->os_mutex));

    if (!reset_sig_count) {
        reset_sig_count = event->signal_count;
    }

    while (!event->is_set && event->signal_count == reset_sig_count)
    {
        pthread_cond_wait(&(event->cond_var), &(event->os_mutex));

        /* Solaris manual said that spurious wakeups may occur: we have to check the 'is_set' variable again */
    }
    os_mutex_unlock(&(event->os_mutex));
#endif
}

int os_event_wait_time(
        os_event_t   event,  /* in: event to wait */
        uint32       microseconds,  /* in: timeout in microseconds, or  OS_SYNC_INFINITE_TIME */
        uint64       reset_sig_count)
{
#ifdef _WIN32
    DWORD err;
    /* Specify an infinite time limit for waiting */
    if (microseconds != OS_WAIT_INFINITE_TIME) {
        err = WaitForSingleObject(event, microseconds / 1000);
    } else {
        err = WaitForSingleObject(event, INFINITE);
    }

    if (err == WAIT_OBJECT_0) {
        return 0;
    } else if (err == WAIT_TIMEOUT) {
        return(OS_WAIT_TIME_EXCEEDED);
    } else {
        // err = WAIT_FAILED: GetLastError
        return OS_WAIT_TIME_FAIL;
    }
#else
    struct timespec abstime;
    int err = 0;
    struct timeval tv;

    if (microseconds != OS_WAIT_INFINITE_TIME) {
        gettimeofday(&tv,NULL);
        tv.tv_sec = tv.tv_sec + microseconds / 1000000;
        tv.tv_usec = tv.tv_usec + microseconds % 1000000;
        if (tv.tv_usec > 1000000)
        {
            tv.tv_sec += tv.tv_usec / 1000000;
            tv.tv_usec %= 1000000;
        }
        abstime.tv_sec = tv.tv_sec;
        abstime.tv_nsec = tv.tv_usec * 1000;
    }

    os_mutex_lock(&(event->os_mutex));

    if (!reset_sig_count) {
        reset_sig_count = event->signal_count;
    }

    do {
        if (event->is_set || reset_sig_count != event->signal_count) {
            break;
        }

        if (microseconds != OS_WAIT_INFINITE_TIME) {
            err = pthread_cond_timedwait(&(event->cond_var), &(event->os_mutex), &ts);
        } else {
            err = pthread_cond_wait(&(event->cond_var), &(event->os_mutex));
        }
    } while (ETIMEDOUT != err);  /* Solaris manual said that spurious wakeups may occur: 
                                    we have to check the 'is_set' variable again */

    os_mutex_unlock(&(event->os_mutex));

    return err ? OS_WAIT_TIME_EXCEEDED : 0);
#endif
}

void os_mutex_create(os_mutex_t *mutex)
{
#ifdef __WIN__
    InitializeCriticalSection((LPCRITICAL_SECTION)mutex);
#else
    pthread_mutex_init(mutex, NULL);
#endif
}

void os_mutex_enter(os_mutex_t *mutex)
{
#ifdef __WIN__
    EnterCriticalSection((LPCRITICAL_SECTION)mutex);
#else
    pthread_mutex_lock(mutex);
#endif
}

bool32 os_mutex_tryenter(os_mutex_t *mutex)
{
#ifdef __WIN__
    return TryEnterCriticalSection((LPCRITICAL_SECTION)mutex);
#else
    if (pthread_mutex_trylock(mutex) == 0) {
        return TRUE;
    }
#endif

    return FALSE;
}

void os_mutex_exit(os_mutex_t *mutex)
{
#ifdef __WIN__
    LeaveCriticalSection(mutex);
#else
    pthread_mutex_unlock(mutex);
#endif
}

void os_mutex_destroy(os_mutex_t *mutex)
{
#ifdef __WIN__
    DeleteCriticalSection((LPCRITICAL_SECTION)mutex);
#else
    pthread_mutex_destroy(mutex);
#endif
}

