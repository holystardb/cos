#include "cm_mutex.h"

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

void os_event_reset(os_event_t   event)
{
#ifdef _WIN32
    ResetEvent(event);
#else
    os_mutex_lock(&(event->os_mutex));
    if (!event->is_set) {
        /* Do nothing */
    } else {
        event->is_set = FALSE;
    }
    os_mutex_unlock(&(event->os_mutex));
#endif
}

void os_event_free(os_event_t event)
{
#ifdef _WIN32
    CloseHandle(event);
#else
    os_mutex_free(&(event->os_mutex));
    pthread_cond_destroy(&(event->cond_var));
    free(event);
#endif
}

void os_event_wait(os_event_t event)
{
#ifdef _WIN32
    DWORD err;
    /* Specify an infinite time limit for waiting */
    err = WaitForSingleObject(event, INFINITE);
#else
    os_mutex_lock(&(event->os_mutex));
loop:
    if (event->is_set == TRUE)
    {
        os_mutex_unlock(&(event->os_mutex));
        return;
    }
    pthread_cond_wait(&(event->cond_var), &(event->os_mutex));
    /* Solaris manual said that spurious wakeups may occur: 
           we have to check the 'is_set' variable again */
    goto loop;
#endif
}

int os_event_wait_time(
        os_event_t  event,  /* in: event to wait */
        unsigned int microseconds)  /* in: timeout in microseconds, or  OS_SYNC_INFINITE_TIME */
{
#ifdef _WIN32
    DWORD err;
    /* Specify an infinite time limit for waiting */
    if (microseconds != OS_SYNC_INFINITE_TIME) {
        err = WaitForSingleObject(event, microseconds / 1000);
    } else {
        err = WaitForSingleObject(event, INFINITE);
    }

    if (err == WAIT_OBJECT_0) {
        return(0);
    } else if (err == WAIT_TIMEOUT) {
        return(OS_SYNC_TIME_EXCEEDED);
    } else {
        // err = WAIT_FAILED: GetLastError
        return OS_SYNC_TIME_FAIL;
    }
#else
    struct timespec ts;
    int err = 0;
    struct timeval tv;

    os_mutex_lock(&(event->os_mutex));
loop:
    if (event->is_set == TRUE)
    {
        os_mutex_unlock(&(event->os_mutex));
        return 0;
    }

    gettimeofday(&tv,NULL);
    tv.tv_sec = tv.tv_sec + microseconds / 1000000;
    tv.tv_usec = tv.tv_usec + microseconds % 1000000;
    if (tv.tv_usec > 1000000)
    {
        tv.tv_sec += tv.tv_usec / 1000000;
        tv.tv_usec %= 1000000;
    }
    ts.tv_sec = tv.tv_sec;
    ts.tv_nsec = tv.tv_usec * 1000;
    err = pthread_cond_timedwait(&(event->cond_var), &(event->os_mutex), &ts);
    if (ETIMEDOUT == err)
    {
        os_mutex_unlock(&(event->os_mutex));
        return(OS_SYNC_TIME_EXCEEDED);
    }
    /* Solaris manual said that spurious wakeups may occur: 
           we have to check the 'is_set' variable again */
    goto loop;

    return(0);
#endif
}


void os_mutex_init(os_mutex_t* mutex)
{
#ifdef __WIN__
    InitializeCriticalSection((LPCRITICAL_SECTION)mutex);
#else
    pthread_mutex_init(mutex, NULL);
#endif
}

void os_mutex_lock(os_mutex_t* mutex)
{
#ifdef __WIN__
    EnterCriticalSection((LPCRITICAL_SECTION)mutex);
#else
    pthread_mutex_lock(mutex);
#endif
}

void os_mutex_unlock(os_mutex_t* mutex)
{
#ifdef __WIN__
    LeaveCriticalSection(mutex);
#else
    pthread_mutex_unlock(mutex);
#endif
}

void os_mutex_free(os_mutex_t* mutex)
{
#ifdef __WIN__
    DeleteCriticalSection((LPCRITICAL_SECTION)mutex);
#else
    pthread_mutex_destroy(mutex);
#endif
}






/***********************************************************************************************
*                                     spin lock                                                *
***********************************************************************************************/

#ifdef __WIN__
__declspec(thread) uint64 g_tls_spin_sleeps = 0;
#else
__thread uint64 g_tls_spin_sleeps = 0;
#endif

void spin_sleep_and_stat(spin_statis_t *stat)
{

}


