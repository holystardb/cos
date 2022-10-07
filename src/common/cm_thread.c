
#include "cm_thread.h"



/*Returns the thread identifier of current thread. */
os_thread_id_t os_thread_get_curr_id(void)
/*=======================*/
{
#ifdef __WIN__
	return(GetCurrentThreadId());
#else
	pthread_t    pthr;
	pthr = pthread_self();
	/* TODO: in the future we have to change os_thread_id
	   to pthread_t; the following cast may work in a wrong way on some
	   systems if pthread_t is a struct; this is just a quick fix
	   for HP-UX to eliminate a compiler warning */
	return(*(os_thread_id_t*)((void*) (&pthr)));
#endif
}

os_thread_t os_thread_create(
    void*               (*start_f)(void*),     /* in: pointer to function  	from which to start */
    void*               arg,        /* in: argument to start function */
    os_thread_id_t     *thread_id)  /* out: id of created	thread */	
    {
#ifdef __WIN__
    os_thread_t	thread;
    thread = CreateThread(NULL, /* no security attributes */
                          0,    /* default size stack */
                          (LPTHREAD_START_ROUTINE)start_f,
                          arg,
                          0,  /* thread runs immediately */
                          thread_id);
    return(thread);
#else
    os_thread_t     pthread;
    pthread_attr_t  attr;
    
    pthread_attr_init(&attr);
    pthread_create(&pthread, &attr, start_f, arg);
    pthread_attr_destroy(&attr);
    return(pthread);
#endif
}

bool32 os_is_valid_thread(os_thread_t thread)
{
#ifdef __WIN__
    return (thread == NULL) ? FALSE : TRUE;
#else
    return (thread == 0) ? FALSE : TRUE;
#endif
}

/*Returns handle to the current thread. */
os_thread_t os_thread_get_curr(void)
{
#ifdef __WIN__
	return(GetCurrentThread());
#else
	return(pthread_self());
#endif
}

	
/*Advises the os to give up remainder of the thread's time slice. */
void os_thread_yield(void)
{
#if defined(__WIN__)
	Sleep(0);
	/*
#elif (defined(HAVE_SCHED_YIELD) && defined(HAVE_SCHED_H))
        sched_yield();
#elif defined(HAVE_PTHREAD_YIELD_ZERO_ARG)
	pthread_yield();
#elif defined(HAVE_PTHREAD_YIELD_ONE_ARG)
	pthread_yield(0);*/
#else
    os_thread_sleep(0);
#endif
}

/*The thread sleeps at least the time given in microseconds. */
void os_thread_sleep(unsigned int microseconds)	/* in: time in microseconds */
{
#ifdef __WIN__
    if (microseconds < 1000) {
        Sleep(1);
    } else {
        Sleep(microseconds / 1000);
    }
#else
	struct timeval	t;
	t.tv_sec = microseconds / 1000000;
	t.tv_usec = microseconds % 1000000;
	select(0, NULL, NULL, NULL, &t);
#endif
}

/*Gets the last operating system error code for the calling thread. */
unsigned int os_thread_get_last_error(void)
{
#ifdef __WIN__
	return(GetLastError());
#else
	return(0);
#endif
}

