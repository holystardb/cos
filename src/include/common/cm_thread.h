#ifndef _CM_THREAD_H
#define _CM_THREAD_H

#include "cm_type.h"

#ifdef __cplusplus
extern "C" {
#endif


/* Maximum number of threads which can be created in the program */
#define	OS_THREAD_MAX_N		1000

/* Possible fixed priorities for threads */
#define OS_THREAD_PRIORITY_NONE		100
#define OS_THREAD_PRIORITY_BACKGROUND	1
#define OS_THREAD_PRIORITY_NORMAL	2
#define OS_THREAD_PRIORITY_ABOVE_NORMAL	3

#ifdef __WIN__
typedef void*                            os_thread_t;
#define os_thread_key_t(type)            _declspec(thread) type
#else
typedef pthread_t                        os_thread_t;
#define os_thread_key_t(type)            pthread_key_t
#endif

typedef unsigned long int   os_thread_id_t;

#ifdef __WIN__
#define os_thread_get_specific(key, type, value)    \
    do {                                            \
        value = key;                                \
    } while (0)
#else
#define os_thread_get_specific(key, type, value)    \
    do {                                            \
        void *ptr = pthread_getspecific(key);       \
        if(ptr != NULL) {                          \
            value = *(type *)ptr;                   \
        }                                           \
    } while (0)
#endif


#ifdef __WIN__
#define os_thread_set_specific(key, value)          \
    do {                                            \
        key = value;                                \
    } while (0)
#else
#define os_thread_set_specific(key, value)          \
    do {                                            \
        pthread_setspecific(key, (void *)&value);   \
    } while (0)
#endif



os_thread_t os_thread_create(
    void*            (*start_f)(void*),    /* in: pointer to function from which to start */
    void*            arg,    /* in: argument to start function */
    os_thread_id_t*  thread_id);    /* out: id of created 	thread */	

bool32 os_is_valid_thread(os_thread_t thread);

os_thread_id_t os_thread_get_curr_id(void);

os_thread_t os_thread_get_curr(void);

void os_thread_yield(void);

void os_thread_sleep(unsigned int microseconds);

unsigned int os_thread_get_last_error(void);

#ifdef __cplusplus
}
#endif

#endif  /* _CM_THREAD_H */

