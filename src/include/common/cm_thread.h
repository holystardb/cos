#ifndef _CM_THREAD_H
#define _CM_THREAD_H

#include "cm_type.h"
#include <memory>
#include <system_error>
#include <tuple>

#ifdef __cplusplus
extern "C" {
#endif


#ifdef __WIN__
typedef void*                     os_thread_t;
typedef DWORD                     os_thread_id_t;
typedef DWORD                     os_thread_local_key_t;
typedef uint32                    os_thread_ret_t;
#define INVALID_OS_THREAD         NULL
#define OS_THREAD_DUMMY_RETURN    return(0)
#else
typedef pthread_t                 os_thread_t;
typedef uint32                    os_thread_id_t;
typedef pthread_key_t             os_thread_local_key_t;
typedef void*                     os_thread_ret_t;
#define INVALID_OS_THREAD         0
#define OS_THREAD_DUMMY_RETURN    return(NULL)
#endif


#ifdef __WIN__
/* In the Win32 API, the x86 PAUSE instruction is executed by calling
the YieldProcessor macro defined in WinNT.h. It is a CPU architecture-
independent way by using YieldProcessor. */
#define OS_RELAX_CPU()           YieldProcessor()
#elif defined(__arm__) || defined(__aarch64__)
#define OS_RELAX_CPU()           __asm__ __volatile__("rep; nop")
#else
#define OS_RELAX_CPU()           __asm__ __volatile__("pause")
#endif  // __WIN__



inline int os_create_thread_local_key(os_thread_local_key_t *key,            void (*destructor)(void *))
{
#ifdef __WIN__
    *key = TlsAlloc();
    return (*key == TLS_OUT_OF_INDEXES);
#else
    return pthread_key_create(key, destructor);
#endif
}

inline int os_delete_thread_local_key(os_thread_local_key_t key)
{
#ifdef __WIN__
    return !TlsFree(key);
#else
    return pthread_key_delete(key);
#endif
}

inline void* os_get_thread_local(os_thread_local_key_t key)
{
#ifdef __WIN__
    return TlsGetValue(key);
#else
    return pthread_getspecific(key);
#endif
}

inline int os_set_thread_local(os_thread_local_key_t key, void *value)
{
#ifdef __WIN__
    return !TlsSetValue(key, value);
#else
    return pthread_setspecific(key, value);
#endif
}

extern os_thread_t os_thread_create(
    void*            (*start_f)(void*),  // in: pointer to function from which to start
    void*            arg,    // in: argument to start function
    os_thread_id_t*  thread_id);    // out: id of created thread
extern void os_thread_exit(void* exit_value);

extern bool32 os_thread_join(os_thread_t thread);
extern inline bool32 os_thread_eq(os_thread_id_t a, os_thread_id_t b);
extern inline bool32 os_thread_is_valid(os_thread_t thread);
extern inline os_thread_id_t os_thread_get_curr_id(void);
extern inline os_thread_t os_thread_get_curr(void);
extern inline uint32 os_thread_get_last_error(void);

extern void os_thread_yield(void);
extern void os_thread_sleep(unsigned int microseconds);
extern uint64 os_thread_delay(uint64 delay);

extern void os_thread_set_internal_id();
extern inline uint64 os_thread_get_internal_id();

extern THREAD_LOCAL uint64 g_thread_internal_id;
#include "cm_thread.ic"

#ifdef __cplusplus
}
#endif


/***************************************************************************************
*                                thread template                                       *
***************************************************************************************/

template<typename Function, typename Args, size_t ... N>
auto invokeImpl(Function func, Args args, std::index_sequence<N...>)
{
    // To implement a callback function, the parameter packets in std:: tuple need to be unpacked.
    return func(std::get<N>(args) ...);
}

template<typename Function, typename Args>
auto invoke(Function func, Args args)
{
    // Get the size of the parameter tuple
    static constexpr auto size = std::tuple_size<Args>::value;
    // Build parameters to access tuple containers
    return invokeImpl(func, args, std::make_index_sequence<size>{});
}

template<typename Function, typename... Args>
class wrapper {
public:
    wrapper(Function &&function, Args&&... arguments)
        : func_(std::forward<Function>(function))
        , args_(std::forward<Args>(arguments)...)
    {
    }

    static void* MY_ATTRIBUTE(__stdcall) start_routine(void* lpThreadParameter);

public:
    Function func_;
    std::tuple<Args...> args_;
};

template<typename Function, typename... Args>
void* MY_ATTRIBUTE(__stdcall) wrapper<Function, Args...>::start_routine(void* lpThreadParameter)
{
    std::unique_ptr<wrapper<Function, Args...>> pw(reinterpret_cast<wrapper<Function, Args...>*>(lpThreadParameter));
    invoke(pw->func_, pw->args_);
    return NULL;
}

template<typename Function, typename... Args>
os_thread_t thread_start(Function&& func, Args&&... args)
{
    os_thread_t thd;

    auto w = new wrapper<Function, Args...>(std::forward<Function>(func), std::forward<Args>(args)...);
    thd = os_thread_create(w->start_routine, static_cast<void*>(w), NULL);
    if (!thd) {
        delete w;
    }

    return thd;
}

#endif  /* _CM_THREAD_H */

