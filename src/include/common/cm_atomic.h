#ifndef _CM_ATOMIC_H_
#define _CM_ATOMIC_H_

#include "cm_type.h"

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

#ifdef __WIN__

typedef volatile long atomic32_t;
typedef volatile int64 atomic_t;

inline atomic_t atomic_get(atomic_t *ptr)
{
    return InterlockedAdd64(ptr, 0);
}

inline atomic_t atomic_set(atomic_t *ptr, int64 value)
{
    return InterlockedExchange64(ptr, value);
}

inline atomic_t atomic_inc(atomic_t *ptr)
{
    return InterlockedIncrement64(ptr);
}

inline atomic_t atomic_dec(atomic_t *ptr)
{
    return InterlockedDecrement64(ptr);
}

inline bool32 atomic_cas(atomic_t *ptr, int64 oldval, int64 newval)
{
    return InterlockedCompareExchange64(ptr, newval, oldval) == oldval ? true : false;
}

inline atomic_t atomic32_inc(atomic32_t *ptr)
{
    return InterlockedIncrement(ptr);
}

inline atomic_t atomic32_dec(atomic32_t *ptr)
{
    return InterlockedDecrement(ptr);
}

inline atomic_t atomic_add(atomic_t *ptr, int64 count)
{
    return InterlockedAdd64(ptr, count);
}

inline bool32 atomic32_cas(atomic32_t *ptr, int32 oldval, int32 newval)
{
    return InterlockedCompareExchange(ptr, newval, oldval) == oldval ? true : false;
}

#else

typedef volatile int32 atomic32_t;
typedef volatile int64 atomic_t;

#if defined(__arm__) || defined(__aarch64__)

inline atomic_t atomic_get(atomic_t *ptr)
{
    return __atomic_load_n(ptr, __ATOMIC_SEQ_CST);
}

inline atomic_t atomic_set(atomic_t *ptr, int64 value)
{
    return __atomic_store_n(ptr, value, __ATOMIC_SEQ_CST);
}

inline atomic_t atomic_inc(atomic_t *ptr)
{
    return __atomic_add_fetch(ptr, 1, __ATOMIC_SEQ_CST);
}

inline atomic_t atomic_dec(atomic_t *ptr)
{
    return __atomic_add_fetch(ptr, -1, __ATOMIC_SEQ_CST);
}

inline atomic_t atomic_add(atomic_t *ptr, int64 count)
{
    return __atomic_add_fetch(ptr, count, __ATOMIC_SEQ_CST);
}

inline bool32 atomic_cas(atomic_t *ptr, int64 oldval, int64 newval)
{
    return __atomic_compare_exchange(ptr, &oldval, &newval, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
}

inline atomic_t atomic32_inc(atomic32_t *ptr)
{
    return __atomic_add_fetch(ptr, 1, __ATOMIC_SEQ_CST);
}

inline atomic_t atomic32_dec(atomic32_t *ptr)
{
    return __atomic_add_fetch(ptr, -1, __ATOMIC_SEQ_CST);
}

inline bool32 atomic32_cas(atomic32_t *ptr, int32 oldval, int32 newval)
{
    return __atomic_compare_exchange(ptr, &oldval, &newval, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
}

#else

inline atomic_t atomic_get(atomic_t *ptr)
{
    return *ptr;
}

inline atomic_t atomic_set(atomic_t *ptr, int64 value)
{
    return *ptr = value;
}

inline atomic_t atomic_inc(atomic_t *ptr)
{
    return __sync_add_and_fetch(ptr, 1);
}

inline atomic_t atomic_dec(atomic_t *ptr)
{
    return __sync_add_and_fetch(ptr, -1);
}

inline atomic_t atomic_add(atomic_t *ptr, int64 count)
{
    return __sync_add_and_fetch(ptr, count);
}

inline bool32 atomic_cas(atomic_t *ptr, int64 oldval, int64 newval)
{
    return __sync_bool_compare_and_swap(ptr, oldval, newval);
}

inline atomic_t atomic32_inc(atomic32_t *ptr)
{
    return __sync_add_and_fetch(ptr, 1);
}

inline atomic_t atomic32_dec(atomic32_t *ptr)
{
    return __sync_add_and_fetch(ptr, -1);
}

inline bool32 atomic32_cas(atomic32_t *ptr, int32 oldval, int32 newval)
{
    return __sync_bool_compare_and_swap(ptr, oldval, newval);
}

#endif

#endif

#ifdef __cplusplus
}
#endif // __cplusplus

#endif // _CM_ATOMIC_H_
