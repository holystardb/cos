#ifndef _CM_ATOMIC_H_
#define _CM_ATOMIC_H_

#include "cm_type.h"

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus


typedef volatile int64 atomic64_t;

#ifdef __WIN__

typedef volatile LONG atomic32_t;

inline atomic64_t atomic64_get(atomic64_t *ptr)
{
    return InterlockedAdd64(ptr, 0);
}

inline atomic64_t atomic64_test_and_set(atomic64_t *ptr, int64 value)
{
    return InterlockedExchange64(ptr, value);
}

inline atomic64_t atomic64_inc(atomic64_t *ptr)
{
    return InterlockedIncrement64(ptr);
}

inline atomic64_t atomic64_dec(atomic64_t *ptr)
{
    return InterlockedDecrement64(ptr);
}

inline atomic64_t atomic64_add(atomic64_t *ptr, int64 val)
{
    return InterlockedAdd64(ptr, val);
}

inline bool32 atomic64_compare_and_swap(atomic64_t *ptr, int64 oldval, int64 newval)
{
    return InterlockedCompareExchange64(ptr, newval, oldval) == oldval ? true : false;
}

inline atomic32_t atomic32_get(atomic32_t *ptr)
{
    return InterlockedAdd(ptr, 0);
}

inline atomic32_t atomic32_test_and_set(atomic32_t *ptr, int32 value)
{
    return InterlockedExchange(ptr, value);
}

inline atomic32_t atomic32_inc(atomic32_t *ptr)
{
    return InterlockedIncrement(ptr);
}

inline atomic32_t atomic32_dec(atomic32_t *ptr)
{
    return InterlockedDecrement(ptr);
}

inline atomic32_t atomic32_add(atomic32_t *ptr, int32 val)
{
    return InterlockedAdd(ptr, val);
}

inline bool32 atomic32_compare_and_swap(atomic32_t *ptr, int32 oldval, int32 newval)
{
    return InterlockedCompareExchange(ptr, newval, oldval) == oldval ? true : false;
}

#else

typedef volatile int32 atomic32_t;


#if defined(__arm__) || defined(__aarch64__)

inline atomic64_t atomic64_get(atomic64_t *ptr)
{
    return __atomic_load_n(ptr, __ATOMIC_SEQ_CST);
}

inline atomic64_t atomic64_set(atomic64_t *ptr, int64 value)
{
    return __atomic_store_n(ptr, value, __ATOMIC_SEQ_CST);
}

inline atomic64_t atomic64_test_and_set(atomic64_t *ptr,          int64 newval)
{
    atomic64_t ret;

    /* Silence a compiler warning about unused ptr. */
    (void)ptr;
    __atomic_exchange(ptr, &newval, &ret, __ATOMIC_SEQ_CST);

    return ret;
}

inline atomic64_t atomic64_inc(atomic64_t *ptr)
{
    return __atomic_add_fetch(ptr, 1, __ATOMIC_SEQ_CST);
}

inline atomic64_t atomic64_dec(atomic64_t *ptr)
{
    return __atomic_add_fetch(ptr, -1, __ATOMIC_SEQ_CST);
}

inline atomic64_t atomic64_add(atomic64_t *ptr, int64 count)
{
    return __atomic_add_fetch(ptr, count, __ATOMIC_SEQ_CST);
}

inline bool32 atomic64_compare_and_swap(atomic64_t *ptr, int64 oldval, int64 newval)
{
    return __atomic_compare_exchange(ptr, &oldval, &newval, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST) == oldval ? true : false;
}

inline atomic32_t atomic32_get(atomic32_t *ptr)
{
    return __atomic_load_n(ptr, __ATOMIC_SEQ_CST);
}

inline atomic32_t atomic32_set(atomic32_t *ptr, int32 value)
{
    return __atomic_store_n(ptr, value, __ATOMIC_SEQ_CST);
}

inline uint32 atomic32_test_and_set(atomic32_t *ptr,          int32 newval)
{
    int32 ret;

    /* Silence a compiler warning about unused ptr. */
    (void)ptr;
    __atomic_exchange(ptr, &newval, &ret, __ATOMIC_SEQ_CST);

    return ret;
}

inline atomic32_t atomic32_inc(atomic32_t *ptr)
{
    return __atomic_add_fetch(ptr, 1, __ATOMIC_SEQ_CST);
}

inline atomic32_t atomic32_dec(atomic32_t *ptr)
{
    return __atomic_add_fetch(ptr, -1, __ATOMIC_SEQ_CST);
}

inline bool32 atomic32_compare_and_swap(atomic32_t *ptr, int32 oldval, int32 newval)
{
    return __atomic_compare_exchange(ptr, &oldval, &newval, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST) == oldval ? true : false;
}



#else

inline atomic64_t atomic64_get(atomic64_t *ptr)
{
    return __sync_add_and_fetch(ptr, 0);
}

inline atomic64_t atomic64_test_and_set(atomic64_t *ptr, int64 newval) 
{
    return __sync_lock_test_and_set(ptr, newval);
}

inline atomic64_t atomic64_inc(atomic64_t *ptr)
{
    return __sync_add_and_fetch(ptr, 1);
}

inline atomic64_t atomic64_dec(atomic64_t *ptr)
{
    return __sync_add_and_fetch(ptr, -1);
}

inline atomic64_t atomic64_add(atomic64_t *ptr, int64 count)
{
    return __sync_add_and_fetch(ptr, count);
}

inline bool32 atomic64_compare_and_swap(atomic64_t *ptr, int64 oldval, int64 newval)
{
    return __sync_bool_compare_and_swap(ptr, oldval, newval) == oldval ? true : false;
}

inline atomic32_t atomic32_get(atomic64_t *ptr)
{
    return __sync_add_and_fetch(ptr, 0);
}

inline atomic32_t atomic32_test_and_set(atomic32_t *ptr, int32 newval) 
{
    return __sync_lock_test_and_set(ptr, newval);
}

inline atomic32_t atomic32_inc(atomic32_t *ptr)
{
    return __sync_add_and_fetch(ptr, 1);
}

inline atomic32_t atomic32_dec(atomic32_t *ptr)
{
    return __sync_add_and_fetch(ptr, -1);
}

inline atomic32_t atomic32_add(atomic32_t *ptr, int32 count)
{
    return __sync_add_and_fetch(ptr, count);
}

inline bool32 atomic32_compare_and_swap(atomic32_t *ptr, int32 oldval, int32 newval)
{
    return __sync_bool_compare_and_swap(ptr, oldval, newval) == oldval ? true : false;
}

#endif

#endif

#ifdef __cplusplus
}
#endif // __cplusplus

#endif // _CM_ATOMIC_H_
