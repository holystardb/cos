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
    return InterlockedCompareExchange64(ptr, newval, oldval) == oldval ? TRUE : FALSE;
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
    return InterlockedCompareExchange(ptr, newval, oldval) == oldval ? TRUE : FALSE;
}

#else  // __WIN__

typedef volatile int32 atomic32_t;
typedef volatile int128 atomic128_t;


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
    return __atomic_compare_exchange(ptr, &oldval, &newval, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST) == oldval ? TRUE : FALSE;
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
    return __atomic_compare_exchange(ptr, &oldval, &newval, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST) == oldval ? TRUE : FALSE;
}

typedef union {
    uint128   u128;
    uint64    u64[2];
    uint32    u32[4];
} duint128;

// Exclusive load/store 2 uint64_t variables to fullfil 128bit atomic compare and swap
static inline int128 __excl_compare_and_swap_u128(atomic128_t *ptr, duint128 oldval, duint128 newval)
{
    uint64 tmp, ret;
    duint128 old;

    asm volatile("1:     ldxp    %0, %1, %4\n"
                 "       eor     %2, %0, %5\n"
                 "       eor     %3, %1, %6\n"
                 "       orr     %2, %3, %2\n"
                 "       cbnz    %2, 2f\n"
                 "       stlxp   %w2, %7, %8, %4\n"
                 "       cbnz    %w2, 1b\n"
                 "       b 3f\n"
                 "2:"
                 "       stlxp   %w2, %0, %1, %4\n"
                 "       cbnz    %w2, 1b\n"
                 "3:"
                 "       dmb ish\n"
                 : "=&r"(old.u64[0]), "=&r"(old.u64[1]), "=&r"(ret), "=&r"(tmp), 
                   "+Q"(*ptr)
                 : "r"(oldval.u64[0]), "r"(oldval.u64[1]), "r"(newval.u64[0]), "r"(newval.u64[1])
                 : "memory");
    return old.u128;
}


/*
 * using CASP instinct to atomically compare and swap 2 uint64_t variables to fullfil
 * 128bit atomic compare and swap
 * */
static inline uint128 __lse_compare_and_swap_u128(atomic128_t *ptr, duint128 oldval, duint128 newval)
{                                                                               \
    duint128 old;                                                               \
    register unsigned long x0 asm ("x0") = oldval.u64[0];                       \
    register unsigned long x1 asm ("x1") = oldval.u64[1];                       \
    register unsigned long x2 asm ("x2") = newval.u64[0];                       \
    register unsigned long x3 asm ("x3") = newval.u64[1];                       \
                                                                                \
    asm volatile(".arch_extension lse\n"                                        \
    "   caspal    %[old_low], %[old_high], %[new_low], %[new_high], %[v]\n"     \
    : [old_low] "+&r" (x0), [old_high] "+&r" (x1),                              \
      [v] "+Q" (*(ptr))                                                         \
    : [new_low] "r" (x2), [new_high] "r" (x3)                                   \
    : "x30", "memory");                                                         \
                                                                                \
    old.u64[0] = x0;                                                            \
    old.u64[1] = x1;                                                            \
    return old.u128;
}

inline int128 atomic128_compare_and_swap(atomic128_t *ptr, duint128 oldval, duint128 newval)
{
#ifdef __ARM_LSE
    return __lse_compare_and_swap_u128(ptr, oldval, newval);
#else
    return __excl_compare_and_swap_u128(ptr, oldval, newval);
#endif
}


#else

inline int128 atomic128_get(atomic128_t *ptr)
{
    int128 oldval = 0, newval = 0;
    return __sync_val_compare_and_swap(ptr, oldval, newval);
}

inline bool32 atomic128_compare_and_swap(atomic128_t *ptr, int128 oldval, int128 newval)
{
    return __sync_val_compare_and_swap(ptr, oldval, newval) == oldval ? TRUE : FALSE;
}

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
    return __sync_bool_compare_and_swap(ptr, oldval, newval) == oldval ? TRUE : FALSE;
}

inline atomic32_t atomic32_get(atomic32_t *ptr)
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
    return __sync_bool_compare_and_swap(ptr, oldval, newval) == oldval ? TRUE : FALSE;
}

#endif

#endif

#ifdef __cplusplus
}
#endif // __cplusplus

#endif // _CM_ATOMIC_H_
