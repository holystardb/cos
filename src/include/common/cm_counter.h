#ifndef _CM_COUNTER_H
#define _CM_COUNTER_H

#include "cm_random.h"
#include "cm_dbug.h"

/** Default number of slots to use in counter_t */
#define COUNTER_SLOTS_64        64
#define COUNTER_SLOTS_128       128
#define COUNTER_SLOTS_256       256
#define COUNTER_SLOTS_512       512


/** Class for using fuzzy counters.
The counter is not protected by any mutex and the results are not guaranteed to be 100% accurate but close enough.*/

template <typename Type, int N = COUNTER_SLOTS_64>
class counter_t {
public:
    counter_t() { memset(m_counter, 0x0, sizeof(m_counter)); }
    ~counter_t() { }

    /** If you can't use a good index id. Increment by 1. */
    void inc()  { add(1); }

    /** If you can't use a good index id. @param n is the amount to increment */
    void add(Type n) {
        size_t i = offset(ut_rnd_interval(1, N));
        ut_ad(i < UT_ARR_SIZE(m_counter));
        m_counter[i] += n;
    }

    /** Use this if you can use a unique identifier.
    @param index index into a slot
    @param n amount to increment */
    void add(size_t index, Type n) {
        size_t i = offset(index);
        ut_ad(i < UT_ARR_SIZE(m_counter));
        m_counter[i] += n;
    }

    /** If you can't use a good index id. Decrement by 1. */
    void dec()  { sub(1); }

    /** If you can't use a good index id.
    @param n the amount to decrement */
    void sub(Type n) {
        size_t i = offset(ut_rnd_interval(1, N));
        ut_ad(i < UT_ARR_SIZE(m_counter));
        m_counter[i] -= n;
    }

    /** Use this if you can use a unique identifier.
    @param index index into a slot
    @param n amount to decrement */
    void sub(size_t index, Type n) {
        size_t i = offset(index);
        ut_ad(i < UT_ARR_SIZE(m_counter));
        m_counter[i] -= n;
    }

    /* @return total value - not 100% accurate, since it is not atomic. */
    operator Type() const {
        Type total = 0;
        for (size_t i = 0; i < N; ++i) {
            total += m_counter[offset(i)];
        }
        return (total);
    }

    Type operator[](size_t index) const {
        size_t i = offset(index);
        ut_ad(i < UT_ARR_SIZE(m_counter));
        return (m_counter[i]);
    }

private:

    /** @return offset within m_counter */
    static size_t offset(size_t index)  {
        return (((index % N) + 1) * sizeof(Type));
    }

private:

    /** Slot 0 is unused. */
    Type m_counter[(N + 1) * sizeof(Type)];
};

#endif /* _CM_COUNTER_H */
