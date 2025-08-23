#include "cm_random.h"
#include "cm_dbug.h"

#define UT_HASH_RANDOM_MASK 1463735687
#define UT_HASH_RANDOM_MASK2 1653893711

#define UT_RND1     151117737
#define UT_RND2     119785373
#define UT_RND3     85689495
#define UT_RND4     76595339
#define UT_SUM_RND2 98781234
#define UT_SUM_RND3 126792457
#define UT_SUM_RND4 63498502
#define UT_XOR_RND1 187678878
#define UT_XOR_RND2 143537923

static uint64 ut_rnd_uint64_counter = 65654363;

inline uint64 ut_rnd_gen_next_uint64(uint64 rnd) // in: the previous random number value
{
  uint64 n_bits;

  n_bits = 8 * sizeof(uint64);

  rnd = UT_RND2 * rnd + UT_SUM_RND3;
  rnd = UT_XOR_RND1 ^ rnd;
  rnd = (rnd << 20) + (rnd >> (n_bits - 20));
  rnd = UT_RND3 * rnd + UT_SUM_RND4;
  rnd = UT_XOR_RND2 ^ rnd;
  rnd = (rnd << 20) + (rnd >> (n_bits - 20));
  rnd = UT_RND1 * rnd + UT_SUM_RND2;

  return (rnd);
}

inline uint64 ut_rnd_gen_uint64()
{
  ut_rnd_uint64_counter = UT_RND1 * ut_rnd_uint64_counter + UT_RND2;
  return ut_rnd_gen_next_uint64(ut_rnd_uint64_counter);
}

inline uint64 ut_rnd_interval(
    uint64 low,  /*!< in: low limit; can generate also this value */
    uint64 high) /*!< in: high limit; can generate also this value */
{
  uint64 rnd;

  ut_ad(high >= low);

  if (low == high) {
    return (low);
  }

  rnd = ut_rnd_gen_uint64();

  return (low + (rnd % (high - low)));
}

inline uint32 ut_fold_uint32_pair(uint32 n1, uint32 n2)
{
    return(((((n1 ^ n2 ^ UT_HASH_RANDOM_MASK2) << 8) + n1) ^ UT_HASH_RANDOM_MASK) + n2);
}

inline uint32 ut_fold_binary(const byte *str, uint32 len)
{
    uint32  fold = 0;
    const byte* str_end = str + (len & 0xFFFFFFF8);

    ut_ad(str || !len);

    while (str < str_end) {
        fold = ut_fold_uint32_pair(fold, (uint32)(*str++));
        fold = ut_fold_uint32_pair(fold, (uint32)(*str++));
        fold = ut_fold_uint32_pair(fold, (uint32)(*str++));
        fold = ut_fold_uint32_pair(fold, (uint32)(*str++));
        fold = ut_fold_uint32_pair(fold, (uint32)(*str++));
        fold = ut_fold_uint32_pair(fold, (uint32)(*str++));
        fold = ut_fold_uint32_pair(fold, (uint32)(*str++));
        fold = ut_fold_uint32_pair(fold, (uint32)(*str++));
    }

    switch (len & 0x7) {
        case 7:
            fold = ut_fold_uint32_pair(fold, (uint32)(*str++));
        case 6:
            fold = ut_fold_uint32_pair(fold, (uint32)(*str++));
        case 5:
            fold = ut_fold_uint32_pair(fold, (uint32)(*str++));
        case 4:
            fold = ut_fold_uint32_pair(fold, (uint32)(*str++));
        case 3:
            fold = ut_fold_uint32_pair(fold, (uint32)(*str++));
        case 2:
            fold = ut_fold_uint32_pair(fold, (uint32)(*str++));
        case 1:
            fold = ut_fold_uint32_pair(fold, (uint32)(*str++));
    }

    return(fold);
}

inline uint32 ut_fold_string(const char* str)
{
    uint32 fold = 0;

    while (*str != '\0') {
        fold = ut_fold_uint32_pair(fold, (uint32)(*str));
        str++;
    }

    return fold;
}


