#include "cm_random.h"
#include "cm_dbug.h"

#define UT_HASH_RANDOM_MASK 1463735687
#define UT_HASH_RANDOM_MASK2 1653893711

#define UT_RND1 151117737
#define UT_RND2 119785373
#define UT_RND3 85689495
#define UT_RND4 76595339
#define UT_SUM_RND2 98781234
#define UT_SUM_RND3 126792457
#define UT_SUM_RND4 63498502
#define UT_XOR_RND1 187678878
#define UT_XOR_RND2 143537923

THREAD_LOCAL uint64 ut_rnd_uint64_counter = 0;


uint64 ut_rnd_gen_next_uint64(   uint64 rnd) /*!< in: the previous random number value */
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

uint64 ut_rnd_gen_uint64() {
  uint64 rnd = ut_rnd_uint64_counter;
  if (rnd == 0) {
    rnd = 65654363;
  }

  rnd = UT_RND1 * rnd + UT_RND2;
  ut_rnd_uint64_counter = rnd;

  return (ut_rnd_gen_next_uint64(rnd));
}

uint64 ut_rnd_interval(
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


