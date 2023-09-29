#ifndef _CM_RANDOM_H
#define _CM_RANDOM_H

#include "cm_type.h"

#ifdef __cplusplus
extern "C" {
#endif

uint64 ut_rnd_gen_next_uint64(   uint64 rnd); /*!< in: the previous random number value */
uint64 ut_rnd_gen_uint64(void);
uint64 ut_rnd_interval(uint64 low, uint64 high);

uint32 ut_fold_binary(const byte *str, uint32 len);
uint32 ut_fold_ulint_pair(uint32 n1, uint32 n2);

#ifdef __cplusplus
}
#endif

#endif   // _CM_RANDOM_H