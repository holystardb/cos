#ifndef _CM_RANDOM_H
#define _CM_RANDOM_H

#include "cm_type.h"

#ifdef __cplusplus
extern "C" {
#endif

extern inline uint64 ut_rnd_gen_next_uint64(   uint64 rnd); /*!< in: the previous random number value */
extern inline uint64 ut_rnd_gen_uint64(void);
extern inline uint64 ut_rnd_interval(uint64 low, uint64 high);

extern inline uint32 ut_fold_binary(const byte *str, uint32 len);
extern inline uint32 ut_fold_uint32_pair(uint32 n1, uint32 n2);
extern inline uint32 ut_fold_string(const char* str);

#define ut_fold_uint64(d) ((uint32)d & 0xFFFFFFFF, (uint32)(d >> 32))


#ifdef __cplusplus
}
#endif

#endif   // _CM_RANDOM_H