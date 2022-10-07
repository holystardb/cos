#ifndef _CM_MD5_H
#define _CM_MD5_H

#include "cm_type.h"

#ifdef __cplusplus
extern "C" {
#endif

// MD5 outputs a 16 byte digest
#define MD5_BLOCK_SIZE       16

typedef struct {
   uint8 data[64];
   uint32 datalen;
   unsigned long long bitlen;
   uint32 state[4];
} MD5_CTX;

/*********************** FUNCTION DECLARATIONS **********************/
void md5_init(MD5_CTX *ctx);
void md5_update(MD5_CTX *ctx, uint8 data[], uint32 len);
void md5_final(MD5_CTX *ctx, uint8 hash[]);

#ifdef __cplusplus
}
#endif

#endif   // _CM_MD5_H