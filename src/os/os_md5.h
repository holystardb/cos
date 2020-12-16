#ifndef _OS_MD5_H
#define _OS_MD5_H

#include "os_type.h"

#define MD5 5

#ifndef MD
  #define MD MD5
#endif

#ifndef PROTOTYPES
  #define PROTOTYPES 1
#endif

#if PROTOTYPES
  #define PROTO_LIST(list) list
#else
  #define PROTO_LIST(list) ()
#endif

typedef struct
{
  UINT32 state[4];
  UINT32 count[2];
  UCHAR buffer[64];
} MD5_CTX;

void MD5Init PROTO_LIST ((MD5_CTX* Md5_ctx));
void MD5Update PROTO_LIST ((MD5_CTX* Md5_ctx, PUCHAR Input, UINT32 Legth));
void MD5Final PROTO_LIST ((UCHAR Digest[16], MD5_CTX* Md5_ctx));

int MDString (char* In, char* Out);

#endif

