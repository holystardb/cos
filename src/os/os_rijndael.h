#ifndef _OS_RIJNDAEL_H
#define _OS_RIJNDAEL_H

#include "os_type.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    AES_CYPHER_128,
    AES_CYPHER_192,
    AES_CYPHER_256,
} AES_CYPHER_T;

int aes_encrypt_ecb(AES_CYPHER_T mode, uint8 *data, int len, uint8 *key);
int aes_decrypt_ecb(AES_CYPHER_T mode, uint8 *data, int len, uint8 *key);
int aes_encrypt_cbc(AES_CYPHER_T mode, uint8 *data, int len, uint8 *key, uint8 *iv);
int aes_decrypt_cbc(AES_CYPHER_T mode, uint8 *data, int len, uint8 *key, uint8 *iv);

#ifdef __cplusplus
};
#endif

#endif