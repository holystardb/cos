#ifndef _CM_ENCRYPT_H
#define _CM_ENCRYPT_H

#include "cm_type.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    AES_CYPHER_128,
    AES_CYPHER_192,
    AES_CYPHER_256,
} aes_cypher_bits;

typedef struct st_aes_cypher {
    unsigned char key[32];
    int bits;
    unsigned char ivec[16];
} aes_cypher_t;

uint32 aes_get_padding_len(uint32 in_len);
uint32 aes_get_cypher_len(uint32 in_len);
bool32 aes_padding_buf(uint8 *buf, uint32 data_len, uint32 buf_size);

bool32 aes_set_key(aes_cypher_t *aes, uint8 *key, uint32 key_len, aes_cypher_bits bits, uint8 *iv);

bool32 aes_encrypt(aes_cypher_t *aes, uint8 *in, uint32 in_len, uint8 *out, uint32 out_len);
bool32 aes_decrypt(aes_cypher_t *aes, uint8 *in, uint32 in_len, uint8 *out, uint32 out_len);

bool32 aes_encrypt_cbc(aes_cypher_t *aes, uint8 *in, uint32 in_len, uint8 *out, uint32 out_len);
bool32 aes_decrypt_cbc(aes_cypher_t *aes, uint8 *in, uint32 in_len, uint8 *out, uint32 out_len);

#ifdef __cplusplus
};
#endif

#endif