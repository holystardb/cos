#include "cm_encrypt.h"
#include "openssl\aes.h"

uint32 aes_get_padding_len(uint32 in_len)
{
    return (in_len % AES_BLOCK_SIZE) == 0 ? 0 : AES_BLOCK_SIZE - (in_len % AES_BLOCK_SIZE);
}

uint32 aes_get_cypher_len(uint32 in_len)
{
    return (in_len % AES_BLOCK_SIZE) == 0 ? in_len : in_len + AES_BLOCK_SIZE - (in_len % AES_BLOCK_SIZE);
}

bool32 aes_padding_buf(uint8 *buf, uint32 data_len, uint32 buf_size)
{
    uint32 padding_size = aes_get_cypher_len(data_len);
    if (data_len == 0 || padding_size > buf_size) {
        return FALSE;
    }
    if (padding_size > data_len) {
        memset(buf + data_len, 0x00, padding_size - data_len);
    }

    return TRUE;
}

bool32 aes_set_key(aes_cypher_t *aes, uint8 *key, uint32 key_len, aes_cypher_bits bits, uint8 *iv)
{
    switch (bits) {
    case AES_CYPHER_128:
        aes->bits = 128;
        break;
    case AES_CYPHER_192:
        aes->bits = 192;
        break;
    case AES_CYPHER_256:
        aes->bits = 256;
        break;
    default:
        return FALSE;
    }

    if (key == NULL || key_len > 32) {
        return FALSE;
    }
    memset(aes->key, 0x00, 32);
    memcpy(aes->key, key, key_len);

    if (iv) {
        memcpy(aes->ivec, iv, 16);
    } else {
        for (uint32 i = 0; i < 16; i++) {
            aes->ivec[i] = i;
        }
    }

    return TRUE;
}

bool32 aes_encrypt(aes_cypher_t *aes, uint8 *in, uint32 in_len, uint8 *out, uint32 out_len)
{
    AES_KEY aes_key;
    uint32 pos = 0;
    unsigned char raw[16];

    if (in_len == 0 || aes_get_cypher_len(in_len) > out_len) {
        return FALSE;
    }

    AES_set_encrypt_key(aes->key, aes->bits, &aes_key);

    while (pos + 16 <= in_len) {
        AES_encrypt(in + pos, out + pos, &aes_key);
        pos += 16;
    }

    if (pos == in_len) {
        return TRUE;
    }
    
    memcpy(raw, in + pos, in_len - pos);
    memset(raw + in_len - pos, 0x00, 16 - (in_len - pos));
    AES_encrypt(raw, out + pos, &aes_key);

    return TRUE;
}

bool32 aes_decrypt(aes_cypher_t *aes, uint8 *in, uint32 in_len, uint8 *out, uint32 out_len)
{
    AES_KEY aes_key;
    uint32 pos = 0;

    if (in_len == 0 || (in_len % 16) != 0 || in_len > out_len) {
        return FALSE;
    }

    AES_set_decrypt_key(aes->key, aes->bits, &aes_key);

    while (pos + 16 <= in_len) {
        AES_decrypt(in + pos, out + pos, &aes_key);
        pos += 16;
    }

    return TRUE;
}

bool32 aes_encrypt_cbc(aes_cypher_t *aes, uint8 *in, uint32 in_len, uint8 *out, uint32 out_len)
{
    AES_KEY aes_key;
    unsigned char ivec[16];

    if (in_len == 0 || (in_len % 16) != 0 || in_len > out_len) {
        return FALSE;
    }

    AES_set_encrypt_key(aes->key, aes->bits, &aes_key);
    memcpy(ivec, aes->ivec, 16);

    AES_cbc_encrypt(in, out, in_len, &aes_key, ivec, AES_ENCRYPT);
    
    return TRUE;
}

bool32 aes_decrypt_cbc(aes_cypher_t *aes, uint8 *in, uint32 in_len, uint8 *out, uint32 out_len)
{
    AES_KEY aes_key;
    unsigned char ivec[16];

    if (in_len == 0 || (in_len % 16) != 0 || in_len > out_len) {
        return FALSE;
    }

    AES_set_decrypt_key(aes->key, aes->bits, &aes_key);
    memcpy(ivec, aes->ivec, 16);

    AES_cbc_encrypt(in, out, in_len, &aes_key, ivec, AES_DECRYPT);

    return TRUE;
}