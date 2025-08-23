#include "cm_type.h"
#include "cm_memory.h"
#include "cm_encrypt.h"

void aes_cbc()
{
    printf("\n**************** aes_cbc *****************\n");

    aes_cypher_t aes;

    char out1[256], out2[256];
    char in[256];
    char *key = "simplekey";
    char *iv = "0123456789012345";
    int len;

    strncpy_s(in, 256, "0123456789012345678901234567890123456789", 40);

    aes_set_key(&aes, (uint8 *)key, (uint32)strlen(key), AES_CYPHER_128, (uint8 *)iv);
    aes_padding_buf((uint8 *)in, 19, 256);
    printf("%s\n", in);
    len = aes_get_cypher_len((int)strlen(in));
    aes_encrypt_cbc(&aes, (uint8*)in, len, (uint8 *)out1, len);
    memset(out2, 0x00, 256);
    aes_decrypt_cbc(&aes, (uint8*)out1, len, (uint8 *)out2, len);
    printf("%s\n", out2);
}

void aes()
{
    printf("\n**************** aes *****************\n");

    aes_cypher_t aes;

    char out1[256], out2[256];
    char *in = "0123456789012345678";
    char *key = "simplekey";
    char *iv = "0123456789012345";

    aes_set_key(&aes, (uint8 *)key, (uint32)strlen(key), AES_CYPHER_128, (uint8 *)iv);

    printf("%s\n", in);
    aes_encrypt(&aes, (uint8*)in, (uint32)strlen(in), (uint8 *)out1, 256);
    memset(out2, 0x00, 256);
    aes_decrypt(&aes, (uint8*)out1, aes_get_cypher_len((uint32)strlen(in)), (uint8 *)out2, 256);
    printf("%s\n", out2);
}

int main22(int argc, char *argv[])
{
    aes();
    aes_cbc();

    return 0;
}
