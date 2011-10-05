#include <string.h>
#include <stdio.h>
#include "hash.h"

long int
treewalk_filename_hash(char *in, unsigned char out[65])
{
    unsigned char digest[SHA256_DIGEST_LENGTH];
    int i = 0;

    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, in, strlen(in));
    SHA256_Final(digest, &ctx);

    for(i = 0; i < SHA256_DIGEST_LENGTH; i++)
    {
        sprintf((char*)out + (i * 2), "%02x", (char) digest[i]);
    }

    out[64] = 0;
    return strtol(digest,NULL,16);
}

/* EOF */
