#include <string.h>
#include "hash.h"

void
treewalk_filename_hash(char *in, char out[65])
{
    unsigned char digest[SHA256_DIGEST_LENGTH];
    int i = 0;

    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, in, strlen(in));
    SHA256_Final(digest, &ctx);

    for(i = 0; i < SHA256_DIGEST_LENGTH; i++)
    {
        sprintf(out + (i * 2), "%02x", digest[i]);
    }

    out[64] = 0;
}

/* EOF */
