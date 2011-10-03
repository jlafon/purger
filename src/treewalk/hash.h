#ifndef HASH_H
#define HASH_H

#include <openssl/sha.h>

void treewalk_filename_hash(char *in, char out[65]);

#endif /* HASH_H */
