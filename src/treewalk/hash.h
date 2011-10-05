#ifndef HASH_H
#define HASH_H

#include <openssl/sha.h>

long int treewalk_filename_hash(char *in, unsigned char out[65]);

#endif /* HASH_H */
