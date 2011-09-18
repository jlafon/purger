#ifndef HASH_H
#define HASH_H

#include <openssl/sha.h>

void treewalk_filename_hash(unsigned char* md, const unsigned char* input);

#endif /* HASH_H */
