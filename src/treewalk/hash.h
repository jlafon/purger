#ifndef HASH_H
#define HASH_H

#include <stdint.h>
#include <openssl/sha.h>

int treewalk_filename_hash(char *in, unsigned char out[65]);
int32_t crc32(const void *buf, size_t size);
#endif /* HASH_H */
