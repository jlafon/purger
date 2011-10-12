#ifndef HASH_H
#define HASH_H

#include <stdint.h>
#include <openssl/sha.h>
#include <openssl/bio.h>
#include <openssl/hmac.h>
#include <openssl/evp.h>
#include <openssl/buffer.h>

char * treewalk_base64_encode(const unsigned char * plaintext);
int treewalk_filename_hash(char *in, unsigned char out[65]);
int32_t treewalk_crc32(const void *buf, size_t size);
#endif /* HASH_H */
