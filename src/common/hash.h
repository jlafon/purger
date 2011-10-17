#ifndef PURGER_HASH_H
#define PURGER_HASH_H

#include <stdint.h>
#include <openssl/sha.h>
#include <openssl/bio.h>
#include <openssl/hmac.h>
#include <openssl/evp.h>
#include <openssl/buffer.h>
char * purger_base64_decode(unsigned char * encodedtext, int len);
char * purger_base64_encode(const unsigned char * plaintext);
int purger_filename_hash(char *in, unsigned char out[65]);
int32_t purger_crc32(const void *buf, size_t size);
#endif /* PURGER_HASH_H */
