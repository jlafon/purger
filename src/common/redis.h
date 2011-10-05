#ifndef REDIS_H
#define REDIS_H
#include <hiredis.h>
#include "log.h"
#define REDIS_PIPELINE_MAX 100

redisContext *REDIS;
redisReply *REPLY;
redisContext *BLOCKING_redis;
redisReply *BLOCKING_reply;
int redis_pipeline_size;
int redis_init(char * hostname, int port);
void redis_print_error(redisContext * context);
int redis_command(char * cmd);
int redis_blocking_command(char * cmd, void * result);
#endif
