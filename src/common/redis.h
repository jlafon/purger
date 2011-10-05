#ifndef REDIS_H
#define REDIS_H
#include <hiredis.h>
#include "log.h"
#define REDIS_PIPELINE_MAX 100

redisContext *REDIS;
redisReply *REPLY;
int redis_pipeline_size;
int redis_init(char * hostname, int port);
void redis_print_error();
int redis_command(char * cmd);
#endif
