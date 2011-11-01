#ifndef REDIS_H
#define REDIS_H
#include <hiredis.h>
#include "log.h"
#define REDIS_PIPELINE_MAX 1000
typedef enum { INT, CHAR } returnType;
redisContext *REDIS;
redisReply *REPLY;

redisContext *BLOCKING_redis;
redisReply *BLOCKING_reply;

redisContext **redis_rank;
redisReply **redis_rank_reply;

int * redis_local_sharded_pipeline;
char ** hostlist;
int redis_port;
int current_redis_rank;
int shard_count;
int redis_pipeline_size;
int redis_local_pipeline_max;
int redis_init(char * hostname, int port, int db_number);
int redis_shard_init(char * hostnames, int port, int db_number);
void redis_print_error(redisContext * context);
int redis_command(int rank,char * cmd);
int redis_shard_vcommand(int rank, const char * format, va_list ap);
int redis_shard_command(int rank, char * cmd);
int redis_blocking_shard_command(int rank, char * cmd, void * result, returnType ret);
int redis_blocking_command(char * cmd, void * result, returnType ret);
int redis_finalize();
int redis_shard_finalize();
int redis_handle_sigpipe();
#endif
