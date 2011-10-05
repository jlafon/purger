#include <string.h>
#include "redis.h"


extern redisContext **redis_rank;
extern redisReply **redis_rank_reply;
extern redisContext *REDIS;
extern redisContext *BLOCKING_redis;
extern redisReply *REPLY;
extern redisReply *BLOCKING_reply;
extern int redis_pipeline_size;
extern int redis_local_pipeline_max;
extern int * redis_local_sharded_pipeline;
extern int shard_count;
int redis_finalize()
{
    int i = 0;
    if(redis_pipeline_size > 0)
    {
        LOG(PURGER_LOG_DBG,"Flushing %d items from pipeline",redis_pipeline_size);
        for(i = 0; i < redis_pipeline_size; i++)
            if(redisGetReply(REDIS,(void*)&REPLY) == REDIS_OK)
            {
                freeReplyObject(REPLY);
            }
    }
    redisFree(REDIS);
    redisFree(BLOCKING_redis);
}
int redis_shard_finalize()
{
    int i = 0, j = 0;
    for(i = 0; i < shard_count; i++)
    {
        if(redis_local_sharded_pipeline[i] > 0)
        {
            LOG(PURGER_LOG_DBG,"Flushing %d items from sharded pipeline %d",i);
            for(j = 0; j < redis_local_sharded_pipeline[i]; j++)
                if(redisGetReply(redis_rank[i],(void*)&redis_rank_reply[i]) == REDIS_OK)
                {
                    freeReplyObject(redis_rank_reply[i]);
                }
        }
        redisFree(redis_rank[i]);
    }
}
int redis_shard_init(char * hostnames, int port)
{
    int i = 0;
    char * host = strtok(hostnames,",");
    redis_rank = (redisContext **) malloc(sizeof(redisContext**));
    while(host != NULL)
    {
        LOG(PURGER_LOG_INFO,"Initializing redis connection to %s",host);
        redis_rank[i] = (redisContext *) malloc(sizeof(redisContext*));
        redis_rank[i] = redisConnect(host,port);
        LOG(PURGER_LOG_INFO,"Initialized redis connection to %s",host);
        if(redis_rank[i] && redis_rank[i]->err)
        {
            LOG(PURGER_LOG_FATAL,"Redis server (%s) error: %s",host[i],redis_rank[i]->errstr);
            return -1;
        }
        i++;
        host = strtok(NULL,",");
    }
    shard_count = i;
    redis_local_sharded_pipeline = (int *) calloc(i,sizeof(int));
    redis_rank_reply = (redisReply**) malloc(sizeof(redisReply*)*i);
    for(i = 0; i < shard_count; i++)
	redis_local_sharded_pipeline[i] = 0;
    LOG(PURGER_LOG_DBG,"Initialized %d redis connections.",shard_count);
    return shard_count;
}
int redis_init(char * hostname, int port)
{
    redis_pipeline_size = 0;
    redis_local_pipeline_max = rand() % REDIS_PIPELINE_MAX + 1000;
    REDIS = redisConnect(hostname, port);
    BLOCKING_redis = redisConnect(hostname, port);
    if(REDIS->err || BLOCKING_redis->err)
    {
        LOG(PURGER_LOG_FATAL, "Redis error: %s", REDIS->errstr);
        return -1;	    
    }
    return 0; 
}
void redis_print_error(redisContext * context)
{
    if(REPLY == NULL)
    {
        LOG(PURGER_LOG_ERR,"Unable to print redis error.");
    }
    switch(context->err)
    {
         case REDIS_ERR_IO:       LOG(PURGER_LOG_ERR,"There was an I/O error with redis:%s",context->errstr); break;
         case REDIS_ERR_EOF:      LOG(PURGER_LOG_ERR,"The server closed the connection:%s",context->errstr); break;
         case REDIS_ERR_PROTOCOL: LOG(PURGER_LOG_ERR,"There was an error while parsing the protocol:%s",context->errstr); break;
         case REDIS_ERR_OTHER:    LOG(PURGER_LOG_ERR,"Unknown error:%s",context->errstr); break;
         default:                 LOG(PURGER_LOG_ERR,"Redis error status not set."); break;
    }
    return;
}
int redis_blocking_command(char * cmd, void * result, returnType ret)
{
    BLOCKING_reply = redisCommand(BLOCKING_redis,cmd);
    if(BLOCKING_reply == NULL)
    {
	LOG(PURGER_LOG_ERR,"Redis command failed: %s",cmd);
        redis_print_error(BLOCKING_redis);
	return -1; 
    }
    switch(BLOCKING_reply->type)
    {
        case REDIS_REPLY_STATUS: break;
        case REDIS_REPLY_INTEGER: 
                            if(result != NULL && ret == INT) 
                                *(int*)result = BLOCKING_reply->integer;
                            LOG(PURGER_LOG_DBG,"Returning type int: %d",BLOCKING_reply->integer);
                            break;
        case REDIS_REPLY_NIL: break;
        case REDIS_REPLY_STRING: 
                            if(result != NULL && ret == CHAR) 
                                strcpy((char*)result,BLOCKING_reply->str);
                            else if(result != NULL && ret == INT)
                                *(int*)result = atoi(BLOCKING_reply->str);
                            LOG(PURGER_LOG_DBG,"Returning type char: %s",BLOCKING_reply->str);
                            break;
        case REDIS_REPLY_ARRAY: break;
        default: break;
    }
    return 0;
}

int redis_shard_command(int rank, char * cmd)
{
    LOG(PURGER_LOG_DBG,"Sending %s to %d. Pipeline has %d commands",cmd,rank,redis_local_sharded_pipeline[rank]);
    redisAppendCommand(redis_rank[rank],cmd);
    redis_local_sharded_pipeline[rank] = redis_local_sharded_pipeline[rank]+1;
    if(redis_local_sharded_pipeline[rank] > REDIS_PIPELINE_MAX)
    {
        LOG(PURGER_LOG_INFO,"Flushing pipeline %d with %d commands.",rank,redis_local_sharded_pipeline[rank]);
        int i;
        for(i = 0; i < redis_local_sharded_pipeline[rank]; i++)
            if(redisGetReply(redis_rank[rank],(void*)&redis_rank_reply[rank]) == REDIS_OK)
            {
                freeReplyObject(redis_rank_reply[rank]);
            }
            else 
            {
                redis_print_error(redis_rank[rank]);
            }    
        redis_local_sharded_pipeline[rank] = 0;
    }
    return 0;

}
int redis_command(int rank,char * cmd)
{

    redisAppendCommand(REDIS,cmd);
    if(redis_pipeline_size++ > REDIS_PIPELINE_MAX)
    {
        LOG(PURGER_LOG_INFO,"Flushing pipeline.");
        int i;
        for(i = 0; i < redis_pipeline_size; i++)
            if(redisGetReply(REDIS,(void*)&REPLY) == REDIS_OK)
            {
                freeReplyObject(REPLY);
            }
            else 
            {
                redis_print_error(REDIS);
            }    
        redis_pipeline_size = 0;
    }
    return 0;
} 
