#include <string.h>
#include "redis.h"

extern redisContext *REDIS;
extern redisContext *BLOCKING_redis;
extern redisReply *REPLY;
extern redisReply *BLOCKING_reply;
extern int redis_pipeline_size;



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
int redis_init(char * hostname, int port)
{
    redis_pipeline_size = 0;
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
int redis_blocking_command(char * cmd, void * result)
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
        case REDIS_REPLY_INTEGER: if(result != NULL) *(int*)result = BLOCKING_reply->integer; break;
        case REDIS_REPLY_NIL: break;
        case REDIS_REPLY_STRING: if(result != NULL) strcpy((char*)result,BLOCKING_reply->str); break;
        case REDIS_REPLY_ARRAY: break;
        default: break;
    }
    return 0;
}
int redis_command(char * cmd)
{
    LOG(PURGER_LOG_DBG,"Appending %s\n",cmd);
    redisAppendCommand(REDIS,cmd);
    if(redis_pipeline_size++ > REDIS_PIPELINE_MAX)
    {
        redis_pipeline_size = 0;
        if(redisGetReply(REDIS,(void*)&REPLY) == REDIS_OK)
	    {
  	        freeReplyObject(REPLY);
	    }
        else 
        {
            redis_print_error(REDIS);
            return -1;
        }    
    }
    return 0;
} 
