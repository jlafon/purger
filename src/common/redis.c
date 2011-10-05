#include "redis.h"

extern redisContext *REDIS;
extern redisReply *REPLY;
extern int redis_pipeline_size;


int redis_init(char * hostname, int port)
{
    redis_pipeline_size = 0;
    REDIS = redisConnect(hostname, port);
    if(REDIS->err)
    {
        LOG(PURGER_LOG_FATAL, "Redis error: %s", REDIS->errstr);
	return -1;	    
    }
    return 0; 
}
void redis_print_error()
{
    if(REPLY == NULL)
    {
        LOG(PURGER_LOG_ERR,"Unable to print redis error.");
    }
    switch(REDIS->err)
    {
         case REDIS_ERR_IO:       LOG(PURGER_LOG_ERR,"There was an I/O error with redis:%s",REDIS->errstr); break;
         case REDIS_ERR_EOF:      LOG(PURGER_LOG_ERR,"The server closed the connection:%s",REDIS->errstr); break;
         case REDIS_ERR_PROTOCOL: LOG(PURGER_LOG_ERR,"There was an error while parsing the protocol:%s",REDIS->errstr); break;
         case REDIS_ERR_OTHER:    LOG(PURGER_LOG_ERR,"Unknown error:%s",REDIS->errstr); break;
         default:                 LOG(PURGER_LOG_ERR,"Redis error status not set."); break;
    }
    return;
}
int redis_command(char * cmd)
{
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
            redis_print_error();
            return -1;
        }    
    }
    return 0;
} 
