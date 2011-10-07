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
extern char ** hostlist;

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
    redis_port = port;
    char * host = strtok(hostnames,",");
    redis_rank = (redisContext **) malloc(sizeof(redisContext**));
    hostlist = (char **) malloc(sizeof(char **));
    while(host != NULL)
    {
        LOG(PURGER_LOG_INFO,"Initializing redis connection to %s",host);
        redis_rank[i] = (redisContext *) malloc(sizeof(redisContext*));
        redis_rank[i] = redisConnect(host,port);
	hostlist[i] = (char*) malloc(sizeof(char)*2048);
	strcpy(hostlist[i],host);
        LOG(PURGER_LOG_INFO,"Initialized redis connection to %s",host);
        if(redis_rank[i] && redis_rank[i]->err)
        {
            LOG(PURGER_LOG_FATAL,"Redis server (%s) error: %s",host,redis_rank[i]->errstr);
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
    srand(2^PURGER_global_rank-1,time(NULL));
    redis_local_pipeline_max = rand() % REDIS_PIPELINE_MAX + 500;
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
    switch(context->err)
    {
         case REDIS_ERR_IO:       
			LOG(PURGER_LOG_ERR,"There was an I/O error with redis:%s",context->errstr);
			redis_handle_sigpipe(); 
			break;
         case REDIS_ERR_EOF:      LOG(PURGER_LOG_ERR,"The server closed the connection:%s",context->errstr); break;
         case REDIS_ERR_PROTOCOL: LOG(PURGER_LOG_ERR,"There was an error while parsing the protocol:%s",context->errstr); break;
         case REDIS_ERR_OTHER:    LOG(PURGER_LOG_ERR,"Unknown error:%s",context->errstr); break;
         case REDIS_ERR_OOM:	  LOG(PURGER_LOG_ERR,"Redis OOM:%s",context->errstr); break;
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
int redis_handle_sigpipe()
{
   LOG(PURGER_LOG_ERR,"Caught sigpipe, attempting to reinitialize connection.");
   redisFree(redis_rank[current_redis_rank]);
   redis_rank[current_redis_rank] = redisConnect(hostlist[current_redis_rank],redis_port);
   if(redis_rank[current_redis_rank] && redis_rank[current_redis_rank]->err)
        {
            LOG(PURGER_LOG_FATAL,"Redis server (%s) error: %s",hostlist[current_redis_rank],redis_rank[current_redis_rank]->errstr);
            return -1;
        }
   LOG(PURGER_LOG_ERR,"Connection established.");
return 0;
}

int redis_flush_pipe(redisContext * c, redisReply * r)
{
        int done = 0;
        do
        {
             if(redisBufferWrite(c,&done) == REDIS_ERR)
             {
                LOG(PURGER_LOG_ERR,"Error on redisBufferWrite");
                perror("redisBufferWrite");
				_exit(1); 
				redis_print_error(c);
                break;
             }
        } while( !done );

            if(redisGetReply(c,(void*)&r) == REDIS_OK)
            {
                freeReplyObject(r);
            }
            else 
            {
                LOG(PURGER_LOG_ERR,"Error on redisGetReply");
                redis_print_error(c);
            }  
}
int redis_shard_command(int rank, char * cmd)
{
    current_redis_rank = rank;
    LOG(PURGER_LOG_DBG,"Sending %s to %d. Pipeline has %d commands",cmd,rank,redis_local_sharded_pipeline[rank]);
    if(redisAppendCommand(redis_rank[rank],cmd) != REDIS_OK)
    {
			LOG(PURGER_LOG_ERR,"Error on redisAppendCommand, attempting to flush pipe");
			redis_print_error(redis_rank[rank]);
                        LOG(PURGER_LOG_INFO,"Flushing pipeline %d with %d commands.",rank,redis_local_sharded_pipeline[rank]);
                        redis_flush_pipe(redis_rank[rank],redis_rank_reply[rank]);
			LOG(PURGER_LOG_INFO,"Pipeline %d flushed.",rank);
    }
    redis_local_sharded_pipeline[rank] = redis_local_sharded_pipeline[rank]+1;
    if(redis_local_sharded_pipeline[rank] > redis_local_pipeline_max)
    {
        LOG(PURGER_LOG_INFO,"Flushing pipeline %d with %d commands.",rank,redis_local_sharded_pipeline[rank]);
	redis_flush_pipe(redis_rank[rank],redis_rank_reply[rank]);
        LOG(PURGER_LOG_INFO,"Pipeline %d flushed.",rank);
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
