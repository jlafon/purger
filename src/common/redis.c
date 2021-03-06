#include <string.h>
#include "sds.h"
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
            LOG(PURGER_LOG_INFO,"Flushing %d items from sharded pipeline %d",redis_local_sharded_pipeline[i],i);
            for(j = 0; j < redis_local_sharded_pipeline[i]; j++)
            {
                redisAppendCommand(redis_rank[i],"EXEC");
                redisAppendCommand(redis_rank[i],"SAVE");
                if(redisGetReply(redis_rank[i],(void*)&redis_rank_reply[i]) == REDIS_OK)
                {
                    freeReplyObject(redis_rank_reply[i]);
                }
        }
        }
        redisFree(redis_rank[i]);
    }
}
int redis_shard_init(char * hostnames, int port, int db_number)
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
        redisCommand(redis_rank[i],"SELECT %d", db_number);
        i++;
        host = strtok(NULL,",");
    }
    shard_count = i;
    redis_local_sharded_pipeline = (int *) calloc(i,sizeof(int));
    redis_rank_reply = (redisReply**) malloc(sizeof(redisReply*)*i);
    for(i = 0; i < shard_count; i++)
    {
        redis_local_sharded_pipeline[i] = 0;
        redisAppendCommand(redis_rank[i],"MULTI");
    }
    LOG(PURGER_LOG_DBG,"Initialized %d redis connections.",shard_count);
    return shard_count;
}
int redis_init(char * hostname, int port, int db_number)
{
    redis_pipeline_size = 0;
    srand(2^PURGER_global_rank-1,time(NULL));
    redis_local_pipeline_max = rand() % REDIS_PIPELINE_MAX + 10000;
    REDIS = redisConnect(hostname, port);
    BLOCKING_redis = redisConnect(hostname, port);
    if(REDIS->err || BLOCKING_redis->err)
    {
        LOG(PURGER_LOG_FATAL, "Redis error: %s", REDIS->errstr);
        return -1;        
    }
    redisCommand(BLOCKING_redis,"SELECT %d",db_number);
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
         case REDIS_ERR_OOM:      LOG(PURGER_LOG_ERR,"Redis OOM:%s",context->errstr); break;
         default:                 LOG(PURGER_LOG_ERR,"Redis error status not set."); break;
    }
    return;
}
int redis_blocking_shard_command(int rank, char * cmd, void * result, returnType ret, size_t len)
{
    redisAppendCommand(redis_rank[rank],"EXEC");
    redis_flush_pipe(redis_rank[rank],redis_rank_reply[rank]); 
    redis_rank_reply[rank] = redisCommand(redis_rank[rank],cmd);
    redisAppendCommand(redis_rank[rank],"MULTI");
     
    LOG(PURGER_LOG_DBG,"Executed (%s) on rank %d",cmd,rank);
    if(redis_rank_reply[rank] == NULL)
    {
        LOG(PURGER_LOG_ERR,"Redis command failed: %s",cmd);
        redis_print_error(redis_rank[rank]);
        return -1; 
    }
    switch(redis_rank_reply[rank]->type)
    {
        case REDIS_REPLY_STATUS: LOG(PURGER_LOG_DBG,"REDIS_REPLY_STATUS: (%s)",redis_rank_reply[rank]->str); 
                            break;
        case REDIS_REPLY_INTEGER: 
                            if(result != NULL && ret == INT) 
                                *(int*)result = redis_rank_reply[rank]->integer;
                            LOG(PURGER_LOG_DBG,"Returning type int: %d",redis_rank_reply[rank]->integer);
                            return (ret == INT);
                            break;
        case REDIS_REPLY_NIL:   
                            LOG(PURGER_LOG_DBG,"REDIS_NIL"); 
                            return (ret == INT || ret == CHAR)?-1:0; 
                            break;
        case REDIS_REPLY_STRING: 
                            if(result != NULL && ret == CHAR) 
                                strcpy((char*)result,redis_rank_reply[rank]->str);
                            else if(result != NULL && ret == INT)
                                *(int*)result = atoi(redis_rank_reply[rank]->str);
                            LOG(PURGER_LOG_DBG,"Returning type char: %s",redis_rank_reply[rank]->str);
                            break;
        case REDIS_REPLY_ARRAY:  
                            if(ret != CHAR_ARRAY)
                                return -1;
                            size_t chars_copied = 0;
                            char * tail = (char*)result;
                            LOG(PURGER_LOG_DBG,"REDIS_REPLY_ARRAY[%d]",redis_rank_reply[rank]->elements); 
                            int i = 0;
                            for(i = 0; i < redis_rank_reply[rank]->elements; i++)
                            {
                                if(redis_rank_reply[rank]->element[i]->type == REDIS_REPLY_INTEGER)
                                    LOG(PURGER_LOG_DBG,"Element[%d] = (%d)",i,redis_rank_reply[rank]->element[i]->integer);
                                else if(redis_rank_reply[rank]->element[i]->type == REDIS_REPLY_STRING)
                                {
                                    LOG(PURGER_LOG_DBG,"Element[%d] = (%s)",i,redis_rank_reply[rank]->element[i]->str);
                                    chars_copied += strlen(redis_rank_reply[rank]->element[i]->str);
                                    chars_copied += 1; // NULL char
                                    if(chars_copied < len)
                                    {
                                        strcpy(tail,redis_rank_reply[rank]->element[i]->str);
                                        tail = (char*)result + chars_copied;    
                                    }
                                    else
                                        LOG(PURGER_LOG_ERR,"Unable to copy string, not enough buffer left.");
                                }
                                else if(redis_rank_reply[rank]->element[i]->type == REDIS_REPLY_STATUS)
                                    LOG(PURGER_LOG_DBG,"Element[%d] = (%s)",i,redis_rank_reply[rank]->element[i]->str);
                                else if(redis_rank_reply[rank]->element[i]->type == REDIS_REPLY_NIL)
                                    LOG(PURGER_LOG_DBG,"Element[%d] = (NIL)",i,redis_rank_reply[rank]->element[i]->str);
                                else if(redis_rank_reply[rank]->element[i]->type == REDIS_REPLY_ARRAY)
                                    LOG(PURGER_LOG_DBG,"Element[%d] = (ARRAY)",i,redis_rank_reply[rank]->element[i]->str);
                            }
                            return (ret == INT || ret == CHAR)?-1:0; break;
        default: break;
    }
    return 0;
}
int redis_blocking_command(char * cmd, void * result, returnType ret)
{
    if(result == NULL)
        return;
    BLOCKING_reply = redisCommand(BLOCKING_redis,cmd);
    LOG(PURGER_LOG_DBG,"Executed (%s).",cmd);
    if(BLOCKING_reply == NULL)
    {
        LOG(PURGER_LOG_ERR,"Redis command failed: %s",cmd);
        redis_print_error(BLOCKING_redis);
        return -1; 
    }
    switch(BLOCKING_reply->type)
    {
        case REDIS_REPLY_STATUS: LOG(PURGER_LOG_DBG,"REDIS_REPLY_STATUS (%s)",BLOCKING_reply->str);
                                 break;
        case REDIS_REPLY_INTEGER: 
                            if(ret == INT) 
                                *(int*)result = BLOCKING_reply->integer;
                            LOG(PURGER_LOG_DBG,"Returning type int: %d",BLOCKING_reply->integer);
                            break;
        case REDIS_REPLY_NIL: LOG(PURGER_LOG_DBG,"REDIS_REPLY_NIL"); return (ret == INT || ret == CHAR)?-1:0; break;
        case REDIS_REPLY_STRING: 
                            if(ret == CHAR) 
                                strcpy((char*)result,BLOCKING_reply->str);
                            else if(ret == INT)
                                *(int*)result = atoi(BLOCKING_reply->str);
                            LOG(PURGER_LOG_DBG,"Returning type char: %d",*(int*)result);
                            return 0;
                            break;
        case REDIS_REPLY_ARRAY: LOG(PURGER_LOG_DBG,"REDIS_REPLY_ARRAY[%d]",BLOCKING_reply->elements); return (ret == INT || ret == CHAR)?-1:0; break;
        default: break;
    }
    return 0;
}
int redis_handle_sigpipe()
{
   LOG(PURGER_LOG_ERR,"Caught sigpipe, attempting to reinitialize connection.");
   LOG(PURGER_LOG_ERR,"Output buffer contains %d bytes",sdslen(redis_rank[current_redis_rank]->obuf));
   redisContext * temp = redisConnect(hostlist[current_redis_rank],redis_port);
//   redis_rank[current_redis_rank] = redisConnect(hostlist[current_redis_rank],redis_port);
   if(temp && temp->err)
   //if(redis_rank[current_redis_rank] && redis_rank[current_redis_rank]->err)
    {
        LOG(PURGER_LOG_FATAL,"Redis server (%s) error: %s",hostlist[current_redis_rank],redis_rank[current_redis_rank]->errstr);
        return -1;
    }
   //redisFree(redis_rank[current_redis_rank]);
   temp->obuf = redis_rank[current_redis_rank]->obuf;
   if(redis_rank[current_redis_rank]->fd > 0)
       close(redis_rank[current_redis_rank]);
   if(redis_rank[current_redis_rank]->reader != NULL)
       redisReaderFree(redis_rank[current_redis_rank]->reader);
   redis_rank[current_redis_rank] = temp;
   LOG(PURGER_LOG_ERR,"Connection established.");
return 0;
}

int redis_flush_pipe(redisContext * c, redisReply * r)
{
    LOG(PURGER_LOG_DBG,"Flushing pipe.");
    int done = 0;
    do
    {
         if(redisBufferWrite(c,&done) != REDIS_OK)
         {
            LOG(PURGER_LOG_ERR,"Error on redisBufferWrite during flush");
            perror("redisBufferWrite");
            redis_print_error(c);
            break;
         }
    } while( !done );
    for(done = 0; done < redis_local_sharded_pipeline[current_redis_rank]+2; done++)
        if(redisGetReply(c,(void*)&r) == REDIS_OK)
        {
            freeReplyObject(r);
        }
        else 
        {
            LOG(PURGER_LOG_ERR,"Error on redisGetReply");
            redis_print_error(c);
        }  
    LOG(PURGER_LOG_DBG,"Flushing pipe done.");
}
int redis_shard_vcommand(int rank, const char *format, va_list ap)
{
    current_redis_rank = rank;
    if(redisAppendCommand(redis_rank[rank],format, ap) != REDIS_OK)
    {
        LOG(PURGER_LOG_ERR,"Error on redisAppendFormattedCommand  attempting to flush pipe");
        redis_print_error(redis_rank[rank]);
    }
    if(redis_local_sharded_pipeline[rank]++ > redis_local_pipeline_max)
    {
        LOG(PURGER_LOG_INFO,"Flushing pipeline %d with %d commands.",rank,redis_local_sharded_pipeline[rank]);
        redis_flush_pipe(redis_rank[rank], redis_rank_reply[rank]);
        LOG(PURGER_LOG_INFO,"Pipeline %d flushed.",rank);
        redis_local_sharded_pipeline[rank] = 0;
    }
    return 0;

}

int redis_shard_command(int rank, char * cmd)
{
    current_redis_rank = rank;
    LOG(PURGER_LOG_DBG,"Sending %s to %d. Pipeline has %d commands",cmd,rank,redis_local_sharded_pipeline[rank]);
    if(redisAppendCommand(redis_rank[rank],cmd) != REDIS_OK)
    {
        LOG(PURGER_LOG_ERR,"Error on redisAppendFormattedCommand \"%s\", attempting to flush pipe",cmd);
        redis_print_error(redis_rank[rank]);
    }
    if(redis_local_sharded_pipeline[rank]++ > redis_local_pipeline_max)
    {
        LOG(PURGER_LOG_INFO,"Flushing pipeline %d with %d commands.",rank,redis_local_sharded_pipeline[rank]);
        redisAppendCommand(redis_rank[rank],"EXEC");
        redis_flush_pipe(redis_rank[rank], redis_rank_reply[rank]);
        redisAppendCommand(redis_rank[rank],"MULTI");
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
