#include <stdlib.h>
#include <string.h>

#include <hiredis.h>

#include "config.h"
#include "../common/log.h"
#include "database.h"

extern int PURGER_global_rank;

redisContext *REDIS;

int
reaper_pop_zset(char **results, char *zset, long long start, long long end)
{
    unsigned int num_poped = 0;

    redisReply *watchReply = redisCommand(REDIS, "WATCH %s", zset);
    if(watchReply->type == REDIS_REPLY_STATUS)
    {
        LOG(PURGER_LOG_DBG, "Watch returned: %s", watchReply->str);
    }
    else
    {
        LOG(PURGER_LOG_ERR, "Redis didn't return a status when trying to watch %s.", zset);
        return -1;
    }

    redisReply *zrangeReply = redisCommand(REDIS, "ZRANGE %s %lld %lld", zset, start, end - 1);
    if(zrangeReply->type == REDIS_REPLY_ARRAY)
    {
        LOG(PURGER_LOG_DBG, "Zrange returned an array of size: %zu", zrangeReply->elements);

        for(num_poped = 0; num_poped < zrangeReply->elements; num_poped++)
        {
            strcpy(*(results+num_poped), zrangeReply->element[num_poped]->str);
        }
    }
    else
    {
        LOG(PURGER_LOG_ERR, "Redis didn't return an array when trying to zrange %s.", zset);
        return -1;
    }

    redisReply *multiReply = redisCommand(REDIS, "MULTI");
    if(multiReply->type == REDIS_REPLY_STATUS)
    {
        LOG(PURGER_LOG_DBG, "Multi returned a status of: %s", multiReply->str);
    }
    else
    {
        LOG(PURGER_LOG_ERR, "Redis didn't return a status when trying to multi %s.", zset);
        return -1;
    }

    redisReply *zremReply = redisCommand(REDIS, "ZREMRANGEBYRANK %s %lld %lld", zset, start, end);
    if(zremReply->type == REDIS_REPLY_STATUS)
    {
        LOG(PURGER_LOG_DBG, "Zremrangebyrank returned a status of: %s", zremReply->str);
    }
    else
    {
        LOG(PURGER_LOG_ERR, "Redis didn't return an integer when trying to zremrangebyrank %s.", zset);
        return -1;
    }

    redisReply *execReply = redisCommand(REDIS, "EXEC");
    if(execReply->type == REDIS_REPLY_ARRAY)
    {
        LOG(PURGER_LOG_DBG, "Exec returned an array of size: %ld", execReply->elements);

        if(execReply->elements == -1)
        {
            LOG(PURGER_LOG_DBG, "Normal pop from the zset clashed. Try it again later.");
            return -1;
        }
        else
        {
            /* Success */
            return num_poped;
        }
    }
    else
    {
        LOG(PURGER_LOG_ERR, "Redis didn't return an array trying to exec %s.", zset);
        exit(EXIT_FAILURE);
    }
}

void
reaper_check_database_for_more(CIRCLE_handle *handle)
{
    int batch_size = 10;
    char *del_keys[batch_size];

    int num_poped;
    int i;

    for(i = 0; i < batch_size; i++)
    {
        del_keys[i] = (char *)malloc(CIRCLE_MAX_STRING_LEN);
    }

    if((num_poped = reaper_pop_zset((char **)&del_keys, "mtime", 0, batch_size)) >= 0)
    {
        for(i = 0; i < num_poped; i++)
        {
            LOG(PURGER_LOG_DBG, "Queueing: %s", del_keys[i]);
            handle->enqueue(del_keys[i]);
        }
    }
    else
    {
        LOG(PURGER_LOG_DBG, "Atomic pop failed (%d)", num_poped);
    }

    for(i = 0; i < batch_size; i++)
    {
        free(del_keys[i]);
    }
}

void
reaper_redis_zrangebyscore(char *zset, long long from, long long to)
{
    redisReply *reply;
    int numReplies = 0;

    reply = redisCommand(REDIS, "ZRANGEBYSCORE %s %lld %lld", zset, from, to);

    if(reply->type == REDIS_REPLY_ARRAY)
    {
        LOG(PURGER_LOG_DBG, "We have an array.");

        for(numReplies = reply->elements - 1; numReplies >= 0; numReplies--)
        {
            if(reply->element[numReplies]->type == REDIS_REPLY_STRING)
            {
                LOG(PURGER_LOG_DBG, "Replied with: %s", reply->element[numReplies]->str);
            }
            else
            {
                LOG(PURGER_LOG_DBG, "WTF");
            }
        }
    }
    else
    {
        LOG(PURGER_LOG_DBG, "Reply was something wheird.");
    }
}

/* EOF */
