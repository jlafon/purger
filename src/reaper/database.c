#include <stdlib.h>
#include <string.h>
#include <math.h>

#include <hiredis.h>

#include "config.h"
#include "../common/log.h"
#include "database.h"

extern int PURGER_global_rank;

redisContext *REDIS;

void
reaper_backoff_database(CIRCLE_handle *handle)
{
    float rand_num = 0.0;
    float exp_factor = 2.0;

    int init_timeout = 10;
    int num_retries = 0;
    int max_timeout = 120000; // 2 minutes

    int delay = 0;
    int status = REAPER_DB_COLLISION;

    /* Something around 1.5 */
    rand_num = (2.0 * (rand() / (RAND_MAX + 1.0)));

    while(status == REAPER_DB_COLLISION)
    {
        status = reaper_check_database_for_more(handle);

        /* delay = MIN( R * T * F ^ N , M ) */
        delay = (int)(rand_num * (float)init_timeout * (pow(exp_factor, (float)num_retries)));
        delay = ((((delay) - (max_timeout)) & 0x80000000) >> 31) ? (delay) : (max_timeout);

        LOG(PURGER_LOG_DBG, "Database collision. Backing off with delay of %d", delay);

        if(delay >= max_timeout)
        {
            LOG(PURGER_LOG_INFO, "Things are running very slow. Is the database overloaded? (%d)", max_timeout);
        }

        reaper_msleep(delay);
        num_retries++;
    }
}

int
reaper_msleep(unsigned long milisec)
{
    struct timespec req;
    time_t sec = (int)(milisec / 1000);

    milisec = milisec - (sec * 1000);
    req.tv_sec = sec;
    req.tv_nsec = milisec * 1000000L;

    while(nanosleep(&req, &req) == -1)
    {
         continue;
    }

    return 1;
}

int
reaper_pop_zset(char **results, char *zset, long long start, long long end)
{
    size_t num_poped = 0;

    redisReply *watchReply = redisCommand(REDIS, "WATCH %s", zset);
    if(watchReply->type == REDIS_REPLY_STATUS)
    {
        LOG(PURGER_LOG_DBG, "Watch returned: %s", watchReply->str);
    }
    else
    {
        LOG(PURGER_LOG_ERR, "Redis didn't return a status when trying to watch %s.", zset);
        return REAPER_DB_FATAL;
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
        return REAPER_DB_FATAL;
    }

    redisReply *multiReply = redisCommand(REDIS, "MULTI");
    if(multiReply->type == REDIS_REPLY_STATUS)
    {
        /* Multi always returns OK. So lets only worry about the exec return. */
        LOG(PURGER_LOG_DBG, "Multi returned a status of: %s", multiReply->str);
    }
    else
    {
        LOG(PURGER_LOG_ERR, "Redis didn't return a status when trying to multi %s. Discarding transaction.", zset);
        redisCommand(REDIS, "DISCARD");
        return REAPER_DB_FATAL;
    }

    redisReply *zremReply = redisCommand(REDIS, "ZREMRANGEBYRANK %s %lld %lld", zset, start, end);
    if(zremReply->type == REDIS_REPLY_STATUS)
    {
        LOG(PURGER_LOG_DBG, "Zremrangebyrank returned a status of: %s", zremReply->str);
    }
    else
    {
        LOG(PURGER_LOG_ERR, "Redis didn't return an integer when trying to zremrangebyrank %s. Discarding transaction.", zset);
        redisCommand(REDIS, "DISCARD");
        return REAPER_DB_FATAL;
    }

    redisReply *execReply = redisCommand(REDIS, "EXEC");
    if(execReply->type == REDIS_REPLY_ERROR)
    {
        LOG(PURGER_LOG_DBG, "Normal pop from the zset clashed. Try it again later. (%s)", REDIS->errstr);
        return REAPER_DB_COLLISION;
    }
    else if(execReply->type == REDIS_REPLY_ARRAY)
    {
        LOG(PURGER_LOG_DBG, "Exec returned an array of size: %ld", execReply->elements);

        if(execReply->elements > 1)
        {
            LOG(PURGER_LOG_DBG, "Success on a reaper transaction. (%s)", zset);
            return num_poped;
        }
        else
        {
            LOG(PURGER_LOG_DBG, "The transaction returned an empty result, something is wrong. (%s)", zset);
            return REAPER_DB_FATAL;
        }
    }
    else
    {
        LOG(PURGER_LOG_ERR, "Redis didn't return an array trying to exec %s.", zset);
        return REAPER_DB_FATAL;
    }

    LOG(PURGER_LOG_DBG, "If we got here, then we missed an edge case.");
    return REAPER_DB_FATAL;
}

int
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

    num_poped = reaper_pop_zset((char **)&del_keys, "mtime", 0, batch_size);

    if(num_poped >= 0)
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

    /* Return error code or the number poped. */
    return num_poped;
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
