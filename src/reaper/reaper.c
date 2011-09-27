#include <unistd.h>
#include <stdlib.h>
#include <string.h>

#include "config.h"

#include "reaper.h"
#include "../common/log.h"

#include <hiredis.h>
#include <async.h>

redisContext *REDIS;

time_t time_started;
time_t time_finished;

FILE *PURGER_dbgstream;
int  PURGER_global_rank;
int  PURGER_debug_level;

int
reaper_pop_zset(char **results, char *zset, long long start, long long end)
{
    int num_poped = 0;

    redisReply *watchReply = redisCommand(REDIS, "WATCH %s", zset);
    if(watchReply->type == REDIS_REPLY_STATUS)
    {
        LOG(LOG_DBG, "Watch returned: %s", watchReply->str);
    }
    else
    {
        LOG(LOG_ERR, "Redis didn't return a status when trying to watch %s.", zset);
        exit(EXIT_FAILURE);
    }

    redisReply *zrangeReply = redisCommand(REDIS, "ZRANGE %s %lld %lld", zset, start, end - 1);
    if(zrangeReply->type == REDIS_REPLY_ARRAY)
    {
        LOG(LOG_DBG, "Zrange returned an array of size: %zu", zrangeReply->elements);

        for(num_poped = 0; num_poped < zrangeReply->elements; num_poped++)
        {
            strcpy(*(results+num_poped), zrangeReply->element[num_poped]->str);
        }
    }
    else
    {
        LOG(LOG_ERR, "Redis didn't return an array when trying to zrange %s.", zset);
        exit(EXIT_FAILURE);
    }

    redisReply *multiReply = redisCommand(REDIS, "MULTI");
    if(multiReply->type == REDIS_REPLY_STATUS)
    {
        LOG(LOG_DBG, "Multi returned a status of: %s", multiReply->str);
    }
    else
    {
        LOG(LOG_ERR, "Redis didn't return a status when trying to multi %s.", zset);
        exit(EXIT_FAILURE);
    }

    redisReply *zremReply = redisCommand(REDIS, "ZREMRANGEBYRANK %s %lld %lld", zset, start, end);
    if(zremReply->type == REDIS_REPLY_STATUS)
    {
        LOG(LOG_DBG, "Zremrangebyrank returned a status of: %s", zremReply->str);
    }
    else
    {
        LOG(LOG_ERR, "Redis didn't return an integer when trying to zremrangebyrank %s.", zset);
        exit(EXIT_FAILURE);
    }

    redisReply *execReply = redisCommand(REDIS, "EXEC");
    if(execReply->type == REDIS_REPLY_ARRAY)
    {
        LOG(LOG_DBG, "Exec returned an array of size: ", execReply->elements);

        if(execReply->elements == -1)
        {
            LOG(LOG_DBG, "Normal pop from the zset clashed.");
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
        LOG(LOG_ERR, "Redis didn't return an array trying to exec %s.", zset);
        exit(EXIT_FAILURE);
    }
}

void
process_files(CIRCLE_handle *handle)
{
    int batch_size = 10;
    char *del_keys[batch_size];

    int num_poped;
    int i;

    for(i = 0; i < batch_size; i++)
    {
        del_keys[i] = (char *)malloc(CIRCLE_MAX_STRING_LEN);
    }

    /* Atomically pop a few keys from the mtime zset. */
    if(num_poped = reaper_pop_zset((char **)&del_keys, "mtime", 0, batch_size))
    {
        for(i = 0; i < num_poped; i++)
        {
            LOG(LOG_DBG, "Queueing: %s", del_keys[i]);
            handle->enqueue(del_keys[i]);
        }
    }
    else
    {
        LOG(LOG_DBG, "Atomic pop failed");
    }

    for(i = 0; i < batch_size; i++)
    {
        free(del_keys[i]);
    }

    char *key = (char *)malloc(CIRCLE_MAX_STRING_LEN);
    handle->dequeue(key);

    redisReply *hmgetReply = redisCommand(REDIS, "HMGET %s mtime name", key);
    if(hmgetReply->type == REDIS_REPLY_ARRAY)
    {
        LOG(LOG_DBG, "Hmget returned an array of size: %zu", hmgetReply->elements);
        LOG(LOG_DBG, "mtime for %s is %s", hmgetReply->element[1]->str, hmgetReply->element[0]->str);
    }
    else
    {
        LOG(LOG_ERR, "Redis didn't return an array when trying to hmget %s.", key);
        exit(EXIT_FAILURE);
    }

/***
    3) Now, lets grab a file.

        old_file_stat_info = hmget handle->dequeue()

    4) And stat it.

        new_file_stat_info = stat(old_file_stat_info)

    5) Then, check to see if it's still a file we should delete.

        if((new_file_stat_info->mtime + 6 days) < now) {
            Be paranoid here.
            unlink(file)
        } else {
            if(debug) {
                Check to see if the ZREM from the atomic pop worked.
            }
        }
****/
}

long long
reaper_redis_zcard(char *zset)
{
    redisReply *reply;
    int numReplies = 0;

    reply = redisCommand(REDIS, "ZCARD %s", zset);

    if(reply->type == REDIS_REPLY_INTEGER)
    {
        return reply->integer;
    }
    else
    {
        LOG(LOG_ERR, "Redis didn't return an integer when trying to count the number in a zset.");
        exit(EXIT_FAILURE);
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
        LOG(LOG_DBG, "We have an array.");

        for(numReplies = reply->elements - 1; numReplies >= 0; numReplies--)
        {
            if(reply->element[numReplies]->type == REDIS_REPLY_STRING)
            {
                LOG(LOG_DBG, "Replied with: %s", reply->element[numReplies]->str);
            }
            else
            {
                LOG(LOG_DBG, "WTF");
            }
        }
    }
    else
    {
        LOG(LOG_DBG, "Reply was something wheird.");
    }
}

void
print_usage(char **argv)
{
    fprintf(stderr, "Usage: %s [-h <redis_hostname> -p <redis_port>]\n", argv[0]);
}

int
main (int argc, char **argv)
{
    int index;
    int c;

    char *redis_hostname;
    int redis_port;

    int redis_hostname_flag = 0;
    int redis_port_flag = 0;

    /* Enable logging. */
    PURGER_dbgstream = stderr;
    PURGER_debug_level = PURGER_LOGLEVEL;

    opterr = 0;
    while((c = getopt(argc, argv, "h:p:")) != -1)
    {
        switch(c)
        {
            case 'h':
                redis_hostname = optarg;
                redis_hostname_flag = 1;
                break;

            case 'p':
                redis_port = atoi(optarg);
                redis_port_flag = 1;
                break;

            case '?':
                if(optopt == 'h' || optopt == 'p')
                {
                    print_usage(argv);
                    fprintf(stderr, "Option -%c requires an argument.\n", optopt);
                    exit(EXIT_FAILURE);
                }
                else if (isprint (optopt))
                {
                    print_usage(argv);
                    fprintf(stderr, "Unknown option `-%c'.\n", optopt);
                    exit(EXIT_FAILURE);
                }
                else
                {
                    print_usage(argv);
                    fprintf(stderr,
                        "Unknown option character `\\x%x'.\n",
                        optopt);
                    exit(EXIT_FAILURE);
                }

            default:
                abort();
        }
    }

    if(redis_hostname_flag == 0)
    {
        LOG(LOG_WARN, "A hostname for redis was not specified, defaulting to localhost.");
        redis_hostname = "localhost";
    }

    if(redis_port_flag == 0)
    {
        LOG(LOG_WARN, "A port number for redis was not specified, defaulting to 6379.");
        redis_port = 6379;
    }

    for (index = optind; index < argc; index++)
        LOG(LOG_WARN, "Non-option argument %s", argv[index]);

    REDIS = redisConnect(redis_hostname, redis_port);
    if (REDIS->err)
    {
        LOG(LOG_FATAL, "Redis error: %s", REDIS->errstr);
        exit(EXIT_FAILURE);
    }

    time(&time_started);

//    NUMBER_FILES_REMAINING = reaper_redis_zcard("mtime");

    PURGER_global_rank = CIRCLE_init(argc, argv);
    CIRCLE_cb_process(&process_files);
    CIRCLE_begin();
    CIRCLE_finalize();

    time(&time_finished);
/***
    LOG(LOG_INFO, "reaper run started at: %l", time_started);
    LOG(LOG_INFO, "reaper run completed at: %l", time_finished);
    LOG(LOG_INFO, "reaper total time (seconds) for this run: %l",
        ((double) (time_finished - time_started)) / CLOCKS_PER_SEC);
***/
    exit(EXIT_SUCCESS);
}

/* EOF */
