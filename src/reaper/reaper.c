#include <unistd.h>
#include <stdlib.h>

#include "config.h"

#include "reaper.h"
#include "log.h"

#include <hiredis.h>
#include <async.h>

redisContext *REDIS;

long long NUMBER_FILES_REMAINING = 0;

time_t time_started;
time_t time_finished;

void
process_files(CIRCLE_handle *handle)
{
/***
    WATCH warnedtime
    files = ZRANGE warnedtime start stop
    MULTI
    ZREM rangebyrank warnedtime start stop 
    EXEC
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
    dbgstream = stderr;
    debug_level = PURGER_LOGLEVEL;

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

    NUMBER_FILES_REMAINING = reaper_redis_zcard("mtime");
    LOG(LOG_DBG, "The number of files we have to process: %lld.", NUMBER_FILES_REMAINING);

    CIRCLE_init(argc, argv);
    CIRCLE_cb_create(&process_files);
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
