#include <unistd.h>
#include <stdlib.h>

#include "reaper.h"
#include "log.h"
#include "hash.h"

#include <hiredis.h>
#include <async.h>

redisContext *REDIS;

time_t time_started;
time_t time_finished;

void
reaper_redis_run_cmd(char *cmd, char *filename)
{
    LOG(LOG_DBG, "RedisCmd = \"%s\"", cmd);

    if(redisCommand(REDIS, cmd) != NULL)
    {
        LOG(LOG_DBG, "Sent %s to redis", filename);
    }
    else
    {
        LOG(LOG_DBG, "Failed to SET %s", filename);
        if (REDIS->err)
        {
            LOG(LOG_ERR, "Redis error: %s", REDIS->errstr);
        }
    }
}

int
reaper_redis_keygen(char *buf, char *filename)
{
    unsigned char filename_hash[32];

    int hash_idx = 0;
    int cnt = 0;

    /* Add the key header */
    cnt += sprintf(buf, "file:");

    /* Generate and add the key */
    reaper_filename_hash(filename_hash, (unsigned char *)filename);
    for(hash_idx = 0; hash_idx < 32; hash_idx++)
        cnt += sprintf(buf + cnt, "%02x", filename_hash[hash_idx]);

    return cnt;
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

    /* TODO: stuff */

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
