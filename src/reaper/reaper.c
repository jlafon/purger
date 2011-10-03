#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <hiredis.h>
#include <ctype.h>

#include "config.h"

#include "reaper.h"
#include "local.h"
#include "database.h"

#include "../common/log.h"

FILE *PURGER_dbgstream;
int  PURGER_global_rank;
PURGER_loglevel PURGER_debug_level;

extern redisContext *REDIS;

void
process_files(CIRCLE_handle *handle)
{
    /* Attempt to grab a key from the local queue if it has any keys. */
    char *key = (char *)malloc(CIRCLE_MAX_STRING_LEN);
    handle->dequeue(key);

    if(key != NULL && strlen(key) > 0)
    {
        reaper_check_local_queue(handle, key);
    }
    else
    {
        reaper_backoff_database(handle);
    }
    /* TODO: if both of the above fail, add a card check. */

    free(key);
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
    PURGER_debug_level = PURGER_LOG_DBG;

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
        LOG(PURGER_LOG_WARN, "A hostname for redis was not specified, defaulting to localhost.");
        redis_hostname = "localhost";
    }

    if(redis_port_flag == 0)
    {
        LOG(PURGER_LOG_WARN, "A port number for redis was not specified, defaulting to 6379.");
        redis_port = 6379;
    }

    for (index = optind; index < argc; index++)
        LOG(PURGER_LOG_WARN, "Non-option argument %s", argv[index]);

    REDIS = redisConnect(redis_hostname, redis_port);
    if (REDIS->err)
    {
        LOG(PURGER_LOG_FATAL, "Redis error: %s", REDIS->errstr);
        exit(EXIT_FAILURE);
    }

    PURGER_global_rank = CIRCLE_init(argc, argv);
    CIRCLE_cb_process(&process_files);
    CIRCLE_begin();
    CIRCLE_finalize();

    exit(EXIT_SUCCESS);
}

/* EOF */
