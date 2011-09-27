#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <time.h>

#include "warnusers.h"
#include "log.h"

#include <hiredis.h>
#include <async.h>

char         *TOP_DIR;
redisContext *REDIS;

time_t time_started;
time_t time_finished;

void
add_objects(CIRCLE_handle *handle)
{
    char buf[256];
    if(warnusers_redis_run_spop(buf)< 0)
    {
        LOG(LOG_WARN,"No elements in set.");
        int status = warnusers_redis_run_get("treewalk");
        if(status == 0)
        {
            LOG(LOG_WARN,"Treewalk no longer running, and the set is empty.  Exiting warnusers.");
            exit(0);
        }
        else if (status == 1)
        {
            LOG(LOG_WARN,"Treewalk is running, but the set is empty.  Warnusers will now spinlock waiting for set elements.");
            while(warnusers_redis_run_scard("warnlist") <= 0 && warnusers_redis_run_get("treewalk") == 1)
                ;
            if(warnusers_redis_run_spop(buf)< 0)
            {
                LOG(LOG_WARN,"Warnusers giving up.  No items in the set.");
                exit(0);
            }
        }
    }
    handle->enqueue(buf);
}
void warnusers_get_uids(CIRCLE_handle *handle)
{
    char uid[256];
    int status = warnusers_redis_run_get("treewalk");
    switch(status)
    {
        case 0:
            LOG(LOG_DBG,"Treewalk no longer running, and set is empty.");
            break;
        case 1:
            if(warnusers_redis_run_scard("warnlist") < 0)
                LOG(LOG_WARN,"Treewalk is running, but the set is empty.  Warnusers will now spinlock waiting for set elements, or for treewalk to finish");
            
            /* Spin lock */
            while(warnusers_redis_run_scard("warnlist") <= 0 && warnusers_redis_run_get("treewalk") == 1)
                ;
            int i = 0;
            for(i=0; i < warnusers_redis_run_scard("warnlist"); i++)
            {
                if(warnusers_redis_run_spop(uid) == -1)
                {
                    LOG(LOG_ERR,"Something went badly wrong with the uid set.");
                    exit(0);
                }
                handle->enqueue(uid);
            }
            break;
        default:
            LOG(LOG_ERR,"Treewalk key not set.  Unexpected state.");
            break;
    }
}
void
process_objects(CIRCLE_handle *handle)
{
    char uid[256];
    LOG(LOG_DBG,"Rank %d in process_objects",CIRCLE_global_rank);
    if(CIRCLE_global_rank == 0)
    {
        warnusers_get_uids(handle);
    }
    else
    {
        char temp[CIRCLE_MAX_STRING_LEN];
        /* Pop an item off the queue */ 
        LOG(LOG_DBG, "Popping, queue has %d elements", CIRCLE_queue_count());
        handle->dequeue(temp);
        LOG(LOG_DBG, "Popped [%s]", temp);
    }
    return;
}
void
warnusers_redis_run_sadd(int id)
{
    char *buf = (char*)malloc(2048 * sizeof(char));
    sprintf(buf,"SADD warnlist %d",id);
    warnusers_redis_run_cmd(buf,buf);
}
void
warnusers_redis_run_cmd(char *cmd, char *filename)
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
warnusers_redis_run_scard(char * set)
{
    char * redis_cmd_buf = (char*)malloc(2048 * sizeof(char));
    sprintf(redis_cmd_buf,"SCARD %s",set);
    redisReply *getReply = redisCommand(REDIS,redis_cmd_buf);
    if(getReply->type == REDIS_REPLY_NIL)
        return -1;
    else if (getReply->type == REDIS_REPLY_STRING)
    {
        LOG(LOG_DBG,"GET returned a string \"%s\"\n",getReply->str);
        return atoi(getReply->str);
    }
    else if(getReply->type == REDIS_REPLY_INTEGER)
    {
        LOG(LOG_DBG,"GET returned an int: %d.",getReply->integer);
        return getReply->integer;
    }
    else
        LOG(LOG_DBG,"GET returned something else.");
    return -1;
}
int 
warnusers_redis_run_get(char * key)
{
    char * redis_cmd_buf = (char*)malloc(2048 * sizeof(char));
    sprintf(redis_cmd_buf,"GET %s",key);
    redisReply *getReply = redisCommand(REDIS,redis_cmd_buf);
    if(getReply->type == REDIS_REPLY_NIL)
        return -1;
    else if (getReply->type == REDIS_REPLY_STRING)
    {
        LOG(LOG_DBG,"GET returned a string \"%s\"\n",getReply->str);
        return atoi(getReply->str);
    }
    else
        LOG(LOG_DBG,"GET didn't return a string");
    return -1;
}
int
warnusers_redis_run_spop(char * uid)
{
   redisReply *spopReply = redisCommand(REDIS,"SPOP warnlist");
   if(spopReply->type == REDIS_REPLY_NIL)
       return -1;
   else if(spopReply->type == REDIS_REPLY_STRING)
   {
       LOG(LOG_DBG,"SPOP returned a string %s",spopReply->str);
       strcpy(uid,spopReply->str);
   }
   else 
   {
       LOG(LOG_DBG,"SPOP did not return a string");
   }
   return 0;
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
   // dbgstream = stderr;
   // debug_level = PURGER_LOGLEVEL;

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
                if (optopt == 'd' || optopt == 'h' || optopt == 'p')
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
    CIRCLE_init(argc, argv);
    //CIRCLE_cb_create(&add_objects);
    CIRCLE_cb_process(&process_objects);
    CIRCLE_begin();
    CIRCLE_finalize();
    time(&time_finished);

    //LOG(LOG_INFO, "warnusers run started at: %l", time_started);
    //LOG(LOG_INFO, "warnusers run completed at: %l", time_finished);
    //LOG(LOG_INFO, "warnusers total time (seconds) for this run: %l",
    //    ((double) (time_finished - time_started)) / CLOCKS_PER_SEC);

    exit(EXIT_SUCCESS);
}

/* EOF */
