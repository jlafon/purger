#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <time.h>

#include "state.h"
#include "warnusers.h"
#include "mail.h"
#include "log.h"

#include <hiredis.h>
#include <async.h>
FILE           *PURGER_debug_stream;
PURGER_loglevel PURGER_debug_level;
int             PURGER_global_rank;
int             sharded_count;
char           *TOP_DIR;
redisContext   *REDIS;

time_t time_started;
time_t time_finished;

int (*redis_command_ptr)(int rank, char *cmd);

mailinfo_t mailinfo; 
void
add_objects(CIRCLE_handle *handle)
{
    char buf[256];
    if(warnusers_redis_run_spop(buf)< 0)
    {
        LOG(PURGER_LOG_WARN,"No elements in set.");
        int status = warnusers_redis_run_get("treewalk");
        if(status == 0)
        {
            LOG(PURGER_LOG_WARN,"Treewalk no longer running, and the set is empty.  Exiting warnusers.");
            exit(0);
        }
        else if (status == 1)
        {
            LOG(PURGER_LOG_WARN,"Treewalk is running, but the set is empty.  Exiting warnusers.");
            exit(0);
        }
    }
    handle->enqueue(buf);
}

void 
warnusers_get_uids(CIRCLE_handle *handle)
{
    int i = 0;
    int qty = 0;
    char uid[256];
    qty = warnusers_redis_run_scard("warnlist");
    if(qty <= 0)
    {
        LOG(PURGER_LOG_WARN,"There are no users to warn.");
        return;
    }
    for(i=0; i < qty; i++)
    {
        if(warnusers_redis_run_spop(uid) == -1)
        {
            LOG(PURGER_LOG_ERR,"Something went badly wrong with the uid get.");
            exit(0);
        }
        handle->enqueue(uid);
    }
    
}

void
process_objects(CIRCLE_handle *handle)
{
    char uid[256];
    if(PURGER_global_rank == 0)
    {
        if(sharded_count == 0)
            warnusers_get_uids(handle);
        else
        {
            int i = 0;
            for(i = 0; i < sharded_count; i++)
            {
                
            }
        }
    }
    else
    {
        char temp[CIRCLE_MAX_STRING_LEN];
        /* Pop an item off the queue */ 
        LOG(PURGER_LOG_DBG, "Popping, queue has %d elements", handle->local_queue_size());
        handle->dequeue(temp);
        LOG(PURGER_LOG_DBG, "Popped [%s]", temp);
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

int
warnusers_redis_run_cmd(char *cmd, char *filename)
{
    LOG(PURGER_LOG_DBG, "RedisCmd = \"%s\"", cmd);
    redisReply *reply = redisCommand(REDIS, cmd);
    if(reply->type != REDIS_REPLY_ERROR)
    {   
        LOG(PURGER_LOG_DBG, "Sent %s to redis", cmd);
    }   
    else
    {   
        LOG(PURGER_LOG_DBG, "Failed %s: %s", cmd,reply->str);
        if (REDIS->err)
        {   
            LOG(PURGER_LOG_ERR, "Redis error: %s", REDIS->errstr);
            return -1; 
        }   
    
    }   
    return 0;
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
        LOG(PURGER_LOG_DBG,"GET returned a string \"%s\"\n",getReply->str);
        return atoi(getReply->str);
    }
    else if(getReply->type == REDIS_REPLY_INTEGER)
    {
        LOG(PURGER_LOG_DBG,"GET returned an int: %lld.",getReply->integer);
        return getReply->integer;
    }
    else
        LOG(PURGER_LOG_DBG,"GET returned something else.");
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
        LOG(PURGER_LOG_DBG,"GET returned a string \"%s\"\n",getReply->str);
        return atoi(getReply->str);
    }
    else
        LOG(PURGER_LOG_DBG,"GET didn't return a string");
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
       LOG(PURGER_LOG_DBG,"SPOP returned a string %s",spopReply->str);
       strcpy(uid,spopReply->str);
   }
   else 
   {
       LOG(PURGER_LOG_DBG,"SPOP did not return a string");
   }
   return 0;
}

int
warnusers_redis_run_get_str(char * key, char * str)
{
    char * redis_cmd_buf = (char*)malloc(2048*sizeof(char));
    sprintf(redis_cmd_buf, "GET %s",key);
    redisReply *getReply = redisCommand(REDIS,redis_cmd_buf);
    if(getReply->type == REDIS_REPLY_NIL)
        return -1;
    else if(getReply->type == REDIS_REPLY_STRING)
    {
        LOG(PURGER_LOG_DBG,"GET returned a string \"%s\"\n", getReply->str);
        strcpy(str,getReply->str);
        return 0;
    }
    else
        LOG(PURGER_LOG_DBG,"GET didn't return a string.");
    return -1;
}


int
warnusers_check_state(int rank, int force)
{
   char * getCmd = (char *) malloc(sizeof(char)*256);
   sprintf(getCmd,"PURGER_STATE");
   int status = warnusers_redis_run_get(getCmd);
   int transition_check = purger_state_check(status,PURGER_STATE_WARNUSERS); 
   if(transition_check == PURGER_STATE_R_NO)
   {
       if(rank == 0) LOG(PURGER_LOG_FATAL,"Error: You cannot run warnusers at this time.  You can only run warnusers after a previous warnusers, or after treewalk has been run.");
       return -1;
   }
   else if(transition_check == PURGER_STATE_R_FORCE && !force)
   {
       if(rank == 0) LOG(PURGER_LOG_ERR,"Error: You are asking warnusers to run, but not after treewalk has run.  If you are sure you want to do this, run again with -f to force.");
       return -1;
   }
   else if(transition_check == PURGER_STATE_R_YES)
   {
       if(rank == 0)
       {
           char time_stamp[256];
           warnusers_redis_run_get_str("warnusers_timestamp",time_stamp);
           LOG(PURGER_LOG_INFO,"Warnusers starting normally. Last successfull warnusers: %s",time_stamp);
       }
   }
   sprintf(getCmd,"warnusers-rank-%d",rank);
   status = warnusers_redis_run_get(getCmd);
   sprintf(getCmd,"set warnusers-rank-%d 1", rank);
   if(status == 1 && !force)
   {
       if(rank == 0) LOG(PURGER_LOG_ERR,"Warnusers is already running.  If you wish to continue, verify that there is not a warnusers already running and re-run with -f to force it.");
       return -1;
   }
   if(rank == 0 && warnusers_redis_run_cmd(getCmd,"")<0)
   {
       LOG(PURGER_LOG_ERR,"Unable to %s",getCmd);
       return -1;
   }
   sprintf(getCmd,"set PURGER_STATE %d",PURGER_STATE_TREEWALK);
   if(rank == 0 && warnusers_redis_run_cmd(getCmd,"")<0)
   {
       LOG(PURGER_LOG_ERR,"Unable to %s",getCmd);
       return -1;
   }
   return 0;
}


void
print_usage(char **argv)
{
    fprintf(stderr, "Usage: %s [-h <redis_hostname> -p <redis_port> -f -l <loglevel>]\n", argv[0]);
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
    int sharded_flag = 0;
    int force_flag = 0;
    int db_number = 0;
    sharded_count = 0;
    char * redis_host_list;
    PURGER_debug_stream = stdout;
    PURGER_debug_level = PURGER_LOG_DBG;

    int PURGER_global_rank = CIRCLE_init(argc, argv);
    if(PURGER_global_rank < 0)
        exit(1);
    opterr = 0;
    CIRCLE_enable_logging(CIRCLE_LOG_ERR);
    while((c = getopt(argc, argv, "h:p:l:f")) != -1)
    {
        switch(c)
        {
            case 'h':
                redis_hostname = optarg;
                redis_hostname_flag = 1;
                break;

            case 'i':
                db_number = atoi(optarg);
                break;

            case 'p':
                redis_port = atoi(optarg);
                redis_port_flag = 1;
                break;

            case 'f':
                force_flag = 1;
                if(PURGER_global_rank == 0) LOG(PURGER_LOG_WARN,"Warning: You have chosen to force warnusers.");
                break; 
            case 'l':
                PURGER_debug_level = atoi(optarg);
                break;
            
            case 's':
                redis_host_list = optarg;
                sharded_flag;
                break;

            case '?':
                if (optopt == 'd' || optopt == 'h' || optopt == 'p' || optopt == 'l')
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
        if(PURGER_global_rank == 0) LOG(PURGER_LOG_WARN, "A hostname for redis was not specified, defaulting to localhost.");
        redis_hostname = "localhost";
    }

    if(redis_port_flag == 0)
    {
        if(PURGER_global_rank == 0) LOG(PURGER_LOG_WARN, "A port number for redis was not specified, defaulting to 6379.");
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
    
    if(sharded_flag)
    {
        sharded_count = redis_shard_init(redis_host_list, redis_port, db_number);
    }

    mailinfo.from     = "consult@lanl.gov";
    mailinfo.fromreal = "ydms-master@lanl.gov";
    mailinfo.subject = "[PURGER NOTIFICATION]";
    mailinfo.defaultto = "nfs@lanl.gov";
    mailinfo.server = "mail.lanl.gov";
    mailinfo.txt = "The following text file in the Turquiose network contains a list of       \
                   your scratch files that have not been modified in the last 14+ days.       \
                   Those files will be deleted in at least 6 days if not modified by then.    \ 
                   This notification may not have up-to-the-minute information, but we        \
                   will verify a file's actual age before purging it.   For more information, \
                    please see our purge policy:  http://hpc.lanl.gov/purge_policy.           \
                   If you have questions or concerns, please contact ICN Consultants          \
                   at 505-665-4444 option 3."; 

    time(&time_started);
    if(warnusers_check_state(PURGER_global_rank,force_flag) < 0)
        exit(1);
    CIRCLE_cb_process(&process_objects);
    CIRCLE_begin();
    CIRCLE_finalize();
    time(&time_finished);
    char starttime_str[256];
    char endtime_str[256];
    struct tm * localstart = localtime( &time_started );
    struct tm * localend = localtime ( &time_finished );
    strftime(starttime_str, 256, "%b-%d-%Y,%H:%M:%S",localstart);
    strftime(endtime_str, 256, "%b-%d-%Y,%H:%M:%S",localend);
    char getCmd[256];
    sprintf(getCmd,"set warnusers_timestamp \"%s\"",endtime_str);
    if(warnusers_redis_run_cmd(getCmd,getCmd)<0)
    {   
         LOG(PURGER_LOG_ERR,"Unable to %s",getCmd);
    }  
    sprintf(getCmd,"set warnusers-rank-%d 0", PURGER_global_rank);
    if(warnusers_redis_run_cmd(getCmd,getCmd)<0)
    {
         LOG(PURGER_LOG_ERR,"Unable to %s",getCmd);
    }
    if(PURGER_global_rank == 0)
    {
        LOG(PURGER_LOG_INFO, "Warnusers run started at: %s", starttime_str);
        LOG(PURGER_LOG_INFO, "Warnusers run completed at: %s", endtime_str);
        LOG(PURGER_LOG_INFO, "Warnusers total time (seconds) for this run: %f",difftime(time_finished,time_started));
    }
    exit(EXIT_SUCCESS);
}

/* EOF */
