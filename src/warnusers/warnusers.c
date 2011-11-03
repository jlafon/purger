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
#include "redis.h"
#define WARN_STRING_LEN 4096
FILE           *PURGER_debug_stream;
PURGER_loglevel PURGER_debug_level;
int             PURGER_global_rank;
int             sharded_count;
char           *TOP_DIR;

time_t time_started;
time_t time_finished;

int (*redis_command_ptr)(int rank, char *cmd,void * result, returnType ret);

static int non_sharded_redis_command(int rank, char *cmd, void * result, returnType ret)
{
    return redis_blocking_command(cmd,result,ret);
}

mailinfo_t mailinfo; 

void 
warnusers_get_uids(int rank, CIRCLE_handle *handle)
{
    int i = 0;
    int qty = 0;
    char uid[256];
    redis_blocking_shard_command(rank,"scard warnlist",(void*)&qty,INT);
    if(qty <= 0)
    {
        LOG(PURGER_LOG_WARN,"There are no users to warn.");
        return;
    }
    for(i=0; i < qty; i++)
    {
        LOG(PURGER_LOG_DBG,"Popping uid.");
        if(redis_blocking_shard_command(rank,"spop warnlist",(void*)uid,CHAR) < 0)
        {
            LOG(PURGER_LOG_ERR,"Something went badly wrong with the uid get.");
            exit(0);
        }
        else
            handle->enqueue(uid);
    }
    
}
void add_objects(CIRCLE_handle *handle)
{
    if(sharded_count == 0)
        warnusers_get_uids(0,handle);
    else
    {
        int i = 0;
        for(i = 0; i < sharded_count; i++)
        {
            warnusers_get_uids(i,handle);
        }
    }
    return;
}

void
process_uid_list(char * uid_str)
{
    int uid = atoi(uid_str);
    int num_files = 0;
    int i = 0;
    char command_str[WARN_STRING_LEN];
    sprintf(command_str,"llen %d",uid);
    redis_blocking_shard_command(uid % sharded_count,command_str,(void*)&num_files,INT);
    LOG(PURGER_LOG_DBG,"User %d has %d expired files.",uid,num_files);
    for(i = 0; i < num_files; i++)
}

void
process_objects(CIRCLE_handle *handle)
{
    char temp[CIRCLE_MAX_STRING_LEN];
    /* Pop an item off the queue */ 
    LOG(PURGER_LOG_DBG, "Popping, queue has %d elements", handle->local_queue_size());
    handle->dequeue(temp);
    LOG(PURGER_LOG_DBG, "Popped [%s]", temp);
    process_uid_list(temp);
    return;
}

int
warnusers_check_state(int rank, int force)
{
   char * getCmd = (char *) malloc(sizeof(char)*256);
   int status = -1;
   if(redis_blocking_command("get PURGER_STATE",(void*)&status,INT) < 0)
   {
       LOG(PURGER_LOG_ERR,"get PURGER_STATE failed.");
       return -1;
   }
   LOG(PURGER_LOG_DBG,"Current purger state: %d",status);
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
           redis_blocking_command("get warnusers_timestamp",(void*)time_stamp,CHAR);
               LOG(PURGER_LOG_INFO,"Warnusers starting normally. Last successfull warnusers: %s",time_stamp);
       }
   }
   sprintf(getCmd,"get warnusers-rank-%d",rank);
   redis_blocking_command(getCmd,(void*)&status,INT);
   if(status == 1 && !force)
   {
       if(rank == 0) LOG(PURGER_LOG_ERR,"Warnusers is already running.  If you wish to continue, verify that there is not a warnusers already running and re-run with -f to force it.");
       return -1;
   }
   sprintf(getCmd,"set warnusers-rank-%d 1", rank);
   if(rank == 0 && redis_blocking_command(getCmd,(void*)&status,NIL)<0)
   {
       LOG(PURGER_LOG_ERR,"Unable to %s",getCmd);
       return -1;
   }
   sprintf(getCmd,"set PURGER_STATE %d",PURGER_STATE_TREEWALK);
   if(rank == 0 && redis_blocking_command(getCmd,(void*)&status,NIL)<0)
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
    redis_command_ptr = &non_sharded_redis_command;
    PURGER_debug_stream = stdout;
    PURGER_debug_level = PURGER_LOG_DBG;

    int PURGER_global_rank = CIRCLE_init(argc, argv);
    if(PURGER_global_rank < 0)
        exit(1);
    opterr = 0;
    CIRCLE_enable_logging(CIRCLE_LOG_ERR);
    while((c = getopt(argc, argv, "h:p:l:fi:s:")) != -1)
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
                sharded_flag = 1;
                redis_command_ptr = &redis_blocking_shard_command;
                break;

            case '?':
                if (PURGER_global_rank == 0 && (optopt == 'd' || optopt == 'h' || optopt == 'p' || optopt == 'l'))
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

    if(redis_init(redis_hostname, redis_port, db_number) < 0)
    {
        LOG(PURGER_LOG_ERR,"Unable to initialize redis.");
        exit(0);
    }
    if(sharded_flag)
    {
        LOG(PURGER_LOG_INFO,"Sharding enabled.");
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
    CIRCLE_cb_create(&add_objects);
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
    if(redis_blocking_command(getCmd,(void*)NULL,NIL)<0)
    {   
         LOG(PURGER_LOG_ERR,"Unable to %s",getCmd);
    }  
    sprintf(getCmd,"set warnusers-rank-%d 0", PURGER_global_rank);
    if(redis_blocking_command(getCmd,(void*)NULL,NIL)<0)
    {
         LOG(PURGER_LOG_ERR,"Unable to %s",getCmd);
    }
    redis_finalize();
    if(sharded_flag)
        redis_shard_finalize();
    if(PURGER_global_rank == 0)
    {
        LOG(PURGER_LOG_INFO, "Warnusers run started at: %s", starttime_str);
        LOG(PURGER_LOG_INFO, "Warnusers run completed at: %s", endtime_str);
        LOG(PURGER_LOG_INFO, "Warnusers total time (seconds) for this run: %f",difftime(time_finished,time_started));
    }
    exit(EXIT_SUCCESS);
}

/* EOF */
