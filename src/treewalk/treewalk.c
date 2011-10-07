#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>
#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <execinfo.h>

#include "state.h"
#include "treewalk.h"
#include "sprintstatf.h"
#include "hash.h"

#include "log.h"
#include "redis.h"
#include <hiredis.h>
#include <async.h>
#include <mpi.h>

FILE* PURGER_debug_stream;
PURGER_loglevel PURGER_debug_level;
int PURGER_global_rank;
char         *TOP_DIR;


static void treewalk_signal_handler(int signum, struct sigcontext ctx)
{
     if(signum == SIGSEGV)
	LOG(PURGER_LOG_ERR,"Received SIGSEGV (%d), offending address %p",signum,(void*)ctx.cr2);
     LOG(PURGER_LOG_ERR,"Received signal %d",signum);
     redis_handle_sigpipe(); 
     return;
}
int (*redis_command_ptr)(int rank, char * cmd);
double process_objects_total[2];
double hash_time[2];
double redis_time[2];
double stat_time[2];
double readdir_time[2];
int benchmarking_flag;
int sharded_flag;
int sharded_count;
time_t time_started;
time_t time_finished;
#define SECONDS_PER_DAY 60.0*60.0*24.0
float expire_threshold = SECONDS_PER_DAY*14.0;

void
add_objects(CIRCLE_handle *handle)
{
    handle->enqueue(TOP_DIR);
}

void process_dir(char * parent,char * dir, CIRCLE_handle *handle)
{
    DIR *current_dir;
    struct dirent *current_ent; 
    current_dir = opendir(dir);
    if(!current_dir) 
    {
        LOG(PURGER_LOG_ERR, "Unable to open dir: %s",dir);
    }
    else
	{
	    /* Read in each directory entry */
	    while((current_ent = readdir(current_dir)) != NULL)
	    {
	    /* We don't care about . or .. */
	    if((strncmp(current_ent->d_name,".",2)) && (strncmp(current_ent->d_name,"..",3)))
		{
		    strcpy(parent,dir);
		    strcat(parent,"/");
		    strcat(parent,current_ent->d_name);

		    LOG(PURGER_LOG_DBG, "Pushing [%s] <- [%s]", parent, dir);
		    handle->enqueue(&parent[0]);
		}
	    }
	}
	closedir(current_dir);
return;
}

void
process_objects(CIRCLE_handle *handle)
{
    process_objects_total[0] = MPI_Wtime();
    static char temp[CIRCLE_MAX_STRING_LEN];
    static char stat_temp[CIRCLE_MAX_STRING_LEN];
    static char redis_cmd_buf[CIRCLE_MAX_STRING_LEN];
    static char filekey[512];
    static int count = 0;
    struct stat st;
    int status = 0;
    int crc = 0;
    /* Pop an item off the queue */ 
    handle->dequeue(temp);
    /* Try and stat it, checking to see if it is a link */
    stat_time[0] = MPI_Wtime();
    status = lstat(temp,&st);
    stat_time[1] += MPI_Wtime()-stat_time[0];
    if(count++ % 10000 == 0)
	   LOG(PURGER_LOG_INFO,"Count %d",count);
    if(status != EXIT_SUCCESS)
    {
            LOG(PURGER_LOG_ERR, "Error: Couldn't stat \"%s\"", temp);
    }
    /* Check to see if it is a directory.  If so, put its children in the queue */
    else if(S_ISDIR(st.st_mode) && !(S_ISLNK(st.st_mode)))
    {
        readdir_time[0] = MPI_Wtime();
        process_dir(stat_temp,temp,handle); 
        readdir_time[1] += MPI_Wtime() - readdir_time[0];
    }
    else if(!benchmarking_flag && S_ISREG(st.st_mode)) 
    {
        /* Hash the file */
        hash_time[0] = MPI_Wtime();
        treewalk_redis_keygen(filekey, temp);
        crc = (int)crc32(filekey,32) % sharded_count;
        hash_time[1] += MPI_Wtime() - hash_time[0];
        
        /* Create and hset with basic attributes. */
        treewalk_create_redis_attr_cmd(redis_cmd_buf, &st, temp, filekey);
        
        /* Execute the redis command */
	redis_time[0] = MPI_Wtime();
        (*redis_command_ptr)(crc,redis_cmd_buf);
        redis_time[1] += MPI_Wtime() - redis_time[0];

        /* Check to see if the file is expired.
           If so, zadd it by mtime and add the user id
           to warnlist */
        if(difftime(time_started,st.st_mtime) > expire_threshold)
        {
            LOG(PURGER_LOG_DBG,"File expired: \"%s\"",temp);
            redis_time[0] = MPI_Wtime();
            /* The mtime of the file as a zadd. */
            treewalk_redis_run_zadd(filekey, (long)st.st_mtime, "mtime",crc);
            /* add user to warn list */
            treewalk_redis_run_sadd(&st);
            redis_time[1] += MPI_Wtime() - redis_time[0];
        }
    }
    process_objects_total[1] += MPI_Wtime() - process_objects_total[0];
}

void
treewalk_redis_run_sadd(struct stat *st)
{
    char *buf = (char*)malloc(2048 * sizeof(char));
    sprintf(buf, "SADD warnlist %d",st->st_uid);
    (*redis_command_ptr)(st->st_uid % sharded_count,buf);
}

int
treewalk_redis_run_zadd(char *filekey, long val, char *zset, int crc)
{
    int cnt = 0;
    char *buf = (char *)malloc(2048 * sizeof(char));

    cnt += sprintf(buf, "ZADD %s ", zset);
    cnt += sprintf(buf + cnt, "%ld ", val);
    cnt += sprintf(buf + cnt, "%s", filekey);
    (*redis_command_ptr)(crc, buf);
    free(buf);

    return cnt;
}

int
treewalk_create_redis_attr_cmd(char *buf, struct stat *st, char *filename, char *filekey)
{
    int fmt_cnt = 0;
    int buf_cnt = 0;

    char *redis_cmd_fmt = (char *)malloc(2048 * sizeof(char));
    char *redis_cmd_fmt_cnt = \
            "gid_decimal   \"%g\" "
            "mtime_decimal \"%m\" "
            "size          \"%s\" "
            "uid_decimal   \"%u\" ";

    /* Create the start of the command, i.e. "HMSET file:<hash>" */
    fmt_cnt += sprintf(redis_cmd_fmt + fmt_cnt, "HMSET ");

    /* Add in the file key */
    fmt_cnt += sprintf(redis_cmd_fmt + fmt_cnt, "%s", filekey);

    /* Add the filename itself to the redis set command */
    fmt_cnt += sprintf(redis_cmd_fmt + fmt_cnt, " name \"%s\"", filename);

    /* Add the args for sprintstatf */
    fmt_cnt += sprintf(redis_cmd_fmt + fmt_cnt, " %s", redis_cmd_fmt_cnt);

    /* Add the stat struct values. */
    buf_cnt += sprintstatf(buf, redis_cmd_fmt, st);

    free(redis_cmd_fmt);
    return buf_cnt;
}



int
treewalk_redis_keygen(char *buf, char *filename)
{
    static unsigned char hash_buffer[65];
    int cnt = 0;

    treewalk_filename_hash(filename, hash_buffer);
    cnt += sprintf(buf, "file:%s\n", hash_buffer);
    
    return cnt;
}
int
treewalk_check_state(int rank, int force)
{
   char * getCmd = (char *) malloc(sizeof(char)*256);
   sprintf(getCmd,"get PURGER_STATE");
   int status = -1;
   redis_blocking_command(getCmd,(void*)&status,INT);
   if(status == -1)
       LOG(PURGER_LOG_ERR,"Status not set.");
   LOG(PURGER_LOG_DBG,"Status = %d\n",status);
   int transition_check = purger_state_check(status,PURGER_STATE_TREEWALK); 
   if(transition_check == PURGER_STATE_R_NO)
   {
       if(rank == 0) LOG(PURGER_LOG_FATAL,"Error: You cannot run treewalk at this time.  You can only run treewalk after a previous treewalk, or after reaper has been run.");
       return -1;
   }
   else if(transition_check == PURGER_STATE_R_FORCE && !force)
   {
       if(rank == 0) LOG(PURGER_LOG_ERR,"Error: You are asking treewalk to run, but not after reaper has run.  If you are sure you want to do this, run again with -f to force.");
       return -1;
   }
   else if(transition_check == PURGER_STATE_R_YES)
   {
       if(rank == 0)
       {
           char time_stamp[256];
           redis_blocking_command("GET treewalk_timestamp",(void*)time_stamp,CHAR);
           LOG(PURGER_LOG_INFO,"Treewalk starting normally. Last successfull treewalk: %s",time_stamp);
       }
   }
   sprintf(getCmd,"GET treewalk-rank-%d",rank);
   redis_blocking_command(getCmd,(void*)&status,INT);
   sprintf(getCmd,"set treewalk-rank-%d 1", rank);
    if(status == 1 && !force)
    {
        if(rank == 0) LOG(PURGER_LOG_ERR,"Treewalk is already running.  If you wish to continue, verify that there is not a treewalk already running and re-run with -f to force it.");
        return -1;
    }
    if(rank == 0 && (*redis_command_ptr)(0,getCmd)<0)
    {
        LOG(PURGER_LOG_ERR,"Unable to %s",getCmd);
        return -1;
    }
   sprintf(getCmd,"set PURGER_STATE %d",PURGER_STATE_TREEWALK);
   if(rank == 0 && (*redis_command_ptr)(0,getCmd)<0)
    {
        LOG(PURGER_LOG_ERR,"Unable to %s",getCmd);
        return -1;
    }
  return 0;
}
void
print_usage(char **argv)
{
    fprintf(stderr, "Usage: %s -d <starting directory> [-h <redis_hostname> -p <redis_port> -t <days to expire> -f -b]\n", argv[0]);
}

int
main (int argc, char **argv)
{
    int index;
    int c;

    char *redis_hostname;
    char *redis_hostlist;
    int redis_port;

    int time_flag = 0;
    int dir_flag = 0;
    int force_flag = 0;
    int restart_flag = 0;
    int redis_hostname_flag = 0;
    benchmarking_flag = 0;
    sharded_flag = 0;
    int redis_port_flag = 0;

    process_objects_total[2] = 0;
    redis_time[2] = 0;
    stat_time[2] = 0;
    readdir_time[2] = 0;
    redis_command_ptr = &redis_command;
    struct sigaction sa;
    sa.sa_handler = (void *)treewalk_signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    if(sigaction(SIGBUS, &sa, NULL) == -1)
	LOG(PURGER_LOG_ERR,"Failed to set signal handler.");
    if(sigaction(SIGPIPE, &sa, NULL) == -1)
	LOG(PURGER_LOG_ERR,"Failed to set signal handler.");
    if(sigaction(SIGINT, &sa, NULL) == -1)
	LOG(PURGER_LOG_ERR,"Failed to set signal handler.");
    
    /* Enable logging. */
    PURGER_debug_stream = stdout;
    PURGER_debug_level = PURGER_LOG_DBG;
     
    int rank = CIRCLE_init(argc, argv);
    CIRCLE_enable_logging(CIRCLE_LOG_INFO);
    PURGER_global_rank = rank;
    opterr = 0;
    while((c = getopt(argc, argv, "d:h:p:ft:l:rs:b")) != -1)
    {
        switch(c)
        {
            case 'b':
		benchmarking_flag = 1;
		break;
            case 'd':
                TOP_DIR = realpath(optarg, NULL);
                if(rank == 0) LOG(PURGER_LOG_INFO,"Using %s as a root path.",TOP_DIR);
                dir_flag = 1;
                break;
        
            case 'l':
                PURGER_debug_level = atoi(optarg);
                break;

            case 'h':
                redis_hostname = optarg;
                redis_hostname_flag = 1;
                break;

            case 'p':
                redis_port = atoi(optarg);
                redis_port_flag = 1;
                break;
            
            case 'r':
                if(rank == 0) LOG(PURGER_LOG_WARN,"You have specified to use restart files.");
                restart_flag = 1;
                break;

            case 't':
                time_flag = 1;
                expire_threshold = (float)SECONDS_PER_DAY * atof(optarg);
                if(rank == 0) LOG(PURGER_LOG_WARN,"Changed file expiration time to %.2f days, or %.2f seconds.",expire_threshold/(60.0*60.0*24),expire_threshold);
                break;
            case 's':
                sharded_flag = 1;
                redis_hostlist = optarg;
                break;

            case 'f':
                force_flag = 1;
                if(rank == 0) LOG(PURGER_LOG_WARN,"Warning: You have chosen to force treewalk.");
                break;
            
            case '?':
                if (optopt == 'd' || optopt == 'h' || optopt == 'p' || optopt == 't' || optopt == 'l' || optopt == 's')
                {
                    print_usage(argv);
                    fprintf(stderr, "Option -%c requires an argument.\n", optopt);
                }
                else if (isprint (optopt))
                {
                    print_usage(argv);
                    fprintf(stderr, "Unknown option `-%c'.\n", optopt);
                }
                else
                {
                    print_usage(argv);
                    fprintf(stderr,
                        "Unknown option character `\\x%x'.\n",
                        optopt);
                }
                exit(EXIT_FAILURE);
            default:
                abort();
        }
    }
    if(restart_flag && dir_flag && !benchmarking_flag)
    {
        if(rank == 0) LOG(PURGER_LOG_WARN, "You have told treewalk to use both checkpoint files and a directory.  You cannot combine these options.\n"
                                    "If you use a directory, treewalk will start from scratch.  If you use a checkpoint file, it will start from\n"
                                    "from the data in the checkpoint files.\n");
        exit(EXIT_FAILURE);
    }
    if(time_flag == 0 && !benchmarking_flag)
    {
        if(rank == 0) LOG(PURGER_LOG_WARN, "A file timeout value was not specified.  Files older than %.2f seconds (%.2f days) will be expired.",expire_threshold,expire_threshold/(60.0*60.0*24.0));
    }

    if(dir_flag == 0 && !restart_flag)
    {
         print_usage(argv);
         if(rank == 0) LOG(PURGER_LOG_FATAL, "You must specify a starting directory");
         exit(EXIT_FAILURE);
    }

    if(redis_hostname_flag == 0 && !benchmarking_flag)
    {
        if(rank == 0) LOG(PURGER_LOG_WARN, "A hostname for redis was not specified, defaulting to localhost.");
        redis_hostname = "localhost";
    }

    if(redis_port_flag == 0 && !benchmarking_flag)
    {
        if(rank == 0) LOG(PURGER_LOG_WARN, "A port number for redis was not specified, defaulting to 6379.");
        redis_port = 6379;
    }

    for (index = optind; index < argc; index++)
        LOG(PURGER_LOG_WARN, "Non-option argument %s", argv[index]);
    if (!benchmarking_flag && redis_init(redis_hostname,redis_port) < 0)
    {
        LOG(PURGER_LOG_FATAL, "Redis error: %s", REDIS->errstr);
        exit(EXIT_FAILURE);
    }
    

   time(&time_started);
   if(!benchmarking_flag && treewalk_check_state(rank,force_flag) < 0)
       exit(1);
    if(!benchmarking_flag && restart_flag)
        CIRCLE_read_restarts();
    if(!benchmarking_flag && sharded_flag)
    {
        sharded_count = redis_shard_init(redis_hostlist,redis_port);
        redis_command_ptr = &redis_shard_command;
    }
    CIRCLE_cb_create(&add_objects);
    CIRCLE_cb_process(&process_objects);
    CIRCLE_begin();
    
    char getCmd[256];
    sprintf(getCmd,"set treewalk-rank-%d 0", rank);
    if(!benchmarking_flag && redis_blocking_command(getCmd,NULL,INT)<0)
    {
        fprintf(stderr,"Unable to %s",getCmd);
    }

    time(&time_finished);
    char starttime_str[256];
    char endtime_str[256];
    struct tm * localstart = localtime( &time_started );
    struct tm * localend = localtime ( &time_finished );
    strftime(starttime_str, 256, "%b-%d-%Y,%H:%M:%S",localstart);
    strftime(endtime_str, 256, "%b-%d-%Y,%H:%M:%S",localend);
    sprintf(getCmd,"set treewalk_timestamp \"%s\"",endtime_str);
    if(!benchmarking_flag && redis_blocking_command(getCmd,NULL,INT) < 0)
    {
        fprintf(stderr,"Unable to %s",getCmd);
    }
    
    CIRCLE_finalize();
    if(rank == 0)
    {
        LOG(PURGER_LOG_INFO, "treewalk run started at: %s", starttime_str);
        LOG(PURGER_LOG_INFO, "treewalk run completed at: %s", endtime_str);
        LOG(PURGER_LOG_INFO, "treewalk total time (seconds) for this run: %f",difftime(time_finished,time_started));
        LOG(PURGER_LOG_INFO, "\nTotal time in process_objects: %lf\n\
                   \tRedis commands: %lf %lf%%\n\
                   \tStating:  %lf %lf%%\n\
                   \tReaddir: %lf %lf%%\n\
                   \tHashing: %lf %lf%%\n",
                   process_objects_total[1],redis_time[1],redis_time[1]/process_objects_total[1]*100.0,stat_time[1],stat_time[1]/process_objects_total[1]*100.0,readdir_time[1],readdir_time[1]/process_objects_total[1]*100.0
                   ,hash_time[1],hash_time[1]/process_objects_total[1]*100.0);
    }
    if(!benchmarking_flag && sharded_flag)
	redis_shard_finalize();
    if(!benchmarking_flag)
        redis_finalize(); 
    _exit(EXIT_SUCCESS);
}

/* EOF */
