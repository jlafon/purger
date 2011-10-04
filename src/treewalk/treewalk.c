#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <time.h>

#include "state.h"
#include "treewalk.h"
#include "sprintstatf.h"
#include "hash.h"

#include "../common/log.h"

#include <hiredis.h>
#include <async.h>
#include <mpi.h>

FILE* PURGER_debug_stream;
PURGER_loglevel PURGER_debug_level;
int PURGER_global_rank;
char         *TOP_DIR;
redisContext *REDIS;

double process_objects_total[2];
double hash_time[2];
double redis_time[2];
double stat_time[2];
double readdir_time[2];

time_t time_started;
time_t time_finished;
#define SECONDS_PER_DAY 60.0*60.0*24.0
float expire_threshold = SECONDS_PER_DAY*14.0;

void
add_objects(CIRCLE_handle *handle)
{
    handle->enqueue(TOP_DIR);
}

void
process_objects(CIRCLE_handle *handle)
{
    process_objects_total[0] = MPI_Wtime();
    DIR *current_dir;
    char temp[CIRCLE_MAX_STRING_LEN];
    char stat_temp[CIRCLE_MAX_STRING_LEN];
    struct dirent *current_ent; 
    struct stat st;
    int status = 0;
    char *redis_cmd_buf = (char *)malloc(2048 * sizeof(char));
    /* Pop an item off the queue */ 
    handle->dequeue(temp);
    /* Try and stat it, checking to see if it is a link */
    stat_time[0] = MPI_Wtime();
    status = lstat(temp,&st);
    stat_time[1] += MPI_Wtime()-stat_time[0];
    if(status != EXIT_SUCCESS)
    {
            LOG(PURGER_LOG_ERR, "Error: Couldn't stat \"%s\"", temp);
    }
    /* Check to see if it is a directory.  If so, put its children in the queue */
    else if(S_ISDIR(st.st_mode) && !(S_ISLNK(st.st_mode)))
    {
        current_dir = opendir(temp);
        if(!current_dir) {
            LOG(PURGER_LOG_ERR, "Unable to open dir: %s",temp);
        }
        else
        {
            readdir_time[0] = MPI_Wtime();
            /* Read in each directory entry */
            while((current_ent = readdir(current_dir)) != NULL)
            {
            /* We don't care about . or .. */
            if((strncmp(current_ent->d_name,".",2)) && (strncmp(current_ent->d_name,"..",3)))
                {
                    strcpy(stat_temp,temp);
                    strcat(stat_temp,"/");
                    strcat(stat_temp,current_ent->d_name);

                    LOG(PURGER_LOG_DBG, "Pushing [%s] <- [%s]", stat_temp, temp);
                    handle->enqueue(&stat_temp[0]);
                }
            }
            readdir_time[1] += MPI_Wtime() - readdir_time[0];
        }
        closedir(current_dir);
    }
    else if(S_ISREG(st.st_mode)) {

        redis_time[0] = MPI_Wtime();


        char filekey[512];
        hash_time[0] = MPI_Wtime();
        treewalk_redis_keygen(filekey, temp);
        hash_time[1] += MPI_Wtime() - hash_time[0];
        
        treewalk_redis_run_cmd("MULTI", temp);

        /* Create and hset with basic attributes. */
        treewalk_create_redis_attr_cmd(redis_cmd_buf, &st, temp, filekey);
        treewalk_redis_run_cmd(redis_cmd_buf, temp);
        
        /* Check to see if the file is expired.
           If so, zadd it by mtime and add the user id
           to warnlist */
        if(difftime(time_started,st.st_mtime) > expire_threshold)
        {
            LOG(PURGER_LOG_DBG,"File expired: \"%s\"",temp);
            /* The mtime of the file as a zadd. */
            treewalk_redis_run_zadd(filekey, (long)st.st_mtime, "mtime", temp);
            /* add user to warn list */
            treewalk_redis_run_sadd(&st);
        }
        /* The start time of this treewalk run as a zadd. */
        //treewalk_redis_run_zadd(filekey, (long)time_started, "starttime", temp);

        /* Run all of the cmds. */
        treewalk_redis_run_cmd("EXEC", temp);
        redis_time[1] += MPI_Wtime() - redis_time[0];

    }

    process_objects_total[1] += MPI_Wtime() - process_objects_total[0];
    free(redis_cmd_buf);
}
void
treewalk_redis_run_sadd(struct stat *st)
{
    char *buf = (char*)malloc(2048 * sizeof(char));
    sprintf(buf, "SADD warnlist %d",st->st_uid);
    //!\todo: Use a different function?  This command needs two arguments, but I don't care about the second
    treewalk_redis_run_cmd(buf,buf);
}

int
treewalk_redis_run_zadd(char *filekey, long val, char *zset, char *filename)
{
    int cnt = 0;
    char *buf = (char *)malloc(2048 * sizeof(char));

    cnt += sprintf(buf, "ZADD %s ", zset);
    cnt += sprintf(buf + cnt, "%ld ", val);
    cnt += sprintf(buf + cnt, "%s", filekey);

    treewalk_redis_run_cmd(buf, filename);
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
treewalk_redis_run_cmd(char *cmd, char *filename)
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
treewalk_redis_run_get_str(char * key, char * str)
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
treewalk_redis_run_get(char * key)
{
    char * redis_cmd_buf = (char*)malloc(2048*sizeof(char));
    sprintf(redis_cmd_buf, "GET %s",key);
    redisReply *getReply = redisCommand(REDIS,redis_cmd_buf);
    if(getReply->type == REDIS_REPLY_NIL)
        return -1;
    else if(getReply->type == REDIS_REPLY_STRING)
    {
        LOG(PURGER_LOG_DBG,"GET returned a string \"%s\"\n", getReply->str);
        return atoi(getReply->str);
    }
    else
        LOG(PURGER_LOG_DBG,"GET didn't return a string.");
    return -1;
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
   sprintf(getCmd,"PURGER_STATE");
   int status = treewalk_redis_run_get(getCmd);
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
           treewalk_redis_run_get_str("treewalk_timestamp",time_stamp);
           LOG(PURGER_LOG_INFO,"Treewalk starting normally. Last successfull treewalk: %s",time_stamp);
       }
   }
   sprintf(getCmd,"treewalk-rank-%d",rank);
   status = treewalk_redis_run_get(getCmd);
   sprintf(getCmd,"set treewalk-rank-%d 1", rank);
    if(status == 1 && !force)
    {
        if(rank == 0) LOG(PURGER_LOG_ERR,"Treewalk is already running.  If you wish to continue, verify that there is not a treewalk already running and re-run with -f to force it.");
        return -1;
    }
    if(rank == 0 && treewalk_redis_run_cmd(getCmd,"")<0)
    {
        LOG(PURGER_LOG_ERR,"Unable to %s",getCmd);
        return -1;
    }
   sprintf(getCmd,"set PURGER_STATE %d",PURGER_STATE_TREEWALK);
   if(rank == 0 && treewalk_redis_run_cmd(getCmd,"")<0)
    {
        LOG(PURGER_LOG_ERR,"Unable to %s",getCmd);
        return -1;
    }
  return 0;
}
void
print_usage(char **argv)
{
    fprintf(stderr, "Usage: %s -d <starting directory> [-h <redis_hostname> -p <redis_port> -t <days to expire> -f]\n", argv[0]);
}

int
main (int argc, char **argv)
{
    int index;
    int c;

    char *redis_hostname;
    int redis_port;

    int time_flag = 0;
    int dir_flag = 0;
    int force_flag = 0;
    int restart_flag = 0;
    int redis_hostname_flag = 0;
    int redis_port_flag = 0;

    process_objects_total[2] = 0;
    redis_time[2] = 0;
    stat_time[2] = 0;
    readdir_time[2] = 0;

    
    /* Enable logging. */
    PURGER_debug_stream = stdout;
    PURGER_debug_level = PURGER_LOG_DBG;
    int rank = CIRCLE_init(argc, argv);

    opterr = 0;
    while((c = getopt(argc, argv, "d:h:p:ft:l:r")) != -1)
    {
        switch(c)
        {
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
           
            case 'f':
                force_flag = 1;
                if(rank == 0) LOG(PURGER_LOG_WARN,"Warning: You have chosen to force treewalk.");
                break;
            
            case '?':
                if (optopt == 'd' || optopt == 'h' || optopt == 'p' || optopt == 't' || optopt == 'l')
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
    if(restart_flag && dir_flag)
    {
        if(rank == 0) LOG(PURGER_LOG_WARN, "You have told treewalk to use both checkpoint files and a directory.  You cannot combine these options.\n"
                                    "If you use a directory, treewalk will start from scratch.  If you use a checkpoint file, it will start from\n"
                                    "from the data in the checkpoint files.\n");
        exit(EXIT_FAILURE);
    }
    if(time_flag == 0)
    {
        if(rank == 0) LOG(PURGER_LOG_WARN, "A file timeout value was not specified.  Files older than %.2f seconds (%.2f days) will be expired.",expire_threshold,expire_threshold/(60.0*60.0*24.0));
    }

    if(dir_flag == 0 && !restart_flag)
    {
         print_usage(argv);
         if(rank == 0) LOG(PURGER_LOG_FATAL, "You must specify a starting directory");
         exit(EXIT_FAILURE);
    }

    if(redis_hostname_flag == 0)
    {
        if(rank == 0) LOG(PURGER_LOG_WARN, "A hostname for redis was not specified, defaulting to localhost.");
        redis_hostname = "localhost";
    }

    if(redis_port_flag == 0)
    {
        if(rank == 0) LOG(PURGER_LOG_WARN, "A port number for redis was not specified, defaulting to 6379.");
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
    

   time(&time_started);
   if(treewalk_check_state(rank,force_flag) < 0)
       exit(1);
    if(restart_flag)
        CIRCLE_read_restarts();
    CIRCLE_cb_create(&add_objects);
    CIRCLE_cb_process(&process_objects);
    CIRCLE_begin();
    CIRCLE_finalize();
    
    char getCmd[256];
    sprintf(getCmd,"set treewalk-rank-%d 0", rank);
    if(treewalk_redis_run_cmd(getCmd,"")<0)
    {
        fprintf(stderr,"Unable to %s",getCmd);
        exit(1);
    }


    time(&time_finished);
    char starttime_str[256];
    char endtime_str[256];
    struct tm * localstart = localtime( &time_started );
    struct tm * localend = localtime ( &time_finished );
    strftime(starttime_str, 256, "%b-%d-%Y,%H:%M:%S",localstart);
    strftime(endtime_str, 256, "%b-%d-%Y,%H:%M:%S",localend);
    sprintf(getCmd,"set treewalk_timestamp \"%s\"",endtime_str);
    if(treewalk_redis_run_cmd(getCmd,getCmd)<0)
    {
        fprintf(stderr,"Unable to %s",getCmd);
    }
    if(rank == 0)
    {
        LOG(PURGER_LOG_INFO, "treewalk run started at: %s", starttime_str);
        LOG(PURGER_LOG_INFO, "treewalk run completed at: %s", endtime_str);
        LOG(PURGER_LOG_INFO, "treewalk total time (seconds) for this run: %f",difftime(time_finished,time_started));
        LOG(PURGER_LOG_INFO, "\nTotal time in process_objects: %lf\n\
                   \tRedis commands: %lf %lf\n\
                   \tStating:  %lf %lf\n\
                   \tReaddir: %lf %lf\n\
                   \tHashing: %lf %lf\n",
                   process_objects_total[1],redis_time[1],redis_time[1]/process_objects_total[1]*100.0,stat_time[1],stat_time[1]/process_objects_total[1]*100.0,readdir_time[1],readdir_time[1]/process_objects_total[1]*100.0
                   ,hash_time[1],hash_time[1]/process_objects_total[1]*100.0);
    }
    _exit(EXIT_SUCCESS);
}

/* EOF */
