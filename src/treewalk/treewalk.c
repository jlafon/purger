/**
  *
  * @file treewalk.c
  * @authors Jharrod LaFon, Jon Bringhurst
  *
  */
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
#define SECONDS_PER_DAY 60*60*24

/* Output for debugging */
FILE*           PURGER_debug_stream;

/* Verbosity */
PURGER_loglevel PURGER_debug_level;

/* Each rank's global id */
int             PURGER_global_rank;

/* File tree root */
char*           TOP_DIR;

/**
  * Function pointer used to call the correct redis function (sharded or not sharded).
  * This avoids a comparison for every database call.
  */
int (*redis_command_ptr)(int rank, char * cmd);

/* Global accounting/performance variables */
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
int file_count;
int dir_count;

/* Default time for which a file expires */
float expire_threshold = SECONDS_PER_DAY*14.0;

/**
  * Signal handler used to catch signals sent to treewalk.
  */
static void 
treewalk_signal_handler(int signum, struct sigcontext ctx)
{
     if(signum == SIGSEGV)
         LOG(PURGER_LOG_ERR,"Received SIGSEGV (%d), offending address %p",signum,(void*)ctx.cr2);
     LOG(PURGER_LOG_ERR,"Received signal %d",signum);
     redis_handle_sigpipe(); 
     return;
}

/* Call back given to initialize the data set.  Adds the first path to the queue. */
void
add_objects(CIRCLE_handle *handle)
{
    handle->enqueue(TOP_DIR);
}


/*
 * Handles one directory.  Adds all its children to the queue.
 */
void 
process_dir(char * parent,char * dir, CIRCLE_handle *handle)
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

/*
 * Creates a command like: "LPUSH <uid> <filekey>" to append
 * an expired file key to a user's list.
 */
void
treewalk_create_redis_lpush_cmd(char * cmd, char * key, int uid)
{
    sprintf(cmd,"LPUSH %d %s",uid,key);
}
void
treewalk_create_redis_expire_cmd(char * cmd, char * key)
{
    sprintf(cmd,"EXPIRE %s %d",key,((int)SECONDS_PER_DAY*7));
}
/*
 * Creates a command to add a user to the list of users to be warned.
 */
void treewalk_redis_run_sadd(struct stat *st)
{
    char *buf = (char*)malloc(2048 * sizeof(char));
    sprintf(buf, "SADD warnlist %d",st->st_uid);
    (*redis_command_ptr)(st->st_uid % sharded_count,buf);
}

/*
 * Callback for handling work queue elements. 
 */
void
process_objects(CIRCLE_handle *handle)
{
    process_objects_total[0] = MPI_Wtime();
    static char temp[CIRCLE_MAX_STRING_LEN];
    static char stat_temp[CIRCLE_MAX_STRING_LEN];
    static char redis_cmd_buf[CIRCLE_MAX_STRING_LEN];
    static char filekey[512];
    struct stat st;
    int status = 0;
    int crc = 0;
    int count = 0;
    for(count = 0; count < 10; count++)
    {
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
            // break loop
            count = 10;
            dir_count++;
            readdir_time[0] = MPI_Wtime();
            process_dir(stat_temp,temp,handle); 
            readdir_time[1] += MPI_Wtime() - readdir_time[0];
        }
        else if(!benchmarking_flag && S_ISREG(st.st_mode)) 
        {
            file_count++;
            /* Hash the file */
            hash_time[0] = MPI_Wtime();
            treewalk_redis_keygen(filekey, temp);
            crc = (int)purger_crc32(filekey,32) % sharded_count;
            hash_time[1] += MPI_Wtime() - hash_time[0];
            
            /* Create and hset with basic attributes. */
            treewalk_create_redis_attr_cmd(redis_cmd_buf, &st, temp, filekey);
            
            /* Execute the redis command */
            redis_time[0] = MPI_Wtime();
            (*redis_command_ptr)(crc,redis_cmd_buf);
            redis_time[1] += MPI_Wtime() - redis_time[0];
            treewalk_create_redis_expire_cmd(redis_cmd_buf,filekey);
            (*redis_command_ptr)(st.st_uid % sharded_count,redis_cmd_buf);

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
                /* add file to list of expired files for that user */
                treewalk_create_redis_lpush_cmd(redis_cmd_buf,filekey, st.st_uid);
                (*redis_command_ptr)(st.st_uid % sharded_count,redis_cmd_buf);
                redis_time[1] += MPI_Wtime() - redis_time[0];
            }
        }
    }
    process_objects_total[1] += MPI_Wtime() - process_objects_total[0];
}
/*
 * Function to run a ZADD in redis.
 */
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

/*
 * Helper function to create a redis command from a file's attributes.
 */
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

    char * encoded_filename = purger_base64_encode(filename);
    /* Create the start of the command, i.e. "HMSET file:<hash>" */
    fmt_cnt += sprintf(redis_cmd_fmt + fmt_cnt, "HMSET ");

    /* Add in the file key */
    fmt_cnt += sprintf(redis_cmd_fmt + fmt_cnt, "%s", filekey);

    /* Add the filename itself to the redis set command */
    fmt_cnt += sprintf(redis_cmd_fmt + fmt_cnt, " name \"%s\"", encoded_filename);

    /* Add the args for sprintstatf */
    fmt_cnt += sprintf(redis_cmd_fmt + fmt_cnt, " %s", redis_cmd_fmt_cnt);

    /* Add the stat struct values. */
    buf_cnt += sprintstatf(buf, redis_cmd_fmt, st);

    free(redis_cmd_fmt);
    free(encoded_filename);
    return buf_cnt;
}

/*
 * Helper function to hash a filename into a key.
 */
int
treewalk_redis_keygen(char *buf, char *filename)
{
    static unsigned char hash_buffer[65];
    int cnt = 0;
    purger_filename_hash(filename, hash_buffer);
    cnt += sprintf(buf, "file:%s ", hash_buffer);
    return cnt;
}

/*
 * Asserts that the state transition for purger is valid.
 */
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

/*
 * Prints usage.
 */
void
print_usage(char **argv)
{
    fprintf(stderr, "Usage: %s -d <starting directory> [-h <redis_hostname> -p <redis_port> -t <days to expire> -f -b]\n", argv[0]);
}

/*
 * Initializes global variables.
 */
void 
treewalk_init_globals()
{
    benchmarking_flag = 0;
    sharded_flag = 0;
    process_objects_total[2] = 0;
    redis_time[2] = 0;
    stat_time[2] = 0;
    readdir_time[2] = 0;
    redis_command_ptr = &redis_command;
    dir_count = 0;
    file_count = 0; 
    opterr = 0;
    return; 
}

/*
 * Installs signal handler functions.
 */
void 
treewalk_install_signal_handlers()
{
    struct sigaction * sa = (struct sigaction *) malloc(sizeof(struct sigaction));
    sa->sa_handler = (void *)treewalk_signal_handler;
    sigemptyset(&sa->sa_mask);
    sa->sa_flags = SA_RESTART;
    if(sigaction(SIGBUS, sa, NULL) == -1)
        LOG(PURGER_LOG_ERR,"Failed to set signal handler.");
    if(sigaction(SIGPIPE, sa, NULL) == -1)
        LOG(PURGER_LOG_ERR,"Failed to set signal handler.");
    if(sigaction(SIGINT, sa, NULL) == -1)
        LOG(PURGER_LOG_ERR,"Failed to set signal handler.");
    return; 
}

/*
 * Process user options and set flags.
 */
void 
treewalk_process_options(int argc, char **argv,treewalk_options_st * opts)
{
    int c;
    int index;
    int dir_flag = 0;
    int redis_hostname_flag = 0;
    int redis_port_flag = 0;
    int time_flag = 0;
    /* Parse options */ 
    while((c = getopt(argc, argv, "d:h:p:ft:l:rs:bi:")) != -1)
    {
        switch(c)
        {
            case 'b':
                benchmarking_flag = 1;
                break;
            case 'i':
                opts->db_number = atoi(optarg);
                break;
            case 'd':
                TOP_DIR = realpath(optarg, NULL);
                if(opts->rank == 0) LOG(PURGER_LOG_INFO,"Using %s as a root path.",TOP_DIR);
                dir_flag = 1;
                break;
        
            case 'l':
                PURGER_debug_level = atoi(optarg);
                break;

            case 'h':
                opts->redis_hostname = optarg;
                redis_hostname_flag = 1;
                break;

            case 'p':
                opts->redis_port = atoi(optarg);
                redis_port_flag = 1;
                break;
            
            case 'r':
                if(opts->rank == 0) LOG(PURGER_LOG_WARN,"You have specified to use restart files.");
                opts->restart_flag = 1;
                break;

            case 't':
                time_flag = 1;
                opts->expire_threshold = (float)SECONDS_PER_DAY * atof(optarg);
                if(opts->rank == 0) LOG(PURGER_LOG_WARN,"Changed file expiration time to %.2f days, or %.2f seconds.",expire_threshold/(60.0*60.0*24),expire_threshold);
                break;
            case 's':
                sharded_flag = 1;
                opts->redis_hostlist = optarg;
                break;

            case 'f':
                opts->force_flag = 1;
                if(opts->rank == 0) LOG(PURGER_LOG_WARN,"Warning: You have chosen to force treewalk.");
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
    if(opts->restart_flag && dir_flag && !benchmarking_flag)
    {
        if(opts->rank == 0) LOG(PURGER_LOG_WARN, "You have told treewalk to use both checkpoint files and a directory.  You cannot combine these options.\n"
                                    "If you use a directory, treewalk will start from scratch.  If you use a checkpoint file, it will start from\n"
                                    "from the data in the checkpoint files.\n");
        exit(EXIT_FAILURE);
    }
    if(time_flag == 0 && !benchmarking_flag)
    {
        if(opts->rank == 0) LOG(PURGER_LOG_WARN, "A file timeout value was not specified.  Files older than %.2f seconds (%.2f days) will be expired.",opts->expire_threshold,opts->expire_threshold/(60.0*60.0*24.0));
    }

    if(dir_flag == 0 && !opts->restart_flag)
    {
         print_usage(argv);
         if(opts->rank == 0) LOG(PURGER_LOG_FATAL, "You must specify a starting directory");
         exit(EXIT_FAILURE);
    }

    if(redis_hostname_flag == 0 && !benchmarking_flag)
    {
        if(opts->rank == 0) LOG(PURGER_LOG_WARN, "A hostname for redis was not specified, defaulting to localhost.");
        opts->redis_hostname = "localhost";
    }

    if(redis_port_flag == 0 && !benchmarking_flag)
    {
        if(opts->rank == 0) LOG(PURGER_LOG_WARN, "A port number for redis was not specified, defaulting to 6379.");
        opts->redis_port = 6379;
    }

    for (index = optind; index < argc; index++)
        LOG(PURGER_LOG_WARN, "Non-option argument %s", argv[index]);
}

/*
 * Initialize the options struct
 */
void 
treewalk_init_opts(treewalk_options_st * opts)
{
    opts->redis_hostname = NULL;
    opts->redis_hostlist = NULL;
    opts->redis_port = 0;
    opts->force_flag = 0;
    opts->db_number = 0;
    opts->rank = 0;
    opts->restart_flag = 0;
    opts->expire_threshold = SECONDS_PER_DAY*14;
}

/*
 * Main
 */
int
main (int argc, char **argv)
{
    /* Locals */
    char starttime_str[256];
    char endtime_str[256];
    char getCmd[256];

    treewalk_options_st opts;    
    treewalk_init_opts(&opts);

    /* Globals */
    treewalk_init_globals();

    /* Set up signal handler */
    treewalk_install_signal_handlers();

    /* Enable logging. */
    PURGER_debug_stream = stdout;
    PURGER_debug_level = PURGER_LOG_DBG;
    
    /* Init lib circle */
    int rank = CIRCLE_init(argc, argv);
    if(rank < 0)
        exit(1);
    CIRCLE_enable_logging(CIRCLE_LOG_ERR);
    PURGER_global_rank = rank;
    opts.rank = rank;

    /* Process command line options */    
    treewalk_process_options(argc,argv,&opts); 

    /* Init redis */
    if (!benchmarking_flag && redis_init(opts.redis_hostname,opts.redis_port,opts.db_number) < 0)
    {
        LOG(PURGER_LOG_FATAL, "Redis error: %s", REDIS->errstr);
        exit(EXIT_FAILURE);
    }
    
    /* Timing */
    time(&time_started);
    
    /* Ensure it's OK to run at this time */
    if(!benchmarking_flag && treewalk_check_state(opts.rank,opts.force_flag) < 0)
       exit(1);

    /* Read from restart files */
    if(!benchmarking_flag && opts.restart_flag)
        CIRCLE_read_restarts();

    /* Enable sharding */
    if(!benchmarking_flag && sharded_flag)
    {
        sharded_count = redis_shard_init(opts.redis_hostlist,opts.redis_port,opts.db_number);
        redis_command_ptr = &redis_shard_command;
    }

    /* Parallel section */
    CIRCLE_cb_create(&add_objects);
    CIRCLE_cb_process(&process_objects);
    CIRCLE_begin();
    /* End parallel section (well, kind of) */   

    /* Set state */ 
    sprintf(getCmd,"set treewalk-rank-%d 0", rank);
    if(!benchmarking_flag && redis_blocking_command(getCmd,NULL,INT)<0)
    {
          fprintf(stderr,"Unable to %s",getCmd);
    }
     
    time(&time_finished);
    struct tm * localstart = localtime( &time_started );
    struct tm * localend = localtime ( &time_finished );
    strftime(starttime_str, 256, "%b-%d-%Y,%H:%M:%S",localstart);
    strftime(endtime_str, 256, "%b-%d-%Y,%H:%M:%S",localend);
    sprintf(getCmd,"set treewalk_timestamp \"%s\"",endtime_str);
    if(!benchmarking_flag && redis_blocking_command(getCmd,NULL,INT) < 0)
    {
        fprintf(stderr,"Unable to %s",getCmd);
    }
    LOG(PURGER_LOG_INFO,"Files: %d\tDirs: %d\tTotal: %d\n",file_count,dir_count,file_count+dir_count);
    if(!benchmarking_flag && sharded_flag)
        redis_shard_finalize();
    if(!benchmarking_flag)
        redis_finalize(); 
   
    if(rank == 0)
    {
        LOG(PURGER_LOG_INFO, "treewalk run started at: %s", starttime_str);
        LOG(PURGER_LOG_INFO, "treewalk run completed at: %s", endtime_str);
        LOG(PURGER_LOG_INFO, "treewalk total time (seconds) for this run: %f",difftime(time_finished,time_started));
    }
        LOG(PURGER_LOG_INFO, "\nTotal time in process_objects: %lf\n\
                   \tRedis commands: %lf %lf%%\n\
                   \tStating:  %lf %lf%%\n\
                   \tReaddir: %lf %lf%%\n\
                   \tHashing: %lf %lf%%\n",
                   process_objects_total[1],redis_time[1],redis_time[1]/process_objects_total[1]*100.0,stat_time[1],stat_time[1]/process_objects_total[1]*100.0,readdir_time[1],readdir_time[1]/process_objects_total[1]*100.0
                   ,hash_time[1],hash_time[1]/process_objects_total[1]*100.0);
    CIRCLE_finalize();
    _exit(EXIT_SUCCESS);
}

/* EOF */
