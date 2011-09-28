#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <time.h>

#include "treewalk.h"
#include "log.h"
#include "sprintstatf.h"
#include "hash.h"

#include <hiredis.h>
#include <async.h>

FILE* TREEWALK_debug_stream;
char         *TOP_DIR;
redisContext *REDIS;

time_t time_started;
time_t time_finished;
#define SECONDS_PER_DAY 60*60*24
#define SECONDS_IN_TWO_WEEKS SECONDS_PER_DAY*14
time_t expire_threshold = SECONDS_IN_TWO_WEEKS;

void
add_objects(CIRCLE_handle *handle)
{
    handle->enqueue(TOP_DIR);
}

void
process_objects(CIRCLE_handle *handle)
{
    DIR *current_dir;
    char temp[CIRCLE_MAX_STRING_LEN];
    char stat_temp[CIRCLE_MAX_STRING_LEN];
    struct dirent *current_ent; 
    struct stat st;

    char *redis_cmd_buf = (char *)malloc(2048 * sizeof(char));

    /* Pop an item off the queue */ 
    handle->dequeue(temp);
    LOG(LOG_DBG, "Popped [%s]", temp);

    /* Try and stat it, checking to see if it is a link */
    if(lstat(temp,&st) != EXIT_SUCCESS)
    {
            LOG(LOG_ERR, "Error: Couldn't stat \"%s\"", temp);
    }
    /* Check to see if it is a directory.  If so, put its children in the queue */
    else if(S_ISDIR(st.st_mode) && !(S_ISLNK(st.st_mode)))
    {
        current_dir = opendir(temp);
        if(!current_dir) {
            LOG(LOG_ERR, "Unable to open dir: %s",temp);
        }
        else
        {
            /* Read in each directory entry */
            while((current_ent = readdir(current_dir)) != NULL)
            {
            /* We don't care about . or .. */
            if((strncmp(current_ent->d_name,".",2)) && (strncmp(current_ent->d_name,"..",3)))
                {
                    strcpy(stat_temp,temp);
                    strcat(stat_temp,"/");
                    strcat(stat_temp,current_ent->d_name);

                    LOG(LOG_DBG, "Pushing [%s] <- [%s]", stat_temp, temp);
                    handle->enqueue(&stat_temp[0]);
                }
            }
        }
        closedir(current_dir);
    }
    else if(S_ISREG(st.st_mode)) {
        treewalk_redis_run_cmd("MULTI", temp);

        char filekey[512];
        treewalk_redis_keygen(filekey, temp);

        /* Create and hset with basic attributes. */
        treewalk_create_redis_attr_cmd(redis_cmd_buf, &st, temp, filekey);
        treewalk_redis_run_cmd(redis_cmd_buf, temp);
        
        /* Check to see if the file is expired.
           If so, zadd it by mtime and add the user id
           to warnlist */
        if(difftime(time_started,st.st_mtime) > SECONDS_IN_TWO_WEEKS)
        {
            LOG(LOG_DBG,"File expired: \"%s\"",temp);
            /* The mtime of the file as a zadd. */
            treewalk_redis_run_zadd(filekey, (long)st.st_mtime, "mtime", temp);
            /* add user to warn list */
            treewalk_redis_run_sadd(&st);
        }
        /* The start time of this treewalk run as a zadd. */
        //treewalk_redis_run_zadd(filekey, (long)time_started, "starttime", temp);

        /* Run all of the cmds. */
        treewalk_redis_run_cmd("EXEC", temp);

    }

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
            "atime_decimal \"%a\" "
            "atime_string  \"%A\" "
            "ctime_decimal \"%c\" "
            "ctime_string  \"%C\" "
            "gid_decimal   \"%g\" "
            "gid_string    \"%G\" "
            "ino           \"%i\" "
            "mtime_decimal \"%m\" "
            "mtime_string  \"%M\" "
            "nlink         \"%n\" "
            "mode_octal    \"%p\" "
            "mode_string   \"%P\" "
            "size          \"%s\" "
            "uid_decimal   \"%u\" "
            "uid_string    \"%U\" ";

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
    LOG(LOG_DBG, "RedisCmd = \"%s\"", cmd);

    if(redisCommand(REDIS, cmd) != NULL)
    {
        LOG(LOG_DBG, "Sent %s to redis", cmd);
    }
    else
    {
        LOG(LOG_DBG, "Failed to SET %s", filename);
        if (REDIS->err)
        {
            LOG(LOG_ERR, "Redis error: %s", REDIS->errstr);
            return -1;
        }
    
    }
    return 0;
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
        LOG(LOG_DBG,"GET returned a string \"%s\"\n", getReply->str);
        return atoi(getReply->str);
    }
    else
        LOG(LOG_DBG,"GET didn't return a string.");
    return -1;
}

int
treewalk_redis_keygen(char *buf, char *filename)
{
    unsigned char filename_hash[32];

    int hash_idx = 0;
    int cnt = 0;

    /* Add the key header */
    cnt += sprintf(buf, "file:");

    /* Generate and add the key */
    treewalk_filename_hash(filename_hash, (unsigned char *)filename);
    for(hash_idx = 0; hash_idx < 32; hash_idx++)
        cnt += sprintf(buf + cnt, "%02x", filename_hash[hash_idx]);

    return cnt;
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

    int dir_flag = 0;
    int force_flag = 0;
    int redis_hostname_flag = 0;
    int redis_port_flag = 0;

    /* Enable logging. */
    TREEWALK_debug_stream = stdout;
   // debug_level = PURGER_LOGLEVEL;

    opterr = 0;
    while((c = getopt(argc, argv, "d:h:p:ft:")) != -1)
    {
        switch(c)
        {
            case 'd':
                TOP_DIR = optarg;
                dir_flag = 1;
                break;

            case 'h':
                redis_hostname = optarg;
                redis_hostname_flag = 1;
                break;

            case 'p':
                redis_port = atoi(optarg);
                redis_port_flag = 1;
                break;

            case 't':
                expire_threshold = atof(optarg) * SECONDS_PER_DAY;
                LOG(LOG_WARN,"Changed file expiration time to %f days, or %lf seconds.",expire_threshold/SECONDS_PER_DAY,expire_threshold);
                break;
            case 'f':
                force_flag = 1;
                break;
            case '?':
                if (optopt == 'd' || optopt == 'h' || optopt == 'p' || optopt == 't')
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

    if(dir_flag == 0)
    {
         print_usage(argv);
         LOG(LOG_FATAL, "You must specify a starting directory");
         exit(EXIT_FAILURE);
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
   int rank = CIRCLE_init(argc, argv);
   char * getCmd = (char *) malloc(sizeof(char)*256);
   sprintf(getCmd,"treewalk-rank-%d",rank);
   int status = treewalk_redis_run_get(getCmd);
   sprintf(getCmd,"set treewalk-rank-%d 1", rank);
    if(status == 1 && !force_flag)
    {
        LOG(LOG_ERR,"Treewalk is already running.  If you wish to continue, verify that there is not a treewalk already running and re-run with -f to force it.");
        CIRCLE_finalize();
        exit(1);
    }
    if(rank == 0 && treewalk_redis_run_cmd(getCmd,"")<0)
    {
        LOG(LOG_ERR,"Unable to %s",getCmd);
        CIRCLE_finalize();
        exit(1);
    }
    CIRCLE_cb_create(&add_objects);
    CIRCLE_cb_process(&process_objects);
    CIRCLE_begin();
    CIRCLE_finalize();
    sprintf(getCmd,"set treewalk-rank-%d 0", rank);
    if(treewalk_redis_run_cmd(getCmd,"")<0)
    {
        fprintf(stderr,"Unable to %s",getCmd);
        exit(1);
    }


    time(&time_finished);

    LOG(LOG_INFO, "treewalk run started at: %l", time_started);
    LOG(LOG_INFO, "treewalk run completed at: %l", time_finished);
    LOG(LOG_INFO, "treewalk total time (seconds) for this run: %l",
        ((double) (time_finished - time_started)) / CLOCKS_PER_SEC);

    exit(EXIT_SUCCESS);
}

/* EOF */
