#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>
#include <limits.h>
#include <hiredis.h>

#include "config.h"

#include "local.h"
#include "../common/log.h"

extern redisContext *REDIS;
extern int PURGER_global_rank;

static unsigned long int
reaper_strtoul(const char *nptr, int *ret_code)
{
    unsigned long int value;

    /* assume all is well */
    *ret_code = 1;

    /* check for strtoul errors */
    errno = 0;
    value = strtoul(nptr, NULL, 10);

    if ((ERANGE == errno && (ULONG_MAX == value || 0 == value)) ||
        (0 != errno && 0 == value)) {
        *ret_code = -1;
    }
    if (nptr == NULL) {
        *ret_code = -2;
    }

    /* caller must always check the return code */
    return value;
}

void
reaper_check_local_queue(CIRCLE_handle *handle, char *key)
{
    redisReply *hmgetReply = redisCommand(REDIS, "HMGET %s mtime_decimal name", key);
    if(hmgetReply->type == REDIS_REPLY_ARRAY)
    {
        LOG(LOG_DBG, "Hmget returned an array of size: %zu", hmgetReply->elements);

        if(hmgetReply->element[1]->type != REDIS_REPLY_STRING || \
                hmgetReply->element[0]->type != REDIS_REPLY_STRING || \
                hmgetReply->elements != 2)
        {
            LOG(LOG_ERR, "Hmget elements were not in the correct format (bad key? \"%s\")", key);
            return;
        }
        else
        {
            char *filename = hmgetReply->element[1]->str + 1;
            filename[strlen(filename)-1] = '\0';
            char *mtime_str = hmgetReply->element[0]->str + 1;
            mtime_str[strlen(mtime_str)-1] = '\0';

            long int db_mtime_number = reaper_mtime_to_number(mtime_str);

            if(db_mtime_number > 0)
            {
                /* It looks like we might delete this one. Lets run it through a final check. */
                reaper_check_and_delete_file(filename, db_mtime_number);
            }
        }
    }
    else
    {
        LOG(LOG_ERR, "Redis didn't return an array when trying to hmget %s.", key);
    }
}

long int
reaper_mtime_to_number(char *mtime_str)
{
    int status = 0;
    long int mtime_num = 0;

    mtime_num = reaper_strtoul(mtime_str, &status);

    if(status <= 0)
    {
        LOG(LOG_DBG, "The mtime string conversion failed: from \"%s\" to \"%ld\"", mtime_str, mtime_num);
        return -1;
    }

    return mtime_num;
}

void
reaper_check_and_delete_file(char *filename, long int db_mtime)
{
    struct stat new_stat_buf;

    if(lstat(filename, &new_stat_buf) != 0)
    {
        LOG(LOG_DBG, "The stat of the potential file failed (%s): %s", strerror(errno), filename);
        return;
    }
    else
    {
        int convert_status = 0;
        long int new_mtime = (long int)new_stat_buf.st_mtime;

        if(convert_status <= 0)
        {
            LOG(LOG_DBG, "The mtime string conversion failed: \"%ld\"", db_mtime);
        }
        else
        {
            if(db_mtime == new_mtime)
            {
                /* The file hasn't been modified since we last saw it */
                long int cur_time = time(NULL);
                long int age_secs = cur_time - new_mtime;
                long int age_days = (long int)(age_secs / SECONDS_IN_A_DAY);

                if(age_days > 6)
                {
                    LOG(LOG_DBG, "File has been unlinked: %s", filename);
                    // TODO: unlink(filename);
                }
                else
                {
                    LOG(LOG_DBG, "The file is too new to be purged: %s", filename);
                    /* TODO: Check to see if the ZREM from the atomic pop worked. */
                    return;
                }
            }
            else
            {
                LOG(LOG_DBG, "File was modified (diff %ld): %s", new_mtime - db_mtime, filename);
            }
        }
    }
}

/* EOF */
