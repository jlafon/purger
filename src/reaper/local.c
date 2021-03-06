#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <hiredis.h>

#include "config.h"

#include "local.h"
#include "../common/log.h"

extern redisContext *REDIS;
extern int PURGER_global_rank;

unsigned long int
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
reaper_check_local_queue(char *key)
{
    redisReply *hmgetReply = redisCommand(REDIS, "HMGET %s mtime_decimal name", key);
    if(hmgetReply->type == REDIS_REPLY_ARRAY)
    {
        LOG(PURGER_LOG_DBG, "Hmget returned an array of size: %zu", hmgetReply->elements);

        if(hmgetReply->element[1]->type != REDIS_REPLY_STRING || \
                hmgetReply->element[0]->type != REDIS_REPLY_STRING || \
                hmgetReply->elements != 2)
        {
            LOG(PURGER_LOG_ERR, "Hmget elements were not in the correct format (bad key? \"%s\")", key);
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
        LOG(PURGER_LOG_ERR, "Redis didn't return an array when trying to hmget %s.", key);
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
        LOG(PURGER_LOG_DBG, "The mtime string conversion failed: from \"%s\" to \"%ld\"", mtime_str, mtime_num);
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
        LOG(PURGER_LOG_DBG, "The stat of the potential file failed (%s): %s", strerror(errno), filename);
        return;
    }
    else
    {
        int convert_status = 0;
        long int new_mtime = (long int)new_stat_buf.st_mtime;

        if(convert_status <= 0)
        {
            LOG(PURGER_LOG_DBG, "The mtime string conversion failed: \"%ld\"", db_mtime);
        }
        else
        {
            if(reaper_is_file_expired(db_mtime, new_mtime, 6, filename) == 1)
            {
                LOG(PURGER_LOG_DBG, "File has been unlinked: %s", filename);

                // TODO: Last minute sanity checks.

                // TODO: Check to see if both times are sane (not negative, not MAX_INT, etc).
                // TODO: If the difference in time is some huge number, bail out to be safe.

                // TODO: unlink(filename);
            }
            else
            {
                LOG(PURGER_LOG_DBG, "File has not been unlinked: %s", filename);
                // File is NOT expired. TODO: check database in debug mode to make sure it's been removed.
            }
        }
    }
}

int
reaper_is_file_expired(long int old_db_mtime, long int new_stat_mtime, int age_allowed_in_days, char *filename)
{
    int expired = 0; // 0 is NOT expired. 1 IS expired.

    long int current_time = time(NULL);
    long int age_in_secs = current_time - new_stat_mtime;
    long int age_in_days = (long int)(age_in_secs / SECONDS_IN_A_DAY);

    /*
     * If the file has been modified, the new and old mtimes would be
     * different, so lets only move forward if they're the same.
     *
     * On some platforms, the mtime is a floating point number, so keep
     * in mind that the inverse of this statement does not hold true
     * due to potential relative error (new != old).
     */
    if(old_db_mtime == new_stat_mtime)
    {
        if(age_in_days > age_allowed_in_days)
        {
            LOG(PURGER_LOG_DBG, "This file has been marked for deletion: \"%s\"", filename);
            expired = 1;
        }
        else
        {
            LOG(PURGER_LOG_DBG, "File was modified less than %d days ago: \"%s\"", age_allowed_in_days, filename);
            expired = 0;
        }
    }
    else
    {
        LOG(PURGER_LOG_DBG, "File was POSSIBLY modified (diff %ld): %s", new_stat_mtime - old_db_mtime, filename);
        expired = 0;
    }
   
    return expired;
}

/* EOF */
