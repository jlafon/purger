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

            LOG(LOG_DBG, "mtime for %s is %s", filename, mtime_str);

            /*
             * It looks like we have a potential one to delete here, lets check it out.
             * Lets grab the current file information in case it was changed since we last saw it.
             */
             struct stat new_stat_buf;
             if(lstat(filename, &new_stat_buf) != 0)
             {
                 LOG(LOG_DBG, "The stat of the potential file failed (%s): %s", strerror(errno), filename);
             }
             else
             {
                 int convert_status = 0;
                 long int old_mtime = reaper_strtoul(mtime_str, &convert_status);
                 long int new_mtime = (long int)new_stat_buf.st_mtime;

                 if(convert_status <= 0)
                 {
                     LOG(LOG_DBG, "The mtime string conversion failed: \"%ld\"", old_mtime);
                 }
                 else
                 {
                    if(old_mtime == new_mtime)
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
                        }
                    }
                    else
                    {
                        LOG(LOG_DBG, "File was modified (diff %ld): %s", new_mtime - old_mtime, filename);
                    }
                }
            }
        }
    }
    else
    {
        LOG(LOG_ERR, "Redis didn't return an array when trying to hmget %s.", key);
    }
}

/* EOF */
