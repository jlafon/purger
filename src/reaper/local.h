#ifndef LOCAL_H
#define LOCAL_H

#include <libcircle.h>

#define SECONDS_IN_A_DAY 60*60*24

static unsigned long int reaper_strtoul(const char *nptr, int *ret_code);
void reaper_check_local_queue(CIRCLE_handle *handle, char *key);
long int reaper_mtime_to_number(char *mtime_str);
void reaper_check_and_delete_file(char *filename, long int db_mtime);
int reaper_is_file_expired(long int old_db_mtime, long int new_stat_mtime, int age_allowed_in_days, char *filename);

#endif /* LOCAL_H */
