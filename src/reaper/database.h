#ifndef DATABASE_H
#define DATABASE_H

#include <libcircle.h>

#define REAPER_DB_SUCCESS 1
#define REAPER_DB_FATAL -1
#define REAPER_DB_COLLISION -2

int  reaper_msleep(unsigned long milisec);
int  reaper_pop_zset(char **results, char *zset, long long start, long long end);
int  reaper_check_database_for_more(CIRCLE_handle *handle);
void reaper_redis_zrangebyscore(char *zset, long long from, long long to);
void reaper_backoff_database(CIRCLE_handle *handle);

#endif /* DATABASE_H */
