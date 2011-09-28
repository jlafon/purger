#ifndef DATABASE_H
#define DATABASE_H

#include <libcircle.h>

int reaper_pop_zset(char **results, char *zset, long long start, long long end);
void reaper_check_database_for_more(CIRCLE_handle *handle);
void reaper_redis_zrangebyscore(char *zset, long long from, long long to);

#endif /* DATABASE_H */
