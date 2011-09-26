#ifndef REAPER_H
#define REAPER_H

#include <libcircle.h>

void reaper_pop_zset(char **results, char *zset, long long start, long long end);
void process_files(CIRCLE_handle *handle);
long long reaper_redis_zcard(char *zset);
void reaper_redis_zrangebyscore(char *zset, long long from, long long to);
void print_usage(char **argv);

#endif /* REAPER_H */
