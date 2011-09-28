#ifndef REAPER_H
#define REAPER_H

#include <libcircle.h>

void reaper_check_database_for_more(CIRCLE_handle *handle);
void reaper_check_local_queue(CIRCLE_handle *handle, char *key);
int reaper_pop_zset(char **results, char *zset, long long start, long long end);
void process_files(CIRCLE_handle *handle);
void reaper_redis_zrangebyscore(char *zset, long long from, long long to);
void print_usage(char **argv);

#endif /* REAPER_H */
