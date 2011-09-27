#ifndef TREEWALK_H
#define TREEWALK_H

#include <libcircle.h>

void add_objects(CIRCLE_handle *handle);
void process_objects(CIRCLE_handle *handle);
int treewalk_create_redis_attr_cmd(char *buf, struct stat *st, char *filename, char *filekey);
int treewalk_redis_run_zadd(char *filekey, long val, char *zset, char *filename);
void treewalk_redis_run_cmd(char *cmd, char *filename);
int treewalk_redis_keygen(char *buf, char *filename);
void print_usage(char **argv);
void treewalk_redis_run_sadd(struct stat * st);
#endif /* TREEWALK_H */
