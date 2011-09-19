#ifndef REAPER_H
#define REAPER_H

#include <libcircle.h>

void reaper_redis_run_cmd(char *cmd, char *filename);
int reaper_redis_keygen(char *buf, char *filename);
void print_usage(char **argv);

#endif /* REAPER_H */
