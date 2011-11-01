#ifndef WARNUSERS_H
#define WARNUSERS_H

#include <libcircle.h>

void add_objects(CIRCLE_handle *handle);
void process_objects(CIRCLE_handle *handle);
void warnusers_get_uids(int rank,CIRCLE_handle *handle);
void print_usage(char **argv);
#endif /* WARNUSERS_H */
