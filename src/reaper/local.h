#ifndef LOCAL_H
#define LOCAL_H

#include <libcircle.h>

#define SECONDS_IN_A_DAY 60*60*24

static unsigned long int reaper_strtoul(const char *nptr, int *ret_code);
void reaper_check_local_queue(CIRCLE_handle *handle, char *key);

#endif /* LOCAL_H */
