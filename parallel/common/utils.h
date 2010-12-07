#ifndef      __MPI_UTILS_H
#define      __MPI_UTILS_H

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include "mpi.h"
#include "common.h"

/*function prototypes*/
void usage(int);
int get_proc_status(int*, int, int);
void send_abort_cmd(char*, int);
time_t return_latest(time_t, time_t, time_t);

#endif 
