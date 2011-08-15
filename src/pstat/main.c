/* @file pstat.c
 * @authors Jharrod LaFon
 * @date August 2011
 * @brief Parallel filesystem exploration.
 */

#include <stdio.h>
#include <mpi.h>
#include <getopt.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include "pstat.h"

/* Main */
int main( int argc, char *argv[] )
{
    options opts;
    opts.beginning_path = (char*) malloc(sizeof(char)*MAX_STRING_LEN);
    opts.beginning_path[0] = '\0';
    opts.verbose = 0;
    if(parse_args(argc,argv,&opts) < 0)
        exit(0);
    if(opts.beginning_path[0] == '\0')
    {
        fprintf(stderr,"Error: A beginning path is required.\n");
        exit(1);
    }
    MPI_Init(&argc,&argv);
    worker( &opts );
    MPI_Finalize();
    redisAsyncDisconnect(redis_context);
    return 0;
}

