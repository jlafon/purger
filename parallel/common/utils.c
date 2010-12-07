/************************************************************************************
* Name:  purger utilities
*
* Description:
*  This file contains utility functions used by pstat.c
*
* Author:  Alfred Torrez / Ben McClelland
*
* History:
*
* 04/04/2007 Created File - moved mpi_ftw utility functions to here 
* 09/01/2007 - adapted for use as purger (actually parallel stat-er)
*
**********************************************************************************************/

#include "utils.h"

/***********************************************************************************************
*
* Function:  usage
*
* Description: Prints usage information.

***********************************************************************************************/
void usage(int rank)
{
    /* print usage statement */
    if (rank == 0) {
    printf("\npstat:\n");
    printf("Walk through directory tree structure and gather statistics on files and\n");
    printf("directories encountered.\n");
    printf("Usage:  pstat --path [-p] path --expire [-e] {#}{d,w,m,y} [--verbose[-v] path] [--usage[-u] path] [--stats[-s] path] [-db[-d]] [--deletefiles[-f]] [--help[-h]]\n");
    printf("--path -p <path>     Path to start walk (required argument)\n");
    printf("--expire -e <time>   expiration time (required argument)\n");
    printf("--verbose -v <path>  Print debug to rankfiles prefixed by specified path\n");
    printf("--stats -s <path>    Print filesystem stats to files prefixed by specified path\n");
    printf("--usage -u <file>    Print filesystem usage by uid to file specified\n");
    printf("--db -d              Turn on database inserts\n");
    printf("--deletefiles -f     Turn on expired file deletion\n");
    printf("--help -h            Display help\n\n");
    }
    return;
}

/***********************************************************************************************
* Function:  get_proc_status
*
* Description: This function returns the next available free process (rank) by examining the process
*              status array.  If no processes are available, a -1 is returned.
*
***********************************************************************************************/
int get_proc_status(int *proc_status, int proc_cnt, int index)
{
    while (proc_status[index] == 1 && index < proc_cnt-1) index++;
    return ((proc_status[index] != 0) ? -1 : index);
}

/***********************************************************************************************
* Function: send_abort_cmd
*
* Description: This function sends an internal abort command to all processes so that the
*              application can shut down gracefully.
*
***********************************************************************************************/
void send_abort_cmd(char *abort_cmd, int nprocs)
{
    int i;
    for (i=1; i<nprocs; i++) {
      if (MPI_Send (abort_cmd, strlen(abort_cmd) + 1, MPI_CHAR, i, i, MPI_COMM_WORLD ) != MPI_SUCCESS) {
        fprintf(stderr, "ERROR in manager sending when sending exit to slaves.\n");
         MPI_Abort(MPI_COMM_WORLD, -1);
      }
    }
}

/***********************************************************************************************
* Function: return_latest
*
* Description: This function is designed for the comparison of the file timestamps to the 
*              expiration date.  It seemed safest to just get the most recent of atime, 
*              ctime, and mtime and use that for the comparison.  So this function returns
*              the value of the latest one.
*
***********************************************************************************************/
time_t return_latest(time_t a, time_t b, time_t c)
{
  if ((a > b) && (a > c))
    return (a);
  else if (b > c)
    return (b);
  else 
    return (c);
}
