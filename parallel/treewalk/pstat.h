#ifndef      __PSTAT_H
#define      __PSTAT_H

#include <signal.h>
#include <dirent.h>
#include <getopt.h>
#include "../common/utils.h"
#include "../../src/config.h"

/*PostgreSQL*/
#include <libpq-fe.h>

/* Function Prototypes */
void manager(char *, int, char *, int);
void worker(int);

/*four our MPI communications*/
#define MANAGER_PROC 0
#define NAMEREAD_PROC 1
#define NAMESTAT_PROC 2
#define MANAGER_TAG 0
#define NAMEREAD_TAG 1
#define NAMESTAT_TAG 2

#define WAIT_TIME 30
#define QSIZE 10000
#define PACKSIZE MAX_STAT
#define WORKSIZE (QSIZE * PACKSIZE * 10)
#define MAX_NCOUNT 300000

/*these are the different commands that can be sent around
  basically its just a tag of what we are talking about
  in our MPI communications*/
enum cmd_opcode {
  DIRCMD = 1,
  NAMECMD,
  REQCMD,
  EXITCMD,
  OUTCMD,
  STATCMD,
  ABORTCMD,
  WAITCMD
};

/*boolean value to turn on and off database access*/
int db_on = 0;
/*boolean value to restart from checkpoint*/
int restart = 0;

#endif
