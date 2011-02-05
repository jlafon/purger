#ifndef      __PSTAT_H
#define      __PSTAT_H

#include <signal.h>
#include <dirent.h>
#include <getopt.h>
#include "utils.h"

/* Config parser */
#include "lconfig.h"

/*PostgreSQL*/
#include <postgresql/libpq-fe.h>

/*Time*/
#include <time.h>

/* Function Prototypes */
void manager(char *, int);
void worker(int);

/*four our MPI communications*/
#define MANAGER_PROC 0
#define MANAGER_TAG 0

#define WAIT_TIME 30
#define QSIZE 10000
#define PACKSIZE MAX_STAT
#define WORKSIZE (QSIZE * PATHSIZE_PLUS)

/*these are the different commands that can be sent around
  basically its just a tag of what we are talking about
  in our MPI communications*/
enum cmd_opcode {
  REQCMD,
  EXITCMD,
  DELCMD,
  RESCMD,
  ABORTCMD
};

int delcnt=0;
int rejcnt=0;
int sumcnt=0;
int totcnt=0;
int starttime=0;
int *proc_status;

PGconn *conn;
char snapshot_name[16];

struct stinfo_t {
  time_t atime;
  time_t ctime;
  time_t mtime;
};
typedef struct stinfo_t stinfo_t;

#endif
