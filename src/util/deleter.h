#ifndef  __DELETER_H
#define  __DELETER_H

// Standard includes
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// Macros
#define PATHSIZE_PLUS 1050
#define STRLIM PATHSIZE_PLUS


// PostgreSQL
#include <postgresql/libpq-fe.h>

// File Stats
#include <sys/stat.h>
#include <time.h>

// Config Parser
#include "../../include/lconfig.h"
#define CFG_FILE "purger.conf"


// Function Definitions
static void exit_nicely(PGconn *conn);
int process_files(PGconn *conn, PGresult *res);

#endif
