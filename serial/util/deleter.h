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
#include <libpq-fe.h>

// File Stats
#include <sys/stat.h>
#include <time.h>

// Config Parser
#include "config.h"
#define CFG_FILE "purger.conf"

// Structs
struct dbinfo_t {
  char host[256];
  char port[16];
  char user[256];
  char pass[256];
};
typedef struct dbinfo_t dbinfo_t;

// Function Definitions
static void exit_nicely(PGconn *conn);
int process_files(PGconn *conn, PGresult *res);
int parse_config(dbinfo_t *dbinfo);

#endif
