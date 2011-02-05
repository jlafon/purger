#ifndef  __PURGER_H
#define  __PURGER_H

/* Standard includes */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <getopt.h>

/* Macros */
#define PATHSIZE_PLUS 1050
#define STRLIM PATHSIZE_PLUS
#define MAX_STATS 20000
/* Config Parser */
#include "../../include/lconfig.h"

/* PostgreSQL */
#include <postgresql/libpq-fe.h>

/* File Stats */
#include <sys/stat.h>
#include <time.h>

/* Email */
#include "mail.h"

/* LDAP */
#include "lanl-ldap.h"
#include <ldap.h>
#include <lber.h>
/* Puger Logging */
#include "log.h"


/* Memory Debug */
#include <malloc.h>

/* Function Definitions */
static void usage();
static void exit_nicely(PGconn *conn);
int process_warned_files(PGconn *conn, char *uid, char *filesystem, char *ins_timenow);
int process_unwarned_files(PGconn *conn, char *uid, char *filesystem, char *ins_timenow, ldapinfo_t *ldapinfo);
int diff_date(char *date1, char *date2);
int send_mail(char *uid, char *file_list, ldapinfo_t *ldapinfo);
void delete_file(char *filename, PGconn *conn, FILE *dlog);

/* global variables */
int force = 0;

#endif
