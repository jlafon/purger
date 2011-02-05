#ifndef  __CONFIG_H
#define  __CONFIG_H

/* Standard Inlcudes */
#include <stdlib.h>
#include <errno.h>
#include <string.h>

/* Puger Logging */
#include "log.h"

/* Config Parser */
#include <libconfig.h>
#define CFG_FILE "purger.conf"

/* Structs */
struct dbinfo_t {
  char host[256];
  char port[16];
  char user[256];
  char pass[256];
};
typedef struct dbinfo_t dbinfo_t;

struct ldapinfo_t {
  char host[256];
  char base[256];
  char basem[256];
};
typedef struct ldapinfo_t ldapinfo_t;

/* Function Definitions */
int parse_config(dbinfo_t *dbinfo, ldapinfo_t *ldapinfo);
int parse_config_dbonly(dbinfo_t *dbinfo);
#if (((LIBCONFIG_VER_MAJOR == 1) && (LIBCONFIG_VER_MINOR <= 4))) 
#define /* const char * */ config_error_text(/* const config_t * */ C)  \
      ((C)->error_text)

#define /* const char * */ config_error_file(/* const config_t * */ C)  \
      ((C)->error_file)

#define /* int */ config_error_line(/* const config_t * */ C)   \
      ((C)->error_line)

#define /* config_error_t */ config_error_type(/* const config_t * */ C) \
      ((C)->error_type)
#endif

#endif
