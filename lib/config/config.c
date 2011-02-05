#include "../../include/lconfig.h"
/*
#include <stdlib.h>
#include <errno.h>
#include <string.h>

#include "log.h"

#include <libconfig.h>
#define CFG_FILE "purger.conf"

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

int parse_config(dbinfo_t *dbinfo, ldapinfo_t *ldapinfo);
int parse_config_dbonly(dbinfo_t *dbinfo);

*/

int parse_config(dbinfo_t *dbinfo, ldapinfo_t *ldapinfo){
  config_t cfg, *cf;
  const char *host = NULL;
  const char *port = NULL;
  const char *user = NULL;
  const char *passwd = NULL;
  const char *ldaphost = NULL;
  const char *ldapbase = NULL;
  const char *ldapbasem = NULL;

  cf = &cfg;
  config_init(cf);

  if (!config_read_file(cf, CFG_FILE)){
    fprintf(stderr, "%s:%d - %s\n", config_error_file(cf), config_error_line(cf), config_error_text(cf));
    config_destroy(cf);
    return EXIT_FAILURE;
  }

  if(config_lookup_string(cf, "host", &host) == CONFIG_FALSE){
    fprintf(stderr, "host not found or incorrect format\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }    
  if(config_lookup_string(cf, "port", &port) == CONFIG_FALSE){
    fprintf(stderr, "port not found or incorrect format\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }    
  if(config_lookup_string(cf, "user", &user) == CONFIG_FALSE){
    fprintf(stderr, "user not found or incorrect format\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }    
  if(config_lookup_string(cf, "passwd", &passwd) == CONFIG_FALSE){
    fprintf(stderr, "passwd not found or incorrect format\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }    
  if(config_lookup_string(cf, "ldaphost", &ldaphost) == CONFIG_FALSE){
    fprintf(stderr, "ldaphost not found or incorrect format\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }    
  if(config_lookup_string(cf, "ldapbase", &ldapbase) == CONFIG_FALSE){
    fprintf(stderr, "ldapbase not found or incorrect format\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }    
  if(config_lookup_string(cf, "ldapbasem", &ldapbasem) == CONFIG_FALSE){
    fprintf(stderr, "ldapbasem not found or incorrect format\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }    

  if (!strncpy(dbinfo->host, host, 256)) {
    fprintf(stderr, "could not copy host\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }    
  if (!strncpy(dbinfo->port, port, 16)) {
    fprintf(stderr, "could not copy port\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }    
  if (!strncpy(dbinfo->user, user, 256)) {
    fprintf(stderr, "could not copy user\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }    
  if (!strncpy(dbinfo->pass, passwd, 256)) {
    fprintf(stderr, "could not copy passwd\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }    
  if (!strncpy(ldapinfo->host, ldaphost, 256)) {
    fprintf(stderr, "could not copy ldaphost\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }    
  if (!strncpy(ldapinfo->base, ldapbase, 256)) {
    fprintf(stderr, "could not copy ldapbase\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }    
  if (!strncpy(ldapinfo->basem, ldapbasem, 256)) {
    fprintf(stderr, "could not copy ldapbasem\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }    

  return EXIT_SUCCESS;
}

int parse_config_dbonly(dbinfo_t *dbinfo){
  config_t cfg, *cf;
  const char *host = NULL;
  const char *port = NULL;
  const char *user = NULL;
  const char *passwd = NULL;

  cf = &cfg;
  config_init(cf);

  if (!config_read_file(cf, CFG_FILE)){
    fprintf(stderr, "%s:%d - %s\n", config_error_file(cf), config_error_line(cf), config_error_text(cf));
    config_destroy(cf);
    return EXIT_FAILURE;
  }

  if(config_lookup_string(cf, "host", &host) == CONFIG_FALSE){
    fprintf(stderr, "host not found or incorrect format\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }    
  if(config_lookup_string(cf, "port", &port) == CONFIG_FALSE){
    fprintf(stderr, "port not found or incorrect format\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }    
  if(config_lookup_string(cf, "user", &user) == CONFIG_FALSE){
    fprintf(stderr, "user not found or incorrect format\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }    
  if(config_lookup_string(cf, "passwd", &passwd) == CONFIG_FALSE){
    fprintf(stderr, "passwd not found or incorrect format\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }    

  if (!strncpy(dbinfo->host, host, 256)) {
    fprintf(stderr, "could not copy host\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }    
  if (!strncpy(dbinfo->port, port, 16)) {
    fprintf(stderr, "could not copy port\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }    
  if (!strncpy(dbinfo->user, user, 256)) {
    fprintf(stderr, "could not copy user\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }    
  if (!strncpy(dbinfo->pass, passwd, 256)) {
    fprintf(stderr, "could not copy passwd\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }    

  return EXIT_SUCCESS;
}
