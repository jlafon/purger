#include "lconfig.h"

int parse_config(dbinfo_t *dbinfo, ldapinfo_t *ldapinfo, mailinfo_t *mailinfo){
  config_t cfg, *cf;
  const char *host = NULL;
  const char *port = NULL;
  const char *user = NULL;
  const char *passwd = NULL;
  const char *ldaphost = NULL;
  const char *ldapbase = NULL;
  const char *ldapbasem = NULL;
  const char *mailtxt = NULL;
  const char *mailfrom = NULL;
  const char *mailfromreal = NULL;
  const char *maildefaultto = NULL;
  const char *mailsubject = NULL;
  const char *mailserver = NULL;

  cf = &cfg;
  config_init(cf);

  if (!config_read_file(cf, CFG_FILE)){
    if (!config_read_file(cf, CFG_FILE_ALT)){
      fprintf(stderr, "%s:%d - %s\n", config_error_file(cf), config_error_line(cf), config_error_text(cf));
      config_destroy(cf);
      return EXIT_FAILURE;
    }
  }

  if(config_lookup_string(cf, "purger.postgres.host", &host) == CONFIG_FALSE){
    fprintf(stderr, "host not found or incorrect format\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }
  if(config_lookup_string(cf, "purger.postgres.port", &port) == CONFIG_FALSE){
    fprintf(stderr, "port not found or incorrect format\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }
  if(config_lookup_string(cf, "purger.postgres.user", &user) == CONFIG_FALSE){
    fprintf(stderr, "user not found or incorrect format\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }
  if(config_lookup_string(cf, "purger.postgres.passwd", &passwd) == CONFIG_FALSE){
    fprintf(stderr, "passwd not found or incorrect format\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }
  if(config_lookup_string(cf, "purger.ldap.ldaphost", &ldaphost) == CONFIG_FALSE){
    fprintf(stderr, "ldaphost not found or incorrect format\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }
  if(config_lookup_string(cf, "purger.ldap.ldapbase", &ldapbase) == CONFIG_FALSE){
    fprintf(stderr, "ldapbase not found or incorrect format\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }
  if(config_lookup_string(cf, "purger.ldap.ldapbasem", &ldapbasem) == CONFIG_FALSE){
    fprintf(stderr, "ldapbasem not found or incorrect format\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }
  if(config_lookup_string(cf, "purger.mail.mailtxt", &mailtxt) == CONFIG_FALSE){
    fprintf(stderr, "mailtxt not found or incorrect format\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }
  if(config_lookup_string(cf, "purger.mail.mailfrom", &mailfrom) == CONFIG_FALSE){
    fprintf(stderr, "mailfrom not found or incorrect format\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }
  if(config_lookup_string(cf, "purger.mail.mailfromreal", &mailfromreal) == CONFIG_FALSE){
    fprintf(stderr, "mailfromreal not found or incorrect format\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }
  if(config_lookup_string(cf, "purger.mail.maildefaultto", &maildefaultto) == CONFIG_FALSE){
    fprintf(stderr, "maildefaultto not found or incorrect format\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }
  if(config_lookup_string(cf, "purger.mail.mailsubject", &mailsubject) == CONFIG_FALSE){
    fprintf(stderr, "mailsubject not found or incorrect format\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }
  if(config_lookup_string(cf, "purger.mail.mailserver", &mailserver) == CONFIG_FALSE){
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
  if (!strncpy(mailinfo->txt, mailtxt, 2048)) {
    fprintf(stderr, "could not copy mailtxt\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }
  if (!strncpy(mailinfo->from, mailfrom, 256)) {
    fprintf(stderr, "could not copy mailfrom\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }
  if (!strncpy(mailinfo->fromreal, mailfromreal, 256)) {
    fprintf(stderr, "could not copy mailfromreal\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }
  if (!strncpy(mailinfo->defaultto, maildefaultto, 256)) {
    fprintf(stderr, "could not copy maildefaultto\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }
  if (!strncpy(mailinfo->subject, mailsubject, 256)) {
    fprintf(stderr, "could not copy mailsubject\n");
    config_destroy(cf);
    return EXIT_FAILURE;
  }
  if (!strncpy(mailinfo->server, mailserver, 256)) {
    fprintf(stderr, "could not copy mailserver\n");
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
