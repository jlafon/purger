#include<stdio.h>
#include "lconfig.h"
#include "lanl-ldap.h"

int main ( int argc, char *argv[] ) {

  dbinfo_t dbinfo;
  ldapinfo_t ldapinfo;
  mailinfo_t mailinfo;
  char email[128];

  /*put in config parse*/

  if(parse_config(&dbinfo, &ldapinfo, &mailinfo) != EXIT_SUCCESS)
  {
      fprintf(stderr,"Unable to parse config file.\n");
      exit(-1);
  }
  
  if (get_email ( "ben", &ldapinfo.host, &ldapinfo.base, &email ) == 0)
    printf("%s\n", email);
  else
    printf("error\n");

  return 0;
}
