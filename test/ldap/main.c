#include "../../include/lanl-ldap.h"

int main ( int argc, char *argv[] ) {
  char ldap_host[256];
  char ldap_base[256];
  char email[256];

  /*put in config parse*/

  if (get_email ( "ben", ldap_host, ldap_base, email ) == 0)
    printf("%s\n", email);
  else
    printf("error\n");

  return 0;
}
