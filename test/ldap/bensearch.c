#define LDAP_DEPRECATED 1
#define DEBUG 1
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <lber.h>
#include <ldap.h>
#include "lanl-ldap.h"
#include "lconfig.h"

int main( int argc, char *argv[] ) {
  LDAP *ld;
  int  result;
  int  auth_method = LDAP_AUTH_SIMPLE;
  int desired_version = LDAP_VERSION3;
  ldapinfo_t ldapinfo;
  dbinfo_t dbinfo;
  mailinfo_t mailinfo;

  parse_config(&dbinfo,&ldapinfo,&mailinfo);
  BerElement* ber;
  LDAPMessage* msg;
  LDAPMessage* entry;

  char* filter="(uid=ben)";
  char* errstring;
  char* dn = NULL;
  char* attr;
  char** vals;
  int i;
if(DEBUG)
  printf("Initializing LDAP conection %s:%d...\n",ldapinfo.host,LDAP_PORT);
  if ((ld = ldap_init(ldapinfo.host, LDAP_PORT)) == NULL ) {
    perror( "ldap_init failed" );
    exit( EXIT_FAILURE );
  }
if(DEBUG)
  printf("Setting LDAP options...\n");
  /* set the LDAP version to be 3 */
  if (ldap_set_option(ld, LDAP_OPT_PROTOCOL_VERSION, &desired_version) != LDAP_OPT_SUCCESS)
    {
      ldap_perror(ld, "ldap_set_option");
      exit(EXIT_FAILURE);
    }
if(DEBUG)
    printf("Executing LDAP search...\n");
  if (ldap_search_s(ld, ldapinfo.base, LDAP_SCOPE_SUBTREE, filter, NULL, 0, &msg) != LDAP_SUCCESS) {
    ldap_perror( ld, "ldap_search_s" );
    exit(EXIT_FAILURE);
  }

  /* printf("The number of entries returned was %d\n\n", ldap_count_entries(ld, msg)); */

  /* Iterate through the returned entries */
  for(entry = ldap_first_entry(ld, msg); entry != NULL; entry = ldap_next_entry(ld, entry)) {

    if((dn = ldap_get_dn(ld, entry)) != NULL) {
      printf("Returned dn: %s\n", dn);
      ldap_memfree(dn);
    }

    for( attr = ldap_first_attribute(ld, entry, &ber); 
	 attr != NULL; 
	 attr = ldap_next_attribute(ld, entry, ber)) {
      if ((vals = ldap_get_values(ld, entry, attr)) != NULL) {
	for(i = 0; vals[i] != NULL; i++) {
	  if (strncmp (attr, "mail", 4) == 0) {
	    printf("%s: %s\n", attr, vals[i]);
	    break;
	  }
	}

	ldap_value_free(vals);
      }

      ldap_memfree(attr);
    }

    if (ber != NULL) {
      ber_free(ber,0);
    }
    /* printf("\n"); */
  }

  /* clean up */
  ldap_msgfree(msg);
  result = ldap_unbind_s(ld);

  if (result != 0) {
    fprintf(stderr, "ldap_unbind_s: %s\n", ldap_err2string(result));
    exit( EXIT_FAILURE );
  }

  return EXIT_SUCCESS;
}
