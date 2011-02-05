#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "mail.h"
#include "../../include/lanl-ldap.h"

#define CFG_FILE "purger.conf"

struct ldapinfo_t {
  char host[256];
  char base[256];
  char basem[256];
};
typedef struct ldapinfo_t ldapinfo_t;

int parse_config(ldapinfo_t *ldapinfo){
  /*put in config parser*/
}


int main(int argc, char *argv[]){
  int ret=0;
  char email[128];
  char moniker[128];
  char notefile[256];
  char uid[10]="11071";

  ldapinfo_t ldapinfo;

  parse_config(&ldapinfo);

  /* grab moniker from uid */
  if (get_moniker( uid, ldapinfo.host, ldapinfo.basem, moniker ) == 1) {
    printf("Error getting moniker from ldap host: %s base: %s uid: %s\n", ldapinfo.host, ldapinfo.basem, uid);
    return EXIT_FAILURE;
  }

  /* grab e-mail from uid */
  if (get_email( moniker, ldapinfo.host, ldapinfo.base, email ) == 1) {
    printf("Error getting email from ldap host: %s base: %s uid: %s\n", ldapinfo.host, ldapinfo.base, uid);
    return EXIT_FAILURE;
  }

  snprintf(notefile, 256, "/%s/%s/expired-files.txt", "scratch1", moniker);

  /* send e-mail containing file list */
  

  ret = sendmail(
		 "root@turq-fsdb.lanl.gov",   /* from     */
		 email,              /* to       */
		 "[PURGER-NOTIFICATION]",                  /* subject  */
		 notefile,                   /* body     */
		 "mail.lanl.gov",             /* hostname */
		 25                           /* port     */
		 );
  
  if (ret != 0)
    printf("Failed to send mail (code: %i).\n", ret);
  else
    printf("Mail successfully sent. (uid %s)\n", uid);
  
  return ret;
}
