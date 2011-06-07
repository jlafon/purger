#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "lconfig.h"
#include "mail.h"
#include "lanl-ldap.h"

//#define CFG_FILE "purger.conf"



int main(int argc, char *argv[]){
  int ret=0;
  char email[128];
  char moniker[128];
  char notefile[256];
  char uid[10]="11071";

  mailinfo_t mailinfo;
  ldapinfo_t ldapinfo;
  dbinfo_t dbinfo;
  
  if(parse_config(&dbinfo,&ldapinfo,&mailinfo) != EXIT_SUCCESS)
  {
      fprintf(stderr,"Unable to parse config file.\n");
      exit(-1);
  }

  /* grab moniker from uid */
  if (get_moniker( uid, ldapinfo.host, ldapinfo.basem, moniker ) != EXIT_SUCCESS) {
    printf("Error getting moniker from ldap host: %s base: %s uid: %s\n", ldapinfo.host, ldapinfo.basem, uid);
    return EXIT_FAILURE;
  }

  /* grab e-mail from uid */
  if (get_email( moniker, ldapinfo.host, ldapinfo.base, email ) != EXIT_SUCCESS) {
    printf("Error getting email from ldap host: %s base: %s uid: %s\n", ldapinfo.host, ldapinfo.base, uid);
    return EXIT_FAILURE;
  }

  snprintf(notefile, 256, "/%s/%s/expired-files.txt", "scratch1", moniker);

  /* send e-mail containing file list */
  
/*
  ret = sendmail(
		 email,              
		 notefile,          
		 25,
		 &mailinfo
		 );
 */ 
  if (ret != 0)
    printf("Failed to send mail (code: %i).\n", ret);
  else
    printf("Mail successfully sent. (uid %s)\n", uid);
  
  return ret;
}
