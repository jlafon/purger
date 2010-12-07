#ifndef      __PURGER_LDAP_H
#define      __PURGER_LDAP_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <lber.h>
#include <ldap.h>

int get_email ( const char *uid, const char* ldap_host, const char *base, char *email );

#endif
