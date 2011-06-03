#include "lconfig.h"
#include <stdio.h>
#include <stdlib.h>
int main ( int argc, char *argv[] ) 
{
    dbinfo_t dbinfo;
    ldapinfo_t ldapinfo;
    mailinfo_t mailinfo;
    if(parse_config(&dbinfo,&ldapinfo,&mailinfo) != EXIT_SUCCESS)
    {
	fprintf(stderr,"Failed to parse config file.\n");
	exit(-1);
    }
    fprintf(stdout,"===Purger Configuration Settings===\n\
	    Database Settings: \n\
	    \tHost:\t%s\n\
	    \tPort:\t%d\n\
	    \tUser:\t%s\n\
	    \tPass:\t%s\n\
	    \n\
	    LDAP Settings: \n\
	    \tHost:\t%s\n\
	    \tBase:\t%s\n\
	    \tBasem:\t%s\n\
	    \n\
	    Mail Settings: \n\
	    \tFrom:\t\t%s\n\
	    \tFrom(actual):\t%s\n\
	    \tDefault To:\t%s\n\
	    \tSubject:\t%s\n\
	    \tServer:\t\t%s\n\
	    \tText:\t\t%s\n",
	    dbinfo.host,atoi(dbinfo.port),dbinfo.user,dbinfo.pass,
	    ldapinfo.host,ldapinfo.base,ldapinfo.basem,
	    mailinfo.from,mailinfo.fromreal,mailinfo.defaultto,mailinfo.subject,mailinfo.server,mailinfo.txt
	   );
    return 0;
}
