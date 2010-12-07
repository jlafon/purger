#ifndef      __PURGER_MAIL_H
#define      __PURGER_MAIL_H

#include <stdio.h>
#include <string.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

void sendmail_write(
		    const int  sock,
		    const char *str,
		    const char *arg
		    );
int sendmail(
	     const char *from,
	     const char *to,
	     const char *subject,
	     const char *body,
	     const char *hostname,
	     const int   port
	     );

#endif
