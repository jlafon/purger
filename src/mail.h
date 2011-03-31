#ifndef      __PURGER_MAIL_H
#define      __PURGER_MAIL_H

#include <stdio.h>
#include <string.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

/* Config Parser */
#include "config.h"

void sendmail_write(
		    const int  sock,
		    const char *str,
		    const char *arg
		    );
int sendmail(
	     const char       *to,
	     const char       *body,
	     const int        port,
             const mailinfo_t *mailinfo
	     );

#endif
