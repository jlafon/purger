#include <stdio.h>
#include <string.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>

static int sendmail(
                    const char *from,
                    const char *to,
                    const char *subject,
                    const char *body,
                    const char *hostname,
		    const int   port
                    );
