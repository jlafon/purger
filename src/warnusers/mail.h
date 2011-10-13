#ifndef      __WARNUSERS_MAIL_H
#define      __WARNUSERS_MAIL_H

typedef struct
{
  char* from;
  char* fromreal;
  char* defaultto;
  char* subject;
  char* server;
  char* txt;
} mailinfo_t;

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
