#ifndef      __WARNUSERS_MAIL_H
#define      __WARNUSERS_MAIL_H

struct mailinfo_t {
  char from[256];
  char fromreal[256];
  char defaultto[256];
  char subject[256];
  char server[256];
  char txt[2048];
};
typedef struct mailinfo_t mailinfo_t;

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
