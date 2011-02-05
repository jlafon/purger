#include "mail.h"

void sendmail_write(
		    const int  sock,
		    const char *str,
		    const char *arg
		    ) {
  char buf[4096];
  
  if (arg != NULL)
    snprintf(buf, sizeof(buf), str, arg);        
  else
    snprintf(buf, sizeof(buf), str, NULL);
  
  send(sock, buf, strlen(buf), 0);
}

int sendmail(
	     const char *from,
	     const char *to,
	     const char *subject,
	     const char *body,
	     const char *hostname,
	     const int   port
	     ) {
  struct hostent *host;   
  struct sockaddr_in saddr_in;
  int sock = 0;
  
  sock = socket(AF_INET, SOCK_STREAM, 0);
  host = gethostbyname(hostname);
  
  saddr_in.sin_family      = AF_INET;
  saddr_in.sin_port        = htons((u_short)port);
  saddr_in.sin_addr.s_addr = 0;
  
  memcpy((char*)&(saddr_in.sin_addr), host->h_addr, host->h_length);
  
  if (connect(sock, (struct sockaddr*)&saddr_in, sizeof(saddr_in)) == -1) {
    return -2;
  }
  
  sendmail_write(sock, "HELO %s\n",       from);    /* greeting */
  sendmail_write(sock, "MAIL FROM: %s\n", from);    /* from */
  sendmail_write(sock, "RCPT TO: %s\n",   to);      /* to */
/*  sendmail_write(sock, "RCPT TO: nfs@lanl.gov\n"
		 ,                        NULL); */  /* to (BCC) */
  sendmail_write(sock, "DATA\n",          NULL);    /* begin data */
  
  /* next comes mail headers */
  sendmail_write(sock, "From: consult@lanl.gov\n"
		 ,                        NULL);
  sendmail_write(sock, "To: %s\n",        to);
  /* sendmail_write(sock, "Bcc: nfs@lanl.gov\n"
		 ,                        NULL); */
  sendmail_write(sock, "Subject: %s\n",   subject);
  
  sendmail_write(sock, "\n",              NULL);
  
  sendmail_write(sock, "The following text file in the Yellow network contains a list of your scratch files that have not been modified in the last 14+ days.  Those files will be deleted in at least 6 days if not modified by then.  This notification may not have up-to-the-minute information, but we will verify a file's actual age before purging it.   For more information, please see our purge policy:  http://hpc.lanl.gov/purge_policy.  If you have questions or concerns, please contact ICN Consultants at 505-665-4444 option 3 or consult@lanl.gov.\n\n"
		 ,	                  NULL);    /* data */
  sendmail_write(sock, "%s\n",            body);    /* data */
  
  sendmail_write(sock, ".\n",             NULL);    /* end data */
  sendmail_write(sock, "QUIT\n",          NULL);    /* terminate */
  
  close(sock);
  
  return 0;
}
