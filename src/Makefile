CFLAGS 	+= -lconfig -Wall -D_GNU_SOURCE -g -pedantic -ansi --std=c99

all:	config log ldap mail

config: config.c config.h
	$(CC) $(CFLAGS) -g -c $@.c

log: log.c log.h
	$(CC) $(CFLAGS) -g -c $@.c

mail: mail.c mail.h
	$(CC) $(CFLAGS) -g -c $@.c

ldap: ldap.c ldap.h
	$(CC) $(CFLAGS) -g -DWITH_OPENLDAP -DLDAP_DEPRECATED -c $@.c

clean:
	- /bin/rm -f *~
	- /bin/rm -f *.o
	- /bin/rm -f *.x

distclean:	clean
