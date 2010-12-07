SRCDIR  =  ../../src/
INCLUDE += -I`pg_config --includedir` -I$(SRCDIR)
LIBS    += -L`pg_config --libdir`
CFLAGS  += -Wall -D_GNU_SOURCE -g -pedantic -ansi --std=c99

all: purger

test: mailtest logtest

purger: purger.c purger.h
	$(CC) $(INCLUDE) $(LIBS) $(CFLAGS) $@.c $(SRCDIR)config.o $(SRCDIR)mail.o $(SRCDIR)ldap.o $(SRCDIR)log.o -o $@ -g -lpq -lldap -llber

purge_user: purge_user.c purger.h mail.c mail.h ldap.c ldap.h log.c log.h
	$(CC) $(CFLAGS) -g -DWITH_OPENLDAP -DLDAP_DEPRECATED -c ldap.c
	$(CC) $(CFLAGS) -g -c mail.c
	$(CC) $(CFLAGS) -g -c log.c
	$(CC) $(INCLUDE) $(LIBS) $(CFLAGS) $@.c mail.o ldap.o log.o -o $@ -g -lpq -lldap -llber


clean:
	- /bin/rm -f *~
	- /bin/rm -f *.o
	- /bin/rm -f *.x

distclean: clean
	- /bin/rm -f purger
