SRCDIR  =  ../../src/
INCLUDE += -I`pg_config --includedir` -I$(SRCDIR)
LIBS    += -L`pg_config --libdir` -lpq
CFLAGS  += -Wall -D_GNU_SOURCE -g -pedantic -ansi --std=c99

all:	dbtest chkscratch update_expired idtest

dbtest:	dbtest.c
	$(CC) $(INCLUDE) $(LIBS) $(CFLAGS) $@.c -o $@

idtest:	idtest.c
	$(CC) $(INCLUDE) $(LIBS) $(CFLAGS) $@.c -o $@

chkscratch: chkscratch.c
	$(CC) $(INCLUDE) $(LIBS) $(CFLAGS) $@.c $(SRCDIR)config.o $(SRCDIR)log.o -o $@

update_expired:	update_expired.c
	$(CC) $(INCLUDE) $(LIBS) $(CFLAGS) $@.c $(SRCDIR)config.o $(SRCDIR)log.o -o $@

deleter: deleter.c deleter.h
	$(CC) $(INCLUDE) $(LIBS) $(CFLAGS) $@.c $(SRCDIR)config.o $(SRCDIR)log.o -o $@

clean:
	- /bin/rm -f *~
	- /bin/rm -f *.o
	- /bin/rm -f *.x

distclean:	clean
	- /bin/rm -f dbtest chkscratch update_expired idtest

