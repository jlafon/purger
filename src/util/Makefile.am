AUTOMAKE_OPTIONS = foreign
SUBDIRS  =  ../../src/
CFLAGS  = -Wall -D_GNU_SOURCE -g -pedantic -ansi --std=c99 \
		  -I`pg_config --includedir` -I../../src \
		  -DVERSION=\"$(VERSION)\" -DPROG="\"$(PACKAGE)\""
LDFLAGS = -L`pg_config --libdir` -lpq -lconfig
COMMON = ../../src
bin_PROGRAMS = dbtest idtest chkscratch update_expired deleter
dbtest_SOURCES = dbtest.c
idtest_SOURCES = idtest.c
chkscratch_SOURCES = chkscratch.c $(COMMON)/config.c $(COMMON)/log.c
update_expired_SOURCES = update_expired.c $(COMMON)/config.c $(COMMON)/log.c
deleter_SOURCES  = deleter.c $(COMMON)/config.c $(COMMON)/log.c
