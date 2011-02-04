AUTOMAKE_OPTIONS = foreign

CFLAGS = -Wall -I../../../src -D_GNU_SOURCE -pedantic -ansi --std=c99 -DVERSION=\"$(VERSION)\" -DPROG="\"$(PACKAGE)\"" 
LDFLAGS = -lldap -llber

bin_PROGRAMS = logtest
logtest_SOURCES = logtest.c ../../../src/log.c
