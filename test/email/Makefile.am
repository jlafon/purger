AUTOMAKE_OPTIONS = foreign
CFLAGS = -Wall -I/usr/local/include -DVERSION=\"$(VERSION)\" -DPROG="\"$(PACKAGE)\"" \
		 -pedantic -ansi --std=c99 -DWITH_OPENLDAP -DLDAP_DEPRECATED -I../../../src
LDFLAGS = -lldap -llber

bin_PROGRAMS = mailtest
mailtest_SOURCES = mailtest.c ../../../src/ldap.c 
