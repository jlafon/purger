AUTOMAKE_OPTIONS = foreign
CFLAGS = -lldap -llber -Wall -I/usr/include -DVERSION=\"$(VERSION)\" -DPROG="\"$(PACKAGE)\"" \
		 -pedantic -ansi --std=c99 -DWITH_OPENLDAP -DLDAP_DEPRECATED -I../../../src 
LDFLAGS = -lldap -llber
bin_PROGRAMS = bensearch ldapemail
bensearch_SOURCES = ../../../_ldap.h ../../../src/ldap.c
ldapemail_SOURCES = main.c ../../../src/ldap.c
