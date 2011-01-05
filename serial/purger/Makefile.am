AUTOMAKE_OPTIONS = foreign
SUBDIRS = ../../src
CFLAGS = -Wall -I/usr/local/include -DVERSION=\"$(VERSION)\" \
	-DPROG="\"$(PACKAGE)\"" -I`pg_config --includedir` -I../../src \
	-D_GNU_SOURCE -g -pedantic -ansi --std=c99 -L`pg_config --libdir` \
	-DWITH_OPENLDAP -DLDAP_DEPRECATED
LDFLAGS = -lconfig -L/usr/local/lib
bin_PROGRAMS = purger
purger_SOURCES = purge_user.c ../../src/log.c ../../src/ldap.c ../../src/mail.c
