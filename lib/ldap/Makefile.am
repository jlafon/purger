noinst_LTLIBRARIES = libldap.la
libldap_la_SOURCES = ldap.c
libldap_la_LDFLAGS = -llber -lldap -I$(top_srcdir)/include
libldap_la_CFLAGS = -I$(top_srcdir)/include/ -DWITH_OPENLDAP -DLDAP_DEPRECATED
