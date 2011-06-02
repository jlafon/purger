noinst_LTLIBRARIES = libutils.la
libutils_la_SOURCES = $(top_srcdir)/include/common.h $(top_srcdir)/include/utils.h utils.c
libutils_la_CFLAGS = -I$(top_srcdir)/include -I`pg_config --includedir`
libutils_la_LDFLAGS = -I$(top_srcdir)/include -I`pg_config --includedir` -L`pg_config --libdir`
libutils_la_LIBADD = -lpq
