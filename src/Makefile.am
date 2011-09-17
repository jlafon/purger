bin_PROGRAMS = dstat
dstat_SOURCES = sprintstatf.c hash.c dstat.c
dstat_LDADD = \
    -lcrypto                               \
    -llibcircle                            \
    $(MPI_CLDFLAGS)                        \
    $(top_srcdir)/src/hiredis/libhiredis.a

SUBDIRS = hiredis
INCLUDES = \
    $(MPI_CFLAGS)               \
    -I$(top_srcdir)/src         \
    -I$(top_srcdir)/src/hiredis
