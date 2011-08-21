bin_PROGRAMS = pstat
pstat_SOURCES = main.c pstat.c pstat.h worker.c
pstat_LDADD = $(MPI_CLDFLAGS) $(top_srcdir)/extern/hiredis/libhiredis.a

INCLUDES = $(MPI_CFLAGS) -I$(top_srcdir)/extern/hiredis/
