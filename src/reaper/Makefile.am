include $(top_srcdir)/common.mk

bin_PROGRAMS = reaper
reaper_SOURCES = reaper.c local.c database.c
reaper_LDADD = \
    $(libcircle_LIBS)                            \
    $(MPI_CLDFLAGS)                              \
    $(top_srcdir)/src/common/lib_purger_common.a \
    $(top_srcdir)/src/hiredis/libhiredis.a

INCLUDES = \
    $(MPI_CFLAGS)               \
    $(libcircle_CFLAGS)         \
    -I$(top_srcdir)/src/hiredis
