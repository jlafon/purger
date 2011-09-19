bin_PROGRAMS = reaper
reaper_SOURCES = hash.c reaper.c
reaper_LDADD = \
    -lcrypto                               \
    $(libcircle_LIBS)                      \
    $(MPI_CLDFLAGS)                        \
    $(top_srcdir)/src/hiredis/libhiredis.a

INCLUDES = \
    $(MPI_CFLAGS)               \
    $(libcircle_CFLAGS)         \
    -I$(top_srcdir)/src/hiredis
