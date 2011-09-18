bin_PROGRAMS = treewalk
treewalk_SOURCES = sprintstatf.c hash.c treewalk.c
treewalk_LDADD = \
    -lcrypto                               \
    $(libcircle_LIBS)                      \
    $(MPI_CLDFLAGS)                        \
    $(top_srcdir)/src/hiredis/libhiredis.a

INCLUDES = \
    $(MPI_CFLAGS)               \
    $(libcircle_CFLAGS)         \
    -I$(top_srcdir)/src/hiredis
