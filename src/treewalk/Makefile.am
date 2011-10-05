include $(top_srcdir)/common.mk

bin_PROGRAMS = treewalk
treewalk_SOURCES = sprintstatf.c hash.c treewalk.c
treewalk_LDADD = \
    -lcrypto                                     \
    $(libcircle_LIBS)                            \
    $(MPI_CLDFLAGS)                              \
    $(top_srcdir)/src/common/lib_purger_common.a \
    $(top_srcdir)/src/hiredis/libhiredis.a

treewalk_CPPFLAGS = \
    $(MPI_CFLAGS)               \
    $(libcircle_CFLAGS)         \
    -I$(top_srcdir)/src/hiredis \
    -I$(top_srcdir)/src/common
