bin_PROGRAMS = warnusers
warnusers_SOURCES = warnusers.c
warnusers_LDADD = \
    -lcrypto -lldap                        \
    $(libcircle_LIBS)                      \
    $(MPI_CLDFLAGS)                        \
    $(top_srcdir)/src/common/lib_purger_common.a \
    $(top_srcdir)/src/hiredis/libhiredis.a

warnusers_CPPFLAGS = \
    $(MPI_CFLAGS)               \
    $(libcircle_CFLAGS)         \
    -I$(top_srcdir)/src/hiredis \
    -I$(top_srcdir)/src/common
