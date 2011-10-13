noinst_LIBRARIES = lib_purger_common.a
lib_purger_common_a_CFLAGS = -I$(top_srcdir)/common
lib_purger_common_a_SOURCES = state.c redis.c hash.c
lib_purger_common_a_LIBADD = \
	$(top_srcdir)/src/hiredis/libhiredis.a	
lib_purger_common_a_CPPFLAGS = \
	-I$(top_srcdir)/src/hiredis
