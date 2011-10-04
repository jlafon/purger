TESTS = check_filehash
check_PROGRAMS = check_filehash

check_filehash_SOURCES = check_filehash.c $(top_builddir)/src/treewalk/hash.c
check_filehash_CFLAGS = -I$(top_builddir)/src/treewalk/ @CHECK_CFLAGS@
check_filehash_LDADD = @CHECK_LIBS@
