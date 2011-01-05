#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(lanl-purger, 0.1, jharrodlafon@users.sourceforge.net)
AC_CONFIG_SRCDIR([purger.h])
#AC_CONFIG_HEADER([config.h])

# Checks for programs.
AC_PROG_CC

# Checks for libraries.
AC_CHECK_LIB([config], [config_init],[],
	[AC_MSG_ERROR([libconfig is needed for compilation])])
AC_CHECK_LIB([lber], [ber_bvfree],[],
	[AC_MSG_ERROR([OpenLDAP LBER is needed for compilation])])
AC_CHECK_LIB([ldap], [ldap_initialize],[],
	[AC_MSG_ERROR([OpenLDAP is needed for compilation])])
AC_CHECK_LIB([pq], [PQclear],[],
	[AC_MSG_ERROR([Postgres library is needed for compilation])])

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([malloc.h stdlib.h string.h])

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.
AC_FUNC_LSTAT
AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK
AC_FUNC_STRFTIME
AC_CHECK_FUNCS([strerror])
AC_CONFIG_FILES([Makefile])
AC_PROG_MAKE_SET
AM_INIT_AUTOMAKE([lanl-purger],[0.1])
AC_OUTPUT
