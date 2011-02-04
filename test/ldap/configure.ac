#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.65])
AC_INIT([lanl-purger-tests-ldap], [0.1], [jharrodlafon@users.sourceforge.net])
AC_CONFIG_SRCDIR([bensearch.c])

# Checks for programs.
AC_PROG_CC

# Checks for libraries.
AC_CHECK_LIB([lber], [ber_free],[],[
        AC_MSG_ERROR([OpenLDAP LBER is required])])
AC_CHECK_LIB([ldap], [ldap_init],[],[
        AC_MSG_ERROR([OpenLDAP is required])])

# Checks for header files.
AC_CHECK_HEADERS([stdlib.h string.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.

AC_CONFIG_FILES([Makefile])
AM_INIT_AUTOMAKE([lanl-purger-tests-ldap],[0.1])
AC_OUTPUT
