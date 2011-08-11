# Check for autoconf version.
AC_PREREQ([2.68])

AC_INIT([purger], [1.2.0], [github-hpc@lanl.gov])

AC_CONFIG_SRCDIR([src/pstat/main.c])
AC_CONFIG_HEADERS([config.h])

# Checks for programs
AC_PROG_CC

# Checks for libraries.


# Checks for headers.
AC_CHECK_HEADERS([stdlib.h string.h])

# Checks for typedefs, structs, and compiler options.
AC_TYPE_SIZE_T

# Checks for library functs.
AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK
AC_FUNC_MALLOC

AC_OUTPUT
