AC_PREREQ([2.68])

AC_INIT([dstat], [0.0.1], [github-hpc@lanl.gov])
AC_CONFIG_SRCDIR([src])
AC_CONFIG_HEADERS([config.h])

AC_CONFIG_AUX_DIR(auxdir)
AC_CONFIG_MACRO_DIR([m4])

AM_INIT_AUTOMAKE(dstat, [0.0.1])

# Checks for programs.
AC_PROG_CC

# Libtool
LT_INIT
AC_SUBST(LIBTOOL_DEPS)

# Checks for header files.
AC_CHECK_HEADERS([stdlib.h string.h unistd.h])

# Check for libcircle
PKG_CHECK_MODULES([libcircle], libcircle)

# Checks for library functions.
AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK

echo
echo "========================================================"
echo "==          dstat: final build configuration          =="
echo "========================================================"
echo "External Library Support:"
echo "  libcircle ...................................... TODO"
echo
echo "Build Options:"
echo "  Logging level .................................. TODO"
echo "========================================================"
echo

AC_CONFIG_FILES([ \
    Makefile             \
    src/Makefile         \
    src/hiredis/Makefile \
    doc/Makefile         \
    doc/man/Makefile
])

AC_OUTPUT
