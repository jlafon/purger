AC_INIT([purger],[0.0.1],[github-hpc@lanl.gov])
AC_CONFIG_SRCDIR([src])
AC_CONFIG_HEADERS([config.h])

AC_CONFIG_AUX_DIR(auxdir)
AC_CONFIG_MACRO_DIR([m4])

AM_INIT_AUTOMAKE(purger, [0.0.1])

# Checks for programs.
AC_PROG_CC
AC_PROG_RANLIB
AM_PROG_CC_C_O

# Libtool
# Pull in libtool. We need to support ancient autotools, so don't use LT_INIT.
#LT_INIT
AC_PROG_LIBTOOL

# Checks for header files.
AC_CHECK_HEADERS([stdlib.h string.h unistd.h])

# Check for libcircle
PKG_CHECK_MODULES([libcircle], libcircle)

# Checks for library functions.
AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK

# Check for MPI
LX_FIND_MPI
test "x$have_C_mpi" = xyes || \
    AC_MSG_ERROR([You should check to see if MPI is setup properly.])
AM_CONDITIONAL(HAVE_MPI, [test "x$have_C_mpi" = xyes])

echo
echo "========================================================"
echo "==         purger: final build configuration          =="
echo "========================================================"
echo "External Library Support:"
echo "  libcircle ...................................... ????"
echo "  MPI ............................................ $have_C_mpi"
echo
echo "Build Options:"
echo "  N/A"
echo "========================================================"
echo

AC_CONFIG_FILES([ \
    Makefile              \
    src/Makefile          \
    src/common/Makefile   \
    src/hiredis/Makefile  \
    src/treewalk/Makefile \
    src/reaper/Makefile   \
    src/warnusers/Makefile \
    doc/Makefile          \
    doc/man/Makefile
])

AC_OUTPUT
