AC_INIT([purger],[0.1],[github-hpc@lanl.gov],[purger],[http://github.com/hpc/purger])

# Keep artifacts in directories as much as possible.
AC_CONFIG_AUX_DIR(auxdir)
AC_CONFIG_MACRO_DIR([m4])

AM_INIT_AUTOMAKE([-Wall no-define])
AC_CONFIG_HEADERS([config.h])

# Check for compilers.
AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_CXX

# Needed for tests
X_AC_PURGER_CHECK

# Pull in libtool. We need to support ancient autotools, so don't use LT_INIT.
#LT_INIT
AC_PROG_RANLIB
AC_PROG_LIBTOOL

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([stdlib.h string.h unistd.h arpa/inet.h fcntl.h libintl.h])
AC_CHECK_HEADERS([limits.h netdb.h netinet/in.h stddef.h sys/socket.h sys/time.h])

# Check for libcircle
PKG_CHECK_MODULES([libcircle], libcircle)

# Checks for library functions.
AC_C_INLINE
AC_FUNC_REALLOC
AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK
AC_FUNC_LSTAT
AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_FUNC_STRERROR_R

# Various functions used through the codebase
AC_CHECK_FUNCS([gethostbyname])
AC_CHECK_FUNCS([gettimeofday])
AC_CHECK_FUNCS([memmove])
AC_CHECK_FUNCS([memset])
AC_CHECK_FUNCS([pow])
AC_CHECK_FUNCS([realpath])
AC_CHECK_FUNCS([rpmatch])
AC_CHECK_FUNCS([select])
AC_CHECK_FUNCS([socket])
AC_CHECK_FUNCS([strcasecmp])
AC_CHECK_FUNCS([strchr])
AC_CHECK_FUNCS([strerror])
AC_CHECK_FUNCS([strncasecmp])
AC_CHECK_FUNCS([strstr])
AC_CHECK_FUNCS([strtol])
AC_CHECK_FUNCS([strtoul])

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
echo "  Unit tests ..................................... $x_ac_purger_check"
echo "========================================================"
echo

AC_CONFIG_FILES([ \
    Makefile               \
    src/Makefile           \
    src/common/Makefile    \
    src/hiredis/Makefile   \
    src/treewalk/Makefile  \
    src/reaper/Makefile    \
    src/warnusers/Makefile \
    tests/Makefile         \
    doc/Makefile           \
    doc/man/Makefile
])

AC_OUTPUT
