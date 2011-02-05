################################################################################
# Copyright 2008-2009 Los Alamos National Security, LLC. All Rights Reserved.  #
#                                                                              #
# Redistribution and use in source and binary forms, with or without           #
# modification, are permitted provided that the following conditions are met:  #
#                                                                              #
#                                                                              #
#   o Redistributions of source code must retain the above copyright notice,   #
#     this list of conditions and the following disclaimer.                    #
#   o Redistributions in binary form must reproduce the above copyright        #
#     notice, this list of conditions and the following disclaimer in the      #
#     documentation and/or other materials provided with the distribution.     #
#   o Neither the name of the copyright holders nor the names of its           #
#     contributors may be used to endorse or promote products derived from     #
#     this software without specific prior written permission.                 #
#                                                                              #
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"  #
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE    #
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE   #
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE    #
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR          #
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF         #
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS     #
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN      #
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)      #
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE   #
# POSSIBILITY OF SUCH DAMAGE.                                                  #
################################################################################

AC_PREREQ([2.65])
AC_INIT([lanl-purger], [0.1], [jharrodlafon@users.sourceforge.net])
AC_COPYRIGHT([
        [Copyright 2008-2010 Los Alamos National Security, LLC. All Rights Reserved.]
        ])

# Put all build stuff in config
AC_CONFIG_AUX_DIR([config])
AC_CONFIG_MACRO_DIR([config])
    
AM_INIT_AUTOMAKE([foreign -Wall -Werror])

# Minimize output    
m4_ifdef([AM_SILENT_RULES],[AM_SILENT_RULES([yes])])

AC_PROG_CC
AC_PROG_CC_C_O

AM_PROG_CC_C_O
# Not necessary but doesn't hurt anything
AC_CONFIG_HEADERS([config.h])
AC_USE_SYSTEM_EXTENSIONS
AC_PROG_LIBTOOL
AC_GNU_SOURCE
AC_PROG_MAKE_SET
AC_CHECK_HEADERS([netdb.h netinet/in.h stdlib.h string.h sys/socket.h sys/time.h unistd.h])
AC_OUTPUT([
        Makefile
        lib/config/Makefile
        lib/ldap/Makefile
        lib/log/Makefile
        lib/mail/Makefile
        lib/prm/Makefile
        lib/utils/Makefile
        src/Makefile
        lib/Makefile
        test/Makefile
        test/email/Makefile
        test/ldap/Makefile
        test/log/Makefile
        src/purger/Makefile
        src/treewalk/Makefile
        src/util/Makefile
        ])
AC_OUTPUT

