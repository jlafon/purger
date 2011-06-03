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

EXTRA_DIST = deleter.h

libdir = ../../lib

convenience_libs = \
		   $(libdir)/config/.libs/libconfig.la \
		   $(libdir)/ldap/.libs/libldap.la     \
		   $(libdir)/log/.libs/liblog.la
AM_LIBTOOLFLAGS = --silent

bin_PROGRAMS = purger_chkscratch purger_update_expired purger_deleter purger_dbtest

purger_dbtest_SOURCES = dbtest.c

purger_dbtest_CFLAGS = -lpq -lconfig -I`pg_config --includedir` -L`pg_config --libdir`

purger_dbtest_LDADD = $(convenience_libs)

purger_dbtest_LDFLAGS = -lpq -lconfig -I`pg_config --includedir` -L`pg_config --libdir`

purger_chkscratch_SOURCES = ../../include/lconfig.h ../../include/lanl-ldap.h ../../include/log.h chkscratch.c

purger_chkscratch_CFLAGS = -I../../include -I`pg_config --includedir`

purger_chkscratch_LDFLAGS = -lpq -lconfig -I../../include -I`pg_config --includedir` -L`pg_config --libdir`

purger_chkscratch_LDADD = $(convenience_libs)

purger_update_expired_SOURCES = update_expired.c

purger_update_expired_CFLAGS = -I`pg_config --includedir`

purger_update_expired_LDFLAGS = -lpq -lconfig -I`pg_config --includedir` -L`pg_config --libdir`

purger_update_expired_LDADD = $(convenience_libs)

purger_deleter_CFLAGS = -I`pg_config --includedir`

purger_deleter_SOURCES = deleter.c

purger_deleter_LDFLAGS = -lpq -I`pg_config --includedir` -L`pg_config --libdir`
