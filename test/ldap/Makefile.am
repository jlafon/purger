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
AM_LIBTOOLFLAGS = --silent
bin_PROGRAMS = purger_bensearch purger_ldapemail_test
purger_bensearch_SOURCES = bensearch.c
purger_bensearch_CFLAGS = -I../../include \
                   -DWITHOPEN_LDAP -DWITHLDAP_DEPRECATED
purger_bensearch_LDFLAGS = -lldap -llber
purger_bensearch_LDADD = ../../lib/ldap/.libs/libldap.la
purger_ldapemail_test_SOURCES = main.c
purger_ldapemail_test_CFLAGS = -I../../include \
                   -DWITHOPEN_LDAP -DWITHLDAP_DEPRECATED
purger_ldapemail_test_LDFLAGS = -lldap -llber
purger_ldapemail_test_LDADD = ../../lib/ldap/.libs/libldap.la

