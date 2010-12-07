INCLUDE=/usr/local/include
LIB=/usr/local/lib
CFLAGS=-ansi -pedantic

all: ldap ben

ldap:
	gcc $(CFLAGS) -DWITH_OPENLDAP -DLDAP_DEPRECATED -c ldap.c -I$(INCLUDE)/
	gcc $(CFLAGS) main.c ldap.o -o ldapemail -L$(LIB) -lldap -llber

ben:
	gcc $(CFLAGS) -DWITH_OPENLDAP -o bensearch bensearch.c -I$(INCLUDE)/ -L$(LIB) -lldap -llber

clean:
	rm -f ldap_add ldap_search ldap_delete ldap_modify ldap_search_async
