#!/bin/bash 
echo "Testing email..."
email/purger_mailtest
echo "Testing LDAP..."
ldap/purger_ldapemail_test
echo "Testing log..."
log/purger_logtest
echo "Testing database insert..."
insert/purger_insert .
