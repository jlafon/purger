#!/bin/bash
# You may need to modify this line to the correct path of your installed psql
# Use this file at your own risk.  
su - postgres -c "/usr/local/pgsql/bin/psql -f /etc/purger/postgres_setup/init.sql && /usr/local/pgsql/bin/psql -f /etc/purger/postgres_setup/scratch.sql" &&
su -c "mkdir -p /var/lib/pgsql/data/" &&
su -c "echo 'host	scratch		treewalk	127.0.0.1/32	password' >> /var/lib/pgsql/data/postgresql.conf" 
