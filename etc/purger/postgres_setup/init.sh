#!/bin/bash
su - postgres -c "/usr/local/pgsql/bin/psql -f /etc/postgres_setup/init.sql && /usr/local/pgsql/bin/psql -f /etc/postgres_setup/scratch.sql" &&
echo "host	scratch		treewalk	127.0.0.1/32	password" >> /var/lib/pgsql/data/postgresql.conf
