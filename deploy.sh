#!/bin/bash
#Deploy SQL views for ZUMBR course project database

source conf/environment_vars.sh

export PGPASSWORD=$db_pwd

echo "Deploying tables..."

find sql -name \*.sql | while read sql_file; do
	echo "Deploying file '$sql_file'"
	psql -h $db_host -d $db_name -U $db_user -f $sql_file
done
