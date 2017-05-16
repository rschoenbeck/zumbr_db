#!/bin/bash
# Deploy tables for ZUMBR course project database

source conf/environment_vars.sh
export PGPASSWORD=$db_pwd

echo "Uploading files..."

source data_upload.sh

echo "Deploying SQL tables..."

find sql -name \*.sql | while read sql_file; do
	echo "Deploying file '$sql_file'"
	table_name=`echo $sql_file | grep -E "[A-Za-z_]+.sql" -o | sed 's/.\{4\}$//'`
	psql -v tableschema="$db_schema" -v tablename="$table_name" -h $db_host -d $db_name -U $db_user -f $sql_file
	echo "Setting grants for table '$table_name'"
	psql -v tableschema="$db_schema" -v tablename="$table_name" -h $db_host -d $db_name -U $db_user -f grants/grants.sql
done

source test.sh
