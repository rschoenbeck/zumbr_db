#!/bin/bash
# Export data files to Zumbr DB

echo ""
echo "*** UPLOADING DATA TO DB ***"

find data -name \*.csv | while read csv_file; do
	echo "Creating table '$csv_file'..."
	table_name=`echo $csv_file | grep -E "[A-Za-z0-9_]+.csv" -o | sed 's/.\{4\}$//'`
	psql -v tablename="$table_name" -h $db_host -d $db_name -U $db_user -f "data/$table_name.sql"
	echo "Uploading file '$csv_file'..."
	psql -h $db_host -d $db_name -U $db_user -c "\copy $table_name FROM '$csv_file' DELIMITER ',' CSV HEADER"
	echo "Setting grants for table '$table_name'"
	psql -v tableschema="$db_schema" -v tablename="$table_name" -h $db_host -d $db_name -U $db_user -f grants/grants.sql
done

echo "File upload complete."
echo ""